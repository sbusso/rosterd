//! The session commands of R14.3. KEY is resolved against the swarm snapshot (this node
//! included), then the exact key goes to the local API, which proxies to the owner, R7.5.

use std::time::Duration;

use reqwest::Method;
use rosterd_proto::{Explain, PeerState, Record, SwarmSnapshot};
use serde_json::{Map, Value, json};

use super::client::{emit, parse, table};
use super::resolve::{display_name, resolve};
use super::{Client, Command, Exit, Out, Policy, Wait};
use crate::config::{Config, config_dir};

pub async fn run(client: &Client, config: &Config, command: Command, json: bool) -> Out<()> {
    match command {
        Command::Start { harness, cwd, name, interactive, detach, node, policy, model, effort, env } => {
            let env = env
                .iter()
                .map(|pair| pair.split_once('=').map(|(k, v)| (k.to_string(), Value::String(v.to_string()))).ok_or_else(|| Exit::user(format!("--env {pair}: expected K=V"))))
                .collect::<Out<Map<String, Value>>>()?;
            let cwd = match cwd {
                Some(cwd) => cwd,
                None => std::env::current_dir().map_err(|e| Exit::user(format!("no --cwd and no current directory: {e}")))?.to_string_lossy().into_owned(),
            };
            let body = json!({
                "harness": harness, "cwd": cwd, "name": name, "model": model, "effort": effort, "permission_policy": policy.map(policy_word), "env": env,
                "lane": interactive.then_some("interactive"),
            });
            let path = match node {
                Some(name) => format!("/swarm/{}/sessions", node_id(client, &name).await?),
                None => "/sessions".into(),
            };
            let body = client.call(Method::POST, &path, Some(body)).await?;
            // An interactive start from a terminal is joined at once; the key goes to stderr so
            // a script still gets it.
            if interactive && !detach && !json && std::io::IsTerminal::is_terminal(&std::io::stdout()) {
                let value: Value = parse(&body)?;
                for warning in value["warnings"].as_array().into_iter().flatten().filter_map(Value::as_str) {
                    eprintln!("warning: {warning}");
                }
                let key = value["session_key"].as_str().unwrap_or_default().to_string();
                eprintln!("{key}");
                return super::attach::attach(&super::client::socket_path(config), &key).await.map_err(Into::into);
            }
            started(&body, json)
        }
        Command::Prompt { key, text, wait, timeout } => {
            let key = session_key(client, &key).await?;
            let body = json!({ "prompt": text, "wait_until": wait.map(wait_word), "timeout_ms": timeout * 1000 });
            let path = format!("/sessions/{key}/prompt");
            let body = match client.call_with(Method::POST, &path, Some(body), Duration::from_secs(timeout + 30)).await {
                Ok(body) => body,
                // The daemon kept the turn; the activity now is the honest answer, R14.3.
                Err(exit) if exit.message.starts_with("timed out") => {
                    let record = parse::<Record>(&client.call(Method::GET, &format!("/sessions/{key}"), None).await?)?;
                    return Err(Exit::user(format!("timeout after {timeout}s; activity {}", activity(&record))));
                }
                Err(exit) => return Err(exit),
            };
            if json {
                return emit(&body);
            }
            let outcome: Value = parse(&body)?;
            let record: Record = serde_json::from_value(outcome["record"].clone())?;
            if record.session_key != key {
                println!("session {}", record.session_key);
            }
            println!("activity {}", activity(&record));
            if let Some(recap) = outcome["recap"].as_str() {
                println!("recap {recap}");
            }
            if outcome["reached"] == false {
                return Err(Exit::user(format!("timeout after {timeout}s; activity {}", activity(&record))));
            }
            Ok(())
        }
        Command::Cancel { key } => action(client, &key, Method::POST, "/cancel", None, json, "cancelled").await,
        Command::Stop { key } => action(client, &key, Method::DELETE, "", None, json, "stopped").await,
        Command::Suspend { key } => action(client, &key, Method::POST, "/suspend", None, json, "suspended").await,
        Command::Resume { key } => {
            let key = session_key(client, &key).await?;
            let body = client.call(Method::POST, &format!("/sessions/{key}/resume"), None).await?;
            if json {
                return emit(&body);
            }
            let record: Record = parse(&body)?;
            println!("resumed {key} as {}", record.session_key);
            Ok(())
        }
        Command::Handoff { key, to } => {
            let key = session_key(client, &key).await?;
            let nodes: Vec<rosterd_proto::NodeHealth> = parse(&client.call(Method::GET, "/swarm/nodes", None).await?)?;
            let node = nodes.into_iter().find(|n| n.node_id == to || n.name == to).ok_or_else(|| Exit::user(format!("no node {to}; see rosterd nodes")))?;
            // A suspend here, then a holder start and a session load there.
            let body = client.call_with(Method::POST, &format!("/sessions/{key}/handoff"), Some(json!({ "node": node.node_id })), Duration::from_secs(120)).await?;
            if json {
                return emit(&body);
            }
            let moved: Value = parse(&body)?;
            let from: Record = serde_json::from_value(moved["from"].clone())?;
            println!("{} → {} {}", display_name(&from), node.name, moved["to"]["session_key"].as_str().unwrap_or_default());
            Ok(())
        }
        Command::Name { key, label, clear } => {
            let done = if clear { "cleared the name of" } else { "named" };
            action(client, &key, Method::POST, "/name", Some(json!({ "name": label })), json, done).await
        }
        Command::Open { key } => open(client, config, &key).await,
        Command::Attach { key } => {
            let key = session_key(client, &key).await?;
            super::attach::attach(&super::client::socket_path(config), &key).await.map_err(Into::into)
        }
        Command::Ui => open_ui(config, "/ui"),
        Command::Allow { key, always } => answer(client, &key, if always { Choice::AllowAlways } else { Choice::Allow }, None, json).await,
        Command::Deny { key, reason } => answer(client, &key, Choice::Deny, reason, json).await,
        Command::Spawn { key, harness, cwd, name } => {
            let key = session_key(client, &key).await?;
            let body = json!({ "harness": harness, "cwd": cwd, "name": name });
            let body = client.call(Method::POST, &format!("/sessions/{key}/spawn"), Some(body)).await?;
            started(&body, json)
        }
        Command::Read { key } => {
            let key = session_key(client, &key).await?;
            let body = client.call(Method::GET, &format!("/sessions/{key}"), None).await?;
            if json {
                return emit(&body);
            }
            print!("{}", read_text(&parse(&body)?));
            Ok(())
        }
        Command::Explain { key } => {
            let key = session_key(client, &key).await?;
            let body = client.call(Method::GET, &format!("/sessions/{key}/explain"), None).await?;
            if json {
                return emit(&body);
            }
            print!("{}", explain_text(&parse(&body)?));
            Ok(())
        }
        other => unreachable!("not a session command: {other:?}"),
    }
}

fn policy_word(policy: Policy) -> &'static str {
    match policy {
        Policy::Auto => "auto",
        Policy::Attention => "attention",
    }
}

fn wait_word(wait: Wait) -> &'static str {
    match wait {
        Wait::Idle => "idle",
        Wait::NeedsAttention => "needs_attention",
        Wait::Ended => "ended",
    }
}

fn activity(record: &Record) -> String {
    if record.liveness == rosterd_proto::Liveness::Suspended {
        return "suspended".into();
    }
    serde_json::to_value(record.activity).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_default()
}

/// KEY to the exact session_key, R14.1, against everything this node knows.
/// `--node NAME` to its id, from the swarm snapshot.
pub(super) async fn node_id(client: &Client, name: &str) -> Out<String> {
    let swarm: SwarmSnapshot = parse(&client.call(Method::GET, "/swarm/snapshot", None).await?)?;
    swarm.nodes.into_iter().find(|n| n.name.eq_ignore_ascii_case(name)).map(|n| n.node_id).ok_or_else(|| Exit::user(format!("no node {name} in the swarm")))
}

pub(super) async fn session_key(client: &Client, key: &str) -> Out<String> {
    let swarm: SwarmSnapshot = parse(&client.call(Method::GET, "/swarm/snapshot", None).await?)?;
    let local = swarm.nodes.iter().find(|n| n.state == PeerState::Local).map(|n| n.node_id.clone()).unwrap_or_default();
    let records: Vec<Record> = swarm.records.into_iter().map(|r| r.record).collect();
    resolve(key, &local, &records).map(|r| r.session_key.clone())
}

/// One request on the resolved key; the record comes back, `done` names it on a terminal.
async fn action(client: &Client, key: &str, method: Method, suffix: &str, body: Option<Value>, json: bool, done: &str) -> Out<()> {
    let key = session_key(client, key).await?;
    let body = client.call(method, &format!("/sessions/{key}{suffix}"), body).await?;
    if json {
        return emit(&body);
    }
    let record: Record = parse(&body)?;
    println!("{done} {} ({}, {})", record.session_key, display_name(&record), activity(&record));
    Ok(())
}

/// `start` and `spawn`: the session key on stdout, the `warnings` of R16.1 on stderr.
fn started(body: &str, json: bool) -> Out<()> {
    let value: Value = parse(body)?;
    for warning in value["warnings"].as_array().into_iter().flatten().filter_map(Value::as_str) {
        eprintln!("warning: {warning}");
    }
    if json {
        return emit(body);
    }
    println!("{}", value["session_key"].as_str().unwrap_or_default());
    Ok(())
}

#[derive(Clone, Copy, PartialEq)]
enum Choice {
    Allow,
    AllowAlways,
    Deny,
}

/// `allow` and `deny`, R5.3 attention: the first pending request answered with the option
/// whose kind or id matches; allow-always falls back to allow with a note.
async fn answer(client: &Client, key: &str, choice: Choice, reason: Option<String>, json: bool) -> Out<()> {
    let key = session_key(client, key).await?;
    let session: Value = parse(&client.call(Method::GET, &format!("/sessions/{key}"), None).await?)?;
    let Some(pending) = session["state"]["pending"].as_array().and_then(|p| p.first()).cloned() else {
        return Err(Exit::user(format!("no pending permission request on {key}")));
    };
    let options: Vec<Value> = pending["options"].as_array().cloned().unwrap_or_default();
    let pick = |wanted: Choice| options.iter().find(|o| matches_choice(o, wanted));
    let option = match choice {
        Choice::AllowAlways => match pick(Choice::AllowAlways) {
            Some(option) => Some(option),
            None => {
                eprintln!("note: allow-always is not offered; answering allow");
                pick(Choice::Allow)
            }
        },
        other => pick(other),
    };
    let Some(option) = option else {
        let offered = options.iter().map(|o| format!("{} ({})", o["option_id"], o["kind"])).collect::<Vec<_>>().join(", ");
        return Err(Exit::user(format!("no matching option; offered: {offered}")));
    };
    let mut body = json!({ "request_id": pending["request_id"], "outcome": "selected", "option_id": option["option_id"] });
    if let Some(reason) = reason {
        body["reason"] = Value::String(reason);
    }
    let body = client.call(Method::POST, &format!("/sessions/{key}/permission"), Some(body)).await?;
    if json {
        return emit(&body);
    }
    let verb = if choice == Choice::Deny { "denied" } else { "allowed" };
    println!("{verb} {} on {key}", pending["tool"].as_str().unwrap_or("the request"));
    Ok(())
}

/// ACP kinds are allow_once, allow_always, reject_once, reject_always; the gate of R16.2 uses
/// the ids allow, allow_always, deny.
fn matches_choice(option: &Value, choice: Choice) -> bool {
    let kind = option["kind"].as_str().unwrap_or_default();
    let id = option["option_id"].as_str().unwrap_or_default();
    match choice {
        Choice::Allow => kind == "allow_once" || id == "allow" || id == "allow_once",
        Choice::AllowAlways => kind == "allow_always" || id == "allow_always",
        Choice::Deny => kind == "reject_once" || id == "deny" || id == "reject_once",
    }
}

/// `open`: tmux through `rosterd-open` when it is on PATH, else the command to run;
/// headless prints the /ui URL and opens it when a display is present.
async fn open(client: &Client, config: &Config, key: &str) -> Out<()> {
    let key = session_key(client, key).await?;
    let record: Record = parse(&client.call(Method::GET, &format!("/sessions/{key}"), None).await?)?;
    let command = if let Some(t) = &record.tmux {
        format!("tmux attach -t {0} \\; select-window -t {0}:{1} \\; select-pane -t {2}", t.session, t.window_index, t.pane_id)
    } else if record.holder.is_some() || record.lane == rosterd_proto::Lane::Headless {
        return open_ui(config, &format!("/ui/sessions/{key}"));
    } else {
        return Err(Exit::user(format!("nothing to open for {key}: no tmux or holder handle")));
    };
    if which("rosterd-open") {
        let handle = json!({ "kind": "tmux", "machine": record.node, "session_key": key, "tmux": record.tmux });
        let status = std::process::Command::new("rosterd-open").arg("--handle").arg(handle.to_string()).status()?;
        return if status.success() { Ok(()) } else { Err(Exit::user(format!("rosterd-open exited with {status}"))) };
    }
    println!("{command}");
    Ok(())
}

/// Prints the page's loopback URL with the token, and opens it when there is a display.
fn open_ui(config: &Config, path: &str) -> Out<()> {
    let token = std::fs::read_to_string(config_dir().join("loopback.token")).map(|t| t.trim().to_string()).unwrap_or_default();
    let url = format!("http://127.0.0.1:{}{path}{}", config.node.loopback_port, if token.is_empty() { String::new() } else { format!("?token={token}") });
    println!("{url}");
    if display_present() {
        let opener = if cfg!(target_os = "macos") { "open" } else { "xdg-open" };
        std::process::Command::new(opener).arg(&url).status()?;
    }
    Ok(())
}

fn display_present() -> bool {
    cfg!(target_os = "macos") || std::env::var_os("DISPLAY").is_some() || std::env::var_os("WAYLAND_DISPLAY").is_some()
}

fn which(name: &str) -> bool {
    std::env::var_os("PATH").is_some_and(|path| std::env::split_paths(&path).any(|dir| dir.join(name).is_file()))
}

/// `read`: every roster field, then the runtime handle, recap, pending request and children;
/// never the transcript.
fn read_text(session: &Value) -> String {
    let mut out = String::new();
    let Some(fields) = session.as_object() else { return session.to_string() };
    for (name, value) in fields.iter().filter(|(k, _)| !matches!(k.as_str(), "state" | "children" | "warnings")) {
        out += &format!("{name:<20} {}\n", scalar(value));
    }
    let runtime = match (fields.get("tmux"), fields.get("holder")) {
        (Some(t), _) if !t.is_null() => "tmux",
        (_, Some(h)) if !h.is_null() => "headless",
        _ => "none",
    };
    out += &format!("{:<20} {runtime}\n", "runtime");
    let state = &session["state"];
    if let Some(recap) = state["last_recap"].as_str() {
        out += &format!("{:<20} {recap}\n", "recap");
    }
    for pending in state["pending"].as_array().into_iter().flatten() {
        let options = pending["options"].as_array().into_iter().flatten().filter_map(|o| o["option_id"].as_str()).collect::<Vec<_>>().join("|");
        out += &format!("{:<20} {} {} [{options}] at {}\n", "pending", scalar(&pending["tool"]), scalar(&pending["summary"]), scalar(&pending["at"]));
    }
    for child in session["children"].as_array().into_iter().flatten() {
        out += &format!("{:<20} {} {} {}\n", "child", scalar(&child["session_key"]), scalar(&child["name"]), scalar(&child["activity"]));
    }
    out
}

fn scalar(value: &Value) -> String {
    match value {
        Value::Null => "-".into(),
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// `explain`: field → source → at, then the rejected claims with their reasons, R14.3.
fn explain_text(explain: &Explain) -> String {
    let source = |s: rosterd_proto::Source| format!("{s:?}").to_lowercase();
    let rows = explain
        .fields
        .iter()
        .map(|f| (vec![f.field.clone(), source(f.source), f.at.to_rfc3339(), scalar(&f.value)], false))
        .collect::<Vec<_>>();
    let mut out = format!("session {}\n", explain.session_key);
    out += &table(&["FIELD", "SOURCE", "AT", "VALUE"], &rows);
    if !explain.rejected.is_empty() {
        let rows = explain
            .rejected
            .iter()
            .map(|r| {
                let what = match (&r.field, &r.activity, &r.event) {
                    (Some(field), _, _) => field.clone(),
                    (_, Some(activity), event) => format!("{} {}", serde_json::to_value(activity).map(|v| scalar(&v)).unwrap_or_default(), event.clone().unwrap_or_default()),
                    _ => "-".into(),
                };
                (vec![source(r.source), what, r.at.to_rfc3339(), r.reason.clone()], false)
            })
            .collect::<Vec<_>>();
        out += "\nrejected\n";
        out += &table(&["SOURCE", "CLAIM", "AT", "REASON"], &rows);
    }
    out
}
