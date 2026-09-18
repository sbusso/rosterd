//! `rosterd <command>`, R14: every command but `daemon` is a client of the local socket of R6.
//! `--json` prints the API body byte for byte; without it a table or a short text. Exit codes,
//! R14.2: 0 ok, 1 user error, 2 daemon unreachable, 3 swarm peer needed and unreachable.
//!
//! OWNER: the cli agent.

mod attach;
mod attention;
mod client;
mod list;
pub(crate) mod resolve;
mod session;

use std::path::Path;

use clap::{Args, Subcommand, ValueEnum};
use reqwest::Method;
use rosterd_proto::{DayUsage, NodeHealth, NodeUsage, SwarmUsage};
use serde_json::{Value, json};

use crate::config::Config;
use crate::node::VERSION;
pub use client::{Client, Exit, Out};
use client::{emit, parse, table};

/// R14.1: none is this node, `--node` one named node, `--swarm` every node the local one knows.
#[derive(Args, Debug, Clone, Default)]
pub struct Scope {
    /// One named node in the swarm.
    #[arg(long, conflicts_with = "swarm")]
    pub node: Option<String>,
    /// Every node, including unreachable ones with their age.
    #[arg(long)]
    pub swarm: bool,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
pub enum Policy {
    Auto,
    Attention,
}

#[derive(ValueEnum, Debug, Clone, Copy)]
#[value(rename_all = "snake_case")]
pub enum Wait {
    Idle,
    NeedsAttention,
    Ended,
}

// R14.3, in the spec's order.
#[derive(Subcommand, Debug)]
pub enum Command {
    /// One row per session.
    List {
        #[command(flatten)]
        scope: Scope,
    },
    /// The table again on every change; Ctrl+C stops.
    Watch {
        #[command(flatten)]
        scope: Scope,
    },
    /// One line per change across the swarm; Ctrl+C stops.
    Changes,
    /// The sessions waiting on a human and what each waits on; nothing when none.
    Attention,
    /// What happened, from the journal, R18: this node's entries, or every node's with --swarm.
    Journal {
        /// Every reachable node, merged by time.
        #[arg(long, conflicts_with = "since")]
        swarm: bool,
        /// Entries after this seq of this node's journal.
        #[arg(long, conflicts_with = "after")]
        since: Option<u64>,
        /// Entries after this time (RFC 3339).
        #[arg(long)]
        after: Option<chrono::DateTime<chrono::Utc>>,
        /// One session's entries.
        #[arg(long)]
        session: Option<String>,
        /// Keep printing as entries arrive; Ctrl+C stops.
        #[arg(long)]
        follow: bool,
    },
    /// Node, version, listeners, swarm, holders, sources.
    Status,
    /// Swarm membership and health.
    Nodes,
    /// Tokens and cost per day, harness and model from the harnesses' transcripts on this node.
    Usage {
        /// Every reachable node, plus a swarm total.
        #[arg(long)]
        swarm: bool,
        /// `7d`, `12h`, a date or a datetime.
        #[arg(long, default_value = "7d")]
        since: String,
    },
    /// One session in full, never the transcript.
    Read { key: String },
    /// Which source set each field and when, and which claims were rejected.
    Explain { key: String },
    /// Create a session, R5.1: headless, or with --interactive the harness's own terminal UI
    /// in a tmux session, attached here at once when this is a terminal.
    Start {
        #[arg(long)]
        harness: String,
        /// The directory it works in, on the node it starts on; none is this one.
        #[arg(long)]
        cwd: Option<String>,
        #[arg(long)]
        name: Option<String>,
        #[arg(long)]
        interactive: bool,
        /// Print the key and leave an interactive session in the background.
        #[arg(long, requires = "interactive")]
        detach: bool,
        /// The node it starts on; none is this one.
        #[arg(long)]
        node: Option<String>,
        #[arg(long)]
        policy: Option<Policy>,
        #[arg(long)]
        model: Option<String>,
        #[arg(long)]
        effort: Option<String>,
        /// K=V, repeatable.
        #[arg(long = "env", value_name = "K=V")]
        env: Vec<String>,
    },
    /// Send one turn; with --wait return when the session reaches that activity.
    Prompt {
        key: String,
        text: String,
        #[arg(long)]
        wait: Option<Wait>,
        /// Seconds; on timeout exit 1 with the current activity.
        #[arg(long, default_value_t = 600)]
        timeout: u64,
    },
    /// ACP cancel; the session stays live.
    Cancel { key: String },
    /// End the holder.
    Stop { key: String },
    /// R15: stop the holder, keep the session to resume.
    Suspend { key: String },
    /// R15: bring a suspended session back under a new key.
    Resume { key: String },
    /// R15.5: move the session to another node; same session id, same cwd path there.
    Handoff {
        key: String,
        /// The target node, by name or id.
        #[arg(long)]
        to: String,
    },
    /// Set the display name, or clear it.
    Name {
        key: String,
        #[arg(required_unless_present = "clear", conflicts_with = "clear")]
        label: Option<String>,
        #[arg(long)]
        clear: bool,
    },
    /// Jump to the session: tmux or the /ui page.
    Open { key: String },
    /// The session's terminal in this one, R9: over the mesh when it runs elsewhere; the tmux detach key returns.
    Attach { key: String },
    /// Open the roster page in the browser.
    Ui,
    /// Answer the pending permission request with allow.
    Allow {
        key: String,
        /// The allow-always option when the harness offers it.
        #[arg(long)]
        always: bool,
    },
    /// Answer the pending permission request with deny.
    Deny {
        key: String,
        #[arg(long)]
        reason: Option<String>,
    },
    /// A child session of KEY, R5.4.
    Spawn {
        key: String,
        #[arg(long)]
        harness: String,
        #[arg(long)]
        cwd: String,
        #[arg(long)]
        name: Option<String>,
    },
    /// Mint a single use invite, R7.3.
    Invite {
        /// Minutes the invite stays valid.
        #[arg(long, default_value_t = 60)]
        ttl: u64,
    },
    /// Join the swarm through a peer with an invite.
    Join {
        address: String,
        #[arg(long)]
        token: String,
    },
    /// Sign and gossip a revocation.
    Revoke { node_id: String },
    /// Revoke this node and clear its swarm config.
    Leave,
    /// Hook declarations and the pi extension, R16.
    Integrate {
        #[command(subcommand)]
        action: Integrate,
    },
    /// Run the service.
    #[command(alias = "serve")]
    Daemon,
    /// One line per check, pass or a fix command; changes nothing.
    Doctor,
    /// Install or repair everything on this machine: a checklist screen, or `--yes` for the needed rows headless.
    Setup {
        #[arg(long)]
        yes: bool,
    },
    /// Print the version
    Version,
}

#[derive(Subcommand, Debug)]
pub enum Integrate {
    Install { target: crate::integrate::Target },
    Uninstall { target: crate::integrate::Target },
    /// Per harness: binary, version, adapter commit, hooks or extension installed and current.
    Status,
}

/// Runs every command but `daemon`. `json` is the global `--json`.
pub async fn run(command: Command, config_path: &Path, json: bool) -> Out<()> {
    let config = Config::load(config_path)?;
    // Local commands first: they never need the daemon, R14.3.
    match &command {
        Command::Version => {
            if json {
                return emit(&json!({ "version": VERSION }).to_string());
            }
            println!("rosterd {VERSION}");
            return Ok(());
        }
        Command::Doctor => return doctor(&config, json),
        Command::Setup { yes } => {
            return match crate::setup::run(config, config_path, *yes)? {
                true => Ok(()),
                false => Err(Exit::user("setup incomplete")),
            };
        }
        Command::Integrate { action } => return integrate(action, &config, json),
        _ => {}
    }
    let client = Client::new(&config);
    match command {
        Command::List { scope } => list::list(&client, &scope, json).await,
        Command::Watch { scope } => list::watch(&client, &scope, json).await,
        Command::Changes => list::changes(&client, json).await,
        Command::Attention => attention::attention(&client, json).await,
        Command::Journal { swarm, since, after, session, follow } => list::journal(&client, list::JournalArgs { swarm, since, after, session, follow }, json).await,
        Command::Status => status(&client, json).await,
        Command::Nodes => nodes(&client, json).await,
        Command::Usage { swarm, since } => usage(&client, swarm, &since, json).await,
        Command::Invite { ttl } => {
            let body = client.call(Method::POST, "/node/invite", Some(json!({ "ttl_minutes": ttl }))).await?;
            if json {
                return emit(&body);
            }
            let invite = parse::<Value>(&body)?;
            println!("{}", invite["invite"].as_str().unwrap_or_default());
            Ok(())
        }
        Command::Join { address, token } => {
            let body = client.call(Method::POST, "/node/join", Some(json!({ "peer": address, "token": token }))).await?;
            if json {
                return emit(&body);
            }
            let members = parse::<Value>(&body)?;
            println!("swarm {}", members["swarm_id"].as_str().unwrap_or("-"));
            for member in members["members"].as_array().into_iter().flatten() {
                println!("  {}  {}", member["name"].as_str().unwrap_or("-"), member["node_id"].as_str().unwrap_or("-"));
            }
            Ok(())
        }
        Command::Revoke { node_id } => {
            let body = client.call(Method::POST, "/node/revoke", Some(json!({ "node_id": node_id }))).await?;
            if json {
                return emit(&body);
            }
            println!("revoked {node_id}");
            Ok(())
        }
        Command::Leave => {
            let body = client.call(Method::POST, "/swarm/leave", None).await?;
            if json {
                return emit(&body);
            }
            println!("left the swarm");
            Ok(())
        }
        Command::Version | Command::Doctor | Command::Setup { .. } | Command::Integrate { .. } | Command::Daemon => {
            unreachable!("handled above")
        }
        other => session::run(&client, &config, other, json).await,
    }
}

/// `status`: GET /status plus the reachability of /swarm/nodes, the swarm id of /node/members
/// and the holder count of /snapshot, none of which /status carries yet.
async fn status(client: &Client, json: bool) -> Out<()> {
    let body = client.call(Method::GET, "/status", None).await?;
    if json {
        return emit(&body);
    }
    let status = parse::<Value>(&body)?;
    let text = |key: &str| status[key].as_str().map(str::to_string).unwrap_or_else(|| status[key].to_string());
    let count = |path: &[&str]| path.iter().fold(&status, |v, k| &v[*k]).as_u64().unwrap_or(0);
    let sources = status["sources"].as_array().map(|a| a.iter().filter_map(Value::as_str).collect::<Vec<_>>().join(" ")).unwrap_or_default();
    println!("node      {} ({})", text("node"), text("node_id"));
    println!("version   {}", text("version"));
    println!("socket    {}", text("socket"));
    println!("loopback  127.0.0.1:{}", text("loopback_port"));
    println!("listen    {}", text("listen"));
    println!("ui        {}", text("ui_listen"));
    println!("swarm     {}", status["swarm_id"].as_str().unwrap_or("none"));
    println!("peers     {} ({} reachable, {} unreachable)", count(&["peers", "total"]), count(&["peers", "reachable"]), count(&["peers", "unreachable"]));
    println!("sessions  {}", text("sessions"));
    println!("holders   {} ({} suspended)", count(&["holders"]), count(&["suspended"]));
    println!("sources   {sources}");
    // R7.7: only the harnesses that cannot work right now.
    for mark in status["health"].as_array().into_iter().flatten() {
        let word = |key: &str| mark[key].as_str().unwrap_or("-");
        let detail = mark["detail"].as_str().map(|d| format!(": {d}")).unwrap_or_default();
        println!("health    {} {} since {}{}{detail}", word("harness"), word("state"), word("since"), mark["until"].as_str().map(|u| format!(" until {u}")).unwrap_or_default());
    }
    Ok(())
}

async fn nodes(client: &Client, json: bool) -> Out<()> {
    let body = client.call(Method::GET, "/swarm/nodes", None).await?;
    if json {
        return emit(&body);
    }
    let nodes = parse::<Vec<NodeHealth>>(&body)?;
    print!("{}", nodes_table(&nodes));
    Ok(())
}

fn nodes_table(nodes: &[NodeHealth]) -> String {
    let rows = nodes
        .iter()
        .map(|node| {
            let state = match node.state {
                rosterd_proto::PeerState::Local => "local",
                rosterd_proto::PeerState::Reachable => "reachable",
                rosterd_proto::PeerState::Unreachable => "unreachable",
                rosterd_proto::PeerState::Incompatible => "incompatible",
            };
            let cells = vec![
                node.name.clone(),
                node.node_id.clone(),
                node.address.clone().unwrap_or_else(|| "-".into()),
                node.version.clone().unwrap_or_else(|| "-".into()),
                node.capabilities.harnesses.join(","),
                harness_health(&node.capabilities),
                state.into(),
                node.seen_ms.map(|ms| rosterd_proto::age(ms / 1000)).unwrap_or_else(|| "-".into()),
                node.uptime_ms.map(|ms| rosterd_proto::age(ms / 1000)).unwrap_or_else(|| "-".into()),
                if node.revoked { "yes" } else { "" }.into(),
            ];
            (cells, false)
        })
        .collect::<Vec<_>>();
    table(&["NAME", "NODE_ID", "ADDRESS", "VERSION", "HARNESSES", "HEALTH", "STATE", "SEEN", "UP", "REVOKED"], &rows)
}

/// `claude login_required · codex ok`, R7.7; `-` when the node names no harness.
fn harness_health(capabilities: &rosterd_proto::Capabilities) -> String {
    if capabilities.harnesses.is_empty() {
        return "-".into();
    }
    let word = |h: &str| capabilities.health.iter().find(|m| m.harness == h).map_or(rosterd_proto::HarnessState::Ok, |m| m.state);
    capabilities.harnesses.iter().map(|h| format!("{h} {}", word(h))).collect::<Vec<_>>().join(" · ")
}

/// `usage`: GET /usage or /swarm/usage, a table per node with its total as the last row, and
/// with `--swarm` a total over every node that answered.
async fn usage(client: &Client, swarm: bool, since: &str, json: bool) -> Out<()> {
    let path = format!("{}?since={}", if swarm { "/swarm/usage" } else { "/usage" }, since);
    let body = client.call(Method::GET, &path, None).await?;
    if json {
        return emit(&body);
    }
    let (nodes, unreachable) = if swarm {
        let usage = parse::<SwarmUsage>(&body)?;
        (usage.nodes, usage.unreachable)
    } else {
        (vec![parse::<NodeUsage>(&body)?], Vec::new())
    };
    let mut all = Vec::new();
    for node in &nodes {
        println!("{} ({}) since {}", node.node, node.node_id, node.since.format("%Y-%m-%d %H:%MZ"));
        println!("{}", usage_table(&node.days));
        all.extend(node.days.iter().cloned());
    }
    if swarm {
        print!("{}", table(USAGE_HEADER, &[(usage_cells("swarm total", &all), false)]));
    }
    for name in unreachable {
        println!("{name}: unreachable");
    }
    Ok(())
}

const USAGE_HEADER: &[&str] = &["DAY", "HARNESS", "MODEL", "INPUT", "OUTPUT", "CACHE_READ", "CACHE_WRITE", "COST", "SESSIONS"];

fn usd(cost: Option<f64>) -> String {
    cost.map_or("-".to_string(), |c| format!("${c:.2}"))
}

/// A total row over `days`; the cost is `-` when any bucket is unpriced, so a partial sum never
/// reads as the whole.
fn usage_cells(label: &str, days: &[DayUsage]) -> Vec<String> {
    let sum = |f: fn(&DayUsage) -> u64| days.iter().map(f).sum::<u64>().to_string();
    let cost = days.iter().map(|d| d.cost_usd).try_fold(0.0, |acc, c| c.map(|c| acc + c));
    vec![label.into(), "".into(), "".into(), sum(|d| d.input_tokens), sum(|d| d.output_tokens), sum(|d| d.cache_read_tokens), sum(|d| d.cache_write_tokens), usd(cost), "".into()]
}

fn usage_table(days: &[DayUsage]) -> String {
    let mut rows: Vec<(Vec<String>, bool)> = days
        .iter()
        .map(|d| {
            let cells = vec![
                d.day.to_string(),
                d.harness.clone(),
                d.model.clone(),
                d.input_tokens.to_string(),
                d.output_tokens.to_string(),
                d.cache_read_tokens.to_string(),
                d.cache_write_tokens.to_string(),
                usd(d.cost_usd),
                d.sessions.to_string(),
            ];
            (cells, false)
        })
        .collect();
    rows.push((usage_cells("total", days), false));
    table(USAGE_HEADER, &rows)
}

/// `doctor`, R14.3: `pass  name: detail` or `FIX   name: detail → fix`; exit 1 when any fails.
fn doctor(config: &Config, json: bool) -> Out<()> {
    let checks = crate::doctor::run(config);
    if json {
        return emit(&serde_json::to_string_pretty(&checks)?);
    }
    for check in &checks {
        match (check.ok, &check.fix) {
            (true, _) => println!("pass  {}: {}", check.name, check.detail),
            (false, Some(fix)) => println!("FIX   {}: {} → {fix}", check.name, check.detail),
            (false, None) => println!("FIX   {}: {}", check.name, check.detail),
        }
    }
    let failed = checks.iter().filter(|c| !c.ok).count();
    if failed > 0 { Err(Exit::user(format!("{failed} check(s) failed"))) } else { Ok(()) }
}

fn integrate(action: &Integrate, config: &Config, json: bool) -> Out<()> {
    match action {
        Integrate::Install { target } => crate::integrate::install(*target, config)?,
        Integrate::Uninstall { target } => crate::integrate::uninstall(*target, config)?,
        Integrate::Status => {
            let statuses = crate::integrate::status(config);
            if json {
                return emit(&serde_json::to_string_pretty(&statuses)?);
            }
            let rows = statuses
                .iter()
                .map(|s| {
                    let dash = || "-".to_string();
                    let cells = vec![
                        s.harness.clone(),
                        s.binary.clone().unwrap_or_else(dash),
                        s.version.clone().unwrap_or_else(dash),
                        s.adapter.clone().unwrap_or_else(dash),
                        s.adapter_commit.clone().unwrap_or_else(dash),
                        if s.installed { "yes" } else { "no" }.into(),
                        if s.current { "yes" } else { "no" }.into(),
                    ];
                    (cells, false)
                })
                .collect::<Vec<_>>();
            print!("{}", table(&["HARNESS", "BINARY", "VERSION", "ADAPTER", "COMMIT", "INSTALLED", "CURRENT"], &rows));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn usage_table_totals_and_refuses_a_partial_cost() {
        let day = |model: &str, cost: Option<f64>| DayUsage {
            day: chrono::NaiveDate::from_ymd_opt(2026, 9, 10).unwrap(),
            harness: "claude".into(),
            model: model.into(),
            input_tokens: 10,
            output_tokens: 20,
            cache_read_tokens: 30,
            cache_write_tokens: 40,
            cost_usd: cost,
            sessions: 1,
        };
        let total = |text: String| text.lines().last().unwrap().split_whitespace().map(str::to_string).collect::<Vec<_>>();
        let priced = usage_table(&[day("claude-opus-5", Some(1.5)), day("claude-sonnet-5", Some(0.25))]);
        assert_eq!(total(priced), ["total", "20", "40", "60", "80", "$1.75"]);
        let partial = usage_table(&[day("claude-opus-5", Some(1.5)), day("gpt-9", None)]);
        assert_eq!(total(partial), ["total", "20", "40", "60", "80", "-"]);
    }
}
