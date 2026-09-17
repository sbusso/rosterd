//! ACP over newline-delimited JSON-RPC, hand-rolled on `serde_json::Value`: framing, and the
//! pure mappings of R5.2 (traffic to activity), R5.3 (permission options per policy), R5.4
//! (subagent spans) and R3 (usage as the harness reported it). No I/O here.

use rosterd_proto::{Activity, Plan, PlanEntry, Usage};
use serde_json::{Value, json};

use super::{AuthMethod, PermissionOption};
use crate::bridge::{Choice, Ruling};

pub const PROTOCOL_VERSION: u64 = 1;
pub const METHOD_NOT_FOUND: i64 = -32601;
/// Longest argument excerpt in a permission summary.
const SUMMARY_ARGS: usize = 200;

pub fn request(id: u64, method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
}

pub fn notification(method: &str, params: Value) -> Value {
    json!({"jsonrpc": "2.0", "method": method, "params": params})
}

pub fn response(id: &Value, result: Value) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "result": result})
}

pub fn error_response(id: &Value, code: i64, message: &str) -> Value {
    json!({"jsonrpc": "2.0", "id": id, "error": {"code": code, "message": message}})
}

#[derive(Debug, PartialEq)]
pub enum Message<'a> {
    Response { id: &'a Value, result: Result<&'a Value, &'a Value> },
    Notification { method: &'a str, params: &'a Value },
    Request { id: &'a Value, method: &'a str, params: &'a Value },
}

/// Which of the three JSON-RPC shapes a line is; None for anything else.
pub fn classify(v: &Value) -> Option<Message<'_>> {
    let id = v.get("id").filter(|id| !id.is_null());
    let method = v.get("method").and_then(Value::as_str);
    let params = v.get("params").unwrap_or(&Value::Null);
    match (id, method) {
        (Some(id), Some(method)) => Some(Message::Request { id, method, params }),
        (None, Some(method)) => Some(Message::Notification { method, params }),
        (Some(id), None) => {
            let result = match v.get("error") {
                Some(error) => Err(error),
                None => Ok(v.get("result").unwrap_or(&Value::Null)),
            };
            Some(Message::Response { id, result })
        }
        (None, None) => None,
    }
}

/// R4 acp: this client reads and writes nothing for the agent, so it advertises nothing.
pub fn initialize_params() -> Value {
    json!({
        "protocolVersion": PROTOCOL_VERSION,
        "clientCapabilities": {
            "fs": {"readTextFile": false, "writeTextFile": false},
            "terminal": false,
            "elicitation": {"form": {}},
        },
    })
}

/// The auth error code, the agent wants a login on its node first.
pub const AUTH_REQUIRED: i64 = -32000;

pub fn auth_required(error: &Value) -> bool {
    error.get("code").and_then(Value::as_i64) == Some(AUTH_REQUIRED)
}

/// The initialize answer's `authMethods`, id and name only.
pub fn auth_methods(init: &Value) -> Vec<AuthMethod> {
    init["authMethods"]
        .as_array()
        .map(|methods| {
            methods
                .iter()
                .filter_map(|m| {
                    let id = m.get("id")?.as_str()?.to_string();
                    let name = m.get("name").and_then(Value::as_str).unwrap_or(&id).to_string();
                    Some(AuthMethod { id, name })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The current mode, from a session/new or session/load answer or a `current_mode_update`.
pub fn mode_of(v: &Value) -> Option<String> {
    v["currentModeId"].as_str().or_else(|| v["modes"]["currentModeId"].as_str()).map(String::from)
}

/// A `plan` update's entries.
pub fn plan_of(update: &Value) -> Option<Plan> {
    if update.get("sessionUpdate").and_then(Value::as_str) != Some("plan") {
        return None;
    }
    let text = |e: &Value, k: &str| e.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    let entries = update["entries"]
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .map(|e| PlanEntry { content: text(e, "content"), priority: text(e, "priority"), status: text(e, "status") })
                .collect()
        })
        .unwrap_or_default();
    Some(Plan { entries })
}

/// The rosterd MCP server every driven harness gets, R6, in ACP's http shape.
pub fn mcp_server(url: &str, token: &str, session_key: &str) -> Value {
    json!({
        "type": "http",
        "name": "rosterd",
        "url": url,
        "headers": [
            {"name": "Authorization", "value": format!("Bearer {token}")},
            {"name": "X-Rosterd-Session", "value": session_key},
        ],
    })
}

pub fn prompt_params(session_id: &str, text: &str) -> Value {
    json!({"sessionId": session_id, "prompt": [{"type": "text", "text": text}]})
}

/// The id of the session config option that sets effort, when the session/new or session/load
/// answer advertises one (`configOptions[].id` containing "effort").
pub fn effort_option(answer: &Value) -> Option<String> {
    answer
        .get("configOptions")?
        .as_array()?
        .iter()
        .filter_map(|o| o.get("id").and_then(Value::as_str))
        .find(|id| id.to_ascii_lowercase().contains("effort"))
        .map(str::to_string)
}

/// R5.2: a session/update to the activity it claims. None for updates that say nothing about
/// activity (mode, commands, usage, replayed user messages, finished tool calls).
pub fn activity_of(update: &Value) -> Option<(Activity, &'static str)> {
    match update.get("sessionUpdate").and_then(Value::as_str) {
        Some("agent_message_chunk" | "agent_thought_chunk" | "plan") => Some((Activity::Active, "message")),
        Some("tool_call") => Some((Activity::Active, "tool_call")),
        Some("tool_call_update") if tool_call_done(update) => None,
        Some("tool_call_update") => Some((Activity::Active, "tool_call")),
        Some("user_message_chunk" | "available_commands_update" | "current_mode_update" | "usage_update") => None,
        _ => Some((Activity::Active, "message")),
    }
}

pub fn tool_call_done(update: &Value) -> bool {
    matches!(update.get("status").and_then(Value::as_str), Some("completed" | "failed" | "cancelled"))
}

/// The text of an agent_message_chunk, for the recap, R5.5.
pub fn message_text(update: &Value) -> Option<&str> {
    if update.get("sessionUpdate")?.as_str()? != "agent_message_chunk" {
        return None;
    }
    let content = update.get("content")?;
    (content.get("type")?.as_str()? == "text").then(|| content.get("text")?.as_str())?
}

/// R5.4: a tool_call that runs an in-process subagent, with a label for the span.
// ponytail: title heuristics per harness; a `kind` for subagents when ACP grows one.
pub fn subagent_label(update: &Value) -> Option<String> {
    if update.get("sessionUpdate").and_then(Value::as_str) != Some("tool_call") {
        return None;
    }
    let title = update.get("title").and_then(Value::as_str).unwrap_or("");
    let raw = update.get("rawInput");
    let subagent_type = raw.and_then(|r| r.get("subagent_type")).and_then(Value::as_str);
    let is_subagent = subagent_type.is_some()
        || title.contains("Task")
        || title.contains("Agent")
        || title.to_ascii_lowercase().contains("subagent");
    if !is_subagent {
        return None;
    }
    let description = raw.and_then(|r| r.get("description")).and_then(Value::as_str);
    Some(description.or(subagent_type).filter(|s| !s.is_empty()).unwrap_or(title).to_string())
}

pub fn permission_options(params: &Value) -> Vec<PermissionOption> {
    params
        .get("options")
        .and_then(Value::as_array)
        .map(|opts| {
            opts.iter()
                .filter_map(|o| {
                    Some(PermissionOption {
                        option_id: o.get("optionId")?.as_str()?.to_string(),
                        name: o.get("name").and_then(Value::as_str).unwrap_or("").to_string(),
                        kind: o.get("kind").and_then(Value::as_str).unwrap_or("").to_string(),
                    })
                })
                .collect()
        })
        .unwrap_or_default()
}

/// The tool title and a summary of title plus a short argument excerpt, R5.3.
pub fn tool_summary(params: &Value) -> (String, String) {
    let tool = params.get("toolCall").unwrap_or(&Value::Null);
    let title = tool.get("title").and_then(Value::as_str).filter(|t| !t.is_empty()).unwrap_or("tool").to_string();
    let args = tool.get("rawInput").map(|r| r.to_string()).unwrap_or_default();
    let args: String = args.chars().take(SUMMARY_ARGS).collect();
    let summary = if args.is_empty() || args == "null" { title.clone() } else { format!("{title} {args}") };
    (title, summary)
}

/// R5.3 auto: the first allow_once option, else allow_always, else nothing (cancelled).
pub fn auto_option(options: &[PermissionOption]) -> Option<String> {
    ["allow_once", "allow_always"]
        .iter()
        .find_map(|kind| options.iter().find(|o| o.kind == *kind))
        .map(|o| o.option_id.clone())
}

/// R5.3 decision: the three choices offered to the workspace.
pub fn decision_choices() -> Vec<Choice> {
    [("allow", "Allow"), ("deny", "Deny"), ("allow_always", "Allow always")]
        .iter()
        .map(|(id, label)| Choice { id: id.to_string(), label: label.to_string() })
        .collect()
}

/// R5.3 decision: the ACP option a ruling selects. None means cancelled.
pub fn ruling_option(ruling: &Ruling, options: &[PermissionOption]) -> Option<String> {
    let choice = match ruling {
        Ruling::Choice { id } => id.as_str(),
        Ruling::FreeText { text } => text.trim(),
        Ruling::Withdrawn | Ruling::Expired => return None,
    };
    let kinds: &[&str] = match choice {
        "allow" => &["allow_once", "allow_always"],
        "allow_always" => &["allow_always", "allow_once"],
        "deny" => &["reject_once", "reject_always"],
        _ => return None,
    };
    kinds.iter().find_map(|kind| options.iter().find(|o| o.kind == *kind)).map(|o| o.option_id.clone())
}

/// R3: tokens and cost only as the harness reported them. A usage_update, or any update that
/// carries a `usage` object. Never estimated; `raw` keeps the whole thing.
pub fn usage_of(update: &Value) -> Option<Usage> {
    let src = match update.get("sessionUpdate").and_then(Value::as_str) {
        Some("usage_update") => update,
        _ => update.get("usage")?,
    };
    let num = |keys: &[&str]| keys.iter().find_map(|k| src.get(k).and_then(Value::as_u64));
    let cost = src
        .get("cost")
        .and_then(|c| c.get("amount").and_then(Value::as_f64).or_else(|| c.as_f64()))
        .or_else(|| ["costUsd", "cost_usd"].iter().find_map(|k| src.get(k).and_then(Value::as_f64)));
    Some(Usage {
        input_tokens: num(&["inputTokens", "input_tokens"]),
        output_tokens: num(&["outputTokens", "output_tokens"]),
        cost_usd: cost,
        context_used: num(&["used"]),
        context_size: num(&["size"]),
        raw: Some(src.clone()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn opts(kinds: &[&str]) -> Vec<PermissionOption> {
        kinds
            .iter()
            .enumerate()
            .map(|(i, k)| PermissionOption { option_id: format!("o{i}"), name: k.to_string(), kind: k.to_string() })
            .collect()
    }

    #[test]
    fn framing_and_classification() {
        let req = request(1, "initialize", json!({"a": 1}));
        assert_eq!(classify(&req), Some(Message::Request { id: &json!(1), method: "initialize", params: &json!({"a": 1}) }));
        let notif = notification("session/cancel", json!({}));
        assert_eq!(classify(&notif), Some(Message::Notification { method: "session/cancel", params: &json!({}) }));
        let ok = response(&json!("x"), json!({"r": true}));
        assert_eq!(classify(&ok), Some(Message::Response { id: &json!("x"), result: Ok(&json!({"r": true})) }));
        let err = error_response(&json!(2), METHOD_NOT_FOUND, "nope");
        match classify(&err) {
            Some(Message::Response { id, result: Err(e) }) => {
                assert_eq!(id, &json!(2));
                assert_eq!(e["code"], METHOD_NOT_FOUND);
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(classify(&json!({"jsonrpc": "2.0"})), None);
        assert!(matches!(classify(&json!({"id": null, "method": "m"})), Some(Message::Notification { .. })));
    }

    #[test]
    fn r5_2_table() {
        let up = |kind: &str| json!({"sessionUpdate": kind});
        assert_eq!(activity_of(&up("agent_message_chunk")), Some((Activity::Active, "message")));
        assert_eq!(activity_of(&up("agent_thought_chunk")), Some((Activity::Active, "message")));
        assert_eq!(activity_of(&up("plan")), Some((Activity::Active, "message")));
        assert_eq!(activity_of(&up("tool_call")), Some((Activity::Active, "tool_call")));
        assert_eq!(activity_of(&json!({"sessionUpdate": "tool_call_update", "status": "in_progress"})), Some((Activity::Active, "tool_call")));
        assert_eq!(activity_of(&json!({"sessionUpdate": "tool_call_update"})), Some((Activity::Active, "tool_call")));
        assert_eq!(activity_of(&json!({"sessionUpdate": "tool_call_update", "status": "completed"})), None);
        assert_eq!(activity_of(&json!({"sessionUpdate": "tool_call_update", "status": "failed"})), None);
        for quiet in ["available_commands_update", "current_mode_update", "usage_update", "user_message_chunk"] {
            assert_eq!(activity_of(&up(quiet)), None, "{quiet}");
        }
        assert_eq!(activity_of(&up("something_new")), Some((Activity::Active, "message")));
        assert_eq!(activity_of(&json!({})), Some((Activity::Active, "message")));
    }

    #[test]
    fn message_text_only_from_text_chunks() {
        let chunk = json!({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "hi"}});
        assert_eq!(message_text(&chunk), Some("hi"));
        let thought = json!({"sessionUpdate": "agent_thought_chunk", "content": {"type": "text", "text": "hmm"}});
        assert_eq!(message_text(&thought), None);
        let image = json!({"sessionUpdate": "agent_message_chunk", "content": {"type": "image", "data": ""}});
        assert_eq!(message_text(&image), None);
    }

    #[test]
    fn permission_selection_per_policy() {
        let options = opts(&["reject_once", "allow_always", "allow_once"]);
        assert_eq!(auto_option(&options).as_deref(), Some("o2"));
        assert_eq!(auto_option(&opts(&["reject_once", "allow_always"])).as_deref(), Some("o1"));
        assert_eq!(auto_option(&opts(&["reject_once"])), None);

        let allow = Ruling::Choice { id: "allow".into() };
        assert_eq!(ruling_option(&allow, &options).as_deref(), Some("o2"));
        assert_eq!(ruling_option(&allow, &opts(&["allow_always"])).as_deref(), Some("o0"));
        assert_eq!(ruling_option(&Ruling::Choice { id: "allow_always".into() }, &options).as_deref(), Some("o1"));
        assert_eq!(ruling_option(&Ruling::Choice { id: "deny".into() }, &options).as_deref(), Some("o0"));
        assert_eq!(ruling_option(&Ruling::FreeText { text: " deny ".into() }, &options).as_deref(), Some("o0"));
        assert_eq!(ruling_option(&Ruling::Choice { id: "maybe".into() }, &options), None);
        assert_eq!(ruling_option(&Ruling::Withdrawn, &options), None);
        assert_eq!(ruling_option(&Ruling::Expired, &options), None);
        assert_eq!(decision_choices().iter().map(|c| c.id.as_str()).collect::<Vec<_>>(), ["allow", "deny", "allow_always"]);

        let params = json!({"options": [{"optionId": "a", "name": "Allow", "kind": "allow_once"}, {"bad": true}]});
        let parsed = permission_options(&params);
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].option_id, "a");
    }

    #[test]
    fn tool_summary_is_title_plus_short_args() {
        let long = "x".repeat(500);
        let params = json!({"toolCall": {"title": "Bash", "rawInput": {"command": long}}});
        let (title, summary) = tool_summary(&params);
        assert_eq!(title, "Bash");
        assert!(summary.starts_with("Bash {\"command\":\"xxx"));
        assert!(summary.len() <= "Bash ".len() + SUMMARY_ARGS);
        assert_eq!(tool_summary(&json!({})), ("tool".to_string(), "tool".to_string()));
    }

    #[test]
    fn span_detection() {
        let task = json!({"sessionUpdate": "tool_call", "title": "Task: explore", "rawInput": {"description": "Explore the repo"}});
        assert_eq!(subagent_label(&task).as_deref(), Some("Explore the repo"));
        let typed = json!({"sessionUpdate": "tool_call", "title": "run", "rawInput": {"subagent_type": "Explore"}});
        assert_eq!(subagent_label(&typed).as_deref(), Some("Explore"));
        let agent = json!({"sessionUpdate": "tool_call", "title": "Agent"});
        assert_eq!(subagent_label(&agent).as_deref(), Some("Agent"));
        let sub = json!({"sessionUpdate": "tool_call", "title": "spawn SubAgent"});
        assert_eq!(subagent_label(&sub).as_deref(), Some("spawn SubAgent"));
        let bash = json!({"sessionUpdate": "tool_call", "title": "Bash: ls", "rawInput": {"command": "ls"}});
        assert_eq!(subagent_label(&bash), None);
        let update = json!({"sessionUpdate": "tool_call_update", "title": "Task: explore"});
        assert_eq!(subagent_label(&update), None);
        assert!(tool_call_done(&json!({"status": "cancelled"})));
        assert!(!tool_call_done(&json!({"status": "in_progress"})));
    }

    #[test]
    fn usage_only_as_reported() {
        let u = usage_of(&json!({"sessionUpdate": "usage_update", "inputTokens": 10, "outputTokens": 3, "cost": {"amount": 0.5, "currency": "USD"}})).unwrap();
        assert_eq!((u.input_tokens, u.output_tokens, u.cost_usd), (Some(10), Some(3), Some(0.5)));
        assert!(u.raw.is_some());
        let bare = usage_of(&json!({"sessionUpdate": "usage_update", "used": 100, "size": 200000})).unwrap();
        assert_eq!((bare.input_tokens, bare.output_tokens, bare.cost_usd), (None, None, None));
        assert_eq!(bare.raw.unwrap()["used"], 100);
        let nested = usage_of(&json!({"sessionUpdate": "agent_message_chunk", "usage": {"input_tokens": 1}})).unwrap();
        assert_eq!(nested.input_tokens, Some(1));
        assert_eq!(usage_of(&json!({"sessionUpdate": "agent_message_chunk"})), None);
    }

    #[test]
    fn effort_option_is_found_by_id() {
        let answer = json!({"sessionId": "s", "configOptions": [{"id": "model", "type": "select"}, {"id": "reasoning_effort", "type": "select"}]});
        assert_eq!(effort_option(&answer).as_deref(), Some("reasoning_effort"));
        assert_eq!(effort_option(&json!({"sessionId": "s", "configOptions": [{"id": "model"}]})), None);
        assert_eq!(effort_option(&json!({"sessionId": "s"})), None);
    }

    #[test]
    fn mcp_server_shape() {
        let v = mcp_server("http://127.0.0.1:8790/mcp", "tok", "n:1:2");
        assert_eq!(v["type"], "http");
        assert_eq!(v["name"], "rosterd");
        assert_eq!(v["headers"][0]["value"], "Bearer tok");
        assert_eq!(v["headers"][1], json!({"name": "X-Rosterd-Session", "value": "n:1:2"}));
        assert_eq!(initialize_params()["clientCapabilities"]["fs"]["readTextFile"], false);
    }
}
