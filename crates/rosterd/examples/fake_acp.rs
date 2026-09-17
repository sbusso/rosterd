//! A fake ACP adapter for the runner's end to end test. Speaks newline-delimited JSON-RPC on
//! stdio like the real adapters: answers initialize and session/new, and on session/prompt
//! streams a message chunk, a subagent tool call, asks for permission, waits for the answer,
//! then ends the turn. Advertises one `effort` config option and reports its value in the
//! usage update. A prompt starting with `ask` asks a form question instead of a permission; one
//! starting with `plan` also streams a plan, a mode change and the context fill. Anything else
//! with an id gets method-not-found. With `--auth`, session/new is refused for want of a login.

use std::io::{BufRead, Write};

use serde_json::{Value, json};

fn main() {
    let stdin = std::io::stdin();
    let mut lines = stdin.lock().lines();
    let mut out = std::io::stdout().lock();
    let mut send = |v: Value| {
        writeln!(out, "{v}").unwrap();
        out.flush().unwrap();
    };
    let update = |session_id: &Value, update: Value| {
        json!({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": session_id, "update": update}})
    };
    let config_options = |effort: &str| json!([{"id": "effort", "name": "Effort", "type": "select", "currentValue": effort, "options": [{"value": "low"}, {"value": "high"}]}]);
    let mut effort = String::from("low");
    let auth = std::env::args().any(|a| a == "--auth");
    while let Some(Ok(line)) = lines.next() {
        let v: Value = match serde_json::from_str(&line) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let id = v.get("id").cloned().unwrap_or(Value::Null);
        match v.get("method").and_then(Value::as_str).unwrap_or("") {
            "initialize" => send(json!({"jsonrpc": "2.0", "id": id, "result": {
                "protocolVersion": 1, "agentCapabilities": {"loadSession": true},
                "authMethods": if auth { json!([{"id": "claude-login", "name": "Log in with Claude Code"}]) } else { json!([]) }}})),
            "session/new" if auth => send(json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32000, "message": "Authentication required"}})),
            "session/new" => {
                send(json!({"jsonrpc": "2.0", "id": id, "result": {"sessionId": "fake-1", "configOptions": config_options(&effort)}}));
                send(update(&json!("fake-1"), json!({"sessionUpdate": "available_commands_update", "availableCommands": []})));
            }
            "session/load" => {
                let sid = v["params"]["sessionId"].clone();
                send(update(&sid, json!({"sessionUpdate": "user_message_chunk", "content": {"type": "text", "text": "earlier"}})));
                send(json!({"jsonrpc": "2.0", "id": id, "result": {"configOptions": config_options(&effort)}}));
            }
            "session/set_config_option" => {
                if v["params"]["configId"] == "effort" {
                    effort = v["params"]["value"].as_str().unwrap_or("low").to_string();
                }
                send(json!({"jsonrpc": "2.0", "id": id, "result": {"configOptions": config_options(&effort)}}));
            }
            "session/prompt" => {
                let sid = v["params"]["sessionId"].clone();
                let text = v["params"]["prompt"][0]["text"].as_str().unwrap_or("").to_string();
                send(update(&sid, json!({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": "You said: "}})));
                send(update(&sid, json!({"sessionUpdate": "tool_call", "toolCallId": "call_1", "title": "Task: look", "kind": "other", "status": "pending", "rawInput": {"subagent_type": "Explore", "description": "look around"}})));
                if text.starts_with("plan") {
                    send(update(&sid, json!({"sessionUpdate": "current_mode_update", "currentModeId": "plan"})));
                    send(update(&sid, json!({"sessionUpdate": "plan", "entries": [
                        {"content": "Read the runner and the page", "priority": "high", "status": "completed"},
                        {"content": "Add the plan, mode and context fields", "priority": "high", "status": "in_progress"},
                        {"content": "Answer questions from the page", "priority": "medium", "status": "pending"},
                        {"content": "Screenshot", "priority": "low", "status": "pending"}]})));
                    send(update(&sid, json!({"sessionUpdate": "usage_update", "used": 84000, "size": 200000})));
                }
                if text.starts_with("ask") {
                    send(json!({"jsonrpc": "2.0", "id": 101, "method": "elicitation/create", "params": {
                        "sessionId": sid, "mode": "form", "message": "Which branch should the fix land on, and should I open a PR?",
                        "requestedSchema": {"type": "object", "title": "Where to land", "properties": {
                            "branch": {"type": "string", "title": "Branch"},
                            "pr": {"type": "boolean", "title": "Open a PR"},
                            "reviewers": {"type": "integer", "title": "Reviewers", "minimum": 0, "maximum": 3},
                            "target": {"type": "string", "title": "Target", "enum": ["main", "release"]}},
                            "required": ["branch"]}}}));
                } else {
                    send(json!({"jsonrpc": "2.0", "id": 100, "method": "session/request_permission", "params": {
                        "sessionId": sid,
                        "toolCall": {"toolCallId": "call_2", "title": "Bash", "rawInput": {"command": "ls"}},
                        "options": [
                            {"optionId": "allow", "name": "Allow", "kind": "allow_once"},
                            {"optionId": "reject", "name": "Reject", "kind": "reject_once"}
                        ]}}));
                }
                let answer: Value = match lines.next() {
                    Some(Ok(line)) => serde_json::from_str(&line).unwrap_or(Value::Null),
                    _ => return,
                };
                let allowed = answer["result"]["outcome"]["optionId"] == "allow" || answer["result"]["action"] == "accept";
                send(update(&sid, json!({"sessionUpdate": "tool_call_update", "toolCallId": "call_1", "status": if allowed { "completed" } else { "failed" }})));
                send(update(&sid, json!({"sessionUpdate": "agent_message_chunk", "content": {"type": "text", "text": text}})));
                send(update(&sid, json!({"sessionUpdate": "usage_update", "inputTokens": 7, "outputTokens": 2, "effort": effort})));
                send(json!({"jsonrpc": "2.0", "id": id, "result": {"stopReason": "end_turn"}}));
            }
            "session/cancel" => {}
            _ if !id.is_null() => send(json!({"jsonrpc": "2.0", "id": id, "error": {"code": -32601, "message": "method not found"}})),
            _ => {}
        }
    }
}
