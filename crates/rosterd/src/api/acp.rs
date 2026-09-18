//! R20 ACP proxy, the daemon's half: one session's ACP traffic over a websocket at
//! `/sessions/{key}/acp`. Text frames are JSON-RPC both ways. The owner node bridges the
//! runner: the agent's `session/update`s and the requests it left pending go down, the
//! client's `session/prompt`, `session/cancel`, answers and any other `session/*` request go
//! up. Any other node relays to the owner over the mesh, like attach. Many clients may sit on
//! one session: each sees every update; the first answer to a pending request wins.
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket};
use futures::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::sync::mpsc;
use tokio_stream::wrappers::BroadcastStream;

use crate::node::Node;
use crate::runner::acp::{self, METHOD_NOT_FOUND};
use crate::runner::{PermissionAnswer, PromptRequest, QuestionAnswer, RunnerError};

const INVALID_PARAMS: i64 = -32602;
const INTERNAL: i64 = -32603;

/// The text of a prompt's content blocks, for the journal and the recap.
pub fn prompt_text(blocks: &Value) -> String {
    blocks
        .as_array()
        .into_iter()
        .flatten()
        .filter(|b| b["type"] == "text")
        .filter_map(|b| b["text"].as_str())
        .collect::<Vec<_>>()
        .join("\n")
}

fn with_session(mut v: Value, key: &str) -> Value {
    if let Some(params) = v.get_mut("params").and_then(Value::as_object_mut) {
        params.insert("sessionId".into(), json!(key));
    }
    v
}

fn error_of(error: &RunnerError) -> (i64, String) {
    match error {
        RunnerError::NotFound(_) | RunnerError::Suspended(_) | RunnerError::NoPending(_) => (INVALID_PARAMS, error.to_string()),
        other => (INTERNAL, other.to_string()),
    }
}

/// The owner side. Ends when the client goes or the session's stream closes.
pub(super) async fn serve(socket: WebSocket, node: Arc<Node>, key: String) {
    let (mut sink, mut source) = socket.split();
    let updates = match node.runner.stream(&key) {
        Ok(updates) => updates,
        Err(error) => {
            let _ = sink.send(Message::Text(json!({ "error": error.to_string() }).to_string().into())).await;
            let _ = sink.close().await;
            return;
        }
    };
    let (out, mut out_rx) = mpsc::unbounded_channel::<Value>();
    // What the agent is waiting on right now, before anything new.
    if let Ok(state) = node.runner.state(&key) {
        for raw in state.pending.iter().map(|p| &p.raw).chain(state.questions.iter().map(|q| &q.raw)) {
            if !raw.is_null() {
                let _ = out.send(raw.clone());
            }
        }
    }
    let writer = {
        let key = key.clone();
        tokio::spawn(async move {
            while let Some(v) = out_rx.recv().await {
                if sink.send(Message::Text(with_session(v, &key).to_string().into())).await.is_err() {
                    break;
                }
            }
            let _ = sink.close().await;
        })
    };
    let forward = {
        let out = out.clone();
        tokio::spawn(async move {
            let mut updates = BroadcastStream::new(updates);
            while let Some(item) = updates.next().await {
                // A lagged reader lost updates; the pending requests are re-sent on reconnect.
                if let Ok(v) = item
                    && out.send(v).is_err()
                {
                    break;
                }
            }
        })
    };
    while let Some(Ok(message)) = source.next().await {
        let text = match message {
            Message::Text(text) => text,
            Message::Close(_) => break,
            _ => continue,
        };
        let Ok(v) = serde_json::from_str::<Value>(text.as_str()) else { continue };
        if forward.is_finished() {
            break;
        }
        tokio::spawn(handle(node.clone(), key.clone(), v, out.clone()));
    }
    forward.abort();
    writer.abort();
}

/// One frame from the client.
async fn handle(node: Arc<Node>, key: String, v: Value, out: mpsc::UnboundedSender<Value>) {
    let Some(message) = acp::classify(&v) else { return };
    match message {
        acp::Message::Notification { method: "session/cancel", .. } => {
            let _ = node.runner.cancel(&key).await;
        }
        acp::Message::Notification { .. } => {}
        acp::Message::Response { id, result } => {
            // The client's answer to a request the agent left pending; a late one is dropped.
            let Ok(result) = result else { return };
            if let Some(outcome) = result.get("outcome") {
                let answer = match outcome["outcome"].as_str() {
                    Some("selected") => PermissionAnswer::Selected { option_id: outcome["optionId"].as_str().unwrap_or_default().to_string() },
                    _ => PermissionAnswer::Cancelled,
                };
                let _ = node.runner.answer_permission(&key, id, answer).await;
            } else if let Ok(answer) = serde_json::from_value::<QuestionAnswer>(result.clone()) {
                let _ = node.runner.answer_question(&key, Some(id), answer).await;
            }
        }
        acp::Message::Request { id, method, params } => {
            let answer = match method {
                "session/prompt" => {
                    let blocks = params.get("prompt").cloned().unwrap_or(Value::Null);
                    let request = PromptRequest { prompt: prompt_text(&blocks), blocks: Some(blocks), wait_until: None, timeout_ms: None };
                    node.journal.action("prompt", Some(key.clone()), Some("acp".into()), json!({ "prompt": super::routes::brief(&request.prompt) }));
                    node.runner.prompt(&key, request).await.map(|outcome| json!({ "stopReason": outcome.stop_reason.unwrap_or_else(|| "end_turn".into()) }))
                }
                "_rosterd/handshake" => node.runner.handshake_answer(&key),
                m if m.starts_with("session/") => node.runner.forward(&key, m, params.clone()).await,
                m => {
                    let _ = out.send(acp::error_response(id, METHOD_NOT_FOUND, &format!("{m} is not served per session")));
                    return;
                }
            };
            let _ = out.send(match answer {
                Ok(result) => acp::response(id, result),
                Err(error) => {
                    let (code, message) = error_of(&error);
                    acp::error_response(id, code, &message)
                }
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_text_joins_text_blocks() {
        let blocks = json!([{"type": "text", "text": "a"}, {"type": "image", "data": "x"}, {"type": "text", "text": "b"}]);
        assert_eq!(prompt_text(&blocks), "a\nb");
        assert_eq!(prompt_text(&Value::Null), "");
    }

    #[test]
    fn frames_carry_the_roster_key_as_session_id() {
        let v = json!({"jsonrpc": "2.0", "method": "session/update", "params": {"sessionId": "harness-id", "update": {}}});
        assert_eq!(with_session(v, "n:1:2")["params"]["sessionId"], "n:1:2");
        let r = json!({"jsonrpc": "2.0", "id": 1, "result": {}});
        assert_eq!(with_session(r.clone(), "k"), r);
    }
}
