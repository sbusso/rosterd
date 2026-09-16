//! R16.2 gating: a harness extension (pi) posts a tool call it is about to make and waits for
//! allow or deny. Under policy `attention` the answer comes from `POST /sessions/{key}/permission`
//! (the CLI's allow and deny); under `decision` from the workspace ruling through the bridge. A
//! wait past the harness's `gate_timeout_s` is a deny with reason `timeout`. The pending calls
//! use the runner's `PendingPermission` shape so one answer route serves both lanes.
//!
//! OWNER: the roster/api agent.

use std::collections::HashMap;
use std::sync::{LazyLock, Mutex};
use std::time::Duration;

use chrono::Utc;
use rosterd_proto::PermissionPolicy;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::oneshot;

use crate::bridge::{Bridge, Choice, DecisionRequest, Ruling};
use crate::runner::{PendingPermission, PermissionAnswer, PermissionOption, RunnerError};

/// `POST /gate`'s answer.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GateAnswer {
    pub outcome: Outcome,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Outcome {
    Allow,
    Deny,
}

struct Waiting {
    permission: PendingPermission,
    reply: oneshot::Sender<GateAnswer>,
}

// ponytail: one process wide registry keyed by session key; the daemon is one per machine (R2).
// Hang it on `Node` if a second daemon ever shares a process.
static PENDING: LazyLock<Mutex<HashMap<String, Vec<Waiting>>>> = LazyLock::new(Default::default);

fn allow() -> GateAnswer {
    GateAnswer { outcome: Outcome::Allow, reason: None }
}

fn deny(reason: impl Into<String>) -> GateAnswer {
    GateAnswer { outcome: Outcome::Deny, reason: Some(reason.into()) }
}

/// The three options of R16.2, with the ACP kinds so the CLI's matching works unchanged.
fn options() -> Vec<PermissionOption> {
    [("allow", "Allow", "allow_once"), ("allow_always", "Allow always", "allow_always"), ("deny", "Deny", "reject_once")]
        .iter()
        .map(|(id, name, kind)| PermissionOption { option_id: id.to_string(), name: name.to_string(), kind: kind.to_string() })
        .collect()
}

/// What `GET /sessions/{key}` shows as pending for a gated session.
pub fn pending(session_key: &str) -> Vec<PendingPermission> {
    let pending = PENDING.lock().unwrap();
    pending.get(session_key).map(|list| list.iter().map(|w| w.permission.clone()).collect()).unwrap_or_default()
}

/// Answers a waiting gate, R16.2 attention. Without a `request_id` the oldest is answered.
/// Anything but allow or allow-always denies; `reason` travels to the extension on a deny.
pub fn answer(session_key: &str, request_id: Option<&Value>, answer: PermissionAnswer, reason: Option<String>) -> Result<(), RunnerError> {
    let waiting = take(session_key, |list| match request_id {
        Some(id) => list.iter().position(|w| &w.permission.request_id == id),
        None => (!list.is_empty()).then_some(0),
    })
    .ok_or_else(|| RunnerError::NoPending(request_id.map(Value::to_string).unwrap_or_else(|| session_key.into())))?;
    let outcome = match answer {
        PermissionAnswer::Selected { option_id } if matches!(option_id.as_str(), "allow" | "allow_always") => allow(),
        PermissionAnswer::Selected { .. } => GateAnswer { outcome: Outcome::Deny, reason },
        PermissionAnswer::Cancelled => deny(reason.unwrap_or_else(|| "cancelled".into())),
    };
    let _ = waiting.reply.send(outcome);
    Ok(())
}

fn take(session_key: &str, position: impl FnOnce(&[Waiting]) -> Option<usize>) -> Option<Waiting> {
    let mut pending = PENDING.lock().unwrap();
    let list = pending.get_mut(session_key)?;
    let at = position(list)?;
    let waiting = list.remove(at);
    if list.is_empty() {
        pending.remove(session_key);
    }
    Some(waiting)
}

/// Holds a gated `tool` call on `session_key` until it is allowed or denied, or `timeout`
/// passes (deny, reason `timeout`). Under `decision` the workspace rules through `bridge`;
/// `attempt_id` is the session's.
pub async fn wait(session_key: &str, attempt_id: Option<&str>, tool: &str, summary: &str, policy: PermissionPolicy, bridge: &Bridge, timeout: Duration) -> GateAnswer {
    match policy {
        PermissionPolicy::Auto => allow(),
        PermissionPolicy::Attention => {
            let request_id = Value::String(ulid::Ulid::new().to_string().to_lowercase());
            let (reply, answered) = oneshot::channel();
            let permission = PendingPermission { request_id: request_id.clone(), tool: tool.into(), summary: summary.into(), options: options(), at: Utc::now() };
            PENDING.lock().unwrap().entry(session_key.into()).or_default().push(Waiting { permission, reply });
            match tokio::time::timeout(timeout, answered).await {
                Ok(Ok(answer)) => answer,
                Ok(Err(_)) => deny("cancelled"),
                Err(_) => {
                    take(session_key, |list| list.iter().position(|w| w.permission.request_id == request_id));
                    deny("timeout")
                }
            }
        }
        PermissionPolicy::Decision => {
            let Some(attempt_id) = attempt_id else { return deny("no workspace attempt to decide under") };
            let request = DecisionRequest {
                attempt_id: attempt_id.into(),
                title: tool.into(),
                summary: summary.into(),
                choices: options().into_iter().map(|o| Choice { id: o.option_id, label: o.name }).collect(),
                default_choice_id: None,
                context: None,
            };
            match tokio::time::timeout(timeout, bridge.request_decision(request)).await {
                Ok(Ok(Ruling::Choice { id })) if matches!(id.as_str(), "allow" | "allow_always") => allow(),
                Ok(Ok(Ruling::FreeText { text })) if matches!(text.trim(), "allow" | "allow_always") => allow(),
                Ok(Ok(Ruling::Choice { id })) => deny(id),
                Ok(Ok(Ruling::FreeText { text })) => deny(text),
                Ok(Ok(Ruling::Withdrawn)) => deny("withdrawn"),
                Ok(Ok(Ruling::Expired)) => deny("expired"),
                Ok(Err(error)) => deny(error.to_string()),
                Err(_) => deny("timeout"),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;
    use crate::roster::Roster;
    use rosterd_proto::Capabilities;
    use std::sync::Arc;

    fn bridge() -> Arc<Bridge> {
        let config = Arc::new(Config::default());
        Bridge::new(config, Roster::new("gibson", "abc", Capabilities::default())).unwrap()
    }

    #[tokio::test]
    async fn attention_waits_for_allow_or_deny_and_times_out() {
        let bridge = bridge();
        let key = "abc:1:1";
        let long = Duration::from_secs(5);
        let gate = tokio::spawn({
            let bridge = bridge.clone();
            async move { wait(key, None, "bash", "bash {\"cmd\":\"ls\"}", PermissionPolicy::Attention, &bridge, long).await }
        });
        while pending(key).is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        let shown = pending(key);
        assert_eq!((shown.len(), shown[0].tool.as_str(), shown[0].options.len()), (1, "bash", 3));
        assert_eq!(shown[0].options.iter().map(|o| o.option_id.as_str()).collect::<Vec<_>>(), ["allow", "allow_always", "deny"]);
        assert!(matches!(answer("abc:9:9", None, PermissionAnswer::Cancelled, None), Err(RunnerError::NoPending(_))));
        answer(key, Some(&shown[0].request_id), PermissionAnswer::Selected { option_id: "allow_always".into() }, None).unwrap();
        assert_eq!(gate.await.unwrap(), allow());
        assert!(pending(key).is_empty());

        let gate = tokio::spawn({
            let bridge = bridge.clone();
            async move { wait(key, None, "edit", "edit a.rs", PermissionPolicy::Attention, &bridge, long).await }
        });
        while pending(key).is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        answer(key, None, PermissionAnswer::Selected { option_id: "deny".into() }, Some("not that file".into())).unwrap();
        assert_eq!(gate.await.unwrap(), deny("not that file"));

        let timed_out = wait(key, None, "rm", "rm -rf", PermissionPolicy::Attention, &bridge, Duration::from_millis(30)).await;
        assert_eq!(timed_out, deny("timeout"));
        assert!(pending(key).is_empty(), "a timed out gate leaves nothing pending");
        assert_eq!(wait(key, None, "x", "x", PermissionPolicy::Auto, &bridge, long).await, allow());
        // Decision without a workspace: denied at once, nothing pending.
        let denied = wait(key, Some("att"), "x", "x", PermissionPolicy::Decision, &bridge, long).await;
        assert_eq!(denied.outcome, Outcome::Deny);
        assert_eq!(wait(key, None, "x", "x", PermissionPolicy::Decision, &bridge, long).await, deny("no workspace attempt to decide under"));
    }
}
