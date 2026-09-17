//! Agent to agent, R5.5: one session prompts another by key, name or pid anywhere in the swarm
//! and gets the answer back. Shared by the MCP tools `session.send` and `session.find` and by
//! `POST /send`. rosterd resolves and relays; it never schedules.

use axum::http::{Method, StatusCode};
use rosterd_proto::{Record, SwarmRecord, SwarmSnapshot};
use serde_json::{Value, json};

use super::routes::{ApiError, remote_owner, session_state};
use crate::cli::resolve::{Miss, find};
use crate::node::Node;
use crate::runner::{PromptRequest, WaitUntil};

/// `session.send` waits this long for the answer unless told otherwise.
const DEFAULT_TIMEOUT_MS: u64 = 120_000;

impl Node {
    /// The target of an action, R14.1 server side: an exact session_key, a local pid, or a
    /// unique display name (the name, else the cwd basename with or without its brackets)
    /// among sessions that have not ended. Ambiguous is 409 with the candidates; none is 404.
    pub fn resolve_session(&self, target: &str) -> Result<SwarmRecord, ApiError> {
        resolve_in(&self.mesh.swarm_snapshot(), &self.identity.node_id, target)
    }
}

pub fn resolve_in(snapshot: &SwarmSnapshot, ours: &str, target: &str) -> Result<SwarmRecord, ApiError> {
    let records: Vec<Record> = snapshot.records.iter().map(|r| r.record.clone()).collect();
    let key = match find(target, ours, &records) {
        Ok(record) => record.session_key.as_str(),
        Err(Miss::None) => return Err(ApiError::new(StatusCode::NOT_FOUND, format!("no session {target}"))),
        Err(Miss::Ambiguous(candidates)) => {
            let listed: Vec<String> = candidates.iter().map(|r| format!("{} on {}", r.session_key, r.node)).collect();
            let mut refused = ApiError::new(StatusCode::CONFLICT, format!("{target} is ambiguous: {}", listed.join(", ")));
            let candidates: Vec<Value> = candidates.iter().map(|r| json!({ "session_key": r.session_key, "node": r.node, "node_id": r.node_id })).collect();
            refused.details.insert("candidates".into(), Value::Array(candidates));
            return Err(refused);
        }
    };
    Ok(snapshot.records.iter().find(|r| r.record.session_key == key).cloned().expect("resolved from these records"))
}

#[derive(Debug, serde::Deserialize)]
pub struct SendBody {
    /// A session_key, a display name or a local pid.
    pub to: String,
    pub prompt: String,
    #[serde(default)]
    pub wait_until: Option<WaitUntil>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
}

/// Resolves `to`, prompts it where it lives and answers with the recap and what is pending.
/// `from` is the calling session when known: a session may not send to itself.
pub async fn send(node: &Node, from: Option<&str>, body: SendBody) -> Result<Value, ApiError> {
    let target = node.resolve_session(&body.to)?;
    let key = target.record.session_key;
    if from == Some(key.as_str()) {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, format!("{} is the calling session", body.to)));
    }
    let request = PromptRequest {
        prompt: body.prompt,
        wait_until: Some(body.wait_until.unwrap_or(WaitUntil::Idle)),
        timeout_ms: Some(body.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS)),
    };
    let detail = json!({ "from": from, "prompt": super::routes::brief(&request.prompt) });
    let outcome = prompt_session(node, &key, request).await?;
    node.journal.action("send", Some(key.clone()), Some("local".into()), detail);
    // After a resume, R15.3, the session answers under a new key.
    let key = outcome["record"]["session_key"].as_str().unwrap_or(&key).to_string();
    let state = state_of(node, &key).await.unwrap_or(Value::Null);
    Ok(json!({
        "session_key": key,
        "node": target.record.node,
        "node_id": target.record.node_id,
        "reached": outcome["reached"],
        "stop_reason": outcome["stop_reason"],
        "recap": outcome["recap"],
        "activity": outcome["record"]["activity"],
        "pending": state["pending"],
        "questions": state["questions"],
    }))
}

/// A proxied action's answer, R7.5: the owner's body, or its `{error}` under its status.
pub async fn relay(node: &Node, owner: &str, method: Method, path: &str, body: Option<Value>) -> Result<Value, ApiError> {
    let (status, value) = node.mesh.proxy(owner, method, path, body).await?;
    if status.is_success() {
        return Ok(value);
    }
    let message = value.get("error").and_then(Value::as_str).unwrap_or("proxy failed").to_string();
    Err(ApiError::new(status, message))
}

/// One turn on the session wherever it lives; the `PromptOutcome` as JSON.
pub async fn prompt_session(node: &Node, key: &str, request: PromptRequest) -> Result<Value, ApiError> {
    if let Some(owner) = remote_owner(node, None, key, false)? {
        return relay(node, &owner, Method::POST, &format!("/sessions/{key}/prompt"), Some(serde_json::to_value(&request)?)).await;
    }
    Ok(serde_json::to_value(node.runner.prompt(key, request).await?)?)
}

/// Activity, pending requests and the last recap, never the transcript, R6. A session this
/// node does not drive (no holder) answers from its record alone.
pub async fn state_of(node: &Node, key: &str) -> Result<Value, ApiError> {
    if let Some(owner) = remote_owner(node, None, key, false)? {
        let answer = relay(node, &owner, Method::GET, &format!("/sessions/{key}"), None).await?;
        return Ok(answer.get("state").cloned().unwrap_or(answer));
    }
    let record = node.roster.get(key).ok_or_else(|| ApiError::new(StatusCode::NOT_FOUND, format!("no session {key}")))?;
    if let Some(state) = session_state(node, &record) {
        return Ok(serde_json::to_value(state)?);
    }
    Ok(json!({
        "session_key": record.session_key,
        "activity": record.activity,
        "last_recap": null,
        "pending": [],
        "permission_policy": record.permission_policy.unwrap_or(node.config.runner.default_permission_policy),
    }))
}

#[cfg(test)]
mod tests {
    use rosterd_proto::{Activity, Lane, Liveness, PeerState};

    use super::*;

    fn record(node_id: &str, pid: u32, name: Option<&str>, cwd: Option<&str>) -> SwarmRecord {
        let record = Record {
            node: node_id.into(),
            node_id: node_id.into(),
            session_key: rosterd_proto::session_key(node_id, pid, 1),
            pid,
            start_ticks: 1,
            started_at: chrono::Utc::now(),
            harness: "claude".into(),
            session_id: None,
            lane: Lane::Interactive,
            sources: vec![],
            name: name.map(Into::into),
            activity: Activity::Idle,
            activity_event: None,
            activity_at: None,
            activity_seq: 0,
            parent_session_key: None,
            cwd: cwd.map(Into::into),
            origin: None,
            tty: None,
            tmux: None,
            holder: None,
            liveness: Liveness::Live,
            ended_at: None,
            ended_reason: None,
            usage: None,
            load: None,
            mode: None,
            plan: None,
            permission_policy: None,
        };
        let peer_state = if node_id == "local" { PeerState::Local } else { PeerState::Reachable };
        SwarmRecord { record, peer_state, peer_age_ms: 0 }
    }

    #[test]
    fn resolves_key_name_basename_and_pid_and_refuses_the_rest() {
        let mut snapshot = SwarmSnapshot {
            schema: "rosterd.swarm.v1".into(),
            generated_at: chrono::Utc::now(),
            nodes: vec![],
            records: vec![
                record("local", 42, Some("builder"), None),
                record("local", 43, None, Some("/home/me/proj")),
                record("remote", 42, Some("builder"), None),
                record("remote", 44, Some("reviewer"), None),
            ],
        };
        snapshot.records[2].record.liveness = Liveness::Ended;
        let by = |target: &str| resolve_in(&snapshot, "local", target);
        assert_eq!(by("remote:44:1").unwrap().record.node, "remote", "exact key anywhere");
        assert_eq!(by("remote:42:1").unwrap().record.liveness, Liveness::Ended, "an exact key finds an ended record");
        assert_eq!(by("[proj]").unwrap().record.pid, 43, "the cwd basename in brackets");
        assert_eq!(by("proj").unwrap().record.pid, 43, "and without them");
        assert_eq!(by("43").unwrap().record.pid, 43, "a local pid");
        assert_eq!(by("builder").unwrap().record.node, "local", "an ended record never competes for a name");
        assert_eq!(by("nobody").unwrap_err().status, StatusCode::NOT_FOUND);

        snapshot.records[2].record.liveness = Liveness::Live;
        let refused = resolve_in(&snapshot, "local", "builder").unwrap_err();
        assert_eq!(refused.status, StatusCode::CONFLICT);
        assert!(refused.message.contains("local:42:1 on local") && refused.message.contains("remote:42:1 on remote"), "{}", refused.message);
        assert_eq!(refused.details["candidates"].as_array().unwrap().len(), 2);
        assert_eq!(resolve_in(&snapshot, "remote", "44").unwrap().record.name.as_deref(), Some("reviewer"), "a pid is local to the resolving node");
    }
}
