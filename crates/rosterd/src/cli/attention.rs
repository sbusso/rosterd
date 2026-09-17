//! `attention`, R9: the sessions waiting on a human, each with what it waits on. /swarm/snapshot
//! narrowed to needs_attention and not ended, then the session of each for the pending request.
//! Nothing waiting prints nothing, exit 0.

use chrono::{DateTime, Utc};
use reqwest::Method;
use rosterd_proto::{Activity, Liveness, PeerState, SwarmRecord, SwarmSnapshot, age};
use serde_json::{Value, json};

use super::client::{emit, parse};
use super::resolve::display_name;
use super::{Client, Out};

pub async fn attention(client: &Client, json: bool) -> Out<()> {
    let swarm: SwarmSnapshot = parse(&client.call(Method::GET, "/swarm/snapshot", None).await?)?;
    let local = swarm.nodes.iter().find(|n| n.state == PeerState::Local).map(|n| n.node_id.as_str()).unwrap_or_default();
    let mut items: Vec<(&SwarmRecord, Value)> = Vec::new();
    for row in waiting(&swarm) {
        let r = &row.record;
        let path = if r.node_id == local { format!("/sessions/{}", r.session_key) } else { format!("/swarm/{}/sessions/{}", r.node_id, r.session_key) };
        // An owner that does not answer still leaves the row: the record says it waits, the session says on what.
        let session = match client.call(Method::GET, &path, None).await {
            Ok(body) => parse(&body)?,
            Err(_) => Value::Null,
        };
        items.push((row, session));
    }
    if json {
        return emit(&Value::Array(items.iter().map(|(record, session)| json!({ "record": record, "session": session })).collect()).to_string());
    }
    let now = Utc::now();
    for (record, session) in &items {
        println!("{}", line(record, session, now));
    }
    Ok(())
}

/// The records a human is waited on for, in snapshot order.
fn waiting(swarm: &SwarmSnapshot) -> Vec<&SwarmRecord> {
    swarm.records.iter().filter(|r| r.record.activity == Activity::NeedsAttention && r.record.liveness != Liveness::Ended).collect()
}

/// `name  node  event  what  age`: what is the first pending permission (tool and summary),
/// else the first question's text, else the login methods, else `-`.
fn line(row: &SwarmRecord, session: &Value, now: DateTime<Utc>) -> String {
    let r = &row.record;
    let state = &session["state"];
    let what = if let Some(p) = state["pending"].as_array().and_then(|p| p.first()) {
        format!("{} {}", p["tool"].as_str().unwrap_or("-"), p["summary"].as_str().unwrap_or_default()).trim_end().to_string()
    } else if let Some(q) = state["questions"].as_array().and_then(|q| q.first()) {
        q["message"].as_str().unwrap_or("-").to_string()
    } else if let Some(methods) = state["login"].as_array() {
        methods.iter().filter_map(|m| m["name"].as_str()).collect::<Vec<_>>().join(", ")
    } else {
        "-".to_string()
    };
    let when = r.activity_at.map(|at| age((now - at).num_seconds().max(0) as u64)).unwrap_or_else(|| "-".into());
    format!("{}  {}  {}  {what}  {when}", display_name(r), r.node, r.activity_event.as_deref().filter(|e| !e.is_empty()).unwrap_or("-"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn waiting_rows_and_their_lines() {
        let rec = |key: &str, activity: &str, liveness: &str, event: &str| {
            json!({
                "node": "gibson", "node_id": "g", "session_key": key, "pid": 1, "start_ticks": 1, "started_at": "2026-09-16T10:00:00Z",
                "harness": "claude", "lane": "headless", "activity": activity, "activity_event": event, "activity_at": "2026-09-16T10:00:00Z",
                "liveness": liveness, "cwd": "/home/me/api", "peer_state": "local", "peer_age_ms": 0,
            })
        };
        let swarm: SwarmSnapshot = serde_json::from_value(json!({
            "schema": "rosterd.swarm.v1", "generated_at": "2026-09-16T10:00:00Z", "nodes": [],
            "records": [rec("g:1:1", "needs_attention", "live", "permission"), rec("g:2:1", "active", "live", "tool_call"), rec("g:3:1", "needs_attention", "ended", "permission"), rec("g:4:1", "needs_attention", "live", "question")],
        }))
        .unwrap();
        let rows = waiting(&swarm);
        assert_eq!(rows.iter().map(|r| r.record.session_key.as_str()).collect::<Vec<_>>(), ["g:1:1", "g:4:1"]);
        let now = "2026-09-16T10:01:30Z".parse().unwrap();
        let perm = json!({ "state": { "pending": [{ "tool": "bash", "summary": "rm -rf build" }] } });
        assert_eq!(line(rows[0], &perm, now), "[api]  gibson  permission  bash rm -rf build  1m");
        let question = json!({ "state": { "pending": [], "questions": [{ "message": "Which branch?" }] } });
        assert_eq!(line(rows[1], &question, now), "[api]  gibson  question  Which branch?  1m");
        let login = json!({ "state": { "pending": [], "login": [{ "id": "oauth", "name": "Log in with Claude" }] } });
        assert_eq!(line(rows[1], &login, now), "[api]  gibson  question  Log in with Claude  1m");
        assert_eq!(line(rows[1], &Value::Null, now), "[api]  gibson  question  -  1m", "an owner that did not answer");
    }
}
