//! `list` and `watch`, R14.3: the roster as a table, or the API frame byte for byte. They read
//! /snapshot, /swarm/snapshot, /events and /swarm/events only, so they answer when the runner,
//! the bridge or the mesh is broken, R14.2.

use std::io::Write;

use chrono::Utc;
use reqwest::Method;
use rosterd_proto::{Activity, Liveness, NodeHealth, PeerState, Record, Snapshot, SwarmRecord, SwarmSnapshot, age};
use serde_json::Value;

use super::client::{emit, parse, stdout_is_tty, table};
use super::resolve::display_name;
use super::{Client, Exit, Out, Scope};

pub async fn list(client: &Client, scope: &Scope, json: bool) -> Out<()> {
    let body = client.call(Method::GET, snapshot_path(scope), None).await?;
    let body = filter(scope, &body)?;
    if json {
        return emit(&body);
    }
    print!("{}", render(scope, &body)?);
    Ok(())
}

/// The whole table again on every change (screen cleared on a tty), or one frame per line.
pub async fn watch(client: &Client, scope: &Scope, json: bool) -> Out<()> {
    let path = if scope.swarm || scope.node.is_some() { "/swarm/events" } else { "/events" };
    let tty = stdout_is_tty();
    client
        .events(path, |frame| {
            let frame = filter(scope, frame)?;
            let mut out = std::io::stdout().lock();
            if json {
                writeln!(out, "{frame}")?;
            } else {
                let text = render(scope, &frame)?;
                if tty {
                    write!(out, "\x1b[2J\x1b[H")?;
                }
                write!(out, "{text}")?;
            }
            out.flush()?;
            Ok(true)
        })
        .await
}

fn snapshot_path(scope: &Scope) -> &'static str {
    if scope.swarm || scope.node.is_some() { "/swarm/snapshot" } else { "/snapshot" }
}

/// `--node NAME`: the swarm frame narrowed to that node. The API has no frame for it, so this
/// one is re-serialized; every other scope passes through untouched.
fn filter(scope: &Scope, body: &str) -> Out<String> {
    let Some(name) = &scope.node else { return Ok(body.to_string()) };
    let mut frame: Value = parse(body)?;
    let known = frame["nodes"].as_array().is_some_and(|nodes| nodes.iter().any(|n| n["name"] == name.as_str()));
    if !known {
        return Err(Exit::user(format!("no node {name} in the swarm")));
    }
    for (list, field) in [("nodes", "name"), ("records", "node")] {
        if let Some(items) = frame[list].as_array_mut() {
            items.retain(|item| item[field] == name.as_str());
        }
    }
    Ok(frame.to_string())
}

fn render(scope: &Scope, body: &str) -> Out<String> {
    let (nodes, rows): (Vec<NodeHealth>, Vec<SwarmRecord>) = if scope.swarm || scope.node.is_some() {
        let swarm: SwarmSnapshot = parse(body)?;
        (swarm.nodes, swarm.records)
    } else {
        let snapshot: Snapshot = parse(body)?;
        let rows = snapshot.records.into_iter().map(|record| SwarmRecord { record, peer_state: PeerState::Local, peer_age_ms: 0 }).collect();
        (vec![], rows)
    };
    let mut out = String::new();
    if scope.swarm {
        out += &header(&nodes, &rows);
        out += "\n";
    }
    out += &rows_table(&rows);
    Ok(out)
}

/// Ended records stay in the snapshot for a while, R3; the table shows what is on the machine.
fn shown(row: &SwarmRecord) -> bool {
    row.record.liveness != Liveness::Ended
}

fn activity_word(record: &Record) -> &'static str {
    match (record.liveness, record.activity) {
        (Liveness::Suspended, _) => "suspended",
        (_, Activity::Active) => "active",
        (_, Activity::Idle) => "idle",
        (_, Activity::NeedsAttention) => "needs_attention",
        (_, Activity::Unknown) => "unknown",
    }
}

fn updated(record: &Record) -> String {
    match record.activity_at {
        Some(at) => age((Utc::now() - at).num_seconds().max(0) as u64),
        None => "-".into(),
    }
}

fn node_cell(row: &SwarmRecord) -> String {
    match row.peer_state {
        PeerState::Unreachable => format!("{} stale {}s", row.record.node, row.peer_age_ms / 1000),
        _ => row.record.node.clone(),
    }
}

fn rows_table(rows: &[SwarmRecord]) -> String {
    let cells = rows
        .iter()
        .filter(|row| shown(row))
        .map(|row| {
            let r = &row.record;
            let lane = match r.lane {
                rosterd_proto::Lane::Headless => "headless",
                rosterd_proto::Lane::Interactive => "interactive",
            };
            // A suspended session has no process: its session_id stands where the pid would, R14.3.
            let pid = if r.liveness == Liveness::Suspended { r.session_id.clone().unwrap_or_else(|| "-".into()) } else { r.pid.to_string() };
            let cells = vec![display_name(r), r.harness.clone(), activity_word(r).into(), updated(r), lane.into(), node_cell(row), pid];
            (cells, row.peer_state == PeerState::Unreachable)
        })
        .collect::<Vec<_>>();
    table(&["NAME", "HARNESS", "ACTIVITY", "UPDATED", "LANE", "NODE", "PID"], &cells)
}

/// `--swarm`: per node counts of the four words and the node's reachability.
fn header(nodes: &[NodeHealth], rows: &[SwarmRecord]) -> String {
    let cells = nodes
        .iter()
        .map(|node| {
            let count = |word: &str| rows.iter().filter(|row| shown(row) && row.record.node_id == node.node_id && activity_word(&row.record) == word).count().to_string();
            let state = match node.state {
                PeerState::Local => "local".to_string(),
                PeerState::Reachable => format!("reachable {}", age(node.peer_age_ms / 1000)),
                PeerState::Unreachable => format!("unreachable stale {}s", node.peer_age_ms / 1000),
            };
            let cells = vec![node.name.clone(), state, count("active"), count("idle"), count("needs_attention"), count("unknown")];
            (cells, node.state == PeerState::Unreachable)
        })
        .collect::<Vec<_>>();
    table(&["NODE", "STATE", "ACTIVE", "IDLE", "NEEDS_ATTENTION", "UNKNOWN"], &cells)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn node_scope_narrows_the_swarm_frame_and_refuses_unknown_nodes() {
        let frame = json!({
            "schema": "rosterd.swarm.v1",
            "nodes": [{ "name": "gibson" }, { "name": "wintermute" }],
            "records": [{ "node": "gibson", "pid": 1 }, { "node": "wintermute", "pid": 2 }]
        })
        .to_string();
        let scope = Scope { node: Some("wintermute".into()), swarm: false };
        let narrowed: Value = serde_json::from_str(&filter(&scope, &frame).unwrap()).unwrap();
        assert_eq!(narrowed["nodes"].as_array().unwrap().len(), 1);
        assert_eq!(narrowed["records"][0]["pid"], 2);
        assert_eq!(filter(&Scope { node: Some("nope".into()), swarm: false }, &frame).unwrap_err().message, "no node nope in the swarm");
        // Without --node the body passes through byte for byte.
        assert_eq!(filter(&Scope { node: None, swarm: true }, &frame).unwrap(), frame);
    }
}
