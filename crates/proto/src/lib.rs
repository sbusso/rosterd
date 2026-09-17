//! Types shared by the daemon and the holder: the roster record and snapshot of spec R3, the
//! holder's state file of R2.1, and the source precedence of R4. Nothing here does I/O.
//!
//! Consumers ignore unknown fields, so every struct that crosses a wire is `deny_unknown_fields`
//! free and every optional field defaults.

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Schema marker on every node snapshot, R3.
pub const SNAPSHOT_SCHEMA: &str = "rosterd.snapshot.v1";
/// Schema marker on the swarm union, R6.
pub const SWARM_SCHEMA: &str = "rosterd.swarm.v1";
/// Schema marker on one node's token roll-up, GET /usage.
pub const USAGE_SCHEMA: &str = "rosterd.usage.v1";
/// Schema marker on the swarm roll-up, GET /swarm/usage.
pub const SWARM_USAGE_SCHEMA: &str = "rosterd.swarm_usage.v1";
/// The holder keeps this many ACP notifications for a reconnecting daemon, R2.1.
pub const HOLDER_REPLAY_BUFFER: usize = 256;

/// Where a field's value came from, R4. Declared highest precedence first; `rank` is the order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Source {
    Launcher,
    Acp,
    Hook,
    Files,
    Scan,
}

impl Source {
    /// Lower wins. A lower-ranked source never overwrites a higher one but may fill a null.
    pub fn rank(self) -> u8 {
        self as u8
    }
    pub fn overrides(self, other: Source) -> bool {
        self.rank() <= other.rank()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Lane {
    Headless,
    Interactive,
}

/// agentd's four words, R1. `unknown` is the absence of a claim and is never claimed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum Activity {
    Active,
    Idle,
    NeedsAttention,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum Liveness {
    #[default]
    Live,
    Stale,
    /// R15.1: holder stopped on purpose, session_id kept, no process.
    Suspended,
    Ended,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EndedReason {
    Exit,
    Crash,
    Reboot,
    Killed,
    /// R15.3: the key ended because the session was resumed under a new key.
    Suspended,
    /// R15.1: the session will not come back under this key.
    Expired,
}

/// R14.3 `explain`: which source set each field of a record and when, and which claims lost.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Explain {
    pub session_key: String,
    pub fields: Vec<FieldOrigin>,
    pub rejected: Vec<RejectedClaim>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FieldOrigin {
    pub field: String,
    pub value: serde_json::Value,
    pub source: Source,
    pub at: DateTime<Utc>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RejectedClaim {
    pub source: Source,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub activity: Option<Activity>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub event: Option<String>,
    pub at: DateTime<Utc>,
    pub reason: String,
}

/// R5.3.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PermissionPolicy {
    Auto,
    #[default]
    Attention,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TmuxHandle {
    pub session: String,
    pub window_index: u32,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub window_name: Option<String>,
    pub pane_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HerdrHandle {
    pub session: String,
    pub workspace_id: String,
    pub pane_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub agent_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HolderHandle {
    /// Socket path on Unix, pipe name on Windows.
    pub socket: String,
}

/// Tokens and cost as the harness reported them over ACP, R3. Never estimated.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct Usage {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub output_tokens: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cost_usd: Option<f64>,
    /// Context window fill, ACP `usage_update` `used` / `size`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_used: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub context_size: Option<u64>,
    /// Whatever else the harness sent, kept verbatim.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub raw: Option<serde_json::Value>,
}

/// The agent's plan for the turn, ACP `plan`. Each notification replaces the whole list.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Plan {
    pub entries: Vec<PlanEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PlanEntry {
    pub content: String,
    /// `high` | `medium` | `low`.
    pub priority: String,
    /// `pending` | `in_progress` | `completed`.
    pub status: String,
}

/// What the session's process tree takes from the machine, from the last scan pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Load {
    /// Share of one core, whole percent; more than 100 across threads.
    pub cpu_pct: u16,
    pub rss_mb: u64,
}

/// One session on one node, R3.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub node: String,
    pub node_id: String,
    pub session_key: String,
    pub pid: u32,
    pub start_ticks: u64,
    pub started_at: DateTime<Utc>,
    pub harness: String,
    #[serde(default)]
    pub session_id: Option<String>,
    pub lane: Lane,
    #[serde(default)]
    pub sources: Vec<Source>,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub activity: Activity,
    #[serde(default)]
    pub activity_event: Option<String>,
    #[serde(default)]
    pub activity_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub activity_seq: u64,
    #[serde(default)]
    pub parent_session_key: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    /// What a scanned session sits under: the app that owns it or the login it came in through.
    #[serde(default)]
    pub origin: Option<String>,
    #[serde(default)]
    pub tty: Option<String>,
    #[serde(default)]
    pub tmux: Option<TmuxHandle>,
    #[serde(default)]
    pub herdr: Option<HerdrHandle>,
    #[serde(default)]
    pub holder: Option<HolderHandle>,
    #[serde(default)]
    pub liveness: Liveness,
    #[serde(default)]
    pub ended_at: Option<DateTime<Utc>>,
    #[serde(default)]
    pub ended_reason: Option<EndedReason>,
    #[serde(default)]
    pub usage: Option<Usage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub load: Option<Load>,
    /// The harness mode the session runs in (ACP `current_mode_update`), e.g. `plan` or `code`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub mode: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub plan: Option<Plan>,
    #[serde(default)]
    pub permission_policy: Option<PermissionPolicy>,
}

/// `12s`, `3m`, `2h`, `4d`: how the CLI and the tray print an age.
pub fn age(seconds: u64) -> String {
    match seconds {
        0..=59 => format!("{seconds}s"),
        60..=3599 => format!("{}m", seconds / 60),
        3600..=86_399 => format!("{}h", seconds / 3600),
        _ => format!("{}d", seconds / 86_400),
    }
}

/// `node_id:pid:start_ticks`, stable for the life of the process, R3.
pub fn session_key(node_id: &str, pid: u32, start_ticks: u64) -> String {
    format!("{node_id}:{pid}:{start_ticks}")
}

/// Whether a harness on a node can work right now, R7.7. Observed, never managed: rosterd
/// holds no credentials (R12).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HarnessState {
    Ok,
    /// The agent refused a session for want of a login on its node.
    LoginRequired,
    /// A turn ended on a rate limit or quota error; transient, starts are still accepted.
    RateLimited,
    /// The adapter cannot be spawned or exits before initialize.
    Broken,
}

impl std::fmt::Display for HarnessState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            HarnessState::Ok => "ok",
            HarnessState::LoginRequired => "login_required",
            HarnessState::RateLimited => "rate_limited",
            HarnessState::Broken => "broken",
        })
    }
}

/// One harness that is not ok on a node, R7.7. Only non-ok entries travel.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HarnessHealth {
    pub harness: String,
    pub state: HarnessState,
    pub since: DateTime<Utc>,
    /// When the mark expires on its own; none means until cleared.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<DateTime<Utc>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

/// What a node knows about the harnesses on it, R7.2.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct Capabilities {
    #[serde(default)]
    pub harnesses: Vec<String>,
    #[serde(default)]
    pub files_enabled: bool,
    #[serde(default)]
    pub herdr: bool,
    #[serde(default)]
    pub tmux: bool,
    /// Harnesses that cannot work right now, R7.7; empty means all ok.
    #[serde(default)]
    pub health: Vec<HarnessHealth>,
}

/// The complete roster of one node. Every emission is the whole table, never a diff.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Snapshot {
    pub schema: String,
    pub node: String,
    pub node_id: String,
    pub generated_at: DateTime<Utc>,
    /// Monotonic per node process; a peer keeps the highest it has seen.
    pub seq: u64,
    #[serde(default)]
    pub capabilities: Capabilities,
    /// When the node's daemon started; peers show it as uptime.
    #[serde(default)]
    pub up_since: Option<DateTime<Utc>>,
    pub records: Vec<Record>,
}

impl Snapshot {
    pub fn empty(node: &str, node_id: &str) -> Self {
        Snapshot {
            schema: SNAPSHOT_SCHEMA.to_string(),
            node: node.to_string(),
            node_id: node_id.to_string(),
            generated_at: Utc::now(),
            seq: 0,
            capabilities: Capabilities::default(),
            up_since: None,
            records: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum PeerState {
    Reachable,
    Unreachable,
    /// This node itself.
    Local,
}

/// Membership plus health, GET /swarm/nodes.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeHealth {
    pub node_id: String,
    pub name: String,
    #[serde(default)]
    pub address: Option<String>,
    pub state: PeerState,
    /// Milliseconds since the last complete snapshot arrived; 0 for the local node.
    pub peer_age_ms: u64,
    /// Milliseconds since the node was last heard from, a hello or a snapshot; none when never.
    #[serde(default)]
    pub seen_ms: Option<u64>,
    /// Milliseconds the node's daemon has been up, from its last snapshot.
    #[serde(default)]
    pub uptime_ms: Option<u64>,
    #[serde(default)]
    pub version: Option<String>,
    #[serde(default)]
    pub capabilities: Capabilities,
    #[serde(default)]
    pub revoked: bool,
}

/// A record in the swarm union, tagged with how fresh its node's snapshot is, R6.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwarmRecord {
    #[serde(flatten)]
    pub record: Record,
    pub peer_state: PeerState,
    pub peer_age_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwarmSnapshot {
    pub schema: String,
    pub generated_at: DateTime<Utc>,
    pub nodes: Vec<NodeHealth>,
    pub records: Vec<SwarmRecord>,
}

/// One (day UTC, harness, model) bucket of the token roll-up read from the harnesses' own
/// transcripts on a node. `cost_usd` is None for a model the built-in price table does not know.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DayUsage {
    pub day: chrono::NaiveDate,
    pub harness: String,
    pub model: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cache_read_tokens: u64,
    pub cache_write_tokens: u64,
    pub cost_usd: Option<f64>,
    /// Transcript files that contributed to the bucket.
    pub sessions: u32,
}

/// GET /usage: one node's roll-up since `since`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NodeUsage {
    pub schema: String,
    pub node: String,
    pub node_id: String,
    pub generated_at: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub days: Vec<DayUsage>,
}

/// GET /swarm/usage: every reachable node's roll-up; `unreachable` names the nodes that did not answer.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SwarmUsage {
    pub schema: String,
    pub generated_at: DateTime<Utc>,
    pub since: DateTime<Utc>,
    pub nodes: Vec<NodeUsage>,
    pub unreachable: Vec<String>,
}

/// The file the holder writes next to its socket, R2.1. Read back by the daemon after a restart
/// or a reboot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct HolderState {
    /// The daemon's key for this session once known; the holder starts without one.
    #[serde(default)]
    pub session_key: Option<String>,
    /// Harness native session id, set after ACP session/new or session/load.
    #[serde(default)]
    pub session_id: Option<String>,
    pub harness: String,
    pub cwd: String,
    /// The ACP adapter child of the holder.
    pub adapter_pid: u32,
    /// The holder itself.
    pub holder_pid: u32,
    pub started_at: DateTime<Utc>,
    pub socket: String,
    /// Extra launch facts the daemon wants back after a restart: name, policy, recap, env keys.
    #[serde(default)]
    pub meta: HashMap<String, serde_json::Value>,
    /// R15.1: the holder was stopped on purpose and keeps this file for a later resume.
    #[serde(default)]
    pub suspended: bool,
}

/// Holder to daemon control frames travel on the same socket as ACP JSON-RPC, distinguished by
/// the `rosterd` key so the relay stays unchanged for everything else, R2.1.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "rosterd", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)] // a handful of frames per session; boxing buys nothing
pub enum HolderFrame {
    /// First frame after connect: the buffered notifications the daemon missed.
    Replay { frames: Vec<serde_json::Value> },
    /// The daemon asks the holder to update its state file.
    SetState { state: HolderState },
    /// The adapter exited; the holder sends this then closes the socket and removes its files.
    Exited { code: Option<i32>, signal: Option<i32> },
}

/// One thing that changed between two swarm snapshots, R6: what `/swarm/changes` streams so a
/// client never diffs frames itself. Derived, never stored; `record` and `node` are the state
/// after the change.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "event", rename_all = "snake_case")]
#[allow(clippy::large_enum_variant)]
pub enum Change {
    /// A session key seen for the first time.
    SessionStarted { at: DateTime<Utc>, record: SwarmRecord },
    /// Liveness became ended; `record.ended_reason` says why.
    SessionEnded { at: DateTime<Utc>, record: SwarmRecord },
    SessionSuspended { at: DateTime<Utc>, record: SwarmRecord },
    /// An accepted claim that left the session needing a human: a permission, a question, a
    /// login. `record.activity_event` names which. Sent again for every new claim while it waits.
    Attention { at: DateTime<Utc>, record: SwarmRecord },
    /// The session no longer needs a human and has not ended.
    AttentionCleared { at: DateTime<Utc>, record: SwarmRecord },
    /// Any other accepted claim.
    Activity { at: DateTime<Utc>, record: SwarmRecord },
    Renamed { at: DateTime<Utc>, record: SwarmRecord },
    /// A node appeared, went unreachable, or came back, or the health of its harnesses
    /// changed; `node.state` and `node.capabilities.health` are the state now.
    Node { at: DateTime<Utc>, node: NodeHealth },
    NodeLeft { at: DateTime<Utc>, node: NodeHealth },
}

impl Change {
    /// The SSE event name: the serde tag.
    pub fn name(&self) -> &'static str {
        match self {
            Change::SessionStarted { .. } => "session_started",
            Change::SessionEnded { .. } => "session_ended",
            Change::SessionSuspended { .. } => "session_suspended",
            Change::Attention { .. } => "attention",
            Change::AttentionCleared { .. } => "attention_cleared",
            Change::Activity { .. } => "activity",
            Change::Renamed { .. } => "renamed",
            Change::Node { .. } => "node",
            Change::NodeLeft { .. } => "node_left",
        }
    }
}

/// The changes from `prev` to `next`, records then nodes, in `next`'s order. A record that
/// ended and was then dropped is not reported twice: a key missing from `next` is silent.
pub fn changes(prev: &SwarmSnapshot, next: &SwarmSnapshot) -> Vec<Change> {
    let at = next.generated_at;
    let mut out = Vec::new();
    let before: HashMap<&str, &SwarmRecord> = prev.records.iter().map(|r| (r.record.session_key.as_str(), r)).collect();
    for r in &next.records {
        let now = &r.record;
        let Some(old) = before.get(now.session_key.as_str()).map(|o| &o.record) else {
            if now.liveness != Liveness::Ended {
                out.push(Change::SessionStarted { at, record: r.clone() });
                if now.activity == Activity::NeedsAttention {
                    out.push(Change::Attention { at, record: r.clone() });
                }
            }
            continue;
        };
        if now.liveness != old.liveness {
            match now.liveness {
                Liveness::Ended => {
                    out.push(Change::SessionEnded { at, record: r.clone() });
                    continue;
                }
                Liveness::Suspended => out.push(Change::SessionSuspended { at, record: r.clone() }),
                Liveness::Live | Liveness::Stale => {}
            }
        }
        if now.name != old.name {
            out.push(Change::Renamed { at, record: r.clone() });
        }
        let claimed = now.activity_seq != old.activity_seq || now.activity != old.activity;
        let needs = now.activity == Activity::NeedsAttention;
        if claimed && needs {
            out.push(Change::Attention { at, record: r.clone() });
        } else if !needs && old.activity == Activity::NeedsAttention {
            out.push(Change::AttentionCleared { at, record: r.clone() });
        } else if claimed {
            out.push(Change::Activity { at, record: r.clone() });
        }
    }
    let nodes_before: HashMap<&str, &NodeHealth> = prev.nodes.iter().map(|n| (n.node_id.as_str(), n)).collect();
    for n in &next.nodes {
        if nodes_before.get(n.node_id.as_str()).is_none_or(|old| old.state != n.state || old.capabilities.health != n.capabilities.health) {
            out.push(Change::Node { at, node: n.clone() });
        }
    }
    for n in &prev.nodes {
        if !next.nodes.iter().any(|m| m.node_id == n.node_id) {
            out.push(Change::NodeLeft { at, node: n.clone() });
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn age_picks_the_largest_whole_unit() {
        assert_eq!(age(0), "0s");
        assert_eq!(age(59), "59s");
        assert_eq!(age(60), "1m");
        assert_eq!(age(3599), "59m");
        assert_eq!(age(7200), "2h");
        assert_eq!(age(90_000), "1d");
    }

    #[test]
    fn precedence_is_declaration_order() {
        assert!(Source::Launcher.overrides(Source::Acp));
        assert!(Source::Acp.overrides(Source::Hook));
        assert!(Source::Hook.overrides(Source::Files));
        assert!(Source::Files.overrides(Source::Scan));
        assert!(!Source::Scan.overrides(Source::Hook));
        assert!(Source::Hook.overrides(Source::Hook));
    }

    #[test]
    fn snapshot_round_trips_and_ignores_unknown_fields() {
        let mut snap = Snapshot::empty("gibson", "abc");
        snap.records.push(Record {
            node: "gibson".into(),
            node_id: "abc".into(),
            session_key: session_key("abc", 42, 7),
            pid: 42,
            start_ticks: 7,
            started_at: Utc::now(),
            harness: "claude".into(),
            session_id: None,
            lane: Lane::Interactive,
            sources: vec![Source::Hook, Source::Scan],
            name: None,
            activity: Activity::Active,
            activity_event: Some("tool_call".into()),
            activity_at: None,
            activity_seq: 3,
            parent_session_key: None,
            cwd: None,
            origin: None,
            tty: None,
            tmux: None,
            herdr: None,
            holder: None,
            liveness: Liveness::Live,
            ended_at: None,
            ended_reason: None,
            usage: None,
            load: None,
            mode: None,
            plan: None,
            permission_policy: None,
        });
        let mut json: serde_json::Value = serde_json::to_value(&snap).unwrap();
        json["records"][0]["future_field"] = serde_json::json!(1);
        let back: Snapshot = serde_json::from_value(json).unwrap();
        assert_eq!(back, snap);
        assert_eq!(back.records[0].session_key, "abc:42:7");
    }

    #[test]
    fn changes_between_two_swarm_frames() {
        let rec = |key: &str, activity: Activity, seq: u64, liveness: Liveness| -> SwarmRecord {
            let mut r: Record = serde_json::from_value(serde_json::json!({
                "node": "a", "node_id": "a", "session_key": key, "pid": 1, "start_ticks": 1, "started_at": Utc::now(),
                "harness": "claude", "lane": "headless", "activity": activity, "activity_seq": seq, "liveness": liveness,
            }))
            .unwrap();
            r.activity_event = Some("permission".into());
            SwarmRecord { record: r, peer_state: PeerState::Local, peer_age_ms: 0 }
        };
        let node = |id: &str, state: PeerState| NodeHealth {
            node_id: id.into(), name: id.into(), address: None, state, peer_age_ms: 0, seen_ms: None, uptime_ms: None,
            version: None, capabilities: Capabilities::default(), revoked: false,
        };
        let unhealthy = |id: &str, state: PeerState| {
            let mut n = node(id, state);
            n.capabilities.health.push(HarnessHealth { harness: "claude".into(), state: HarnessState::LoginRequired, since: Utc::now(), until: None, detail: None });
            n
        };
        let frame = |records: Vec<SwarmRecord>, nodes: Vec<NodeHealth>| SwarmSnapshot { schema: SWARM_SCHEMA.into(), generated_at: Utc::now(), nodes, records };
        let prev = frame(
            vec![rec("k1", Activity::Active, 1, Liveness::Live), rec("k2", Activity::NeedsAttention, 4, Liveness::Live), rec("k3", Activity::Idle, 2, Liveness::Live), rec("k5", Activity::Idle, 1, Liveness::Live)],
            vec![node("a", PeerState::Local), node("b", PeerState::Reachable), node("c", PeerState::Reachable), node("e", PeerState::Reachable)],
        );
        let next = frame(
            vec![
                rec("k1", Activity::NeedsAttention, 2, Liveness::Live),
                rec("k2", Activity::Active, 5, Liveness::Live),
                rec("k3", Activity::Idle, 2, Liveness::Suspended),
                rec("k4", Activity::Unknown, 0, Liveness::Live),
                rec("k5", Activity::Idle, 1, Liveness::Ended),
                rec("k6", Activity::Idle, 1, Liveness::Ended),
            ],
            // e is still reachable but claude needs a login there: a node change too.
            vec![node("a", PeerState::Local), node("b", PeerState::Unreachable), node("d", PeerState::Reachable), unhealthy("e", PeerState::Reachable)],
        );
        let names: Vec<&str> = changes(&prev, &next).iter().map(Change::name).collect();
        assert_eq!(names, ["attention", "attention_cleared", "session_suspended", "session_started", "session_ended", "node", "node", "node", "node_left"]);
        // The same claim twice is no change; a new claim while still waiting is attention again.
        assert!(changes(&next, &next).is_empty());
        let again = frame(vec![rec("k1", Activity::NeedsAttention, 3, Liveness::Live)], vec![]);
        assert_eq!(changes(&next, &again).iter().map(Change::name).collect::<Vec<_>>(), ["attention", "node_left", "node_left", "node_left", "node_left"]);
        let json = serde_json::to_value(&changes(&prev, &next)[0]).unwrap();
        assert_eq!((json["event"].as_str(), json["record"]["session_key"].as_str()), (Some("attention"), Some("k1")));
    }
}
