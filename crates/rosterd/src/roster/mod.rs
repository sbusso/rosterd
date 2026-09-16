//! The roster, R3 and R4: the in-memory table of sessions on this node, the precedence of
//! sources per field, and the full-snapshot output. Never blocks on another module.
//!
//! OWNER: the roster/scanner agent. Other modules code against the public API below; extend it
//! freely, change existing signatures only after grepping their callers.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use rosterd_proto::{
    Activity, Capabilities, EndedReason, Explain, FieldOrigin, HerdrHandle, HolderHandle, Lane, Liveness, Load,
    PermissionPolicy, Record, RejectedClaim, Snapshot, Source, TmuxHandle, Usage,
};
use tokio::sync::{broadcast, watch};

use crate::scanner;

/// A partial record from one source. Every `Some` is a value the source asserts; the roster
/// keeps it only when no higher ranked source already set that field, R4. Identity is
/// `session_key` or `pid` plus `start_ticks`.
#[derive(Debug, Clone, Default)]
pub struct Patch {
    pub session_key: Option<String>,
    pub pid: Option<u32>,
    pub start_ticks: Option<u64>,
    pub started_at: Option<DateTime<Utc>>,
    pub harness: Option<String>,
    pub session_id: Option<String>,
    pub lane: Option<Lane>,
    pub name: Option<String>,
    pub attempt_id: Option<String>,
    pub parent_attempt_id: Option<String>,
    pub parent_session_key: Option<String>,
    pub cwd: Option<String>,
    pub origin: Option<String>,
    pub tty: Option<String>,
    pub tmux: Option<TmuxHandle>,
    pub herdr: Option<HerdrHandle>,
    pub holder: Option<HolderHandle>,
    pub usage: Option<Usage>,
    pub permission_policy: Option<PermissionPolicy>,
}

/// What the bridge forwards to the workspace, R8. Emitted after the table changed.
#[derive(Debug, Clone)]
pub enum RosterEvent {
    /// First time a session key is seen, or an existing record gained an attempt id or a
    /// runtime handle (tmux, herdr, holder).
    Registered(Record),
    /// An accepted activity claim; `record.activity_seq` is the node's sequence for it.
    Claimed(Record),
    /// No consumer yet: the workspace has no field for a session name, R8.
    #[allow(dead_code)]
    Named(Record),
    Ended(Record),
    /// R15.1. The holder stopped on purpose; the record stays, no process exists.
    Suspended(Record),
    /// R5.4. A second live process claimed an attempt bound to `bound_to` (a session key).
    Conflict { record: Record, attempt_id: String, bound_to: String },
}

#[derive(Debug, thiserror::Error)]
pub enum RosterError {
    #[error("no session {0}")]
    NotFound(String),
    #[error("a patch needs session_key or pid and start_ticks")]
    NoIdentity,
    #[error("{0}")]
    Invalid(String),
}

/// The fields R4 precedence is tracked for. Identity fields (pid, start_ticks) have no source.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Field {
    StartedAt,
    Harness,
    SessionId,
    Lane,
    Name,
    Activity,
    AttemptId,
    ParentAttemptId,
    ParentSessionKey,
    Cwd,
    Origin,
    Tty,
    Tmux,
    Herdr,
    Holder,
}

impl Field {
    /// Declaration order, the order `explain` lists them in.
    const ALL: [Field; 15] = [
        Field::StartedAt,
        Field::Harness,
        Field::SessionId,
        Field::Lane,
        Field::Name,
        Field::Activity,
        Field::AttemptId,
        Field::ParentAttemptId,
        Field::ParentSessionKey,
        Field::Cwd,
        Field::Origin,
        Field::Tty,
        Field::Tmux,
        Field::Herdr,
        Field::Holder,
    ];

    /// The record's field name, R3.
    fn name(self) -> &'static str {
        match self {
            Field::StartedAt => "started_at",
            Field::Harness => "harness",
            Field::SessionId => "session_id",
            Field::Lane => "lane",
            Field::Name => "name",
            Field::Activity => "activity",
            Field::AttemptId => "attempt_id",
            Field::ParentAttemptId => "parent_attempt_id",
            Field::ParentSessionKey => "parent_session_key",
            Field::Cwd => "cwd",
            Field::Origin => "origin",
            Field::Tty => "tty",
            Field::Tmux => "tmux",
            Field::Herdr => "herdr",
            Field::Holder => "holder",
        }
    }

    fn value(self, record: &Record) -> serde_json::Value {
        let value = match self {
            Field::StartedAt => serde_json::to_value(record.started_at),
            Field::Harness => serde_json::to_value(&record.harness),
            Field::SessionId => serde_json::to_value(&record.session_id),
            Field::Lane => serde_json::to_value(record.lane),
            Field::Name => serde_json::to_value(&record.name),
            Field::Activity => serde_json::to_value(record.activity),
            Field::AttemptId => serde_json::to_value(&record.attempt_id),
            Field::ParentAttemptId => serde_json::to_value(&record.parent_attempt_id),
            Field::ParentSessionKey => serde_json::to_value(&record.parent_session_key),
            Field::Cwd => serde_json::to_value(&record.cwd),
            Field::Origin => serde_json::to_value(&record.origin),
            Field::Tty => serde_json::to_value(&record.tty),
            Field::Tmux => serde_json::to_value(&record.tmux),
            Field::Herdr => serde_json::to_value(&record.herdr),
            Field::Holder => serde_json::to_value(&record.holder),
        };
        value.unwrap_or(serde_json::Value::Null)
    }
}

/// R14.3 `explain` keeps this many refused claims per record, oldest dropped first.
const REJECTED_KEEP: usize = 50;

/// Which source last set each field and when, R4, plus the claims that lost. Absent from
/// `set_by` means null: any source may fill it.
#[derive(Default)]
struct Origins {
    set_by: HashMap<Field, (Source, DateTime<Utc>)>,
    rejected: VecDeque<RejectedClaim>,
}

impl Origins {
    /// R4 per field: `source` may set `field` when it is unset or `source` outranks the setter;
    /// equal rank overwrites. A refusal is recorded for `explain`.
    fn allows(&mut self, source: Source, field: Field, at: DateTime<Utc>) -> bool {
        match self.set_by.get(&field) {
            Some((prev, _)) if !source.overrides(*prev) => {
                self.reject(source, Some(field), None, None, at, format!("outranked by {}", source_name(*prev)));
                false
            }
            _ => {
                self.set_by.insert(field, (source, at));
                true
            }
        }
    }

    fn reject(&mut self, source: Source, field: Option<Field>, activity: Option<Activity>, event: Option<&str>, at: DateTime<Utc>, reason: String) {
        if self.rejected.len() == REJECTED_KEEP {
            self.rejected.pop_front();
        }
        self.rejected.push_back(RejectedClaim {
            source,
            field: field.map(|f| f.name().to_string()),
            activity,
            event: event.map(str::to_string),
            at,
            reason,
        });
    }
}

fn source_name(source: Source) -> String {
    serde_json::to_value(source).ok().and_then(|v| v.as_str().map(str::to_string)).unwrap_or_else(|| format!("{source:?}"))
}

struct Entry {
    record: Record,
    origins: Origins,
}

#[derive(Default)]
struct Table {
    entries: HashMap<String, Entry>,
    /// Nested harness processes collapsed into a root, R3: nested key to root key.
    aliases: HashMap<String, String>,
    capabilities: Capabilities,
    seq: u64,
    /// The last activity_seq per session key, live records and the file's, R8: the workspace
    /// dedupes on attempt plus seq, so a counter must survive a daemon restart.
    seqs: HashMap<String, u64>,
}

impl Table {
    /// A record's own key, else the root a nested process was collapsed into, else the key as is.
    fn resolve(&self, key: &str) -> String {
        if self.entries.contains_key(key) {
            return key.to_string();
        }
        self.aliases.get(key).cloned().unwrap_or_else(|| key.to_string())
    }
}

/// Parent chain of a pid, nearest first. Injectable so tests need no live processes.
pub type AncestorLookup = Arc<dyn Fn(u32) -> Vec<u32> + Send + Sync>;

pub struct Roster {
    node: String,
    node_id: String,
    snapshot: watch::Sender<Arc<Snapshot>>,
    events: broadcast::Sender<RosterEvent>,
    table: RwLock<Table>,
    ancestors: AncestorLookup,
    /// The seq map after each accepted claim or ending; `persist_seqs` writes it, debounced.
    seqs: watch::Sender<Arc<HashMap<String, u64>>>,
}

impl Roster {
    pub fn new(node: &str, node_id: &str, capabilities: Capabilities) -> Arc<Roster> {
        let mut snapshot = Snapshot::empty(node, node_id);
        snapshot.capabilities = capabilities.clone();
        let (snapshot, _) = watch::channel(Arc::new(snapshot));
        let (events, _) = broadcast::channel(1024);
        let (seqs, _) = watch::channel(Arc::new(HashMap::new()));
        Arc::new(Roster {
            node: node.into(),
            node_id: node_id.into(),
            snapshot,
            events,
            table: RwLock::new(Table { capabilities, ..Table::default() }),
            ancestors: Arc::new(scanner::ancestors),
            seqs,
        })
    }

    /// Loads `{session_key: seq}` from `path` (missing file is empty) and from then on writes it
    /// back, debounced 250 ms, after every accepted claim and every ending. A record created for
    /// a key in the file continues its counter, so a session reattached after a restart never
    /// reuses a seq the workspace already stored. Keys of dead processes are dropped at load.
    /// Call inside the tokio runtime.
    pub fn persist_seqs(&self, path: PathBuf) {
        let loaded: HashMap<String, u64> = std::fs::read(&path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .unwrap_or_default();
        let mut table = self.table.write().unwrap();
        for (key, seq) in loaded {
            let alive = parse_key(&key).is_some_and(|(pid, ticks)| scanner::is_alive(pid, ticks));
            if alive {
                if let Some(entry) = table.entries.get_mut(&key) {
                    entry.record.activity_seq = entry.record.activity_seq.max(seq);
                }
                table.seqs.entry(key).and_modify(|s| *s = (*s).max(seq)).or_insert(seq);
            }
        }
        let mut rx = self.seqs.subscribe();
        tokio::spawn(async move {
            while rx.changed().await.is_ok() {
                tokio::time::sleep(Duration::from_millis(250)).await;
                let seqs = rx.borrow_and_update().clone();
                let tmp = path.with_extension("json.tmp");
                let written = serde_json::to_vec(&*seqs)
                    .map_err(anyhow::Error::from)
                    .and_then(|bytes| crate::config::write_private(&tmp, &bytes))
                    .and_then(|_| std::fs::rename(&tmp, &path).map_err(Into::into));
                if let Err(error) = written {
                    tracing::warn!(path = %path.display(), %error, "seqs not persisted");
                }
            }
        });
    }

    /// Replaces the process tree lookup used by the collapse rule, R3. For tests.
    #[cfg(test)]
    pub fn with_ancestor_lookup(self: Arc<Self>, lookup: AncestorLookup) -> Arc<Roster> {
        let Roster { node, node_id, snapshot, events, table, seqs, .. } =
            Arc::try_unwrap(self).unwrap_or_else(|_| panic!("roster already shared"));
        Arc::new(Roster { node, node_id, snapshot, events, table, ancestors: lookup, seqs })
    }

    pub fn key_of(&self, pid: u32, start_ticks: u64) -> String {
        rosterd_proto::session_key(&self.node_id, pid, start_ticks)
    }

    /// The complete roster as of now.
    pub fn snapshot(&self) -> Arc<Snapshot> {
        self.snapshot.borrow().clone()
    }

    /// Every change publishes the whole table.
    pub fn watch(&self) -> watch::Receiver<Arc<Snapshot>> {
        self.snapshot.subscribe()
    }

    pub fn events(&self) -> broadcast::Receiver<RosterEvent> {
        self.events.subscribe()
    }

    pub fn get(&self, session_key: &str) -> Option<Record> {
        self.snapshot().records.iter().find(|r| r.session_key == session_key).cloned()
    }

    /// The record bound to an attempt on this node, R5.4: live or suspended (R15.1, the attempt
    /// is still bound), never ended.
    pub fn live_by_attempt(&self, attempt_id: &str) -> Option<Record> {
        self.snapshot()
            .records
            .iter()
            .find(|r| r.attempt_id.as_deref() == Some(attempt_id) && r.liveness != Liveness::Ended)
            .cloned()
    }

    /// The session key a process reports under: its own, or the root it was collapsed into,
    /// R3. None when the process is not in the roster.
    pub fn resolve_key(&self, pid: u32, start_ticks: u64) -> Option<String> {
        let key = self.key_of(pid, start_ticks);
        let table = self.table.read().unwrap();
        let resolved = table.resolve(&key);
        table.entries.contains_key(&resolved).then_some(resolved)
    }

    pub fn set_capabilities(&self, capabilities: Capabilities) {
        let mut table = self.table.write().unwrap();
        if table.capabilities != capabilities {
            table.capabilities = capabilities;
            self.publish(&mut table);
        }
    }

    /// Registers or updates a session from one source, applying R4 precedence per field and
    /// adding the source to `sources`. Returns the record after the change.
    pub fn apply(&self, source: Source, mut patch: Patch) -> Result<Record, RosterError> {
        let mut table = self.table.write().unwrap();
        let now = Utc::now();

        // Identity, R3: a session key, else pid plus start ticks.
        let (key, pid, start_ticks) = match patch.session_key.take() {
            Some(key) => {
                let key = table.resolve(&key);
                let (pid, ticks) = match table.entries.get(&key) {
                    Some(e) => (e.record.pid, e.record.start_ticks),
                    None => parse_key(&key).or_else(|| patch.pid.zip(patch.start_ticks)).ok_or(RosterError::NoIdentity)?,
                };
                (key, pid, ticks)
            }
            None => {
                let pid = patch.pid.ok_or(RosterError::NoIdentity)?;
                let ticks = patch
                    .start_ticks
                    .or_else(|| scanner::start_ticks(pid))
                    .ok_or_else(|| RosterError::Invalid(format!("pid {pid} has no start time; is it alive?")))?;
                (self.key_of(pid, ticks), pid, ticks)
            }
        };

        // A process the roster has not seen: is it nested in a session it knows? R3.
        let mut key = key;
        let mut parent: Option<(String, Option<String>)> = None;
        let collapsible = matches!(source, Source::Hook | Source::Scan | Source::Files);
        if !table.entries.contains_key(&key) && collapsible {
            let root = (self.ancestors)(pid).into_iter().find_map(|a| {
                table.entries.values().find(|e| e.record.pid == a && e.record.liveness != Liveness::Ended)
            });
            if let Some(root) = root {
                let root_key = root.record.session_key.clone();
                let root_attempt = root.record.attempt_id.clone();
                let spawned = patch.attempt_id.is_some() && patch.attempt_id != root_attempt;
                if spawned {
                    // (a) a spawned child: its own record, R3 and R5.4.
                    parent = Some((root_key, root_attempt));
                } else {
                    // (b) a nested process of the same session, collapsed into the root.
                    table.aliases.insert(key.clone(), root_key.clone());
                    key = root_key;
                    patch.started_at = None;
                }
            }
        }

        // R5.4, node local: an attempt already bound to another live or suspended record is not
        // bound twice. An ended record frees its attempt (resume, R2.2 and R15.3).
        let mut conflict = None;
        if let Some(attempt) = patch.attempt_id.as_deref() {
            let bound = table.entries.values().find(|e| {
                e.record.session_key != key
                    && e.record.attempt_id.as_deref() == Some(attempt)
                    && e.record.liveness != Liveness::Ended
            });
            if let Some(bound) = bound {
                conflict = Some((patch.attempt_id.take().unwrap(), bound.record.session_key.clone()));
            }
        }

        let created = !table.entries.contains_key(&key);
        if created {
            table.aliases.remove(&key);
        }
        let stored_seq = table.seqs.get(&key).copied().unwrap_or(0);
        let entry = table.entries.entry(key.clone()).or_insert_with(|| Entry {
            record: Record {
                node: self.node.clone(),
                node_id: self.node_id.clone(),
                session_key: key.clone(),
                pid,
                start_ticks,
                started_at: now,
                harness: "unknown".into(),
                session_id: None,
                lane: Lane::Interactive,
                sources: Vec::new(),
                name: None,
                activity: Activity::Unknown,
                activity_event: None,
                activity_at: None,
                activity_seq: stored_seq,
                attempt_id: None,
                parent_attempt_id: None,
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
                conflict: false,
                permission_policy: None,
            },
            origins: Origins::default(),
        });
        if let Some((root_key, root_attempt)) = parent {
            patch.parent_session_key = patch.parent_session_key.or(Some(root_key));
            patch.parent_attempt_id = patch.parent_attempt_id.or(root_attempt);
        }

        let Entry { record, origins } = entry;
        if let Some((attempt_id, bound_to)) = &conflict {
            let reason = format!("attempt {attempt_id} is bound to {bound_to} (R5.4)");
            origins.reject(source, Some(Field::AttemptId), None, None, now, reason);
        }
        let had_attempt = record.attempt_id.is_some();
        let had_handle = record.tmux.is_some() || record.herdr.is_some() || record.holder.is_some();
        let mut changed = created;
        changed |= fill_plain(origins, source, now, Field::StartedAt, &mut record.started_at, patch.started_at);
        changed |= fill_plain(origins, source, now, Field::Harness, &mut record.harness, patch.harness);
        changed |= fill_plain(origins, source, now, Field::Lane, &mut record.lane, patch.lane);
        changed |= fill(origins, source, now, Field::SessionId, &mut record.session_id, patch.session_id);
        changed |= fill(origins, source, now, Field::Name, &mut record.name, patch.name);
        changed |= fill(origins, source, now, Field::AttemptId, &mut record.attempt_id, patch.attempt_id);
        changed |= fill(origins, source, now, Field::ParentAttemptId, &mut record.parent_attempt_id, patch.parent_attempt_id);
        changed |= fill(origins, source, now, Field::ParentSessionKey, &mut record.parent_session_key, patch.parent_session_key);
        changed |= fill(origins, source, now, Field::Cwd, &mut record.cwd, patch.cwd);
        changed |= fill(origins, source, now, Field::Origin, &mut record.origin, patch.origin);
        changed |= fill(origins, source, now, Field::Tty, &mut record.tty, patch.tty);
        changed |= fill(origins, source, now, Field::Tmux, &mut record.tmux, patch.tmux);
        changed |= fill(origins, source, now, Field::Herdr, &mut record.herdr, patch.herdr);
        changed |= fill(origins, source, now, Field::Holder, &mut record.holder, patch.holder);
        if let Some(usage) = patch.usage {
            changed |= record.usage.as_ref() != Some(&usage);
            record.usage = Some(usage);
        }
        if let Some(policy) = patch.permission_policy {
            changed |= record.permission_policy != Some(policy);
            record.permission_policy = Some(policy);
        }
        if conflict.is_some() && !record.conflict {
            record.conflict = true;
            changed = true;
        }
        changed |= add_source(record, source);
        let record = record.clone();
        if !changed {
            return Ok(record);
        }

        let registered = created
            || (!had_attempt && record.attempt_id.is_some())
            || (!had_handle && (record.tmux.is_some() || record.herdr.is_some() || record.holder.is_some()));
        self.publish(&mut table);
        if registered {
            self.emit(RosterEvent::Registered(record.clone()));
        }
        if let Some((attempt_id, bound_to)) = conflict {
            self.emit(RosterEvent::Conflict { record: record.clone(), attempt_id, bound_to });
        }
        Ok(record)
    }

    /// An activity claim, R5.2 and R4. Bumps `activity_seq` and emits `Claimed` when the source
    /// may set activity on this record (acp beats hook on a headless session). A claim the
    /// source may not make returns the record unchanged.
    pub fn claim(
        &self,
        source: Source,
        session_key: &str,
        activity: Activity,
        event: &str,
        at: DateTime<Utc>,
    ) -> Result<Record, RosterError> {
        if activity == Activity::Unknown {
            return Err(RosterError::Invalid("unknown is the absence of a claim and is never claimed".into()));
        }
        let mut table = self.table.write().unwrap();
        let key = table.resolve(session_key);
        let Table { entries, seqs, .. } = &mut *table;
        let entry = entries.get_mut(&key).ok_or_else(|| RosterError::NotFound(session_key.into()))?;
        let Entry { record, origins } = entry;
        // Refused: an ended record, a claim older than the accepted one, an outranked source.
        let refused = if record.liveness == Liveness::Ended {
            Some("record has ended".to_string())
        } else if record.activity_at.is_some_and(|current| at < current) {
            Some("older than the accepted claim".to_string())
        } else {
            origins.set_by.get(&Field::Activity).filter(|(prev, _)| !source.overrides(*prev)).map(|(prev, _)| format!("outranked by {}", source_name(*prev)))
        };
        if let Some(reason) = refused {
            origins.reject(source, Some(Field::Activity), Some(activity), Some(event), at, reason);
            return Ok(record.clone());
        }
        origins.set_by.insert(Field::Activity, (source, at));
        record.activity = activity;
        record.activity_event = Some(event.to_string());
        record.activity_at = Some(at);
        record.activity_seq += 1;
        seqs.insert(key, record.activity_seq);
        add_source(record, source);
        let record = record.clone();
        self.publish(&mut table);
        self.publish_seqs(&table);
        self.emit(RosterEvent::Claimed(record.clone()));
        Ok(record)
    }

    /// Binds a display name to the session key, R3, under R4 precedence. None clears it and
    /// lets any source fill it again.
    pub fn set_name(&self, session_key: &str, name: Option<String>, source: Source) -> Result<Record, RosterError> {
        let mut table = self.table.write().unwrap();
        let key = table.resolve(session_key);
        let entry = table.entries.get_mut(&key).ok_or_else(|| RosterError::NotFound(session_key.into()))?;
        if !entry.origins.allows(source, Field::Name, Utc::now()) {
            return Ok(entry.record.clone());
        }
        if name.is_none() {
            entry.origins.set_by.remove(&Field::Name);
        }
        let mut changed = add_source(&mut entry.record, source);
        if entry.record.name != name {
            entry.record.name = name;
            changed = true;
        }
        let record = entry.record.clone();
        if changed {
            self.publish(&mut table);
            self.emit(RosterEvent::Named(record.clone()));
        }
        Ok(record)
    }

    /// Usage comes from ACP only, R3, so it has no precedence to check.
    pub fn set_usage(&self, session_key: &str, usage: Usage) -> Result<Record, RosterError> {
        self.update(session_key, |r| r.usage = Some(usage))
    }

    /// One scan pass's process load for every live session, published once when any changed.
    pub fn set_load(&self, loads: &HashMap<String, Load>) {
        let mut table = self.table.write().unwrap();
        let mut changed = false;
        for entry in table.entries.values_mut() {
            let load = loads.get(&entry.record.session_key).copied();
            changed |= entry.record.load != load;
            entry.record.load = load;
        }
        if changed {
            self.publish(&mut table);
        }
    }

    /// Set by the runner alone, R5.3.
    pub fn set_policy(&self, session_key: &str, policy: PermissionPolicy) -> Result<Record, RosterError> {
        self.update(session_key, |r| r.permission_policy = Some(policy))
    }

    /// Marks a session ended. Ended records stay in the snapshot for a while so a client sees
    /// the ending, then are dropped by the scanner's sweep. Ending twice keeps the first reason.
    /// A suspended record has no process (R15.1), so only the R15 reasons end it: `Suspended`
    /// (resumed under a new key) or `Expired`; a process ending is a no-op on it.
    pub fn end(&self, session_key: &str, reason: EndedReason, at: DateTime<Utc>) -> Result<Record, RosterError> {
        let mut table = self.table.write().unwrap();
        let key = table.resolve(session_key);
        let entry = table.entries.get_mut(&key).ok_or_else(|| RosterError::NotFound(session_key.into()))?;
        let no_process = entry.record.liveness == Liveness::Suspended && !matches!(reason, EndedReason::Suspended | EndedReason::Expired);
        if entry.record.liveness == Liveness::Ended || no_process {
            return Ok(entry.record.clone());
        }
        entry.record.liveness = Liveness::Ended;
        entry.record.ended_at = Some(at);
        entry.record.ended_reason = Some(reason);
        let record = entry.record.clone();
        table.seqs.remove(&key);
        self.publish(&mut table);
        self.publish_seqs(&table);
        self.emit(RosterEvent::Ended(record.clone()));
        Ok(record)
    }

    /// R15.1: the holder stopped on purpose. The record stays with everything needed to resume;
    /// only the holder handle goes, since no process exists. Suspending twice is a no-op.
    pub fn suspend(&self, session_key: &str, at: DateTime<Utc>) -> Result<Record, RosterError> {
        let mut table = self.table.write().unwrap();
        let key = table.resolve(session_key);
        let entry = table.entries.get_mut(&key).ok_or_else(|| RosterError::NotFound(session_key.into()))?;
        if entry.record.liveness == Liveness::Ended {
            return Err(RosterError::Invalid(format!("session {session_key} has ended")));
        }
        if entry.record.liveness == Liveness::Suspended {
            return Ok(entry.record.clone());
        }
        entry.record.liveness = Liveness::Suspended;
        entry.record.holder = None;
        entry.record.activity_at.get_or_insert(at);
        let record = entry.record.clone();
        self.publish(&mut table);
        self.emit(RosterEvent::Suspended(record.clone()));
        Ok(record)
    }

    /// R14.3 `explain`: every field a source set, with the value, the source and when, in R3
    /// order, then the refused claims oldest first.
    pub fn explain(&self, session_key: &str) -> Result<Explain, RosterError> {
        let table = self.table.read().unwrap();
        let key = table.resolve(session_key);
        let entry = table.entries.get(&key).ok_or_else(|| RosterError::NotFound(session_key.into()))?;
        let fields = Field::ALL
            .iter()
            .filter_map(|field| {
                let (source, at) = *entry.origins.set_by.get(field)?;
                Some(FieldOrigin { field: field.name().to_string(), value: field.value(&entry.record), source, at })
            })
            .collect();
        Ok(Explain { session_key: key, fields, rejected: entry.origins.rejected.iter().cloned().collect() })
    }

    /// Drops ended records older than `older_than`. A suspended record is never dropped: no
    /// process is expected, R15.1.
    pub fn sweep(&self, older_than: DateTime<Utc>) {
        let mut table = self.table.write().unwrap();
        let before = table.entries.len();
        table
            .entries
            .retain(|_, e| !(e.record.liveness == Liveness::Ended && e.record.ended_at.is_some_and(|t| t < older_than)));
        if table.entries.len() != before {
            let Table { entries, aliases, .. } = &mut *table;
            aliases.retain(|_, root| entries.contains_key(root));
            self.publish(&mut table);
        }
    }

    fn update(&self, session_key: &str, f: impl FnOnce(&mut Record)) -> Result<Record, RosterError> {
        let mut table = self.table.write().unwrap();
        let key = table.resolve(session_key);
        let entry = table.entries.get_mut(&key).ok_or_else(|| RosterError::NotFound(session_key.into()))?;
        let before = entry.record.clone();
        f(&mut entry.record);
        let record = entry.record.clone();
        if record != before {
            self.publish(&mut table);
        }
        Ok(record)
    }

    /// The whole table, sorted, as a new snapshot on the watch channel.
    fn publish(&self, table: &mut Table) {
        table.seq += 1;
        let mut records: Vec<Record> = table.entries.values().map(|e| e.record.clone()).collect();
        records.sort_by(|a, b| a.started_at.cmp(&b.started_at).then_with(|| a.session_key.cmp(&b.session_key)));
        let snapshot = Snapshot {
            schema: rosterd_proto::SNAPSHOT_SCHEMA.to_string(),
            node: self.node.clone(),
            node_id: self.node_id.clone(),
            generated_at: Utc::now(),
            seq: table.seq,
            capabilities: table.capabilities.clone(),
            records,
        };
        self.snapshot.send_replace(Arc::new(snapshot));
    }

    fn publish_seqs(&self, table: &Table) {
        self.seqs.send_replace(Arc::new(table.seqs.clone()));
    }

    fn emit(&self, event: RosterEvent) {
        // No subscriber yet is not an error.
        let _ = self.events.send(event);
    }
}

/// R4 per field: a value is stored when the field is null or the source overrides the one that
/// set it. Equal rank overwrites. Returns whether the value changed.
fn fill<T: PartialEq>(origins: &mut Origins, source: Source, at: DateTime<Utc>, field: Field, slot: &mut Option<T>, value: Option<T>) -> bool {
    let Some(value) = value else { return false };
    if !origins.allows(source, field, at) || slot.as_ref() == Some(&value) {
        return false;
    }
    *slot = Some(value);
    true
}

/// `fill` for a field the record cannot leave null: unset means no source has spoken for it.
fn fill_plain<T: PartialEq>(origins: &mut Origins, source: Source, at: DateTime<Utc>, field: Field, slot: &mut T, value: Option<T>) -> bool {
    let Some(value) = value else { return false };
    if !origins.allows(source, field, at) || *slot == value {
        return false;
    }
    *slot = value;
    true
}

/// Every source that touched a record is listed, once, in rank order, R3.
fn add_source(record: &mut Record, source: Source) -> bool {
    if record.sources.contains(&source) {
        return false;
    }
    record.sources.push(source);
    record.sources.sort();
    true
}

/// `node_id:pid:start_ticks`, R3.
fn parse_key(key: &str) -> Option<(u32, u64)> {
    let mut parts = key.rsplitn(3, ':');
    let ticks = parts.next()?.parse().ok()?;
    let pid = parts.next()?.parse().ok()?;
    Some((pid, ticks))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roster(tree: &[(u32, &[u32])]) -> Arc<Roster> {
        let tree: HashMap<u32, Vec<u32>> = tree.iter().map(|(p, a)| (*p, a.to_vec())).collect();
        Roster::new("gibson", "abc", Capabilities::default())
            .with_ancestor_lookup(Arc::new(move |pid| tree.get(&pid).cloned().unwrap_or_default()))
    }

    fn patch(pid: u32) -> Patch {
        Patch { pid: Some(pid), start_ticks: Some(7), harness: Some("claude".into()), ..Patch::default() }
    }

    fn cwd(pid: u32, cwd: &str) -> Patch {
        Patch { cwd: Some(cwd.into()), ..patch(pid) }
    }

    #[test]
    fn precedence_per_field_and_sources_in_rank_order() {
        let r = roster(&[]);
        assert_eq!(r.apply(Source::Scan, cwd(1, "a")).unwrap().cwd.as_deref(), Some("a"));
        assert_eq!(r.apply(Source::Hook, cwd(1, "b")).unwrap().cwd.as_deref(), Some("b"));
        assert_eq!(r.apply(Source::Scan, cwd(1, "c")).unwrap().cwd.as_deref(), Some("b"));
        assert_eq!(r.apply(Source::Hook, cwd(1, "b2")).unwrap().cwd.as_deref(), Some("b2"), "equal rank overwrites");
        assert_eq!(r.apply(Source::Launcher, cwd(1, "d")).unwrap().cwd.as_deref(), Some("d"));
        let rec = r.apply(Source::Acp, cwd(1, "e")).unwrap();
        assert_eq!(rec.cwd.as_deref(), Some("d"));
        assert_eq!(rec.sources, vec![Source::Launcher, Source::Acp, Source::Hook, Source::Scan]);
        // Another field is independent: hook owns cwd, scan still fills the null tty.
        let rec = r.apply(Source::Scan, Patch { tty: Some("/dev/ttys1".into()), ..patch(1) }).unwrap();
        assert_eq!(rec.tty.as_deref(), Some("/dev/ttys1"));
        assert_eq!(rec.cwd.as_deref(), Some("d"));
    }

    #[test]
    fn a_lower_source_fills_a_null_and_a_higher_one_then_overwrites() {
        let r = roster(&[]);
        let rec = r.apply(Source::Hook, patch(2)).unwrap();
        assert_eq!(rec.cwd, None);
        assert_eq!(r.apply(Source::Scan, cwd(2, "/x")).unwrap().cwd.as_deref(), Some("/x"));
        assert_eq!(r.apply(Source::Hook, cwd(2, "/y")).unwrap().cwd.as_deref(), Some("/y"));
        // Harness is not optional on the record but follows the same rule.
        let rec = r.apply(Source::Scan, Patch { harness: Some("codex".into()), ..patch(2) }).unwrap();
        assert_eq!(rec.harness, "claude");
    }

    #[test]
    fn claims_bump_seq_and_are_gated_by_source() {
        let r = roster(&[]);
        let key = r.apply(Source::Hook, patch(3)).unwrap().session_key;
        let now = Utc::now();
        let rec = r.claim(Source::Hook, &key, Activity::Active, "prompt", now).unwrap();
        assert_eq!((rec.activity, rec.activity_seq, rec.activity_event.as_deref()), (Activity::Active, 1, Some("prompt")));
        assert_eq!(r.claim(Source::Hook, &key, Activity::Idle, "turn_end", now).unwrap().activity_seq, 2);
        let rec = r.claim(Source::Acp, &key, Activity::Active, "tool_call", now).unwrap();
        assert_eq!((rec.activity, rec.activity_seq), (Activity::Active, 3));
        // Hook may no longer move activity once acp has claimed, R5.1.
        let rec = r.claim(Source::Hook, &key, Activity::Idle, "turn_end", now).unwrap();
        assert_eq!((rec.activity, rec.activity_seq), (Activity::Active, 3));
        assert_eq!(r.claim(Source::Acp, &key, Activity::Idle, "turn_end", now).unwrap().activity_seq, 4);
        assert!(r.claim(Source::Acp, &key, Activity::Unknown, "x", now).is_err());
        assert!(matches!(r.claim(Source::Acp, "abc:9:9", Activity::Idle, "x", now), Err(RosterError::NotFound(_))));
        assert_eq!(r.get(&key).unwrap().sources, vec![Source::Acp, Source::Hook]);
    }

    #[test]
    fn nested_processes_collapse_and_spawned_children_are_records() {
        let r = roster(&[(200, &[100, 1]), (300, &[100, 1]), (400, &[100, 1]), (500, &[300, 100, 1])]);
        let root = r.apply(Source::Hook, Patch { attempt_id: Some("A".into()), ..patch(100) }).unwrap();
        // (b) same session: no attempt id, collapsed into the root; its cwd fills the root's null.
        let rec = r.apply(Source::Scan, cwd(200, "/root")).unwrap();
        assert_eq!(rec.session_key, root.session_key);
        assert_eq!(rec.cwd.as_deref(), Some("/root"));
        assert_eq!(r.snapshot().records.len(), 1);
        assert_eq!(r.resolve_key(200, 7).as_deref(), Some(root.session_key.as_str()));
        assert_eq!(r.resolve_key(201, 7), None);
        // A claim by the nested pid's key lands on the root.
        let nested_key = r.key_of(200, 7);
        assert_eq!(r.claim(Source::Hook, &nested_key, Activity::Active, "prompt", Utc::now()).unwrap().session_key, root.session_key);
        // (a) a different attempt id: a spawned child with parent_session_key.
        let child = r.apply(Source::Hook, Patch { attempt_id: Some("B".into()), ..patch(300) }).unwrap();
        assert_ne!(child.session_key, root.session_key);
        assert_eq!(child.parent_session_key.as_deref(), Some(root.session_key.as_str()));
        assert_eq!(child.parent_attempt_id.as_deref(), Some("A"));
        assert_eq!(child.attempt_id.as_deref(), Some("B"));
        // Same attempt id as the root: same session, collapsed.
        assert_eq!(r.apply(Source::Hook, Patch { attempt_id: Some("A".into()), ..patch(400) }).unwrap().session_key, root.session_key);
        // Nearest ancestor wins: a process under the child collapses into the child.
        assert_eq!(r.apply(Source::Scan, patch(500)).unwrap().session_key, child.session_key);
        // Launcher and acp never collapse.
        let own = r.apply(Source::Launcher, patch(400)).unwrap();
        assert_ne!(own.session_key, root.session_key);
        assert_eq!(own.parent_session_key, None);
        assert_eq!(r.snapshot().records.len(), 3);
    }

    #[test]
    fn a_second_live_process_on_a_bound_attempt_is_a_conflict() {
        let r = roster(&[]);
        let mut events = r.events();
        let first = r.apply(Source::Hook, Patch { attempt_id: Some("X".into()), ..patch(10) }).unwrap();
        let second = r.apply(Source::Hook, Patch { attempt_id: Some("X".into()), ..patch(11) }).unwrap();
        assert_eq!(second.attempt_id, None);
        assert!(second.conflict);
        assert_eq!(r.live_by_attempt("X").unwrap().session_key, first.session_key);
        let mut saw = false;
        while let Ok(ev) = events.try_recv() {
            if let RosterEvent::Conflict { record, attempt_id, bound_to } = ev {
                assert_eq!(record.session_key, second.session_key);
                assert_eq!((attempt_id.as_str(), bound_to.as_str()), ("X", first.session_key.as_str()));
                saw = true;
            }
        }
        assert!(saw);
        // Rebinding the same attempt on its own record is not a conflict.
        assert!(!r.apply(Source::Acp, Patch { attempt_id: Some("X".into()), ..patch(10) }).unwrap().conflict);
        // Once the first ends, the attempt may be bound again (resume, R2.2).
        r.end(&first.session_key, EndedReason::Crash, Utc::now()).unwrap();
        let third = r.apply(Source::Acp, Patch { attempt_id: Some("X".into()), ..patch(12) }).unwrap();
        assert_eq!(third.attempt_id.as_deref(), Some("X"));
        assert!(!third.conflict);
    }

    #[test]
    fn snapshot_seq_is_monotonic_and_a_no_op_does_not_publish() {
        let r = roster(&[]);
        assert_eq!(r.snapshot().seq, 0);
        r.apply(Source::Hook, cwd(20, "/a")).unwrap();
        assert_eq!(r.snapshot().seq, 1);
        r.apply(Source::Hook, cwd(20, "/a")).unwrap();
        assert_eq!(r.snapshot().seq, 1, "nothing changed, nothing published");
        r.apply(Source::Scan, cwd(20, "/b")).unwrap();
        assert_eq!(r.snapshot().seq, 2, "the value was refused but scan joined sources");
        r.apply(Source::Scan, cwd(20, "/b")).unwrap();
        assert_eq!(r.snapshot().seq, 2, "a refused value from a known source is not a change");
        r.set_capabilities(Capabilities { tmux: true, ..Capabilities::default() });
        assert_eq!(r.snapshot().seq, 3);
        assert!(r.snapshot().capabilities.tmux);
        // Sorted by started_at then key.
        let early = Utc::now() - chrono::Duration::hours(1);
        r.apply(Source::Hook, Patch { started_at: Some(early), ..patch(21) }).unwrap();
        assert_eq!(r.snapshot().records[0].pid, 21);
        assert_eq!(r.snapshot().seq, 4);
    }

    #[test]
    fn end_keeps_the_first_reason_and_sweep_drops_old_endings() {
        let r = roster(&[(31, &[30])]);
        let key = r.apply(Source::Hook, patch(30)).unwrap().session_key;
        r.apply(Source::Scan, patch(31)).unwrap();
        let at = Utc::now();
        let rec = r.end(&key, EndedReason::Exit, at).unwrap();
        assert_eq!((rec.liveness, rec.ended_reason, rec.ended_at), (Liveness::Ended, Some(EndedReason::Exit), Some(at)));
        assert_eq!(r.end(&key, EndedReason::Crash, at).unwrap().ended_reason, Some(EndedReason::Exit));
        assert_eq!(r.live_by_attempt("none"), None);
        r.sweep(at - chrono::Duration::minutes(10));
        assert_eq!(r.snapshot().records.len(), 1);
        r.sweep(at + chrono::Duration::seconds(1));
        assert!(r.snapshot().records.is_empty());
        assert_eq!(r.resolve_key(31, 7), None, "aliases of a dropped root go with it");
        assert!(matches!(r.end(&key, EndedReason::Exit, at), Err(RosterError::NotFound(_))));
    }

    #[test]
    fn names_bind_to_the_key_under_precedence() {
        let r = roster(&[]);
        let key = r.apply(Source::Scan, patch(40)).unwrap().session_key;
        assert_eq!(r.set_name(&key, Some("one".into()), Source::Hook).unwrap().name.as_deref(), Some("one"));
        assert_eq!(r.set_name(&key, Some("two".into()), Source::Scan).unwrap().name.as_deref(), Some("one"));
        assert_eq!(r.set_name(&key, Some("two".into()), Source::Acp).unwrap().name.as_deref(), Some("two"));
        assert_eq!(r.set_name(&key, None, Source::Hook).unwrap().name.as_deref(), Some("two"), "hook cannot clear acp's name");
        assert_eq!(r.set_name(&key, None, Source::Acp).unwrap().name, None);
        assert_eq!(r.set_name(&key, Some("three".into()), Source::Scan).unwrap().name.as_deref(), Some("three"));
        assert_eq!(r.apply(Source::Hook, Patch { name: Some("four".into()), ..patch(40) }).unwrap().name.as_deref(), Some("four"));
    }

    #[tokio::test]
    async fn seqs_survive_a_restart() {
        let path = std::env::temp_dir().join(format!("rosterd-seqs-{}", std::process::id())).join("seqs.json");
        let _ = std::fs::remove_file(&path);
        let me = std::process::id();
        let ticks = scanner::start_ticks(me).unwrap();
        let mine = || Patch { pid: Some(me), start_ticks: Some(ticks), harness: Some("claude".into()), ..Patch::default() };
        let r = roster(&[]);
        r.persist_seqs(path.clone());
        let key = r.apply(Source::Acp, mine()).unwrap().session_key;
        for _ in 0..4 {
            r.claim(Source::Acp, &key, Activity::Active, "prompt", Utc::now()).unwrap();
        }
        // A key whose process is gone (pid 3 never started at tick 7) is dropped at the next load.
        let dead = r.apply(Source::Hook, patch(3)).unwrap().session_key;
        r.claim(Source::Hook, &dead, Activity::Active, "prompt", Utc::now()).unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let file: HashMap<String, u64> = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert_eq!((file.get(&key), file.get(&dead)), (Some(&4), Some(&1)));

        drop(r);
        let r = roster(&[]);
        r.persist_seqs(path.clone());
        assert_eq!(r.apply(Source::Acp, mine()).unwrap().activity_seq, 4, "reattached: the counter continues");
        assert_eq!(r.claim(Source::Acp, &key, Activity::Idle, "turn_end", Utc::now()).unwrap().activity_seq, 5);
        r.end(&key, EndedReason::Exit, Utc::now()).unwrap();
        tokio::time::sleep(Duration::from_millis(400)).await;
        let file: HashMap<String, u64> = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        assert!(file.is_empty(), "ended and dead keys are gone: {file:?}");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// A14 acceptance 9: acp wins every field both sources set, and `explain` says so.
    #[test]
    fn explain_shows_acp_winning_over_hook_and_the_refused_claims() {
        let r = roster(&[]);
        let t0 = Utc::now();
        let sec = chrono::Duration::seconds(1);
        let both = |cwd: &str, sid: &str| Patch { cwd: Some(cwd.into()), session_id: Some(sid.into()), ..patch(70) };
        let key = r.apply(Source::Hook, both("/hook", "s-hook")).unwrap().session_key;
        r.claim(Source::Hook, &key, Activity::Active, "prompt", t0).unwrap();
        r.apply(Source::Acp, both("/acp", "s-acp")).unwrap();
        r.claim(Source::Acp, &key, Activity::Idle, "turn_end", t0 + sec).unwrap();
        r.apply(Source::Hook, Patch { harness: Some("codex".into()), ..both("/hook2", "s-hook2") }).unwrap();
        r.claim(Source::Hook, &key, Activity::Active, "prompt", t0 + sec * 2).unwrap();

        let explain = r.explain(&key).unwrap();
        assert_eq!(explain.session_key, key);
        let by_name: HashMap<&str, &FieldOrigin> = explain.fields.iter().map(|f| (f.field.as_str(), f)).collect();
        for field in ["harness", "session_id", "cwd", "activity"] {
            assert_eq!(by_name[field].source, Source::Acp, "{field}");
        }
        assert_eq!(by_name["cwd"].value, serde_json::json!("/acp"));
        assert_eq!(by_name["session_id"].value, serde_json::json!("s-acp"));
        assert_eq!(by_name["activity"].value, serde_json::json!("idle"));
        assert_eq!(by_name["activity"].at, t0 + sec);
        assert!(!by_name.contains_key("name"), "a null field has no origin");
        let names: Vec<&str> = explain.fields.iter().map(|f| f.field.as_str()).collect();
        assert_eq!(names, ["harness", "session_id", "activity", "cwd"], "R3 order; defaults nobody asserted are absent");

        let rejected: Vec<(&str, Option<&str>)> = explain.rejected.iter().map(|c| (c.field.as_deref().unwrap(), c.event.as_deref())).collect();
        assert_eq!(rejected, [("harness", None), ("session_id", None), ("cwd", None), ("activity", Some("prompt"))]);
        assert!(explain.rejected.iter().all(|c| c.source == Source::Hook && c.reason == "outranked by acp"), "{:?}", explain.rejected);
        assert_eq!(explain.rejected[3].activity, Some(Activity::Active));

        // Oldest first, bounded, and a claim older than the accepted one is refused too.
        for i in 0..60 {
            r.claim(Source::Hook, &key, Activity::Active, &format!("p{i}"), t0 + sec * 3).unwrap();
        }
        r.claim(Source::Acp, &key, Activity::Active, "stale", t0).unwrap();
        let explain = r.explain(&key).unwrap();
        assert_eq!(explain.rejected.len(), REJECTED_KEEP);
        assert_eq!(explain.rejected[0].event.as_deref(), Some("p11"));
        let last = explain.rejected.last().unwrap();
        assert_eq!((last.source, last.event.as_deref(), last.reason.as_str()), (Source::Acp, Some("stale"), "older than the accepted claim"));
        assert_eq!(r.get(&key).unwrap().activity_event.as_deref(), Some("turn_end"));
        assert!(matches!(r.explain("abc:9:9"), Err(RosterError::NotFound(_))));
    }

    /// R15.1 and R15.3: a suspended record keeps its attempt, no process ending touches it, and
    /// only a resume (old key ends `suspended`) frees the attempt for the new key.
    #[test]
    fn a_suspended_record_keeps_its_attempt_until_resumed() {
        let r = roster(&[]);
        let mut events = r.events();
        let holder = Some(HolderHandle { socket: "/h.sock".into() });
        let first = r.apply(Source::Launcher, Patch { attempt_id: Some("S".into()), holder, ..patch(80) }).unwrap();
        let at = Utc::now();
        let rec = r.suspend(&first.session_key, at).unwrap();
        assert_eq!((rec.liveness, rec.holder, rec.attempt_id.as_deref()), (Liveness::Suspended, None, Some("S")));
        assert_eq!(r.suspend(&first.session_key, at).unwrap().liveness, Liveness::Suspended);
        let mut suspended = 0;
        while let Ok(ev) = events.try_recv() {
            suspended += matches!(ev, RosterEvent::Suspended(ref r) if r.session_key == first.session_key) as u32;
        }
        assert_eq!(suspended, 1);
        assert_eq!(r.live_by_attempt("S").unwrap().session_key, first.session_key);
        for reason in [EndedReason::Exit, EndedReason::Crash, EndedReason::Reboot, EndedReason::Killed] {
            assert_eq!(r.end(&first.session_key, reason, at).unwrap().liveness, Liveness::Suspended, "{reason:?} needs a process");
        }
        r.sweep(at + chrono::Duration::hours(1));
        assert_eq!(r.snapshot().records.len(), 1, "sweep keeps a suspended record");
        assert!(r.apply(Source::Launcher, Patch { attempt_id: Some("S".into()), ..patch(81) }).unwrap().conflict, "still bound, R5.4");

        let ended = r.end(&first.session_key, EndedReason::Suspended, at).unwrap();
        assert_eq!((ended.liveness, ended.ended_reason), (Liveness::Ended, Some(EndedReason::Suspended)));
        let resumed = r.apply(Source::Launcher, Patch { attempt_id: Some("S".into()), ..patch(82) }).unwrap();
        assert_eq!((resumed.conflict, resumed.attempt_id.as_deref()), (false, Some("S")));
        assert_eq!(r.live_by_attempt("S").unwrap().session_key, resumed.session_key);
        r.claim(Source::Hook, &first.session_key, Activity::Active, "late", at).unwrap();
        assert_eq!(r.explain(&first.session_key).unwrap().rejected.last().unwrap().reason, "record has ended");
        assert!(matches!(r.suspend(&first.session_key, at), Err(RosterError::Invalid(_))));
    }

    #[test]
    fn registered_fires_on_creation_attempt_and_handle() {
        let r = roster(&[]);
        let mut events = r.events();
        r.apply(Source::Scan, patch(50)).unwrap();
        r.apply(Source::Scan, cwd(50, "/x")).unwrap();
        r.apply(Source::Hook, Patch { attempt_id: Some("A".into()), ..patch(50) }).unwrap();
        r.apply(
            Source::Scan,
            Patch {
                tmux: Some(TmuxHandle { session: "main".into(), window_index: 0, window_name: None, pane_id: "%1".into() }),
                ..patch(50)
            },
        )
        .unwrap();
        let mut registered = 0;
        while let Ok(ev) = events.try_recv() {
            if matches!(ev, RosterEvent::Registered(_)) {
                registered += 1;
            }
        }
        assert_eq!(registered, 3);
        assert!(matches!(r.apply(Source::Hook, Patch::default()), Err(RosterError::NoIdentity)));
        let by_key = r.apply(Source::Hook, Patch { session_key: Some("abc:60:8".into()), ..Patch::default() }).unwrap();
        assert_eq!((by_key.pid, by_key.start_ticks), (60, 8));
        assert!(matches!(
            r.apply(Source::Hook, Patch { session_key: Some("nonsense".into()), ..Patch::default() }),
            Err(RosterError::NoIdentity)
        ));
    }
}
