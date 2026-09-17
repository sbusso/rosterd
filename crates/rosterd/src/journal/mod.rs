//! The per-node journal, R18: an append-only log of this node's own changes and of the actions
//! the API ran, one JSON line per entry under `<state dir>/journal/YYYY-MM-DD.jsonl`, `seq`
//! monotonic across files and restarts. A client that was down replays it with
//! `GET /journal?since=` or `GET /swarm/changes?since=`, so nobody keeps the SSE open to know
//! what agents did.
//!
//! The writer (`run`) is the single source of local change events: it diffs consecutive roster
//! snapshots, appends, and broadcasts every entry; `/swarm/changes` subscribes for local changes
//! and still diffs swarm frames for peers.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use rosterd_proto::{JournalEntry, JournalKind, changes};
use serde_json::Value;
use tokio::sync::broadcast;

use crate::roster::Roster;

const FILE_SUFFIX: &str = ".jsonl";
const BROADCAST_CAPACITY: usize = 256;

pub struct Journal {
    node: String,
    node_id: String,
    dir: PathBuf,
    keep_days: u32,
    inner: Mutex<Inner>,
    tx: broadcast::Sender<JournalEntry>,
}

struct Inner {
    seq: u64,
    /// The open file and its day; reopened when the day changes.
    file: Option<(NaiveDate, File)>,
}

impl Journal {
    /// Opens the journal under `dir`, continuing `seq` from the last line of the newest file.
    /// The directory is created on the first append.
    pub fn open(dir: PathBuf, node: &str, node_id: &str, keep_days: u32) -> Arc<Journal> {
        let seq = last_seq(&dir);
        let (tx, _) = broadcast::channel(BROADCAST_CAPACITY);
        Arc::new(Journal { node: node.into(), node_id: node_id.into(), dir, keep_days, inner: Mutex::new(Inner { seq, file: None }), tx })
    }

    /// Records an API action that ran on this node.
    pub fn action(&self, action: &str, session_key: Option<String>, by: Option<String>, detail: Value) {
        self.append(JournalKind::Action { at: Utc::now(), action: action.into(), session_key, by, detail });
    }

    /// Appends one entry and broadcasts it. A write failure is logged, never fatal: the roster
    /// keeps answering without its history, R2.
    // ponytail: one synchronous write under a std mutex per entry; a buffered writer flushed on a
    // timer if the rate ever matters.
    pub fn append(&self, kind: JournalKind) -> JournalEntry {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.seq += 1;
        let entry = JournalEntry { seq: inner.seq, node: self.node.clone(), node_id: self.node_id.clone(), kind };
        if let Err(error) = self.write(&mut inner, &entry) {
            tracing::warn!(%error, dir = %self.dir.display(), "journal append failed");
        }
        drop(inner);
        let _ = self.tx.send(entry.clone());
        entry
    }

    fn write(&self, inner: &mut Inner, entry: &JournalEntry) -> std::io::Result<()> {
        let today = entry.at().date_naive();
        if inner.file.as_ref().is_none_or(|(day, _)| *day != today) {
            std::fs::create_dir_all(&self.dir)?;
            let file = OpenOptions::new().create(true).append(true).open(self.dir.join(format!("{today}{FILE_SUFFIX}")))?;
            inner.file = Some((today, file));
        }
        let (_, file) = inner.file.as_mut().expect("opened above");
        let mut line = serde_json::to_vec(entry)?;
        line.push(b'\n');
        file.write_all(&line)
    }

    /// Every entry appended from now on.
    pub fn subscribe(&self) -> broadcast::Receiver<JournalEntry> {
        self.tx.subscribe()
    }

    /// Entries with `seq > since` (and `at > after`, and about `session`, when given), oldest
    /// first, at most `limit`.
    // ponytail: reads every file front to back; skip files by their last seq if 30 days ever weigh.
    pub fn read(&self, since: u64, after: Option<DateTime<Utc>>, limit: usize, session: Option<&str>) -> Vec<JournalEntry> {
        let mut out = Vec::new();
        for path in files(&self.dir) {
            let Ok(file) = File::open(&path) else { continue };
            for line in BufReader::new(file).lines().map_while(Result::ok) {
                let Ok(entry) = serde_json::from_str::<JournalEntry>(&line) else { continue };
                if entry.seq > since && after.is_none_or(|after| entry.at() > after) && session.is_none_or(|key| entry.session_key() == Some(key)) {
                    out.push(entry);
                    if out.len() >= limit {
                        return out;
                    }
                }
            }
        }
        out
    }

    /// Deletes files older than `keep_days`.
    pub fn prune(&self) {
        let cutoff = Utc::now().date_naive() - chrono::Days::new(u64::from(self.keep_days));
        for path in files(&self.dir) {
            if day_of(&path).is_some_and(|day| day < cutoff) {
                match std::fs::remove_file(&path) {
                    Ok(()) => tracing::info!(path = %path.display(), "journal file pruned"),
                    Err(error) => tracing::warn!(%error, path = %path.display(), "journal prune failed"),
                }
            }
        }
    }

    /// The writer: prunes at start and daily, and appends the changes between consecutive roster
    /// snapshots. Every record of the local roster is this node's own, so nothing is filtered;
    /// the node list is empty, so no node events are written.
    pub async fn run(self: Arc<Self>, roster: Arc<Roster>) {
        let mut local = roster.watch();
        let mut prev = local.borrow_and_update().as_swarm();
        let mut daily = tokio::time::interval(Duration::from_secs(24 * 3600));
        loop {
            tokio::select! {
                changed = local.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    let next = local.borrow_and_update().as_swarm();
                    for change in changes(&prev, &next) {
                        self.append(JournalKind::Change { change });
                    }
                    prev = next;
                }
                _ = daily.tick() => self.prune(),
            }
        }
    }
}

/// The journal files, oldest first: the names sort by date.
fn files(dir: &PathBuf) -> Vec<PathBuf> {
    let Ok(entries) = std::fs::read_dir(dir) else { return Vec::new() };
    let mut files: Vec<PathBuf> = entries.filter_map(Result::ok).map(|e| e.path()).filter(|p| day_of(p).is_some()).collect();
    files.sort();
    files
}

fn day_of(path: &std::path::Path) -> Option<NaiveDate> {
    path.file_name()?.to_str()?.strip_suffix(FILE_SUFFIX)?.parse().ok()
}

/// The `seq` of the last line of the newest file, 0 without one.
fn last_seq(dir: &PathBuf) -> u64 {
    let Some(newest) = files(dir).pop() else { return 0 };
    let Ok(text) = std::fs::read_to_string(&newest) else { return 0 };
    text.lines().rev().find_map(|line| serde_json::from_str::<JournalEntry>(line).ok()).map(|e| e.seq).unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn temp(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rosterd-journal-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn seq_continues_across_a_restart_and_reads_filter_and_limit() {
        let dir = temp("seq");
        let journal = Journal::open(dir.clone(), "gibson", "abc", 30);
        let mut rx = journal.subscribe();
        journal.action("prompt", Some("k1".into()), Some("local".into()), json!({ "prompt": "go" }));
        journal.action("cancel", Some("k1".into()), Some("local".into()), Value::Null);
        assert_eq!(rx.try_recv().unwrap().seq, 1);
        assert_eq!(rx.try_recv().unwrap().seq, 2);
        drop(journal);

        let journal = Journal::open(dir.clone(), "gibson", "abc", 30);
        journal.action("name", Some("k2".into()), None, json!({ "name": "x" }));
        let all = journal.read(0, None, 200, None);
        assert_eq!(all.iter().map(|e| e.seq).collect::<Vec<_>>(), [1, 2, 3]);
        assert_eq!(all[0].session_key(), Some("k1"));
        assert_eq!(journal.read(1, None, 1, None).iter().map(|e| e.seq).collect::<Vec<_>>(), [2]);
        assert_eq!(journal.read(0, Some(all[1].at()), 200, None).iter().map(|e| e.seq).collect::<Vec<_>>(), [3]);
        assert_eq!(journal.read(0, None, 200, Some("k2")).iter().map(|e| e.seq).collect::<Vec<_>>(), [3]);
        assert!(journal.read(3, None, 200, None).is_empty());
        let files = files(&dir);
        assert_eq!(files.len(), 1, "one file per day");
        assert_eq!(std::fs::read_to_string(&files[0]).unwrap().lines().count(), 3);
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn prune_deletes_files_older_than_keep_days() {
        let dir = temp("prune");
        std::fs::create_dir_all(&dir).unwrap();
        let today = Utc::now().date_naive();
        let old = today - chrono::Days::new(31);
        let kept = today - chrono::Days::new(29);
        for day in [old, kept] {
            std::fs::write(dir.join(format!("{day}.jsonl")), b"").unwrap();
        }
        std::fs::write(dir.join("notes.txt"), b"").unwrap();
        Journal::open(dir.clone(), "gibson", "abc", 30).prune();
        assert!(!dir.join(format!("{old}.jsonl")).exists());
        assert!(dir.join(format!("{kept}.jsonl")).exists());
        assert!(dir.join("notes.txt").exists(), "only journal files are touched");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[tokio::test]
    async fn the_writer_journals_roster_changes() {
        let dir = temp("writer");
        let roster = Roster::new("gibson", "abc", Default::default());
        let journal = Journal::open(dir.clone(), "gibson", "abc", 30);
        let mut rx = journal.subscribe();
        tokio::spawn(journal.clone().run(roster.clone()));
        // The writer takes its first snapshot when first polled.
        tokio::task::yield_now().await;
        let key = roster.apply(rosterd_proto::Source::Hook, crate::roster::Patch { pid: Some(1), start_ticks: Some(1), harness: Some("claude".into()), ..Default::default() }).unwrap().session_key;
        let entry = tokio::time::timeout(Duration::from_secs(5), rx.recv()).await.unwrap().unwrap();
        assert_eq!((entry.seq, entry.name(), entry.session_key()), (1, "session_started", Some(key.as_str())));
        let json: Value = serde_json::to_value(&entry).unwrap();
        assert_eq!((json["kind"].as_str(), json["event"].as_str(), json["record"]["peer_state"].as_str()), (Some("change"), Some("session_started"), Some("local")));
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
