//! R8 offline: the append-only JSONL queue at `<state_dir>/bridge-queue.jsonl`.
//!
//! One line per queued request, one `{"done": id}` line per request the workspace has taken.
//! The file is rewritten with only the pending items (compacted) past 1 MB, on a clean drain,
//! and on open when it carries done markers. Bodies carry no token: the sender looks the
//! attempt's token up when it sends, so this file needs no special mode.

use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub const COMPACT_BYTES: u64 = 1024 * 1024;

/// One request waiting for the workspace, sent with `attempt_id`'s token.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Item {
    pub id: String,
    pub attempt_id: String,
    pub method: String,
    pub path: String,
    pub body: serde_json::Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Line {
    Done { done: String },
    Item(Item),
}

pub struct Queue {
    path: PathBuf,
    items: VecDeque<Item>,
    file: File,
    bytes: u64,
}

impl Queue {
    pub fn open(path: &Path) -> Result<Queue> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut items = VecDeque::new();
        let mut done = 0usize;
        if let Ok(file) = File::open(path) {
            for (n, line) in BufReader::new(file).lines().enumerate() {
                let line = line?;
                if line.trim().is_empty() {
                    continue;
                }
                match serde_json::from_str::<Line>(&line) {
                    Ok(Line::Item(item)) => items.push_back(item),
                    Ok(Line::Done { done: id }) => {
                        items.retain(|i| i.id != id);
                        done += 1;
                    }
                    // A line cut short by a crash mid-append; the rest of the file still counts.
                    Err(error) => tracing::warn!(line = n + 1, %error, "skipping a bad bridge-queue line"),
                }
            }
        }
        let file = append_handle(path)?;
        let bytes = file.metadata()?.len();
        let mut queue = Queue { path: path.to_path_buf(), items, file, bytes };
        if done > 0 {
            queue.compact()?;
        }
        Ok(queue)
    }

    pub fn push(&mut self, item: Item) -> Result<()> {
        self.append(&serde_json::to_string(&item)?)?;
        self.items.push_back(item);
        self.compact_if_large()
    }

    pub fn len(&self) -> usize {
        self.items.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Item> {
        self.items.iter()
    }

    /// The workspace took (or refused for good) the item; it leaves the queue. A clean drain
    /// truncates the file so a quiet node keeps an empty one.
    pub fn remove(&mut self, id: &str) -> Result<()> {
        let Some(at) = self.items.iter().position(|i| i.id == id) else { return Ok(()) };
        self.items.remove(at);
        if self.items.is_empty() {
            return self.compact();
        }
        self.append(&serde_json::json!({ "done": id }).to_string())?;
        self.compact_if_large()
    }

    fn append(&mut self, line: &str) -> Result<()> {
        self.file.write_all(line.as_bytes())?;
        self.file.write_all(b"\n")?;
        self.file.flush()?;
        self.bytes += line.len() as u64 + 1;
        Ok(())
    }

    fn compact_if_large(&mut self) -> Result<()> {
        if self.bytes > COMPACT_BYTES { self.compact() } else { Ok(()) }
    }

    /// Rewrites the file with only the pending items, through a rename so a crash leaves either
    /// the old file or the new one, never half of each.
    fn compact(&mut self) -> Result<()> {
        let tmp = self.path.with_extension("jsonl.tmp");
        let mut out = File::create(&tmp).with_context(|| format!("create {}", tmp.display()))?;
        for item in &self.items {
            out.write_all(serde_json::to_string(item)?.as_bytes())?;
            out.write_all(b"\n")?;
        }
        out.sync_all()?;
        std::fs::rename(&tmp, &self.path)?;
        self.file = append_handle(&self.path)?;
        self.bytes = self.file.metadata()?.len();
        Ok(())
    }
}

fn append_handle(path: &Path) -> Result<File> {
    OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("open {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn item(n: u32) -> Item {
        Item {
            id: format!("id{n}"),
            attempt_id: "A".into(),
            method: "POST".into(),
            path: "/attempts/A/activity".to_string(),
            body: serde_json::json!({ "seq": n }),
            created_at: Utc::now(),
        }
    }

    fn temp(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("rosterd-queue-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        dir.join("bridge-queue.jsonl")
    }

    #[test]
    fn survives_reopen_in_order_and_forgets_done_items() {
        let path = temp("reopen");
        let mut q = Queue::open(&path).unwrap();
        for n in 1..=4 {
            q.push(item(n)).unwrap();
        }
        q.remove("id2").unwrap();
        drop(q);

        let q = Queue::open(&path).unwrap();
        let ids: Vec<_> = q.iter().map(|i| i.id.as_str()).collect();
        assert_eq!(ids, ["id1", "id3", "id4"]);
        // Reopen compacted the done marker away: three lines, no `done`.
        let text = std::fs::read_to_string(&path).unwrap();
        assert_eq!(text.lines().count(), 3);
        assert!(!text.contains("done"));
    }

    #[test]
    fn clean_drain_truncates_and_a_bad_line_is_skipped() {
        let path = temp("drain");
        let mut q = Queue::open(&path).unwrap();
        q.push(item(1)).unwrap();
        q.push(item(2)).unwrap();
        q.remove("id1").unwrap();
        q.remove("id2").unwrap();
        assert_eq!(q.len(), 0);
        assert_eq!(std::fs::metadata(&path).unwrap().len(), 0);

        q.push(item(3)).unwrap();
        drop(q);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        file.write_all(b"{\"id\":\"cut").unwrap();
        let q = Queue::open(&path).unwrap();
        assert_eq!(q.len(), 1);
    }

    #[test]
    fn compacts_past_the_size_ceiling() {
        let path = temp("compact");
        let mut q = Queue::open(&path).unwrap();
        let big = Item { body: serde_json::json!({ "pad": "x".repeat(200_000) }), ..item(0) };
        for n in 1..=6 {
            q.push(Item { id: format!("id{n}"), ..big.clone() }).unwrap();
            q.remove(&format!("id{n}")).unwrap_or_else(|e| panic!("{e}"));
            q.push(Item { id: format!("keep{n}"), ..item(n) }).unwrap();
        }
        // 1.2 MB was appended; without compaction the file would be past the ceiling.
        assert!(std::fs::metadata(&path).unwrap().len() < COMPACT_BYTES);
        assert_eq!(q.len(), 6);
        drop(q);
        let ids: Vec<_> = Queue::open(&path).unwrap().iter().map(|i| i.id.clone()).collect();
        assert_eq!(ids, ["keep1", "keep2", "keep3", "keep4", "keep5", "keep6"]);
    }
}
