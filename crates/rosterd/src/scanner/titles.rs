//! The harness's own thread name for an interactive session. Claude Code appends a
//! `custom-title` line to the transcript when the user names the thread; Codex keeps the
//! thread's name in its state database. Both are read on the node the session runs on and
//! applied at rank `files`: under a name given at start or through the API, over the pane
//! title the scanner reads.
use std::collections::HashMap;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use std::time::SystemTime;

use rosterd_proto::Record;
use serde_json::Value;

use crate::runner::transcript::claude_project_dir;

#[derive(Default)]
pub struct Titles {
    /// Per session key: how far into the Claude transcript was read, and the last title seen.
    claude: HashMap<String, (u64, Option<String>)>,
    /// The Codex state file's mtime at the last query, and the thread names it held.
    codex: Option<(SystemTime, HashMap<String, String>)>,
}

impl Titles {
    /// `(key, title)` for every record whose harness names it differently from the record.
    pub async fn refresh(&mut self, records: &[Record], home: &Path) -> Vec<(String, String)> {
        let mut out = Vec::new();
        self.claude.retain(|key, _| records.iter().any(|r| &r.session_key == key));
        let mut codex_wanted = false;
        for rec in records {
            let (Some(sid), Some(cwd)) = (rec.session_id.as_deref(), rec.cwd.as_deref()) else { continue };
            match rec.harness.as_str() {
                "claude" => {
                    let path = home.join(".claude").join("projects").join(claude_project_dir(cwd)).join(format!("{sid}.jsonl"));
                    let entry = self.claude.entry(rec.session_key.clone()).or_default();
                    if let Some(title) = claude_title(&path, &mut entry.0) {
                        entry.1 = Some(title);
                    }
                    if let Some(title) = &entry.1
                        && rec.name.as_deref() != Some(title)
                    {
                        out.push((rec.session_key.clone(), title.clone()));
                    }
                }
                "codex" => codex_wanted = true,
                _ => {}
            }
        }
        if codex_wanted {
            let db = home.join(".codex").join("state_5.sqlite");
            let mtime = std::fs::metadata(&db).and_then(|m| m.modified()).ok();
            if let Some(mtime) = mtime
                && self.codex.as_ref().is_none_or(|(seen, _)| *seen != mtime)
            {
                self.codex = Some((mtime, codex_names(&db).await));
            }
            if let Some((_, names)) = &self.codex {
                for rec in records.iter().filter(|r| r.harness == "codex") {
                    if let Some(title) = rec.session_id.as_deref().and_then(|sid| names.get(sid))
                        && rec.name.as_deref() != Some(title)
                    {
                        out.push((rec.session_key.clone(), title.clone()));
                    }
                }
            }
        }
        out
    }
}

/// The newest `custom-title` past `offset`, which moves to the end of what was read. A file
/// shorter than the offset was replaced: read again from the start.
fn claude_title(path: &Path, offset: &mut u64) -> Option<String> {
    let mut file = std::fs::File::open(path).ok()?;
    let len = file.metadata().ok()?.len();
    if len < *offset {
        *offset = 0;
    }
    if len == *offset {
        return None;
    }
    file.seek(SeekFrom::Start(*offset)).ok()?;
    let mut reader = BufReader::new(file);
    let mut title = None;
    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line).ok()?;
        if n == 0 || !line.ends_with('\n') {
            // A partial last line is read again next time.
            break;
        }
        *offset += n as u64;
        if !line.contains("\"custom-title\"") {
            continue;
        }
        if let Ok(v) = serde_json::from_str::<Value>(&line)
            && v["type"] == "custom-title"
            && let Some(t) = v["customTitle"].as_str().map(str::trim).filter(|t| !t.is_empty())
        {
            title = Some(t.to_string());
        }
    }
    title
}

/// Thread id to name from Codex's state database, through the sqlite3 CLI; empty when it is
/// not installed or the schema is not the one known.
async fn codex_names(db: &Path) -> HashMap<String, String> {
    let out = tokio::process::Command::new("sqlite3")
        .arg("-readonly")
        .arg("-json")
        .arg(db)
        .arg("select id, name from threads where name is not null and name <> ''")
        .output()
        .await;
    let Ok(out) = out else { return HashMap::new() };
    if !out.status.success() {
        return HashMap::new();
    }
    serde_json::from_slice::<Vec<Value>>(&out.stdout)
        .unwrap_or_default()
        .into_iter()
        .filter_map(|row| Some((row["id"].as_str()?.to_string(), row["name"].as_str()?.trim().to_string())))
        .filter(|(_, name)| !name.is_empty())
        .collect()
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use chrono::Utc;

    use super::*;

    fn record(key: &str, harness: &str, sid: &str, cwd: &str, name: Option<&str>) -> Record {
        serde_json::from_value(serde_json::json!({
            "node": "n", "node_id": "n", "session_key": key, "pid": 1, "start_ticks": 1, "started_at": Utc::now(),
            "harness": harness, "session_id": sid, "lane": "interactive", "cwd": cwd, "name": name,
        }))
        .unwrap()
    }

    #[tokio::test]
    async fn claude_titles_follow_the_transcript_incrementally() {
        let home = std::env::temp_dir().join(format!("rosterd-titles-{}", std::process::id()));
        let dir = home.join(".claude/projects").join(claude_project_dir("/w/app"));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("s-1.jsonl");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, r#"{{"type":"user","message":"hi"}}"#).unwrap();
        writeln!(f, r#"{{"type":"custom-title","customTitle":"Fix login","sessionId":"s-1"}}"#).unwrap();
        let mut titles = Titles::default();
        let rec = record("k1", "claude", "s-1", "/w/app", None);
        assert_eq!(titles.refresh(std::slice::from_ref(&rec), &home).await, vec![("k1".to_string(), "Fix login".to_string())]);
        // Nothing new, nothing to say once the record carries it; a rename is picked up.
        let named = record("k1", "claude", "s-1", "/w/app", Some("Fix login"));
        assert!(titles.refresh(std::slice::from_ref(&named), &home).await.is_empty());
        writeln!(f, r#"{{"type":"custom-title","customTitle":"Fix login and signup","sessionId":"s-1"}}"#).unwrap();
        write!(f, r#"{{"type":"custom-title","customTitle":"partial"#).unwrap();
        assert_eq!(titles.refresh(&[named], &home).await, vec![("k1".to_string(), "Fix login and signup".to_string())]);
        // A session of another harness, or without a transcript, is silent.
        let other = record("k2", "claude", "s-2", "/w/app", None);
        assert!(titles.refresh(&[other], &home).await.is_empty());
        let _ = std::fs::remove_dir_all(&home);
    }

    #[tokio::test]
    async fn codex_names_come_from_the_state_database() {
        if crate::integrate::which(&std::env::var_os("PATH").unwrap_or_default(), "sqlite3").is_none() {
            eprintln!("skipped: no sqlite3");
            return;
        }
        let home = std::env::temp_dir().join(format!("rosterd-codex-titles-{}", std::process::id()));
        std::fs::create_dir_all(home.join(".codex")).unwrap();
        let db = home.join(".codex/state_5.sqlite");
        let sql = "create table threads(id text primary key, title text, name text); insert into threads values ('c-1', 'first message', 'Run echo pong'), ('c-2', 'x', '');";
        assert!(std::process::Command::new("sqlite3").arg(&db).arg(sql).status().unwrap().success());
        let mut titles = Titles::default();
        let recs = [record("k1", "codex", "c-1", "/w", None), record("k2", "codex", "c-2", "/w", None)];
        assert_eq!(titles.refresh(&recs, &home).await, vec![("k1".to_string(), "Run echo pong".to_string())]);
        let _ = std::fs::remove_dir_all(&home);
    }
}
