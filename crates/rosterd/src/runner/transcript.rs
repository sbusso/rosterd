//! The harness's own transcript, R15.5: `session/load` reads it from the harness's home, so a
//! handoff carries it to the other node. rosterd never reads what is in it; it moves the file.
//!
//! Claude Code: `~/.claude/projects/<cwd with every '/' and '.' as '-'>/<session_id>.jsonl`.
//! Codex: `~/.codex/sessions/YYYY/MM/DD/rollout-<stamp>-<session_id>.jsonl`.
//! Anything else keeps nothing rosterd knows of.

use std::path::{Component, Path, PathBuf};

use super::RunnerError;

/// The directory name Claude Code derives from a cwd.
pub fn claude_project_dir(cwd: &str) -> String {
    cwd.chars().map(|c| if c == '/' || c == '.' { '-' } else { c }).collect()
}

/// The transcript's path relative to `home`, when the harness keeps one and it exists.
pub fn locate(harness: &str, cwd: &str, session_id: &str, home: &Path) -> Option<PathBuf> {
    match harness {
        "claude" => {
            let rel = Path::new(".claude").join("projects").join(claude_project_dir(cwd)).join(format!("{session_id}.jsonl"));
            home.join(&rel).is_file().then_some(rel)
        }
        "codex" => {
            let suffix = format!("-{session_id}.jsonl");
            let sessions = home.join(".codex").join("sessions");
            // ponytail: walks every day directory; fine for years of sessions, index if not.
            let dirs = |p: &Path| std::fs::read_dir(p).into_iter().flatten().flatten().map(|e| e.path()).filter(|p| p.is_dir()).collect::<Vec<_>>();
            for year in dirs(&sessions) {
                for month in dirs(&year) {
                    for day in dirs(&month) {
                        let hit = std::fs::read_dir(&day).into_iter().flatten().flatten().map(|e| e.path()).find(|p| p.file_name().and_then(|n| n.to_str()).is_some_and(|n| n.starts_with("rollout-") && n.ends_with(&suffix)));
                        if let Some(hit) = hit {
                            return hit.strip_prefix(home).ok().map(Path::to_path_buf);
                        }
                    }
                }
            }
            None
        }
        _ => None,
    }
}

/// A path from another node is written under this home only when it is plainly relative:
/// no root, no `..`, no prefix.
fn safe_relative(rel: &str) -> Option<&Path> {
    let path = Path::new(rel);
    (!rel.is_empty() && path.components().all(|c| matches!(c, Component::Normal(_)))).then_some(path)
}

/// Writes the transcript at `rel` under `home`. One already there must be the same bytes;
/// a different one is never overwritten.
pub fn place(home: &Path, rel: &str, bytes: &[u8]) -> Result<(), RunnerError> {
    let rel = safe_relative(rel).ok_or_else(|| RunnerError::Conflict(format!("transcript path {rel} is not a plain relative path")))?;
    let path = home.join(rel);
    match std::fs::read(&path) {
        Ok(existing) if existing == bytes => Ok(()),
        Ok(_) => Err(RunnerError::Conflict(format!("{} exists here and differs", path.display()))),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            Ok(std::fs::write(&path, bytes)?)
        }
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_project_dir_replaces_slashes_and_dots() {
        assert_eq!(claude_project_dir("/Users/mato/Code/gtm/hr-exec"), "-Users-mato-Code-gtm-hr-exec");
        assert_eq!(claude_project_dir("/Users/mato/Code/x/.claude/worktrees/a"), "-Users-mato-Code-x--claude-worktrees-a");
        assert_eq!(claude_project_dir("/home/me/v1.2_app"), "-home-me-v1-2_app");
    }

    #[test]
    fn locate_finds_claude_and_codex_transcripts_under_a_home() {
        let home = std::env::temp_dir().join(format!("rosterd-transcript-{}", std::process::id()));
        let claude = home.join(".claude/projects/-w-repo");
        std::fs::create_dir_all(&claude).unwrap();
        std::fs::write(claude.join("sid-1.jsonl"), b"{}\n").unwrap();
        let codex = home.join(".codex/sessions/2026/09/17");
        std::fs::create_dir_all(&codex).unwrap();
        std::fs::write(codex.join("rollout-2026-09-17T10-00-00-uuid-2.jsonl"), b"{}\n").unwrap();

        assert_eq!(locate("claude", "/w/repo", "sid-1", &home).unwrap(), Path::new(".claude/projects/-w-repo/sid-1.jsonl"));
        assert_eq!(locate("claude", "/w/other", "sid-1", &home), None);
        assert_eq!(locate("codex", "/w/repo", "uuid-2", &home).unwrap(), Path::new(".codex/sessions/2026/09/17/rollout-2026-09-17T10-00-00-uuid-2.jsonl"));
        assert_eq!(locate("codex", "/w/repo", "uuid-9", &home), None);
        assert_eq!(locate("pi", "/w/repo", "sid-1", &home), None);
        let _ = std::fs::remove_dir_all(&home);
    }

    #[test]
    fn place_writes_once_and_only_plain_relative_paths() {
        let home = std::env::temp_dir().join(format!("rosterd-place-{}", std::process::id()));
        let rel = ".claude/projects/-w/x.jsonl";
        place(&home, rel, b"one").unwrap();
        assert_eq!(std::fs::read(home.join(rel)).unwrap(), b"one");
        place(&home, rel, b"one").unwrap();
        assert!(matches!(place(&home, rel, b"two"), Err(RunnerError::Conflict(m)) if m.ends_with("differs")));
        assert_eq!(std::fs::read(home.join(rel)).unwrap(), b"one", "never overwritten");
        for bad in ["/etc/passwd", "../.ssh/authorized_keys", "a/../../b", ""] {
            assert!(matches!(place(&home, bad, b"x"), Err(RunnerError::Conflict(_))), "{bad}");
        }
        let _ = std::fs::remove_dir_all(&home);
    }
}
