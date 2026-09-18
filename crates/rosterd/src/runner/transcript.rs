//! The harness's own transcript, R15.5: `session/load` reads it from the harness's home, so a
//! handoff carries it to the other node. rosterd never reads what is in it; it moves the file.
//!
//! Claude Code: `~/.claude/projects/<cwd with every '/' and '.' as '-'>/<session_id>.jsonl`.
//! Codex: `~/.codex/sessions/YYYY/MM/DD/rollout-<stamp>-<session_id>.jsonl`.
//! Anything else keeps nothing rosterd knows of. R20 reads the messages out of it, once, to
//! replay them to an ACP client that loads the session.

use std::path::{Component, Path, PathBuf};

use serde_json::{Value, json};

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

/// The conversation in a transcript as ACP `session/update` updates, R20: user and agent
/// message chunks, one per message, text only. Tool calls, thoughts, images and what the
/// harness injects around the user's words are left out; a line that is not one of the
/// harness's message records is skipped.
pub fn updates(harness: &str, text: &str) -> Vec<Value> {
    let chunk = |kind: &str, text: String| (!text.trim().is_empty()).then(|| json!({ "sessionUpdate": kind, "content": { "type": "text", "text": text } }));
    text.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).ok())
        .filter_map(|v| match harness {
            "claude" => {
                // Claude Code: `{"type":"user"|"assistant","message":{"content":...}}`; a meta
                // or sidechain record is the harness talking to itself.
                if v["isMeta"].as_bool() == Some(true) || v["isSidechain"].as_bool() == Some(true) {
                    return None;
                }
                let kind = match v["type"].as_str()? {
                    "user" => "user_message_chunk",
                    "assistant" => "agent_message_chunk",
                    _ => return None,
                };
                let text = match &v["message"]["content"] {
                    Value::String(s) => s.clone(),
                    Value::Array(blocks) => blocks.iter().filter(|b| b["type"] == "text").filter_map(|b| b["text"].as_str()).collect::<Vec<_>>().join("\n"),
                    _ => return None,
                };
                chunk(kind, text)
            }
            "codex" => {
                // Codex: `{"type":"response_item","payload":{"type":"message","role":...,"content":[...]}}`.
                // ponytail: a user text starting with `<` or `# AGENTS.md instructions` is
                // Codex's own context block (`<environment_context>`, `<app-context>`, the
                // instructions file), not the user's; a user opening with either is the ceiling.
                let payload = &v["payload"];
                if v["type"] != "response_item" || payload["type"] != "message" {
                    return None;
                }
                let kind = match payload["role"].as_str()? {
                    "user" => "user_message_chunk",
                    "assistant" => "agent_message_chunk",
                    _ => return None,
                };
                let text = payload["content"]
                    .as_array()?
                    .iter()
                    .filter(|b| b["type"] == "input_text" || b["type"] == "output_text")
                    .filter_map(|b| b["text"].as_str())
                    .filter(|t| kind != "user_message_chunk" || !(t.trim_start().starts_with('<') || t.starts_with("# AGENTS.md instructions")))
                    .collect::<Vec<_>>()
                    .join("\n");
                chunk(kind, text)
            }
            _ => None,
        })
        .collect()
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
    fn updates_are_the_messages_of_either_transcript() {
        let claude = concat!(
            r#"{"type":"user","message":{"role":"user","content":"fix the build"}}"#, "\n",
            r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"thinking","thinking":"hmm"},{"type":"text","text":"On it."},{"type":"tool_use","id":"t1","name":"Bash"}]}}"#, "\n",
            r#"{"type":"user","message":{"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"ok"}]}}"#, "\n",
            r#"{"type":"user","isMeta":true,"message":{"role":"user","content":"[Image: 2x2]"}}"#, "\n",
            r#"{"type":"user","isSidechain":true,"message":{"role":"user","content":"subagent brief"}}"#, "\n",
            r#"{"type":"assistant","message":{"role":"assistant","content":[{"type":"text","text":"Done."}]}}"#, "\n",
            "not json\n",
            r#"{"type":"custom-title","customTitle":"build fix"}"#, "\n",
        );
        let got: Vec<(String, String)> = updates("claude", claude).iter().map(|u| (u["sessionUpdate"].as_str().unwrap().into(), u["content"]["text"].as_str().unwrap().into())).collect();
        assert_eq!(got, vec![("user_message_chunk".into(), "fix the build".into()), ("agent_message_chunk".into(), "On it.".into()), ("agent_message_chunk".into(), "Done.".into())]);

        let codex = concat!(
            r#"{"type":"session_meta","payload":{"id":"x"}}"#, "\n",
            r#"{"type":"response_item","payload":{"type":"message","role":"developer","content":[{"type":"input_text","text":"You are Codex"}]}}"#, "\n",
            r##"{"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"<environment_context>\n</environment_context>"},{"type":"input_text","text":"# AGENTS.md instructions for /w\n\n<INSTRUCTIONS>\nbe brief\n</INSTRUCTIONS>"},{"type":"input_text","text":"fix the build"},{"type":"input_image","image_url":"data:"}]}}"##, "\n",
            r#"{"type":"response_item","payload":{"type":"reasoning","summary":[]}}"#, "\n",
            r#"{"type":"response_item","payload":{"type":"function_call","name":"shell"}}"#, "\n",
            r#"{"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"Done."}]}}"#, "\n",
            r#"{"type":"event_msg","payload":{"type":"task_complete"}}"#, "\n",
        );
        let got: Vec<(String, String)> = updates("codex", codex).iter().map(|u| (u["sessionUpdate"].as_str().unwrap().into(), u["content"]["text"].as_str().unwrap().into())).collect();
        assert_eq!(got, vec![("user_message_chunk".into(), "fix the build".into()), ("agent_message_chunk".into(), "Done.".into())]);
        assert!(updates("pi", claude).is_empty());
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
