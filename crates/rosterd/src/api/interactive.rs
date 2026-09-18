//! POST /sessions with lane interactive, R9 (DESIGN 7.1): the harness's own terminal UI in a
//! new tmux session on this node, started through rosterd-launch so it registers like one a
//! human typed, and attachable from any node through /sessions/{key}/attach.

use std::time::Duration;

use axum::http::StatusCode;
use rosterd_proto::{Record, Source, TmuxHandle};

use super::routes::ApiError;
use crate::node::Node;
use crate::roster::Patch;
use crate::runner::StartSession;

/// The launcher registers the pane's pid within milliseconds of the tmux session existing;
/// this is the wait for that record.
const REGISTER_TIMEOUT: Duration = Duration::from_secs(5);

/// The tmux session and the record, once the launcher has registered it. `warnings` names
/// what the request asked for that the harness's command line cannot carry.
pub async fn start(node: &Node, req: &StartSession) -> Result<(Record, Vec<String>), ApiError> {
    // Without a name the tmux session is the harness, numbered past the first, and the record's
    // name stays the scanner's to fill from the pane title the harness sets.
    let name = req.name.clone().filter(|n| !n.is_empty());
    let base: String = name.as_deref().unwrap_or(&req.harness).chars().map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' }).collect();
    let mut session = base.clone();
    if name.is_none() {
        let mut n = 1;
        while tmux(&["has-session", "-t", &format!("={session}")]).await.is_some() {
            n += 1;
            session = format!("{base}-{n}");
        }
    }
    let Some(cwd) = req.cwd.as_deref().filter(|c| std::path::Path::new(c).is_dir()) else {
        return Err(ApiError::new(StatusCode::BAD_REQUEST, format!("cwd {} is not a directory here", req.cwd.as_deref().unwrap_or("(none)"))));
    };
    let (argv, warnings) = harness_argv(req);
    let launcher = std::env::current_exe().ok().map(|exe| exe.with_file_name("rosterd-launch")).filter(|p| p.is_file());
    let launcher = launcher.as_deref().map(|p| p.to_string_lossy().into_owned()).unwrap_or_else(|| "rosterd-launch".into());

    // tmux runs the command directly, no shell between: the pane's pid is the launcher's, which
    // exec makes the harness's, so the record it registers is the one the scanner will find.
    let mut cmd = tokio::process::Command::new("tmux");
    cmd.args(["new-session", "-d", "-s", &session, "-c", cwd]);
    for (k, v) in &req.env {
        cmd.args(["-e", &format!("{k}={v}")]);
    }
    cmd.arg("--").arg(&launcher);
    if let Some(name) = &name {
        cmd.args(["--name", name]);
    }
    cmd.args(["--harness", &req.harness, "--socket"]).arg(node.config.socket_path()).arg("--").args(&argv);
    let out = cmd.output().await.map_err(|e| ApiError::new(StatusCode::INTERNAL_SERVER_ERROR, format!("tmux: {e}")))?;
    if !out.status.success() {
        let err = String::from_utf8_lossy(&out.stderr).trim().to_string();
        let status = if err.contains("duplicate session") { StatusCode::CONFLICT } else { StatusCode::INTERNAL_SERVER_ERROR };
        return Err(ApiError::new(status, format!("tmux new-session {session}: {err}")));
    }
    // The handle at birth (DESIGN 7.1): the pane is known now, not at the scanner's next pass.
    let pane = tmux(&["list-panes", "-t", &format!("={session}"), "-F", "#{pane_pid}|#{pane_id}|#{pane_tty}|#{window_index}|#{window_name}"]).await.unwrap_or_default();
    let f: Vec<&str> = pane.trim().split('|').collect();
    let pid: u32 = f.first().and_then(|p| p.parse().ok()).unwrap_or(0);
    let handle = (f.len() >= 5).then(|| TmuxHandle {
        session: session.clone(),
        window_index: f[3].parse().unwrap_or(0),
        window_name: Some(f[4].to_string()).filter(|n| !n.is_empty()),
        pane_id: f[1].to_string(),
    });
    let tty = f.get(2).map(|t| t.to_string()).filter(|t| !t.is_empty());
    let deadline = tokio::time::Instant::now() + REGISTER_TIMEOUT;
    loop {
        if let Some(record) = node.roster.snapshot().records.iter().find(|r| pid != 0 && r.pid == pid) {
            let patch = Patch { session_key: Some(record.session_key.clone()), tmux: handle, tty, ..Patch::default() };
            let record = node.roster.apply(Source::Scan, patch).unwrap_or_else(|_| record.clone());
            return Ok((record, warnings));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(ApiError::new(StatusCode::BAD_GATEWAY, format!("tmux session {session} started but pid {pid} never registered; is rosterd-launch installed beside the daemon?")));
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// One tmux command's stdout, none when it failed.
async fn tmux(args: &[&str]) -> Option<String> {
    let out = tokio::process::Command::new("tmux").env("LC_ALL", "C.UTF-8").args(args).output().await.ok()?;
    out.status.success().then(|| String::from_utf8_lossy(&out.stdout).into_owned())
}

/// The harness command with what its command line can carry. Model for claude and codex,
/// effort for codex; the rest is reported, not silently dropped.
fn harness_argv(req: &StartSession) -> (Vec<String>, Vec<String>) {
    let mut argv = vec![req.harness.clone()];
    let mut warnings = Vec::new();
    match req.harness.as_str() {
        "claude" => {
            if let Some(m) = &req.model {
                argv.extend(["--model".into(), m.clone()]);
            }
            if req.effort.is_some() {
                warnings.push("claude takes no effort on its command line; set it in the session".into());
            }
        }
        "codex" => {
            if let Some(m) = &req.model {
                argv.extend(["-m".into(), m.clone()]);
            }
            if let Some(e) = &req.effort {
                argv.extend(["-c".into(), format!("model_reasoning_effort=\"{e}\"")]);
            }
        }
        _ => {
            if req.model.is_some() || req.effort.is_some() {
                warnings.push(format!("{}: model and effort are not carried on its command line", req.harness));
            }
        }
    }
    if req.permission_policy.is_some() {
        warnings.push("an interactive session answers its own permission prompts; policy ignored".into());
    }
    (argv, warnings)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn argv_carries_what_the_command_line_can() {
        let req = StartSession { harness: "codex".into(), model: Some("gpt-5".into()), effort: Some("high".into()), ..Default::default() };
        let (argv, warnings) = harness_argv(&req);
        assert_eq!(argv, ["codex", "-m", "gpt-5", "-c", "model_reasoning_effort=\"high\""]);
        assert!(warnings.is_empty());
        let req = StartSession { harness: "claude".into(), effort: Some("high".into()), permission_policy: Some(rosterd_proto::PermissionPolicy::Auto), ..Default::default() };
        let (argv, warnings) = harness_argv(&req);
        assert_eq!(argv, ["claude"]);
        assert_eq!(warnings.len(), 2);
    }
}
