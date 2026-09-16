//! The daemon's side of a holder, R2.1: where its files live, how it is spawned, how its
//! state file is read back, how its socket is reached, and how it is stopped.
//!
//! Unix sockets only; Windows named pipes are a later target.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use rosterd_proto::HolderState;
use serde_json::Value;
use tokio::net::UnixStream;
use tokio::process::Command;
use tokio::task::JoinHandle;

use super::RunnerError;
use crate::config::Config;

/// One holder's files under `config.runner.holder_dir`: `<id>.sock`, `<id>.json`, `<id>.log`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Paths {
    pub socket: PathBuf,
    pub state: PathBuf,
    pub log: PathBuf,
}

impl Paths {
    pub fn new(dir: &Path, id: &str) -> Paths {
        Paths { socket: dir.join(format!("{id}.sock")), state: dir.join(format!("{id}.json")), log: dir.join(format!("{id}.log")) }
    }

    /// The paths a state file belongs to.
    pub fn of_state_file(state: &Path) -> Paths {
        let id = state.file_stem().map(|s| s.to_string_lossy().into_owned()).unwrap_or_default();
        Paths::new(state.parent().unwrap_or(Path::new(".")), &id)
    }

    /// Removes what a dead holder left behind. The log stays for the post mortem.
    // ponytail: logs pile up in holder_dir; sweep old ones when it matters.
    pub fn clean(&self) {
        let _ = std::fs::remove_file(&self.socket);
        let _ = std::fs::remove_file(&self.state);
    }
}

/// `config.runner.holder_bin`, else `rosterd-holder` next to the running daemon.
pub fn holder_bin(config: &Config) -> PathBuf {
    config.runner.holder_bin.clone().unwrap_or_else(|| {
        std::env::current_exe()
            .ok()
            .and_then(|exe| exe.parent().map(|d| d.join("rosterd-holder")))
            .unwrap_or_else(|| PathBuf::from("rosterd-holder"))
    })
}

/// Creates `dir` with mode 0700, R6.
pub fn ensure_dir(dir: &Path) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    use std::os::unix::fs::PermissionsExt;
    std::fs::set_permissions(dir, std::fs::Permissions::from_mode(0o700))
}

pub struct Launch<'a> {
    pub bin: &'a Path,
    pub paths: &'a Paths,
    pub harness: &'a str,
    pub cwd: &'a Path,
    pub attempt_id: Option<&'a str>,
    pub parent_attempt_id: Option<&'a str>,
    pub meta: &'a Value,
    pub adapter: &'a str,
    pub args: &'a [String],
    pub env: &'a HashMap<String, String>,
}

/// Spawns a holder in its own process group so it outlives the daemon, R2.1. Returns its pid
/// and the task reaping it.
pub fn spawn(launch: Launch<'_>) -> Result<(u32, JoinHandle<std::io::Result<std::process::ExitStatus>>), RunnerError> {
    let log = std::fs::File::create(&launch.paths.log)?;
    let mut cmd = Command::new(launch.bin);
    cmd.arg("--socket")
        .arg(&launch.paths.socket)
        .arg("--state")
        .arg(&launch.paths.state)
        .arg("--harness")
        .arg(launch.harness)
        .arg("--cwd")
        .arg(launch.cwd)
        .arg("--meta")
        .arg(launch.meta.to_string());
    if let Some(a) = launch.attempt_id {
        cmd.arg("--attempt-id").arg(a);
    }
    if let Some(p) = launch.parent_attempt_id {
        cmd.arg("--parent-attempt-id").arg(p);
    }
    cmd.arg("--").arg(launch.adapter).args(launch.args);
    cmd.envs(launch.env).stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::from(log)).process_group(0);
    let mut child = cmd.spawn().map_err(|e| RunnerError::Holder(format!("spawn {}: {e}", launch.bin.display())))?;
    let pid = child.id().ok_or_else(|| RunnerError::Holder("holder exited before it had a pid".into()))?;
    Ok((pid, tokio::spawn(async move { child.wait().await })))
}

pub fn read_state(path: &Path) -> Option<HolderState> {
    serde_json::from_slice(&std::fs::read(path).ok()?).ok()
}

/// The daemon's own rewrite of a state file whose holder is gone, R15.4: same atomic shape as
/// the holder's.
pub fn write_state(path: &Path, state: &HolderState) -> std::io::Result<()> {
    let tmp = path.with_extension("json.tmp");
    std::fs::write(&tmp, serde_json::to_vec_pretty(state)?)?;
    std::fs::rename(&tmp, path)
}

/// Polls for the state file the holder writes before listening, giving up when the holder
/// exits first or `timeout` passes.
pub async fn wait_state(
    paths: &Paths,
    reaper: &JoinHandle<std::io::Result<std::process::ExitStatus>>,
    timeout: Duration,
) -> Result<HolderState, RunnerError> {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if let Some(state) = read_state(&paths.state) {
            return Ok(state);
        }
        if reaper.is_finished() {
            return Err(RunnerError::Holder(format!("holder exited before writing its state; see {}", paths.log.display())));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(RunnerError::Holder(format!("holder wrote no state file within {timeout:?}")));
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

pub async fn connect(socket: &Path) -> std::io::Result<UnixStream> {
    UnixStream::connect(socket).await
}

/// The holder listens right after writing its state; a few retries cover the gap.
pub async fn connect_retry(socket: &Path, attempts: u32) -> std::io::Result<UnixStream> {
    let mut last = None;
    for _ in 0..attempts {
        match connect(socket).await {
            Ok(s) => return Ok(s),
            Err(e) => last = Some(e),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(last.unwrap_or_else(|| std::io::Error::other("no attempts")))
}

/// Every state file under the holder directory, R2.1 recovery.
pub fn list_states(dir: &Path) -> Vec<(Paths, HolderState)> {
    let Ok(entries) = std::fs::read_dir(dir) else { return Vec::new() };
    let mut states: Vec<(Paths, HolderState)> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .filter_map(|p| read_state(&p).map(|s| (Paths::of_state_file(&p), s)))
        .collect();
    states.sort_by_key(|(_, s)| s.started_at);
    states
}

/// SIGTERM to the holder; it forwards to the adapter and cleans up, R5.6.
pub fn terminate(holder_pid: u32) {
    unsafe { libc::kill(holder_pid as libc::pid_t, libc::SIGTERM) };
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_round_trip_through_the_state_file() {
        let dir = Path::new("/tmp/holders");
        let paths = Paths::new(dir, "01ABC");
        assert_eq!(paths.socket, Path::new("/tmp/holders/01ABC.sock"));
        assert_eq!(paths.log, Path::new("/tmp/holders/01ABC.log"));
        assert_eq!(Paths::of_state_file(&paths.state), paths);
    }
}
