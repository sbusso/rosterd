//! Process enumeration and runtime handles, R4 `scan`: every 2 s, confirm registered PIDs are
//! alive, add stray harness processes as unknown, collapse nested harness processes of one
//! session into the root, set `parent_session_key` when the parent is another harness, and fill
//! tmux and herdr handles from one bounded `tmux list-panes` and one herdr pane list per pass.
//!
//! R11: sysinfo does /proc, libproc and toolhelp underneath, so one code path serves Linux,
//! macOS and Windows. Identity is pid plus start time (seconds since the epoch) everywhere.
//!
//! OWNER: the roster/scanner agent.

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, LazyLock, Mutex, RwLock};
use std::time::{Duration, Instant};

use chrono::{DateTime, TimeZone, Utc};
use rosterd_proto::{Activity, EndedReason, HerdrHandle, Lane, Liveness, Load, Record, Source, TmuxHandle};
use sysinfo::{Pid, ProcessRefreshKind, ProcessesToUpdate, System, UpdateKind};

use crate::config::Config;
use crate::roster::{Patch, Roster};

/// Ended records stay this long so clients see the ending.
const KEEP_ENDED: Duration = Duration::from_secs(600);
/// Budget for one tmux or herdr call, R4.
const HANDLE_TIMEOUT: Duration = Duration::from_millis(500);
/// The harness names every node knows without config, R4. `ccd-cli` is the Claude desktop app's
/// copy of Claude Code, laid out as `~/.claude/remote/ccd-cli/<version>`.
const BUILTIN: [(&str, &str); 5] =
    [("claude", "claude"), ("codex", "codex"), ("claude-agent-acp", "claude"), ("codex-acp", "codex"), ("ccd-cli", "claude")];
const TMUX_FORMAT: &str =
    "#{session_name}\t#{window_index}\t#{window_name}\t#{pane_id}\t#{pane_pid}\t#{pane_tty}";
/// CPU activity, the claim a scan-only session gets when no hook or adapter speaks for it: the
/// process tree used at least this share of one pass, or has been under it for `QUIET`.
/// ponytail: sampled on a 2s pass an idle Claude Code sits at 0.5–2.5%, a working one spikes to
/// 4–12% every few seconds and drops below 1% while it waits on the API; raise QUIET before
/// lowering the share if a long think shows as idle.
const BUSY_PCT: f32 = 4.0;
const QUIET: chrono::Duration = chrono::Duration::seconds(60);

#[derive(Debug, Clone)]
struct ProcInfo {
    ppid: Option<u32>,
    start_ticks: u64,
    name: String,
    exe: Option<String>,
    cmd: Vec<String>,
    cwd: Option<String>,
    /// CPU-milliseconds since the process was first seen, the share of the last pass it used,
    /// and resident memory.
    cpu_ms: u64,
    busy_pct: f32,
    rss: u64,
}

/// The last pass, keyed by pid.
static TABLE: LazyLock<RwLock<HashMap<u32, ProcInfo>>> = LazyLock::new(|| RwLock::new(HashMap::new()));
/// When the last pass sampled, for the CPU share.
static SAMPLED_AT: Mutex<Option<Instant>> = Mutex::new(None);
/// When each live session's tree was last busy, or first seen; pruned with the live set.
static LAST_BUSY: LazyLock<Mutex<HashMap<String, DateTime<Utc>>>> = LazyLock::new(|| Mutex::new(HashMap::new()));

/// Runs forever. Identity is pid plus start time on every platform, R4.
pub async fn run(config: Arc<Config>, roster: Arc<Roster>) {
    let names = harness_names(&config);
    let interval = Duration::from_millis(config.sources.scan_interval_ms.max(200));
    let mut sys = System::new();
    loop {
        sys = tokio::task::spawn_blocking(move || {
            refresh(&mut sys);
            sys
        })
        .await
        .unwrap_or_else(|_| System::new());
        pass(&roster, &names).await;
        tokio::time::sleep(interval).await;
    }
}

fn refresh_kind() -> ProcessRefreshKind {
    ProcessRefreshKind::nothing()
        .with_cmd(UpdateKind::OnlyIfNotSet)
        .with_exe(UpdateKind::OnlyIfNotSet)
        .with_cwd(UpdateKind::OnlyIfNotSet)
        .with_cpu()
        .with_memory()
}

/// One enumeration into `TABLE`.
fn refresh(sys: &mut System) {
    sys.refresh_processes_specifics(ProcessesToUpdate::All, true, refresh_kind());
    let elapsed_ms = SAMPLED_AT.lock().unwrap().replace(Instant::now()).map(|t| t.elapsed().as_millis() as f32);
    let prev = TABLE.read().unwrap();
    let table = sys
        .processes()
        .iter()
        .map(|(pid, p)| {
            let cpu_ms = p.accumulated_cpu_time();
            let before = prev.get(&pid.as_u32()).filter(|b| b.start_ticks == p.start_time()).map(|b| b.cpu_ms);
            let info = ProcInfo {
                ppid: p.parent().map(|p| p.as_u32()),
                start_ticks: p.start_time(),
                name: p.name().to_string_lossy().into_owned(),
                exe: p.exe().map(|e| e.to_string_lossy().into_owned()),
                cmd: p.cmd().iter().map(|c| c.to_string_lossy().into_owned()).collect(),
                cwd: p.cwd().map(|c| c.to_string_lossy().into_owned()),
                cpu_ms,
                rss: p.memory(),
                busy_pct: match (before, elapsed_ms) {
                    (Some(before), Some(elapsed)) if elapsed > 0.0 => cpu_ms.saturating_sub(before) as f32 * 100.0 / elapsed,
                    _ => 0.0,
                },
            };
            (pid.as_u32(), info)
        })
        .collect();
    drop(prev);
    *TABLE.write().unwrap() = table;
}

/// CPU share and resident memory of a process and everything under it, from the last pass.
fn tree_load(table: &[(u32, ProcInfo)]) -> HashMap<u32, (f32, u64)> {
    let mut tree: HashMap<u32, (f32, u64)> = HashMap::new();
    for (pid, info) in table {
        for member in chain_of(*pid) {
            let t = tree.entry(member).or_default();
            t.0 += info.busy_pct;
            t.1 += info.rss;
        }
    }
    tree
}

/// The activity a scan-only session's CPU share earns, R5.2 fallback: busy is active now, quiet
/// for `QUIET` is idle since the tree was last busy. None when the record already says so.
fn cpu_activity(current: Activity, busy: bool, last_busy: DateTime<Utc>, now: DateTime<Utc>) -> Option<(Activity, DateTime<Utc>)> {
    match (busy, current) {
        (true, Activity::Active) | (false, Activity::Idle) => None,
        (true, _) => Some((Activity::Active, now)),
        (false, _) if now - last_busy >= QUIET => Some((Activity::Idle, last_busy)),
        _ => None,
    }
}

/// The platform start time of a live process, the `start_ticks` half of a session key. None when
/// the process is gone. Reads the process directly, so it works before the first scan pass.
pub fn start_ticks(pid: u32) -> Option<u64> {
    if let Some(p) = TABLE.read().unwrap().get(&pid) {
        return Some(p.start_ticks);
    }
    let mut sys = System::new();
    sys.refresh_processes_specifics(ProcessesToUpdate::Some(&[Pid::from_u32(pid)]), true, ProcessRefreshKind::nothing());
    sys.process(Pid::from_u32(pid)).map(|p| p.start_time())
}

/// Parent chain of a process, nearest first, from the last scan pass.
pub fn ancestors(pid: u32) -> Vec<u32> {
    let table = TABLE.read().unwrap();
    let mut chain = Vec::new();
    let mut cur = pid;
    while let Some(parent) = table.get(&cur).and_then(|p| p.ppid) {
        if parent == 0 || parent == cur || chain.contains(&parent) || chain.len() >= 64 {
            break;
        }
        chain.push(parent);
        cur = parent;
    }
    chain
}

/// Whether a pid is alive with the given start ticks.
pub fn is_alive(pid: u32, start_ticks: u64) -> bool {
    self::start_ticks(pid) == Some(start_ticks)
}

/// Process name to harness label: the builtin four plus every `[harness.<key>]` and its adapter.
fn harness_names(config: &Config) -> BTreeMap<String, String> {
    let mut names: BTreeMap<String, String> = BUILTIN.iter().map(|(n, l)| (n.to_string(), l.to_string())).collect();
    for (key, h) in &config.harness {
        names.insert(key.clone(), key.clone());
        if let Some(adapter) = basename(&h.adapter) {
            names.insert(adapter, key.clone());
        }
    }
    names
}

fn basename(path: &str) -> Option<String> {
    Path::new(path).file_name().map(|n| n.to_string_lossy().into_owned()).filter(|n| !n.is_empty())
}

/// The harness label of a process, from its name, exe, or first argument; the script argument
/// of an interpreter counts too (`node .../claude`), and so does the directory of a binary that
/// is only a version number (`ccd-cli/2.1.270`).
fn harness_of(names: &BTreeMap<String, String>, info: &ProcInfo) -> Option<String> {
    let mut candidates = vec![info.name.clone()];
    candidates.extend(info.exe.as_deref().and_then(basename));
    candidates.extend(info.cmd.first().and_then(|c| basename(c)));
    if info.cmd.first().and_then(|c| basename(c)).is_some_and(|c| matches!(c.as_str(), "node" | "bun" | "deno")) {
        candidates.extend(info.cmd.get(1).and_then(|c| basename(c)));
    }
    for path in info.exe.iter().chain(info.cmd.first()) {
        let path = Path::new(path);
        let versioned = basename(&path.to_string_lossy()).is_some_and(|n| n.split('.').all(|part| !part.is_empty() && part.bytes().all(|b| b.is_ascii_digit())));
        if versioned {
            candidates.extend(path.parent().and_then(|d| basename(&d.to_string_lossy())));
        }
    }
    candidates.iter().find_map(|c| names.get(c).cloned())
}

async fn pass(roster: &Roster, names: &BTreeMap<String, String>) {
    let now = Utc::now();
    let mut table: Vec<(u32, ProcInfo)> = TABLE.read().unwrap().iter().map(|(p, i)| (*p, i.clone())).collect();
    // Parents before children, so a nested harness finds its root already in the roster.
    table.sort_by_key(|(pid, info)| (info.start_ticks, *pid));

    // Strays: every harness pid the roster has not seen. The roster's collapse rule decides
    // whether it becomes a record, R3.
    for (pid, info) in &table {
        let Some(harness) = harness_of(names, info) else { continue };
        if roster.resolve_key(*pid, info.start_ticks).is_some() {
            continue;
        }
        let patch = Patch {
            pid: Some(*pid),
            start_ticks: Some(info.start_ticks),
            started_at: Utc.timestamp_opt(info.start_ticks as i64, 0).single(),
            harness: Some(harness),
            lane: Some(Lane::Interactive),
            cwd: info.cwd.clone(),
            ..Patch::default()
        };
        if let Err(error) = roster.apply(Source::Scan, patch) {
            tracing::debug!(pid, %error, "scan registration refused");
        }
    }

    // Liveness: a pid that is gone ended the session, R2.2. Idle at the time means it exited;
    // anything else is a crash. `end` keeps an earlier reason (a holder's exit, a DELETE).
    let mut live: Vec<Record> =
        roster.snapshot().records.iter().filter(|r| r.liveness != Liveness::Ended).cloned().collect();
    live.retain(|rec| {
        if is_alive(rec.pid, rec.start_ticks) {
            return true;
        }
        let reason = if rec.activity == Activity::Idle { EndedReason::Exit } else { EndedReason::Crash };
        let _ = roster.end(&rec.session_key, reason, now);
        false
    });

    // Activity from CPU for the sessions nothing better speaks for, R5.2. A claim by any other
    // source hands the record over to it: hook and adapter claims outrank a scan.
    let tree = tree_load(&table);
    let loads: HashMap<String, Load> = live
        .iter()
        .filter_map(|r| tree.get(&r.pid).map(|(cpu, rss)| (r.session_key.clone(), Load { cpu_pct: cpu.round() as u16, rss_mb: rss >> 20 })))
        .collect();
    roster.set_load(&loads);
    let claims: Vec<(String, Activity, DateTime<Utc>)> = {
        let mut last_busy = LAST_BUSY.lock().unwrap();
        last_busy.retain(|key, _| live.iter().any(|r| &r.session_key == key));
        live.iter()
            .filter(|r| r.sources.iter().all(|s| *s == Source::Scan))
            .filter_map(|rec| {
                let busy = tree.get(&rec.pid).is_some_and(|(pct, _)| *pct >= BUSY_PCT);
                let since = *last_busy.entry(rec.session_key.clone()).or_insert(now);
                if busy {
                    last_busy.insert(rec.session_key.clone(), now);
                }
                cpu_activity(rec.activity, busy, since, now).map(|(activity, at)| (rec.session_key.clone(), activity, at))
            })
            .collect()
    };
    // No event name: what the tree did is not known, only that it did something; `explain`
    // carries the source.
    for (key, activity, at) in claims {
        let _ = roster.claim(Source::Scan, &key, activity, "", at);
    }

    // parent_session_key from the process tree when the parent is also a harness, R3.
    for rec in live.iter().filter(|r| r.parent_session_key.is_none()) {
        let parent = ancestors(rec.pid)
            .into_iter()
            .find_map(|a| live.iter().find(|p| p.pid == a && p.session_key != rec.session_key));
        if let Some(parent) = parent {
            let patch = Patch {
                session_key: Some(rec.session_key.clone()),
                parent_session_key: Some(parent.session_key.clone()),
                ..Patch::default()
            };
            let _ = roster.apply(Source::Scan, patch);
        }
    }

    // Runtime handles, R4: one tmux list-panes and one herdr pane list per pass, only when a
    // live record still lacks the handle.
    let tmux_present = on_path("tmux");
    if tmux_present && live.iter().any(|r| r.tmux.is_none() || r.tty.is_none()) {
        let panes = tmux_panes().await;
        for rec in live.iter().filter(|r| r.tmux.is_none() || r.tty.is_none()) {
            let chain = chain_of(rec.pid);
            if let Some(pane) = owning_pane(&chain, &panes) {
                // ponytail: tty comes from tmux's pane_tty, not from the process (sysinfo has no
                // tty); read it through libproc/procfs if a session outside tmux ever needs it.
                let patch = Patch {
                    session_key: Some(rec.session_key.clone()),
                    tmux: Some(pane.handle.clone()),
                    tty: pane.tty.clone(),
                    ..Patch::default()
                };
                let _ = roster.apply(Source::Scan, patch);
            }
        }
    }
    let sockets = herdr_sockets();
    if !sockets.is_empty() && live.iter().any(|r| r.herdr.is_none()) {
        let panes = herdr_panes(&sockets).await;
        for rec in live.iter().filter(|r| r.herdr.is_none()) {
            let chain = chain_of(rec.pid);
            if let Some(pane) = panes.iter().find(|p| chain.iter().any(|pid| p.pids.contains(pid))) {
                let patch = Patch {
                    session_key: Some(rec.session_key.clone()),
                    herdr: Some(pane.handle.clone()),
                    tty: pane.tty.clone(),
                    ..Patch::default()
                };
                let _ = roster.apply(Source::Scan, patch);
            }
        }
    }

    let mut capabilities = roster.snapshot().capabilities.clone();
    capabilities.tmux = tmux_present;
    capabilities.herdr = !sockets.is_empty();
    roster.set_capabilities(capabilities);

    roster.sweep(now - chrono::Duration::from_std(KEEP_ENDED).unwrap_or_default());
}

/// A pid then its ancestors, nearest first.
fn chain_of(pid: u32) -> Vec<u32> {
    let mut chain = vec![pid];
    chain.extend(ancestors(pid));
    chain
}

fn on_path(bin: &str) -> bool {
    std::env::var_os("PATH")
        .map(|path| std::env::split_paths(&path).any(|dir| dir.join(bin).is_file()))
        .unwrap_or(false)
}

// ---- tmux ---------------------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq)]
struct TmuxPane {
    handle: TmuxHandle,
    pane_pid: u32,
    tty: Option<String>,
}

/// One line of `tmux list-panes -a -F TMUX_FORMAT`.
fn parse_tmux_line(line: &str) -> Option<TmuxPane> {
    let f: Vec<&str> = line.split('\t').collect();
    if f.len() < 6 {
        return None;
    }
    Some(TmuxPane {
        handle: TmuxHandle {
            session: f[0].to_string(),
            window_index: f[1].parse().ok()?,
            window_name: Some(f[2].to_string()).filter(|n| !n.is_empty()),
            pane_id: f[3].to_string(),
        },
        pane_pid: f[4].parse().ok()?,
        tty: Some(f[5].to_string()).filter(|t| !t.is_empty()),
    })
}

/// The pane whose shell is nearest in the chain (the pid itself, then each ancestor).
fn owning_pane<'a>(chain: &[u32], panes: &'a [TmuxPane]) -> Option<&'a TmuxPane> {
    chain.iter().find_map(|pid| panes.iter().find(|p| p.pane_pid == *pid))
}

async fn tmux_panes() -> Vec<TmuxPane> {
    let output = tokio::time::timeout(
        HANDLE_TIMEOUT,
        tokio::process::Command::new("tmux").args(["list-panes", "-a", "-F", TMUX_FORMAT]).output(),
    )
    .await;
    match output {
        Ok(Ok(out)) if out.status.success() => {
            String::from_utf8_lossy(&out.stdout).lines().filter_map(parse_tmux_line).collect()
        }
        // No server running, or tmux missing: no panes this pass.
        _ => Vec::new(),
    }
}

// ---- herdr --------------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct HerdrPane {
    handle: HerdrHandle,
    /// The pane's shell and its foreground processes.
    pids: Vec<u32>,
    tty: Option<String>,
}

/// Herdr sockets on this machine: `HERDR_SOCKET_PATH`, the default session's socket, and every
/// named session under `~/.config/herdr/sessions/<name>/herdr.sock`.
fn herdr_sockets() -> Vec<(String, PathBuf)> {
    let mut found: Vec<(String, PathBuf)> = Vec::new();
    if let Some(p) = std::env::var_os("HERDR_SOCKET_PATH") {
        found.push(("default".into(), PathBuf::from(p)));
    }
    if let Some(home) = dirs::home_dir() {
        let root = home.join(".config").join("herdr");
        found.push(("default".into(), root.join("herdr.sock")));
        if let Ok(sessions) = std::fs::read_dir(root.join("sessions")) {
            for entry in sessions.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                found.push((name, entry.path().join("herdr.sock")));
            }
        }
    }
    found.dedup_by(|a, b| a.1 == b.1);
    found.retain(|(_, p)| p.exists());
    found
}

static HERDR_LOGGED: AtomicBool = AtomicBool::new(false);

/// Every pane of every herdr session with the pids under it. Best effort: an error skips the
/// session this pass and is logged once.
async fn herdr_panes(sockets: &[(String, PathBuf)]) -> Vec<HerdrPane> {
    let mut panes = Vec::new();
    for (session, socket) in sockets {
        match herdr_session_panes(session, socket).await {
            Ok(found) => panes.extend(found),
            Err(error) => {
                if !HERDR_LOGGED.swap(true, Ordering::Relaxed) {
                    tracing::warn!(%session, socket = %socket.display(), %error, "herdr pane list failed; skipping");
                }
            }
        }
    }
    panes
}

async fn herdr_session_panes(session: &str, socket: &Path) -> anyhow::Result<Vec<HerdrPane>> {
    let list = herdr_call(socket, "pane.list", serde_json::json!({})).await?;
    let mut panes = Vec::new();
    for pane in list["panes"].as_array().into_iter().flatten() {
        let Some(pane_id) = pane["pane_id"].as_str() else { continue };
        let info = herdr_call(socket, "pane.process_info", serde_json::json!({ "pane_id": pane_id })).await?;
        let info = &info["process_info"];
        let mut pids: Vec<u32> = info["shell_pid"].as_u64().map(|p| p as u32).into_iter().collect();
        pids.extend(
            info["foreground_processes"].as_array().into_iter().flatten().filter_map(|p| p["pid"].as_u64()).map(|p| p as u32),
        );
        panes.push(HerdrPane {
            handle: HerdrHandle {
                session: session.to_string(),
                workspace_id: pane["workspace_id"].as_str().unwrap_or_default().to_string(),
                pane_id: pane_id.to_string(),
                agent_name: pane["agent"].as_str().or(pane["display_agent"].as_str()).map(str::to_string),
            },
            pids,
            tty: info["tty"].as_str().map(str::to_string),
        });
    }
    Ok(panes)
}

/// One request on Herdr's socket, section 38.1 of the workspace spec: newline delimited JSON,
/// `{id, method, params}` with a string id, snake_case params, reply by id, error body
/// `{code, message}`. Herdr 0.8.2 closes the connection after one reply, so each call connects.
#[cfg(unix)]
async fn herdr_call(socket: &Path, method: &str, params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    tokio::time::timeout(HANDLE_TIMEOUT, async {
        let mut stream = tokio::net::UnixStream::connect(socket).await?;
        let request = serde_json::json!({ "id": "1", "method": method, "params": params });
        stream.write_all(format!("{request}\n").as_bytes()).await?;
        let mut reader = BufReader::new(stream);
        let mut line = String::new();
        loop {
            line.clear();
            if reader.read_line(&mut line).await? == 0 {
                anyhow::bail!("herdr closed the socket before answering {method}");
            }
            let mut reply: serde_json::Value = serde_json::from_str(&line)?;
            if reply["id"].as_str() != Some("1") {
                continue; // a pushed event, not our reply
            }
            if let Some(error) = reply.get("error") {
                anyhow::bail!("herdr {method}: {error}");
            }
            return Ok(reply["result"].take());
        }
    })
    .await
    .map_err(|_| anyhow::anyhow!("herdr {method} timed out"))?
}

/// R11: no tmux or herdr handles on Windows.
#[cfg(not(unix))]
async fn herdr_call(_socket: &Path, method: &str, _params: serde_json::Value) -> anyhow::Result<serde_json::Value> {
    anyhow::bail!("herdr {method}: no unix socket on this platform")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cpu_share_claims_active_now_and_idle_since_the_last_busy_pass() {
        let t0 = Utc.timestamp_opt(1_000, 0).unwrap();
        let later = t0 + QUIET;
        assert_eq!(cpu_activity(Activity::Unknown, true, t0, t0), Some((Activity::Active, t0)));
        assert_eq!(cpu_activity(Activity::Active, true, t0, later), None);
        assert_eq!(cpu_activity(Activity::Active, false, t0, t0 + chrono::Duration::seconds(5)), None);
        assert_eq!(cpu_activity(Activity::Active, false, t0, later), Some((Activity::Idle, t0)));
        assert_eq!(cpu_activity(Activity::Unknown, false, t0, later), Some((Activity::Idle, t0)));
        assert_eq!(cpu_activity(Activity::Idle, false, t0, later), None);
    }

    fn info(name: &str, exe: Option<&str>, cmd: &[&str]) -> ProcInfo {
        ProcInfo {
            ppid: None,
            start_ticks: 1,
            name: name.into(),
            exe: exe.map(Into::into),
            cmd: cmd.iter().map(|c| c.to_string()).collect(),
            cwd: None,
            cpu_ms: 0,
            busy_pct: 0.0,
            rss: 0,
        }
    }

    #[test]
    fn harnesses_are_detected_by_name_exe_or_argument() {
        let mut config = Config::default();
        config.harness.insert("pi".into(), crate::config::HarnessConfig { adapter: "/opt/pi/pi-acp".into(), ..Default::default() });
        let names = harness_names(&config);
        assert_eq!(harness_of(&names, &info("claude", Some("/x/versions/2.1.0"), &["claude", "--resume"])).as_deref(), Some("claude"));
        assert_eq!(harness_of(&names, &info("claude-agent-acp", None, &[])).as_deref(), Some("claude"));
        assert_eq!(harness_of(&names, &info("codex-acp", None, &[])).as_deref(), Some("codex"));
        assert_eq!(harness_of(&names, &info("node", Some("/usr/bin/node"), &["node", "/home/u/.local/bin/claude"])).as_deref(), Some("claude"));
        assert_eq!(harness_of(&names, &info("x", Some("/opt/pi/pi-acp"), &[])).as_deref(), Some("pi"));
        assert_eq!(harness_of(&names, &info("pi", None, &[])).as_deref(), Some("pi"));
        let ccd = "/Users/u/.claude/remote/ccd-cli/2.1.270";
        assert_eq!(harness_of(&names, &info("2.1.270", Some(ccd), &[ccd, "--output-format", "stream-json"])).as_deref(), Some("claude"), "the desktop app's Claude Code");
        assert_eq!(harness_of(&names, &info("2.1.270", Some("/opt/claude/2.1.270"), &[])).as_deref(), Some("claude"));
        assert_eq!(harness_of(&names, &info("server", Some("/Users/u/projects/claude/server"), &[])), None, "a directory names the harness only for a versioned binary");
        assert_eq!(harness_of(&names, &info("zsh", Some("/bin/zsh"), &["-zsh"])), None);
        assert_eq!(harness_of(&names, &info("node", None, &["node", "server.js", "claude"])), None, "only the script argument counts");
    }

    #[test]
    fn tmux_lines_parse_and_the_nearest_pane_owns_the_process() {
        let line = "main\t2\teditor\t%5\t4242\t/dev/ttys003";
        let pane = parse_tmux_line(line).unwrap();
        assert_eq!(pane.handle, TmuxHandle { session: "main".into(), window_index: 2, window_name: Some("editor".into()), pane_id: "%5".into() });
        assert_eq!((pane.pane_pid, pane.tty.as_deref()), (4242, Some("/dev/ttys003")));
        assert_eq!(parse_tmux_line("main\t2\t\t%6\t4343\t").unwrap().handle.window_name, None);
        assert_eq!(parse_tmux_line("main\tx\t\t%6\t4343\t"), None);
        assert_eq!(parse_tmux_line("short\tline"), None);
        let other = parse_tmux_line("main\t3\t\t%7\t5000\t/dev/ttys004").unwrap();
        let panes = vec![pane.clone(), other.clone()];
        // A harness two levels under the pane shell: pane_pid is an ancestor, not the pid.
        assert_eq!(owning_pane(&[9000, 8000, 4242, 1], &panes), Some(&pane));
        // Nearest wins when both are in the chain (a nested tmux).
        assert_eq!(owning_pane(&[9000, 5000, 4242, 1], &panes), Some(&other));
        assert_eq!(owning_pane(&[9000, 1], &panes), None);
    }

    #[test]
    fn a_live_scan_sees_this_process() {
        let mut sys = System::new();
        refresh(&mut sys);
        let me = std::process::id();
        let ticks = start_ticks(me).expect("the test binary has a start time");
        assert!(is_alive(me, ticks));
        assert!(!is_alive(me, ticks + 1));
        assert!(!ancestors(me).is_empty(), "the test runner is at least one parent");
        assert!(!is_alive(u32::MAX - 1, ticks));
        // A direct read, before any pass, answers for a pid the table has not seen.
        TABLE.write().unwrap().clear();
        assert_eq!(start_ticks(me), Some(ticks));
        assert_eq!(start_ticks(u32::MAX - 1), None);
    }
}
