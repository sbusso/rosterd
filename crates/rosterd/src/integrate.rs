//! R14.3 `rosterd integrate`: hook declarations for Claude Code and Codex (only marked entries
//! are ours, everything else is kept, atomic writes),
//! the pi extension of R16.2, and the adapter lock file of R10. Install and uninstall write only
//! when the bytes would change, so a second run changes nothing, A14 acceptance 8.

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::config::{Config, config_dir};

/// The extension `install pi` writes, embedded so the binary is self contained.
pub const PI_EXTENSION: &str = include_str!("../../../extensions/rosterd-pi.ts");

/// The marker of 43.3 rule 5: an entry whose command is `rosterd-hook` is ours.
const HOOK_COMMAND: &str = "rosterd-hook";
const CLAUDE_EVENTS: &[&str] = &["SessionStart", "UserPromptSubmit", "PreToolUse", "PermissionRequest", "Notification", "Stop"];
/// Codex raises PermissionRequest instead of Notification.
const CODEX_EVENTS: &[&str] = &["SessionStart", "UserPromptSubmit", "PreToolUse", "PermissionRequest", "Stop"];

#[derive(Clone, Copy, Debug, PartialEq, Eq, clap::ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum Target {
    Claude,
    Codex,
    Pi,
}

impl Target {
    pub const ALL: [Target; 3] = [Target::Claude, Target::Codex, Target::Pi];

    pub fn name(self) -> &'static str {
        match self {
            Target::Claude => "claude",
            Target::Codex => "codex",
            Target::Pi => "pi",
        }
    }

    /// The default adapter of R10 and R16.4, when the config names none.
    fn default_adapter(self) -> &'static str {
        match self {
            Target::Claude => "claude-agent-acp",
            Target::Codex => "codex-acp",
            Target::Pi => "pi-acp",
        }
    }
}

/// Where each harness reads its integration and where binaries are looked up. Resolved from the
/// environment: `HOME`, `PI_CODING_AGENT_DIR` (pi's own override of `~/.pi/agent`),
/// `ROSTERD_CONFIG_DIR`, `PATH`. Tests build one over temp dirs instead of touching the environment.
#[derive(Debug, Clone)]
pub struct Paths {
    pub claude_settings: PathBuf,
    pub codex_hooks: PathBuf,
    pub pi_extension: PathBuf,
    /// `<config dir>/adapters.lock`, R10: one entry per harness, the pi adapter by commit, R16.1.
    pub lock: PathBuf,
    /// The PATH binaries are searched on.
    pub search_path: OsString,
}

impl Paths {
    pub fn resolve() -> Paths {
        let home = dirs::home_dir().unwrap_or_else(|| PathBuf::from("."));
        let pi_agent = std::env::var_os("PI_CODING_AGENT_DIR").map(PathBuf::from).unwrap_or_else(|| home.join(".pi").join("agent"));
        Paths {
            claude_settings: home.join(".claude").join("settings.json"),
            codex_hooks: home.join(".codex").join("hooks.json"),
            pi_extension: pi_agent.join("extensions").join("rosterd-pi.ts"),
            lock: config_dir().join("adapters.lock"),
            search_path: std::env::var_os("PATH").unwrap_or_default(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct HarnessStatus {
    pub harness: String,
    /// Path of the harness binary on PATH.
    pub binary: Option<String>,
    pub version: Option<String>,
    /// Path of the adapter binary on PATH.
    pub adapter: Option<String>,
    /// The pinned commit: the lock file's, else the config's.
    pub adapter_commit: Option<String>,
    /// Hooks or extension present.
    pub installed: bool,
    /// Present and equal to what `install` would write.
    pub current: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct LockEntry {
    adapter: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    commit: Option<String>,
    installed_at: String,
}

type Lock = BTreeMap<String, LockEntry>;

pub fn install(target: Target, config: &Config) -> Result<()> {
    install_at(target, config, &Paths::resolve())
}

pub fn uninstall(target: Target, config: &Config) -> Result<()> {
    uninstall_at(target, config, &Paths::resolve())
}

pub fn status(config: &Config) -> Vec<HarnessStatus> {
    status_at(config, &Paths::resolve(), true)
}

/// R16.1: the runner warns when a pi session under attention has no extension.
pub fn pi_extension_installed() -> bool {
    std::fs::read(Paths::resolve().pi_extension).is_ok_and(|bytes| bytes == PI_EXTENSION.as_bytes())
}

pub fn install_at(target: Target, config: &Config, paths: &Paths) -> Result<()> {
    match target {
        Target::Claude => merge_hooks(&paths.claude_settings, CLAUDE_EVENTS)?,
        Target::Codex => merge_hooks(&paths.codex_hooks, CODEX_EVENTS)?,
        Target::Pi => write_if_changed(&paths.pi_extension, PI_EXTENSION.as_bytes())?,
    }
    let mut lock = read_lock(&paths.lock)?;
    let harness = config.harness.get(target.name());
    let adapter = adapter_name(target, config);
    let commit = harness.and_then(|h| h.adapter_commit.clone());
    let same = lock.get(target.name()).is_some_and(|e| e.adapter == adapter && e.commit == commit);
    if !same {
        let installed_at = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Secs, true);
        lock.insert(target.name().into(), LockEntry { adapter, commit, installed_at });
    }
    write_lock(&paths.lock, &lock)
}

pub fn uninstall_at(target: Target, _config: &Config, paths: &Paths) -> Result<()> {
    match target {
        Target::Claude => remove_hooks(&paths.claude_settings)?,
        Target::Codex => remove_hooks(&paths.codex_hooks)?,
        Target::Pi => {
            if paths.pi_extension.exists() {
                std::fs::remove_file(&paths.pi_extension).with_context(|| format!("remove {}", paths.pi_extension.display()))?;
            }
        }
    }
    let mut lock = read_lock(&paths.lock)?;
    if lock.remove(target.name()).is_some() {
        write_lock(&paths.lock, &lock)?;
    }
    Ok(())
}

/// `versions` runs each binary's `--version`; doctor passes false, since a harness may write
/// under HOME when run (codex does) and doctor changes nothing.
pub fn status_at(config: &Config, paths: &Paths, versions: bool) -> Vec<HarnessStatus> {
    let lock = read_lock(&paths.lock).unwrap_or_default();
    Target::ALL
        .into_iter()
        .map(|target| {
            let name = target.name();
            let binary = which(&paths.search_path, name);
            let (installed, current) = match target {
                Target::Claude => hooks_state(&paths.claude_settings, CLAUDE_EVENTS),
                Target::Codex => hooks_state(&paths.codex_hooks, CODEX_EVENTS),
                Target::Pi => match std::fs::read(&paths.pi_extension) {
                    Ok(bytes) => (true, bytes == PI_EXTENSION.as_bytes()),
                    Err(_) => (false, false),
                },
            };
            HarnessStatus {
                harness: name.into(),
                version: binary.as_deref().filter(|_| versions).and_then(version_of),
                binary: binary.map(|p| p.display().to_string()),
                adapter: which(&paths.search_path, &adapter_name(target, config)).map(|p| p.display().to_string()),
                adapter_commit: lock
                    .get(name)
                    .and_then(|e| e.commit.clone())
                    .or_else(|| config.harness.get(name).and_then(|h| h.adapter_commit.clone())),
                installed,
                current,
            }
        })
        .collect()
}

/// The adapter the config names for a harness, else the default of R10/R16.4.
pub fn adapter_name(target: Target, config: &Config) -> String {
    config
        .harness
        .get(target.name())
        .map(|h| h.adapter.clone())
        .filter(|a| !a.is_empty())
        .unwrap_or_else(|| target.default_adapter().into())
}

// Hooks files, 43.3 rule 5.

fn hook_entry(event: &str) -> Value {
    let hooks = json!([{ "type": "command", "command": HOOK_COMMAND }]);
    if event == "PreToolUse" { json!({ "matcher": "", "hooks": hooks }) } else { json!({ "hooks": hooks }) }
}

/// An entry is ours when any of its commands is `rosterd-hook`, bare or by path.
fn is_marked(entry: &Value) -> bool {
    entry.get("hooks").and_then(Value::as_array).is_some_and(|hooks| {
        hooks.iter().any(|h| {
            h.get("command")
                .and_then(Value::as_str)
                .is_some_and(|c| c == HOOK_COMMAND || c.ends_with(&format!("/{HOOK_COMMAND}")))
        })
    })
}

fn read_hooks_file(path: &Path) -> Result<Value> {
    match std::fs::read(path) {
        Ok(bytes) => {
            let value: Value = serde_json::from_slice(&bytes).with_context(|| format!("parse {}", path.display()))?;
            anyhow::ensure!(value.is_object(), "{} is not a JSON object", path.display());
            Ok(value)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(json!({})),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

fn merge_hooks(path: &Path, events: &[&str]) -> Result<()> {
    let mut root = read_hooks_file(path)?;
    let hooks = root.as_object_mut().expect("checked").entry("hooks").or_insert_with(|| json!({}));
    anyhow::ensure!(hooks.is_object(), "{}: \"hooks\" is not an object", path.display());
    for event in events {
        let wanted = hook_entry(event);
        let list = hooks.as_object_mut().expect("checked").entry(*event).or_insert_with(|| json!([]));
        anyhow::ensure!(list.is_array(), "{}: hooks.{event} is not an array", path.display());
        let list = list.as_array_mut().expect("checked");
        // One entry of ours per event, where the first marked one was; other entries keep their place.
        let position = list.iter().position(is_marked).unwrap_or(list.len());
        list.retain(|entry| !is_marked(entry));
        list.insert(position.min(list.len()), wanted);
    }
    write_json(path, &root)
}

fn remove_hooks(path: &Path) -> Result<()> {
    let mut root = read_hooks_file(path)?;
    let Some(hooks) = root.get_mut("hooks").and_then(Value::as_object_mut) else { return Ok(()) };
    let mut removed = false;
    for list in hooks.values_mut() {
        if let Some(list) = list.as_array_mut() {
            let before = list.len();
            list.retain(|entry| !is_marked(entry));
            removed |= list.len() != before;
        }
    }
    if !removed {
        return Ok(()); // nothing of ours: the file is not even reformatted
    }
    hooks.retain(|_, list| list.as_array().is_none_or(|l| !l.is_empty()));
    if hooks.is_empty() {
        root.as_object_mut().expect("checked").remove("hooks");
    }
    write_json(path, &root)
}

/// (installed, current): every event has a marked entry; every marked entry is the current one.
fn hooks_state(path: &Path, events: &[&str]) -> (bool, bool) {
    let Ok(root) = read_hooks_file(path) else { return (false, false) };
    let Some(hooks) = root.get("hooks").and_then(Value::as_object) else { return (false, false) };
    let marked = |event: &str| hooks.get(event).and_then(Value::as_array).into_iter().flatten().filter(|e| is_marked(e)).collect::<Vec<_>>();
    let installed = events.iter().any(|event| !marked(event).is_empty());
    let current = events.iter().all(|event| {
        let entries = marked(event);
        !entries.is_empty() && entries.iter().all(|e| **e == hook_entry(event))
    });
    (installed, current)
}

// Files.

fn write_json(path: &Path, value: &Value) -> Result<()> {
    let mut bytes = serde_json::to_vec_pretty(value)?;
    bytes.push(b'\n');
    write_if_changed(path, &bytes)
}

/// Writes through a sibling temp file and a rename; nothing when the bytes are already there.
fn write_if_changed(path: &Path, bytes: &[u8]) -> Result<()> {
    if std::fs::read(path).is_ok_and(|current| current == bytes) {
        return Ok(());
    }
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    std::fs::create_dir_all(parent).with_context(|| format!("create {}", parent.display()))?;
    let tmp = parent.join(format!(".{}.rosterd-tmp", path.file_name().and_then(|n| n.to_str()).unwrap_or("file")));
    std::fs::write(&tmp, bytes).with_context(|| format!("write {}", tmp.display()))?;
    std::fs::rename(&tmp, path).with_context(|| format!("rename {} to {}", tmp.display(), path.display()))
}

fn read_lock(path: &Path) -> Result<Lock> {
    match std::fs::read_to_string(path) {
        Ok(text) => toml::from_str(&text).with_context(|| format!("parse {}", path.display())),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Lock::new()),
        Err(e) => Err(e).with_context(|| format!("read {}", path.display())),
    }
}

fn write_lock(path: &Path, lock: &Lock) -> Result<()> {
    if lock.is_empty() {
        if path.exists() {
            std::fs::remove_file(path).with_context(|| format!("remove {}", path.display()))?;
        }
        return Ok(());
    }
    write_if_changed(path, toml::to_string(lock)?.as_bytes())
}

// Binaries.

/// The first `name` on `path` that is an executable file.
pub fn which(path: &OsStr, name: &str) -> Option<PathBuf> {
    std::env::split_paths(path).map(|dir| dir.join(name)).find(|candidate| is_executable(candidate))
}

fn is_executable(path: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(path) else { return false };
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.is_file() && meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        meta.is_file()
    }
}

/// First line of `<bin> --version`, or None after 2 s.
pub fn version_of(bin: &Path) -> Option<String> {
    run_bounded(std::process::Command::new(bin).arg("--version"), Duration::from_secs(2))
        .and_then(|out| out.lines().next().map(|l| l.trim().to_string()))
        .filter(|l| !l.is_empty())
}

/// Stdout of a command that finishes within `limit`; killed and None otherwise.
pub fn run_bounded(command: &mut std::process::Command, limit: Duration) -> Option<String> {
    let mut child = command.stdin(std::process::Stdio::null()).stdout(std::process::Stdio::piped()).stderr(std::process::Stdio::null()).spawn().ok()?;
    let started = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) if status.success() => break,
            Ok(Some(_)) => return None,
            Ok(None) if started.elapsed() < limit => std::thread::sleep(Duration::from_millis(25)),
            _ => {
                let _ = child.kill();
                let _ = child.wait();
                return None;
            }
        }
    }
    // ponytail: read after exit; a `--version` never fills the pipe.
    let mut out = String::new();
    std::io::Read::read_to_string(&mut child.stdout.take()?, &mut out).ok()?;
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HarnessConfig;

    fn temp(name: &str) -> (Paths, PathBuf) {
        let dir = std::env::temp_dir().join(format!("rosterd-integrate-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let paths = Paths {
            claude_settings: dir.join("claude").join("settings.json"),
            codex_hooks: dir.join("codex").join("hooks.json"),
            pi_extension: dir.join("pi").join("extensions").join("rosterd-pi.ts"),
            lock: dir.join("config").join("adapters.lock"),
            search_path: OsString::new(),
        };
        (paths, dir)
    }

    fn config() -> Config {
        let mut config = Config::default();
        config.harness.insert("pi".into(), HarnessConfig { adapter: "pi-acp".into(), adapter_commit: Some("abc123".into()), extension: true, ..Default::default() });
        config
    }

    fn snapshot(path: &Path) -> (Vec<u8>, std::time::SystemTime) {
        let meta = std::fs::metadata(path).unwrap();
        (std::fs::read(path).unwrap(), meta.modified().unwrap())
    }

    #[test]
    fn claude_install_merges_marked_entries_and_is_byte_idempotent() {
        let (paths, dir) = temp("claude");
        std::fs::create_dir_all(paths.claude_settings.parent().unwrap()).unwrap();
        let original = r#"{"model":"opus","hooks":{"PreToolUse":[{"matcher":"*","hooks":[{"type":"command","command":"/x/other-hook.sh"}]}],"SubagentStop":[{"hooks":[{"type":"command","command":"/x/other-hook.sh"}]}]}}"#;
        std::fs::write(&paths.claude_settings, original).unwrap();

        install_at(Target::Claude, &config(), &paths).unwrap();
        let first = snapshot(&paths.claude_settings);
        let root: Value = serde_json::from_slice(&first.0).unwrap();
        assert_eq!(root["model"], "opus");
        let pre = root["hooks"]["PreToolUse"].as_array().unwrap();
        assert_eq!(pre.len(), 2, "the other entry is kept, ours is appended");
        assert_eq!(pre[0]["hooks"][0]["command"], "/x/other-hook.sh");
        assert_eq!(pre[1], hook_entry("PreToolUse"));
        assert_eq!(root["hooks"]["SubagentStop"].as_array().unwrap().len(), 1);
        for event in CLAUDE_EVENTS {
            assert!(root["hooks"][*event].as_array().unwrap().iter().any(is_marked), "{event} installed");
        }
        assert!(first.0.ends_with(b"}\n"));
        let lock_first = snapshot(&paths.lock);

        std::thread::sleep(Duration::from_millis(20));
        install_at(Target::Claude, &config(), &paths).unwrap();
        assert_eq!(snapshot(&paths.claude_settings), first, "second install changes nothing");
        assert_eq!(snapshot(&paths.lock), lock_first);

        let status = status_at(&config(), &paths, false);
        let claude = status.iter().find(|s| s.harness == "claude").unwrap();
        assert!(claude.installed && claude.current);
        assert!(!status.iter().find(|s| s.harness == "codex").unwrap().installed);

        uninstall_at(Target::Claude, &config(), &paths).unwrap();
        let removed = snapshot(&paths.claude_settings);
        let root: Value = serde_json::from_slice(&removed.0).unwrap();
        assert_eq!(root["hooks"]["PreToolUse"].as_array().unwrap().len(), 1);
        assert_eq!(root["hooks"]["SubagentStop"].as_array().unwrap().len(), 1);
        assert!(root["hooks"].get("SessionStart").is_none(), "an array we emptied goes away");
        assert!(!paths.lock.exists(), "an empty lock file goes away");
        std::thread::sleep(Duration::from_millis(20));
        uninstall_at(Target::Claude, &config(), &paths).unwrap();
        assert_eq!(snapshot(&paths.claude_settings), removed, "second uninstall changes nothing");

        std::fs::write(&paths.claude_settings, original).unwrap();
        uninstall_at(Target::Claude, &config(), &paths).unwrap();
        assert_eq!(std::fs::read_to_string(&paths.claude_settings).unwrap(), original, "a file without our entries is not reformatted");
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn pi_install_writes_the_extension_and_the_lock_once() {
        let (paths, dir) = temp("pi");
        install_at(Target::Pi, &config(), &paths).unwrap();
        let ext = snapshot(&paths.pi_extension);
        assert_eq!(ext.0, PI_EXTENSION.as_bytes());
        let lock = snapshot(&paths.lock);
        let text = String::from_utf8(lock.0.clone()).unwrap();
        assert!(text.contains("[pi]") && text.contains("adapter = \"pi-acp\"") && text.contains("commit = \"abc123\""), "{text}");

        std::thread::sleep(Duration::from_millis(20));
        install_at(Target::Pi, &config(), &paths).unwrap();
        assert_eq!(snapshot(&paths.pi_extension), ext);
        assert_eq!(snapshot(&paths.lock), lock, "installed_at is kept when nothing changed");
        let pi = status_at(&config(), &paths, false).into_iter().find(|s| s.harness == "pi").unwrap();
        assert!(pi.installed && pi.current);
        assert_eq!(pi.adapter_commit.as_deref(), Some("abc123"));

        std::fs::write(&paths.pi_extension, "stale").unwrap();
        let pi = status_at(&config(), &paths, false).into_iter().find(|s| s.harness == "pi").unwrap();
        assert!(pi.installed && !pi.current);

        uninstall_at(Target::Pi, &config(), &paths).unwrap();
        uninstall_at(Target::Pi, &config(), &paths).unwrap();
        assert!(!paths.pi_extension.exists() && !paths.lock.exists());
        std::fs::remove_dir_all(&dir).unwrap();
    }

    #[test]
    fn which_and_version_are_bounded() {
        let path = std::env::var_os("PATH").unwrap();
        assert!(which(&path, "sh").is_some());
        assert!(which(&path, "no-such-binary-rosterd").is_none());
        assert!(which(OsStr::new(""), "sh").is_none());
        assert!(run_bounded(std::process::Command::new("sleep").arg("5"), Duration::from_millis(100)).is_none());
        assert_eq!(run_bounded(std::process::Command::new("echo").arg("v1"), Duration::from_secs(2)).as_deref(), Some("v1\n"));
    }
}
