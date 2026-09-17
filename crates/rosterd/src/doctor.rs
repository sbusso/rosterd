//! R14.3 `rosterd doctor`: one line per check, pass or a fix command. Pure: it reads and
//! connects, runs only `tailscale ip`, and never creates or changes anything, A14 acceptance 10.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Serialize;

use crate::config::{Config, config_dir};
use crate::integrate::{self, Paths, Target, run_bounded, which};

const PROBE: Duration = Duration::from_secs(2);

#[derive(Debug, Clone, Serialize)]
pub struct Check {
    pub name: String,
    pub ok: bool,
    pub detail: String,
    pub fix: Option<String>,
}

fn check(name: &str, ok: bool, detail: impl Into<String>, fix: &str) -> Check {
    Check { name: name.into(), ok, detail: detail.into(), fix: (!ok).then(|| fix.into()) }
}

pub fn run(config: &Config) -> Vec<Check> {
    run_at(config, &config_dir(), &Paths::resolve())
}

/// `config_dir` holds node.key and swarm.json; `paths` the hooks, extension, and lock.
pub fn run_at(config: &Config, config_dir: &Path, paths: &Paths) -> Vec<Check> {
    let mut checks = Vec::new();

    let socket = config.socket_path();
    checks.push(match socket_status(&socket) {
        Ok(()) => check("socket", true, format!("answers on {}", socket.display()), ""),
        Err(e) => check("socket", false, format!("{} on {}", e, socket.display()), "rosterd daemon"),
    });

    let loopback = std::net::SocketAddr::from(([127, 0, 0, 1], config.node.loopback_port));
    let open = std::net::TcpStream::connect_timeout(&loopback, PROBE).is_ok();
    checks.push(check("loopback", open, format!("{loopback} {}", if open { "listening" } else { "not listening" }), "rosterd daemon"));

    checks.push(if config.node.listen == "tailscale" {
        match which(&paths.search_path, "tailscale") {
            None => check("tailscale", false, "tailscale not on PATH", "install Tailscale, or set node.listen = \"off\" in rosterd.toml"),
            Some(bin) => match run_bounded(std::process::Command::new(bin).args(["ip", "-4"]), PROBE)
                .and_then(|out| out.lines().next().map(|l| l.trim().to_string()))
                .filter(|ip| !ip.is_empty())
            {
                Some(ip) => check("tailscale", true, format!("{ip}:{}", config.node.port), ""),
                None => check("tailscale", false, "tailscale has no IP", "tailscale up"),
            },
        }
    } else {
        check("tailscale", true, "listener off", "")
    });

    let node_key = config_dir.join("node.key");
    checks.push(private_file("node key", &node_key, 0o600, "rosterd daemon"));
    let swarm = config_dir.join("swarm.json");
    checks.push(if swarm.exists() {
        private_file("swarm", &swarm, 0o600, "")
    } else {
        check("swarm", true, "standalone; rosterd invite or rosterd join to form one", "")
    });

    let holders = &config.runner.holder_dir;
    let holders_fix = format!("mkdir -p {0} && chmod 700 {0}", holders.display());
    checks.push(match std::fs::metadata(holders) {
        Ok(meta) if meta.is_dir() => match mode_of(&meta) {
            Some(mode) if mode != 0o700 => check("holders", false, format!("{} is mode {mode:o}, not 700", holders.display()), &holders_fix),
            _ => check("holders", true, holders.display().to_string(), ""),
        },
        _ => check("holders", false, format!("{} missing (the daemon creates it)", holders.display()), &holders_fix),
    });

    checks.push(match which(&paths.search_path, "rosterd-hook") {
        Some(p) => check("rosterd-hook", true, p.display().to_string(), ""),
        None => check("rosterd-hook", false, "not on PATH", "install -m755 scripts/rosterd-hook next to rosterd (packaging/install.sh)"),
    });

    for status in integrate::status_at(config, paths, false) {
        let target = Target::ALL.into_iter().find(|t| t.name() == status.harness).expect("status lists the three targets");
        let h = status.harness.as_str();
        checks.push(match &status.binary {
            Some(bin) => check(&format!("harness {h}"), true, bin.clone(), ""),
            None => check(&format!("harness {h}"), false, "not on PATH", &format!("npm i -g {}", harness_package(target))),
        });
        let adapter = integrate::adapter_name(target, config);
        checks.push(match &status.adapter {
            Some(path) => check(&format!("adapter {adapter}"), true, format!("{path} {}", status.adapter_commit.as_deref().unwrap_or("")), ""),
            None => check(&format!("adapter {adapter}"), false, "not on PATH", &format!("npm i -g {}", adapter_package(target, &adapter, status.adapter_commit.as_deref()))),
        });
        let what = if target == Target::Pi { "extension" } else { "hooks" };
        let detail = match (status.installed, status.current) {
            (true, true) => "installed",
            (true, false) => "installed, not current",
            _ => "not installed",
        };
        checks.push(check(&format!("{what} {h}"), status.current, detail, &format!("rosterd integrate install {h}")));
    }
    checks
}

pub(crate) fn harness_package(target: Target) -> &'static str {
    match target {
        Target::Claude => "@anthropic-ai/claude-code",
        Target::Codex => "@openai/codex",
        Target::Pi => "@earendil-works/pi-coding-agent",
    }
}

/// The published adapter packages (agentclientprotocol.com registry); the pi adapter by its
/// pinned commit when there is one, R16.1. A configured adapter of another name installs as is.
pub(crate) fn adapter_package(target: Target, adapter: &str, commit: Option<&str>) -> String {
    match (target, adapter) {
        (Target::Claude, "claude-agent-acp") => "@agentclientprotocol/claude-agent-acp".into(),
        (Target::Codex, "codex-acp") => "@agentclientprotocol/codex-acp".into(),
        (Target::Pi, "pi-acp") => match commit {
            Some(sha) => format!("github:svkozak/pi-acp#{sha}"),
            None => "pi-acp".into(),
        },
        _ => adapter.into(),
    }
}

/// `GET /status` over the Unix socket, R6, within `PROBE`.
#[cfg(unix)]
pub(crate) fn socket_status(path: &Path) -> Result<(), String> {
    use std::io::{Read, Write};
    if !path.exists() {
        return Err("no socket".into());
    }
    let mut stream = std::os::unix::net::UnixStream::connect(path).map_err(|e| format!("connect failed: {e}"))?;
    stream.set_read_timeout(Some(PROBE)).and_then(|_| stream.set_write_timeout(Some(PROBE))).map_err(|e| e.to_string())?;
    stream.write_all(b"GET /status HTTP/1.0\r\nHost: rosterd\r\n\r\n").map_err(|e| format!("write failed: {e}"))?;
    let mut answer = String::new();
    let _ = stream.read_to_string(&mut answer);
    match answer.lines().next() {
        Some(line) if line.contains(" 200 ") => Ok(()),
        Some(line) => Err(format!("GET /status answered {line}")),
        None => Err("no answer within 2 s".into()),
    }
}

#[cfg(not(unix))]
pub(crate) fn socket_status(path: &Path) -> Result<(), String> {
    if path.exists() { Ok(()) } else { Err("no socket".into()) }
}

fn mode_of(meta: &std::fs::Metadata) -> Option<u32> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        Some(meta.permissions().mode() & 0o777)
    }
    #[cfg(not(unix))]
    {
        let _ = meta;
        None
    }
}

/// A file that must exist, be non-empty, and be mode `want`, R10.
fn private_file(name: &str, path: &PathBuf, want: u32, fix: &str) -> Check {
    match std::fs::metadata(path) {
        Ok(meta) if meta.len() == 0 => check(name, false, format!("{} is empty", path.display()), fix),
        Ok(meta) => match mode_of(&meta) {
            Some(mode) if mode != want => check(name, false, format!("{} is mode {mode:o}, not {want:o}", path.display()), &format!("chmod {want:o} {}", path.display())),
            _ => check(name, true, path.display().to_string(), ""),
        },
        Err(_) => check(name, false, format!("{} missing", path.display()), fix),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A fresh machine: an empty config dir, no harnesses, no daemon. Every harness and adapter
    /// is listed with its install command, and nothing appears on disk, A14 acceptance 10.
    #[test]
    fn lists_every_missing_harness_and_changes_nothing() {
        let dir = std::env::temp_dir().join(format!("rosterd-doctor-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut config = Config::default();
        config.node.socket = Some(dir.join("rosterd.sock"));
        config.node.loopback_port = 1; // nothing listens on 127.0.0.1:1
        config.node.listen = "off".into();
        config.runner.holder_dir = dir.join("holders");
        let paths = Paths {
            claude_settings: dir.join("home/.claude/settings.json"),
            codex_hooks: dir.join("home/.codex/hooks.json"),
            pi_extension: dir.join("home/.pi/agent/extensions/rosterd-pi.ts"),
            lock: dir.join("config/adapters.lock"),
            // An empty PATH: no harness, no adapter, no rosterd-hook, whatever the machine has.
            search_path: Default::default(),
        };
        let checks = run_at(&config, &dir.join("config"), &paths);

        let by_name = |name: &str| checks.iter().find(|c| c.name == name).unwrap_or_else(|| panic!("check {name}"));
        for (name, fix) in [
            ("harness claude", "npm i -g @anthropic-ai/claude-code"),
            ("harness codex", "npm i -g @openai/codex"),
            ("harness pi", "npm i -g @earendil-works/pi-coding-agent"),
            ("adapter claude-agent-acp", "npm i -g @agentclientprotocol/claude-agent-acp"),
            ("adapter codex-acp", "npm i -g @agentclientprotocol/codex-acp"),
            ("adapter pi-acp", "npm i -g pi-acp"),
            ("hooks claude", "rosterd integrate install claude"),
            ("hooks codex", "rosterd integrate install codex"),
            ("extension pi", "rosterd integrate install pi"),
            ("socket", "rosterd daemon"),
            ("loopback", "rosterd daemon"),
        ] {
            let c = by_name(name);
            assert!(!c.ok, "{name} should fail on a fresh machine");
            assert_eq!(c.fix.as_deref(), Some(fix), "{name}");
        }
        assert!(by_name("tailscale").ok);
        assert!(!by_name("holders").ok && !by_name("node key").ok);
        assert!(by_name("swarm").ok, "standalone is not a failure");
        let mut ok_json = serde_json::to_string(&checks).unwrap();
        ok_json.truncate(1);
        assert_eq!(ok_json, "[");

        let left: Vec<_> = std::fs::read_dir(&dir).unwrap().map(|e| e.unwrap().file_name()).collect();
        assert!(left.is_empty(), "doctor created {left:?}");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
