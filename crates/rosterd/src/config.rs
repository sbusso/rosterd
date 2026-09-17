//! One TOML file per node, R10, plus the platform paths of R6 and R11.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use rosterd_proto::PermissionPolicy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub node: NodeConfig,
    pub swarm: SwarmConfig,
    pub runner: RunnerConfig,
    pub sources: SourcesConfig,
    /// `[harness.claude] adapter = "claude-agent-acp"`.
    pub harness: BTreeMap<String, HarnessConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct NodeConfig {
    /// Pinned in config; defaults to the hostname on first start, R7.1.
    pub name: String,
    /// Tailscale listener port, R7.
    pub port: u16,
    /// `tailscale` or `off`. A node refuses any other interface, R7.6.
    pub listen: String,
    /// Loopback HTTP port with the bearer token, R6.
    pub loopback_port: u16,
    /// `loopback` or `tailscale`: the page and the bearer API also answer on the Tailscale IP at
    /// `loopback_port`, so a phone or another machine on the tailnet opens `/ui` directly.
    pub ui_listen: String,
    /// Overrides the platform default socket path, R6.
    pub socket: Option<PathBuf>,
    /// The daemon runs `rosterd-tray` beside itself when it has a GUI session (a LaunchAgent, a
    /// shell), so one service is the daemon and the menu bar. A LaunchDaemon has no GUI; the
    /// setup's tray row covers it.
    pub tray: bool,
}

impl Default for NodeConfig {
    fn default() -> Self {
        NodeConfig {
            name: hostname(),
            port: 8791,
            listen: "tailscale".into(),
            loopback_port: 8790,
            ui_listen: "tailscale".into(),
            socket: None,
            tray: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct SwarmConfig {
    pub static_peers: Vec<String>,
    pub mdns: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct RunnerConfig {
    pub default_permission_policy: PermissionPolicy,
    pub holder_dir: PathBuf,
    pub resume_on_crash: bool,
    pub recap: bool,
    /// Path of the holder binary; next to the daemon when unset.
    pub holder_bin: Option<PathBuf>,
    /// R15.2. Seconds idle before a live headless session is suspended.
    pub idle_timeout_s: u64,
    pub suspend_on_needs_attention: bool,
    pub resume_on_prompt: bool,
    pub max_resumes_per_hour: u32,
}

impl Default for RunnerConfig {
    fn default() -> Self {
        RunnerConfig {
            default_permission_policy: PermissionPolicy::Attention,
            holder_dir: state_dir().join("holders"),
            resume_on_crash: true,
            recap: true,
            holder_bin: None,
            idle_timeout_s: 1800,
            suspend_on_needs_attention: false,
            resume_on_prompt: true,
            max_resumes_per_hour: 6,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct SourcesConfig {
    pub files: bool,
    pub scan_interval_ms: u64,
}

impl Default for SourcesConfig {
    fn default() -> Self {
        SourcesConfig { files: false, scan_interval_ms: 2000 }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct HarnessConfig {
    /// The ACP adapter command, R10.
    pub adapter: String,
    pub args: Vec<String>,
    /// R16.1. The git commit the adapter is pinned to in the node lock file.
    pub adapter_commit: Option<String>,
    /// R16.2. Whether the harness's rosterd extension is expected (pi only).
    pub extension: bool,
    /// R16.2. Seconds a gated tool call waits for allow or deny before it is denied.
    pub gate_timeout_s: u64,
}

impl Default for HarnessConfig {
    fn default() -> Self {
        HarnessConfig { adapter: String::new(), args: Vec::new(), adapter_commit: None, extension: false, gate_timeout_s: 3600 }
    }
}

impl Config {
    pub fn load(path: &Path) -> Result<Config> {
        if !path.exists() {
            let mut config = Config::default();
            config.harness.insert(
                "claude".into(),
                HarnessConfig { adapter: "claude-agent-acp".into(), ..Default::default() },
            );
            config.harness.insert(
                "codex".into(),
                HarnessConfig { adapter: "codex-acp".into(), ..Default::default() },
            );
            // R16.4. The pi adapter is pinned by commit in the lock file; unset until `integrate install pi`.
            config.harness.insert(
                "pi".into(),
                HarnessConfig { adapter: "pi-acp".into(), extension: true, ..Default::default() },
            );
            return Ok(config);
        }
        let text = std::fs::read_to_string(path).with_context(|| format!("read {}", path.display()))?;
        let config: Config = toml::from_str(&text).with_context(|| format!("parse {}", path.display()))?;
        anyhow::ensure!(
            matches!(config.node.listen.as_str(), "tailscale" | "off"),
            "node.listen must be tailscale or off, R7.6; nothing listens elsewhere"
        );
        anyhow::ensure!(
            matches!(config.node.ui_listen.as_str(), "loopback" | "tailscale"),
            "node.ui_listen must be loopback or tailscale, R7.6; nothing listens elsewhere"
        );
        Ok(config)
    }

    pub fn socket_path(&self) -> PathBuf {
        self.node.socket.clone().unwrap_or_else(default_socket_path)
    }
}

pub fn config_dir() -> PathBuf {
    std::env::var_os("ROSTERD_CONFIG_DIR")
        .map(PathBuf::from)
        .or_else(|| dirs::config_dir().map(|d| d.join("rosterd")))
        .unwrap_or_else(|| PathBuf::from(".rosterd"))
}

pub fn config_path() -> PathBuf {
    config_dir().join("rosterd.toml")
}

pub fn state_dir() -> PathBuf {
    std::env::var_os("ROSTERD_STATE_DIR")
        .map(PathBuf::from)
        .or_else(|| dirs::state_dir().map(|d| d.join("rosterd")))
        .or_else(|| dirs::data_local_dir().map(|d| d.join("rosterd")))
        .unwrap_or_else(|| PathBuf::from(".rosterd/state"))
}

/// R6: $XDG_RUNTIME_DIR/rosterd.sock on Linux, $TMPDIR/rosterd.sock on macOS, a named pipe on
/// Windows. ROSTERD_SOCKET overrides everywhere.
pub fn default_socket_path() -> PathBuf {
    if let Some(p) = std::env::var_os("ROSTERD_SOCKET") {
        return PathBuf::from(p);
    }
    #[cfg(target_os = "linux")]
    {
        if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR") {
            return PathBuf::from(dir).join("rosterd.sock");
        }
    }
    #[cfg(windows)]
    {
        return PathBuf::from(r"\\.\pipe\rosterd");
    }
    #[allow(unreachable_code)]
    std::env::temp_dir().join("rosterd.sock")
}

pub fn hostname() -> String {
    std::env::var("HOSTNAME")
        .ok()
        .filter(|h| !h.is_empty())
        .or_else(|| {
            std::process::Command::new("hostname")
                .output()
                .ok()
                .map(|o| String::from_utf8_lossy(&o.stdout).trim().to_string())
                .filter(|h| !h.is_empty())
        })
        .map(|h| short(&h))
        .unwrap_or_else(|| "node".into())
}

/// The host part alone: `MacBook-Pro-2.local` and `box.example.net` name the node `MacBook-Pro-2`
/// and `box`, as `hostname -s` would.
fn short(hostname: &str) -> String {
    hostname.split('.').next().unwrap_or(hostname).to_string()
}

/// Ensures `path`'s parent exists and, on Unix, that the file is 0600, R10.
pub fn write_private(path: &Path, bytes: &[u8]) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    std::fs::write(path, bytes)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_node_name_is_the_host_part() {
        assert_eq!(short("MacBook-Pro-2.local"), "MacBook-Pro-2");
        assert_eq!(short("omarchy64"), "omarchy64");
    }

    #[test]
    fn parses_the_spec_example_and_refuses_other_interfaces() {
        let dir = std::env::temp_dir().join(format!("rosterd-cfg-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("rosterd.toml");
        std::fs::write(
            &path,
            r#"
[node]
name = "gibson"
port = 8791
listen = "tailscale"
loopback_port = 8790
[swarm]
static_peers = ["100.64.0.12:8791"]
[runner]
default_permission_policy = "auto"
[harness.claude]
adapter = "claude-agent-acp"
"#,
        )
        .unwrap();
        let config = Config::load(&path).unwrap();
        assert_eq!(config.node.name, "gibson");
        assert_eq!(config.swarm.static_peers, vec!["100.64.0.12:8791"]);
        assert_eq!(config.runner.default_permission_policy, PermissionPolicy::Auto);
        assert_eq!(config.harness["claude"].adapter, "claude-agent-acp");
        assert_eq!(config.sources.scan_interval_ms, 2000);

        std::fs::write(&path, "[node]\nlisten = \"0.0.0.0\"\n").unwrap();
        assert!(Config::load(&path).is_err());
        std::fs::write(&path, "[node]\nui_listen = \"0.0.0.0\"\n").unwrap();
        assert!(Config::load(&path).is_err());
        std::fs::write(&path, "[node]\nui_listen = \"tailscale\"\n").unwrap();
        assert_eq!(Config::load(&path).unwrap().node.ui_listen, "tailscale");
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
