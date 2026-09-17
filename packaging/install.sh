#!/usr/bin/env bash
# Installs rosterd for the current user: binaries and scripts into ~/.local/bin, the service for
# this OS, and a default rosterd.toml when there is none. Run after packaging/build.sh, or with
# --from <dir> for a downloaded dist/<target> directory.
#
#   packaging/install.sh [--from dist/<target>] [--no-service]
#
# Linux: the systemd user unit (packaging/rosterd.service), enabled and started. For a server
# without a login session use packaging/rosterd-system@.service instead, see its header.
# macOS: the LaunchAgent (packaging/com.rosterd.daemon.plist), no sudo.
set -euo pipefail
cd "$(dirname "$0")/.."

from='' service=1
while [ $# -gt 0 ]; do
  case "$1" in
    --from) from=$2; shift 2 ;;
    --no-service) service=0; shift ;;
    *) echo "usage: packaging/install.sh [--from dist/<target>] [--no-service]" >&2; exit 2 ;;
  esac
done
[ -n "$from" ] || from="dist/$(rustc -vV | sed -n 's/^host: //p')"
[ -x "$from/rosterd" ] || { echo "install.sh: no binaries in $from; run packaging/build.sh native first" >&2; exit 1; }

bin="$HOME/.local/bin"
mkdir -p "$bin"
install -m755 "$from/rosterd" "$from/rosterd-holder" "$bin/"
install -m755 scripts/rosterd-hook scripts/rosterd-launch scripts/rosterd-open "$bin/"
[ -x "$from/rosterd-tray" ] && install -m755 "$from/rosterd-tray" "$bin/"
echo "installed rosterd, rosterd-holder, rosterd-hook, rosterd-launch, rosterd-open$([ -x "$from/rosterd-tray" ] && echo ', rosterd-tray') into $bin"
case ":$PATH:" in *":$bin:"*) ;; *) echo "note: $bin is not on PATH; the hook must be reachable by the harness" ;; esac

# The config directory as crates/rosterd/src/config.rs resolves it.
config_dir=${ROSTERD_CONFIG_DIR-}
if [ -z "$config_dir" ]; then
  case "$(uname)" in
    Darwin) config_dir="$HOME/Library/Application Support/rosterd" ;;
    *) config_dir="${XDG_CONFIG_HOME:-$HOME/.config}/rosterd" ;;
  esac
fi
mkdir -p "$config_dir"; chmod 700 "$config_dir"
if [ ! -f "$config_dir/rosterd.toml" ]; then
  cat >"$config_dir/rosterd.toml" <<TOML
# rosterd, R10. Pin the name: a renamed machine is a new name, the id stays.
[node]
name = "$(hostname -s 2>/dev/null || hostname)"
port = 8791
listen = "tailscale"
loopback_port = 8790

[swarm]
static_peers = []

[runner]
default_permission_policy = "attention"
resume_on_crash = true
recap = true

[sources]
files = false
scan_interval_ms = 2000

[harness.claude]
adapter = "claude-agent-acp"
[harness.codex]
adapter = "codex-acp"
TOML
  chmod 600 "$config_dir/rosterd.toml"
  echo "wrote $config_dir/rosterd.toml"
fi

[ "$service" = 1 ] || exit 0
case "$(uname)" in
  Linux)
    install -Dm644 packaging/rosterd.service "${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/rosterd.service"
    systemctl --user daemon-reload
    systemctl --user enable --now rosterd
    echo "rosterd.service enabled and started (systemctl --user status rosterd)"
    ;;
  Darwin)
    mkdir -p "$HOME/Library/LaunchAgents" "$HOME/.local/state/rosterd"
    target="$HOME/Library/LaunchAgents/com.rosterd.daemon.plist"
    sed -e "s|__HOME__|$HOME|g" packaging/com.rosterd.daemon.plist >"$target"
    launchctl bootout "gui/$(id -u)/com.rosterd.daemon" 2>/dev/null || true
    launchctl bootstrap "gui/$(id -u)" "$target"
    echo "LaunchAgent loaded (launchctl print gui/$(id -u)/com.rosterd.daemon)"
    ;;
  *) echo "no service file for $(uname); start with: rosterd daemon" ;;
esac
