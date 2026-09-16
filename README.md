# rosterd

One daemon per machine that knows every coding agent session on it, drives headless sessions over
ACP, joins a swarm of peers over Tailscale, and reports to the workspace. The spec is SPEC.md.

Its own repository and Cargo workspace; the workspace it reports to lives in its own.

```
crates/proto     types shared by daemon and holder: Record, Snapshot, HolderState, Source
crates/holder    rosterd-holder, R2.1: owns one ACP adapter's stdio, relays over a socket
crates/tray      rosterd-tray: the roster in the menu bar (macOS, Linux), a native menu over the CLI
crates/rosterd   the daemon
  src/config.rs     TOML config and platform paths, R10 R6
  src/identity.rs   Ed25519 node key and node_id, R7.1
  src/node.rs       what a handler can reach
  src/roster/       the table, precedence, snapshot, events         R3 R4
  src/scanner/      process enumeration, tmux and herdr handles     R4
  src/runner/       ACP client, holders, policy, resume             R5 R2.2
  src/mesh/         discovery, membership, snapshot exchange, proxy R7
  src/bridge/       workspace client and disk queue                 R8
  src/api/          socket, loopback, Tailscale listeners, MCP      R6 R9
  src/cli/          the command line client                         R14
scripts/         hook, launcher, opener
skill/           SKILL.md, the CLI summary agents load, R14.4
ui/              the single page client served at /ui, R9
packaging/       systemd unit, launchd plist, build script
```

Build: `cargo build --release`. Cross: `cargo zigbuild --release --target x86_64-unknown-linux-gnu`
(see packaging/build.sh). Test: `cargo test`.

Module ownership during the build: each module directory has one owner named in its header
comment. Public signatures in the stubs are the contract between modules; extend freely, change
only after grepping callers.

## Usage

Install with a package manager:

```
brew tap sbusso/rosterd https://github.com/sbusso/rosterd && brew install --HEAD rosterd   # macOS, Formula/rosterd.rb
makepkg -si -p packaging/arch/PKGBUILD                                                     # Arch, from the tag
```

The formula is head only and the PKGBUILD has no checksum until the first tag;
`packaging/arch/test.sh` builds and installs the Arch package in a container from this checkout.

Or from source: `packaging/build.sh native` writes `dist/<host>/rosterd`, `rosterd-holder` and
`rosterd-tray`; with no argument it also cross-builds `x86_64-unknown-linux-gnu` and
`aarch64-unknown-linux-gnu` with cargo zigbuild. `packaging/install.sh [--from dist/<target>]`
copies them and the three scripts into `~/.local/bin`.

Then, either way:

```
rosterd setup
```

A checklist with one row per thing the machine needs: binaries on PATH, config, the service
(a LaunchDaemon on macOS, a systemd user unit on Linux), the daemon, then per harness the binary,
its ACP adapter and the hooks or extension, the tray at login, and two optional rows that ask for
input, the workspace credential and a swarm to join. Space picks rows, enter runs them, `a` picks
everything needed. `rosterd setup --yes`, or a pipe, runs the needed rows headless. The screen is
`crates/rosterd/src/setup/tui.rs`, generic over the rows: another tool brings its own `steps()`.

After that the roster lives in three places, all reading the same daemon.

**The menu bar.** `rosterd-tray` (started at login by the `tray` row) puts the roster under an
icon: sessions grouped by state (needs attention, active, idle, unknown, suspended) with a coloured
dot, the harness, the CPU share and the age of the last activity. A session that needs attention
opens a submenu with Allow, Allow always and Deny; a click on any other row jumps to it (`rosterd
open`: the tmux pane, the herdr pane, or the conversation view of a headless session). On macOS the
count of sessions needing attention sits beside the icon; on Linux the icon's colour is the
signal, shown wherever StatusNotifierItem trays are (KDE and most desktops; GNOME with the
AppIndicator extension). "Open in browser" at the bottom opens the roster page. The tray is a menu
over the CLI (`rosterd watch --json` feeds it, `rosterd allow|deny|open|ui` act), so it needs no
socket, token or config of its own; what it cannot do, start, name or spawn a session, join a
swarm, is the CLI or the page.

**The roster page.** Every node serves one HTML page at `http://127.0.0.1:8790/ui`; `rosterd ui`
or the tray's "Open in browser" opens it with `?token=<loopback.token>` once and the tab keeps the
bearer. It shows the roster with the same states and actions as the tray, a project badge per
row, and `/ui/sessions/<session_key>` renders the ACP stream of a headless session live with its
last recap. "Add workspace" on the page stores a workspace URL and token in the browser and lists
what needs you first. With `ui_listen = "tailscale"` in `[node]` the page and its API also
answer on the node's Tailscale IP at the same port, so a phone or another machine on the tailnet
opens `http://<tailscale-ip>:8790/ui?token=<loopback.token>` directly; the token is still the
application boundary and Tailscale the network one.

**The shell.** `rosterd status` prints the node, its listeners, swarm, bridge state and the
counts; `rosterd list` the rows; `rosterd daemon` runs the daemon by hand. The config lives in
`~/.config/rosterd/rosterd.toml` on Linux and `~/Library/Application Support/rosterd/rosterd.toml`
on macOS (`ROSTERD_CONFIG_DIR` overrides), next to `node.key`, `loopback.token` and `swarm.json`.

## Swarm

A swarm is a set of nodes that answer for each other: `rosterd list --swarm` on any of them is the
union roster, and an action on a session owned elsewhere is proxied to its owner. No leader, no
shared database; each node keeps its own roster plus the last snapshot it heard from every peer,
R7.

What it needs. Every machine on the same tailnet, with `listen = "tailscale"` and the same
`port` (8791 by default) in `[node]`, the defaults `rosterd setup` writes. Nothing listens
elsewhere; a node refuses to start with any other interface, R7.6. Tailscale ACLs stay the network
boundary, the swarm key is the application boundary, and a node needs both to be heard.

Create and join. There is no create step: the first `rosterd invite` on a node that is in no
swarm creates one and signs the node in. So, on a node already running:

```
rosterd invite            # prints a single-use token, good for one hour (--ttl MINUTES)
```

On the new machine, either the `swarm` row of `rosterd setup` (it asks for the peer address and
the token) or:

```
rosterd join <peer tailscale ip>:8791 --token <invite>
```

The joiner presents the invite with its hello; the admitting node verifies it, returns the swarm
id, the swarm key sealed to the joiner's key and the current membership, and the joiner greets
every member. Membership is gossiped from there, so the invite can come from any node, and
discovery of who is up runs on `tailscale status`: every online peer is probed for `/node/hello`
and admitted when its hello carries the swarm key. `[swarm] static_peers = ["100.64.0.12:8791"]`
is the fallback when Tailscale is absent.

Then:

```
rosterd nodes             # membership and health, including unreachable peers with their age
rosterd list --swarm      # every session on every node
rosterd list --node NAME  # one node, proxied through this one
rosterd revoke NODE_ID    # removes a node everywhere
rosterd leave             # takes this node out
```

The tray shows the local node; the roster page follows `/swarm/events`, so it shows every node.
Windows joins as a headless-only node: hooks, no tmux or herdr handles.

## CLI

One binary, R14. `rosterd` alone prints help; everything but `daemon` talks to the local socket.
Every command takes `--json`, which prints the API frame for the resource byte for byte
(`rosterd list --json` is GET /snapshot, `--swarm --json` is GET /swarm/snapshot). Exit codes:
0 ok, 1 user error, 2 daemon unreachable, 3 a swarm peer was needed and is unreachable.

```
rosterd list|watch [--swarm|--node N]      one row per session; watch reprints on every change
rosterd status | nodes                     this node; swarm membership and health
rosterd read KEY | explain KEY             one session in full; which source set each field
rosterd start --harness H --cwd DIR --attempt ATT [--parent-attempt P] [--name L]
              [--policy auto|attention|decision] [--model M] [--effort E] [--env K=V ...]
rosterd prompt KEY TEXT [--wait idle|needs_attention|ended] [--timeout SECONDS]
rosterd cancel|stop|suspend|resume|open KEY
rosterd name KEY LABEL | name KEY --clear
rosterd allow KEY [--always] | deny KEY [--reason TEXT]
rosterd spawn KEY --harness H --cwd DIR --task TASK [--name L]
rosterd invite [--ttl MINUTES] | join ADDRESS --token T | revoke NODE_ID | leave
rosterd integrate install|uninstall claude|codex|pi | integrate status
rosterd ui                                 the roster page in the browser
rosterd daemon [--config PATH] | setup [--yes] | doctor | version
```

KEY is an exact session key, a PID on this node, or a unique display name (the name, else the
cwd basename shown in brackets); an ambiguous name lists the candidates. `list` and `watch` read
the roster only and answer with the runner, bridge or mesh broken. The agent-facing summary is
`skill/SKILL.md`.

Hook. Put `rosterd-hook` on PATH and add this to `~/.claude/settings.json`; the same object without
`Notification` goes in `~/.codex/hooks.json` (Codex raises `PermissionRequest`):

```json
{
  "hooks": {
    "SessionStart":      [{ "hooks": [{ "type": "command", "command": "rosterd-hook" }] }],
    "UserPromptSubmit":  [{ "hooks": [{ "type": "command", "command": "rosterd-hook" }] }],
    "PreToolUse":        [{ "matcher": "", "hooks": [{ "type": "command", "command": "rosterd-hook" }] }],
    "PermissionRequest": [{ "hooks": [{ "type": "command", "command": "rosterd-hook" }] }],
    "Notification":      [{ "hooks": [{ "type": "command", "command": "rosterd-hook" }] }],
    "Stop":              [{ "hooks": [{ "type": "command", "command": "rosterd-hook" }] }]
  }
}
```

The hook reads `session_id`, `hook_event_name`, `cwd` and `notification_type` from the event,
nothing else, and posts the claim to the daemon socket and, when the three workspace variables
are set, to the workspace. It fails open in 800 ms and always exits 0, R4. `rosterd-hook
--self-test` runs the mapping offline.

Launch. `rosterd-launch [--attempt <id> --token <t>] [--name <n>] [--harness claude|codex] --
claude --model opus` exports the variables below, registers the session with the daemon as
source `launcher`, and execs the harness with the rosterd MCP server in its config
(`--mcp-config` for Claude Code, `-c mcp_servers.rosterd.*` for Codex). Without `--attempt`
the roster sees the session and the workspace does not.

Open. `rosterd-open --handle '<runtime handle json>'` jumps to a session from a shell: tmux, herdr,
or the conversation view; `rosterd open KEY` resolves the handle first.

Environment.

| variable | read by | meaning |
| --- | --- | --- |
| `WORKSPACE_URL`, `WORKSPACE_ATTEMPT_ID`, `WORKSPACE_ATTEMPT_TOKEN` | hook, bridge | the attempt a session works on; all three or the hook skips the workspace |
| `ROSTERD_SOCKET` | daemon, CLI, hook, launcher | the daemon socket; default `$XDG_RUNTIME_DIR/rosterd.sock` on Linux, `$TMPDIR/rosterd.sock` on macOS, R6 |
| `ROSTERD_HARNESS` | hook | the harness name when the process tree does not say |
| `ROSTERD_CONFIG_DIR`, `ROSTERD_STATE_DIR` | daemon, scripts | override the config and state directories |
| `ROSTERD_CONFIG` | CLI, daemon | the config file (`--config`) |
| `ROSTERD_MACHINE`, `ROSTERD_TERMINAL_CMD`, `ROSTERD_HERDR_LINK` | opener | this machine's name, how to open a terminal, whether a `herdr://` handler is registered |
| `RUST_LOG` | daemon | log filter, `info` by default |
