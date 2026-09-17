# rosterd

One daemon per machine that knows every coding agent session on it, drives headless sessions over
ACP, and joins a swarm of peers over Tailscale. Anything above it (a project board, a task
runner) is a client of its API. The spec is SPEC.md.

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
  src/api/          socket, loopback, Tailscale listeners, MCP      R6 R9
  src/cli/          the command line client                         R14
scripts/         hook, launcher, opener
skill/           SKILL.md, the CLI summary agents load, R14.4
ui/              the single page client served at /ui, R9
ios/             the phone app, paired by the page's QR
packaging/       systemd unit, launchd plist, build script
```

Build: `cargo build --release`. Cross: `cargo zigbuild --release --target x86_64-unknown-linux-gnu`
(see packaging/build.sh). Test: `cargo test`.

Module ownership during the build: each module directory has one owner named in its header
comment. Public signatures in the stubs are the contract between modules; extend freely, change
only after grepping callers.

## Install

```
brew tap sbusso/rosterd https://github.com/sbusso/rosterd && brew trust sbusso/rosterd
brew install rosterd && brew services start rosterd          # macOS, the release bottle
tar xzf rosterd-<ver>-x86_64-unknown-linux-gnu.tar.gz \
  && rosterd-<ver>/packaging/install.sh --from rosterd-<ver>/dist/x86_64-unknown-linux-gnu   # Linux, the release tarball
makepkg -si -p packaging/arch/PKGBUILD                       # Arch, from source
```

On macOS that is the whole install: the daemon runs as a LaunchAgent and keeps the menu bar
tray beside it (Homebrew's sandbox keeps a formula out of launchd, hence the `services start`);
`brew services stop rosterd` is the off switch. The harness hooks and ACP adapters are
`rosterd setup`, since they edit `~/.claude` and `~/.codex`.

From source: `packaging/build.sh native` then `packaging/install.sh` (`--from dist/<target>` for a
downloaded build). Cross builds need cargo zigbuild.

Then:

```
rosterd setup
```

One row per thing the machine needs. Space picks, enter runs, `a` picks everything.
`rosterd setup --yes` runs it headless.

- binaries on PATH, config, the service (LaunchAgent on macOS, systemd user unit on Linux), the daemon
- per harness: the binary, its ACP adapter, the hooks or extension
- the tray at login
- optional: a swarm to join

## Using it

States come from hooks and ACP. A session only the process scan knows is active while its process
tree uses the CPU and idle after a minute without; a hook or ACP claim takes over as soon as one
arrives, R4.

**Menu bar** (`rosterd-tray`)

- sessions grouped by state: needs attention, active, idle, unknown, suspended
- each row: name, harness, CPU share, age of the last activity
- needs attention → Allow, Allow always, Deny
- any other row → jumps to it: the tmux pane, the herdr pane, or the conversation view
- Open in browser → the roster page
- macOS: the attention count beside the icon. Linux: the icon colour (KDE and most desktops; GNOME needs the AppIndicator extension)

**Roster page** (`rosterd ui`)

- `http://127.0.0.1:8790/ui`, opened once with `?token=<loopback.token>`; the tab keeps it
- the roster with the same states and actions as the tray, every node of the swarm
- each row: project badge, state and its age, the folder with `~` for home; on the right the harness, CPU share, memory and uptime of the process tree, then the actions
- `/ui/sessions/<session_key>`: the live conversation of a headless session, with its last recap
- also on the Tailscale IP (`ui_listen = "tailscale"`, the default): `http://<tailscale-ip>:8790/ui?token=…` from another machine on the tailnet

**Phone** (`ios/`)

- a SwiftUI app, the roster page on a phone: same rows, states and actions, the conversation of a headless session
- pairing: `pair phone` on the roster page shows a QR of a `rosterd://pair` link with the Tailscale address and the bearer; the app scans it (or the Camera app opens it). Needs the phone on the tailnet
- build: open `ios/Rosterd.xcodeproj` in Xcode, run on a device; `swiftc Rosterd/Models.swift check/main.swift` is the decode check

**Shell**

- `rosterd status`: the node, listeners, swarm, counts
- `rosterd list` / `rosterd watch`: the rows
- `rosterd daemon`: run the daemon by hand
- config: `~/.config/rosterd/rosterd.toml` (Linux), `~/Library/Application Support/rosterd/rosterd.toml` (macOS), next to `node.key`, `loopback.token`, `swarm.json`

Starting, naming and spawning sessions, and joining a swarm, are CLI or page, not tray.

## Swarm

Nodes that answer for each other: `rosterd list --swarm` anywhere is the union roster, and an
action on a session owned elsewhere is proxied to its owner. No leader, no shared database.

Needs: every machine on the same tailnet, `listen = "tailscale"` and the same `port` (8791) in
`[node]`. These are the setup defaults. Nothing listens on any other interface.

There is no create step. The first `rosterd invite` creates the swarm.

```
# on a running node
rosterd invite                                   # single-use token, one hour (--ttl MINUTES)

# on the new machine (or the `swarm` row of rosterd setup)
rosterd join <peer tailscale ip>:8791 --token <invite>
```

Membership gossips from there; discovery runs on `tailscale status`. `[swarm] static_peers` is
the fallback without Tailscale.

```
rosterd nodes             # membership and health
rosterd list --swarm      # every session on every node
rosterd list --node NAME  # one node
rosterd changes           # one line per change anywhere: started, attention, ended, node
rosterd revoke NODE_ID    # remove a node everywhere
rosterd leave             # take this node out
```

Tailscale ACLs are the network boundary, the swarm key the application boundary. Windows joins
headless only: hooks, no tmux or herdr handles.

## CLI

One binary, R14. `rosterd` alone prints help; everything but `daemon` talks to the local socket.
Every command takes `--json`, which prints the API frame for the resource byte for byte
(`rosterd list --json` is GET /snapshot, `--swarm --json` is GET /swarm/snapshot). Exit codes:
0 ok, 1 user error, 2 daemon unreachable, 3 a swarm peer was needed and is unreachable.

```
rosterd list|watch [--swarm|--node N]      one row per session; watch reprints on every change
rosterd changes                            one line per change across the swarm
rosterd status | nodes                     this node; swarm membership and health
rosterd read KEY | explain KEY             one session in full; which source set each field
rosterd start --harness H --cwd DIR [--name L] [--policy auto|attention] [--model M]
              [--effort E] [--env K=V ...]
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
the roster only and answer with the runner or mesh broken. The agent-facing summary is
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
nothing else, and posts the claim to the daemon socket. It fails open in 800 ms and always
exits 0, R4. `rosterd-hook
--self-test` runs the mapping offline.

Launch. `rosterd-launch [--name <n>] [--harness claude|codex] -- claude --model opus` exports
the variables below, registers the session with the daemon as source `launcher`, and execs the
harness with the rosterd MCP server in its config (`--mcp-config` for Claude Code,
`-c mcp_servers.rosterd.*` for Codex).

Open. `rosterd-open --handle '<runtime handle json>'` jumps to a session from a shell: tmux, herdr,
or the conversation view; `rosterd open KEY` resolves the handle first.

Environment.

| variable | read by | meaning |
| --- | --- | --- |
| `ROSTERD_SOCKET` | daemon, CLI, hook, launcher | the daemon socket; default `$XDG_RUNTIME_DIR/rosterd.sock` on Linux, `$TMPDIR/rosterd.sock` on macOS, R6 |
| `ROSTERD_HARNESS` | hook | the harness name when the process tree does not say |
| `ROSTERD_CONFIG_DIR`, `ROSTERD_STATE_DIR` | daemon, scripts | override the config and state directories |
| `ROSTERD_CONFIG` | CLI, daemon | the config file (`--config`) |
| `ROSTERD_MACHINE`, `ROSTERD_TERMINAL_CMD`, `ROSTERD_HERDR_LINK` | opener | this machine's name, how to open a terminal, whether a `herdr://` handler is registered |
| `RUST_LOG` | daemon | log filter, `info` by default |
