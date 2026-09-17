# Roster daemon and swarm spec

Working name for the daemon: rosterd. Working name for the network of daemons: the swarm. Rename freely, keep the vocabulary.

Status: the runtime layer and its client contract. rosterd is independent of anything above it: a project board, a task runner or a workspace is a client of the API described here, never a dependency. rosterd holds no URL, token or id of such a client.

Audience: the build team (agents). Everything here is buildable as written. Where a choice is open, the default in the text applies.

## R0. What this is and is not

rosterd is one daemon per machine. It knows every coding agent session on that machine, whoever started it. It can start and drive sessions itself over ACP. It joins a swarm of other rosterd instances over Tailscale so that any machine can answer for all of them. Tasks, attempts, decisions and history are not its business; a client that keeps them pulls from rosterd.

Compared with the tools it borrows from.

agentd. Same four activity words, same identity rule, same full-snapshot output, same hook contract, same refusal to infer. Adds running sessions, a mesh, and cross platform.

agentd-hub. Replaced by the swarm. No central aggregator, every node is a hub.

herdr. Same holder shape for process persistence. rosterd does not render terminals and does not replace tmux or herdr for the interactive lane. It can read them.

Tightbeam. Same idea of a gateway that owns headless sessions, resumes them, and delivers decisions to any surface. Deliberately without Tightbeam's gates, verbs, statutes, credential onboarding, or identity repository. Harnesses use their own vendor login. Rules and history live in the client, not in the daemon.

What rosterd never does. Read prompts, transcripts, or terminal screens. Infer activity from time, CPU, or output. Schedule or wake agents on its own. Store task or decision history. Dial a client. Bind to a non Tailscale, non loopback address.

## R1. Vocabulary

Terms: harness, session, activity (active, idle, needs_attention, unknown), claim, runtime handle, record.

node. One rosterd instance on one machine. Identified by a keypair generated at first start. The node name is the machine name humans use.

swarm. The set of nodes that know each other. No leader, no shared database. Every node holds its own roster plus the last snapshot it received from every peer.

peer. Another node in the swarm.

lane. How a session is owned. headless means rosterd started it over ACP. interactive means a human or a terminal manager owns it and rosterd only observes.

holder. A small process that owns the stdio pipes of one headless harness and exposes a local socket. rosterd connects to holders. Restarting rosterd does not restart holders.

source. Where a roster entry's information came from. launcher, hook, acp, scan, or files. An entry can have several sources. The highest ranked one wins for each field.

roster. The complete table of sessions on one node. The swarm roster is the union across nodes.

## R2. Node architecture

One binary, one systemd user or system unit on Linux, one launchd daemon on macOS, one service on Windows. Modules inside it.

roster. In-memory table of sessions. Rebuilt on start from holders, hook claims, and a process scan. Serves snapshots. Never blocks on other modules. If the runner is wedged the roster still answers.

runner. ACP client. Spawns holders, drives sessions, maps ACP traffic to claims, handles resume.

intake. Accepts claims and registrations from hooks, launchers, and holders on the local socket.

scanner. Enumerates harness processes every 2 s, confirms liveness of registered PIDs, adds strays as unknown.

mesh. Discovers peers, exchanges snapshots, proxies actions to the owning node.

api. Local socket plus a Tailscale listener. Also an MCP server exposing the roster to harnesses.

### R2.1 Process model

rosterd owns no harness process directly. Every headless session is holder to harness to ACP adapter. The holder is a separate small binary shipped with rosterd. It does exactly this.

1. Starts the ACP adapter for the harness as a child with stdio pipes.
2. Listens on a per-session Unix socket (named pipe on Windows) under the node's runtime directory.
3. Relays JSON-RPC both ways between the socket and the child, unchanged.
4. Keeps a bounded replay buffer of the last 256 ACP notifications so a reconnecting rosterd can catch up.
5. Writes a small state file next to the socket with session id, harness, cwd, PID of the adapter, and start time.
6. Exits when the child exits, removing socket and state file.

rosterd on start lists the holder directory, reconnects to every live holder, replays the buffer, and rebuilds those sessions. Holders whose state file exists but whose socket is dead are reported as dead sessions once, then cleaned.

### R2.2 Restart and resume

rosterd restart. Holders keep running. Roster rebuilds in under a second. Nothing is lost.

Holder or harness crash. The scanner notices the PID is gone. The session is marked ended with reason crash. If the session had a stored harness session id, the runner resumes it through ACP session load in a new holder, once, and registers it as a new session identity with the same session id. A second crash within 5 minutes is not resumed.

Machine reboot. Holders are gone. On start rosterd reads the holder state files left behind and restores each one as suspended (R15), resumed when addressed.

## R3. Roster data model

One record per session. Schema marker rosterd.snapshot.v1. Consumers ignore unknown fields.

| field | type | notes |
| --- | --- | --- |
| node | text | node name |
| node_id | text | node public key fingerprint |
| session_key | text | node_id plus pid plus start_ticks. Stable for the life of the process. |
| pid | integer | |
| start_ticks | integer | platform process start time, opaque |
| started_at | timestamp | |
| harness | text | claude, codex, or free text |
| session_id | text or null | harness native session id |
| lane | enum | headless, interactive |
| sources | array | subset of launcher, hook, acp, scan, files |
| name | text or null | display name. From launcher or hook, or set by the agent through the MCP tool. Bound to session_key. |
| activity | enum | active, idle, needs_attention, unknown |
| activity_event | text or null | |
| activity_at | timestamp or null | |
| activity_seq | integer | |
| parent_session_key | text or null | set by scanner from the process tree when the parent is also a harness |
| cwd | text or null | |
| tty | text or null | |
| tmux | object or null | session, window_index, window_name, pane_id |
| herdr | object or null | session, workspace_id, pane_id, agent_name, read from the herdr socket when present |
| holder | object or null | socket path, headless only |
| liveness | enum | live, stale, ended |
| ended_at, ended_reason | | exit, crash, reboot, killed |
| usage | object or null | tokens and cost reported by the harness over ACP when available. Never estimated. |

Nested harness processes of the same session are collapsed into the root as agentd does. A harness process whose parent is a different harness session is a separate record with parent_session_key set. That is the spawned child case.

In-process subagents are not records (R5.4).

## R4. Sources and precedence

Precedence for any single field, highest first: launcher, acp, hook, files, scan. A lower source never overwrites a value set by a higher one, but every source can fill a null.

launcher. A script or tool that starts a session registers it first. POST /local/register with pid, start_ticks, harness, lane, name, cwd, tmux or herdr handle. The launcher exports ROSTERD_SOCKET and ROSTERD_HARNESS to the harness environment.

acp. Everything the runner sees from a session it drives.

hook. SessionStart, UserPromptSubmit, PreToolUse, Stop, Notification for Claude Code. SessionStart, UserPromptSubmit, PreToolUse, PermissionRequest, Stop for Codex. SubagentStop where the harness offers it. The hook script posts to the local rosterd socket when ROSTERD_SOCKET is set or the default socket exists. Fail open, 800 ms, exit 0.

files. Opt-in per node, off by default. Watches the harness session directories and reads only session ids, timestamps, and subagent boundaries. Never message bodies. When enabled the node advertises files_enabled true so clients can show it.

scan. Process enumeration. On Linux /proc. On macOS libproc. On Windows toolhelp. Identity is pid plus start time everywhere. The scanner also runs one bounded tmux list-panes per pass and one herdr pane list when a herdr socket exists, to fill runtime handles.

## R5. Runner

### R5.1 Starting a session

POST /local/sessions with harness, cwd, name, model options, permission_policy, env. The runner creates a holder, waits for ACP initialize and session new, stores the session id, and registers the record with sources acp and launcher.

The launch environment always carries ROSTERD_SOCKET so hooks inside the harness also work. A headless session therefore has both acp and hook as sources. When they disagree, acp wins.

### R5.2 ACP to activity

| ACP traffic | activity | event |
| --- | --- | --- |
| session prompt sent by runner | active | prompt |
| session update, agent message chunk | active | message |
| session update, tool call | active | tool_call |
| session update, tool call update in progress | active | tool_call |
| session request permission received | needs_attention | permission |
| permission answered | active | permission_answered |
| session prompt response returned | idle | turn_end |
| session cancel sent | idle | cancelled |
| adapter exit | ended | exit or crash |

Every transition is a claim with a sequence; clients dedupe on session key plus sequence.

### R5.3 Permission policy

Set per session at start, default inherited from node config.

auto. Every permission request is approved. Activity stays active. This is YOLO.

attention. The request is left pending, activity goes to needs_attention, the human answers through any client. The client action is proxied to the owning node which answers the ACP request. A client that keeps its own decision record reads the pending request from GET /sessions/{key} and answers it the same way.

A session can change policy mid life through PATCH /local/sessions/{key}.

### R5.4 Subagents and children

In-process subagents arrive as ACP tool calls of a subagent kind, or as SubagentStop hooks. They are visible on the session's stream and nowhere else. No roster record.

Spawned children are new sessions. A harness that wants a child calls the MCP tool session spawn (R7.3) or runs the launcher. Either starts a new session with parent_session_key set to the caller.

A child inherits the parent's permission policy unless the spawn call overrides it.

### R5.5 Prompting and reading

POST /local/sessions/{key}/prompt sends a turn. Optional wait_until with idle, needs_attention, or ended and a timeout, mirroring herdr agent wait. GET /local/sessions/{key}/stream is the raw ACP notification stream for clients that render a conversation. rosterd does not store it. The final assistant message of each turn is kept as the recap if the session was started with recap true, readable through GET /sessions/{key}, and nothing else from the transcript.

### R5.6 Ending

POST /local/sessions/{key}/cancel sends ACP cancel. DELETE /local/sessions/{key} stops the holder.

## R6. Local API and MCP

Unix socket at $XDG_RUNTIME_DIR/rosterd.sock on Linux, $TMPDIR/rosterd.sock on macOS, a named pipe on Windows. Mode 0600. Also a loopback HTTP listener on a configured port with a bearer token stored in the node config, for tools that cannot use sockets.

Endpoints, all JSON.

GET /snapshot. Complete node roster.
GET /events. SSE, each event is the complete node roster.
POST /register. Launcher or hook registration.
POST /claim. Activity claim for a session_key or pid plus start_ticks.
POST /name. Set or clear a display name.
POST /sessions, GET /sessions/{key}, PATCH, DELETE, /prompt, /cancel, /stream as in R5.
POST /sessions/{key}/suspend, /resume as in R15; POST /sessions/{key}/export, POST /sessions/import, POST /sessions/{key}/handoff as in R15.5.
GET /swarm/snapshot. Union of this node's roster and every peer's last snapshot, each tagged with node and peer_age_ms.
GET /swarm/events. SSE, complete swarm snapshot on any change anywhere.
GET /swarm/changes. SSE, the swarm snapshot once as event `snapshot`, then one event per change between consecutive frames, in the order records appear: `session_started`, `session_ended`, `session_suspended`, `attention` (an accepted claim landed on needs_attention, sent again for every new claim while it waits, `record.activity_event` names permission, question or login), `attention_cleared`, `activity`, `renamed`, each carrying `at` and the swarm record; `node` (a node joined or changed state) and `node_left` carrying the node. Pure function of two frames, so a client that missed events resyncs from the next `snapshot`.
GET /swarm/nodes. Membership with health.
Any /sessions path under /swarm/{node_id}/ is proxied to that node.

Error bodies are `{error}`: 400 validation, 404 not found, 409 conflict or suspended, 413 transcript over the limit, 429 resume limit with `retry_after_s`, 502 a peer refused or was unreachable, 503 no swarm.

MCP server on the same socket, tool names.

roster.list, roster.watch. Node or swarm scope.
session.name. Rename the caller's own session. The caller is identified by its PID from the socket peer credentials.
session.spawn. Create a child session under the caller, returns its key and record. R5.4.
session.prompt, session.read_state. For coordinator agents. read_state returns activity and the last recap only, never the transcript.
swarm.nodes.

The MCP server is what replaces the shell based agentd skill. Every harness started by the launcher or runner gets it in its MCP config under the name rosterd.

## R7. Swarm

### R7.1 Node identity

At first start a node generates an Ed25519 keypair under its config directory and derives node_id from the public key. The node name defaults to the hostname and must be pinned in config, for the same reason Tightbeam pins TIGHTBEAM_LOCAL_HOST_NAME. A renamed machine is a new name, the id stays.

### R7.2 Discovery

Tailscale first. The node runs tailscale status in JSON mode, takes every online peer, and probes https://<peer tailscale ip>:<port>/node/hello. A peer that answers with a valid hello is a candidate. Static peers in config are probed the same way and are the fallback when Tailscale is absent. mDNS on the local network is optional and off by default.

Hello response: node_id, name, public key, version, swarm_id, capabilities (installed harnesses, files_enabled, herdr present, tmux present), and a signature over the response with the node key.

### R7.3 Membership and joining

A swarm has a swarm_id and a swarm key. The first node creates both. A new node joins with

rosterd join <peer address> --token <invite>

The invite is minted on any existing node with rosterd invite, is single use, expires in one hour, and is signed with the swarm key. The joining node presents it in POST /node/join together with its hello. The admitting node verifies, adds the new node to its membership list, returns the swarm id, the swarm key encrypted to the joiner's public key, and the current membership. The joiner then greets every member. Membership is gossiped: every node includes its membership list in its hello and merges what it hears, newest signed record wins per node_id.

A node can be removed with rosterd revoke <node_id> on any node. The revocation is signed and gossiped. Revoked nodes are refused on hello.

Tailscale ACLs remain the network boundary. The swarm key is the application boundary. A node needs both to be heard.

### R7.4 Snapshot exchange

Each node keeps a long lived SSE connection to every peer's /events, authenticated with a request signed by the node key. It stores the latest complete snapshot per peer with the time it was received. That is the whole protocol. There is no diff format, no vector clock, no merge conflict, because every node is the only writer of its own roster.

On peer loss the last snapshot is kept and served with peer_age_ms growing and peer_state unreachable. Clients render it dimmed, as herdr does. After 24 hours it is dropped. Nothing else changes when a peer is unreachable.

### R7.5 Cross node actions

Any client can send an action for a session to any node. The receiving node looks up the owning node_id from the swarm snapshot and proxies the request over the mesh with its own signature. The owner executes it and the response travels back. Actions are prompt, cancel, permission answer, question answer, name, spawn, suspend, resume, export and handoff. Nothing about the record travels this way; a handoff (R15.5) carries the session's launch facts and transcript, never its record.

### R7.6 Trust

All node to node traffic is HTTPS on the Tailscale interface with a self signed certificate pinned to the node public key, requests signed with the sender's node key, and the swarm key required in a header. Loopback traffic uses the bearer token. Nothing listens on any other interface. A node refuses to start with a listener configured outside Tailscale and loopback.

## R8. Systems above rosterd

Anything that keeps a record over sessions, a project board with tasks and attempts, a task runner, a workspace, is a client and only a client. It pulls: GET /swarm/snapshot and /swarm/events for the roster, /swarm/changes for what happened (an `attention` event is the one to notify on), GET /sessions/{key} for pending requests and the recap, the session action endpoints to answer, prompt and spawn, proxied to the owning node by any node. It maps sessions to its own ids on its side, dedupes claims on session key plus activity_seq, and derives its own attention from the roster. rosterd never dials it, holds no credential for it and carries none of its ids.

## R9. HITL client contract

A client is anything a human uses to see and act: the Omarchy bar widget, a macOS menu bar item, a phone page, a CLI. Every client needs one upstream: any node in the swarm, for live state and actions. GET /swarm/snapshot and /swarm/events for the roster. The session action endpoints for prompt, cancel, permission answer, question answer, and open.

The reference clients ship as three thin pieces: a Quickshell bar module for Omarchy, a menu bar app for macOS, and a single HTML page served by every node at /ui that works on a phone over Tailscale.

The open action resolves the runtime handle. For tmux and herdr it hands the target to the opener script. For headless it opens the conversation view at /ui/sessions/{key}, which renders the ACP stream live and shows the last recap.

Sound, urgency, badges, and toasts remain the client's job. rosterd supplies lists and events only.

## R10. Configuration

One TOML file per node.

```toml
[node]
name = "gibson"
port = 8791
listen = "tailscale"
loopback_port = 8790

[swarm]
static_peers = ["100.64.0.12:8791"]
mdns = false

[runner]
default_permission_policy = "attention"
holder_dir = "~/.local/state/rosterd/holders"
resume_on_crash = true
recap = true

[sources]
files = false
scan_interval_ms = 2000

[harness.claude]
adapter = "claude-agent-acp"
[harness.codex]
adapter = "codex-acp"
```

Keys and tokens live in the config directory with mode 0600. Adapters are pinned by version in a lock file the node writes on first install, as Tightbeam does.

## R11. Cross platform notes

Linux. /proc, systemd user unit for a desktop, system unit for a server with lingering off.
macOS. libproc for pids, paths, start time, tty. LaunchAgent in the GUI session, so open reaches the display, the tmux server and the Herdr socket; a logout ends those anyway. Holder sockets under $TMPDIR.
Windows. Toolhelp for enumeration, process creation time from the handle, named pipes, a service. Interactive lane on Windows is hooks only, no tmux or herdr handles.

The holder, the hook script, and the opener are the only platform-conditional code. Everything else is shared.

## R12. Not in scope

Terminal rendering. Task or decision history. Rules or gates over agent behaviour. Credential management for harnesses. Cross swarm federation. Relay for machines that cannot reach each other over Tailscale. Multi user tenancy on one node.

## R13. Acceptance

1. Start a headless Claude Code session through POST /sessions. The roster shows lane headless, sources acp and hook, activity moving prompt to tool_call to turn_end with increasing sequences.
2. Kill rosterd during an active turn. Restart it. The session is still live, the turn completes, no claim is lost.
3. Kill the holder. The session ends with reason crash, a resumed session appears with the same session_id under a new session_key.
4. Start claude by hand in tmux with the hook installed. The roster shows lane interactive, source hook and scan, the tmux handle.
5. Two nodes on Tailscale. Join the second with an invite. Both /swarm/snapshot responses list both rosters within 2 s of any change on either node.
6. Disconnect the second node from Tailscale. The first node keeps serving the last snapshot with peer_state unreachable and a growing age. Reconnect. The stale snapshot is replaced.
7. From node A, prompt a session owned by node B. The prompt reaches B, the claim appears on both nodes.
8. A session with permission policy attention requests a tool. The pending request is on GET /sessions/{key} within 1 s, answering it from another node answers the ACP request, and the session continues.
9. A session calls session.spawn. A child session exists with parent_session_key set.
10. A subagent runs inside a session. No new roster record appears.
11. Revoke node B. Its hello is refused on every node and its snapshot is dropped.
12. On macOS and Linux, the same test suite passes. Windows passes items 1, 2, 3, 8, 9, 10.


---

# Addon: CLI, session lifecycle, and pi harness

Status: extends the rosterd and swarm spec with sections R14, R15, and R16. Where this addon conflicts with the earlier text, this addon wins.

Audience: the build team (agents). Build as written, take the stated defaults.

---

# rosterd additions

## R14. Command line interface

One binary, `rosterd`, serves both the daemon and the CLI. `rosterd` with no subcommand prints help. `rosterd daemon` runs the service. Everything else is a client command that talks to the local socket unless told otherwise.

### R14.1 Targeting

Every read command accepts a scope.

| flag | meaning |
| --- | --- |
| none | this node |
| `--node NAME` | one named node in the swarm, proxied through the local node |
| `--swarm` | every node the local node knows, including unreachable ones with their age |

Every command that acts on a session takes a `KEY`. KEY resolves in this order: an exact session_key, a PID on the local node, a unique display name in the current scope. An ambiguous name is an error listing the candidates. Never guess.

### R14.2 Output

Every command accepts `--json`. The JSON is the same frame the API returns for that resource, byte for byte where the API has one, so scripts and the MCP skill never parse tables. Without `--json` the output is a human table or a short text block. Exit code is 0 on success, 1 on a user error, 2 when the daemon is unreachable, 3 when a swarm peer was needed and unreachable.

`list` and `watch` must answer when the runner or the mesh is broken. They read the roster module only.

### R14.3 Commands

Reading.

```
rosterd list [--swarm|--node N] [--json]
rosterd watch [--swarm|--node N] [--json]
rosterd status [--json]
rosterd nodes [--json]
rosterd read KEY [--json]
rosterd explain KEY [--json]
```

`list` prints one row per session. Columns, in order: name or the cwd basename fallback in brackets, harness, activity, updated (age of the last accepted claim, never time spent working), lane, node, pid. With `--swarm` a header block first shows per node counts of active, idle, needs attention, unknown, and the node's reachability. Unreachable nodes are printed dimmed with `stale Ns` in the node column. Sessions in state suspended (R15) show activity `suspended` in place of the four words and their session_id instead of a pid.

`watch` prints the complete table again on every change, or the complete JSON frame per line with `--json`. Ctrl+C stops it.

`status` shows the node name and id, version, listeners, swarm id, peer count and reachability, holder count, and which sources are enabled.

`nodes` lists membership: name, node id, address, version, capabilities, reachability, last hello age, revoked flag.

`read` shows one session in full: every roster field, the runtime handle, the last recap if one exists, pending permission request if any, and the child sessions. It never shows the transcript.

`explain` shows, for each field of a session, which source set it and when, and which claims were rejected and why. This is the truthfulness view.

Sessions.

```
rosterd start --harness H --cwd DIR [--name LABEL] [--policy auto|attention] [--model M] [--effort E] [--env K=V ...] [--json]
rosterd prompt KEY TEXT [--wait idle|needs_attention|ended] [--timeout SECONDS] [--json]
rosterd cancel KEY
rosterd stop KEY
rosterd suspend KEY
rosterd resume KEY
rosterd handoff KEY --to NODE [--json]
rosterd name KEY LABEL
rosterd name KEY --clear
rosterd open KEY
rosterd allow KEY [--always]
rosterd deny KEY [--reason TEXT]
rosterd spawn KEY --harness H --cwd DIR [--name LABEL] [--json]
```

`start` creates a headless session through the runner (R5.1) and prints the session key.

`prompt` sends one turn. With `--wait` it returns when the session reaches the named activity or ended, printing the final activity and the recap if any. Default timeout 600 s. Timeout exits 1 with the current activity.

`cancel` sends ACP cancel and leaves the session live. `stop` ends the holder. `suspend` and `resume` are R15. `handoff` is R15.5: NODE by name or id, prints `<name> → <node> <new session_key>`.

`open` resolves the runtime handle and calls the opener for tmux and herdr, or prints the /ui URL for headless sessions and opens it with the platform opener when a display is present.

`allow` and `deny` answer the pending permission request of a session in policy attention. `--always` answers with the allow-always option when the harness offers it. Both are proxied to the owning node when KEY lives elsewhere.

`spawn` creates a child session under KEY with parent_session_key set, following R5.4. It is the same operation as the MCP tool session.spawn.

Swarm.

```
rosterd invite [--ttl MINUTES]
rosterd join ADDRESS --token TOKEN
rosterd revoke NODE_ID
rosterd leave
```

`invite` prints a single use token, default one hour. `join` performs R7.3 and prints the swarm id and the membership. `revoke` signs and gossips a revocation. `leave` revokes the local node and clears its swarm config.

Integrations.

```
rosterd integrate install claude|codex|pi
rosterd integrate uninstall claude|codex|pi
rosterd integrate status
```

Install writes the hook declarations for Claude Code and Codex (only marked entries are ours, everything else is kept, atomic writes), and installs the pi extension (R16). Uninstall removes only marked entries. Status shows, per harness, whether the binary is on PATH, its version, whether the adapter is present and at which pinned commit, and whether the hooks or extension are installed and current. Both install and uninstall are byte idempotent on the second run.

Maintenance.

```
rosterd daemon [--config PATH]
rosterd doctor
rosterd version
```

`doctor` checks the socket, listeners, Tailscale presence, swarm key, holder directory permissions, each harness binary and adapter, and prints one line per check with pass or a fix command. It never changes anything.

### R14.4 Skill

The skill shipped with rosterd for agents is a short document that lists these commands with `--json`, explains the four activity words and suspended, and states the one rule agents must follow: never parse the human tables. The MCP server (R6) remains the preferred path. The CLI is for shells, SSH, and scripts.

## R15. Session lifecycle policy

A headless session that has nothing to do still holds a harness in memory. The runner manages this with a third session state.

### R15.1 States

live. Holder running, harness process alive. Activity is one of the four words.

suspended. Holder stopped on purpose, session_id kept. No process exists. The roster keeps the record with lane headless, state suspended, and everything needed to resume.

ended. Holder stopped and the session will not come back under this key. Reason exit, crash, reboot, killed, expired, or handed_off (R15.5).

Interactive sessions are never suspended by rosterd. They end when their process ends.

### R15.2 Idle timeout

Config per node with per session override at start.

```toml
[runner]
idle_timeout_s = 1800
suspend_on_needs_attention = false
resume_on_prompt = true
max_resumes_per_hour = 6
```

A live headless session whose activity has been idle for idle_timeout_s is suspended. A session in needs_attention is not suspended unless suspend_on_needs_attention is true, because a pending ACP permission request cannot survive the harness.

`rosterd suspend KEY` and `rosterd resume KEY` do the same by hand.

### R15.3 Resume on demand

A suspended session is brought back by the runner when any of these happens.

1. A prompt arrives for it, from the CLI, the MCP tool or a client. The runner starts a new holder, calls ACP session load with the stored session_id, waits for it to settle, then delivers the prompt. The caller sees one prompt call that takes a little longer.
2. `rosterd resume KEY`.

Each resume creates a new session_key with the same session_id; the old key ends with reason suspended. Resumes are counted per hour, and a session over max_resumes_per_hour stays suspended with a warning until the window passes.

The runner never resumes on a timer. There is no wake up schedule. This keeps the rule that nothing in the runtime layer schedules agents.

### R15.4 Reboot and crash

R2.2 stays as written with one change. On reboot, sessions found in holder state files are restored as suspended, not resumed. They come back when something addresses them. Crash resume within five minutes stays as written because a crash mid turn is not idleness.

### R15.5 Handoff

A suspended session can move to another node of the swarm and continue there with the same harness session id. The repository must exist at the same path on the target: the caller's responsibility, the target refuses with 409 when the cwd is not there.

`POST /sessions/{key}/handoff {"node": "<name or id>"}` on the owning node (proxied like any other action when KEY lives elsewhere), in this order.

1. Suspend. A live session is suspended first (R15.2 by hand); a suspended one is used as is.
2. Export. The node builds a `SessionExport`: `schema` `rosterd.session_export.v1`, `harness`, `cwd`, `session_id`, `meta` (the holder's launch facts verbatim: name, policy, model, env, effort, idle timeout), `name`, and `transcript`. The record stays suspended here.
3. Import. The export goes to the target as `POST /sessions/import` over the mesh. The target checks the cwd, writes the transcript under its own home at the same relative path (a file already there must be byte for byte the same; a differing one is never overwritten, 409; a path that is not plainly relative is refused, 409), then starts a new holder and loads the session as a resume does (R15.3), under a new key on that node. 201 with the new record.
4. End here. Only once the import answered 2xx does the origin end the record with reason `handed_off` and delete its state file, so the session cannot be resumed twice. The answer is `{from, to}`: the old record, ended, and the new one, live on the target.

A failed import (unreachable target, 409, anything but 2xx) leaves the session suspended on the origin and answers 502 with the target's reason. Nothing was started elsewhere; `resume` still works here.

The transcript rule. Resume is ACP `session/load`, which reads the harness's own local transcript, so the export carries it and the import puts it where the harness will look. Claude Code: `~/.claude/projects/<cwd with every / and . replaced by ->/<session_id>.jsonl`. Codex: `~/.codex/sessions/YYYY/MM/DD/rollout-<stamp>-<session_id>.jsonl`, found by its suffix. pi and any other harness: `transcript` is null and the import only works when the harness needs nothing local. A transcript over 32 MiB does not travel: 413. rosterd never reads what is in the file; it moves it.

`POST /sessions/{key}/export` alone does steps 1, 2 and 4 at once: the caller carries the export away and the record ends `handed_off` immediately. `POST /sessions/import` alone is step 3 and works on the local socket, loopback and the peer listener alike.

The new record has a different `node` and `session_key` and the same `session_id`. Clients follow the harness session id across a handoff, as they do across a resume.

## R16. pi as a harness

pi is the third harness after Claude Code and Codex.

### R16.1 Headless lane

Adapter: a pi ACP adapter wrapping `pi --mode rpc`, pinned by git commit in the node lock file, not by version range, because the adapters are community maintained and move. The adapter is spawned under a holder like the others. ACP to activity mapping (R5.2) applies unchanged. Resume uses pi's own session resume through the adapter's session load.

Permission policy. Current pi adapters do not forward pre-execution tool intents, so ACP request permission never fires. A pi session under policy attention behaves as auto, and `rosterd start` prints a warning saying so. The pi extension in R16.2 closes this when installed.

### R16.2 Interactive lane and gating

pi has no hooks file. It loads TypeScript extensions at start. `rosterd integrate install pi` installs one extension, `rosterd-pi`, into pi's extension directory. It does four things.

1. On session start, posts a registration to the rosterd socket with pid, session id and cwd. Same payload as the hook.
2. On turn start and before each tool call, posts active. On turn end, posts idle. Same claims as the hook mapping.
3. When rosterd is unreachable, prints one line and continues. Fail open, 800 ms.
4. Optional gating. When the session environment carries ROSTERD_GATE=attention, the extension pauses before a tool call, posts needs_attention with the tool name and an argument summary, and waits for allow or deny from rosterd; `rosterd allow` or `deny` answers it. A wait longer than gate_timeout_s (default 3600) denies the call and reports it.

The extension is the pi equivalent of both the hook script and the missing ACP permission request. When it is present, pi sessions get both permission policies, in both lanes.

### R16.3 Scanner and identity

Nothing special. pi is a Node process. Identity is pid plus start time. `rosterd integrate status` reports the pi binary, its version, the adapter commit, and whether the extension is installed and current.

### R16.4 Configuration

```toml
[harness.pi]
adapter = "pi-acp"
adapter_commit = "<sha>"
extension = true
gate_timeout_s = 3600
```

---

## R17. Acceptance additions

1. A headless session idles past idle_timeout_s. rosterd shows it suspended with no pid, activity idle.
2. `rosterd prompt KEY "continue"` on that session resumes it under a new session key with the same session_id and delivers the prompt.
3. Reboot the node. Every headless session comes back as suspended, none is resumed until addressed.
5. `rosterd list --swarm --json` on node A equals GET /swarm/snapshot on node A byte for byte.
6. `rosterd list` works with the mesh stopped.
7. A pi session started through `rosterd start` with policy attention prints the warning and runs as auto when the extension is absent. With the extension installed and ROSTERD_GATE=attention, a tool call blocks until `rosterd allow KEY`.
8. `rosterd integrate install pi` twice, then uninstall twice. Second runs change zero bytes.
9. `rosterd explain KEY` on a session with both acp and hook sources shows acp winning every field they both set.
10. `rosterd doctor` on a fresh machine lists every missing harness and adapter with the command to install it, and changes nothing.