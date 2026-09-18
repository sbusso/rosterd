# rosterd design

What rosterd is, why it is shaped this way, what was looked at and left aside, and what comes
next. SPEC.md is the contract (R0 to R19); this document is the reasoning behind it, written on
2026-09-17 at 0.1.12 after the herdr removal and the mesh compatibility work. When the two
disagree the spec wins and this document is out of date.

## 1. The idea

Coding agents run as terminal programs on several machines. Some a human started in a terminal
and talks to; some a tool started and drives over a protocol. Any of them can stop and wait for a
person: a permission, a question, a login. The person is at a laptop, a desktop, or a phone.

rosterd is the roster and the remote control for those sessions. One daemon per machine knows
every session on it, whoever started it. The daemons form a swarm over Tailscale, so any one of
them answers for all. From any node, phone included, a person sees what is running and what is
waiting, answers it, and reaches the terminal of an interactive session.

Two lanes, one contract. A headless session is one rosterd started over ACP and can prompt,
cancel, answer, suspend, resume, hand off to another node. An interactive session is one a human
owns in tmux, which rosterd observes through hooks and a process scan and can open. Both are one
record with a state, a handle, and actions proxied by any node to the owning node.

What rosterd is not: a task board, a terminal, a multiplexer, a credential store, a policy engine.
Everything that keeps history over sessions is a client of its API. Everything that draws a
terminal is a client of its API.

## 2. Principles

1. One service per machine, one package. `brew install rosterd && brew services start rosterd`
   is the whole Mac install; a tarball and a script on Linux. Any fix lives in the binary or the
   release flow, never in per-machine setup: no sidecars, no `tailscale serve`, no extra launchd
   agents, no firewall scripts. The user ruled this twice; it is the pitch.
2. Independent of anything above it. rosterd never learns a project board's URL, token, or ids.
   The workspace is one client among others and pulls; rosterd never dials a client except
   through a URL the client registered (R19). The earlier bridge to the workspace was an
   antipattern and is gone.
3. Observe, never infer. Activity comes from hooks and ACP, never from CPU, time, or screen text.
   rosterd never reads prompts, transcripts, or terminal screens. The one exception is `/usage`,
   which reads the usage fields of the harnesses' own transcript files and nothing else.
4. Full snapshots, no deltas as the source of truth. Every answer is the whole roster; events
   are a convenience over it. A client that lost its stream resyncs from one GET.
5. No leader, no shared database. Each node owns its roster and keeps the last snapshot from
   every peer. A node is never wrong about itself.
6. Tailscale and loopback only. Nothing listens elsewhere; a node refuses to start otherwise.
   Trust is the swarm key plus per-node Ed25519 identities.
7. The daemon survives its work. Holders keep harness pipes across a daemon restart; tmux keeps
   interactive sessions across everything. Restart rebuilds the roster in under a second.
8. Clients are thin and do not explain themselves. A page, a tray, a phone app, a CLI, each a
   list and the actions; no footers, hints, or provenance text in any of them.
9. Cross platform from the start: Linux (systemd), macOS (launchd), Windows deferred but not
   designed out.

## 3. What exists today

Three Rust crates plus clients and packaging, about 16k lines of Rust, 89 commits since the
repository was cut out of the workspace on 2026-09-16, released through 0.1.12.

| piece | what |
| --- | --- |
| `crates/proto` | shared types: `Record`, `Snapshot`, `Capabilities`, `PeerState`, holder state |
| `crates/holder` | `rosterd-holder`: owns one ACP adapter's stdio, relays over a socket, replay buffer, state file |
| `crates/rosterd` | the daemon: `roster`, `scanner`, `runner`, `mesh`, `api`, `journal`, `usage`, `hooks`, `gate`, `setup`, `doctor`, `integrate`, `cli` |
| `crates/tray` | menu bar roster on macOS and Linux, a native menu over the CLI |
| `ui/index.html` | the one page every node serves at `/ui`, phone-sized, QR pairing for iOS |
| `ios/` | the phone app, a 7-day Personal Team build today |
| `scripts/` | `rosterd-hook` (harness hooks to claims), `rosterd-launch` (register then exec), `rosterd-open` (tmux here or over ssh) |
| `extensions/` | the pi extension that posts tool intents for gating (R16.2) |
| `skill/` | the CLI summary agents load |
| `packaging/` | build, sign, systemd unit, launchd plist, Arch PKGBUILD, Homebrew formula in `Formula/` |

Shipped behaviour, by spec section: roster and precedence (R3, R4), ACP runner with holders,
policy, resume (R5, R2.2), local socket and MCP (R6), swarm discovery, membership, snapshot
exchange, proxied actions, trust, harness health (R7), clients and attention notifications (R9),
CLI (R14), lifecycle with idle timeout, resume, reboot, handoff (R15), pi as a harness (R16),
journal with replay (R18), webhooks (R19), usage roll-up, setup and doctor.

## 4. Concepts

node, swarm, peer, lane, holder, source, roster, record: R1.

activity. Four words, `active`, `idle`, `needs_attention`, `unknown`; a claim carries the word,
the event that caused it, and a sequence number. `attention` is the event every client notifies
on, deduplicated on `session_key` plus `activity_seq`.

policy. `auto` answers every permission; `attention` leaves it pending and marks the session.
Set at start, changeable mid life. A pending request is readable so a client with its own
decision record answers it the same way.

handle. What a client opens. For interactive sessions a tmux target (session, window, pane); for
headless ones the conversation view of the page. Handles are the one thing the record carries
about how to reach a session.

health. Per harness per node: `login_required`, `rate_limited`, `broken`, each with a deadline,
so a coordinator skips a node whose harness cannot work right now. Observation only.

journal. What rosterd saw and what its API did, per node, append-only, replayable into the
event stream by sequence. Never what a session worked on.

usage. Tokens and cost from the harnesses' own files, rolled up per day and model; subscription
windows when a harness reports them. Never estimated.

handoff. A suspended headless session exported (harness, cwd, session id, launch facts,
transcript) and imported on another node that has the same repository path, resumed there,
ended here only after the import answered.

hooks. URLs a client registers for event names, called in order with a bearer token the client
chose. The push channel that keeps R8 true.

compatibility. `MIN_COMPAT` in the binary; a peer below it is `incompatible` in the node list,
and gets 426 from every mesh route, instead of being silently unreachable.

## 5. Decisions and their reasons

Each entry is the decision, the alternative it beat, and why. Newest last.

ACP for the headless lane, not each harness's own CLI or SDK. One client drives Claude Code,
Codex, pi and whatever the registry adds; adapters are pinned by commit in a lock file because
they are community maintained and move.

A holder process per headless session, not pipes held by the daemon. A daemon restart, an
upgrade, a crash must not kill sessions. The holder is a few hundred lines and ships in the same
package, so it costs nothing at install.

Hooks and a scan for the interactive lane, not screen scraping. Harnesses already emit lifecycle
hooks; the scan catches strays as `unknown`. Reading screens would be inference (principle 3).

Full snapshot exchange between peers, not a CRDT or a log. The roster of one node is small and
owned by that node; the simplest correct thing is to resend it.

Signed hellos verified over the received bytes, not over a re-serialisation. 0.1.10 added a
field, older peers re-serialised the hello without it and every signature failed with 401. Now
the signature covers the canonical JSON exactly as received, unknown fields ride along, and a
`MIN_COMPAT` floor turns a too-old peer into a visible `incompatible` state with 426 rather than
a mystery. Unparsable versions are never old, so dev builds keep talking.

Mac binaries signed with the Apple Development identity in the release flow. An ad hoc
signature is a new program to the application firewall every build, so each upgrade was blocked
until someone clicked Allow on the machine. A stable identity keeps the rule. Signing runs in the
GUI session because `codesign` cannot see the login keychain from ssh; the script hands itself
to a Terminal window and waits. This is a release-flow fix, not a machine fix (principle 1).

tmux is the one interactive substrate; herdr is gone. Herdr was a second daemon per machine with
its own session model and it did not work. tmux is already on every node, holds sessions across
disconnects, and gives everything a client needs: `capture-pane` for the screen, `send-keys` for
input, `pipe-pane` for a live tail, `attach` for a real terminal. Handles are tmux targets; open
is `tmux attach` here or over ssh.

The journal is a lifecycle log, not a memory. Clients keep history; a daemon that stored what
sessions worked on would become the board it refuses to be.

Webhooks over a push protocol. A URL and a bearer token the client chose keep rosterd ignorant of
what it calls.

Workspace decoupled. The bridge, the `[workspace]` config, the decision policy and attempt ids
were removed; the workspace is a puller against the same API as every client.

Homebrew bottle, not a source build. A Mac installs without a Rust toolchain; the bottle pour
keeps the signature.

## 6. Landscape

What was looked at while deciding the terminal lane and the shape of the whole, and what was
taken from each. Stars and dates as of 2026-09-17.

### Control planes and session managers

| project | what it is | relation to rosterd |
| --- | --- | --- |
| vibe-kanban (BloopAI, 28k) | board plus worktrees plus agent runs, Rust | the board rosterd refuses to be; a natural client of it |
| claude-squad (smtg-ai, 8.5k) | tmux-backed TUI to run several agents in worktrees, Go | same substrate choice (tmux), single machine, no daemon, no protocol |
| agent-deck (908) | terminal session manager for several harnesses | same, one TUI, one machine |
| termic (271), Conductor.build | desktop app running the real CLIs in real terminals | a front, Mac only, one machine |
| harnss (377), acp-ui (478) | desktop and web ACP clients | the conversation view rosterd's page has, without the roster or the swarm |
| acpx (openclaw, 3.3k) | headless CLI client for stateful ACP sessions, TypeScript | overlaps the runner plus holder; not adopted because it would add a node runtime to a one-binary install |
| termio (termio-sh, 515) | Swift daemon and CLI with working/idle/needs-you, hooks, tray, iPhone mirror, remote hosts by copying one binary over ssh | the closest cousin: same three states, same daemon idea. Mac only, ssh fan-out instead of a mesh, its own session store |
| cmux (manaflow-ai, 27k) | Ghostty-based macOS terminal with notifications for agents, CLI and socket API | a front: `ROSTERD_TERMINAL_CMD` can hand tmux targets to it |
| ccmux (Alec-Raymond, shell, macOS) | one tmux window per agent, a sidebar pane, hooks to state and macOS alerts that return to the exact window, a usage meter, Ghostty optional | the single-machine version of rosterd's interactive lane: same substrate, same hook source, same refusal to read prompts; no daemon, API or mesh. Worth copying: the board (one tmux session, a sidebar drawn from `rosterd list`) and alert click → `rosterd-open` |
| cove, ccs | Claude Code session managers over tmux | confirm the pattern; one machine each |
| agentd, agentd-hub, Tightbeam | the tools rosterd borrowed from (R0) | vocabulary, hook contract, gateway idea; hub replaced by the swarm |

Reading: every project in the category picks one of three things, a board, a terminal front, or
a single-machine manager. None is a daemon with a mesh and a protocol under all of them; that is
the gap rosterd fills, and the reason it must stay under, not beside, the others.

### Terminal substrates and remote terminals

| project | what it is | relation to rosterd |
| --- | --- | --- |
| tmux | the multiplexer already on every node | the substrate, decided |
| zmx (neurosnap, 2.1k, Zig) | attach/detach per session, libghostty-vt, no panes, `attach/run/send/history/tail/wait/kill` | tempting for its `wait` and `history`, but a second substrate on every node; not adopted while tmux gives the same through `capture-pane` and `pipe-pane` |
| boo (coder, 785, Zig) | GNU screen style multiplexer on libghostty | same reasoning as zmx |
| zellij web | `zellij web`, one URL per session, tokens, TLS, `zellij attach https://…` | a whole feature rosterd would otherwise build, but requires zellij as the substrate and per-machine tokens and TLS (principle 1) |
| wezterm mux | mux domains, headless server, `wezterm cli spawn/list/send-text` | a substrate tied to one terminal |
| remux (h3nock, 493, Swift) | iOS client for remote tmux over ssh, GhosttyKit | works today against any node's tmux; no URL scheme, so the page cannot hand it a target |
| restty, wispterm, go-libghostty | libghostty-vt in the browser and in Go | show that a client can render a byte stream with Ghostty's VT; the page could, later |
| Superlogical (Mitchell Hashimoto, pre-alpha demo 2026-09-08) | persistent local and remote sessions, a Rex server as a full system login over Tailscale identity, Mosh-like transport, injected CLI; closed | if it ships as described it replaces ssh plus tmux for the attach path; rosterd's contract (a session key, an attach stream) survives the swap. Watched, not waited for |
| ACP SDKs | official Rust, TypeScript, Python, Kotlin, Java; Go by coder; the one Zig SDK has 4 stars and stopped in April | no reason to leave Rust for the runner |

Reading: the substrate question is settled by what is already installed. The attach question is
not about the multiplexer but about the transport: today ssh, later maybe Superlogical. rosterd
should own the endpoint and not the transport.

## 7. The terminal lane, decided

The gap after the herdr removal: from the phone, open does nothing for an interactive session,
because open means `ssh node -t tmux attach` and a phone has no ssh. The fix is to carry the
terminal over the mesh rosterd already has, still without rendering anything.

1. Interactive sessions start in tmux. `rosterd start --interactive [--node N]` has the daemon
   there run `tmux new-session` around rosterd-launch, which registers the pane's pid and
   execs the harness; the scanner attaches the handle on its next pass, so the record has it
   from birth. `rosterd claude` is the short form: here, this directory, joined at once, and
   the pane title the harness sets is the record's name until someone names it. From a shell,
   `rosterd-launch --tmux` does the same in place. A human-started session keeps being found
   by the scanner as before. Shipped in 0.1.19, 0.1.21 and 0.1.22.
2. Attach over the mesh. `GET /sessions/{key}/attach` upgrades to a websocket; the owning node
   runs `tmux attach -t <target>` in a PTY and relays bytes both ways, with resize; any node
   proxies it like every other session route. `rosterd attach KEY` is the raw-mode client; the
   page renders it with xterm.js; the phone renders it with GhosttyKit. rosterd relays bytes and
   never interprets them (principle 3 holds: relaying is not reading). Shipped in 0.1.18.
3. Fronts stay optional. cmux or any terminal through `ROSTERD_TERMINAL_CMD` for the desktop;
   remux for the phone against the same tmux until the app renders itself.

Not built: a multiplexer, a screen model, a scrollback store, a VT parser in the daemon.

The other lane, decided the same day: rosterd is an ACP proxy (R20). The pty lane carries a
harness's own UI; the ACP lane carries the protocol, and any client, an editor, the workspace,
a phone card, a script, talks to every headless session on every node through one `rosterd acp`
whose session ids are roster keys. `session/list` tells a client what is behind the connection;
`session/new` names the node and harness in `_meta.rosterd`. Fan-out is the point: several
clients hold one session, each sees every update, the first answer to a permission wins. The
workspace becomes one such client, and gets updates pushed instead of asking agents to report.
A loaded session replays its conversation from the harness's own transcript, read once at load;
the daemon still stores none of it, the file is the harness's and the memory is the client's.

## 8. Roadmap

In order, each shippable alone.

1. `rosterd start --interactive` and `rosterd-launch --tmux` (7.1), done.
2. `/sessions/{key}/attach`, `rosterd attach`, xterm.js in the page (7.2), done.
3. The ACP proxy: `rosterd acp` and `/sessions/{key}/acp`, the swarm as one agent with the
   roster key as session id, fan-out to many clients, done in 0.1.24; history replay on load
   from the harness's transcript in 0.1.26.
4. iOS rendering on GhosttyKit; the page's QR pairing already gives the phone its node.
5. Then, from the spec's acceptance list and the deferred items: Windows service, the Linux
   install as a package, `MIN_COMPAT` bumps as part of the release checklist.

Operational now: every node must run 0.1.11 or later (the compatibility floor); the MacBook and
omarchy64 need upgrading, and each Mac accepts the firewall prompt once for the signed identity.

## 9. Open questions

- Terminal input from any client. Attach gives keystrokes to whoever holds the swarm key. That
  is the existing trust model (R7.6) and the same power `rosterd prompt` already has, but it is
  worth saying out loud before the phone can type into a shell.
- tmux as a dependency. The formula and the Linux script could depend on tmux so `--interactive`
  never fails on a fresh machine; one line, no per-machine step.
- The page's weight. xterm.js is ~300 KB; the page is one inline HTML file. Either inline it or
  let the daemon serve it as a second file; both keep one binary.
- Superlogical. When it ships, decide whether attach's transport swaps to it, node by node,
  behind the same endpoint.
- Zig. libghostty is the renderer clients will use; nothing in the daemon needs it. Building on
  it only makes sense on the client side, in the phone app and maybe the page.
- iOS distribution. A 7-day Personal Team build is fine for one person; TestFlight is the step
  after, and does not change the daemon.
