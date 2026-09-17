---
name: rosterd
description: Read and drive coding agent sessions on this machine and its swarm through the rosterd CLI. Use the MCP server (rosterd, R6) when it is configured; the CLI is for shells, SSH and scripts.
---

# rosterd

One daemon per machine keeps the roster: every coding agent session and its activity, on this
machine and across the swarm. The MCP server is the preferred path (`roster.list`,
`roster.watch`, `session.prompt`, `session.read_state`, `session.spawn`, `session.name`,
`swarm.nodes`). From a shell, use the CLI below, always with `--json`.

## One rule

Never parse the human tables. Pass `--json`: the output is the API frame for the resource,
byte for byte. `rosterd list --json` is GET /snapshot, `rosterd list --swarm --json` is
GET /swarm/snapshot, `rosterd watch --json` is one complete frame per line.

## Activity

Every record carries one of four words, from the last accepted claim, never a guess:

| word | meaning |
| --- | --- |
| `active` | a turn is running or a tool is being called |
| `idle` | the turn ended, waiting for input |
| `needs_attention` | a permission request, a question or a login is waiting on a human |
| `unknown` | no claim yet |

A record with `liveness: "suspended"` is a headless session whose holder was stopped on
purpose (idle timeout or `rosterd suspend`): no process, `session_id` kept. A prompt resumes
it under a new `session_key` with the same `session_id`. `liveness: "ended"`
records stay in the snapshot for a while with `ended_reason`; `handed_off` means the session
went on under the same `session_id` on another node.

## Commands

KEY is an exact `session_key`, a PID on this node, or a unique display name (name, else the cwd
basename). Ambiguous names exit 1 listing the candidates. Exit codes: 0 ok, 1 user error,
2 daemon unreachable, 3 swarm peer needed and unreachable.

```
rosterd list [--swarm|--node N] --json          GET /snapshot or /swarm/snapshot
rosterd watch [--swarm|--node N] --json         one frame per line on every change
rosterd changes --json                          GET /swarm/changes: the snapshot, then one
                                                change per line (`event`: session_started,
                                                attention, attention_cleared, activity, ...)
rosterd status --json                           node, listeners, swarm, counts
rosterd nodes --json                            membership and reachability
rosterd read KEY --json                         the record, runtime state, pending request, children
rosterd explain KEY --json                      which source set each field; rejected claims
rosterd start --harness H --cwd DIR [--name L] [--policy auto|attention] [--model M]
              [--effort E] [--env K=V ...] --json
rosterd prompt KEY TEXT [--wait idle|needs_attention|ended] [--timeout S] --json
rosterd cancel KEY | stop KEY | suspend KEY | resume KEY --json
rosterd handoff KEY --to NODE --json            move a suspended or live session to NODE
                                                (name or id); `{from, to}`, same session_id,
                                                new session_key on NODE; the cwd must exist there
rosterd name KEY LABEL | name KEY --clear
rosterd allow KEY [--always] | deny KEY [--reason TEXT]
rosterd spawn KEY --harness H --cwd DIR [--name L] --json
rosterd open KEY                                jump to the session (tmux, herdr, /ui)
```

`prompt --wait` returns when the session reaches that activity or ends, with `reached`,
`stop_reason`, `recap` and the record; a timeout exits 1 with the current activity. `read`
never shows the transcript.
