---
name: rosterd
description: Read and drive coding agent sessions on this machine and its swarm through the rosterd CLI. Use the MCP server (rosterd, R6) when it is configured; the CLI is for shells, SSH and scripts.
---

# rosterd

One daemon per machine keeps the roster: every coding agent session and its activity, on this
machine and across the swarm. The MCP server is the preferred path (`roster.list`,
`roster.watch`, `session.prompt`, `session.read_state`, `session.send`, `session.find`,
`session.spawn`, `session.name`, `swarm.nodes`). From a shell, use the CLI below, always with
`--json`.

## Talking to other agents

Name yourself with `session.name`. Spawn children with `session.spawn` (their
`parent_session_key` is you). Send any session work with `session.send { to, prompt,
wait_until?, timeout_ms? }`: `to` is a `session_key`, a display name (the name, else the cwd
basename, with or without brackets) or a pid on this node, anywhere in the swarm; it waits
until the target is idle (120 s by default) and answers with `session_key`, `node`, `reached`,
`stop_reason`, `recap`, `activity` and what the target left `pending`. An ambiguous name is 409
with the candidates, no match is 404, and you cannot send to yourself (400). `session.find
{ name }` resolves without sending. rosterd relays; it never schedules. From a shell,
`rosterd prompt NAME TEXT --wait idle --json` is the same call, `POST /send` on the socket too.

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
records stay in the snapshot for a while with `ended_reason`.

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
rosterd name KEY LABEL | name KEY --clear
rosterd allow KEY [--always] | deny KEY [--reason TEXT]
rosterd spawn KEY --harness H --cwd DIR [--name L] --json
rosterd open KEY                                jump to the session (tmux, herdr, /ui)
```

`prompt --wait` returns when the session reaches that activity or ends, with `reached`,
`stop_reason`, `recap` and the record; a timeout exits 1 with the current activity. `read`
never shows the transcript.
