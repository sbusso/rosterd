// The one check: the daemon's frames decode. `swiftc Rosterd/Models.swift check/main.swift -o /tmp/check && /tmp/check [swarm.json]`
import Foundation

let swarm = """
{"schema":"rosterd.swarm.v1","generated_at":"2026-09-16T10:00:00.123456Z",
 "nodes":[{"node_id":"n1","name":"mato","address":null,"state":"local","peer_age_ms":0,"version":"0.1.0","capabilities":{"harnesses":["claude","codex"],"files_enabled":false,"tmux":true},"revoked":false}],
 "records":[{"node":"mato","node_id":"n1","session_key":"n1:12:34","pid":12,"start_ticks":34,"started_at":"2026-09-16T09:00:00Z","harness":"claude","session_id":null,"lane":"headless","sources":["launcher"],"name":null,"activity":"needs_attention","activity_event":"gate:bash","activity_at":"2026-09-16T09:59:00.5Z","activity_seq":7,"parent_session_key":null,"cwd":"/Users/mato/Code/gtm/workspace","tty":null,"tmux":null,"holder":{"socket":"/tmp/h.sock"},"liveness":"live","ended_at":null,"ended_reason":null,"usage":null,"load":{"cpu_pct":12,"rss_mb":1536},"permission_policy":"attention","peer_state":"local","peer_age_ms":0}]}
"""
let session = """
{"node":"mato","node_id":"n1","session_key":"n1:12:34","pid":12,"start_ticks":34,"started_at":"2026-09-16T09:00:00Z","harness":"claude","lane":"interactive","sources":["hook"],"activity":"needs_attention","activity_seq":7,"liveness":"live",
 "state":{"session_key":"n1:12:34","activity":"needs_attention","last_recap":"did things","pending":[
   {"request_id":42,"tool":"bash","summary":"ls","options":[{"option_id":"allow","name":"Allow","kind":"allow_once"},{"option_id":"deny","name":"Deny","kind":"reject_once"}],"at":"2026-09-16T09:59:00Z"},
   {"request_id":"req-7","tool":"edit","summary":"x","options":[],"at":"2026-09-16T09:59:00Z"}],"permission_policy":"attention"},"children":[]}
"""

let s = try decoder.decode(SwarmSnapshot.self, from: Data(swarm.utf8))
assert(s.local == "n1")
let r = s.records[0]
assert(r.word == "needs_attention" && r.driven && r.project == "workspace" && r.label == "" && r.load?.rssMb == 1536)
assert(tilde(r.cwd!) == "~/Code/gtm/workspace" && mem(1536) == "1.5 GB")
let ended = try decoder.decode(SwarmSnapshot.self, from: Data(swarm.replacing("\"liveness\":\"live\"", with: "\"liveness\":\"ended\"").utf8)).records[0]
assert(ended.word == "ended" && r.displayName == "[workspace]" && r.node == "mato")
let change = try decoder.decode(Change.self, from: Data(("{\"event\":\"attention\",\"at\":\"2026-09-16T10:00:01Z\",\"record\":" + String(swarm.split(separator: "\"records\":[")[1].dropLast(2)) + "}").utf8))
assert(change.event == "attention" && change.record?.activitySeq == 7)
let left = try decoder.decode(Change.self, from: Data("{\"event\":\"node_left\",\"at\":\"2026-09-16T10:00:01Z\",\"node\":{}}".utf8))
assert(left.event == "node_left" && left.record == nil)

let g = try decoder.decode(Record.self, from: Data(session.utf8))
let p = g.state!.pending
assert(p.count == 2 && p[0].options[0].tone == 1 && p[0].options[1].tone == -1 && g.state?.lastRecap == "did things")
let ids = String(data: try JSONEncoder().encode([p[0].requestId, p[1].requestId]), encoding: .utf8)
assert(ids == "[42,\"req-7\"]")

if CommandLine.arguments.count > 1 {
    let live = try decoder.decode(SwarmSnapshot.self, from: Data(contentsOf: URL(fileURLWithPath: CommandLine.arguments[1])))
    print("\(live.records.count) records, \(live.nodes.count) nodes")
}
print("ok")
