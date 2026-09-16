// The daemon's frames, R6: the swarm snapshot of /swarm/events and the session of /sessions/{key}.
import Foundation

struct SwarmSnapshot: Decodable {
    var nodes: [NodeHealth]
    var records: [Record]
    var local: String? { nodes.first { $0.state == "local" }?.nodeId }
}

struct NodeHealth: Decodable, Identifiable {
    var nodeId: String, name: String, state: String, peerAgeMs: Int, seenMs: Int?, uptimeMs: Int?, revoked: Bool
    var capabilities: Capabilities?
    var id: String { nodeId }
}

struct Capabilities: Decodable { var harnesses: [String]? }

struct Record: Decodable, Identifiable {
    var sessionKey: String, nodeId: String, pid: Int, startedAt: Date, harness: String, lane: String
    var name: String?, activity: String, activityEvent: String?, activityAt: Date?, activitySeq: Int
    var attemptId: String?, parentAttemptId: String?, cwd: String?, origin: String?
    var liveness: String, endedReason: String?, conflict: Bool
    var load: Load?, holder: Holder?
    var state: SessionState?

    var id: String { sessionKey }
    /// One word for a row, liveness first, as the page does.
    var word: String { liveness == "live" ? activity : liveness }
    var driven: Bool { lane == "headless" || holder != nil }
    var project: String { (cwd ?? "").split(separator: "/").last.map(String.init) ?? "" }
    var label: String { name ?? (project.isEmpty ? (origin ?? "") : "") }
    var when: Date { activityAt ?? startedAt }
}

struct Load: Decodable { var cpuPct: Int, rssMb: Int }
struct Holder: Decodable { var socket: String }

struct SessionState: Decodable {
    var lastRecap: String?
    var pending: [Pending]
}

struct Pending: Decodable, Identifiable {
    var requestId: RequestId, tool: String, summary: String, at: Date
    var options: [PermissionOption]
    var id: String { "\(requestId)" }
}

struct PermissionOption: Decodable, Identifiable {
    var optionId: String, name: String, kind: String
    var id: String { optionId }
    /// ACP option kinds: allow_* are the go, reject_* the no.
    var tone: Int { kind.contains("allow") || optionId.contains("allow") ? 1 : kind.contains("reject") || optionId.contains("deny") ? -1 : 0 }
}

/// ACP request ids are a number or a string; sent back byte for byte.
enum RequestId: Codable, CustomStringConvertible {
    case int(Int), string(String)
    init(from d: Decoder) throws {
        let c = try d.singleValueContainer()
        if let n = try? c.decode(Int.self) { self = .int(n) } else { self = .string(try c.decode(String.self)) }
    }
    func encode(to e: Encoder) throws {
        var c = e.singleValueContainer()
        switch self { case .int(let n): try c.encode(n); case .string(let s): try c.encode(s) }
    }
    var description: String { switch self { case .int(let n): "\(n)"; case .string(let s): s } }
}

let RANK = ["needs_attention": 0, "active": 1, "idle": 2, "unknown": 3, "suspended": 4, "stale": 4, "ended": 5]

func ago(_ date: Date) -> String {
    let s = max(0, Date().timeIntervalSince(date))
    return s < 60 ? "\(Int(s))s" : s < 3600 ? "\(Int(s / 60))m" : s < 86400 ? "\(Int(s / 3600))h" : "\(Int(s / 86400))d"
}

func tilde(_ path: String) -> String {
    path.replacing(#/^(\/Users\/[^\/]+|\/home\/[^\/]+|\/root)(?=\/|$)/#, with: "~")
}

func mem(_ mb: Int) -> String { mb >= 1024 ? String(format: "%.1f GB", Double(mb) / 1024) : "\(mb) MB" }

let decoder: JSONDecoder = {
    let d = JSONDecoder()
    d.keyDecodingStrategy = .convertFromSnakeCase
    let iso = ISO8601DateFormatter()
    // chrono writes microseconds; the fraction is dropped, nothing here reads below a second.
    d.dateDecodingStrategy = .custom { dec in
        let s = try dec.singleValueContainer().decode(String.self)
        guard let date = iso.date(from: s.replacing(#/\.\d+/#, with: "")) else {
            throw DecodingError.dataCorrupted(.init(codingPath: dec.codingPath, debugDescription: "not a date: \(s)"))
        }
        return date
    }
    return d
}()
