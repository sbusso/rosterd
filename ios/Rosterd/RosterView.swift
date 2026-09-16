// The roster page, R9: every node of the swarm, its sessions ranked by state, live over
// /swarm/events; pending permissions answered from the row.
import SwiftUI

struct RosterView: View {
    @Environment(Client.self) private var client
    @Binding var scanning: Bool
    @State private var swarm: SwarmSnapshot?
    @State private var status = "connecting"
    @State private var pending: [String: [Pending]] = [:]
    @State private var seen: [String: Int] = [:]

    private var live: [Record] { (swarm?.records ?? []).filter { $0.liveness != "ended" } }
    private var need: Int { live.filter { $0.activity == "needs_attention" }.count }

    var body: some View {
        NavigationStack {
            TimelineView(.periodic(from: .now, by: 30)) { _ in
                List {
                    ForEach(swarm?.nodes ?? []) { node in
                        Section {
                            let recs = rows(node)
                            if recs.isEmpty { Text("no sessions").foregroundStyle(Color.dim).listRowBackground(Color.card) }
                            ForEach(recs) { r in
                                SessionRow(record: r, pending: pending[r.sessionKey] ?? [], local: swarm?.local, dimmed: node.state == "unreachable") { fail($0) } refresh: { await refreshPending(r.sessionKey) }
                                    .listRowBackground(Color.card)
                            }
                        } header: {
                            HStack(alignment: .firstTextBaseline, spacing: 8) {
                                Text(node.name).textCase(.uppercase)
                                Text(node.revoked ? "revoked" : node.state == "local" ? "this node" : node.state + (node.seenMs.map { " \(ago(Date(timeIntervalSinceNow: -Double($0) / 1000))) ago" } ?? "")).textCase(nil)
                                if let up = node.uptimeMs { Text("· up \(ago(Date(timeIntervalSinceNow: -Double(up) / 1000)))").textCase(nil) }
                                if let h = node.capabilities?.harnesses, !h.isEmpty { Text("·"); ForEach(h, id: \.self) { HarnessMark(harness: $0) } }
                            }.font(.caption).foregroundStyle(Color.dim)
                        }
                    }
                }
            }
            .listStyle(.insetGrouped)
            .scrollContentBackground(.hidden)
            .background(Color.bg)
            .navigationBarTitleDisplayMode(.inline)
            .toolbar {
                ToolbarItem(placement: .principal) {
                    VStack(spacing: 0) {
                        Text(swarm?.nodes.first { $0.state == "local" }?.name ?? "rosterd").fontWeight(.semibold)
                        if swarm != nil {
                            HStack(spacing: 4) {
                                Text("\(live.count) session\(live.count == 1 ? "" : "s")").foregroundStyle(Color.dim)
                                if need > 0 { Text("· \(need) need\(need == 1 ? "s" : "") attention").foregroundStyle(Color.state("needs_attention")) }
                            }.font(.caption)
                        }
                    }
                }
                ToolbarItem(placement: .topBarTrailing) {
                    Menu {
                        Button("pair again", systemImage: "qrcode.viewfinder") { scanning = true }
                        Button("forget", systemImage: "xmark", role: .destructive) { client.unpair() }
                    } label: {
                        Text(status).font(.footnote).foregroundStyle(status == "live" ? Color.dim : Color.err)
                    }
                }
            }
        }
        .task { await stream() }
    }

    private func rows(_ node: NodeHealth) -> [Record] {
        (swarm?.records ?? []).filter { $0.nodeId == node.nodeId }
            .sorted { (RANK[$0.word] ?? 9, $1.when) < (RANK[$1.word] ?? 9, $0.when) }
    }

    private func fail(_ error: Error) { status = error.localizedDescription }

    private func stream() async {
        while !Task.isCancelled {
            do {
                for try await data in client.sse("/swarm/events") {
                    swarm = try decoder.decode(SwarmSnapshot.self, from: data)
                    status = "live"
                    // Pending requests of sessions that need attention, refreshed when their claim sequence moves.
                    for r in swarm?.records ?? [] where r.activity == "needs_attention" && seen[r.sessionKey] != r.activitySeq {
                        seen[r.sessionKey] = r.activitySeq
                        await refreshPending(r.sessionKey)
                    }
                }
            } catch { fail(error) }
            try? await Task.sleep(for: .seconds(2))
        }
    }

    private func refreshPending(_ key: String) async {
        let r: Record? = try? await client.get(client.spath(key, local: swarm?.local))
        pending[key] = r?.state?.pending ?? []
    }
}

struct SessionRow: View {
    @Environment(Client.self) private var client
    let record: Record, pending: [Pending], local: String?, dimmed: Bool
    let fail: (Error) -> Void
    let refresh: () async -> Void
    @State private var draft = ""

    private var r: Record { record }
    private var word: String { r.word }
    private var off: Bool { dimmed || ["ended", "suspended", "stale"].contains(word) }

    var body: some View {
        VStack(alignment: .leading, spacing: 4) {
            HStack(alignment: .firstTextBaseline, spacing: 8) {
                Circle().fill(word == "unknown" ? Color.clear : Color.state(word)).stroke(Color.state(word), lineWidth: 1.5).frame(width: 8, height: 8)
                HarnessMark(harness: r.harness)
                if !r.label.isEmpty { Text(r.label).fontWeight(.semibold).strikethrough(word == "ended") }
                if !r.project.isEmpty { Text(r.project).font(.caption).fontWeight(.medium).padding(.horizontal, 6).padding(.vertical, 1).background(Color.line, in: RoundedRectangle(cornerRadius: 4)) }
                Text(word.replacing("_", with: " ")).font(.footnote).fontWeight(.medium).foregroundStyle(Color.state(word))
                Text([r.activityEvent, ago(r.when), word == "ended" ? r.endedReason : nil].compactMap { $0 }.joined(separator: " · ")).font(.footnote).foregroundStyle(Color.dim)
                if r.conflict { Text("attempt conflict").font(.footnote).foregroundStyle(Color.err) }
            }
            Stats(record: r)
            let meta = [r.lane == "headless" ? "headless" : nil, r.attemptId.map { "attempt \($0)" + (r.parentAttemptId.map { " ← \($0)" } ?? "") }, r.origin, r.cwd.map(tilde)].compactMap { $0 }
            if !meta.isEmpty { Text(meta.joined(separator: " · ")).font(.footnote).foregroundStyle(Color.dim) }
            ForEach(pending) { p in PendingView(key: r.sessionKey, pending: p, local: local, fail: fail, refresh: refresh) }
            if word != "ended" {
                HStack(spacing: 6) {
                    if r.driven {
                        NavigationLink("open conversation") { SessionView(key: r.sessionKey, local: local) }.buttonStyle(.plain).foregroundStyle(Color.link).font(.footnote)
                        TextField("prompt", text: $draft).textFieldStyle(.roundedBorder).font(.footnote).onSubmit { send() }
                    }
                    if r.driven && word == "active" { Act("cancel turn") { try await client.post(client.spath(r.sessionKey, local: local, "/cancel"), [String: String]()) } }
                    if word == "suspended" { Act("resume") { try await client.post(client.spath(r.sessionKey, local: local, "/resume"), [String: String]()) } }
                }.padding(.top, 2)
            }
        }
        .font(.subheadline)
        .foregroundStyle(off ? Color.dim : Color.primary)
        .padding(.vertical, 2)
    }

    private func send() {
        let text = draft.trimmingCharacters(in: .whitespaces)
        guard !text.isEmpty else { return }
        Task { do { try await client.post(client.spath(r.sessionKey, local: local, "/prompt"), ["prompt": text]); draft = "" } catch { fail(error) } }
    }

    private func Act(_ title: String, _ run: @escaping () async throws -> Void) -> some View {
        Button(title) { Task { do { try await run() } catch { fail(error) } } }.buttonStyle(.bordered).controlSize(.small)
    }
}

/// The harness by its logo, Claude's, OpenAI's for codex, pi's, from the asset catalog; any other by name.
struct HarnessMark: View {
    let harness: String
    var body: some View {
        if UIImage(named: harness) != nil {
            Image(harness).resizable().scaledToFit().frame(width: 14, height: 14)
                .foregroundStyle(harness == "claude" ? Color(red: 0.851, green: 0.467, blue: 0.341) : Color.primary)
                .accessibilityLabel(harness)
        } else {
            Text(harness).font(.footnote).foregroundStyle(Color.dim)
        }
    }
}

struct Stats: View {
    let record: Record
    var body: some View {
        HStack(spacing: 12) {
            if let l = record.load { Label("\(l.cpuPct)%", systemImage: "cpu"); Text(mem(l.rssMb)) }
            Label(ago(record.startedAt), systemImage: "clock")
        }.font(.footnote).foregroundStyle(Color.dim).monospacedDigit()
    }
}

struct PendingView: View {
    @Environment(Client.self) private var client
    let key: String, pending: Pending, local: String?
    let fail: (Error) -> Void
    let refresh: () async -> Void

    var body: some View {
        VStack(alignment: .leading, spacing: 6) {
            HStack(alignment: .firstTextBaseline, spacing: 6) {
                Text(pending.tool).fontWeight(.semibold)
                Text("\(pending.summary) · \(ago(pending.at))").font(.footnote).foregroundStyle(Color.dim).lineLimit(3)
            }
            HStack(spacing: 6) {
                ForEach(pending.options) { o in
                    Button(o.name) { answer(["request_id": .init(pending.requestId), "outcome": "selected", "option_id": .init(o.optionId)]) }
                        .buttonStyle(.bordered).controlSize(.small).tint(o.tone > 0 ? .green : o.tone < 0 ? Color.err : Color.dim)
                }
                Button("cancel") { answer(["request_id": .init(pending.requestId), "outcome": "cancelled"]) }.buttonStyle(.bordered).controlSize(.small).tint(Color.dim)
            }
        }
        .padding(.leading, 10).padding(.vertical, 4)
        .overlay(alignment: .leading) { Rectangle().fill(Color.state("needs_attention")).frame(width: 2) }
    }

    private func answer(_ body: [String: Field]) {
        Task { do { try await client.post(client.spath(key, local: local, "/permission"), body); await refresh() } catch { fail(error) } }
    }
}

/// One JSON value of a permission answer: the request id as received, or a string.
enum Field: Encodable {
    case id(RequestId), text(String)
    init(_ id: RequestId) { self = .id(id) }
    init(_ s: String) { self = .text(s) }
    func encode(to e: Encoder) throws {
        switch self { case .id(let id): try id.encode(to: e); case .text(let s): try s.encode(to: e) }
    }
}
extension Field: ExpressibleByStringLiteral { init(stringLiteral s: String) { self = .text(s) } }
