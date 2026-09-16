// /ui/sessions/{key}: the live ACP stream as a plain log plus the last recap, R9.
import SwiftUI

struct SessionView: View {
    @Environment(Client.self) private var client
    let key: String, local: String?
    @State private var record: Record?
    @State private var lines: [Line] = []
    @State private var status = "connecting"
    @State private var draft = ""

    struct Line: Identifiable { let id = UUID(); let text: String; let tool: Bool }

    var body: some View {
        VStack(spacing: 0) {
            if let r = record {
                VStack(alignment: .leading, spacing: 4) {
                    HStack(spacing: 8) {
                        if !r.label.isEmpty { Text(r.label).fontWeight(.semibold) }
                        if !r.project.isEmpty { Text(r.project).font(.caption).padding(.horizontal, 6).background(Color.line, in: RoundedRectangle(cornerRadius: 4)) }
                        Text(r.word.replacing("_", with: " ")).font(.footnote).foregroundStyle(Color.state(r.word))
                        Spacer()
                        Text(status).font(.footnote).foregroundStyle(status == "live" ? Color.dim : Color.err)
                    }
                    Stats(record: r)
                    if let recap = r.state?.lastRecap { Text("last recap: \(recap)").font(.footnote).foregroundStyle(Color.dim) }
                    ForEach(r.state?.pending ?? []) { p in PendingView(key: key, pending: p, local: local, fail: fail, refresh: refresh) }
                }.padding(12)
            }
            ScrollViewReader { proxy in
                ScrollView {
                    LazyVStack(alignment: .leading, spacing: 0) {
                        ForEach(lines) { l in
                            Text(l.text).font(.system(.caption, design: .monospaced)).foregroundStyle(l.tool ? Color.link : Color.primary)
                                .frame(maxWidth: .infinity, alignment: .leading).id(l.id)
                        }
                    }.padding(.horizontal, 12)
                }
                .onChange(of: lines.count) { if let last = lines.last { proxy.scrollTo(last.id, anchor: .bottom) } }
            }
            HStack {
                TextField("prompt", text: $draft).textFieldStyle(.roundedBorder).onSubmit(send)
                Button("send", action: send).buttonStyle(.bordered)
            }.padding(12)
        }
        .background(Color.bg)
        .navigationBarTitleDisplayMode(.inline)
        .task { await refresh(); await stream() }
        .task { while !Task.isCancelled { try? await Task.sleep(for: .seconds(10)); await refresh() } }
    }

    private func fail(_ error: Error) { status = error.localizedDescription }

    private func refresh() async {
        do { record = try await client.get(client.spath(key, local: local)) } catch { fail(error) }
    }

    private func send() {
        let text = draft.trimmingCharacters(in: .whitespaces)
        guard !text.isEmpty else { return }
        Task { do { try await client.post(client.spath(key, local: local, "/prompt"), ["prompt": text]); draft = "" } catch { fail(error) } }
    }

    private func stream() async {
        while !Task.isCancelled {
            do {
                for try await data in client.sse(client.spath(key, local: local, "/stream")) {
                    status = "live"
                    if let msg = try? JSONSerialization.jsonObject(with: data) as? [String: Any], let line = Self.line(msg) { append(line) }
                }
            } catch { fail(error) }
            try? await Task.sleep(for: .seconds(2))
        }
    }

    private func append(_ line: Line) {
        // Chunks of one message join the last line; a tool or user marker starts a new one.
        if let last = lines.last, last.tool == line.tool, !line.text.hasPrefix("\n") {
            lines[lines.count - 1] = Line(text: last.text + line.text, tool: last.tool)
        } else { lines.append(line) }
        if lines.count > 4000 { lines.removeFirst() }
    }

    /// The page's logLine: one text per ACP update kind, nothing for the rest.
    static func line(_ msg: [String: Any]) -> Line? {
        let params = msg["params"] as? [String: Any]
        let u = params?["update"] as? [String: Any] ?? msg["update"] as? [String: Any] ?? msg
        let kind = (u["sessionUpdate"] ?? u["session_update"] ?? msg["method"]) as? String ?? ""
        let content = u["content"] as? [String: Any]
        let text = content?["text"] as? String ?? u["content"] as? String ?? ""
        let title = (u["title"] ?? u["toolCallId"]) as? String ?? "tool"
        switch kind {
        case "agent_message_chunk": return Line(text: text, tool: false)
        case "agent_thought_chunk": return Line(text: text, tool: true)
        case "tool_call": return Line(text: "\n▸ \(title)" + ((u["kind"] as? String).map { " (\($0))" } ?? ""), tool: true)
        case "tool_call_update": return (u["status"] as? String).map { Line(text: "\n  \(title): \($0)", tool: true) }
        case "user_message_chunk": return Line(text: "\n> \(text)", tool: false)
        case "session/request_permission": return Line(text: "\n? permission: \(((params?["toolCall"] as? [String: Any])?["title"] as? String) ?? "")", tool: false)
        default: return nil
        }
    }
}
