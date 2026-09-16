// The daemon's bearer API over the tailnet, R6 R9. The address lives in UserDefaults, the
// token in the keychain; both come from one scanned rosterd://pair link.
import Foundation
import Security

@Observable final class Client {
    var url: URL? = UserDefaults.standard.url(forKey: "url")
    var token: String? = Keychain.get()
    var paired: Bool { url != nil && token != nil }

    func pair(_ link: URL) -> Bool {
        guard link.scheme == "rosterd", link.host == "pair",
              let items = URLComponents(url: link, resolvingAgainstBaseURL: false)?.queryItems,
              let url = items.first(where: { $0.name == "url" })?.value.flatMap(URL.init),
              let token = items.first(where: { $0.name == "token" })?.value, !token.isEmpty else { return false }
        UserDefaults.standard.set(url, forKey: "url")
        Keychain.set(token)
        self.url = url; self.token = token
        return true
    }

    func unpair() {
        UserDefaults.standard.removeObject(forKey: "url")
        Keychain.delete()
        url = nil; token = nil
    }

    private func request(_ path: String, body: Encodable? = nil) throws -> URLRequest {
        guard let url, let token else { throw ClientError("not paired") }
        var r = URLRequest(url: url.appending(path: path))
        r.setValue("Bearer \(token)", forHTTPHeaderField: "Authorization")
        if let body {
            r.httpMethod = "POST"
            r.setValue("application/json", forHTTPHeaderField: "Content-Type")
            r.httpBody = try JSONEncoder().encode(body)
        }
        return r
    }

    func get<T: Decodable>(_ path: String) async throws -> T {
        try decoder.decode(T.self, from: try await send(request(path)))
    }

    @discardableResult
    func post(_ path: String, _ body: Encodable) async throws -> Data {
        try await send(request(path, body: body))
    }

    private func send(_ r: URLRequest) async throws -> Data {
        let (data, response) = try await URLSession.shared.data(for: r)
        let status = (response as? HTTPURLResponse)?.statusCode ?? 0
        guard (200..<300).contains(status) else {
            let e = try? JSONDecoder().decode([String: String].self, from: data)
            throw ClientError(e?["error"] ?? "\(status)")
        }
        return data
    }

    /// One `data:` payload per event of a text/event-stream, R6; ends when the daemon hangs up.
    func sse(_ path: String) -> AsyncThrowingStream<Data, Error> {
        AsyncThrowingStream { cont in
            let task = Task {
                do {
                    let r = try request(path)
                    let (bytes, response) = try await URLSession.shared.bytes(for: r)
                    guard (response as? HTTPURLResponse)?.statusCode == 200 else { throw ClientError("\((response as? HTTPURLResponse)?.statusCode ?? 0)") }
                    // ponytail: one `data:` line is one event; `lines` drops the blank separators and the
                    // daemon's payloads are single-line JSON. Split on "\n\n" if a multi-line event ever appears.
                    for try await line in bytes.lines where line.hasPrefix("data:") {
                        cont.yield(Data(line.dropFirst(5).trimmingCharacters(in: .whitespaces).utf8))
                    }
                    cont.finish(throwing: ClientError("disconnected"))
                } catch { cont.finish(throwing: error) }
            }
            cont.onTermination = { _ in task.cancel() }
        }
    }

    /// Session actions reach the owning node through this one, R7.5.
    func spath(_ key: String, local: String?, _ suffix: String = "") -> String {
        let node = String(key.split(separator: ":").first ?? "")
        return node == local ? "/sessions/\(key)\(suffix)" : "/swarm/\(node)/sessions/\(key)\(suffix)"
    }
}

struct ClientError: LocalizedError {
    let errorDescription: String?
    init(_ s: String) { errorDescription = s }
}

enum Keychain {
    private static let query: [String: Any] = [kSecClass as String: kSecClassGenericPassword, kSecAttrService as String: "rosterd", kSecAttrAccount as String: "token"]
    static func get() -> String? {
        var item: CFTypeRef?
        SecItemCopyMatching(query.merging([kSecReturnData as String: true]) { $1 } as CFDictionary, &item)
        return (item as? Data).flatMap { String(data: $0, encoding: .utf8) }
    }
    static func set(_ token: String) {
        delete()
        SecItemAdd(query.merging([kSecValueData as String: Data(token.utf8)]) { $1 } as CFDictionary, nil)
    }
    static func delete() { SecItemDelete(query as CFDictionary) }
}
