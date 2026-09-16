import SwiftUI

@main
struct RosterdApp: App {
    @State private var client = Client()
    @State private var scanning = false

    var body: some Scene {
        WindowGroup {
            Group {
                if client.paired { RosterView(scanning: $scanning) } else { PairView(onLink: client.pair) }
            }
            .environment(client)
            .preferredColorScheme(.dark)
            .tint(Color.link)
            // The Camera app opens rosterd://pair links here too; the same link the in-app scanner reads.
            .onOpenURL { _ = client.pair($0) }
            .sheet(isPresented: $scanning) { ScanView { let ok = client.pair($0); if ok { scanning = false }; return ok } }
        }
    }
}

extension Color {
    static let bg = Color(red: 0.059, green: 0.059, blue: 0.063)
    static let card = Color(red: 0.090, green: 0.090, blue: 0.102)
    static let line = Color(red: 0.137, green: 0.137, blue: 0.149)
    static let dim = Color(red: 0.545, green: 0.545, blue: 0.573)
    static let link = Color(red: 0.541, green: 0.706, blue: 0.973)
    static let err = Color(red: 0.937, green: 0.325, blue: 0.314)
    static let go = Color(red: 0.165, green: 0.227, blue: 0.125)

    static func state(_ word: String) -> Color {
        switch word {
        case "needs_attention": Color(red: 1, green: 0.702, blue: 0)
        case "active": Color(red: 0.298, green: 0.686, blue: 0.314)
        case "idle": Color(red: 0.533, green: 0.6, blue: 0.667)
        case "unknown": Color(red: 0.42, green: 0.42, blue: 0.451)
        default: Color(red: 0.333, green: 0.333, blue: 0.361)
        }
    }
}
