// The handshake, R9: the page's `pair phone` shows a rosterd://pair QR, the phone reads it.
import SwiftUI
import VisionKit

/// What the app shows until it is paired: the scanner, or the link typed by hand.
struct PairView: View {
    let onLink: (URL) -> Bool
    @State private var scanning = false
    @State private var link = ""
    @State private var refused = false

    var body: some View {
        VStack(spacing: 28) {
            Spacer()
            Image(systemName: "qrcode.viewfinder").font(.system(size: 72, weight: .thin)).foregroundStyle(Color.link)
            VStack(spacing: 6) {
                Text("rosterd").font(.title2).fontWeight(.semibold)
                Text("roster page → pair phone").foregroundStyle(Color.dim)
            }
            Button { scanning = true } label: {
                Label("Scan", systemImage: "camera").frame(maxWidth: 240).padding(.vertical, 6)
            }.buttonStyle(.borderedProminent)
            Spacer()
            VStack(spacing: 6) {
                TextField("rosterd://pair?url=…&token=…", text: $link)
                    .textFieldStyle(.roundedBorder).font(.footnote).autocorrectionDisabled().textInputAutocapitalization(.never)
                    .onSubmit { refused = !(URL(string: link).map(onLink) ?? false) }
                if refused { Text("not a pair link").font(.footnote).foregroundStyle(Color.err) }
            }.padding(.horizontal, 32).padding(.bottom, 24)
        }
        .frame(maxWidth: .infinity, maxHeight: .infinity).background(Color.bg)
        .sheet(isPresented: $scanning) { ScanView { let ok = onLink($0); if ok { scanning = false }; return ok } }
    }
}

struct ScanView: View {
    let onLink: (URL) -> Bool

    var body: some View {
        ZStack {
            if DataScannerViewController.isSupported && DataScannerViewController.isAvailable {
                Scanner(onLink: onLink).ignoresSafeArea()
            } else {
                Color.bg.ignoresSafeArea()
                Label("no camera", systemImage: "camera.metering.unknown").foregroundStyle(Color.dim)
            }
        }
    }
}

private struct Scanner: UIViewControllerRepresentable {
    let onLink: (URL) -> Bool

    func makeUIViewController(context: Context) -> DataScannerViewController {
        let vc = DataScannerViewController(recognizedDataTypes: [.barcode(symbologies: [.qr])], isHighlightingEnabled: true)
        vc.delegate = context.coordinator
        try? vc.startScanning()
        return vc
    }
    func updateUIViewController(_ vc: DataScannerViewController, context: Context) {}
    func makeCoordinator() -> Coordinator { Coordinator(onLink: onLink) }

    final class Coordinator: NSObject, DataScannerViewControllerDelegate {
        let onLink: (URL) -> Bool
        init(onLink: @escaping (URL) -> Bool) { self.onLink = onLink }
        func dataScanner(_ scanner: DataScannerViewController, didAdd added: [RecognizedItem], allItems: [RecognizedItem]) {
            for case .barcode(let code) in added {
                if let s = code.payloadStringValue, let url = URL(string: s), onLink(url) { scanner.stopScanning() }
            }
        }
    }
}
