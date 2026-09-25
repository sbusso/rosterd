# rosterd, R11: the Homebrew formula. The release tarball carries the binaries, so
#   brew tap sbusso/rosterd https://github.com/sbusso/rosterd
#   brew install rosterd
# unpacks and needs no rust; `--HEAD` builds main from source.
class Rosterd < Formula
  desc "One daemon per machine that knows every coding agent session on it"
  homepage "https://github.com/sbusso/rosterd"
  url "https://github.com/sbusso/rosterd/releases/download/v0.1.28/rosterd-0.1.28-aarch64-apple-darwin.tar.gz"
  sha256 "41f549a13a257313d43378e144e52934d7ca20a3ab9ff3ae02d78b1811d63e8f"
  license "MIT"
  # The bottle is what lets a Mac install without a compiler toolchain: `brew bottle` of the
  # tarball install, uploaded next to it on the release.
  bottle do
    root_url "https://github.com/sbusso/rosterd/releases/download/v0.1.28"
    rebuild 1
    sha256 arm64_tahoe: "ffb1c7ca358a3b1c42b7f393f65d755b47ec281843abcbe89fc42462dd8c5117"
  end
  # The bottle is what lets a Mac install without a compiler toolchain: `brew bottle` of the
  # tarball install, uploaded next to it on the release.
  # The bottle is what lets a Mac install without a compiler toolchain: `brew bottle` of the
  # tarball install, uploaded next to it on the release.
  head do
    url "https://github.com/sbusso/rosterd.git", branch: "main"
    depends_on "rust" => :build
  end

  def install
    if build.head?
      system "cargo", "install", *std_cargo_args(path: "crates/rosterd")
      system "cargo", "install", *std_cargo_args(path: "crates/holder")
      system "cargo", "install", *std_cargo_args(path: "crates/tray")
    else
      bin.install Dir["dist/aarch64-apple-darwin/*"]
    end
    bin.install "scripts/rosterd-hook", "scripts/rosterd-launch", "scripts/rosterd-open"
    doc.install "README.md", "SPEC.md" if build.head?
  end

  # A LaunchAgent for this login: the daemon, which runs the menu bar tray beside itself. The
  # adapters live where bun put them and Tailscale in its app, so PATH names both.
  service do
    run [opt_bin/"rosterd", "daemon"]
    keep_alive true
    environment_variables PATH:     "#{Dir.home}/.local/bin:#{Dir.home}/.bun/bin:#{std_service_path_env}:" \
                                    "/Applications/Tailscale.app/Contents/MacOS",
                          RUST_LOG: "info"
    log_path var/"log/rosterd.log"
    error_log_path var/"log/rosterd.log"
  end

  # Homebrew's post-install sandbox keeps a formula out of launchd, so the service is
  # `brew services start`; the daemon brings the menu bar tray up itself.
  def caveats
    <<~EOS
      Start it, menu bar tray included:
        brew services start rosterd
      The roster page is `rosterd ui`.
      Harness hooks and ACP adapters are `rosterd setup` (they edit ~/.claude and ~/.codex).
    EOS
  end

  test do
    assert_match "rosterd", shell_output("#{bin}/rosterd version")
  end
end
