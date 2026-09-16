# rosterd, R11: the Homebrew formula, built from source with cargo.
#   brew tap sbusso/rosterd https://github.com/sbusso/rosterd
#   brew install --HEAD rosterd
# Head only until the first tag; then add `url` and `sha256` for the release tarball above `head`
# and `brew install rosterd` works without --HEAD.
class Rosterd < Formula
  desc "One daemon per machine that knows every coding agent session on it"
  homepage "https://github.com/sbusso/rosterd"
  license "MIT"
  head "https://github.com/sbusso/rosterd.git", branch: "main"

  depends_on "rust" => :build

  def install
    system "cargo", "install", *std_cargo_args(path: "crates/rosterd")
    system "cargo", "install", *std_cargo_args(path: "crates/holder")
    system "cargo", "install", *std_cargo_args(path: "crates/tray")
    bin.install "scripts/rosterd-hook", "scripts/rosterd-launch", "scripts/rosterd-open"
    doc.install "README.md", "SPEC.md"
  end

  # A LaunchAgent for this login: the daemon, which runs the menu bar tray beside itself. The
  # adapters live where bun put them and Tailscale in its app, so PATH names both.
  service do
    run [opt_bin/"rosterd", "daemon"]
    keep_alive true
    environment_variables PATH:     "#{Dir.home}/.local/bin:#{Dir.home}/.bun/bin:#{std_service_path_env}:/Applications/Tailscale.app/Contents/MacOS",
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
