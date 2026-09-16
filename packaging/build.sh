#!/usr/bin/env bash
# Release binaries into dist/<target>/: the native target with cargo, the two Linux gnu targets
# with cargo zigbuild (cargo-zigbuild and zig installed, targets added with rustup).
#
#   packaging/build.sh                 every target
#   packaging/build.sh native          the host only
#   packaging/build.sh x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu
#
# CARGO_TARGET_DIR is honoured. A target that fails to build stops the script; run the rest by name.
set -euo pipefail
cd "$(dirname "$0")/.."

native=$(rustc -vV | sed -n 's/^host: //p')
out=${CARGO_TARGET_DIR:-target}
bins=(rosterd rosterd-holder)
targets=("$@")
[ ${#targets[@]} -gt 0 ] || targets=(native x86_64-unknown-linux-gnu aarch64-unknown-linux-gnu)

copy() { # $1 target, $2 build dir
  mkdir -p "dist/$1"
  for b in "${bins[@]}"; do install -m755 "$2/$b" "dist/$1/$b"; done
  # The tray needs GTK on Linux, so only the native build has it.
  [ -x "$2/rosterd-tray" ] && install -m755 "$2/rosterd-tray" "dist/$1/rosterd-tray"
  echo "dist/$1: $(ls "dist/$1" | tr '\n' ' ')"
}

for t in "${targets[@]}"; do
  case "$t" in
    native | "$native")
      cargo build --release
      copy "$native" "$out/release"
      ;;
    *)
      cargo zigbuild --release --target "$t" -p rosterd -p rosterd-holder
      copy "$t" "$out/$t/release"
      ;;
  esac
done
