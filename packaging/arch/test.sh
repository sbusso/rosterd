#!/usr/bin/env bash
# Builds and installs the Arch package from this checkout inside a throwaway container, then runs
# the installed binary. Docker and the network are all it needs; nothing touches the host.
#
#   packaging/arch/test.sh [--nocheck]
set -euo pipefail
cd "$(dirname "$0")/../.."
# The official image is x86_64 only; on Apple silicon Docker runs it under Rosetta, a few minutes.
docker run --rm --platform linux/amd64 -v "$PWD:/repo:ro" -e MAKEPKG_FLAGS="${1-}" archlinux:base-devel bash -euo pipefail -c '
  # pacman 7 sandboxes downloads with seccomp, which the emulated kernel refuses.
  sed -i "s/^#DisableSandbox/DisableSandbox/" /etc/pacman.conf
  pacman -Syu --noconfirm --needed rust gtk3 libayatana-appindicator >/dev/null
  useradd -m build
  ver=$(sed -n "s/^pkgver=//p" /repo/packaging/arch/PKGBUILD)
  mkdir -p /build/src
  cp -r /repo "/build/src/rosterd-$ver" && rm -rf "/build/src/rosterd-$ver"/{target,dist,.git}
  cp /repo/packaging/arch/PKGBUILD /build/
  chown -R build /build
  # -e: the tarball does not exist yet, so the pre-extracted tree stands in; prepare() is skipped.
  su build -c "cd /build/src/rosterd-$ver && cargo fetch --locked && cd /build && makepkg -e $MAKEPKG_FLAGS"
  pacman -U --noconfirm /build/rosterd-*.pkg.tar*
  rosterd version
  rosterd-tray --help 2>&1 | head -1 || true
  grep ExecStart /usr/lib/systemd/user/rosterd.service /usr/lib/systemd/system/rosterd-system@.service
  pacman -Ql rosterd
'
