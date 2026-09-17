#!/usr/bin/env bash
# Signs the macOS release binaries with the Apple Development identity, so the application
# firewall keeps its allow rule from one release to the next: an ad hoc signature is a new
# program every build. codesign only sees the login keychain from a GUI (Aqua) session, so from
# ssh the work is handed to a Terminal window in that session and awaited.
#
#   packaging/sign.sh dist/aarch64-apple-darwin/*
set -euo pipefail
identity=${ROSTERD_SIGN_IDENTITY:-"Apple Development"}
if [ "$(launchctl managername)" != Aqua ]; then
  log=$(mktemp "${TMPDIR:-/tmp}/rosterd-sign.XXXXXX")
  script=$log.command
  { printf '#!/bin/zsh\ncd %q\nexec > %q 2>&1\n' "$PWD" "$log"; printf '%q ' "$(cd "$(dirname "$0")" && pwd)/sign.sh" "$@"; printf '\necho EXIT $?\n'; } > "$script"
  chmod +x "$script"
  open -a Terminal "$script"
  for _ in $(seq 120); do grep -q '^EXIT' "$log" 2>/dev/null && break; sleep 1; done
  cat "$log"
  grep -q '^EXIT 0$' "$log"
  exit
fi
for file in "$@"; do
  codesign --force --sign "$identity" --identifier "sh.rosterd.$(basename "$file")" "$file"
  codesign -dv "$file" 2>&1 | grep -E '^(Identifier|TeamIdentifier)='
done
