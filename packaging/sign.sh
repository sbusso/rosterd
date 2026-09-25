#!/usr/bin/env bash
# Signs the macOS release binaries with the Apple Development identity, so the application
# firewall keeps its allow rule from one release to the next: an ad hoc signature is a new
# program every build. codesign only sees the login keychain from a GUI (Aqua) session, so from
# ssh the work is handed to a Terminal window in that session and awaited.
#
#   packaging/sign.sh dist/aarch64-apple-darwin/*
#
# ROSTERD_SIGN_IDENTITY=- signs ad hoc, from any session, when no usable identity exists; the
# firewall then asks again for the new program. A signature the system would kill at launch
# (a revoked certificate: macOS SIGKILLs the binary, exit 137) fails the script instead.
set -euo pipefail
identity=${ROSTERD_SIGN_IDENTITY:-"Apple Development"}
if [ "$identity" != - ] && [ "$(launchctl managername)" != Aqua ]; then
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
  if [ "$identity" != - ] && spctl -a -t execute "$file" 2>&1 | grep -q REVOKED; then
    echo "sign.sh: the certificate behind '$identity' is revoked; macOS would kill $file at launch. Renew it in Xcode, or ROSTERD_SIGN_IDENTITY=- for an ad hoc signature." >&2
    exit 1
  fi
done
