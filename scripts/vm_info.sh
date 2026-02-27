#!/usr/bin/env bash
set -euo pipefail

printf 'host=%s\n' "$(uname -n)"
printf 'os=%s\n' "$(uname -s)"
printf 'kernel=%s\n' "$(uname -r)"
printf 'arch=%s\n' "$(uname -m)"
if command -v sysctl >/dev/null 2>&1; then
  printf 'cpu.brand=%s\n' "$(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
  printf 'mem.bytes=%s\n' "$(sysctl -n hw.memsize 2>/dev/null || true)"
fi
