#!/usr/bin/env bash
set -euo pipefail

RUN_MODE_FILE="${DELTA_BENCH_RUN_MODE_PATH:-/etc/delta-bench/security-mode}"
DEFAULT_EXPECTED_RUN_MODE="${DELTA_BENCH_EXPECTED_RUN_MODE:-run-mode}"
EGRESS_POLICY_PATH="${DELTA_BENCH_EGRESS_POLICY_PATH:-/etc/nftables.conf}"
EXPECTED_EGRESS_SHA="${DELTA_BENCH_EGRESS_POLICY_SHA256:-}"
FORBIDDEN_PROCESSES="${DELTA_BENCH_FORBIDDEN_PROCESSES:-apt apt-get dpkg unattended-upgrades do-release-upgrade}"

enforce_run_mode=0
require_no_public_ipv4=0
require_egress_policy=0
expected_run_mode="${DEFAULT_EXPECTED_RUN_MODE}"

usage() {
  cat <<EOF
Usage:
  ./scripts/security_check.sh [--enforce-run-mode] [--require-no-public-ipv4] [--require-egress-policy]
                             [--expected-run-mode run-mode] [--expected-egress-sha256 <sha>]
                             [--egress-policy-path /etc/nftables.conf]

Checks:
  - run-mode marker file and mode (when --enforce-run-mode is set)
  - no public IPv4 assignment (when --require-no-public-ipv4 is set)
  - nftables active ruleset hash (when --require-egress-policy is set; expected hash must be provided explicitly)
  - known noisy/maintenance processes are not running
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --enforce-run-mode)
      enforce_run_mode=1
      shift
      ;;
    --require-no-public-ipv4)
      require_no_public_ipv4=1
      shift
      ;;
    --require-egress-policy)
      require_egress_policy=1
      shift
      ;;
    --expected-run-mode)
      expected_run_mode="$2"
      shift 2
      ;;
    --expected-egress-sha256)
      EXPECTED_EGRESS_SHA="$2"
      shift 2
      ;;
    --egress-policy-path)
      EGRESS_POLICY_PATH="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown arg: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

is_public_ipv4() {
  local ip="$1"
  IFS='.' read -r a b c d <<<"${ip}"
  [[ "${a}" =~ ^[0-9]+$ ]] || return 1
  [[ "${b}" =~ ^[0-9]+$ ]] || return 1

  # RFC1918, loopback, link-local, CGNAT, TEST-NET and other non-public ranges.
  if (( a == 10 )); then return 1; fi
  if (( a == 172 && b >= 16 && b <= 31 )); then return 1; fi
  if (( a == 192 && b == 168 )); then return 1; fi
  if (( a == 127 )); then return 1; fi
  if (( a == 169 && b == 254 )); then return 1; fi
  if (( a == 100 && b >= 64 && b <= 127 )); then return 1; fi
  if (( a == 0 )); then return 1; fi
  if (( a == 255 )); then return 1; fi
  if (( a == 192 && b == 0 && c == 2 )); then return 1; fi
  if (( a == 198 && b == 51 && c == 100 )); then return 1; fi
  if (( a == 203 && b == 0 && c == 113 )); then return 1; fi
  return 0
}

check_run_mode() {
  if [[ ! -f "${RUN_MODE_FILE}" ]]; then
    echo "FAIL: run mode marker missing at ${RUN_MODE_FILE}" >&2
    return 1
  fi
  local current_mode
  current_mode="$(tr -d '\n\r\t ' < "${RUN_MODE_FILE}")"
  if [[ "${current_mode}" != "${expected_run_mode}" ]]; then
    echo "FAIL: expected run mode '${expected_run_mode}', found '${current_mode}'" >&2
    return 1
  fi
  echo "PASS: run mode is '${current_mode}'"
}

check_public_ipv4() {
  if ! command -v ip >/dev/null 2>&1; then
    echo "FAIL: 'ip' command not found; cannot verify public IPv4 state" >&2
    return 1
  fi

  local found_public=0
  while IFS= read -r cidr; do
    [[ -n "${cidr}" ]] || continue
    local ip_addr="${cidr%%/*}"
    if is_public_ipv4 "${ip_addr}"; then
      echo "FAIL: public IPv4 detected on host interface: ${ip_addr}" >&2
      found_public=1
    fi
  done < <(ip -4 -o addr show scope global | awk '{print $4}')

  if (( found_public != 0 )); then
    return 1
  fi
  echo "PASS: no public IPv4 detected on host interfaces"
}

check_egress_policy() {
  if ! command -v nft >/dev/null 2>&1; then
    echo "FAIL: nft command not found; cannot verify active egress policy" >&2
    return 1
  fi
  if [[ -z "${EXPECTED_EGRESS_SHA}" ]]; then
    echo "FAIL: expected egress sha256 must be provided explicitly for active ruleset comparison" >&2
    echo "hint: export DELTA_BENCH_EGRESS_POLICY_SHA256=\"\$(nft list ruleset | sha256sum | awk '{print \$1}')\"" >&2
    if [[ -f "${EGRESS_POLICY_PATH}" ]]; then
      echo "note: file hash fallback is disabled because policy files and active nft output are not canonical equivalents (${EGRESS_POLICY_PATH})" >&2
    fi
    return 1
  fi

  local active_sha
  if command -v sha256sum >/dev/null 2>&1; then
    active_sha="$(nft list ruleset | sha256sum | awk '{print $1}')"
  elif command -v shasum >/dev/null 2>&1; then
    active_sha="$(nft list ruleset | shasum -a 256 | awk '{print $1}')"
  else
    echo "FAIL: no sha256 utility available for nft ruleset hash" >&2
    return 1
  fi

  if [[ "${active_sha}" != "${EXPECTED_EGRESS_SHA}" ]]; then
    echo "FAIL: active nftables ruleset hash mismatch (expected ${EXPECTED_EGRESS_SHA}, got ${active_sha})" >&2
    return 1
  fi
  echo "PASS: nftables egress policy hash matches expected baseline"
}

check_forbidden_processes() {
  local failed=0
  local proc
  for proc in ${FORBIDDEN_PROCESSES}; do
    if pgrep -x "${proc}" >/dev/null 2>&1; then
      echo "FAIL: forbidden process is running during benchmark window: ${proc}" >&2
      failed=1
    fi
  done
  if (( failed != 0 )); then
    return 1
  fi
  echo "PASS: no forbidden maintenance processes detected"
}

check_forbidden_processes

if (( enforce_run_mode != 0 )); then
  check_run_mode
fi

if (( require_no_public_ipv4 != 0 )); then
  check_public_ipv4
fi

if (( require_egress_policy != 0 )); then
  check_egress_policy
fi

echo "security check completed"
