#!/usr/bin/env bash
set -euo pipefail

MODE_DIR="${DELTA_BENCH_MODE_DIR:-/etc/delta-bench}"
STATE_DIR="${DELTA_BENCH_STATE_DIR:-/var/lib/delta-bench}"
MODE_FILE="${MODE_DIR}/security-mode"
STATE_FILE="${STATE_DIR}/security-mode.state"
LOCK_FILE="${DELTA_BENCH_LOCK_FILE:-${STATE_DIR}/security-mode.lock}"
LOCK_TIMEOUT_SECONDS="${DELTA_BENCH_LOCK_TIMEOUT_SECONDS:-30}"

# Baseline units to pause during benchmark run mode.
BASELINE_PAUSE_UNITS=(
  apt-daily.timer
  apt-daily-upgrade.timer
  unattended-upgrades.service
  unattended-upgrades.timer
)

CSV_EXTRA_PAUSE_UNITS="${DELTA_BENCH_PAUSE_UNITS:-}"
CSV_SCAN_UNITS="${DELTA_BENCH_SCAN_UNITS:-}"
CSV_SNAPSHOT_UNITS="${DELTA_BENCH_SNAPSHOT_UNITS:-}"
CSV_LOG_HEAVY_UNITS="${DELTA_BENCH_LOG_HEAVY_UNITS:-}"

usage() {
  cat <<EOF
Usage:
  ./scripts/security_mode.sh run-mode
  ./scripts/security_mode.sh maintenance-mode
  ./scripts/security_mode.sh status

Modes:
  run-mode:
    - stop and mask update/scan/log-shipping units configured for benchmark isolation
    - write mode marker at ${MODE_FILE}
  maintenance-mode:
    - restore unit state captured when entering run-mode
    - write mode marker at ${MODE_FILE}
EOF
}

require_root() {
  if [[ "${EUID}" -ne 0 ]]; then
    echo "this script must run as root (or via sudo)" >&2
    exit 1
  fi
}

require_command() {
  local name="$1"
  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "required command not found: ${name}" >&2
    exit 1
  fi
}

split_csv_units() {
  local csv="$1"
  if [[ -z "${csv}" ]]; then
    return 0
  fi
  local old_ifs="${IFS}"
  IFS=','
  read -r -a units <<<"${csv}"
  IFS="${old_ifs}"
  local unit
  for unit in "${units[@]}"; do
    unit="$(echo "${unit}" | xargs)"
    if [[ -n "${unit}" ]]; then
      echo "${unit}"
    fi
  done
}

managed_units() {
  local unit
  for unit in "${BASELINE_PAUSE_UNITS[@]}"; do
    echo "${unit}"
  done
  split_csv_units "${CSV_EXTRA_PAUSE_UNITS}"
  split_csv_units "${CSV_SCAN_UNITS}"
  split_csv_units "${CSV_SNAPSHOT_UNITS}"
  split_csv_units "${CSV_LOG_HEAVY_UNITS}"
}

unit_exists() {
  local unit="$1"
  systemctl list-unit-files "${unit}" --no-legend >/dev/null 2>&1
}

unique_managed_units() {
  managed_units | awk '!seen[$0]++'
}

systemctl_state_or_unknown() {
  local state_cmd="$1"
  local unit="$2"
  local output
  if output="$(systemctl "${state_cmd}" "${unit}" 2>/dev/null)"; then
    echo "${output}"
  else
    echo "unknown"
  fi
}

capture_state() {
  mkdir -p "${STATE_DIR}"
  local tmp
  tmp="$(mktemp "${STATE_FILE}.XXXXXX")"
  local unit
  while IFS= read -r unit; do
    [[ -n "${unit}" ]] || continue
    if ! unit_exists "${unit}"; then
      continue
    fi
    local enabled active
    enabled="$(systemctl_state_or_unknown is-enabled "${unit}")"
    active="$(systemctl_state_or_unknown is-active "${unit}")"
    echo "${unit}|${enabled}|${active}" >>"${tmp}"
  done < <(unique_managed_units)
  mv "${tmp}" "${STATE_FILE}"
}

apply_run_mode() {
  require_root
  require_command systemctl
  mkdir -p "${MODE_DIR}"
  capture_state
  local failed=0
  local unit
  while IFS= read -r unit; do
    [[ -n "${unit}" ]] || continue
    if ! unit_exists "${unit}"; then
      continue
    fi
    if ! systemctl stop "${unit}"; then
      echo "FAIL: unable to stop unit '${unit}'" >&2
      failed=1
    fi
    if ! systemctl mask "${unit}"; then
      echo "FAIL: unable to mask unit '${unit}'" >&2
      failed=1
    fi
  done < <(unique_managed_units)
  if (( failed != 0 )); then
    echo "failed to enter run-mode; marker not updated" >&2
    return 1
  fi
  printf "run-mode\n" > "${MODE_FILE}"
  echo "entered run-mode"
}

restore_unit_state() {
  local unit="$1"
  local enabled="$2"
  local active="$3"
  local failed=0

  if ! systemctl unmask "${unit}"; then
    echo "FAIL: unable to unmask unit '${unit}'" >&2
    failed=1
  fi
  case "${enabled}" in
    enabled)
      if ! systemctl enable "${unit}"; then
        echo "FAIL: unable to enable unit '${unit}'" >&2
        failed=1
      fi
      ;;
    disabled)
      if ! systemctl disable "${unit}"; then
        echo "FAIL: unable to disable unit '${unit}'" >&2
        failed=1
      fi
      ;;
    masked)
      if ! systemctl mask "${unit}"; then
        echo "FAIL: unable to remask unit '${unit}'" >&2
        failed=1
      fi
      ;;
    *)
      ;;
  esac

  case "${active}" in
    active)
      if ! systemctl start "${unit}"; then
        echo "FAIL: unable to start unit '${unit}'" >&2
        failed=1
      fi
      ;;
    inactive|failed|unknown|*)
      ;;
  esac

  return "${failed}"
}

apply_maintenance_mode() {
  require_root
  require_command systemctl
  mkdir -p "${MODE_DIR}"
  local failed=0
  if [[ -f "${STATE_FILE}" ]]; then
    while IFS='|' read -r unit enabled active; do
      [[ -n "${unit}" ]] || continue
      if ! unit_exists "${unit}"; then
        continue
      fi
      if ! restore_unit_state "${unit}" "${enabled}" "${active}"; then
        failed=1
      fi
    done < "${STATE_FILE}"
    if (( failed == 0 )); then
      rm -f "${STATE_FILE}"
    else
      echo "state file retained for retry: ${STATE_FILE}" >&2
    fi
  fi
  if (( failed != 0 )); then
    echo "failed to enter maintenance-mode; marker not updated" >&2
    return 1
  fi
  printf "maintenance-mode\n" > "${MODE_FILE}"
  echo "entered maintenance-mode"
}

status_mode() {
  local mode="unknown"
  if [[ -f "${MODE_FILE}" ]]; then
    mode="$(tr -d '\n\r\t ' < "${MODE_FILE}")"
  fi
  echo "mode=${mode}"
  echo "mode_file=${MODE_FILE}"
  if [[ -f "${STATE_FILE}" ]]; then
    echo "state_file_present=true"
  else
    echo "state_file_present=false"
  fi
}

with_lock() {
  if ! command -v flock >/dev/null 2>&1; then
    echo "WARN: flock not found; running without transition lock" >&2
    "$@"
    return
  fi
  mkdir -p "$(dirname "${LOCK_FILE}")"
  exec 9>"${LOCK_FILE}"
  if ! flock -w "${LOCK_TIMEOUT_SECONDS}" 9; then
    echo "failed to acquire lock within ${LOCK_TIMEOUT_SECONDS}s: ${LOCK_FILE}" >&2
    return 1
  fi
  "$@"
}

cmd="${1:-}"
case "${cmd}" in
  run-mode)
    with_lock apply_run_mode
    ;;
  maintenance-mode)
    with_lock apply_maintenance_mode
    ;;
  status)
    with_lock status_mode
    ;;
  -h|--help|"")
    usage
    exit 0
    ;;
  *)
    echo "unknown command: ${cmd}" >&2
    usage >&2
    exit 1
    ;;
esac
