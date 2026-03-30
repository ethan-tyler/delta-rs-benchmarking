#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ROOT_DIR_REAL="$(cd "${ROOT_DIR}" && pwd -P)"

default_checkout_lock_file() {
  local checkout_dir="${1:-}"
  local checkout_parent
  checkout_parent="$(dirname "${checkout_dir}")"
  local checkout_name
  checkout_name="$(basename "${checkout_dir}")"
  checkout_name="${checkout_name#/}"
  while [[ "${checkout_name}" == .* ]]; do
    checkout_name="${checkout_name#.}"
  done
  if [[ -z "${checkout_name}" ]]; then
    checkout_name="delta-rs-under-test"
  fi
  printf '%s/.%s.delta_bench_checkout.lock\n' "${checkout_parent}" "${checkout_name}"
}

RESULTS_DIR="${DELTA_BENCH_RESULTS:-${ROOT_DIR}/results}"
COMPARE_CHECKOUT_ROOT="${DELTA_BENCH_COMPARE_CHECKOUT_ROOT:-${ROOT_DIR}/.delta-bench-compare-checkouts}"
FIXTURES_DIR="${DELTA_BENCH_FIXTURES:-${ROOT_DIR}/fixtures}"
DELTA_RS_DIR="${DELTA_RS_DIR:-${ROOT_DIR}/.delta-rs-under-test}"
DELTA_BENCH_CHECKOUT_LOCK_FILE="${DELTA_BENCH_CHECKOUT_LOCK_FILE:-$(default_checkout_lock_file "${DELTA_RS_DIR}")}"

MODE="dry-run"
TARGET_RESULTS=0
TARGET_COMPARE_CHECKOUTS=0
TARGET_FIXTURES=0
TARGET_DELTA_RS_UNDER_TEST=0
ALLOW_OUTSIDE_ROOT=0
KEEP_LAST=""
OLDER_THAN_DAYS=""

usage() {
  cat <<'EOF'
Usage:
  ./scripts/cleanup_local.sh [options]

Modes:
  --dry-run                    Preview cleanup actions without deleting (default)
  --apply                      Execute deletion actions

Targets:
  --results                    Clean entries under results/ (supports retention flags)
  --compare-checkouts          Clean entries under .delta-bench-compare-checkouts/ (supports retention flags)
  --fixtures                   Remove fixtures/ directory
  --delta-rs-under-test        Remove .delta-rs-under-test/ checkout directory

Retention controls:
  --keep-last <N>              Keep newest N top-level results/compare-checkout entries
  --older-than-days <N>        Only include entries older than N days

Other:
  --allow-outside-root         Allow --apply deletions outside repository root
  -h, --help                   Show this help

Notes:
  - Destructive cleanup never runs unless --apply is provided.
  - --apply refuses to delete outside the repo root unless --allow-outside-root is set.
  - If no target flags are provided, all targets are selected.
  - Retention flags apply only to the --results and --compare-checkouts targets.

Examples:
  ./scripts/cleanup_local.sh --results
  ./scripts/cleanup_local.sh --apply --results --keep-last 5
  ./scripts/cleanup_local.sh --apply --compare-checkouts --keep-last 5
  ./scripts/cleanup_local.sh --apply --results --older-than-days 14
  ./scripts/cleanup_local.sh --apply --fixtures --delta-rs-under-test
  ./scripts/cleanup_local.sh --apply --results --allow-outside-root
  ./scripts/cleanup_local.sh --dry-run --results --keep-last 3 --older-than-days 7
EOF
}

is_non_negative_integer() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
}

canonicalize_existing_path() {
  local candidate="$1"
  if [[ -d "${candidate}" ]]; then
    (cd "${candidate}" >/dev/null 2>&1 && pwd -P)
    return
  fi

  local parent_dir
  parent_dir="$(dirname "${candidate}")"
  local basename_part
  basename_part="$(basename "${candidate}")"
  if [[ -d "${parent_dir}" ]]; then
    local parent_real
    parent_real="$(cd "${parent_dir}" >/dev/null 2>&1 && pwd -P)"
    printf '%s/%s\n' "${parent_real}" "${basename_part}"
    return
  fi

  return 1
}

is_within_repo_root() {
  local candidate_real="$1"
  [[ "${candidate_real}" == "${ROOT_DIR_REAL}" || "${candidate_real}" == "${ROOT_DIR_REAL}/"* ]]
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --apply)
      MODE="apply"
      shift
      ;;
    --dry-run)
      MODE="dry-run"
      shift
      ;;
    --results)
      TARGET_RESULTS=1
      shift
      ;;
    --compare-checkouts)
      TARGET_COMPARE_CHECKOUTS=1
      shift
      ;;
    --fixtures)
      TARGET_FIXTURES=1
      shift
      ;;
    --delta-rs-under-test)
      TARGET_DELTA_RS_UNDER_TEST=1
      shift
      ;;
    --allow-outside-root)
      ALLOW_OUTSIDE_ROOT=1
      shift
      ;;
    --keep-last)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --keep-last" >&2
        exit 1
      fi
      KEEP_LAST="$2"
      shift 2
      ;;
    --older-than-days)
      if [[ $# -lt 2 ]]; then
        echo "missing value for --older-than-days" >&2
        exit 1
      fi
      OLDER_THAN_DAYS="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
  esac
done

if [[ -n "${KEEP_LAST}" ]] && ! is_non_negative_integer "${KEEP_LAST}"; then
  echo "invalid --keep-last '${KEEP_LAST}'; expected non-negative integer" >&2
  exit 1
fi

if [[ -n "${OLDER_THAN_DAYS}" ]] && ! is_non_negative_integer "${OLDER_THAN_DAYS}"; then
  echo "invalid --older-than-days '${OLDER_THAN_DAYS}'; expected non-negative integer" >&2
  exit 1
fi

if (( TARGET_RESULTS == 0 && TARGET_COMPARE_CHECKOUTS == 0 && TARGET_FIXTURES == 0 && TARGET_DELTA_RS_UNDER_TEST == 0 )); then
  TARGET_RESULTS=1
  TARGET_COMPARE_CHECKOUTS=1
  TARGET_FIXTURES=1
  TARGET_DELTA_RS_UNDER_TEST=1
fi

if [[ -n "${KEEP_LAST}" || -n "${OLDER_THAN_DAYS}" ]] && (( TARGET_RESULTS == 0 && TARGET_COMPARE_CHECKOUTS == 0 )); then
  echo "--keep-last/--older-than-days require --results, --compare-checkouts, or no target flags" >&2
  exit 1
fi

echo "Mode: ${MODE}"
if [[ "${MODE}" == "dry-run" ]]; then
  echo "Safety: no files will be deleted. Re-run with --apply to execute cleanup."
fi
echo "Targets:"
if (( TARGET_RESULTS != 0 )); then
  echo "  - results (${RESULTS_DIR})"
fi
if (( TARGET_COMPARE_CHECKOUTS != 0 )); then
  echo "  - compare checkouts (${COMPARE_CHECKOUT_ROOT})"
fi
if (( TARGET_FIXTURES != 0 )); then
  echo "  - fixtures (${FIXTURES_DIR})"
fi
if (( TARGET_DELTA_RS_UNDER_TEST != 0 )); then
  echo "  - delta-rs-under-test (${DELTA_RS_DIR})"
fi
if [[ -n "${KEEP_LAST}" ]]; then
  echo "Retention: keep newest ${KEEP_LAST} entries"
fi
if [[ -n "${OLDER_THAN_DAYS}" ]]; then
  echo "Retention: include only entries older than ${OLDER_THAN_DAYS} days"
fi

paths_to_remove=()

collect_retained_top_level_entries() {
  local root_dir="$1"
  local label="$2"

  if [[ ! -d "${root_dir}" ]]; then
    echo "SKIP: ${label} directory not found at ${root_dir}"
    return
  fi

  shopt -s nullglob
  root_entries=( "${root_dir}"/* )
  shopt -u nullglob
  root_entries_count=0
  for entry in "${root_entries[@]-}"; do
    if [[ -n "${entry}" ]]; then
      root_entries_count=$((root_entries_count + 1))
    fi
  done

  root_candidates=()
  if [[ -n "${OLDER_THAN_DAYS}" ]]; then
    while IFS= read -r candidate; do
      if [[ -n "${candidate}" ]]; then
        root_candidates+=( "${candidate}" )
      fi
    done < <(find "${root_dir}" -mindepth 1 -maxdepth 1 -mtime "+${OLDER_THAN_DAYS}" -print 2>/dev/null || true)
  else
    for entry in "${root_entries[@]-}"; do
      if [[ -n "${entry}" ]]; then
        root_candidates+=( "${entry}" )
      fi
    done
  fi

  protected_entries=()
  if [[ -n "${KEEP_LAST}" ]] && [[ "${KEEP_LAST}" != "0" ]] && (( root_entries_count > 0 )); then
    old_ifs="${IFS}"
    IFS=$'\n'
    sorted_entries=( $(ls -1dt "${root_entries[@]}" 2>/dev/null || true) )
    IFS="${old_ifs}"
    sorted_entries_count=0
    for sorted in "${sorted_entries[@]-}"; do
      if [[ -n "${sorted}" ]]; then
        sorted_entries_count=$((sorted_entries_count + 1))
      fi
    done

    keep_limit="${KEEP_LAST}"
    if (( keep_limit > sorted_entries_count )); then
      keep_limit=${sorted_entries_count}
    fi

    idx=0
    while (( idx < keep_limit )); do
      protected_entries+=( "${sorted_entries[$idx]}" )
      idx=$((idx + 1))
    done
  fi

  for candidate in "${root_candidates[@]-}"; do
    if [[ -z "${candidate}" ]]; then
      continue
    fi
    keep_candidate=0
    if [[ -n "${KEEP_LAST}" ]]; then
      for protected in "${protected_entries[@]-}"; do
        if [[ -z "${protected}" ]]; then
          continue
        fi
        if [[ "${candidate}" == "${protected}" ]]; then
          echo "KEEP: ${candidate} (within --keep-last ${KEEP_LAST})"
          keep_candidate=1
          break
        fi
      done
    fi
    if (( keep_candidate == 0 )); then
      paths_to_remove+=( "${candidate}" )
    fi
  done
}

if (( TARGET_RESULTS != 0 )); then
  collect_retained_top_level_entries "${RESULTS_DIR}" "results"
fi

if (( TARGET_COMPARE_CHECKOUTS != 0 )); then
  collect_retained_top_level_entries "${COMPARE_CHECKOUT_ROOT}" "compare checkouts"
fi

if (( TARGET_FIXTURES != 0 )); then
  if [[ -d "${FIXTURES_DIR}" ]]; then
    paths_to_remove+=( "${FIXTURES_DIR}" )
  else
    echo "SKIP: fixtures directory not found at ${FIXTURES_DIR}"
  fi
fi

if (( TARGET_DELTA_RS_UNDER_TEST != 0 )); then
  if [[ -d "${DELTA_RS_DIR}" ]]; then
    paths_to_remove+=( "${DELTA_RS_DIR}" )
  else
    echo "SKIP: delta-rs-under-test directory not found at ${DELTA_RS_DIR}"
  fi
  if [[ -e "${DELTA_BENCH_CHECKOUT_LOCK_FILE}" ]]; then
    paths_to_remove+=( "${DELTA_BENCH_CHECKOUT_LOCK_FILE}" )
  fi
  if [[ -d "${DELTA_BENCH_CHECKOUT_LOCK_FILE}.dir" ]]; then
    paths_to_remove+=( "${DELTA_BENCH_CHECKOUT_LOCK_FILE}.dir" )
  fi
fi

if (( ${#paths_to_remove[@]} == 0 )); then
  echo "No matching artifacts to clean."
  exit 0
fi

if [[ "${MODE}" == "apply" ]] && (( ALLOW_OUTSIDE_ROOT == 0 )); then
  for path in "${paths_to_remove[@]-}"; do
    if [[ -z "${path}" ]]; then
      continue
    fi
    path_real="$(canonicalize_existing_path "${path}" || true)"
    if [[ -z "${path_real}" ]]; then
      echo "unable to resolve path for safety check: ${path}" >&2
      exit 1
    fi
    if ! is_within_repo_root "${path_real}"; then
      echo "refusing to delete outside repository root without --allow-outside-root: ${path}" >&2
      exit 1
    fi
  done
fi

actions=0
for path in "${paths_to_remove[@]-}"; do
  if [[ -z "${path}" ]]; then
    continue
  fi
  if [[ "${MODE}" == "apply" ]]; then
    echo "APPLY: rm -rf ${path}"
    rm -rf -- "${path}"
  else
    echo "DRY-RUN: rm -rf ${path}"
  fi
  actions=$((actions + 1))
done

if [[ "${MODE}" == "apply" ]]; then
  echo "Cleanup complete. Removed ${actions} path(s)."
else
  echo "Dry-run complete. ${actions} path(s) would be removed."
fi
