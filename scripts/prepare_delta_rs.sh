#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DELTA_RS_DIR="${DELTA_RS_DIR:-${ROOT_DIR}/.delta-rs-under-test}"

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

DELTA_RS_REPO_URL="${DELTA_RS_REPO_URL:-https://github.com/delta-io/delta-rs}"
DELTA_RS_BRANCH="${DELTA_RS_BRANCH:-main}"
DELTA_RS_REF="${DELTA_RS_REF:-}"
DELTA_RS_REF_TYPE="${DELTA_RS_REF_TYPE:-auto}"
DELTA_RS_FETCH_URL="${DELTA_RS_FETCH_URL:-}"
DELTA_RS_FETCH_REF="${DELTA_RS_FETCH_REF:-}"
DELTA_BENCH_CHECKOUT_LOCK_FILE="${DELTA_BENCH_CHECKOUT_LOCK_FILE:-$(default_checkout_lock_file "${DELTA_RS_DIR}")}"
DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS="${DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS:-300}"
CHECKOUT_LOCK_FD=""
CHECKOUT_LOCK_DIR=""

path_is_within_dir() {
  python3 - "$1" "$2" <<'PY'
import os
import sys

candidate = os.path.realpath(sys.argv[1])
root = os.path.realpath(sys.argv[2])
try:
    inside = os.path.commonpath([candidate, root]) == root
except ValueError:
    inside = False
raise SystemExit(0 if inside else 1)
PY
}

ensure_checkout_lock_path_safe_for_initial_clone() {
  if [[ -d "${DELTA_RS_DIR}/.git" ]]; then
    return
  fi
  if path_is_within_dir "${DELTA_BENCH_CHECKOUT_LOCK_FILE}" "${DELTA_RS_DIR}"; then
    echo "DELTA_BENCH_CHECKOUT_LOCK_FILE must be outside DELTA_RS_DIR before initial clone: ${DELTA_BENCH_CHECKOUT_LOCK_FILE}" >&2
    exit 1
  fi
}

release_checkout_lock() {
  if [[ -n "${CHECKOUT_LOCK_FD}" ]]; then
    eval "exec ${CHECKOUT_LOCK_FD}>&-" >/dev/null 2>&1 || true
    CHECKOUT_LOCK_FD=""
  fi
  if [[ -n "${CHECKOUT_LOCK_DIR}" ]]; then
    rm -f "${CHECKOUT_LOCK_DIR}/pid" >/dev/null 2>&1 || true
    rmdir "${CHECKOUT_LOCK_DIR}" >/dev/null 2>&1 || true
    CHECKOUT_LOCK_DIR=""
  fi
}

acquire_checkout_lock() {
  if [[ "${DELTA_BENCH_CHECKOUT_LOCK_HELD:-0}" == "1" ]]; then
    return
  fi

  if command -v flock >/dev/null 2>&1; then
    mkdir -p "$(dirname "${DELTA_BENCH_CHECKOUT_LOCK_FILE}")"
    exec {CHECKOUT_LOCK_FD}>"${DELTA_BENCH_CHECKOUT_LOCK_FILE}"
    if ! flock -w "${DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS}" "${CHECKOUT_LOCK_FD}"; then
      echo "failed to acquire checkout lock within ${DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS}s: ${DELTA_BENCH_CHECKOUT_LOCK_FILE}" >&2
      exit 1
    fi
    export DELTA_BENCH_CHECKOUT_LOCK_HELD=1
    return
  fi

  mkdir -p "$(dirname "${DELTA_BENCH_CHECKOUT_LOCK_FILE}")"
  local lock_dir="${DELTA_BENCH_CHECKOUT_LOCK_FILE}.dir"
  local deadline=$((SECONDS + DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS))
  while true; do
    if mkdir "${lock_dir}" >/dev/null 2>&1; then
      CHECKOUT_LOCK_DIR="${lock_dir}"
      printf '%s\n' "$$" > "${CHECKOUT_LOCK_DIR}/pid" || true
      export DELTA_BENCH_CHECKOUT_LOCK_HELD=1
      return
    fi
    if (( SECONDS >= deadline )); then
      echo "failed to acquire checkout lock within ${DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS}s: ${DELTA_BENCH_CHECKOUT_LOCK_FILE}" >&2
      exit 1
    fi
    sleep 1
  done
}

cleanup_harness_overlay_untracked() {
  local managed_paths=(
    "crates/delta-bench"
    "bench/manifests"
    "backends"
    "python/delta_bench_interop"
    "python/delta_bench_tpcds"
  )
  local path
  for path in "${managed_paths[@]}"; do
    # Keep checkout transitions idempotent by removing untracked synced harness artifacts.
    git -C "${DELTA_RS_DIR}" clean -fd -- "${path}" >/dev/null
  done
}

resolve_fetch_ref() {
  local ref="${1:-}"
  if [[ -z "${ref}" || ! "${ref}" =~ ^[0-9a-fA-F]{7,39}$ ]]; then
    printf '%s' "${ref}"
    return 0
  fi

  local ls_remote_output
  if ! ls_remote_output="$(git ls-remote "${DELTA_RS_FETCH_URL}")"; then
    return 1
  fi

  local matches
  matches="$(
    printf '%s\n' "${ls_remote_output}" \
      | awk '{print $1}' \
      | tr 'A-F' 'a-f' \
      | sort -u \
      | grep -i "^${ref}" || true
  )"

  local match_count
  match_count="$(printf '%s\n' "${matches}" | sed '/^$/d' | wc -l | tr -d ' ')"
  case "${match_count}" in
    1)
      printf '%s' "$(printf '%s\n' "${matches}" | sed -n '1p')"
      ;;
    0)
      echo "delta-rs ref '${ref}' is not advertised by alternate fetch URL '${DELTA_RS_FETCH_URL}'; provide the full 40-character SHA or DELTA_RS_FETCH_REF" >&2
      return 1
      ;;
    *)
      echo "delta-rs ref '${ref}' is ambiguous on alternate fetch URL '${DELTA_RS_FETCH_URL}'; provide the full 40-character SHA or DELTA_RS_FETCH_REF" >&2
      return 1
      ;;
  esac
}

fetch_ref_from_alternate_remote_if_needed() {
  if [[ -z "${DELTA_RS_FETCH_URL}" ]]; then
    return 1
  fi

  local target_ref="${DELTA_RS_FETCH_REF:-${DELTA_RS_REF}}"
  if [[ -z "${target_ref}" ]]; then
    return 1
  fi

  if [[ -z "${DELTA_RS_FETCH_REF}" ]]; then
    target_ref="$(resolve_fetch_ref "${target_ref}")" || return 1
  fi

  git -C "${DELTA_RS_DIR}" fetch "${DELTA_RS_FETCH_URL}" "${target_ref}"
}

trap release_checkout_lock EXIT

ensure_checkout_lock_path_safe_for_initial_clone
acquire_checkout_lock

if [[ ! -d "${DELTA_RS_DIR}/.git" ]]; then
  echo "cloning ${DELTA_RS_REPO_URL} into ${DELTA_RS_DIR}"
  git clone --origin origin "${DELTA_RS_REPO_URL}" "${DELTA_RS_DIR}"
fi

cleanup_harness_overlay_untracked
git -C "${DELTA_RS_DIR}" fetch origin

if [[ -n "${DELTA_RS_REF}" ]]; then
  case "${DELTA_RS_REF_TYPE}" in
    auto|commit)
      if ! git -C "${DELTA_RS_DIR}" rev-parse --verify --quiet "${DELTA_RS_REF}^{commit}" >/dev/null; then
        if [[ -n "${DELTA_RS_FETCH_URL}" ]]; then
          fetch_ref_from_alternate_remote_if_needed
        fi
      fi
      if ! git -C "${DELTA_RS_DIR}" rev-parse --verify --quiet "${DELTA_RS_REF}^{commit}" >/dev/null; then
        echo "delta-rs ref '${DELTA_RS_REF}' is not available after fetch; provide a reachable commit SHA, DELTA_RS_FETCH_URL, or DELTA_RS_FETCH_REF" >&2
        exit 1
      fi
      git -C "${DELTA_RS_DIR}" checkout -q --detach "${DELTA_RS_REF}"
      ;;
    *)
      echo "unknown DELTA_RS_REF_TYPE '${DELTA_RS_REF_TYPE}' (expected: auto, commit)" >&2
      exit 1
      ;;
  esac
else
  git -C "${DELTA_RS_DIR}" checkout -q "${DELTA_RS_BRANCH}"
  git -C "${DELTA_RS_DIR}" pull -q --ff-only origin "${DELTA_RS_BRANCH}"
fi

resolved_ref="$(git -C "${DELTA_RS_DIR}" rev-parse --verify HEAD)"
echo "delta-rs checkout ready: ${DELTA_RS_DIR} @ ${resolved_ref}"
