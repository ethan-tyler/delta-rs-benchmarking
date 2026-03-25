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
DELTA_BENCH_CHECKOUT_LOCK_FILE="${DELTA_BENCH_CHECKOUT_LOCK_FILE:-$(default_checkout_lock_file "${DELTA_RS_DIR}")}"
DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS="${DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS:-300}"
CHECKOUT_LOCK_FD=""
CHECKOUT_LOCK_DIR=""

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

trap release_checkout_lock EXIT

if [[ ! -d "${DELTA_RS_DIR}/.git" ]]; then
  echo "cloning ${DELTA_RS_REPO_URL} into ${DELTA_RS_DIR}"
  git clone --origin origin "${DELTA_RS_REPO_URL}" "${DELTA_RS_DIR}"
fi

acquire_checkout_lock
cleanup_harness_overlay_untracked
git -C "${DELTA_RS_DIR}" fetch origin

if [[ -n "${DELTA_RS_REF}" ]]; then
  case "${DELTA_RS_REF_TYPE}" in
    auto|commit)
      if ! git -C "${DELTA_RS_DIR}" rev-parse --verify --quiet "${DELTA_RS_REF}^{commit}" >/dev/null; then
        echo "delta-rs ref '${DELTA_RS_REF}' is not available after fetch; provide a reachable commit SHA" >&2
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
