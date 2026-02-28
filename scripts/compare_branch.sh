#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
BENCH_TIMEOUT_SECONDS="${BENCH_TIMEOUT_SECONDS:-3600}"
BENCH_RETRY_ATTEMPTS="${BENCH_RETRY_ATTEMPTS:-2}"
BENCH_RETRY_DELAY_SECONDS="${BENCH_RETRY_DELAY_SECONDS:-5}"
REMOTE_RUNNER=""
RUNNER_ROOT="${ROOT_DIR}"
ENFORCE_RUN_MODE=0
REQUIRE_NO_PUBLIC_IPV4=0
REQUIRE_EGRESS_POLICY=0
NOISE_THRESHOLD="${BENCH_NOISE_THRESHOLD:-0.05}"
STORAGE_BACKEND="${BENCH_STORAGE_BACKEND:-local}"
STORAGE_OPTIONS=()

sanitize_label() {
  local raw="${1:-}"
  local sanitized
  sanitized="$(printf '%s' "${raw}" | tr -c 'A-Za-z0-9._-' '_')"
  sanitized="$(printf '%s' "${sanitized}" | sed -E 's/_+/_/g; s/^_+//; s/_+$//')"
  if [[ -z "${sanitized}" || "${sanitized}" == "." || "${sanitized}" == ".." ]]; then
    sanitized="label"
  fi
  printf '%s' "${sanitized}"
}

TIMEOUT_BIN=""
if command -v timeout >/dev/null 2>&1; then
  TIMEOUT_BIN="timeout"
elif command -v gtimeout >/dev/null 2>&1; then
  TIMEOUT_BIN="gtimeout"
fi

usage() {
  cat <<EOF
Usage:
  ./scripts/compare_branch.sh [options] <base_branch> <candidate_branch> [suite]

Options:
  --remote-runner <ssh-host>      Run the workflow on a remote runner over SSH
  --remote-root <path>            Repository root path on runner (default: this repo root)
  --enforce-run-mode              Require run-mode marker during preflight checks
  --require-no-public-ipv4        Require that no public IPv4 is assigned to runner interfaces
  --require-egress-policy         Require nftables egress hash check during preflight (set DELTA_BENCH_EGRESS_POLICY_SHA256)
  --ci                            Deprecated no-op (benchmarks are advisory only)
  --noise-threshold <float>       Override compare.py noise threshold (default: 0.05)
  --max-allowed-regressions <n>   Deprecated no-op (benchmarks are advisory only)
  --storage-backend <local|s3|gcs|azure>
                                  Storage backend for fixture generation and suite execution (default: local)
  --storage-option <KEY=VALUE>    Repeatable storage option forwarded to bench.sh (for non-local backends)
  -h, --help                      Show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --remote-runner)
      REMOTE_RUNNER="$2"
      shift 2
      ;;
    --remote-root)
      RUNNER_ROOT="$2"
      shift 2
      ;;
    --enforce-run-mode)
      ENFORCE_RUN_MODE=1
      shift
      ;;
    --require-no-public-ipv4)
      REQUIRE_NO_PUBLIC_IPV4=1
      shift
      ;;
    --require-egress-policy)
      REQUIRE_EGRESS_POLICY=1
      shift
      ;;
    --ci)
      echo "warning: --ci is deprecated and ignored; benchmark compare is advisory-only" >&2
      shift
      ;;
    --noise-threshold)
      NOISE_THRESHOLD="$2"
      shift 2
      ;;
    --max-allowed-regressions)
      echo "warning: --max-allowed-regressions is deprecated and ignored; benchmark compare is advisory-only" >&2
      shift 2
      ;;
    --storage-backend)
      STORAGE_BACKEND="$2"
      shift 2
      ;;
    --storage-option)
      STORAGE_OPTIONS+=("$2")
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    --)
      shift
      break
      ;;
    -*)
      echo "unknown option: $1" >&2
      usage >&2
      exit 1
      ;;
    *)
      break
      ;;
  esac
done

base_branch="${1:-main}"
candidate_branch="${2:-}"
suite="${3:-all}"

if [[ -z "${candidate_branch}" ]]; then
  usage >&2
  exit 1
fi

DELTA_RS_DIR="${DELTA_RS_DIR:-${RUNNER_ROOT}/.delta-rs-under-test}"
RUNNER_RESULTS_DIR="${DELTA_BENCH_RESULTS:-${RUNNER_ROOT}/results}"
storage_args=(--storage-backend "${STORAGE_BACKEND}")
if [[ ${#STORAGE_OPTIONS[@]} -gt 0 ]]; then
  for option in "${STORAGE_OPTIONS[@]}"; do
    storage_args+=(--storage-option "${option}")
  done
fi

run_with_timeout() {
  if [[ -n "${TIMEOUT_BIN}" ]]; then
    "${TIMEOUT_BIN}" "${BENCH_TIMEOUT_SECONDS}" "$@"
    return
  fi
  "$@"
}

run_with_retry() {
  local attempt=1
  while true; do
    if "$@"; then
      return 0
    fi
    if (( attempt >= BENCH_RETRY_ATTEMPTS )); then
      return 1
    fi
    attempt=$((attempt + 1))
    sleep "${BENCH_RETRY_DELAY_SECONDS}"
  done
}

run_step() {
  run_with_retry exec_on_runner "$@"
}

exec_on_runner() {
  if [[ -n "${REMOTE_RUNNER}" ]]; then
    local remote_cmd=""
    local arg
    for arg in "$@"; do
      remote_cmd+=$(printf '%q ' "${arg}")
    done
    local full_cmd
    full_cmd=$(printf "cd %q && %s" "${RUNNER_ROOT}" "${remote_cmd}")
    run_with_timeout ssh "${REMOTE_RUNNER}" "bash -lc $(printf '%q' "${full_cmd}")"
  else
    (
      cd "${RUNNER_ROOT}"
      run_with_timeout "$@"
    )
  fi
}

run_security_check() {
  local check_cmd=(./scripts/security_check.sh)
  if (( ENFORCE_RUN_MODE != 0 )); then
    check_cmd+=(--enforce-run-mode)
  fi
  if (( REQUIRE_NO_PUBLIC_IPV4 != 0 )); then
    check_cmd+=(--require-no-public-ipv4)
  fi
  if (( REQUIRE_EGRESS_POLICY != 0 )); then
    check_cmd+=(--require-egress-policy)
  fi

  if [[ -n "${DELTA_BENCH_EGRESS_POLICY_SHA256:-}" ]]; then
    run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" DELTA_BENCH_EGRESS_POLICY_SHA256="${DELTA_BENCH_EGRESS_POLICY_SHA256}" "${check_cmd[@]}"
  else
    run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" "${check_cmd[@]}"
  fi
}

run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/prepare_delta_rs.sh

base_label="base-$(sanitize_label "${base_branch}")"
cand_label="cand-$(sanitize_label "${candidate_branch}")"

run_step env DELTA_RS_BRANCH="${base_branch}" DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/prepare_delta_rs.sh
run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/sync_harness_to_delta_rs.sh
run_security_check
run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" DELTA_BENCH_RESULTS="${RUNNER_RESULTS_DIR}" DELTA_BENCH_LABEL="${base_label}" ./scripts/bench.sh data --scale sf1 --seed 42 "${storage_args[@]}"
run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" DELTA_BENCH_RESULTS="${RUNNER_RESULTS_DIR}" DELTA_BENCH_LABEL="${base_label}" ./scripts/bench.sh run --scale sf1 --suite "${suite}" --warmup 1 --iters 5 "${storage_args[@]}"

run_step env DELTA_RS_BRANCH="${candidate_branch}" DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/prepare_delta_rs.sh
run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/sync_harness_to_delta_rs.sh
run_security_check
run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" DELTA_BENCH_RESULTS="${RUNNER_RESULTS_DIR}" DELTA_BENCH_LABEL="${cand_label}" ./scripts/bench.sh run --scale sf1 --suite "${suite}" --warmup 1 --iters 5 "${storage_args[@]}"

base_json="${RUNNER_RESULTS_DIR}/${base_label}/${suite}.json"
cand_json="${RUNNER_RESULTS_DIR}/${cand_label}/${suite}.json"

compare_args=(--noise-threshold "${NOISE_THRESHOLD}" --format markdown)

run_step env PYTHONPATH="${RUNNER_ROOT}/python" python3 -m delta_bench_compare.compare "${base_json}" "${cand_json}" "${compare_args[@]}"
