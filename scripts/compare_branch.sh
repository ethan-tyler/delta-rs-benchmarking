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
AGGREGATION="${BENCH_AGGREGATION:-median}"
BENCH_WARMUP="${BENCH_WARMUP:-2}"
BENCH_ITERS="${BENCH_ITERS:-9}"
BENCH_PREWARM_ITERS="${BENCH_PREWARM_ITERS:-1}"
BENCH_COMPARE_RUNS="${BENCH_COMPARE_RUNS:-3}"
BENCH_MEASURE_ORDER="${BENCH_MEASURE_ORDER:-alternate}"
BASE_SHA_OVERRIDE=""
CANDIDATE_SHA_OVERRIDE=""
WORKING_VS_UPSTREAM_MAIN=0
UPSTREAM_REMOTE_OVERRIDE=""
STORAGE_BACKEND="${BENCH_STORAGE_BACKEND:-local}"
STORAGE_OPTIONS=()
BACKEND_PROFILE="${BENCH_BACKEND_PROFILE:-}"
RUNNER_MODE="${BENCH_RUNNER_MODE:-all}"

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

is_positive_integer() {
  [[ "${1:-}" =~ ^[1-9][0-9]*$ ]]
}

is_non_negative_integer() {
  [[ "${1:-}" =~ ^[0-9]+$ ]]
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
  ./scripts/compare_branch.sh [options] <base_ref> <candidate_ref> [suite]
  ./scripts/compare_branch.sh [options] --current-vs-main [suite]

Options:
  --remote-runner <ssh-host>      Run the workflow on a remote runner over SSH
  --remote-root <path>            Repository root path on runner (default: this repo root)
  --enforce-run-mode              Require run-mode marker during preflight checks
  --require-no-public-ipv4        Require that no public IPv4 is assigned to runner interfaces
  --require-egress-policy         Require nftables egress hash check during preflight (set DELTA_BENCH_EGRESS_POLICY_SHA256)
  --noise-threshold <float>       Override compare.py noise threshold (default: 0.05)
  --aggregation <min|median|p95>  Representative sample aggregation for compare.py (default: median)
  --warmup <N>                    Warmup iterations per benchmark case (default: ${BENCH_WARMUP})
  --iters <N>                     Measured iterations per benchmark case (default: ${BENCH_ITERS})
  --prewarm-iters <N>             Unreported prewarm iterations per ref before measured runs (default: ${BENCH_PREWARM_ITERS})
  --compare-runs <N>              Number of measured runs per ref before aggregation (default: ${BENCH_COMPARE_RUNS})
  --measure-order <base-first|candidate-first|alternate>
                                  Per-run execution order used for measured runs (default: ${BENCH_MEASURE_ORDER})
  --base-sha <sha>                Force immutable commit mode for the base revision
  --candidate-sha <sha>           Force immutable commit mode for the candidate revision
  --current-vs-main               Compare current HEAD commit against latest <remote>/main
  --working-vs-upstream-main      Legacy alias for --current-vs-main
  --upstream-remote <name>        Remote used with --current-vs-main (default: upstream, else origin)
  --storage-backend <local|s3>
                                  Storage backend for fixture generation and suite execution (default: local)
  --storage-option <KEY=VALUE>    Repeatable storage option forwarded to bench.sh (for non-local backends)
  --backend-profile <name>        Optional backend profile file under backends/<name>.env
  --runner <rust|python|all>      Runner mode forwarded to bench.sh run (default: all)
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
    --noise-threshold)
      NOISE_THRESHOLD="$2"
      shift 2
      ;;
    --aggregation)
      AGGREGATION="$2"
      shift 2
      ;;
    --warmup)
      BENCH_WARMUP="$2"
      shift 2
      ;;
    --iters)
      BENCH_ITERS="$2"
      shift 2
      ;;
    --prewarm-iters)
      BENCH_PREWARM_ITERS="$2"
      shift 2
      ;;
    --compare-runs)
      BENCH_COMPARE_RUNS="$2"
      shift 2
      ;;
    --measure-order)
      BENCH_MEASURE_ORDER="$2"
      shift 2
      ;;
    --base-sha)
      BASE_SHA_OVERRIDE="$2"
      shift 2
      ;;
    --candidate-sha)
      CANDIDATE_SHA_OVERRIDE="$2"
      shift 2
      ;;
    --current-vs-main|--working-vs-upstream-main)
      WORKING_VS_UPSTREAM_MAIN=1
      shift
      ;;
    --upstream-remote)
      UPSTREAM_REMOTE_OVERRIDE="$2"
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
    --backend-profile)
      BACKEND_PROFILE="$2"
      shift 2
      ;;
    --runner)
      RUNNER_MODE="$2"
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

positional_refs=("$@")
if (( ${#positional_refs[@]} > 3 )); then
  usage >&2
  exit 1
fi

base_ref="${positional_refs[0]:-main}"
candidate_ref="${positional_refs[1]:-}"
suite="${positional_refs[2]:-all}"
base_ref_mode="auto"
candidate_ref_mode="auto"

if (( WORKING_VS_UPSTREAM_MAIN != 0 )); then
  if [[ -n "${BASE_SHA_OVERRIDE}" || -n "${CANDIDATE_SHA_OVERRIDE}" ]]; then
    echo "--current-vs-main cannot be combined with --base-sha/--candidate-sha" >&2
    exit 1
  fi
  if (( ${#positional_refs[@]} > 1 )); then
    echo "with --current-vs-main, provide at most one positional [suite]" >&2
    usage >&2
    exit 1
  fi
  base_ref=""
  candidate_ref=""
  suite="${positional_refs[0]:-all}"
fi

if [[ -n "${BASE_SHA_OVERRIDE}" ]]; then
  base_ref="${BASE_SHA_OVERRIDE}"
  base_ref_mode="commit"
fi
if [[ -n "${CANDIDATE_SHA_OVERRIDE}" ]]; then
  candidate_ref="${CANDIDATE_SHA_OVERRIDE}"
  candidate_ref_mode="commit"
fi

if [[ -n "${BASE_SHA_OVERRIDE}" && -n "${CANDIDATE_SHA_OVERRIDE}" ]]; then
  case "${#positional_refs[@]}" in
    0)
      ;;
    1)
      suite="${positional_refs[0]}"
      ;;
    *)
      echo "when using both --base-sha and --candidate-sha, provide at most one positional [suite]" >&2
      usage >&2
      exit 1
      ;;
  esac
fi

if [[ -z "${candidate_ref}" && ${WORKING_VS_UPSTREAM_MAIN} -eq 0 ]]; then
  usage >&2
  exit 1
fi
case "${AGGREGATION}" in
  min|median|p95)
    ;;
  *)
    echo "invalid --aggregation '${AGGREGATION}'; expected one of: min, median, p95" >&2
    exit 1
    ;;
esac

if ! is_positive_integer "${BENCH_WARMUP}"; then
  echo "invalid --warmup '${BENCH_WARMUP}'; expected positive integer" >&2
  exit 1
fi

if ! is_positive_integer "${BENCH_ITERS}"; then
  echo "invalid --iters '${BENCH_ITERS}'; expected positive integer" >&2
  exit 1
fi

if ! is_non_negative_integer "${BENCH_PREWARM_ITERS}"; then
  echo "invalid --prewarm-iters '${BENCH_PREWARM_ITERS}'; expected non-negative integer" >&2
  exit 1
fi

if ! is_positive_integer "${BENCH_COMPARE_RUNS}"; then
  echo "invalid --compare-runs '${BENCH_COMPARE_RUNS}'; expected positive integer" >&2
  exit 1
fi

case "${BENCH_MEASURE_ORDER}" in
  base-first|candidate-first|alternate)
    ;;
  *)
    echo "invalid --measure-order '${BENCH_MEASURE_ORDER}'; expected base-first, candidate-first, or alternate" >&2
    exit 1
    ;;
esac

DELTA_RS_DIR="${DELTA_RS_DIR:-${RUNNER_ROOT}/.delta-rs-under-test}"
RUNNER_RESULTS_DIR="${DELTA_BENCH_RESULTS:-${RUNNER_ROOT}/results}"
DELTA_BENCH_CHECKOUT_LOCK_FILE="${DELTA_BENCH_CHECKOUT_LOCK_FILE:-${DELTA_RS_DIR}/.delta_bench_checkout.lock}"
DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS="${DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS:-7200}"
CHECKOUT_LOCK_FD=""
CHECKOUT_LOCK_DIR=""
export DELTA_BENCH_CHECKOUT_LOCK_FILE
export DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS
storage_args=(--storage-backend "${STORAGE_BACKEND}")
if [[ ${#STORAGE_OPTIONS[@]} -gt 0 ]]; then
  for option in "${STORAGE_OPTIONS[@]}"; do
    storage_args+=(--storage-option "${option}")
  done
fi
profile_args=()
if [[ -n "${BACKEND_PROFILE}" ]]; then
  profile_args+=(--backend-profile "${BACKEND_PROFILE}")
fi

is_commit_sha() {
  local ref="${1:-}"
  [[ "${ref}" =~ ^[0-9a-fA-F]{7,40}$ ]]
}

branch_ref_exists() {
  local ref="${1:-}"
  exec_on_runner git -C "${DELTA_RS_DIR}" show-ref --verify --quiet "refs/heads/${ref}" || \
    exec_on_runner git -C "${DELTA_RS_DIR}" show-ref --verify --quiet "refs/remotes/origin/${ref}"
}

print_ref_not_found_guidance() {
  local ref="${1:-}"
  echo "benchmark ref '${ref}' not found in delta-rs checkout '${DELTA_RS_DIR}'." >&2
  echo 'use an existing branch (inspect with: git -C "${DELTA_RS_DIR}" branch -a), or pin SHAs with --base-sha/--candidate-sha.' >&2
}

ensure_known_ref_mode() {
  local ref="${1:-}"
  local mode="${2:-auto}"
  if [[ "${mode}" == "commit" ]]; then
    return 0
  fi
  if branch_ref_exists "${ref}"; then
    return 0
  fi
  if is_commit_sha "${ref}"; then
    return 0
  fi
  print_ref_not_found_guidance "${ref}"
  return 1
}

prepare_delta_rs_ref() {
  local ref="${1:-}"
  local mode="${2:-auto}"
  if [[ "${mode}" == "commit" ]]; then
    run_step env DELTA_RS_REF="${ref}" DELTA_RS_REF_TYPE="commit" DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/prepare_delta_rs.sh
    return
  fi
  if branch_ref_exists "${ref}"; then
    run_step env DELTA_RS_BRANCH="${ref}" DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/prepare_delta_rs.sh
    return
  fi
  if is_commit_sha "${ref}"; then
    run_step env DELTA_RS_REF="${ref}" DELTA_RS_REF_TYPE="commit" DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/prepare_delta_rs.sh
    return
  fi
  print_ref_not_found_guidance "${ref}"
  return 1
}

if [[ -n "${BASE_SHA_OVERRIDE}" ]] && ! is_commit_sha "${BASE_SHA_OVERRIDE}"; then
  echo "invalid --base-sha '${BASE_SHA_OVERRIDE}'; expected 7-40 hex characters" >&2
  exit 1
fi
if [[ -n "${CANDIDATE_SHA_OVERRIDE}" ]] && ! is_commit_sha "${CANDIDATE_SHA_OVERRIDE}"; then
  echo "invalid --candidate-sha '${CANDIDATE_SHA_OVERRIDE}'; expected 7-40 hex characters" >&2
  exit 1
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

run_step_no_retry() {
  exec_on_runner "$@"
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
  if [[ -n "${REMOTE_RUNNER}" ]]; then
    return
  fi
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

acquire_checkout_lock

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
    # Keep delta-rs checkout reusable and avoid stash-pop collisions after compare runs.
    exec_on_runner git -C "${DELTA_RS_DIR}" clean -fd -- "${path}" >/dev/null 2>&1 || true
  done
  release_checkout_lock
}

trap cleanup_harness_overlay_untracked EXIT

if (( WORKING_VS_UPSTREAM_MAIN != 0 )); then
  if ! exec_on_runner test -d "${DELTA_RS_DIR}/.git"; then
    run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/prepare_delta_rs.sh
  fi

  working_head_sha="$(exec_on_runner git -C "${DELTA_RS_DIR}" rev-parse --verify HEAD)"

  upstream_remote="${UPSTREAM_REMOTE_OVERRIDE:-}"
  if [[ -z "${upstream_remote}" ]]; then
    if exec_on_runner git -C "${DELTA_RS_DIR}" remote get-url upstream >/dev/null 2>&1; then
      upstream_remote="upstream"
    else
      upstream_remote="origin"
    fi
  fi

  if ! exec_on_runner git -C "${DELTA_RS_DIR}" remote get-url "${upstream_remote}" >/dev/null 2>&1; then
    echo "remote '${upstream_remote}' is not configured in delta-rs checkout '${DELTA_RS_DIR}'." >&2
    exit 1
  fi

  run_step git -C "${DELTA_RS_DIR}" fetch "${upstream_remote}" main
  upstream_main_sha="$(exec_on_runner git -C "${DELTA_RS_DIR}" rev-parse --verify "refs/remotes/${upstream_remote}/main^{commit}")"

  candidate_ref="${working_head_sha}"
  base_ref="${upstream_main_sha}"
  candidate_ref_mode="commit"
  base_ref_mode="commit"
fi

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

run_benchmark_suite_for_ref() {
  local ref="$1"
  local mode="$2"
  local label="$3"
  local warmup="$4"
  local iters="$5"
  local no_summary_table="${6:-0}"

  prepare_delta_rs_ref "${ref}" "${mode}"
  run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/sync_harness_to_delta_rs.sh
  run_security_check

  local run_cmd=(./scripts/bench.sh run --scale sf1 --suite "${suite}" --runner "${RUNNER_MODE}" --warmup "${warmup}" --iters "${iters}")
  run_cmd+=("${storage_args[@]}")
  if [[ ${#profile_args[@]} -gt 0 ]]; then
    run_cmd+=("${profile_args[@]}")
  fi
  if (( no_summary_table != 0 )); then
    run_cmd+=(--no-summary-table)
  fi

  run_step_no_retry env DELTA_RS_DIR="${DELTA_RS_DIR}" DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" DELTA_BENCH_RESULTS="${RUNNER_RESULTS_DIR}" DELTA_BENCH_LABEL="${label}" "${run_cmd[@]}"
}

run_order_for_iteration() {
  local idx="$1"
  case "${BENCH_MEASURE_ORDER}" in
    base-first)
      printf 'base candidate\n'
      ;;
    candidate-first)
      printf 'candidate base\n'
      ;;
    alternate)
      if (( idx % 2 == 1 )); then
        printf 'base candidate\n'
      else
        printf 'candidate base\n'
      fi
      ;;
  esac
}

aggregate_run_labels() {
  local out_label="$1"
  shift
  local labels=("$@")
  if (( ${#labels[@]} == 0 )); then
    echo "internal error: aggregate_run_labels called without labels for ${out_label}" >&2
    exit 1
  fi

  local input_paths=()
  local label
  for label in "${labels[@]}"; do
    input_paths+=("${RUNNER_RESULTS_DIR}/${label}/${suite}.json")
  done

  local out_json="${RUNNER_RESULTS_DIR}/${out_label}/${suite}.json"
  run_step env PYTHONPATH="${RUNNER_ROOT}/python" python3 -m delta_bench_compare.aggregate --output "${out_json}" --label "${out_label}" "${input_paths[@]}"
}

run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/prepare_delta_rs.sh

base_label="base-$(sanitize_label "${base_ref}")"
cand_label="cand-$(sanitize_label "${candidate_ref}")"

ensure_known_ref_mode "${base_ref}" "${base_ref_mode}"
ensure_known_ref_mode "${candidate_ref}" "${candidate_ref_mode}"

prepare_delta_rs_ref "${base_ref}" "${base_ref_mode}"
run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/sync_harness_to_delta_rs.sh
run_security_check
run_step_no_retry env DELTA_RS_DIR="${DELTA_RS_DIR}" DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" DELTA_BENCH_RESULTS="${RUNNER_RESULTS_DIR}" DELTA_BENCH_LABEL="${base_label}" ./scripts/bench.sh data --scale sf1 --seed 42 "${storage_args[@]}" ${profile_args[@]+"${profile_args[@]}"}

if (( BENCH_PREWARM_ITERS > 0 )); then
  run_benchmark_suite_for_ref "${base_ref}" "${base_ref_mode}" "${base_label}-prewarm" 0 "${BENCH_PREWARM_ITERS}" 1
  run_benchmark_suite_for_ref "${candidate_ref}" "${candidate_ref_mode}" "${cand_label}-prewarm" 0 "${BENCH_PREWARM_ITERS}" 1
fi

base_run_labels=()
cand_run_labels=()
run_idx=1
while (( run_idx <= BENCH_COMPARE_RUNS )); do
  order="$(run_order_for_iteration "${run_idx}")"
  for side in ${order}; do
    if [[ "${side}" == "base" ]]; then
      run_label="${base_label}-r${run_idx}"
      run_benchmark_suite_for_ref "${base_ref}" "${base_ref_mode}" "${run_label}" "${BENCH_WARMUP}" "${BENCH_ITERS}" 0
      base_run_labels+=("${run_label}")
    else
      run_label="${cand_label}-r${run_idx}"
      run_benchmark_suite_for_ref "${candidate_ref}" "${candidate_ref_mode}" "${run_label}" "${BENCH_WARMUP}" "${BENCH_ITERS}" 0
      cand_run_labels+=("${run_label}")
    fi
  done
  run_idx=$((run_idx + 1))
done

aggregate_run_labels "${base_label}" "${base_run_labels[@]}"
aggregate_run_labels "${cand_label}" "${cand_run_labels[@]}"

base_json="${RUNNER_RESULTS_DIR}/${base_label}/${suite}.json"
cand_json="${RUNNER_RESULTS_DIR}/${cand_label}/${suite}.json"

compare_args=(--noise-threshold "${NOISE_THRESHOLD}" --aggregation "${AGGREGATION}" --format text)

run_step env PYTHONPATH="${RUNNER_ROOT}/python" python3 -m delta_bench_compare.compare "${base_json}" "${cand_json}" "${compare_args[@]}"
run_step env PYTHONPATH="${RUNNER_ROOT}/python" python3 -m delta_bench_compare.hash_policy "${base_json}" "${cand_json}"
