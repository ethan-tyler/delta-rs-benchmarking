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
  ./scripts/compare_branch.sh [options] --working-vs-upstream-main [suite]

Options:
  --remote-runner <ssh-host>      Run the workflow on a remote runner over SSH
  --remote-root <path>            Repository root path on runner (default: this repo root)
  --enforce-run-mode              Require run-mode marker during preflight checks
  --require-no-public-ipv4        Require that no public IPv4 is assigned to runner interfaces
  --require-egress-policy         Require nftables egress hash check during preflight (set DELTA_BENCH_EGRESS_POLICY_SHA256)
  --noise-threshold <float>       Override compare.py noise threshold (default: 0.05)
  --aggregation <min|median|p95>  Representative sample aggregation for compare.py (default: median)
  --base-sha <sha>                Force immutable commit mode for the base revision
  --candidate-sha <sha>           Force immutable commit mode for the candidate revision
  --working-vs-upstream-main      Compare current HEAD commit against latest <remote>/main
  --upstream-remote <name>        Remote used with --working-vs-upstream-main (default: upstream, else origin)
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
    --base-sha)
      BASE_SHA_OVERRIDE="$2"
      shift 2
      ;;
    --candidate-sha)
      CANDIDATE_SHA_OVERRIDE="$2"
      shift 2
      ;;
    --working-vs-upstream-main)
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
    echo "--working-vs-upstream-main cannot be combined with --base-sha/--candidate-sha" >&2
    exit 1
  fi
  if (( ${#positional_refs[@]} > 1 )); then
    echo "with --working-vs-upstream-main, provide at most one positional [suite]" >&2
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

DELTA_RS_DIR="${DELTA_RS_DIR:-${RUNNER_ROOT}/.delta-rs-under-test}"
RUNNER_RESULTS_DIR="${DELTA_BENCH_RESULTS:-${RUNNER_ROOT}/results}"
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

run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/prepare_delta_rs.sh

base_label="base-$(sanitize_label "${base_ref}")"
cand_label="cand-$(sanitize_label "${candidate_ref}")"

ensure_known_ref_mode "${base_ref}" "${base_ref_mode}"
ensure_known_ref_mode "${candidate_ref}" "${candidate_ref_mode}"

prepare_delta_rs_ref "${base_ref}" "${base_ref_mode}"
run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/sync_harness_to_delta_rs.sh
run_security_check
run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" DELTA_BENCH_RESULTS="${RUNNER_RESULTS_DIR}" DELTA_BENCH_LABEL="${base_label}" ./scripts/bench.sh data --scale sf1 --seed 42 "${storage_args[@]}" ${profile_args[@]+"${profile_args[@]}"}
run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" DELTA_BENCH_RESULTS="${RUNNER_RESULTS_DIR}" DELTA_BENCH_LABEL="${base_label}" ./scripts/bench.sh run --scale sf1 --suite "${suite}" --runner "${RUNNER_MODE}" --warmup 1 --iters 5 "${storage_args[@]}" ${profile_args[@]+"${profile_args[@]}"}

prepare_delta_rs_ref "${candidate_ref}" "${candidate_ref_mode}"
run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/sync_harness_to_delta_rs.sh
run_security_check
run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" DELTA_BENCH_RESULTS="${RUNNER_RESULTS_DIR}" DELTA_BENCH_LABEL="${cand_label}" ./scripts/bench.sh run --scale sf1 --suite "${suite}" --runner "${RUNNER_MODE}" --warmup 1 --iters 5 "${storage_args[@]}" ${profile_args[@]+"${profile_args[@]}"}

base_json="${RUNNER_RESULTS_DIR}/${base_label}/${suite}.json"
cand_json="${RUNNER_RESULTS_DIR}/${cand_label}/${suite}.json"

compare_args=(--noise-threshold "${NOISE_THRESHOLD}" --aggregation "${AGGREGATION}" --format markdown)

run_step env PYTHONPATH="${RUNNER_ROOT}/python" python3 -m delta_bench_compare.compare "${base_json}" "${cand_json}" "${compare_args[@]}"
