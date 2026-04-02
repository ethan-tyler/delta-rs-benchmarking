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
COMPARE_MODE="${BENCH_COMPARE_MODE:-exploratory}"
COMPARE_FAIL_ON="${BENCH_COMPARE_FAIL_ON:-}"
BENCH_WARMUP="${BENCH_WARMUP:-2}"
BENCH_ITERS="${BENCH_ITERS:-9}"
BENCH_PREWARM_ITERS="${BENCH_PREWARM_ITERS:-1}"
BENCH_COMPARE_RUNS="${BENCH_COMPARE_RUNS:-3}"
METHODOLOGY_PROFILE="${BENCH_METHODOLOGY_PROFILE:-}"
METHODOLOGY_VERSION=""
DELTA_BENCH_MIN_FREE_GB="${DELTA_BENCH_MIN_FREE_GB:-20}"
BENCH_MEASURE_ORDER="${BENCH_MEASURE_ORDER:-alternate}"
BASE_SHA_OVERRIDE=""
CANDIDATE_SHA_OVERRIDE=""
BASE_FETCH_URL=""
CANDIDATE_FETCH_URL=""
WORKING_VS_UPSTREAM_MAIN=0
UPSTREAM_REMOTE_OVERRIDE=""
STORAGE_BACKEND="${BENCH_STORAGE_BACKEND:-local}"
STORAGE_OPTIONS=()
BACKEND_PROFILE="${BENCH_BACKEND_PROFILE:-}"
RUNNER_MODE="${BENCH_RUNNER_MODE:-all}"
BENCHMARK_MODE="${BENCH_BENCHMARK_MODE:-perf}"
DATASET_ID="${BENCH_DATASET_ID:-}"
TIMING_PHASE="${BENCH_TIMING_PHASE:-execute}"
DATASET_POLICY=""
SPREAD_METRIC=""
SUB_MS_THRESHOLD_MS=""
SUB_MS_POLICY=""
AGGREGATION_EXPLICIT=0
COMPARE_MODE_EXPLICIT=0
BENCH_WARMUP_EXPLICIT=0
BENCH_ITERS_EXPLICIT=0
BENCH_PREWARM_ITERS_EXPLICIT=0
BENCH_COMPARE_RUNS_EXPLICIT=0
BENCH_MEASURE_ORDER_EXPLICIT=0
TIMING_PHASE_EXPLICIT=0

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

methodology_profile_path() {
	local profile_name="${1:-}"
	case "${profile_name}" in
	pr-macro)
		printf '%s\n' "${ROOT_DIR}/bench/methodologies/pr-macro.env"
		;;
	*)
		return 1
		;;
	esac
}

load_methodology_profile() {
	local profile_name="${1:-}"
	local profile_path=""
	if ! profile_path="$(methodology_profile_path "${profile_name}")"; then
		echo "unknown --methodology-profile '${profile_name}'; expected one of: pr-macro" >&2
		exit 1
	fi
	if [[ ! -f "${profile_path}" ]]; then
		echo "methodology profile '${profile_name}' not found at ${profile_path}" >&2
		exit 1
	fi

	local profile_output=""
	profile_output="$(
		(
			set -euo pipefail
			source "${profile_path}"
			printf 'METHODOLOGY_PROFILE=%s\n' "${METHODOLOGY_PROFILE-}"
			printf 'METHODOLOGY_VERSION=%s\n' "${METHODOLOGY_VERSION-}"
			printf 'COMPARE_MODE=%s\n' "${COMPARE_MODE-}"
			printf 'WARMUP=%s\n' "${WARMUP-}"
			printf 'ITERS=%s\n' "${ITERS-}"
			printf 'PREWARM_ITERS=%s\n' "${PREWARM_ITERS-}"
			printf 'COMPARE_RUNS=%s\n' "${COMPARE_RUNS-}"
			printf 'MEASURE_ORDER=%s\n' "${MEASURE_ORDER-}"
			printf 'TIMING_PHASE=%s\n' "${TIMING_PHASE-}"
			printf 'AGGREGATION=%s\n' "${AGGREGATION-}"
			printf 'DATASET_POLICY=%s\n' "${DATASET_POLICY-}"
			printf 'SPREAD_METRIC=%s\n' "${SPREAD_METRIC-}"
			printf 'SUB_MS_THRESHOLD_MS=%s\n' "${SUB_MS_THRESHOLD_MS-}"
			printf 'SUB_MS_POLICY=%s\n' "${SUB_MS_POLICY-}"
		)
	)"

	local profile_declared_name=""
	local profile_version=""
	local profile_compare_mode=""
	local profile_warmup=""
	local profile_iters=""
	local profile_prewarm_iters=""
	local profile_compare_runs=""
	local profile_measure_order=""
	local profile_timing_phase=""
	local profile_aggregation=""
	local profile_dataset_policy=""
	local profile_spread_metric=""
	local profile_sub_ms_threshold_ms=""
	local profile_sub_ms_policy=""

	while IFS='=' read -r key value; do
		case "${key}" in
		METHODOLOGY_PROFILE)
			profile_declared_name="${value}"
			;;
		METHODOLOGY_VERSION)
			profile_version="${value}"
			;;
		COMPARE_MODE)
			profile_compare_mode="${value}"
			;;
		WARMUP)
			profile_warmup="${value}"
			;;
		ITERS)
			profile_iters="${value}"
			;;
		PREWARM_ITERS)
			profile_prewarm_iters="${value}"
			;;
		COMPARE_RUNS)
			profile_compare_runs="${value}"
			;;
		MEASURE_ORDER)
			profile_measure_order="${value}"
			;;
		TIMING_PHASE)
			profile_timing_phase="${value}"
			;;
		AGGREGATION)
			profile_aggregation="${value}"
			;;
		DATASET_POLICY)
			profile_dataset_policy="${value}"
			;;
		SPREAD_METRIC)
			profile_spread_metric="${value}"
			;;
		SUB_MS_THRESHOLD_MS)
			profile_sub_ms_threshold_ms="${value}"
			;;
		SUB_MS_POLICY)
			profile_sub_ms_policy="${value}"
			;;
		esac
	done <<< "${profile_output}"

	if [[ -z "${profile_declared_name}" ]]; then
		echo "methodology profile '${profile_name}' is missing METHODOLOGY_PROFILE" >&2
		exit 1
	fi
	if [[ "${profile_declared_name}" != "${profile_name}" ]]; then
		echo "methodology profile '${profile_name}' declares METHODOLOGY_PROFILE='${profile_declared_name}'" >&2
		exit 1
	fi

	METHODOLOGY_VERSION="${profile_version}"
	DATASET_POLICY="${profile_dataset_policy}"
	SPREAD_METRIC="${profile_spread_metric}"
	SUB_MS_THRESHOLD_MS="${profile_sub_ms_threshold_ms}"
	SUB_MS_POLICY="${profile_sub_ms_policy}"

	if ((COMPARE_MODE_EXPLICIT == 0)) && [[ -n "${profile_compare_mode}" ]]; then
		COMPARE_MODE="${profile_compare_mode}"
	fi
	if ((BENCH_WARMUP_EXPLICIT == 0)) && [[ -n "${profile_warmup}" ]]; then
		BENCH_WARMUP="${profile_warmup}"
	fi
	if ((BENCH_ITERS_EXPLICIT == 0)) && [[ -n "${profile_iters}" ]]; then
		BENCH_ITERS="${profile_iters}"
	fi
	if ((BENCH_PREWARM_ITERS_EXPLICIT == 0)) && [[ -n "${profile_prewarm_iters}" ]]; then
		BENCH_PREWARM_ITERS="${profile_prewarm_iters}"
	fi
	if ((BENCH_COMPARE_RUNS_EXPLICIT == 0)) && [[ -n "${profile_compare_runs}" ]]; then
		BENCH_COMPARE_RUNS="${profile_compare_runs}"
	fi
	if ((BENCH_MEASURE_ORDER_EXPLICIT == 0)) && [[ -n "${profile_measure_order}" ]]; then
		BENCH_MEASURE_ORDER="${profile_measure_order}"
	fi
	if ((TIMING_PHASE_EXPLICIT == 0)) && [[ -n "${profile_timing_phase}" ]]; then
		TIMING_PHASE="${profile_timing_phase}"
	fi
	if ((AGGREGATION_EXPLICIT == 0)) && [[ -n "${profile_aggregation}" ]]; then
		AGGREGATION="${profile_aggregation}"
	fi
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
  --methodology-profile <name>    Load a harness-owned profile from bench/methodologies/<name>.env
  --compare-mode <exploratory|decision>
                                  Compare classification mode passed to compare.py (default: ${COMPARE_MODE})
  --fail-on <statuses>            Comma-separated compare statuses that force exit code 2 (for decision automation)
  --warmup <N>                    Warmup iterations per benchmark case (default: ${BENCH_WARMUP})
  --iters <N>                     Measured iterations per benchmark case (default: ${BENCH_ITERS})
  --prewarm-iters <N>             Unreported prewarm iterations per ref before measured runs (default: ${BENCH_PREWARM_ITERS})
  --compare-runs <N>              Number of measured runs per ref before aggregation (default: ${BENCH_COMPARE_RUNS})
  --measure-order <base-first|candidate-first|alternate>
                                  Per-run execution order used for measured runs (default: ${BENCH_MEASURE_ORDER})
  --base-sha <sha>                Force immutable commit mode for the base revision
  --candidate-sha <sha>           Force immutable commit mode for the candidate revision
  --base-fetch-url <url>          Fetch URL used when base SHA is only reachable from another remote
  --candidate-fetch-url <url>     Fetch URL used when candidate SHA is only reachable from another remote
  --current-vs-main               Compare current HEAD commit against latest <remote>/main
  --working-vs-upstream-main      Legacy alias for --current-vs-main
  --upstream-remote <name>        Remote used with --current-vs-main (default: upstream, else origin)
  --storage-backend <local|s3>
                                  Storage backend for fixture generation and suite execution (default: local)
  --storage-option <KEY=VALUE>    Repeatable storage option forwarded to bench.sh (for non-local backends)
  --backend-profile <name>        Optional backend profile file under backends/<name>.env
  --runner <rust|python|all>      Runner mode forwarded to bench.sh run (default: all)
  --mode <perf|assert>            Benchmark mode forwarded to bench.sh run (default: perf)
  --dataset-id <id>               Dataset id forwarded to bench.sh data/run
  --timing-phase <phase>          Timing phase forwarded to bench.sh run (default: execute)
                                  Trusted macro-lane compare suites: scan, write_perf, tpcds, interop_py (default: scan)
  -h, --help                      Show this help
EOF
}

trusted_macro_compare_suite() {
	local requested_suite="${1:-}"
	case "${requested_suite}" in
	scan | write_perf | tpcds | interop_py)
		return 0
		;;
	*)
		return 1
		;;
	esac
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
		AGGREGATION_EXPLICIT=1
		shift 2
		;;
	--methodology-profile)
		METHODOLOGY_PROFILE="$2"
		shift 2
		;;
	--compare-mode)
		COMPARE_MODE="$2"
		COMPARE_MODE_EXPLICIT=1
		shift 2
		;;
	--fail-on)
		COMPARE_FAIL_ON="$2"
		shift 2
		;;
	--warmup)
		BENCH_WARMUP="$2"
		BENCH_WARMUP_EXPLICIT=1
		shift 2
		;;
	--iters)
		BENCH_ITERS="$2"
		BENCH_ITERS_EXPLICIT=1
		shift 2
		;;
	--prewarm-iters)
		BENCH_PREWARM_ITERS="$2"
		BENCH_PREWARM_ITERS_EXPLICIT=1
		shift 2
		;;
	--compare-runs)
		BENCH_COMPARE_RUNS="$2"
		BENCH_COMPARE_RUNS_EXPLICIT=1
		shift 2
		;;
	--measure-order)
		BENCH_MEASURE_ORDER="$2"
		BENCH_MEASURE_ORDER_EXPLICIT=1
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
	--base-fetch-url)
		BASE_FETCH_URL="$2"
		shift 2
		;;
	--candidate-fetch-url)
		CANDIDATE_FETCH_URL="$2"
		shift 2
		;;
	--current-vs-main | --working-vs-upstream-main)
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
	--mode)
		BENCHMARK_MODE="$2"
		shift 2
		;;
	--dataset-id)
		DATASET_ID="$2"
		shift 2
		;;
	--timing-phase)
		TIMING_PHASE="$2"
		TIMING_PHASE_EXPLICIT=1
		shift 2
		;;
	-h | --help)
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
if ((${#positional_refs[@]} > 3)); then
	usage >&2
	exit 1
fi

base_ref="${positional_refs[0]:-main}"
candidate_ref="${positional_refs[1]:-}"
suite="${positional_refs[2]:-scan}"
base_ref_mode="auto"
candidate_ref_mode="auto"

if ((WORKING_VS_UPSTREAM_MAIN != 0)); then
	if [[ -n "${BASE_SHA_OVERRIDE}" || -n "${CANDIDATE_SHA_OVERRIDE}" ]]; then
		echo "--current-vs-main cannot be combined with --base-sha/--candidate-sha" >&2
		exit 1
	fi
	if ((${#positional_refs[@]} > 1)); then
		echo "with --current-vs-main, provide at most one positional [suite]" >&2
		usage >&2
		exit 1
	fi
	base_ref=""
	candidate_ref=""
	suite="${positional_refs[0]:-scan}"
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
	0) ;;
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

if [[ -n "${METHODOLOGY_PROFILE}" ]]; then
	load_methodology_profile "${METHODOLOGY_PROFILE}"
fi

case "${AGGREGATION}" in
min | median | p95) ;;
*)
	echo "invalid --aggregation '${AGGREGATION}'; expected one of: min, median, p95" >&2
	exit 1
	;;
esac

case "${COMPARE_MODE}" in
exploratory | decision) ;;
*)
	echo "invalid --compare-mode '${COMPARE_MODE}'; expected exploratory or decision" >&2
	exit 1
	;;
esac

if ! trusted_macro_compare_suite "${suite}"; then
	echo "suite '${suite}' is not supported for macro-lane branch compare." >&2
	echo "compare_branch.sh supports only trusted perf suites: scan, write_perf, tpcds, interop_py." >&2
	echo "use suite 'scan' for the curated default, or run unsupported stateful suites through purpose-built validation/longitudinal flows." >&2
	exit 1
fi

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

if [[ "${COMPARE_MODE}" == "decision" ]]; then
	if ((BENCH_COMPARE_RUNS < 5)); then
		echo "decision mode requires --compare-runs >= 5" >&2
		exit 1
	fi
fi

case "${BENCH_MEASURE_ORDER}" in
base-first | candidate-first | alternate) ;;
*)
	echo "invalid --measure-order '${BENCH_MEASURE_ORDER}'; expected base-first, candidate-first, or alternate" >&2
	exit 1
	;;
esac

case "${BENCHMARK_MODE}" in
perf) ;;
assert)
	echo "compare_branch.sh requires --mode perf; assert mode emits validation-only artifacts that cannot be compared" >&2
	exit 1
	;;
*)
	echo "invalid --mode '${BENCHMARK_MODE}'; expected perf or assert" >&2
	exit 1
	;;
esac

DELTA_RS_DIR="${DELTA_RS_DIR:-${RUNNER_ROOT}/.delta-rs-under-test}"
DELTA_RS_SOURCE_DIR="${DELTA_RS_SOURCE_DIR:-${RUNNER_ROOT}/.delta-rs-source}"
RUNNER_RESULTS_DIR="${DELTA_BENCH_RESULTS:-${RUNNER_ROOT}/results}"
compare_checkout_root="${DELTA_BENCH_COMPARE_CHECKOUT_ROOT:-${RUNNER_ROOT}/.delta-bench-compare-checkouts}"

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

DELTA_BENCH_CHECKOUT_LOCK_FILE="${DELTA_BENCH_CHECKOUT_LOCK_FILE:-$(default_checkout_lock_file "${DELTA_RS_DIR}")}"
DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS="${DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS:-7200}"
CHECKOUT_LOCK_FD=""
CHECKOUT_LOCK_DIR=""
DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE="${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE:-$(default_checkout_lock_file "${DELTA_RS_SOURCE_DIR}")}"
DELTA_BENCH_SOURCE_CHECKOUT_LOCK_TIMEOUT_SECONDS="${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_TIMEOUT_SECONDS:-${DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS}}"
SOURCE_CHECKOUT_LOCK_FD=""
SOURCE_CHECKOUT_LOCK_DIR=""
export DELTA_BENCH_CHECKOUT_LOCK_FILE
export DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS
export DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE
export DELTA_BENCH_SOURCE_CHECKOUT_LOCK_TIMEOUT_SECONDS
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
	exec_on_runner git -C "${DELTA_RS_SOURCE_DIR}" show-ref --verify --quiet "refs/heads/${ref}" ||
		exec_on_runner git -C "${DELTA_RS_SOURCE_DIR}" show-ref --verify --quiet "refs/remotes/origin/${ref}"
}

print_ref_not_found_guidance() {
	local ref="${1:-}"
	echo "benchmark ref '${ref}' not found in delta-rs checkout '${DELTA_RS_SOURCE_DIR}' used for compare pinning." >&2
	echo 'use an existing branch (inspect with: git -C "${DELTA_RS_SOURCE_DIR}" branch -a), or pin SHAs with --base-sha/--candidate-sha.' >&2
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
	local fetch_url="${3:-}"
	if [[ "${mode}" == "commit" ]]; then
		run_step env DELTA_RS_REF="${ref}" DELTA_RS_REF_TYPE="commit" DELTA_RS_FETCH_URL="${fetch_url}" DELTA_RS_DIR="${DELTA_RS_SOURCE_DIR}" ./scripts/prepare_delta_rs.sh
		return
	fi
	if branch_ref_exists "${ref}"; then
		run_step env DELTA_RS_BRANCH="${ref}" DELTA_RS_DIR="${DELTA_RS_SOURCE_DIR}" ./scripts/prepare_delta_rs.sh
		return
	fi
	if is_commit_sha "${ref}"; then
		run_step env DELTA_RS_REF="${ref}" DELTA_RS_REF_TYPE="commit" DELTA_RS_FETCH_URL="${fetch_url}" DELTA_RS_DIR="${DELTA_RS_SOURCE_DIR}" ./scripts/prepare_delta_rs.sh
		return
	fi
	print_ref_not_found_guidance "${ref}"
	return 1
}

pin_ref_to_commit() {
	local ref="${1:-}"
	local mode="${2:-auto}"
	local fetch_url="${3:-}"
	local expected_commit_prefix=""
	if [[ "${mode}" == "commit" ]]; then
		expected_commit_prefix="$(printf '%s' "${ref}" | tr 'A-F' 'a-f')"
	elif ! branch_ref_exists "${ref}" && is_commit_sha "${ref}"; then
		expected_commit_prefix="$(printf '%s' "${ref}" | tr 'A-F' 'a-f')"
	fi

	if ! prepare_delta_rs_ref "${ref}" "${mode}" "${fetch_url}" >/dev/null; then
		if [[ -n "${expected_commit_prefix}" ]]; then
			echo "requested commit '${expected_commit_prefix}' was not pinned in '${DELTA_RS_SOURCE_DIR}'." >&2
		else
			echo "failed to prepare delta-rs ref '${ref}' in '${DELTA_RS_SOURCE_DIR}'." >&2
		fi
		return 1
	fi

	local resolved_ref
	resolved_ref="$(exec_on_runner git -C "${DELTA_RS_SOURCE_DIR}" rev-parse --verify HEAD)"
	local normalized_resolved_ref
	normalized_resolved_ref="$(printf '%s' "${resolved_ref}" | tr 'A-F' 'a-f')"
	if [[ -n "${expected_commit_prefix}" && "${normalized_resolved_ref#${expected_commit_prefix}}" == "${normalized_resolved_ref}" ]]; then
		echo "requested commit '${expected_commit_prefix}' was not pinned in '${DELTA_RS_SOURCE_DIR}'; checkout resolved to '${resolved_ref}'." >&2
		return 1
	fi
	printf '%s\n' "${resolved_ref}"
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
		if ((attempt >= BENCH_RETRY_ATTEMPTS)); then
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

run_local_compare_preflight() {
	if [[ -n "${REMOTE_RUNNER}" ]]; then
		return
	fi
	if ! is_positive_integer "${DELTA_BENCH_MIN_FREE_GB}"; then
		echo "invalid DELTA_BENCH_MIN_FREE_GB '${DELTA_BENCH_MIN_FREE_GB}'; expected positive integer" >&2
		exit 1
	fi

	local free_kb
	free_kb="$(df -Pk "${RUNNER_ROOT}" | awk 'NR==2 {print $4}')"
	if ! is_non_negative_integer "${free_kb}"; then
		echo "failed to determine local compare disk headroom under '${RUNNER_ROOT}'." >&2
		exit 1
	fi

	local free_gib
	free_gib="$((free_kb / 1024 / 1024))"
	echo "Local compare preflight: ${free_gib} GiB free under ${RUNNER_ROOT} (minimum ${DELTA_BENCH_MIN_FREE_GB} GiB)."
	if [[ -n "${CARGO_TARGET_DIR:-}" ]]; then
		echo "Local compare preflight: using CARGO_TARGET_DIR=${CARGO_TARGET_DIR}"
	else
		echo 'Local compare hint: if you hit per-checkout Cargo target instability, export CARGO_TARGET_DIR="$PWD/target".'
	fi

	if ((free_gib < DELTA_BENCH_MIN_FREE_GB)); then
		echo "local compare requires at least ${DELTA_BENCH_MIN_FREE_GB} GiB free under '${RUNNER_ROOT}'; found ${free_gib} GiB." >&2
		echo 'clear stale target/ trees or cached compare checkouts before retrying, and consider: export CARGO_TARGET_DIR="$PWD/target"' >&2
		exit 1
	fi
}

shell_join() {
	local joined=""
	local arg
	for arg in "$@"; do
		joined+=$(printf '%q ' "${arg}")
	done
	printf '%s' "${joined% }"
}

run_command_to_file() {
	local output_path="$1"
	shift
	local cmd=("$@")
	local command_string
	command_string="$(shell_join "${cmd[@]}")"
	run_step bash -c "set -euo pipefail; ${command_string} > $(printf '%q' "${output_path}")"
}

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
	if [[ -n "${REMOTE_RUNNER}" ]]; then
		return
	fi
	if [[ -d "${DELTA_RS_DIR}/.git" ]]; then
		return
	fi
	if path_is_within_dir "${DELTA_BENCH_CHECKOUT_LOCK_FILE}" "${DELTA_RS_DIR}"; then
		echo "DELTA_BENCH_CHECKOUT_LOCK_FILE must be outside DELTA_RS_DIR before initial clone: ${DELTA_BENCH_CHECKOUT_LOCK_FILE}" >&2
		exit 1
	fi
}

ensure_source_checkout_lock_path_safe_for_initial_clone() {
	if [[ -n "${REMOTE_RUNNER}" ]]; then
		return
	fi
	if [[ "${DELTA_RS_SOURCE_DIR}" == "${DELTA_RS_DIR}" || "${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE}" == "${DELTA_BENCH_CHECKOUT_LOCK_FILE}" ]]; then
		return
	fi
	if [[ -d "${DELTA_RS_SOURCE_DIR}/.git" ]]; then
		return
	fi
	if path_is_within_dir "${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE}" "${DELTA_RS_SOURCE_DIR}"; then
		echo "DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE must be outside DELTA_RS_SOURCE_DIR before initial clone: ${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE}" >&2
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

release_source_checkout_lock() {
	if [[ -n "${SOURCE_CHECKOUT_LOCK_FD}" ]]; then
		eval "exec ${SOURCE_CHECKOUT_LOCK_FD}>&-" >/dev/null 2>&1 || true
		SOURCE_CHECKOUT_LOCK_FD=""
	fi
	if [[ -n "${SOURCE_CHECKOUT_LOCK_DIR}" ]]; then
		rm -f "${SOURCE_CHECKOUT_LOCK_DIR}/pid" >/dev/null 2>&1 || true
		rmdir "${SOURCE_CHECKOUT_LOCK_DIR}" >/dev/null 2>&1 || true
		SOURCE_CHECKOUT_LOCK_DIR=""
	fi
}

acquire_checkout_lock() {
	if [[ -n "${REMOTE_RUNNER}" ]]; then
		return
	fi
	if [[ "${DELTA_BENCH_CHECKOUT_LOCK_HELD:-0}" == "1" ]]; then
		return
	fi

	ensure_checkout_lock_path_safe_for_initial_clone

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
			printf '%s\n' "$$" >"${CHECKOUT_LOCK_DIR}/pid" || true
			export DELTA_BENCH_CHECKOUT_LOCK_HELD=1
			return
		fi
		if ((SECONDS >= deadline)); then
			echo "failed to acquire checkout lock within ${DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS}s: ${DELTA_BENCH_CHECKOUT_LOCK_FILE}" >&2
			exit 1
		fi
		sleep 1
	done
}

acquire_source_checkout_lock() {
	if [[ -n "${REMOTE_RUNNER}" ]]; then
		return
	fi
	if [[ "${DELTA_RS_SOURCE_DIR}" == "${DELTA_RS_DIR}" || "${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE}" == "${DELTA_BENCH_CHECKOUT_LOCK_FILE}" ]]; then
		return
	fi
	if [[ "${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_HELD:-0}" == "1" ]]; then
		return
	fi

	ensure_source_checkout_lock_path_safe_for_initial_clone

	if command -v flock >/dev/null 2>&1; then
		mkdir -p "$(dirname "${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE}")"
		exec {SOURCE_CHECKOUT_LOCK_FD}>"${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE}"
		if ! flock -w "${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_TIMEOUT_SECONDS}" "${SOURCE_CHECKOUT_LOCK_FD}"; then
			echo "failed to acquire source checkout lock within ${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_TIMEOUT_SECONDS}s: ${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE}" >&2
			exit 1
		fi
		export DELTA_BENCH_SOURCE_CHECKOUT_LOCK_HELD=1
		return
	fi

	mkdir -p "$(dirname "${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE}")"
	local lock_dir="${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE}.dir"
	local deadline=$((SECONDS + DELTA_BENCH_SOURCE_CHECKOUT_LOCK_TIMEOUT_SECONDS))
	while true; do
		if mkdir "${lock_dir}" >/dev/null 2>&1; then
			SOURCE_CHECKOUT_LOCK_DIR="${lock_dir}"
			printf '%s\n' "$$" >"${SOURCE_CHECKOUT_LOCK_DIR}/pid" || true
			export DELTA_BENCH_SOURCE_CHECKOUT_LOCK_HELD=1
			return
		fi
		if ((SECONDS >= deadline)); then
			echo "failed to acquire source checkout lock within ${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_TIMEOUT_SECONDS}s: ${DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE}" >&2
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
	release_source_checkout_lock
	release_checkout_lock
}

trap cleanup_harness_overlay_untracked EXIT
acquire_source_checkout_lock

run_local_compare_preflight

ensure_source_checkout() {
	if exec_on_runner test -d "${DELTA_RS_SOURCE_DIR}/.git"; then
		return
	fi
	run_step env DELTA_RS_DIR="${DELTA_RS_SOURCE_DIR}" ./scripts/prepare_delta_rs.sh
}

ensure_source_checkout

if ((WORKING_VS_UPSTREAM_MAIN != 0)); then
	if ! exec_on_runner test -d "${DELTA_RS_DIR}/.git"; then
		run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/prepare_delta_rs.sh
	fi

	working_head_sha="$(exec_on_runner git -C "${DELTA_RS_DIR}" rev-parse --verify HEAD)"

	upstream_remote="${UPSTREAM_REMOTE_OVERRIDE:-}"
	if [[ -z "${upstream_remote}" ]]; then
		if exec_on_runner git -C "${DELTA_RS_SOURCE_DIR}" remote get-url upstream >/dev/null 2>&1; then
			upstream_remote="upstream"
		else
			upstream_remote="origin"
		fi
	fi

	if ! exec_on_runner git -C "${DELTA_RS_SOURCE_DIR}" remote get-url "${upstream_remote}" >/dev/null 2>&1; then
		echo "remote '${upstream_remote}' is not configured in delta-rs checkout '${DELTA_RS_SOURCE_DIR}'." >&2
		exit 1
	fi

	run_step git -C "${DELTA_RS_SOURCE_DIR}" fetch "${upstream_remote}" main
	upstream_main_sha="$(exec_on_runner git -C "${DELTA_RS_SOURCE_DIR}" rev-parse --verify "refs/remotes/${upstream_remote}/main^{commit}")"

	candidate_ref="${working_head_sha}"
	base_ref="${upstream_main_sha}"
	candidate_ref_mode="commit"
	base_ref_mode="commit"
	if [[ -z "${CANDIDATE_FETCH_URL}" ]]; then
		CANDIDATE_FETCH_URL="${DELTA_RS_DIR}"
	fi
fi

run_security_check() {
	local check_cmd=(./scripts/security_check.sh)
	if ((ENFORCE_RUN_MODE != 0)); then
		check_cmd+=(--enforce-run-mode)
	fi
	if ((REQUIRE_NO_PUBLIC_IPV4 != 0)); then
		check_cmd+=(--require-no-public-ipv4)
	fi
	if ((REQUIRE_EGRESS_POLICY != 0)); then
		check_cmd+=(--require-egress-policy)
	fi

	if [[ -n "${DELTA_BENCH_EGRESS_POLICY_SHA256:-}" ]]; then
		run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" DELTA_BENCH_EGRESS_POLICY_SHA256="${DELTA_BENCH_EGRESS_POLICY_SHA256}" "${check_cmd[@]}"
	else
		run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" "${check_cmd[@]}"
	fi
}

phase() {
	local step="$1"
	local total="$2"
	local desc="$3"
	printf '\n=== [%d/%d] %s ===\n\n' "${step}" "${total}" "${desc}"
}

checkout_dir_for_ref() {
	local ref="${1:-}"
	printf '%s/%s\n' "${compare_checkout_root}" "${ref}"
}

prepare_ref_checkout_once() {
	local ref="$1"
	local checkout_dir="$2"
	local fetch_url="${3:-}"
	local fetch_source="${fetch_url:-${DELTA_RS_SOURCE_DIR}}"

	run_step env DELTA_RS_DIR="${checkout_dir}" DELTA_RS_REF="${ref}" DELTA_RS_REF_TYPE="commit" DELTA_RS_FETCH_URL="${fetch_source}" ./scripts/prepare_delta_rs.sh
	run_step env DELTA_RS_DIR="${checkout_dir}" ./scripts/sync_harness_to_delta_rs.sh
}

run_benchmark_suite_for_checkout() {
	local checkout_dir="$1"
	local label="$2"
	local warmup="$3"
	local iters="$4"
	local no_summary_table="${5:-0}"

	local run_cmd=(./scripts/bench.sh run --scale sf1 --suite "${suite}" --runner "${RUNNER_MODE}" --lane macro --mode "${BENCHMARK_MODE}" --warmup "${warmup}" --iters "${iters}")
	if [[ -n "${DATASET_ID}" ]]; then
		run_cmd+=(--dataset-id "${DATASET_ID}")
	fi
	run_cmd+=(--timing-phase "${TIMING_PHASE}")
	run_cmd+=("${storage_args[@]}")
	if [[ ${#profile_args[@]} -gt 0 ]]; then
		run_cmd+=("${profile_args[@]}")
	fi
	if ((no_summary_table != 0)); then
		run_cmd+=(--no-summary-table)
	fi

	run_step_no_retry env DELTA_RS_DIR="${checkout_dir}" DELTA_BENCH_EXEC_ROOT="${checkout_dir}" DELTA_BENCH_RESULTS="${RUNNER_RESULTS_DIR}" DELTA_BENCH_LABEL="${label}" "${run_cmd[@]}"
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
		if ((idx % 2 == 1)); then
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
	if ((${#labels[@]} == 0)); then
		echo "internal error: aggregate_run_labels called without labels for ${out_label}" >&2
		exit 1
	fi

	local input_paths=()
	local label
	for label in "${labels[@]}"; do
		input_paths+=("${RUNNER_RESULTS_DIR}/${label}/${suite}.json")
	done

	local out_json="${RUNNER_RESULTS_DIR}/${out_label}/${suite}.json"
	run_step env PYTHONPATH="${RUNNER_ROOT}/python" python3 -m delta_bench_compare.aggregate --mode "${COMPARE_MODE}" --output "${out_json}" --label "${out_label}" "${input_paths[@]}"
}

# Calculate total phases for progress display
total_phases=$((1 + (BENCH_PREWARM_ITERS > 0 ? 1 : 0) + BENCH_COMPARE_RUNS + 1 + 1))
current_phase=1

phase "${current_phase}" "${total_phases}" "Preparing delta-rs checkout and fixtures"
current_phase=$((current_phase + 1))

run_security_check
if ! exec_on_runner test -d "${DELTA_RS_DIR}/.git"; then
	run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/prepare_delta_rs.sh
fi

ensure_known_ref_mode "${base_ref}" "${base_ref_mode}"
ensure_known_ref_mode "${candidate_ref}" "${candidate_ref_mode}"

base_requested_ref="${base_ref}"
candidate_requested_ref="${candidate_ref}"
base_ref="$(pin_ref_to_commit "${base_ref}" "${base_ref_mode}" "${BASE_FETCH_URL}")"
base_ref_mode="commit"
candidate_ref="$(pin_ref_to_commit "${candidate_ref}" "${candidate_ref_mode}" "${CANDIDATE_FETCH_URL}")"
candidate_ref_mode="commit"
echo "Pinned base ref: ${base_requested_ref} -> ${base_ref}"
echo "Pinned candidate ref: ${candidate_requested_ref} -> ${candidate_ref}"

base_label="base-$(sanitize_label "${base_ref}")"
cand_label="cand-$(sanitize_label "${candidate_ref}")"
base_checkout_dir="$(checkout_dir_for_ref "${base_ref}")"
candidate_checkout_dir="$(checkout_dir_for_ref "${candidate_ref}")"

prepare_ref_checkout_once "${base_ref}" "${base_checkout_dir}" "${BASE_FETCH_URL}"
prepare_ref_checkout_once "${candidate_ref}" "${candidate_checkout_dir}" "${CANDIDATE_FETCH_URL}"
data_cmd=(./scripts/bench.sh data --scale sf1 --seed 42)
if [[ -n "${DATASET_ID}" ]]; then
	data_cmd+=(--dataset-id "${DATASET_ID}")
fi
data_cmd+=("${storage_args[@]}")
if [[ ${#profile_args[@]} -gt 0 ]]; then
	data_cmd+=("${profile_args[@]}")
fi
run_step_no_retry env DELTA_RS_DIR="${base_checkout_dir}" DELTA_BENCH_EXEC_ROOT="${base_checkout_dir}" DELTA_BENCH_RESULTS="${RUNNER_RESULTS_DIR}" DELTA_BENCH_LABEL="${base_label}" "${data_cmd[@]}"

if ((BENCH_PREWARM_ITERS > 0)); then
	phase "${current_phase}" "${total_phases}" "Prewarm runs (${BENCH_PREWARM_ITERS} iterations, results discarded)"
	current_phase=$((current_phase + 1))
	run_benchmark_suite_for_checkout "${base_checkout_dir}" "${base_label}-prewarm" 0 "${BENCH_PREWARM_ITERS}" 1
	run_benchmark_suite_for_checkout "${candidate_checkout_dir}" "${cand_label}-prewarm" 0 "${BENCH_PREWARM_ITERS}" 1
fi

base_run_labels=()
cand_run_labels=()
run_idx=1
while ((run_idx <= BENCH_COMPARE_RUNS)); do
	phase "${current_phase}" "${total_phases}" "Measured run ${run_idx}/${BENCH_COMPARE_RUNS}"
	current_phase=$((current_phase + 1))
	order="$(run_order_for_iteration "${run_idx}")"
	for side in ${order}; do
		if [[ "${side}" == "base" ]]; then
			run_label="${base_label}-r${run_idx}"
			echo "  -> base (${base_ref:0:10}...)"
			run_benchmark_suite_for_checkout "${base_checkout_dir}" "${run_label}" "${BENCH_WARMUP}" "${BENCH_ITERS}" 1
			base_run_labels+=("${run_label}")
		else
			run_label="${cand_label}-r${run_idx}"
			echo "  -> candidate (${candidate_ref:0:10}...)"
			run_benchmark_suite_for_checkout "${candidate_checkout_dir}" "${run_label}" "${BENCH_WARMUP}" "${BENCH_ITERS}" 1
			cand_run_labels+=("${run_label}")
		fi
	done
	run_idx=$((run_idx + 1))
done

phase "${current_phase}" "${total_phases}" "Aggregating results"
current_phase=$((current_phase + 1))

aggregate_run_labels "${base_label}" "${base_run_labels[@]}"
aggregate_run_labels "${cand_label}" "${cand_run_labels[@]}"

base_json="${RUNNER_RESULTS_DIR}/${base_label}/${suite}.json"
cand_json="${RUNNER_RESULTS_DIR}/${cand_label}/${suite}.json"

phase "${current_phase}" "${total_phases}" "Comparison report"

compare_artifact_dir="${RUNNER_RESULTS_DIR}/compare/${suite}/${base_ref}__${candidate_ref}"
compare_stdout="${compare_artifact_dir}/stdout.txt"
compare_markdown="${compare_artifact_dir}/summary.md"
compare_json="${compare_artifact_dir}/comparison.json"
hash_policy_txt="${compare_artifact_dir}/hash-policy.txt"
manifest_json="${compare_artifact_dir}/manifest.json"

compare_args=(--mode "${COMPARE_MODE}" --noise-threshold "${NOISE_THRESHOLD}" --aggregation "${AGGREGATION}" --format text)
if [[ -n "${COMPARE_FAIL_ON}" ]]; then
	compare_args+=(--fail-on "${COMPARE_FAIL_ON}")
fi

compare_render_args=(--mode "${COMPARE_MODE}" --noise-threshold "${NOISE_THRESHOLD}" --aggregation "${AGGREGATION}")
compare_cmd=(env PYTHONPATH="${RUNNER_ROOT}/python" python3 -m delta_bench_compare.compare "${base_json}" "${cand_json}")
hash_policy_cmd=(env PYTHONPATH="${RUNNER_ROOT}/python" python3 -m delta_bench_compare.hash_policy "${base_json}" "${cand_json}")

run_step mkdir -p "${compare_artifact_dir}"
run_command_to_file "${compare_stdout}" "${compare_cmd[@]}" "${compare_render_args[@]}" --format text
run_command_to_file "${compare_markdown}" "${compare_cmd[@]}" "${compare_render_args[@]}" --format markdown
run_command_to_file "${compare_json}" "${compare_cmd[@]}" "${compare_render_args[@]}" --format json
run_command_to_file "${hash_policy_txt}" "${hash_policy_cmd[@]}"
manifest_cmd=(env PYTHONPATH="${RUNNER_ROOT}/python" python3 -m delta_bench_compare.manifest \
	--output "${manifest_json}" \
	--suite "${suite}" \
	--base-sha "${base_ref}" \
	--candidate-sha "${candidate_ref}" \
	--base-json "${base_json}" \
	--candidate-json "${cand_json}" \
	--stdout-report "${compare_stdout}" \
	--markdown-report "${compare_markdown}" \
	--comparison-json "${compare_json}" \
	--hash-policy-report "${hash_policy_txt}" \
	--compare-mode "${COMPARE_MODE}" \
	--aggregation "${AGGREGATION}" \
	--noise-threshold "${NOISE_THRESHOLD}" \
	--methodology-compare-mode "${COMPARE_MODE}" \
	--methodology-warmup "${BENCH_WARMUP}" \
	--methodology-iters "${BENCH_ITERS}" \
	--methodology-prewarm-iters "${BENCH_PREWARM_ITERS}" \
	--methodology-compare-runs "${BENCH_COMPARE_RUNS}" \
	--methodology-measure-order "${BENCH_MEASURE_ORDER}" \
	--methodology-timing-phase "${TIMING_PHASE}" \
	--methodology-aggregation "${AGGREGATION}")
if [[ -n "${METHODOLOGY_PROFILE}" ]]; then
	manifest_cmd+=(--methodology-profile "${METHODOLOGY_PROFILE}")
fi
if [[ -n "${METHODOLOGY_VERSION}" ]]; then
	manifest_cmd+=(--methodology-version "${METHODOLOGY_VERSION}")
fi
if [[ -n "${DATASET_POLICY}" ]]; then
	manifest_cmd+=(--methodology-dataset-policy "${DATASET_POLICY}")
fi
if [[ -n "${SPREAD_METRIC}" ]]; then
	manifest_cmd+=(--methodology-spread-metric "${SPREAD_METRIC}")
fi
if [[ -n "${SUB_MS_THRESHOLD_MS}" ]]; then
	manifest_cmd+=(--methodology-sub-ms-threshold-ms "${SUB_MS_THRESHOLD_MS}")
fi
if [[ -n "${SUB_MS_POLICY}" ]]; then
	manifest_cmd+=(--methodology-sub-ms-policy "${SUB_MS_POLICY}")
fi
run_step "${manifest_cmd[@]}"

run_step env PYTHONPATH="${RUNNER_ROOT}/python" python3 -m delta_bench_compare.compare "${base_json}" "${cand_json}" "${compare_args[@]}"
run_step env PYTHONPATH="${RUNNER_ROOT}/python" python3 -m delta_bench_compare.hash_policy "${base_json}" "${cand_json}"
