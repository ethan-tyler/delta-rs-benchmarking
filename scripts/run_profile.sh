#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
METHODOLOGY_DIR="${ROOT_DIR}/bench/methodologies"

dry_run=0
profile_name=""
profile_args=()
compare_only_args=()
criterion_bench=""
criterion_filter=""

usage() {
	cat <<EOF
Usage:
  ./scripts/run_profile.sh [options] <profile> [profile-args...]

Options:
  --dry-run                       Print the resolved command instead of running it
  --current-vs-main               Compare current HEAD against upstream main
  --base-sha <sha>                Force immutable commit mode for the base revision
  --candidate-sha <sha>           Force immutable commit mode for the candidate revision
  --base-fetch-url <url>          Fetch URL used when base SHA is only reachable from another remote
  --candidate-fetch-url <url>     Fetch URL used when candidate SHA is only reachable from another remote
  --enforce-run-mode              Require run-mode marker during compare preflight
  --require-no-public-ipv4        Require no public IPv4 during compare preflight
  --require-egress-policy         Require egress policy enforcement during compare preflight
  -h, --help                      Show this help

Common compare profiles:
  pr-macro
  pr-write-perf
  pr-tpcds
  scan-s3-candidate
  delete-update-perf-s3-candidate
  merge-perf-s3-candidate
  optimize-perf-s3-candidate
  metadata-perf-s3-candidate

Declared remote surfaces in bench/evidence/registry.yaml:
  scan_s3
  delete_update_perf_s3
  merge_perf_s3
  optimize_perf_s3
  metadata_perf_s3
  write_perf_s3 remains explicitly gated and is not in s3-candidate-manual

Common diagnostic Criterion profiles:
  scan-phase-criterion
  metadata-replay-criterion
EOF
}

shell_join() {
	local joined=""
	local arg
	for arg in "$@"; do
		joined+=$(printf '%q ' "${arg}")
	done
	printf '%s' "${joined% }"
}

append_flag_if_set() {
	local flag="${1:-}"
	local value="${2:-}"
	if [[ -n "${value}" ]]; then
		resolved_command+=("${flag}" "${value}")
	fi
}

require_profile_field() {
	local field_name="${1:-}"
	local field_value="${!field_name:-}"
	if [[ -z "${field_value}" ]]; then
		echo "methodology profile '${profile_name}' is missing ${field_name}" >&2
		exit 1
	fi
}

profile_args_include_flag() {
	local flag="${1:-}"
	if [[ ${#profile_args[@]} -eq 0 ]]; then
		return 1
	fi
	local arg
	for arg in "${profile_args[@]}"; do
		if [[ "${arg}" == "${flag}" ]]; then
			return 0
		fi
	done
	return 1
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--dry-run)
		dry_run=1
		shift
		;;
	--current-vs-main)
		compare_only_args+=(--current-vs-main)
		shift
		;;
	--base-sha | --candidate-sha | --base-fetch-url | --candidate-fetch-url)
		if [[ $# -lt 2 ]]; then
			echo "missing value for $1" >&2
			exit 1
		fi
		compare_only_args+=("$1" "$2")
		shift 2
		;;
	--enforce-run-mode | --require-no-public-ipv4 | --require-egress-policy)
		compare_only_args+=("$1")
		shift
		;;
	-h | --help)
		usage
		exit 0
		;;
	--)
		shift
		while [[ $# -gt 0 ]]; do
			profile_args+=("$1")
			shift
		done
		break
		;;
	-*)
		if [[ -z "${profile_name}" ]]; then
			echo "unknown option: $1" >&2
			usage >&2
			exit 1
		fi
		profile_args+=("$1")
		shift
		;;
	*)
		if [[ -z "${profile_name}" ]]; then
			profile_name="$1"
		else
			profile_args+=("$1")
		fi
		shift
		;;
	esac
done

if [[ -z "${profile_name}" ]]; then
	echo "missing methodology profile name" >&2
	usage >&2
	exit 1
fi

profile_path="${METHODOLOGY_DIR}/${profile_name}.env"
if [[ ! -f "${profile_path}" ]]; then
	echo "methodology profile '${profile_name}' not found at ${profile_path}" >&2
	exit 1
fi

source "${profile_path}"

if [[ -z "${METHODOLOGY_PROFILE:-}" ]]; then
	echo "methodology profile '${profile_name}' is missing METHODOLOGY_PROFILE" >&2
	exit 1
fi
if [[ "${METHODOLOGY_PROFILE}" != "${profile_name}" ]]; then
	echo "methodology profile '${profile_name}' declares METHODOLOGY_PROFILE='${METHODOLOGY_PROFILE}'" >&2
	exit 1
fi
if [[ -z "${METHODOLOGY_VERSION:-}" ]]; then
	echo "methodology profile '${profile_name}' is missing METHODOLOGY_VERSION" >&2
	exit 1
fi
if [[ -z "${PROFILE_KIND:-}" ]]; then
	echo "methodology profile '${profile_name}' is missing PROFILE_KIND" >&2
	exit 1
fi
if [[ -z "${TARGET:-}" ]]; then
	echo "methodology profile '${profile_name}' is missing TARGET" >&2
	exit 1
fi
case "${PROFILE_KIND}" in
run)
	require_profile_field RUNNER
	require_profile_field LANE
	require_profile_field MODE
	;;
criterion)
	require_profile_field CRITERION_BENCH
	criterion_bench="${CRITERION_BENCH}"
	criterion_filter="${CRITERION_FILTER:-}"
	;;
compare) ;;
*)
	echo "unsupported PROFILE_KIND='${PROFILE_KIND}' in profile '${profile_name}'" >&2
	exit 1
	;;
esac

resolved_command=()
case "${PROFILE_KIND}" in
compare)
	resolved_command=(./scripts/compare_branch.sh)
	if [[ ${#compare_only_args[@]} -gt 0 ]]; then
		resolved_command+=("${compare_only_args[@]}")
	fi
	if ! profile_args_include_flag --storage-backend; then
		append_flag_if_set --storage-backend "${STORAGE_BACKEND:-}"
	fi
	if ! profile_args_include_flag --backend-profile; then
		append_flag_if_set --backend-profile "${BACKEND_PROFILE:-}"
	fi
	resolved_command+=(--methodology-profile "${profile_name}")
	if [[ ${#profile_args[@]} -eq 0 ]]; then
		resolved_command+=("${TARGET}")
	else
		resolved_command+=("${profile_args[@]}")
	fi
	;;
run)
	if [[ ${#compare_only_args[@]} -gt 0 ]]; then
		echo "compare-only flags are only supported for compare profiles" >&2
		exit 1
	fi
	resolved_command=(./scripts/bench.sh run --suite "${TARGET}")
	append_flag_if_set --runner "${RUNNER:-}"
	append_flag_if_set --lane "${LANE:-}"
	append_flag_if_set --mode "${MODE:-}"
	append_flag_if_set --dataset-id "${DATASET_ID:-}"
	append_flag_if_set --timing-phase "${TIMING_PHASE:-}"
	append_flag_if_set --warmup "${WARMUP:-}"
	append_flag_if_set --iters "${ITERS:-}"
	append_flag_if_set --storage-backend "${STORAGE_BACKEND:-}"
	append_flag_if_set --backend-profile "${BACKEND_PROFILE:-}"
	if [[ ${#profile_args[@]} -gt 0 ]]; then
		resolved_command+=("${profile_args[@]}")
	fi
	;;
criterion)
	if [[ ${#compare_only_args[@]} -gt 0 ]]; then
		echo "compare-only flags are only supported for compare profiles" >&2
		exit 1
	fi
	resolved_command=(cargo bench -p delta-bench --bench "${criterion_bench}")
	if [[ -n "${criterion_filter}" || ${#profile_args[@]} -gt 0 ]]; then
		resolved_command+=(--)
		if [[ -n "${criterion_filter}" ]]; then
			resolved_command+=("${criterion_filter}")
		fi
		if [[ ${#profile_args[@]} -gt 0 ]]; then
			resolved_command+=("${profile_args[@]}")
		fi
	fi
	;;
esac

if ((dry_run != 0)); then
	printf '%s\n' "$(shell_join "${resolved_command[@]}")"
	exit 0
fi

exec "${resolved_command[@]}"
