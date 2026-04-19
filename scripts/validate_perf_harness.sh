#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DELTA_RS_DIR="${DELTA_RS_DIR:-${ROOT_DIR}/.delta-rs-under-test}"
PYTHONPATH_DIR="${ROOT_DIR}/python"

PRIMARY_VALIDATION_DATASET_ID="medium_selective"
METADATA_VALIDATION_DATASET_ID="many_versions"
TPCDS_VALIDATION_DATASET_ID="tpcds_duckdb"
VALIDATION_SHA="${VALIDATION_SHA:-}"
VALIDATION_FETCH_URL="${VALIDATION_FETCH_URL:-}"
VALIDATION_FETCH_REF="${VALIDATION_FETCH_REF:-}"
VALIDATION_DATASET_ID="${VALIDATION_DATASET_ID:-${PRIMARY_VALIDATION_DATASET_ID}}"
VALIDATION_CASE="${VALIDATION_CASE:-scan_filter_flag}"
VALIDATION_COMPARE_RUNS="${VALIDATION_COMPARE_RUNS:-5}"
VALIDATION_WARMUP="${VALIDATION_WARMUP:-1}"
VALIDATION_ITERS="${VALIDATION_ITERS:-5}"
VALIDATION_PREWARM_ITERS="${VALIDATION_PREWARM_ITERS:-1}"
VALIDATION_CANARY_ITERS="${VALIDATION_CANARY_ITERS:-3}"
VALIDATION_DML_CANARY_ITERS="${VALIDATION_DML_CANARY_ITERS:-1}"
VALIDATION_DELAY_MS="${VALIDATION_DELAY_MS:-150}"
VALIDATION_WRITE_PERF_DELAY_MS="${VALIDATION_WRITE_PERF_DELAY_MS:-${VALIDATION_DELAY_MS}}"
WRITE_PERF_CANARY_CASE="write_perf_unpartitioned_1m"
VALIDATION_DELETE_UPDATE_PERF_DELAY_MS="${VALIDATION_DELETE_UPDATE_PERF_DELAY_MS:-${VALIDATION_DELAY_MS}}"
DELETE_UPDATE_PERF_CANARY_CASE="delete_perf_scattered_5pct_small_files"
VALIDATION_MERGE_PERF_DELAY_MS="${VALIDATION_MERGE_PERF_DELAY_MS:-${VALIDATION_DELAY_MS}}"
MERGE_PERF_CANARY_CASE="merge_perf_upsert_50pct"
VALIDATION_OPTIMIZE_PERF_DELAY_MS="${VALIDATION_OPTIMIZE_PERF_DELAY_MS:-${VALIDATION_DELAY_MS}}"
OPTIMIZE_PERF_CANARY_CASE="optimize_perf_compact_small_files"
VALIDATION_METADATA_PERF_DELAY_MS="${VALIDATION_METADATA_PERF_DELAY_MS:-${VALIDATION_DELAY_MS}}"
METADATA_PERF_CANARY_CASE="metadata_perf_load_head_long_history"
VALIDATION_TPCDS_CASE="${VALIDATION_TPCDS_CASE:-tpcds_q03}"
VALIDATION_TPCDS_DELAY_MS="${VALIDATION_TPCDS_DELAY_MS:-${VALIDATION_DELAY_MS}}"

timestamp_utc="$(date -u +"%Y%m%dT%H%M%SZ")"
VALIDATION_ARTIFACT_DIR="${VALIDATION_ARTIFACT_DIR:-${ROOT_DIR}/results/validation/${timestamp_utc}}"

usage() {
	cat <<EOF
Usage:
  ./scripts/validate_perf_harness.sh [options]

Options:
  --sha <commit>          delta-rs commit to validate (default: current HEAD in DELTA_RS_DIR)
  --fetch-url <url>       Alternate fetch URL used when --sha is not reachable from origin
  --fetch-ref <ref>       Optional advertised branch/ref to fetch before resolving --sha
  --dataset-id <id>       Optional gate selector (default: ${VALIDATION_DATASET_ID}; use ${TPCDS_VALIDATION_DATASET_ID} to enable the dedicated TPC-DS gate)
  --artifact-dir <path>   Output directory for validation artifacts (default: ${VALIDATION_ARTIFACT_DIR}); stable gate names such as write-perf-ready, dml-maintenance-gate, metadata-perf-gate, and tpcds-gate run only their focused validator surfaces
  -h, --help              Show this help

Advanced tuning is available through environment variables:
  VALIDATION_COMPARE_RUNS, VALIDATION_WARMUP, VALIDATION_ITERS,
  VALIDATION_PREWARM_ITERS, VALIDATION_CANARY_ITERS, VALIDATION_DELAY_MS,
  VALIDATION_DML_CANARY_ITERS, VALIDATION_CASE, VALIDATION_WRITE_PERF_DELAY_MS,
  VALIDATION_DELETE_UPDATE_PERF_DELAY_MS, VALIDATION_MERGE_PERF_DELAY_MS,
  VALIDATION_OPTIMIZE_PERF_DELAY_MS, VALIDATION_METADATA_PERF_DELAY_MS,
  VALIDATION_TPCDS_CASE, VALIDATION_TPCDS_DELAY_MS, VALIDATION_FETCH_URL,
  VALIDATION_FETCH_REF, DELTA_RS_DIR
EOF
}

canonicalize_dir() {
	local dir="$1"
	mkdir -p "${dir}"
	(
		cd "${dir}"
		pwd -P
	)
}

resolve_validation_scope() {
	local artifact_dir="$1"
	local artifact_name
	artifact_name="$(basename "${artifact_dir}" | tr '[:upper:]' '[:lower:]')"

	case "${artifact_name}" in
	write-perf-ready | write-perf-gate)
		printf 'write_perf\n'
		;;
	dml-maintenance-gate)
		printf 'dml_maintenance\n'
		;;
	metadata-perf-gate)
		printf 'metadata_perf\n'
		;;
	tpcds-gate)
		printf 'tpcds\n'
		;;
	*)
		printf 'full\n'
		;;
	esac
}

planned_validation_gate_labels() {
	local scope="$1"
	local dataset_id="$2"
	local labels=("scan")

	case "${scope}" in
	full)
		labels+=("write_perf" "delete_update_perf" "merge_perf" "optimize_perf" "metadata_perf")
		if [[ "${dataset_id}" == "${TPCDS_VALIDATION_DATASET_ID}" ]]; then
			labels+=("tpcds")
		fi
		;;
	write_perf)
		labels+=("write_perf")
		;;
	dml_maintenance)
		labels+=("delete_update_perf" "merge_perf" "optimize_perf")
		;;
	metadata_perf)
		labels+=("metadata_perf")
		;;
	tpcds)
		if [[ "${dataset_id}" == "${TPCDS_VALIDATION_DATASET_ID}" ]]; then
			labels+=("tpcds")
		fi
		;;
	*)
		echo "unknown validation scope: ${scope}" >&2
		exit 1
		;;
	esac

	printf '%s\n' "${labels[*]}"
}

validation_plan_includes() {
	local planned_labels="$1"
	local suite="$2"
	case " ${planned_labels} " in
	*" ${suite} "*) return 0 ;;
	*) return 1 ;;
	esac
}

validation_same_sha_mode_for_suite() {
	local scope="$1"
	local suite="$2"

	case "${scope}:${suite}" in
	dml_maintenance:delete_update_perf | dml_maintenance:merge_perf | dml_maintenance:optimize_perf)
		printf 'canary_only\n'
		;;
	*)
		printf 'full_suite\n'
		;;
	esac
}

validation_canary_iterations_for_suite() {
	local scope="$1"
	local suite="$2"

	case "${scope}:${suite}" in
	dml_maintenance:delete_update_perf | dml_maintenance:merge_perf | dml_maintenance:optimize_perf)
		printf '%s\n' "${VALIDATION_DML_CANARY_ITERS}"
		;;
	*)
		printf '%s\n' "${VALIDATION_CANARY_ITERS}"
		;;
	esac
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--sha)
		VALIDATION_SHA="$2"
		shift 2
		;;
	--fetch-url)
		VALIDATION_FETCH_URL="$2"
		shift 2
		;;
	--fetch-ref)
		VALIDATION_FETCH_REF="$2"
		shift 2
		;;
	--dataset-id)
		VALIDATION_DATASET_ID="$2"
		shift 2
		;;
	--artifact-dir)
		VALIDATION_ARTIFACT_DIR="$2"
		shift 2
		;;
	-h | --help)
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

VALIDATION_ARTIFACT_DIR="$(canonicalize_dir "${VALIDATION_ARTIFACT_DIR}")"
RESULTS_DIR="${VALIDATION_ARTIFACT_DIR}/results"
PRIMARY_FIXTURES_DIR="${VALIDATION_ARTIFACT_DIR}/fixtures-medium_selective"
METADATA_FIXTURES_DIR="${VALIDATION_ARTIFACT_DIR}/fixtures-many_versions"
TPCDS_FIXTURES_DIR="${VALIDATION_ARTIFACT_DIR}/fixtures-tpcds_duckdb"
LOG_DIR="${VALIDATION_ARTIFACT_DIR}/logs"
SUMMARY_FILE="${VALIDATION_ARTIFACT_DIR}/summary.md"

mkdir -p "${RESULTS_DIR}" "${PRIMARY_FIXTURES_DIR}" "${METADATA_FIXTURES_DIR}" "${TPCDS_FIXTURES_DIR}" "${LOG_DIR}"

note() {
	printf -- '%s\n' "$*" | tee -a "${SUMMARY_FILE}"
}

json_path_for_label() {
	local label="$1"
	local suite="$2"
	printf '%s/%s/%s.json' "${RESULTS_DIR}" "${label}" "${suite}"
}

prepare_checkout_at_sha() {
	env DELTA_RS_REF="${VALIDATION_SHA}" DELTA_RS_REF_TYPE="commit" DELTA_RS_FETCH_URL="${VALIDATION_FETCH_URL}" DELTA_RS_FETCH_REF="${VALIDATION_FETCH_REF}" DELTA_RS_DIR="${DELTA_RS_DIR}" \
		"${SCRIPT_DIR}/prepare_delta_rs.sh" >/dev/null
	env DELTA_RS_DIR="${DELTA_RS_DIR}" "${SCRIPT_DIR}/sync_harness_to_delta_rs.sh" >/dev/null
}

generate_validation_fixtures() {
	env \
		DELTA_RS_DIR="${DELTA_RS_DIR}" \
		DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" \
		DELTA_BENCH_FIXTURES="${PRIMARY_FIXTURES_DIR}" \
		DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
		"${SCRIPT_DIR}/bench.sh" data \
		--dataset-id "${PRIMARY_VALIDATION_DATASET_ID}" \
		--seed 42 >/dev/null
}

generate_metadata_validation_fixtures() {
	env \
		DELTA_RS_DIR="${DELTA_RS_DIR}" \
		DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" \
		DELTA_BENCH_FIXTURES="${METADATA_FIXTURES_DIR}" \
		DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
		"${SCRIPT_DIR}/bench.sh" data \
		--dataset-id "${METADATA_VALIDATION_DATASET_ID}" \
		--seed 42 >/dev/null
}

run_scan_case() {
	local label="$1"
	local timing_phase="$2"
	shift 2

	env \
		DELTA_RS_DIR="${DELTA_RS_DIR}" \
		DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" \
		DELTA_BENCH_FIXTURES="${PRIMARY_FIXTURES_DIR}" \
		DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
		"$@" \
		"${SCRIPT_DIR}/bench.sh" run \
		--suite scan \
		--case-filter "${VALIDATION_CASE}" \
		--runner rust \
		--lane macro \
		--mode perf \
		--dataset-id "${PRIMARY_VALIDATION_DATASET_ID}" \
		--timing-phase "${timing_phase}" \
		--warmup 0 \
		--iters "${VALIDATION_CANARY_ITERS}" \
		--label "${label}" \
		--no-summary-table >/dev/null
}

run_write_perf_case() {
	local label="$1"
	shift

	env \
		DELTA_RS_DIR="${DELTA_RS_DIR}" \
		DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" \
		DELTA_BENCH_FIXTURES="${PRIMARY_FIXTURES_DIR}" \
		DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
		"$@" \
		"${SCRIPT_DIR}/bench.sh" run \
		--suite write_perf \
		--case-filter "${WRITE_PERF_CANARY_CASE}" \
		--runner rust \
		--lane macro \
		--mode perf \
		--warmup 0 \
		--iters "${VALIDATION_CANARY_ITERS}" \
		--label "${label}" \
		--no-summary-table >/dev/null
}

run_delete_update_perf_case() {
	local label="$1"
	shift

	env \
		DELTA_RS_DIR="${DELTA_RS_DIR}" \
		DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" \
		DELTA_BENCH_FIXTURES="${PRIMARY_FIXTURES_DIR}" \
		DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
		"$@" \
		"${SCRIPT_DIR}/bench.sh" run \
		--suite delete_update_perf \
		--case-filter "${DELETE_UPDATE_PERF_CANARY_CASE}" \
		--runner rust \
		--lane macro \
		--mode perf \
		--dataset-id "${PRIMARY_VALIDATION_DATASET_ID}" \
		--timing-phase execute \
		--warmup 0 \
		--iters "$(validation_canary_iterations_for_suite "${VALIDATION_SCOPE}" "delete_update_perf")" \
		--label "${label}" \
		--no-summary-table >/dev/null
}

run_merge_perf_case() {
	local label="$1"
	shift

	env \
		DELTA_RS_DIR="${DELTA_RS_DIR}" \
		DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" \
		DELTA_BENCH_FIXTURES="${PRIMARY_FIXTURES_DIR}" \
		DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
		"$@" \
		"${SCRIPT_DIR}/bench.sh" run \
		--suite merge_perf \
		--case-filter "${MERGE_PERF_CANARY_CASE}" \
		--runner rust \
		--lane macro \
		--mode perf \
		--dataset-id "${PRIMARY_VALIDATION_DATASET_ID}" \
		--timing-phase execute \
		--warmup 0 \
		--iters "$(validation_canary_iterations_for_suite "${VALIDATION_SCOPE}" "merge_perf")" \
		--label "${label}" \
		--no-summary-table >/dev/null
}

run_optimize_perf_case() {
	local label="$1"
	shift

	env \
		DELTA_RS_DIR="${DELTA_RS_DIR}" \
		DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" \
		DELTA_BENCH_FIXTURES="${PRIMARY_FIXTURES_DIR}" \
		DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
		"$@" \
		"${SCRIPT_DIR}/bench.sh" run \
		--suite optimize_perf \
		--case-filter "${OPTIMIZE_PERF_CANARY_CASE}" \
		--runner rust \
		--lane macro \
		--mode perf \
		--dataset-id "${PRIMARY_VALIDATION_DATASET_ID}" \
		--timing-phase execute \
		--warmup 0 \
		--iters "$(validation_canary_iterations_for_suite "${VALIDATION_SCOPE}" "optimize_perf")" \
		--label "${label}" \
		--no-summary-table >/dev/null
}

run_primary_suite_presence_probe() {
	local suite="$1"
	local label="$2"

	env \
		DELTA_RS_DIR="${DELTA_RS_DIR}" \
		DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" \
		DELTA_BENCH_FIXTURES="${PRIMARY_FIXTURES_DIR}" \
		DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
		"${SCRIPT_DIR}/bench.sh" run \
		--suite "${suite}" \
		--runner rust \
		--lane macro \
		--mode perf \
		--dataset-id "${PRIMARY_VALIDATION_DATASET_ID}" \
		--timing-phase execute \
		--warmup 0 \
		--iters 1 \
		--label "${label}" \
		--no-summary-table >/dev/null
}

run_metadata_perf_case() {
	local label="$1"
	shift

	env \
		DELTA_RS_DIR="${DELTA_RS_DIR}" \
		DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" \
		DELTA_BENCH_FIXTURES="${METADATA_FIXTURES_DIR}" \
		DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
		"$@" \
		"${SCRIPT_DIR}/bench.sh" run \
		--suite metadata_perf \
		--case-filter "${METADATA_PERF_CANARY_CASE}" \
		--runner rust \
		--lane macro \
		--mode perf \
		--dataset-id "${METADATA_VALIDATION_DATASET_ID}" \
		--timing-phase execute \
		--warmup 0 \
		--iters "${VALIDATION_CANARY_ITERS}" \
		--label "${label}" \
		--no-summary-table >/dev/null
}

run_tpcds_case() {
	local label="$1"
	shift

	env \
		DELTA_RS_DIR="${DELTA_RS_DIR}" \
		DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" \
		DELTA_BENCH_FIXTURES="${TPCDS_FIXTURES_DIR}" \
		DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
		"$@" \
		"${SCRIPT_DIR}/bench.sh" run \
		--suite tpcds \
		--case-filter "${VALIDATION_TPCDS_CASE}" \
		--runner rust \
		--lane macro \
		--mode perf \
		--dataset-id "${TPCDS_VALIDATION_DATASET_ID}" \
		--timing-phase execute \
		--warmup 0 \
		--iters "${VALIDATION_CANARY_ITERS}" \
		--label "${label}" \
		--no-summary-table >/dev/null
}

aggregate_suite_labels() {
	local suite="$1"
	local output_label="$2"
	shift 2
	local input_paths=()
	local label
	for label in "$@"; do
		input_paths+=("$(json_path_for_label "${label}" "${suite}")")
	done

	mkdir -p "${RESULTS_DIR}/${output_label}"
	env PYTHONPATH="${PYTHONPATH_DIR}" python3 -m delta_bench_compare.aggregate \
		--output "$(json_path_for_label "${output_label}" "${suite}")" \
		--label "${output_label}" \
		"${input_paths[@]}" >/dev/null
}

run_same_sha_canary_compare() {
	local suite="$1"
	local run_func="$2"
	local compare_runs="$3"
	local base_aggregate_label="$4"
	local cand_aggregate_label="$5"
	local base_run_labels=()
	local cand_run_labels=()
	local run_idx=1

	while ((run_idx <= compare_runs)); do
		local base_label="${base_aggregate_label}-r${run_idx}"
		local cand_label="${cand_aggregate_label}-r${run_idx}"
		"${run_func}" "${base_label}"
		"${run_func}" "${cand_label}"
		base_run_labels+=("${base_label}")
		cand_run_labels+=("${cand_label}")
		run_idx=$((run_idx + 1))
	done

	aggregate_suite_labels "${suite}" "${base_aggregate_label}" "${base_run_labels[@]}"
	aggregate_suite_labels "${suite}" "${cand_aggregate_label}" "${cand_run_labels[@]}"
}

run_delayed_canary_compare() {
	local suite="$1"
	local run_func="$2"
	local compare_runs="$3"
	local base_aggregate_label="$4"
	local cand_aggregate_label="$5"
	shift 5
	local delayed_env=("$@")
	local base_run_labels=()
	local cand_run_labels=()
	local run_idx=1

	while ((run_idx <= compare_runs)); do
		local base_label="${base_aggregate_label}-r${run_idx}"
		local cand_label="${cand_aggregate_label}-r${run_idx}"
		"${run_func}" "${base_label}"
		"${run_func}" "${cand_label}" "${delayed_env[@]}"
		base_run_labels+=("${base_label}")
		cand_run_labels+=("${cand_label}")
		run_idx=$((run_idx + 1))
	done

	aggregate_suite_labels "${suite}" "${base_aggregate_label}" "${base_run_labels[@]}"
	aggregate_suite_labels "${suite}" "${cand_aggregate_label}" "${cand_run_labels[@]}"
}

assert_same_sha_compare_is_fail_closed() {
	local baseline_json="$1"
	local candidate_json="$2"
	env PYTHONPATH="${PYTHONPATH_DIR}" python3 - "${baseline_json}" "${candidate_json}" <<'PY'
import sys
from pathlib import Path

from delta_bench_compare.compare import compare_runs
from delta_bench_compare.schema import load_benchmark_payload

baseline = load_benchmark_payload(Path(sys.argv[1]))
candidate = load_benchmark_payload(Path(sys.argv[2]))
comparison = compare_runs(baseline, candidate, mode="decision")
allowed = {"no_change", "inconclusive"}
bad = [(row.case, row.status) for row in comparison.rows if row.status not in allowed]
if bad:
    raise SystemExit(
        "same-SHA decision compare produced unexpected statuses: "
        + ", ".join(f"{case}={change}" for case, change in bad)
    )
counts = {}
for row in comparison.rows:
    counts[row.status] = counts.get(row.status, 0) + 1
print(
    "- Same-SHA decision compare statuses: "
    + ", ".join(f"{status}={counts[status]}" for status in sorted(counts))
)
PY
}

assert_payload_contains_cases() {
	local payload_json="$1"
	shift
	env PYTHONPATH="${PYTHONPATH_DIR}" python3 - "${payload_json}" "$@" <<'PY'
import sys
from pathlib import Path

from delta_bench_compare.schema import load_benchmark_payload

payload = load_benchmark_payload(Path(sys.argv[1]))
expected = list(sys.argv[2:])
present = {case["case"] for case in payload.get("cases", [])}
missing = [case for case in expected if case not in present]
if missing:
    raise SystemExit("missing expected cases: " + ", ".join(missing))
print("- Cases present: " + ", ".join(expected))
PY
}

assert_phase_canary() {
	local selected_baseline="$1"
	local selected_delayed="$2"
	local control_baseline="$3"
	local control_delayed="$4"
	local selected_phase="$5"
	local control_phase="$6"

	env PYTHONPATH="${PYTHONPATH_DIR}" python3 - \
		"${selected_baseline}" \
		"${selected_delayed}" \
		"${control_baseline}" \
		"${control_delayed}" \
		"${VALIDATION_DELAY_MS}" \
		"${selected_phase}" \
		"${control_phase}" \
		"${VALIDATION_CASE}" <<'PY'
import json
import sys
from pathlib import Path

selected_baseline = Path(sys.argv[1])
selected_delayed = Path(sys.argv[2])
control_baseline = Path(sys.argv[3])
control_delayed = Path(sys.argv[4])
delay_ms = float(sys.argv[5])
selected_phase = sys.argv[6]
control_phase = sys.argv[7]
case_name = sys.argv[8]


def median_ms(path: Path) -> float:
    payload = json.loads(path.read_text(encoding="utf-8"))
    for case in payload.get("cases", []):
        if case.get("case") == case_name:
            stats = case.get("elapsed_stats") or {}
            return float(stats["median_ms"])
    raise SystemExit(f"missing case '{case_name}' in {path}")


selected_delta = median_ms(selected_delayed) - median_ms(selected_baseline)
control_delta = median_ms(control_delayed) - median_ms(control_baseline)
if selected_delta < delay_ms - 25.0:
    raise SystemExit(
        f"{selected_phase} canary did not move enough: delta={selected_delta:.3f} ms expected>={delay_ms - 25.0:.3f}"
    )
if abs(control_delta) > 45.0:
    raise SystemExit(
        f"{selected_phase} canary leaked into {control_phase}: delta={control_delta:.3f} ms expected<=45.0"
    )
print(
    f"- {selected_phase} canary: selected delta={selected_delta:.3f} ms, "
    f"{control_phase} control delta={control_delta:.3f} ms"
)
PY
}

compute_regression_canary_delay_ms() {
	local baseline_json="$1"
	local case_name="$2"
	local configured_delay_ms="$3"
	env PYTHONPATH="${PYTHONPATH_DIR}" python3 - \
		"${baseline_json}" \
		"${case_name}" \
		"${configured_delay_ms}" <<'PY'
import statistics
import sys
import math
from pathlib import Path

from delta_bench_compare.schema import load_benchmark_payload

baseline = load_benchmark_payload(Path(sys.argv[1]))
case_name = sys.argv[2]
configured_delay_ms = float(sys.argv[3])

for case in baseline.get("cases", []):
    if case.get("case") != case_name:
        continue

    run_summaries = [
        summary
        for summary in (case.get("run_summaries") or [])
        if isinstance(summary, dict) and summary.get("median_ms") is not None
    ]
    if run_summaries:
        baseline_medians = [float(summary["median_ms"]) for summary in run_summaries]
        baseline_metric_ms = statistics.median(baseline_medians)
    else:
        run_summary = case.get("run_summary") or {}
        median_ms = run_summary.get("median_ms")
        if median_ms is None:
            elapsed_stats = case.get("elapsed_stats") or {}
            median_ms = elapsed_stats.get("median_ms")
        if median_ms is None:
            raise SystemExit(
                f"missing median timing for case '{case_name}' in {sys.argv[1]}"
            )
        baseline_metric_ms = float(median_ms)

    threshold_pct = float(case.get("decision_threshold_pct") or 5.0)
    relative_target = max(0.10, (threshold_pct / 100.0) * 2.0)
    effective_delay_ms = max(configured_delay_ms, baseline_metric_ms * relative_target)
    print(str(int(math.ceil(effective_delay_ms))))
    raise SystemExit(0)

raise SystemExit(f"missing case '{case_name}' in regression canary baseline payload")
PY
}

compute_regression_canary_compare_runs() {
	local baseline_json="$1"
	local case_name="$2"
	local configured_compare_runs="$3"
	env PYTHONPATH="${PYTHONPATH_DIR}" python3 - \
		"${baseline_json}" \
		"${case_name}" \
		"${configured_compare_runs}" <<'PY'
import sys
from pathlib import Path

from delta_bench_compare.schema import load_benchmark_payload

baseline = load_benchmark_payload(Path(sys.argv[1]))
case_name = sys.argv[2]
configured_compare_runs = int(sys.argv[3])

if configured_compare_runs < 1:
    raise SystemExit("configured compare runs must be >= 1")

for case in baseline.get("cases", []):
    if case.get("case") != case_name:
        continue
    required_runs = int(case.get("required_runs") or configured_compare_runs)
    print(str(max(configured_compare_runs, required_runs)))
    raise SystemExit(0)

raise SystemExit(f"missing case '{case_name}' in regression canary baseline payload")
PY
}

assert_regression_canary_detected() {
	local baseline_json="$1"
	local candidate_json="$2"
	local case_name="$3"
	env PYTHONPATH="${PYTHONPATH_DIR}" python3 - \
		"${baseline_json}" \
		"${candidate_json}" \
		"${case_name}" <<'PY'
import sys
from pathlib import Path

from delta_bench_compare.compare import compare_runs
from delta_bench_compare.schema import load_benchmark_payload

baseline = load_benchmark_payload(Path(sys.argv[1]))
candidate = load_benchmark_payload(Path(sys.argv[2]))
case_name = sys.argv[3]
comparison = compare_runs(baseline, candidate, mode="decision")
for row in comparison.rows:
    if row.case != case_name:
        continue
    if row.change != "regression":
        raise SystemExit(
            f"expected deliberate execute delay to classify as regression for {case_name}, got {row.change}"
        )
    print(
        f"- Regression canary: {case_name} classified as regression "
        f"({row.baseline_ms:.3f} ms -> {row.candidate_ms:.3f} ms)"
    )
    raise SystemExit(0)
raise SystemExit(f"missing case '{case_name}' in regression canary comparison")
PY
}

tpcds_validation_enabled() {
	[[ "${VALIDATION_DATASET_ID}" == "${TPCDS_VALIDATION_DATASET_ID}" ]]
}

if [[ ! -d "${DELTA_RS_DIR}/.git" ]]; then
	env DELTA_RS_DIR="${DELTA_RS_DIR}" "${SCRIPT_DIR}/prepare_delta_rs.sh" >/dev/null
fi

if [[ -z "${VALIDATION_SHA}" ]]; then
	VALIDATION_SHA="$(git -C "${DELTA_RS_DIR}" rev-parse HEAD)"
fi

VALIDATION_SCOPE="$(resolve_validation_scope "${VALIDATION_ARTIFACT_DIR}")"
VALIDATION_GATE_LABELS="$(planned_validation_gate_labels "${VALIDATION_SCOPE}" "${VALIDATION_DATASET_ID}")"

prepare_checkout_at_sha
VALIDATION_SHA="$(git -C "${DELTA_RS_DIR}" rev-parse --verify HEAD)"
same_sha_compare_args=(
	--base-sha "${VALIDATION_SHA}"
	--candidate-sha "${VALIDATION_SHA}"
	--base-fetch-url "${DELTA_RS_DIR}"
	--candidate-fetch-url "${DELTA_RS_DIR}"
)
generate_validation_fixtures
generate_metadata_validation_fixtures

cat >"${SUMMARY_FILE}" <<EOF
# Perf Harness Validation Summary

- Date (UTC): ${timestamp_utc}
- delta-rs SHA: ${VALIDATION_SHA}
- Primary validation dataset: ${PRIMARY_VALIDATION_DATASET_ID}
- Metadata validation dataset: ${METADATA_VALIDATION_DATASET_ID}
- Optional gate selector: ${VALIDATION_DATASET_ID}
- Validation scope: ${VALIDATION_SCOPE}
- Planned gate suites: ${VALIDATION_GATE_LABELS}
- TPC-DS validation dataset: ${TPCDS_VALIDATION_DATASET_ID}
- TPC-DS gate enabled: $(if tpcds_validation_enabled; then echo yes; else echo no; fi)
- Scan case canary: ${VALIDATION_CASE}
- Write perf canary case: ${WRITE_PERF_CANARY_CASE}
- Delete/update perf canary case: ${DELETE_UPDATE_PERF_CANARY_CASE}
- Merge perf canary case: ${MERGE_PERF_CANARY_CASE}
- Optimize perf canary case: ${OPTIMIZE_PERF_CANARY_CASE}
- Metadata perf canary case: ${METADATA_PERF_CANARY_CASE}
- TPC-DS canary case: ${VALIDATION_TPCDS_CASE}
- Compare runs per side: ${VALIDATION_COMPARE_RUNS}
- Iterations per run: ${VALIDATION_ITERS}
- Canary delay: ${VALIDATION_DELAY_MS} ms
- Write perf canary delay: ${VALIDATION_WRITE_PERF_DELAY_MS} ms
- Delete/update perf canary delay: ${VALIDATION_DELETE_UPDATE_PERF_DELAY_MS} ms
- Merge perf canary delay: ${VALIDATION_MERGE_PERF_DELAY_MS} ms
- Optimize perf canary delay: ${VALIDATION_OPTIMIZE_PERF_DELAY_MS} ms
- Metadata perf canary delay: ${VALIDATION_METADATA_PERF_DELAY_MS} ms
- TPC-DS canary delay: ${VALIDATION_TPCDS_DELAY_MS} ms
- Artifacts: ${VALIDATION_ARTIFACT_DIR}

## Checks
EOF

note ""
note "Running same-SHA same-path branch compare..."
same_sha_log="${LOG_DIR}/same_sha_compare.log"
env \
	DELTA_BENCH_FIXTURES="${PRIMARY_FIXTURES_DIR}" \
	DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
	"${SCRIPT_DIR}/compare_branch.sh" \
	"${same_sha_compare_args[@]}" \
	--compare-mode decision \
	--warmup "${VALIDATION_WARMUP}" \
	--iters "${VALIDATION_ITERS}" \
	--prewarm-iters "${VALIDATION_PREWARM_ITERS}" \
	--compare-runs "${VALIDATION_COMPARE_RUNS}" \
	--dataset-id "${PRIMARY_VALIDATION_DATASET_ID}" \
	scan | tee "${same_sha_log}"

base_label="base-${VALIDATION_SHA}"
cand_label="cand-${VALIDATION_SHA}"
same_sha_status="$(assert_same_sha_compare_is_fail_closed "$(json_path_for_label "${base_label}" "scan")" "$(json_path_for_label "${cand_label}" "scan")")"
note "${same_sha_status}"

note ""
note "Running phase-isolation canaries..."

run_scan_case "canary-load-baseline" "load"
run_scan_case "canary-load-control-baseline" "execute"
run_scan_case "canary-load-delayed" "load" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_LOAD_MS=${VALIDATION_DELAY_MS}"
run_scan_case "canary-load-control-delayed" "execute" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_LOAD_MS=${VALIDATION_DELAY_MS}"
load_canary_status="$(assert_phase_canary \
	"$(json_path_for_label "canary-load-baseline" "scan")" \
	"$(json_path_for_label "canary-load-delayed" "scan")" \
	"$(json_path_for_label "canary-load-control-baseline" "scan")" \
	"$(json_path_for_label "canary-load-control-delayed" "scan")" \
	"load" \
	"execute")"
note "${load_canary_status}"

run_scan_case "canary-plan-baseline" "plan"
run_scan_case "canary-plan-control-baseline" "execute"
run_scan_case "canary-plan-delayed" "plan" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_PLAN_MS=${VALIDATION_DELAY_MS}"
run_scan_case "canary-plan-control-delayed" "execute" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_PLAN_MS=${VALIDATION_DELAY_MS}"
plan_canary_status="$(assert_phase_canary \
	"$(json_path_for_label "canary-plan-baseline" "scan")" \
	"$(json_path_for_label "canary-plan-delayed" "scan")" \
	"$(json_path_for_label "canary-plan-control-baseline" "scan")" \
	"$(json_path_for_label "canary-plan-control-delayed" "scan")" \
	"plan" \
	"execute")"
note "${plan_canary_status}"

run_scan_case "canary-validate-baseline" "validate"
run_scan_case "canary-validate-control-baseline" "execute"
run_scan_case "canary-validate-delayed" "validate" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_VALIDATE_MS=${VALIDATION_DELAY_MS}"
run_scan_case "canary-validate-control-delayed" "execute" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_VALIDATE_MS=${VALIDATION_DELAY_MS}"
validate_canary_status="$(assert_phase_canary \
	"$(json_path_for_label "canary-validate-baseline" "scan")" \
	"$(json_path_for_label "canary-validate-delayed" "scan")" \
	"$(json_path_for_label "canary-validate-control-baseline" "scan")" \
	"$(json_path_for_label "canary-validate-control-delayed" "scan")" \
	"validate" \
	"execute")"
note "${validate_canary_status}"

run_scan_case "canary-execute-baseline" "execute"
run_scan_case "canary-execute-control-baseline" "plan"
run_scan_case "canary-execute-delayed" "execute" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_EXECUTE_MS=${VALIDATION_DELAY_MS}"
run_scan_case "canary-execute-control-delayed" "plan" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_EXECUTE_MS=${VALIDATION_DELAY_MS}"
execute_canary_status="$(assert_phase_canary \
	"$(json_path_for_label "canary-execute-baseline" "scan")" \
	"$(json_path_for_label "canary-execute-delayed" "scan")" \
	"$(json_path_for_label "canary-execute-control-baseline" "scan")" \
	"$(json_path_for_label "canary-execute-control-delayed" "scan")" \
	"execute" \
	"plan")"
note "${execute_canary_status}"

note ""
note "Running regression-detection canary..."
scan_regression_delay_ms="$(compute_regression_canary_delay_ms \
	"$(json_path_for_label "base-${VALIDATION_SHA}" "scan")" \
	"${VALIDATION_CASE}" \
	"${VALIDATION_DELAY_MS}")"
scan_regression_compare_runs="$(compute_regression_canary_compare_runs \
	"$(json_path_for_label "base-${VALIDATION_SHA}" "scan")" \
	"${VALIDATION_CASE}" \
	"${VALIDATION_COMPARE_RUNS}")"
note "- Scan regression canary delay: ${scan_regression_delay_ms} ms"
note "- Scan regression canary compare runs: ${scan_regression_compare_runs}"

base_run_labels=()
cand_run_labels=()
run_idx=1
while ((run_idx <= scan_regression_compare_runs)); do
	base_label="regression-base-r${run_idx}"
	cand_label="regression-cand-r${run_idx}"
	run_scan_case "${base_label}" "execute"
	run_scan_case "${cand_label}" "execute" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_EXECUTE_MS=${scan_regression_delay_ms}"
	base_run_labels+=("${base_label}")
	cand_run_labels+=("${cand_label}")
	run_idx=$((run_idx + 1))
done

aggregate_suite_labels "scan" "regression-base" "${base_run_labels[@]}"
aggregate_suite_labels "scan" "regression-cand" "${cand_run_labels[@]}"
regression_canary_status="$(assert_regression_canary_detected \
	"$(json_path_for_label "regression-base" "scan")" \
	"$(json_path_for_label "regression-cand" "scan")" \
	"${VALIDATION_CASE}")"
note "${regression_canary_status}"

if validation_plan_includes "${VALIDATION_GATE_LABELS}" "write_perf"; then
	note ""
	note "Running write_perf same-SHA branch compare..."
	write_perf_same_sha_log="${LOG_DIR}/write_perf_same_sha_compare.log"
	write_perf_base_label="base-${VALIDATION_SHA}"
	write_perf_cand_label="cand-${VALIDATION_SHA}"
	env \
		DELTA_BENCH_FIXTURES="${PRIMARY_FIXTURES_DIR}" \
		DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
		"${SCRIPT_DIR}/compare_branch.sh" \
		"${same_sha_compare_args[@]}" \
		--methodology-profile pr-write-perf \
		write_perf | tee "${write_perf_same_sha_log}"

	write_perf_same_sha_status="$(assert_same_sha_compare_is_fail_closed "$(json_path_for_label "${write_perf_base_label}" "write_perf")" "$(json_path_for_label "${write_perf_cand_label}" "write_perf")")"
	note "${write_perf_same_sha_status}"
	write_perf_case_presence_status="$(assert_payload_contains_cases \
		"$(json_path_for_label "${write_perf_base_label}" "write_perf")" \
		"write_perf_partitioned_1m_parts_010" \
		"write_perf_partitioned_1m_parts_100" \
		"write_perf_partitioned_5m_parts_010" \
		"${WRITE_PERF_CANARY_CASE}")"
	note "${write_perf_case_presence_status}"

	note ""
	note "Running write_perf regression-detection canary..."
	write_perf_regression_delay_ms="$(compute_regression_canary_delay_ms \
		"$(json_path_for_label "${write_perf_base_label}" "write_perf")" \
		"${WRITE_PERF_CANARY_CASE}" \
		"${VALIDATION_WRITE_PERF_DELAY_MS}")"
	write_perf_regression_compare_runs="$(compute_regression_canary_compare_runs \
		"$(json_path_for_label "${write_perf_base_label}" "write_perf")" \
		"${WRITE_PERF_CANARY_CASE}" \
		"${VALIDATION_COMPARE_RUNS}")"
	note "- Write perf regression canary delay: ${write_perf_regression_delay_ms} ms"
	note "- Write perf regression canary compare runs: ${write_perf_regression_compare_runs}"

	write_perf_base_run_labels=()
	write_perf_cand_run_labels=()
	run_idx=1
	while ((run_idx <= write_perf_regression_compare_runs)); do
		base_label="write-perf-regression-base-r${run_idx}"
		cand_label="write-perf-regression-cand-r${run_idx}"
		run_write_perf_case "${base_label}"
		run_write_perf_case "${cand_label}" \
			"DELTA_BENCH_ALLOW_WRITE_PERF_DELAY=1" \
			"DELTA_BENCH_WRITE_PERF_DELAY_MS=${write_perf_regression_delay_ms}"
		write_perf_base_run_labels+=("${base_label}")
		write_perf_cand_run_labels+=("${cand_label}")
		run_idx=$((run_idx + 1))
	done

	aggregate_suite_labels "write_perf" "write-perf-regression-base" "${write_perf_base_run_labels[@]}"
	aggregate_suite_labels "write_perf" "write-perf-regression-cand" "${write_perf_cand_run_labels[@]}"
	write_perf_regression_status="$(assert_regression_canary_detected \
		"$(json_path_for_label "write-perf-regression-base" "write_perf")" \
		"$(json_path_for_label "write-perf-regression-cand" "write_perf")" \
		"${WRITE_PERF_CANARY_CASE}")"
	note "${write_perf_regression_status}"
else
	note ""
	note "Skipping write_perf gate because validation scope is '${VALIDATION_SCOPE}'."
fi

if validation_plan_includes "${VALIDATION_GATE_LABELS}" "delete_update_perf"; then
	note ""
	delete_update_perf_same_sha_mode="$(validation_same_sha_mode_for_suite "${VALIDATION_SCOPE}" "delete_update_perf")"
	delete_update_perf_regression_baseline_json=""
	if [[ "${delete_update_perf_same_sha_mode}" == "canary_only" ]]; then
		delete_update_perf_presence_label="delete-update-perf-presence"
		note "Running delete_update_perf suite coverage probe..."
		run_primary_suite_presence_probe "delete_update_perf" "${delete_update_perf_presence_label}"
		delete_update_perf_case_presence_status="$(assert_payload_contains_cases \
			"$(json_path_for_label "${delete_update_perf_presence_label}" "delete_update_perf")" \
			"delete_perf_localized_1pct" \
			"${DELETE_UPDATE_PERF_CANARY_CASE}" \
			"update_perf_literal_5pct_scattered" \
			"update_perf_all_rows_expr")"
		note "${delete_update_perf_case_presence_status}"

		delete_update_perf_same_sha_compare_runs="$(compute_regression_canary_compare_runs \
			"$(json_path_for_label "${delete_update_perf_presence_label}" "delete_update_perf")" \
			"${DELETE_UPDATE_PERF_CANARY_CASE}" \
			"${VALIDATION_COMPARE_RUNS}")"
		note ""
		note "Running delete_update_perf same-SHA canary compare..."
		note "- Delete/update perf same-SHA canary compare runs: ${delete_update_perf_same_sha_compare_runs}"
		run_same_sha_canary_compare \
			"delete_update_perf" \
			"run_delete_update_perf_case" \
			"${delete_update_perf_same_sha_compare_runs}" \
			"delete-update-perf-same-sha-base" \
			"delete-update-perf-same-sha-cand"
		delete_update_perf_same_sha_status="$(assert_same_sha_compare_is_fail_closed \
			"$(json_path_for_label "delete-update-perf-same-sha-base" "delete_update_perf")" \
			"$(json_path_for_label "delete-update-perf-same-sha-cand" "delete_update_perf")")"
		note "${delete_update_perf_same_sha_status}"
		delete_update_perf_regression_baseline_json="$(json_path_for_label "delete-update-perf-same-sha-base" "delete_update_perf")"
	else
		note "Running delete_update_perf same-SHA branch compare..."
		delete_update_perf_same_sha_log="${LOG_DIR}/delete_update_perf_same_sha_compare.log"
		delete_update_perf_base_label="base-${VALIDATION_SHA}"
		delete_update_perf_cand_label="cand-${VALIDATION_SHA}"
		env \
			DELTA_BENCH_FIXTURES="${PRIMARY_FIXTURES_DIR}" \
			DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
			"${SCRIPT_DIR}/compare_branch.sh" \
			"${same_sha_compare_args[@]}" \
			--methodology-profile pr-delete-update-perf \
			delete_update_perf | tee "${delete_update_perf_same_sha_log}"

		delete_update_perf_same_sha_status="$(assert_same_sha_compare_is_fail_closed "$(json_path_for_label "${delete_update_perf_base_label}" "delete_update_perf")" "$(json_path_for_label "${delete_update_perf_cand_label}" "delete_update_perf")")"
		note "${delete_update_perf_same_sha_status}"
		delete_update_perf_case_presence_status="$(assert_payload_contains_cases \
			"$(json_path_for_label "${delete_update_perf_base_label}" "delete_update_perf")" \
			"delete_perf_localized_1pct" \
			"${DELETE_UPDATE_PERF_CANARY_CASE}" \
			"update_perf_literal_5pct_scattered" \
			"update_perf_all_rows_expr")"
		note "${delete_update_perf_case_presence_status}"
		delete_update_perf_regression_baseline_json="$(json_path_for_label "${delete_update_perf_base_label}" "delete_update_perf")"
	fi

	note ""
	note "Running delete_update_perf regression-detection canary..."
	delete_update_perf_regression_delay_ms="$(compute_regression_canary_delay_ms \
		"${delete_update_perf_regression_baseline_json}" \
		"${DELETE_UPDATE_PERF_CANARY_CASE}" \
		"${VALIDATION_DELETE_UPDATE_PERF_DELAY_MS}")"
	delete_update_perf_regression_compare_runs="$(compute_regression_canary_compare_runs \
		"${delete_update_perf_regression_baseline_json}" \
		"${DELETE_UPDATE_PERF_CANARY_CASE}" \
		"${VALIDATION_COMPARE_RUNS}")"
	note "- Delete/update perf regression canary delay: ${delete_update_perf_regression_delay_ms} ms"
	note "- Delete/update perf regression canary compare runs: ${delete_update_perf_regression_compare_runs}"

	run_delayed_canary_compare \
		"delete_update_perf" \
		"run_delete_update_perf_case" \
		"${delete_update_perf_regression_compare_runs}" \
		"delete-update-perf-regression-base" \
		"delete-update-perf-regression-cand" \
		"DELTA_BENCH_ALLOW_DELETE_UPDATE_PERF_DELAY=1" \
		"DELTA_BENCH_DELETE_UPDATE_PERF_DELAY_MS=${delete_update_perf_regression_delay_ms}"
	delete_update_perf_regression_status="$(assert_regression_canary_detected \
		"$(json_path_for_label "delete-update-perf-regression-base" "delete_update_perf")" \
		"$(json_path_for_label "delete-update-perf-regression-cand" "delete_update_perf")" \
		"${DELETE_UPDATE_PERF_CANARY_CASE}")"
	note "${delete_update_perf_regression_status}"

	note ""
	merge_perf_same_sha_mode="$(validation_same_sha_mode_for_suite "${VALIDATION_SCOPE}" "merge_perf")"
	merge_perf_regression_baseline_json=""
	if [[ "${merge_perf_same_sha_mode}" == "canary_only" ]]; then
		merge_perf_presence_label="merge-perf-presence"
		note "Running merge_perf suite coverage probe..."
		run_primary_suite_presence_probe "merge_perf" "${merge_perf_presence_label}"
		merge_perf_case_presence_status="$(assert_payload_contains_cases \
			"$(json_path_for_label "${merge_perf_presence_label}" "merge_perf")" \
			"merge_perf_upsert_10pct" \
			"${MERGE_PERF_CANARY_CASE}" \
			"merge_perf_localized_1pct" \
			"merge_perf_delete_5pct")"
		note "${merge_perf_case_presence_status}"

		merge_perf_same_sha_compare_runs="$(compute_regression_canary_compare_runs \
			"$(json_path_for_label "${merge_perf_presence_label}" "merge_perf")" \
			"${MERGE_PERF_CANARY_CASE}" \
			"${VALIDATION_COMPARE_RUNS}")"
		note ""
		note "Running merge_perf same-SHA canary compare..."
		note "- Merge perf same-SHA canary compare runs: ${merge_perf_same_sha_compare_runs}"
		run_same_sha_canary_compare \
			"merge_perf" \
			"run_merge_perf_case" \
			"${merge_perf_same_sha_compare_runs}" \
			"merge-perf-same-sha-base" \
			"merge-perf-same-sha-cand"
		merge_perf_same_sha_status="$(assert_same_sha_compare_is_fail_closed \
			"$(json_path_for_label "merge-perf-same-sha-base" "merge_perf")" \
			"$(json_path_for_label "merge-perf-same-sha-cand" "merge_perf")")"
		note "${merge_perf_same_sha_status}"
		merge_perf_regression_baseline_json="$(json_path_for_label "merge-perf-same-sha-base" "merge_perf")"
	else
		note "Running merge_perf same-SHA branch compare..."
		merge_perf_same_sha_log="${LOG_DIR}/merge_perf_same_sha_compare.log"
		merge_perf_base_label="base-${VALIDATION_SHA}"
		merge_perf_cand_label="cand-${VALIDATION_SHA}"
		env \
			DELTA_BENCH_FIXTURES="${PRIMARY_FIXTURES_DIR}" \
			DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
			"${SCRIPT_DIR}/compare_branch.sh" \
			"${same_sha_compare_args[@]}" \
			--methodology-profile pr-merge-perf \
			merge_perf | tee "${merge_perf_same_sha_log}"

		merge_perf_same_sha_status="$(assert_same_sha_compare_is_fail_closed "$(json_path_for_label "${merge_perf_base_label}" "merge_perf")" "$(json_path_for_label "${merge_perf_cand_label}" "merge_perf")")"
		note "${merge_perf_same_sha_status}"
		merge_perf_case_presence_status="$(assert_payload_contains_cases \
			"$(json_path_for_label "${merge_perf_base_label}" "merge_perf")" \
			"merge_perf_upsert_10pct" \
			"${MERGE_PERF_CANARY_CASE}" \
			"merge_perf_localized_1pct" \
			"merge_perf_delete_5pct")"
		note "${merge_perf_case_presence_status}"
		merge_perf_regression_baseline_json="$(json_path_for_label "${merge_perf_base_label}" "merge_perf")"
	fi

	note ""
	note "Running merge_perf regression-detection canary..."
	merge_perf_regression_delay_ms="$(compute_regression_canary_delay_ms \
		"${merge_perf_regression_baseline_json}" \
		"${MERGE_PERF_CANARY_CASE}" \
		"${VALIDATION_MERGE_PERF_DELAY_MS}")"
	merge_perf_regression_compare_runs="$(compute_regression_canary_compare_runs \
		"${merge_perf_regression_baseline_json}" \
		"${MERGE_PERF_CANARY_CASE}" \
		"${VALIDATION_COMPARE_RUNS}")"
	note "- Merge perf regression canary delay: ${merge_perf_regression_delay_ms} ms"
	note "- Merge perf regression canary compare runs: ${merge_perf_regression_compare_runs}"

	run_delayed_canary_compare \
		"merge_perf" \
		"run_merge_perf_case" \
		"${merge_perf_regression_compare_runs}" \
		"merge-perf-regression-base" \
		"merge-perf-regression-cand" \
		"DELTA_BENCH_ALLOW_MERGE_PERF_DELAY=1" \
		"DELTA_BENCH_MERGE_PERF_DELAY_MS=${merge_perf_regression_delay_ms}"
	merge_perf_regression_status="$(assert_regression_canary_detected \
		"$(json_path_for_label "merge-perf-regression-base" "merge_perf")" \
		"$(json_path_for_label "merge-perf-regression-cand" "merge_perf")" \
		"${MERGE_PERF_CANARY_CASE}")"
	note "${merge_perf_regression_status}"

	note ""
	optimize_perf_same_sha_mode="$(validation_same_sha_mode_for_suite "${VALIDATION_SCOPE}" "optimize_perf")"
	optimize_perf_regression_baseline_json=""
	if [[ "${optimize_perf_same_sha_mode}" == "canary_only" ]]; then
		optimize_perf_presence_label="optimize-perf-presence"
		note "Running optimize_perf suite coverage probe..."
		run_primary_suite_presence_probe "optimize_perf" "${optimize_perf_presence_label}"
		optimize_perf_case_presence_status="$(assert_payload_contains_cases \
			"$(json_path_for_label "${optimize_perf_presence_label}" "optimize_perf")" \
			"${OPTIMIZE_PERF_CANARY_CASE}" \
			"optimize_perf_noop_already_compact" \
			"vacuum_perf_execute_lite")"
		note "${optimize_perf_case_presence_status}"

		optimize_perf_same_sha_compare_runs="$(compute_regression_canary_compare_runs \
			"$(json_path_for_label "${optimize_perf_presence_label}" "optimize_perf")" \
			"${OPTIMIZE_PERF_CANARY_CASE}" \
			"${VALIDATION_COMPARE_RUNS}")"
		note ""
		note "Running optimize_perf same-SHA canary compare..."
		note "- Optimize perf same-SHA canary compare runs: ${optimize_perf_same_sha_compare_runs}"
		run_same_sha_canary_compare \
			"optimize_perf" \
			"run_optimize_perf_case" \
			"${optimize_perf_same_sha_compare_runs}" \
			"optimize-perf-same-sha-base" \
			"optimize-perf-same-sha-cand"
		optimize_perf_same_sha_status="$(assert_same_sha_compare_is_fail_closed \
			"$(json_path_for_label "optimize-perf-same-sha-base" "optimize_perf")" \
			"$(json_path_for_label "optimize-perf-same-sha-cand" "optimize_perf")")"
		note "${optimize_perf_same_sha_status}"
		optimize_perf_regression_baseline_json="$(json_path_for_label "optimize-perf-same-sha-base" "optimize_perf")"
	else
		note "Running optimize_perf same-SHA branch compare..."
		optimize_perf_same_sha_log="${LOG_DIR}/optimize_perf_same_sha_compare.log"
		optimize_perf_base_label="base-${VALIDATION_SHA}"
		optimize_perf_cand_label="cand-${VALIDATION_SHA}"
		env \
			DELTA_BENCH_FIXTURES="${PRIMARY_FIXTURES_DIR}" \
			DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
			"${SCRIPT_DIR}/compare_branch.sh" \
			"${same_sha_compare_args[@]}" \
			--methodology-profile pr-optimize-perf \
			optimize_perf | tee "${optimize_perf_same_sha_log}"

		optimize_perf_same_sha_status="$(assert_same_sha_compare_is_fail_closed "$(json_path_for_label "${optimize_perf_base_label}" "optimize_perf")" "$(json_path_for_label "${optimize_perf_cand_label}" "optimize_perf")")"
		note "${optimize_perf_same_sha_status}"
		optimize_perf_case_presence_status="$(assert_payload_contains_cases \
			"$(json_path_for_label "${optimize_perf_base_label}" "optimize_perf")" \
			"${OPTIMIZE_PERF_CANARY_CASE}" \
			"optimize_perf_noop_already_compact" \
			"vacuum_perf_execute_lite")"
		note "${optimize_perf_case_presence_status}"
		optimize_perf_regression_baseline_json="$(json_path_for_label "${optimize_perf_base_label}" "optimize_perf")"
	fi

	note ""
	note "Running optimize_perf regression-detection canary..."
	optimize_perf_regression_delay_ms="$(compute_regression_canary_delay_ms \
		"${optimize_perf_regression_baseline_json}" \
		"${OPTIMIZE_PERF_CANARY_CASE}" \
		"${VALIDATION_OPTIMIZE_PERF_DELAY_MS}")"
	optimize_perf_regression_compare_runs="$(compute_regression_canary_compare_runs \
		"${optimize_perf_regression_baseline_json}" \
		"${OPTIMIZE_PERF_CANARY_CASE}" \
		"${VALIDATION_COMPARE_RUNS}")"
	note "- Optimize perf regression canary delay: ${optimize_perf_regression_delay_ms} ms"
	note "- Optimize perf regression canary compare runs: ${optimize_perf_regression_compare_runs}"

	run_delayed_canary_compare \
		"optimize_perf" \
		"run_optimize_perf_case" \
		"${optimize_perf_regression_compare_runs}" \
		"optimize-perf-regression-base" \
		"optimize-perf-regression-cand" \
		"DELTA_BENCH_ALLOW_OPTIMIZE_PERF_DELAY=1" \
		"DELTA_BENCH_OPTIMIZE_PERF_DELAY_MS=${optimize_perf_regression_delay_ms}"
	optimize_perf_regression_status="$(assert_regression_canary_detected \
		"$(json_path_for_label "optimize-perf-regression-base" "optimize_perf")" \
		"$(json_path_for_label "optimize-perf-regression-cand" "optimize_perf")" \
		"${OPTIMIZE_PERF_CANARY_CASE}")"
	note "${optimize_perf_regression_status}"
else
	note ""
	note "Skipping DML/maintenance gates because validation scope is '${VALIDATION_SCOPE}'."
fi

if validation_plan_includes "${VALIDATION_GATE_LABELS}" "metadata_perf"; then
	note ""
	note "Running metadata_perf same-SHA branch compare..."
	metadata_perf_same_sha_log="${LOG_DIR}/metadata_perf_same_sha_compare.log"
	metadata_perf_base_label="base-${VALIDATION_SHA}"
	metadata_perf_cand_label="cand-${VALIDATION_SHA}"
	env \
		DELTA_BENCH_FIXTURES="${METADATA_FIXTURES_DIR}" \
		DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
		"${SCRIPT_DIR}/compare_branch.sh" \
		"${same_sha_compare_args[@]}" \
		--methodology-profile pr-metadata-perf \
		metadata_perf | tee "${metadata_perf_same_sha_log}"

	metadata_perf_same_sha_status="$(assert_same_sha_compare_is_fail_closed "$(json_path_for_label "${metadata_perf_base_label}" "metadata_perf")" "$(json_path_for_label "${metadata_perf_cand_label}" "metadata_perf")")"
	note "${metadata_perf_same_sha_status}"
	metadata_perf_case_presence_status="$(assert_payload_contains_cases \
		"$(json_path_for_label "${metadata_perf_base_label}" "metadata_perf")" \
		"metadata_perf_load_head_long_history" \
		"metadata_perf_time_travel_v0_long_history" \
		"metadata_perf_load_checkpointed_head" \
		"metadata_perf_load_uncheckpointed_head")"
	note "${metadata_perf_case_presence_status}"

	note ""
	note "Running metadata_perf regression-detection canary..."
	metadata_perf_regression_delay_ms="$(compute_regression_canary_delay_ms \
		"$(json_path_for_label "${metadata_perf_base_label}" "metadata_perf")" \
		"${METADATA_PERF_CANARY_CASE}" \
		"${VALIDATION_METADATA_PERF_DELAY_MS}")"
	metadata_perf_regression_compare_runs="$(compute_regression_canary_compare_runs \
		"$(json_path_for_label "${metadata_perf_base_label}" "metadata_perf")" \
		"${METADATA_PERF_CANARY_CASE}" \
		"${VALIDATION_COMPARE_RUNS}")"
	note "- Metadata perf regression canary delay: ${metadata_perf_regression_delay_ms} ms"
	note "- Metadata perf regression canary compare runs: ${metadata_perf_regression_compare_runs}"

	metadata_perf_base_run_labels=()
	metadata_perf_cand_run_labels=()
	run_idx=1
	while ((run_idx <= metadata_perf_regression_compare_runs)); do
		base_label="metadata-perf-regression-base-r${run_idx}"
		cand_label="metadata-perf-regression-cand-r${run_idx}"
		run_metadata_perf_case "${base_label}"
		run_metadata_perf_case "${cand_label}" \
			"DELTA_BENCH_ALLOW_METADATA_PERF_DELAY=1" \
			"DELTA_BENCH_METADATA_PERF_DELAY_MS=${metadata_perf_regression_delay_ms}"
		metadata_perf_base_run_labels+=("${base_label}")
		metadata_perf_cand_run_labels+=("${cand_label}")
		run_idx=$((run_idx + 1))
	done

	aggregate_suite_labels "metadata_perf" "metadata-perf-regression-base" "${metadata_perf_base_run_labels[@]}"
	aggregate_suite_labels "metadata_perf" "metadata-perf-regression-cand" "${metadata_perf_cand_run_labels[@]}"
	metadata_perf_regression_status="$(assert_regression_canary_detected \
		"$(json_path_for_label "metadata-perf-regression-base" "metadata_perf")" \
		"$(json_path_for_label "metadata-perf-regression-cand" "metadata_perf")" \
		"${METADATA_PERF_CANARY_CASE}")"
	note "${metadata_perf_regression_status}"
else
	note ""
	note "Skipping metadata_perf gate because validation scope is '${VALIDATION_SCOPE}'."
fi

if validation_plan_includes "${VALIDATION_GATE_LABELS}" "tpcds"; then
	if tpcds_validation_enabled; then
		note ""
		note "Running tpcds same-SHA branch compare..."
		tpcds_same_sha_log="${LOG_DIR}/tpcds_same_sha_compare.log"
		tpcds_base_label="base-${VALIDATION_SHA}"
		tpcds_cand_label="cand-${VALIDATION_SHA}"
		env \
			DELTA_BENCH_FIXTURES="${TPCDS_FIXTURES_DIR}" \
			DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
			"${SCRIPT_DIR}/compare_branch.sh" \
			"${same_sha_compare_args[@]}" \
			--methodology-profile pr-tpcds \
			tpcds | tee "${tpcds_same_sha_log}"

		tpcds_same_sha_status="$(assert_same_sha_compare_is_fail_closed "$(json_path_for_label "${tpcds_base_label}" "tpcds")" "$(json_path_for_label "${tpcds_cand_label}" "tpcds")")"
		note "${tpcds_same_sha_status}"
		tpcds_case_presence_status="$(assert_payload_contains_cases \
			"$(json_path_for_label "${tpcds_base_label}" "tpcds")" \
			"tpcds_q03" \
			"tpcds_q07" \
			"tpcds_q64")"
		note "${tpcds_case_presence_status}"

		note ""
		note "Running tpcds regression-detection canary..."
		tpcds_regression_delay_ms="$(compute_regression_canary_delay_ms \
			"$(json_path_for_label "${tpcds_base_label}" "tpcds")" \
			"${VALIDATION_TPCDS_CASE}" \
			"${VALIDATION_TPCDS_DELAY_MS}")"
		tpcds_regression_compare_runs="$(compute_regression_canary_compare_runs \
			"$(json_path_for_label "${tpcds_base_label}" "tpcds")" \
			"${VALIDATION_TPCDS_CASE}" \
			"${VALIDATION_COMPARE_RUNS}")"
		note "- TPC-DS regression canary delay: ${tpcds_regression_delay_ms} ms"
		note "- TPC-DS regression canary compare runs: ${tpcds_regression_compare_runs}"

		tpcds_base_run_labels=()
		tpcds_cand_run_labels=()
		run_idx=1
		while ((run_idx <= tpcds_regression_compare_runs)); do
			base_label="tpcds-regression-base-r${run_idx}"
			cand_label="tpcds-regression-cand-r${run_idx}"
			run_tpcds_case "${base_label}"
			run_tpcds_case "${cand_label}" \
				"DELTA_BENCH_ALLOW_TPCDS_DELAY=1" \
				"DELTA_BENCH_TPCDS_DELAY_MS=${tpcds_regression_delay_ms}"
			tpcds_base_run_labels+=("${base_label}")
			tpcds_cand_run_labels+=("${cand_label}")
			run_idx=$((run_idx + 1))
		done

		aggregate_suite_labels "tpcds" "tpcds-regression-base" "${tpcds_base_run_labels[@]}"
		aggregate_suite_labels "tpcds" "tpcds-regression-cand" "${tpcds_cand_run_labels[@]}"
		tpcds_regression_status="$(assert_regression_canary_detected \
			"$(json_path_for_label "tpcds-regression-base" "tpcds")" \
			"$(json_path_for_label "tpcds-regression-cand" "tpcds")" \
			"${VALIDATION_TPCDS_CASE}")"
		note "${tpcds_regression_status}"
	else
		note ""
		note "Skipping tpcds promotion gate because --dataset-id is '${VALIDATION_DATASET_ID}'. Use --dataset-id tpcds_duckdb to enable it."
	fi
else
	note ""
	note "Skipping tpcds promotion gate because validation scope is '${VALIDATION_SCOPE}'."
fi

note ""
note "Validation summary written to ${SUMMARY_FILE}"
