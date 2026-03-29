#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DELTA_RS_DIR="${DELTA_RS_DIR:-${ROOT_DIR}/.delta-rs-under-test}"
PYTHONPATH_DIR="${ROOT_DIR}/python"

VALIDATION_SHA="${VALIDATION_SHA:-}"
VALIDATION_DATASET_ID="${VALIDATION_DATASET_ID:-medium_selective}"
VALIDATION_CASE="${VALIDATION_CASE:-scan_filter_flag}"
VALIDATION_COMPARE_RUNS="${VALIDATION_COMPARE_RUNS:-5}"
VALIDATION_WARMUP="${VALIDATION_WARMUP:-1}"
VALIDATION_ITERS="${VALIDATION_ITERS:-5}"
VALIDATION_PREWARM_ITERS="${VALIDATION_PREWARM_ITERS:-1}"
VALIDATION_CANARY_ITERS="${VALIDATION_CANARY_ITERS:-3}"
VALIDATION_DELAY_MS="${VALIDATION_DELAY_MS:-150}"

timestamp_utc="$(date -u +"%Y%m%dT%H%M%SZ")"
VALIDATION_ARTIFACT_DIR="${VALIDATION_ARTIFACT_DIR:-${ROOT_DIR}/results/validation/${timestamp_utc}}"

usage() {
  cat <<EOF
Usage:
  ./scripts/validate_perf_harness.sh [options]

Options:
  --sha <commit>          delta-rs commit to validate (default: current HEAD in DELTA_RS_DIR)
  --dataset-id <id>       Dataset id for validation runs (default: ${VALIDATION_DATASET_ID})
  --artifact-dir <path>   Output directory for validation artifacts (default: ${VALIDATION_ARTIFACT_DIR})
  -h, --help              Show this help

Advanced tuning is available through environment variables:
  VALIDATION_COMPARE_RUNS, VALIDATION_WARMUP, VALIDATION_ITERS,
  VALIDATION_PREWARM_ITERS, VALIDATION_CANARY_ITERS, VALIDATION_DELAY_MS,
  VALIDATION_CASE, DELTA_RS_DIR
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

while [[ $# -gt 0 ]]; do
  case "$1" in
    --sha)
      VALIDATION_SHA="$2"
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

VALIDATION_ARTIFACT_DIR="$(canonicalize_dir "${VALIDATION_ARTIFACT_DIR}")"
RESULTS_DIR="${VALIDATION_ARTIFACT_DIR}/results"
FIXTURES_DIR="${VALIDATION_ARTIFACT_DIR}/fixtures"
LOG_DIR="${VALIDATION_ARTIFACT_DIR}/logs"
SUMMARY_FILE="${VALIDATION_ARTIFACT_DIR}/summary.md"

mkdir -p "${RESULTS_DIR}" "${FIXTURES_DIR}" "${LOG_DIR}"

note() {
  printf -- '%s\n' "$*" | tee -a "${SUMMARY_FILE}"
}

json_path_for_label() {
  local label="$1"
  printf '%s/%s/scan.json' "${RESULTS_DIR}" "${label}"
}

prepare_checkout_at_sha() {
  env DELTA_RS_REF="${VALIDATION_SHA}" DELTA_RS_REF_TYPE="commit" DELTA_RS_DIR="${DELTA_RS_DIR}" \
    "${SCRIPT_DIR}/prepare_delta_rs.sh" >/dev/null
  env DELTA_RS_DIR="${DELTA_RS_DIR}" "${SCRIPT_DIR}/sync_harness_to_delta_rs.sh" >/dev/null
}

generate_validation_fixtures() {
  env \
    DELTA_RS_DIR="${DELTA_RS_DIR}" \
    DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" \
    DELTA_BENCH_FIXTURES="${FIXTURES_DIR}" \
    DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
    "${SCRIPT_DIR}/bench.sh" data \
      --dataset-id "${VALIDATION_DATASET_ID}" \
      --seed 42 >/dev/null
}

run_scan_case() {
  local label="$1"
  local timing_phase="$2"
  shift 2

  env \
    DELTA_RS_DIR="${DELTA_RS_DIR}" \
    DELTA_BENCH_EXEC_ROOT="${DELTA_RS_DIR}" \
    DELTA_BENCH_FIXTURES="${FIXTURES_DIR}" \
    DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
    "$@" \
    "${SCRIPT_DIR}/bench.sh" run \
      --suite scan \
      --case-filter "${VALIDATION_CASE}" \
      --runner rust \
      --lane macro \
      --mode perf \
      --dataset-id "${VALIDATION_DATASET_ID}" \
      --timing-phase "${timing_phase}" \
      --warmup 0 \
      --iters "${VALIDATION_CANARY_ITERS}" \
      --label "${label}" \
      --no-summary-table >/dev/null
}

aggregate_scan_labels() {
  local output_label="$1"
  shift
  local input_paths=()
  local label
  for label in "$@"; do
    input_paths+=("$(json_path_for_label "${label}")")
  done

  mkdir -p "${RESULTS_DIR}/${output_label}"
  env PYTHONPATH="${PYTHONPATH_DIR}" python3 -m delta_bench_compare.aggregate \
    --output "$(json_path_for_label "${output_label}")" \
    --label "${output_label}" \
    "${input_paths[@]}" >/dev/null
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
allowed = {"no change", "inconclusive"}
bad = [(row.case, row.change) for row in comparison.rows if row.change not in allowed]
if bad:
    raise SystemExit(
        "same-SHA decision compare produced unexpected statuses: "
        + ", ".join(f"{case}={change}" for case, change in bad)
    )
counts = {}
for row in comparison.rows:
    counts[row.change] = counts.get(row.change, 0) + 1
print(
    "- Same-SHA decision compare statuses: "
    + ", ".join(f"{status}={counts[status]}" for status in sorted(counts))
)
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

assert_regression_canary_detected() {
  local baseline_json="$1"
  local candidate_json="$2"
  env PYTHONPATH="${PYTHONPATH_DIR}" python3 - \
    "${baseline_json}" \
    "${candidate_json}" \
    "${VALIDATION_CASE}" <<'PY'
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

if [[ ! -d "${DELTA_RS_DIR}/.git" ]]; then
  env DELTA_RS_DIR="${DELTA_RS_DIR}" "${SCRIPT_DIR}/prepare_delta_rs.sh" >/dev/null
fi

if [[ -z "${VALIDATION_SHA}" ]]; then
  VALIDATION_SHA="$(git -C "${DELTA_RS_DIR}" rev-parse HEAD)"
fi

prepare_checkout_at_sha
generate_validation_fixtures

cat > "${SUMMARY_FILE}" <<EOF
# Perf Harness Validation Summary

- Date (UTC): ${timestamp_utc}
- delta-rs SHA: ${VALIDATION_SHA}
- Dataset: ${VALIDATION_DATASET_ID}
- Scan case canary: ${VALIDATION_CASE}
- Compare runs per side: ${VALIDATION_COMPARE_RUNS}
- Iterations per run: ${VALIDATION_ITERS}
- Canary delay: ${VALIDATION_DELAY_MS} ms
- Artifacts: ${VALIDATION_ARTIFACT_DIR}

## Checks
EOF

note ""
note "Running same-SHA same-path branch compare..."
same_sha_log="${LOG_DIR}/same_sha_compare.log"
env \
  DELTA_BENCH_FIXTURES="${FIXTURES_DIR}" \
  DELTA_BENCH_RESULTS="${RESULTS_DIR}" \
  "${SCRIPT_DIR}/compare_branch.sh" \
    --base-sha "${VALIDATION_SHA}" \
    --candidate-sha "${VALIDATION_SHA}" \
    --compare-mode decision \
    --warmup "${VALIDATION_WARMUP}" \
    --iters "${VALIDATION_ITERS}" \
    --prewarm-iters "${VALIDATION_PREWARM_ITERS}" \
    --compare-runs "${VALIDATION_COMPARE_RUNS}" \
    --dataset-id "${VALIDATION_DATASET_ID}" \
    scan | tee "${same_sha_log}"

base_label="base-${VALIDATION_SHA}"
cand_label="cand-${VALIDATION_SHA}"
same_sha_status="$(assert_same_sha_compare_is_fail_closed "$(json_path_for_label "${base_label}")" "$(json_path_for_label "${cand_label}")")"
note "${same_sha_status}"

note ""
note "Running phase-isolation canaries..."

run_scan_case "canary-load-baseline" "load"
run_scan_case "canary-load-control-baseline" "execute"
run_scan_case "canary-load-delayed" "load" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_LOAD_MS=${VALIDATION_DELAY_MS}"
run_scan_case "canary-load-control-delayed" "execute" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_LOAD_MS=${VALIDATION_DELAY_MS}"
load_canary_status="$(assert_phase_canary \
  "$(json_path_for_label "canary-load-baseline")" \
  "$(json_path_for_label "canary-load-delayed")" \
  "$(json_path_for_label "canary-load-control-baseline")" \
  "$(json_path_for_label "canary-load-control-delayed")" \
  "load" \
  "execute")"
note "${load_canary_status}"

run_scan_case "canary-plan-baseline" "plan"
run_scan_case "canary-plan-control-baseline" "execute"
run_scan_case "canary-plan-delayed" "plan" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_PLAN_MS=${VALIDATION_DELAY_MS}"
run_scan_case "canary-plan-control-delayed" "execute" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_PLAN_MS=${VALIDATION_DELAY_MS}"
plan_canary_status="$(assert_phase_canary \
  "$(json_path_for_label "canary-plan-baseline")" \
  "$(json_path_for_label "canary-plan-delayed")" \
  "$(json_path_for_label "canary-plan-control-baseline")" \
  "$(json_path_for_label "canary-plan-control-delayed")" \
  "plan" \
  "execute")"
note "${plan_canary_status}"

run_scan_case "canary-validate-baseline" "validate"
run_scan_case "canary-validate-control-baseline" "execute"
run_scan_case "canary-validate-delayed" "validate" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_VALIDATE_MS=${VALIDATION_DELAY_MS}"
run_scan_case "canary-validate-control-delayed" "execute" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_VALIDATE_MS=${VALIDATION_DELAY_MS}"
validate_canary_status="$(assert_phase_canary \
  "$(json_path_for_label "canary-validate-baseline")" \
  "$(json_path_for_label "canary-validate-delayed")" \
  "$(json_path_for_label "canary-validate-control-baseline")" \
  "$(json_path_for_label "canary-validate-control-delayed")" \
  "validate" \
  "execute")"
note "${validate_canary_status}"

run_scan_case "canary-execute-baseline" "execute"
run_scan_case "canary-execute-control-baseline" "plan"
run_scan_case "canary-execute-delayed" "execute" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_EXECUTE_MS=${VALIDATION_DELAY_MS}"
run_scan_case "canary-execute-control-delayed" "plan" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_EXECUTE_MS=${VALIDATION_DELAY_MS}"
execute_canary_status="$(assert_phase_canary \
  "$(json_path_for_label "canary-execute-baseline")" \
  "$(json_path_for_label "canary-execute-delayed")" \
  "$(json_path_for_label "canary-execute-control-baseline")" \
  "$(json_path_for_label "canary-execute-control-delayed")" \
  "execute" \
  "plan")"
note "${execute_canary_status}"

note ""
note "Running regression-detection canary..."

base_run_labels=()
cand_run_labels=()
run_idx=1
while (( run_idx <= VALIDATION_COMPARE_RUNS )); do
  base_label="regression-base-r${run_idx}"
  cand_label="regression-cand-r${run_idx}"
  run_scan_case "${base_label}" "execute"
  run_scan_case "${cand_label}" "execute" "DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY=1" "DELTA_BENCH_SCAN_DELAY_EXECUTE_MS=${VALIDATION_DELAY_MS}"
  base_run_labels+=("${base_label}")
  cand_run_labels+=("${cand_label}")
  run_idx=$((run_idx + 1))
done

aggregate_scan_labels "regression-base" "${base_run_labels[@]}"
aggregate_scan_labels "regression-cand" "${cand_run_labels[@]}"
regression_canary_status="$(assert_regression_canary_detected \
  "$(json_path_for_label "regression-base")" \
  "$(json_path_for_label "regression-cand")")"
note "${regression_canary_status}"

note ""
note "Validation summary written to ${SUMMARY_FILE}"
