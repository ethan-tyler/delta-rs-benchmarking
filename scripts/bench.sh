#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DELTA_RS_DIR="${DELTA_RS_DIR:-${ROOT_DIR}/.delta-rs-under-test}"
DELTA_BENCH_EXEC_ROOT="${DELTA_BENCH_EXEC_ROOT:-${ROOT_DIR}}"

FIXTURES_DIR="${DELTA_BENCH_FIXTURES:-${ROOT_DIR}/fixtures}"
RESULTS_DIR="${DELTA_BENCH_RESULTS:-${ROOT_DIR}/results}"
LABEL="${DELTA_BENCH_LABEL:-local}"
BACKEND_PROFILE="${DELTA_BENCH_BACKEND_PROFILE:-}"
DELTA_BENCH_SUPPRESS_RUST_WARNINGS="${DELTA_BENCH_SUPPRESS_RUST_WARNINGS:-1}"
HARNESS_REVISION="${DELTA_BENCH_HARNESS_REVISION:-}"

run_delta_bench() {
  (
    cd "${DELTA_BENCH_EXEC_ROOT}"
    if [[ "${DELTA_BENCH_SUPPRESS_RUST_WARNINGS}" == "1" ]]; then
      RUSTFLAGS="${RUSTFLAGS:-} -Awarnings" cargo run --quiet -p delta-bench -- "$@"
    else
      cargo run -p delta-bench -- "$@"
    fi
  )
}

ensure_harness_available() {
  if [[ "${DELTA_BENCH_EXEC_ROOT}" == "${ROOT_DIR}" ]]; then
    return
  fi

  if [[ ! -f "${DELTA_BENCH_EXEC_ROOT}/crates/delta-bench/Cargo.toml" ]]; then
    "${SCRIPT_DIR}/sync_harness_to_delta_rs.sh"
  fi
}

usage() {
  cat <<EOF
Usage:
  ./scripts/bench.sh <command> [options]
  ./scripts/bench.sh -h, --help

Commands:
  data    Generate benchmark fixtures.
  run     Execute benchmark suites.
  list    List available targets (default: all).
  doctor  Validate local benchmark wiring.

Data command options:
  ./scripts/bench.sh data [options]
    --scale <sf1|sf10|sf100>
    --dataset-id <tiny_smoke|medium_selective|small_files|many_versions|tpcds_duckdb>
    --seed <N>
    --force
    --storage-backend <local|s3>
    --storage-option <KEY=VALUE> (repeatable)
    --backend-profile <NAME>

Run command options:
  ./scripts/bench.sh run [options]
    --scale <sf1>
    --dataset-id <tiny_smoke|medium_selective|small_files|many_versions|tpcds_duckdb>
    --suite <scan|write|write_perf|delete_update|merge|metadata|optimize_vacuum|tpcds|interop_py|all>
    --case-filter <SUBSTR>
    --runner <rust|python|all>
    --lane <smoke|correctness|macro>
    --mode <perf|assert>
    --timing-phase <load|plan|execute|validate>
    --warmup <N>
    --iters <N>
    --no-summary-table
    --label <L>
    --storage-backend <local|s3>
    --storage-option <KEY=VALUE> (repeatable)
    --backend-profile <NAME>

Other commands:
  ./scripts/bench.sh list [target]
  ./scripts/bench.sh doctor

Environment:
  DELTA_BENCH_EXEC_ROOT=/path/to/cargo/workspace
  DELTA_RS_DIR=/path/to/.delta-rs-under-test
  DELTA_BENCH_SUPPRESS_RUST_WARNINGS=1   # set 0 to show compiler warnings
EOF
}

cmd="${1:-}"
if [[ -z "${cmd}" ]]; then
  usage
  exit 1
fi
if [[ "${cmd}" == "-h" || "${cmd}" == "--help" ]]; then
  usage
  exit 0
fi
shift || true

ensure_harness_available

case "${cmd}" in
  data)
    scale="sf1"
    dataset_id=""
    seed="42"
    force=""
    storage_backend="local"
    storage_options=()
    while [[ $# -gt 0 ]]; do
      case "$1" in
        --scale) scale="$2"; shift 2 ;;
        --dataset-id) dataset_id="$2"; shift 2 ;;
        --seed) seed="$2"; shift 2 ;;
        --force) force="--force"; shift 1 ;;
        --storage-backend) storage_backend="$2"; shift 2 ;;
        --storage-option) storage_options+=("$2"); shift 2 ;;
        --backend-profile) BACKEND_PROFILE="$2"; shift 2 ;;
        *) echo "unknown arg: $1"; exit 1 ;;
      esac
    done
    storage_args=(--storage-backend "${storage_backend}")
    profile_args=()
    if [[ -n "${BACKEND_PROFILE}" ]]; then
      profile_args+=(--backend-profile "${BACKEND_PROFILE}")
    fi
    if [[ ${#storage_options[@]} -gt 0 ]]; then
      for option in "${storage_options[@]}"; do
        storage_args+=(--storage-option "${option}")
      done
    fi
    data_args=(--scale "${scale}" --seed "${seed}")
    if [[ -n "${dataset_id}" ]]; then
      data_args+=(--dataset-id "${dataset_id}")
    fi
    cmd_args=(--fixtures-dir "${FIXTURES_DIR}" "${storage_args[@]}")
    if [[ ${#profile_args[@]} -gt 0 ]]; then
      cmd_args+=("${profile_args[@]}")
    fi
    cmd_args+=(data "${data_args[@]}")
    if [[ -n "${force}" ]]; then
      cmd_args+=("${force}")
    fi
    run_delta_bench "${cmd_args[@]}"
    ;;
  run)
    scale="sf1"
    dataset_id=""
    suite="all"
    case_filter=""
    runner="all"
    lane="smoke"
    benchmark_mode="perf"
    timing_phase="execute"
    warmup="1"
    iters="5"
    no_summary_table=0
    storage_backend="local"
    storage_options=()

    while [[ $# -gt 0 ]]; do
      case "$1" in
        --scale) scale="$2"; shift 2 ;;
        --dataset-id) dataset_id="$2"; shift 2 ;;
        --suite) suite="$2"; shift 2 ;;
        --case-filter) case_filter="$2"; shift 2 ;;
        --runner) runner="$2"; shift 2 ;;
        --lane) lane="$2"; shift 2 ;;
        --mode) benchmark_mode="$2"; shift 2 ;;
        --timing-phase) timing_phase="$2"; shift 2 ;;
        --warmup) warmup="$2"; shift 2 ;;
        --iters) iters="$2"; shift 2 ;;
        --no-summary-table) no_summary_table=1; shift 1 ;;
        --label) LABEL="$2"; shift 2 ;;
        --storage-backend) storage_backend="$2"; shift 2 ;;
        --storage-option) storage_options+=("$2"); shift 2 ;;
        --backend-profile) BACKEND_PROFILE="$2"; shift 2 ;;
        *) echo "unknown arg: $1"; exit 1 ;;
      esac
    done

    git_sha=""
    harness_revision="${HARNESS_REVISION}"
    if [[ -d "${DELTA_BENCH_EXEC_ROOT}/.git" ]]; then
      git_sha="$(git -C "${DELTA_BENCH_EXEC_ROOT}" rev-parse HEAD 2>/dev/null || true)"
    elif [[ -d "${DELTA_RS_DIR}/.git" ]]; then
      git_sha="$(git -C "${DELTA_RS_DIR}" rev-parse HEAD 2>/dev/null || true)"
    fi
    if [[ -z "${harness_revision}" && -d "${ROOT_DIR}/.git" ]]; then
      harness_revision="$(git -C "${ROOT_DIR}" rev-parse HEAD 2>/dev/null || true)"
    fi

    storage_args=(--storage-backend "${storage_backend}")
    profile_args=()
    if [[ -n "${BACKEND_PROFILE}" ]]; then
      profile_args+=(--backend-profile "${BACKEND_PROFILE}")
    fi
    if [[ ${#storage_options[@]} -gt 0 ]]; then
      for option in "${storage_options[@]}"; do
        storage_args+=(--storage-option "${option}")
      done
    fi

    run_args=(--scale "${scale}" --target "${suite}" --runner "${runner}" --lane "${lane}" --mode "${benchmark_mode}" --timing-phase "${timing_phase}" --warmup "${warmup}" --iterations "${iters}")
    if [[ -n "${dataset_id}" ]]; then
      run_args+=(--dataset-id "${dataset_id}")
    fi
    if [[ -n "${case_filter}" ]]; then
      run_args+=(--case-filter "${case_filter}")
    fi
    if (( no_summary_table != 0 )); then
      run_args+=(--no-summary-table)
    fi

    cmd_args=(
      --fixtures-dir "${FIXTURES_DIR}"
      --results-dir "${RESULTS_DIR}"
      --label "${LABEL}"
      --git-sha "${git_sha}"
      "${storage_args[@]}"
    )
    if [[ -n "${harness_revision}" ]]; then
      cmd_args+=(--harness-revision "${harness_revision}")
    fi
    if [[ ${#profile_args[@]} -gt 0 ]]; then
      cmd_args+=("${profile_args[@]}")
    fi
    cmd_args+=(run "${run_args[@]}")
    run_delta_bench "${cmd_args[@]}"
    ;;
  list)
    target="${1:-all}"
    run_delta_bench list "${target}"
    ;;
  doctor)
    run_delta_bench doctor
    ;;
  *)
    usage
    exit 1
    ;;
esac
