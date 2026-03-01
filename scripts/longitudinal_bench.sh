#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYTHONPATH_DIR="${ROOT_DIR}/python${PYTHONPATH:+:${PYTHONPATH}}"

usage() {
  cat <<'EOF'
Usage:
  ./scripts/longitudinal_bench.sh <command> [args...]

Commands:
  select-revisions   Generate revision manifest
  build-artifacts    Build artifact binaries for manifest revisions
  run-matrix         Execute benchmark matrix using built artifacts
  ingest-results     Ingest normalized longitudinal rows from result JSON
  report             Generate markdown + HTML trend reports
  prune              Apply retention policies to artifacts/store
  orchestrate        Build -> run -> ingest -> report

Examples:
  ./scripts/longitudinal_bench.sh select-revisions --repository .delta-rs-under-test --strategy one-per-day --start-date 2026-01-01 --end-date 2026-01-31 --output longitudinal/manifests/jan.json
  ./scripts/longitudinal_bench.sh orchestrate --manifest longitudinal/manifests/jan.json --artifacts-dir longitudinal/artifacts --results-dir results --state-path longitudinal/state/matrix.json --store-dir longitudinal/store --markdown-path longitudinal/reports/summary.md --html-path longitudinal/reports/report.html --suite read_scan --scale sf1
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

cmd="$1"
shift || true

case "${cmd}" in
  select-revisions|build-artifacts|run-matrix|ingest-results|report|prune|orchestrate)
    PYTHONPATH="${PYTHONPATH_DIR}" python3 -m delta_bench_longitudinal.cli "${cmd}" "$@"
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "unknown command: ${cmd}" >&2
    usage >&2
    exit 1
    ;;
esac
