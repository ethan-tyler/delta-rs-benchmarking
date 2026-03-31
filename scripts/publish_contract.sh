#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
OUTPUT_DIR="${PUBLISH_CONTRACT_OUTPUT_DIR:-${ROOT_DIR}/results/contracts/latest}"

usage() {
	cat <<EOF
Usage:
  ./scripts/publish_contract.sh [--output-dir <path>]

Publishes the current benchmark contract bundle to an artifact directory.
The bundle includes the operator docs, authoritative manifests, workflow
entrypoints, and a machine-readable manifest describing the published contract.
EOF
}

while [[ $# -gt 0 ]]; do
	case "$1" in
	--output-dir)
		OUTPUT_DIR="$2"
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

publish_file() {
	local relative_path="$1"
	local source_path="${ROOT_DIR}/${relative_path}"
	local destination_path="${OUTPUT_DIR}/${relative_path}"
	mkdir -p "$(dirname "${destination_path}")"
	cp "${source_path}" "${destination_path}"
}

mkdir -p "${OUTPUT_DIR}"

doc_files=()
while IFS= read -r relative_path; do
	doc_files+=("${relative_path}")
done < <(cd "${ROOT_DIR}" && find docs -maxdepth 1 -type f -name '*.md' | sort)

published_files=(
	"README.md"
	"${doc_files[@]}"
	"bench/manifests/core_rust.yaml"
	"bench/manifests/core_python.yaml"
	"scripts/bench.sh"
	"scripts/cleanup_local.sh"
	"scripts/compare_branch.sh"
	"scripts/longitudinal_bench.sh"
	"scripts/publish_contract.sh"
	"scripts/validate_perf_harness.sh"
)

for relative_path in "${published_files[@]}"; do
	publish_file "${relative_path}"
done

export CONTRACT_OUTPUT_DIR="${OUTPUT_DIR}"
export CONTRACT_ROOT_DIR="${ROOT_DIR}"
python3 - "${published_files[@]}" <<'PY'
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

output_dir = Path(os.environ["CONTRACT_OUTPUT_DIR"])
root_dir = Path(os.environ["CONTRACT_ROOT_DIR"])
files = sys.argv[1:]
manifest = {
    "schema_version": 1,
    "published_at": datetime.now(timezone.utc).isoformat(),
    "result_schema_version": 5,
    "perf_status_values": ["trusted", "validation_only", "invalid"],
    "files": files,
    "source_root": str(root_dir),
}
(output_dir / "manifest.json").write_text(
    json.dumps(manifest, indent=2, sort_keys=True) + "\n",
    encoding="utf-8",
)
PY

echo "published contract bundle: ${OUTPUT_DIR}"
