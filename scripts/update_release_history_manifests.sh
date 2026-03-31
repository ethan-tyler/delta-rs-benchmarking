#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DELTA_RS_DIR="${DELTA_RS_DIR:-${ROOT_DIR}/.delta-rs-under-test}"
OUT_DIR="${ROOT_DIR}/longitudinal/manifests"

"${SCRIPT_DIR}/prepare_delta_rs.sh" >/dev/null
mkdir -p "${OUT_DIR}"

"${SCRIPT_DIR}/longitudinal_bench.sh" select-revisions \
	--repository "${DELTA_RS_DIR}" \
	--strategy release-tags \
	--release-tag-pattern '^rust-v\d+\.\d+\.\d+([+-].+)?$' \
	--output "${OUT_DIR}/release-history-rust.json" >/dev/null

"${SCRIPT_DIR}/longitudinal_bench.sh" select-revisions \
	--repository "${DELTA_RS_DIR}" \
	--strategy release-tags \
	--release-tag-pattern '^python-v\d+\.\d+\.\d+([+-].+)?$' \
	--output "${OUT_DIR}/release-history-python.json" >/dev/null

python3 - "${OUT_DIR}" <<'PY'
import json
import os
import sys

out_dir = sys.argv[1]
for lane in ("rust", "python"):
    path = os.path.join(out_dir, f"release-history-{lane}.json")
    with open(path, encoding="utf-8") as fh:
        payload = json.load(fh)
    payload["repository"] = ".delta-rs-under-test"
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(payload, fh, indent=2, sort_keys=True)
        fh.write("\n")
    revisions = payload.get("revisions", [])
    print(f"{lane}: {len(revisions)} revisions -> {path}")
PY
