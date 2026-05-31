#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
PYTHON_BIN="${PYTHON:-}"

if [[ -z "${PYTHON_BIN}" && -x "${ROOT_DIR}/python/.venv/bin/python" ]]; then
	PYTHON_BIN="${ROOT_DIR}/python/.venv/bin/python"
fi
if [[ -z "${PYTHON_BIN}" ]]; then
	PYTHON_BIN="python3"
fi

cd "${ROOT_DIR}"
"${PYTHON_BIN}" -m pytest python/tests/test_docs_contract.py -q "$@"
