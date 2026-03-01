#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DELTA_RS_DIR="${DELTA_RS_DIR:-${ROOT_DIR}/.delta-rs-under-test}"
SRC_CRATE="${ROOT_DIR}/crates/delta-bench"
DEST_CRATE="${DELTA_RS_DIR}/crates/delta-bench"
SRC_BENCH_MANIFESTS="${ROOT_DIR}/bench/manifests"
DEST_BENCH_MANIFESTS="${DELTA_RS_DIR}/bench/manifests"
SRC_BACKEND_PROFILES="${ROOT_DIR}/backends"
DEST_BACKEND_PROFILES="${DELTA_RS_DIR}/backends"
SRC_INTEROP_PY="${ROOT_DIR}/python/delta_bench_interop"
DEST_INTEROP_PY="${DELTA_RS_DIR}/python/delta_bench_interop"

if [[ ! -d "${DELTA_RS_DIR}/.git" ]]; then
  echo "delta-rs checkout not found at ${DELTA_RS_DIR}. Run ./scripts/prepare_delta_rs.sh first."
  exit 1
fi

mkdir -p "${DEST_CRATE}"

rsync -a --delete \
  --exclude 'target/' \
  --exclude 'Cargo.toml.delta-rs' \
  "${SRC_CRATE}/" "${DEST_CRATE}/"

cp "${SRC_CRATE}/Cargo.toml.delta-rs" "${DEST_CRATE}/Cargo.toml"

mkdir -p "${DEST_BENCH_MANIFESTS}"
rsync -a --delete "${SRC_BENCH_MANIFESTS}/" "${DEST_BENCH_MANIFESTS}/"

mkdir -p "${DEST_BACKEND_PROFILES}"
rsync -a --delete "${SRC_BACKEND_PROFILES}/" "${DEST_BACKEND_PROFILES}/"

mkdir -p "${DEST_INTEROP_PY}"
rsync -a --delete "${SRC_INTEROP_PY}/" "${DEST_INTEROP_PY}/"

echo "synced harness to ${DEST_CRATE}"
