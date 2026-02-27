#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DELTA_RS_DIR="${DELTA_RS_DIR:-${ROOT_DIR}/.delta-rs-under-test}"
SRC_CRATE="${ROOT_DIR}/crates/delta-bench"
DEST_CRATE="${DELTA_RS_DIR}/crates/delta-bench"

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

echo "synced harness to ${DEST_CRATE}"
