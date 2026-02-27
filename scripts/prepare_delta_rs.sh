#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DELTA_RS_DIR="${DELTA_RS_DIR:-${ROOT_DIR}/.delta-rs-under-test}"
DELTA_RS_REPO_URL="${DELTA_RS_REPO_URL:-https://github.com/delta-io/delta-rs}"
DELTA_RS_BRANCH="${DELTA_RS_BRANCH:-main}"

if [[ ! -d "${DELTA_RS_DIR}/.git" ]]; then
  echo "cloning ${DELTA_RS_REPO_URL} into ${DELTA_RS_DIR}"
  git clone --origin origin "${DELTA_RS_REPO_URL}" "${DELTA_RS_DIR}"
fi

git -C "${DELTA_RS_DIR}" fetch origin

git -C "${DELTA_RS_DIR}" checkout "${DELTA_RS_BRANCH}"
git -C "${DELTA_RS_DIR}" pull --ff-only origin "${DELTA_RS_BRANCH}"

echo "delta-rs checkout ready: ${DELTA_RS_DIR}"
