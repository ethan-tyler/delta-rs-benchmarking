#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DELTA_RS_DIR="${DELTA_RS_DIR:-${ROOT_DIR}/.delta-rs-under-test}"
DELTA_RS_REPO_URL="${DELTA_RS_REPO_URL:-https://github.com/delta-io/delta-rs}"
DELTA_RS_BRANCH="${DELTA_RS_BRANCH:-main}"
DELTA_RS_REF="${DELTA_RS_REF:-}"
DELTA_RS_REF_TYPE="${DELTA_RS_REF_TYPE:-auto}"

if [[ ! -d "${DELTA_RS_DIR}/.git" ]]; then
  echo "cloning ${DELTA_RS_REPO_URL} into ${DELTA_RS_DIR}"
  git clone --origin origin "${DELTA_RS_REPO_URL}" "${DELTA_RS_DIR}"
fi

git -C "${DELTA_RS_DIR}" fetch origin

if [[ -n "${DELTA_RS_REF}" ]]; then
  case "${DELTA_RS_REF_TYPE}" in
    auto|commit)
      if ! git -C "${DELTA_RS_DIR}" rev-parse --verify --quiet "${DELTA_RS_REF}^{commit}" >/dev/null; then
        echo "delta-rs ref '${DELTA_RS_REF}' is not available after fetch; provide a reachable commit SHA" >&2
        exit 1
      fi
      git -C "${DELTA_RS_DIR}" checkout --detach "${DELTA_RS_REF}"
      ;;
    *)
      echo "unknown DELTA_RS_REF_TYPE '${DELTA_RS_REF_TYPE}' (expected: auto, commit)" >&2
      exit 1
      ;;
  esac
else
  git -C "${DELTA_RS_DIR}" checkout "${DELTA_RS_BRANCH}"
  git -C "${DELTA_RS_DIR}" pull --ff-only origin "${DELTA_RS_BRANCH}"
fi

resolved_ref="$(git -C "${DELTA_RS_DIR}" rev-parse --verify HEAD)"
echo "delta-rs checkout ready: ${DELTA_RS_DIR} @ ${resolved_ref}"
