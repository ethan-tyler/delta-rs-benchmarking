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
SRC_TPCDS_PY="${ROOT_DIR}/python/delta_bench_tpcds"
DEST_TPCDS_PY="${DELTA_RS_DIR}/python/delta_bench_tpcds"
OVERLAY_MANIFEST_PATH="${DEST_CRATE}/.delta_bench_overlay_manifest"
DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS="${DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS:-300}"
CHECKOUT_LOCK_FD=""
CHECKOUT_LOCK_DIR=""

default_checkout_lock_file() {
	local checkout_dir="${1:-}"
	local checkout_parent
	checkout_parent="$(dirname "${checkout_dir}")"
	local checkout_name
	checkout_name="$(basename "${checkout_dir}")"
	checkout_name="${checkout_name#/}"
	while [[ "${checkout_name}" == .* ]]; do
		checkout_name="${checkout_name#.}"
	done
	if [[ -z "${checkout_name}" ]]; then
		checkout_name="delta-rs-under-test"
	fi
	printf '%s/.%s.delta_bench_checkout.lock\n' "${checkout_parent}" "${checkout_name}"
}

DELTA_BENCH_CHECKOUT_LOCK_FILE="${DELTA_BENCH_CHECKOUT_LOCK_FILE:-$(default_checkout_lock_file "${DELTA_RS_DIR}")}"

release_checkout_lock() {
	if [[ -n "${CHECKOUT_LOCK_FD}" ]]; then
		eval "exec ${CHECKOUT_LOCK_FD}>&-" >/dev/null 2>&1 || true
		CHECKOUT_LOCK_FD=""
	fi
	if [[ -n "${CHECKOUT_LOCK_DIR}" ]]; then
		rm -f "${CHECKOUT_LOCK_DIR}/pid" >/dev/null 2>&1 || true
		rmdir "${CHECKOUT_LOCK_DIR}" >/dev/null 2>&1 || true
		CHECKOUT_LOCK_DIR=""
	fi
}

acquire_checkout_lock() {
	if [[ "${DELTA_BENCH_CHECKOUT_LOCK_HELD:-0}" == "1" ]]; then
		return
	fi

	if command -v flock >/dev/null 2>&1; then
		mkdir -p "$(dirname "${DELTA_BENCH_CHECKOUT_LOCK_FILE}")"
		exec {CHECKOUT_LOCK_FD}>"${DELTA_BENCH_CHECKOUT_LOCK_FILE}"
		if ! flock -w "${DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS}" "${CHECKOUT_LOCK_FD}"; then
			echo "failed to acquire checkout lock within ${DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS}s: ${DELTA_BENCH_CHECKOUT_LOCK_FILE}" >&2
			exit 1
		fi
		export DELTA_BENCH_CHECKOUT_LOCK_HELD=1
		return
	fi

	mkdir -p "$(dirname "${DELTA_BENCH_CHECKOUT_LOCK_FILE}")"
	local lock_dir="${DELTA_BENCH_CHECKOUT_LOCK_FILE}.dir"
	local deadline=$((SECONDS + DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS))
	while true; do
		if mkdir "${lock_dir}" >/dev/null 2>&1; then
			CHECKOUT_LOCK_DIR="${lock_dir}"
			printf '%s\n' "$$" >"${CHECKOUT_LOCK_DIR}/pid" || true
			export DELTA_BENCH_CHECKOUT_LOCK_HELD=1
			return
		fi
		if ((SECONDS >= deadline)); then
			echo "failed to acquire checkout lock within ${DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS}s: ${DELTA_BENCH_CHECKOUT_LOCK_FILE}" >&2
			exit 1
		fi
		sleep 1
	done
}

trap release_checkout_lock EXIT

if [[ ! -d "${DELTA_RS_DIR}/.git" ]]; then
	echo "delta-rs checkout not found at ${DELTA_RS_DIR}. Run ./scripts/prepare_delta_rs.sh first."
	exit 1
fi

acquire_checkout_lock

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

mkdir -p "${DEST_TPCDS_PY}"
rsync -a --delete "${SRC_TPCDS_PY}/" "${DEST_TPCDS_PY}/"

{
	while IFS= read -r source_path; do
		relative_path="${source_path#${SRC_CRATE}/}"
		printf 'crates/delta-bench/%s\n' "${relative_path}"
	done < <(find "${SRC_CRATE}" -type f ! -path "${SRC_CRATE}/target/*" ! -name 'Cargo.toml.delta-rs' | LC_ALL=C sort)

	while IFS= read -r source_path; do
		relative_path="${source_path#${ROOT_DIR}/}"
		printf '%s\n' "${relative_path}"
	done < <(find "${SRC_BENCH_MANIFESTS}" -type f | LC_ALL=C sort)

	while IFS= read -r source_path; do
		relative_path="${source_path#${ROOT_DIR}/}"
		printf '%s\n' "${relative_path}"
	done < <(find "${SRC_BACKEND_PROFILES}" -type f | LC_ALL=C sort)

	while IFS= read -r source_path; do
		relative_path="${source_path#${ROOT_DIR}/}"
		printf '%s\n' "${relative_path}"
	done < <(find "${SRC_INTEROP_PY}" -type f | LC_ALL=C sort)

	while IFS= read -r source_path; do
		relative_path="${source_path#${ROOT_DIR}/}"
		printf '%s\n' "${relative_path}"
	done < <(find "${SRC_TPCDS_PY}" -type f | LC_ALL=C sort)
} >"${OVERLAY_MANIFEST_PATH}"

echo "synced harness to ${DEST_CRATE}"
