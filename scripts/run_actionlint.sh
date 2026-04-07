#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
ACTIONLINT_VERSION="${ACTIONLINT_VERSION:-1.7.8}"

platform="$(uname -s | tr '[:upper:]' '[:lower:]')"
case "${platform}" in
linux | darwin) ;;
*)
	echo "unsupported platform for actionlint bootstrap: ${platform}" >&2
	exit 1
	;;
esac

arch="$(uname -m)"
case "${arch}" in
x86_64 | amd64)
	arch="amd64"
	;;
arm64 | aarch64)
	arch="arm64"
	;;
*)
	echo "unsupported architecture for actionlint bootstrap: ${arch}" >&2
	exit 1
	;;
esac

archive="actionlint_${ACTIONLINT_VERSION}_${platform}_${arch}.tar.gz"
download_url="https://github.com/rhysd/actionlint/releases/download/v${ACTIONLINT_VERSION}/${archive}"
temp_dir="$(mktemp -d)"
trap 'rm -rf "${temp_dir}"' EXIT

curl -fsSL "${download_url}" -o "${temp_dir}/actionlint.tgz"
tar -xzf "${temp_dir}/actionlint.tgz" -C "${temp_dir}"

cd "${ROOT_DIR}"
if [[ $# -eq 0 ]]; then
	set -- .github/workflows/benchmark.yml
fi

exec "${temp_dir}/actionlint" "$@"
