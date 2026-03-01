#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

# Provider-agnostic entrypoint for runner provisioning.
# The current backend delegates to the existing Vultr Terraform helper.
exec "${ROOT_DIR}/scripts/provision_vultr.sh" "$@"
