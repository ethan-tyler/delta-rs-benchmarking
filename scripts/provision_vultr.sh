#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
TF_DIR="${VULTR_TERRAFORM_DIR:-${ROOT_DIR}/infra/vultr}"

VULTR_API_ALLOWED_CIDRS="${VULTR_API_ALLOWED_CIDRS:-}"
RUNNER_RESOURCE_ADDR="${VULTR_RUNNER_RESOURCE_ADDR:-vultr_instance.runner}"

usage() {
  cat <<EOF
Usage:
  ./scripts/provision_vultr.sh <command> [terraform args...]

Commands:
  plan           terraform init + plan
  apply          terraform init + apply
  rotate-key     validate API ACL and print a key-rotation checklist
  rotate-runner  terraform apply -replace=<runner resource> (requires 2 approvers)
  destroy        terraform destroy (requires 2 approvers)

Environment:
  VULTR_TERRAFORM_DIR        Terraform working directory (default: infra/vultr)
  VULTR_API_ALLOWED_CIDRS    Comma-separated ACL CIDRs for Vultr API access control
  VULTR_RUNNER_RESOURCE_ADDR Resource address to replace for rotate-runner
  DELTA_BENCH_APPROVER_1     First approver identifier for destructive operations
  DELTA_BENCH_APPROVER_2     Second approver identifier for destructive operations
  DELTA_BENCH_APPROVAL_EVIDENCE_FILE Path to immutable approval evidence for destructive operations
  DELTA_BENCH_ALLOW_LOCAL_DESTRUCTIVE Set to 1 to bypass CI-only destructive guard (breakglass only)
EOF
}

require_command() {
  local name="$1"
  if ! command -v "${name}" >/dev/null 2>&1; then
    echo "required command not found: ${name}" >&2
    exit 1
  fi
}

require_tf_dir() {
  if [[ ! -d "${TF_DIR}" ]]; then
    echo "terraform directory not found: ${TF_DIR}" >&2
    echo "create infra code first or set VULTR_TERRAFORM_DIR" >&2
    exit 1
  fi
}

validate_acl_allowlist() {
  if [[ -z "${VULTR_API_ALLOWED_CIDRS}" ]]; then
    echo "VULTR_API_ALLOWED_CIDRS is empty; provide explicit allowlist CIDRs" >&2
    return 1
  fi
  if [[ "${VULTR_API_ALLOWED_CIDRS}" == *"0.0.0.0/0"* ]]; then
    echo "API ACL must not contain 0.0.0.0/0" >&2
    return 1
  fi
  if [[ "${VULTR_API_ALLOWED_CIDRS}" == *"::/0"* ]]; then
    echo "API ACL must not contain ::/0" >&2
    return 1
  fi
  echo "api_acl_allowlist_ok=true"
}

require_two_person_approval() {
  local a1="${DELTA_BENCH_APPROVER_1:-}"
  local a2="${DELTA_BENCH_APPROVER_2:-}"
  local evidence_file="${DELTA_BENCH_APPROVAL_EVIDENCE_FILE:-}"
  if [[ -z "${a1}" || -z "${a2}" ]]; then
    echo "missing two-person approval (set DELTA_BENCH_APPROVER_1 and DELTA_BENCH_APPROVER_2)" >&2
    exit 1
  fi
  if [[ "${a1}" == "${a2}" ]]; then
    echo "approver identities must be distinct" >&2
    exit 1
  fi
  if [[ -z "${evidence_file}" || ! -s "${evidence_file}" ]]; then
    echo "missing approval evidence file (set DELTA_BENCH_APPROVAL_EVIDENCE_FILE to a non-empty immutable record)" >&2
    exit 1
  fi
  if [[ -n "${GITHUB_ACTOR:-}" ]]; then
    if [[ "${GITHUB_ACTOR}" == "${a1}" || "${GITHUB_ACTOR}" == "${a2}" ]]; then
      echo "workflow actor must be independent from declared approvers" >&2
      exit 1
    fi
  fi
}

require_control_plane_context() {
  if [[ "${DELTA_BENCH_ALLOW_LOCAL_DESTRUCTIVE:-0}" == "1" ]]; then
    echo "WARNING: local destructive bypass enabled (DELTA_BENCH_ALLOW_LOCAL_DESTRUCTIVE=1)" >&2
    return 0
  fi
  if [[ "${CI:-}" != "true" ]]; then
    echo "destructive operations must run from CI control plane; set DELTA_BENCH_ALLOW_LOCAL_DESTRUCTIVE=1 for breakglass local use" >&2
    exit 1
  fi
}

tf_init() {
  terraform -chdir="${TF_DIR}" init -input=false
}

tf_plan() {
  terraform -chdir="${TF_DIR}" plan "$@"
}

tf_apply() {
  terraform -chdir="${TF_DIR}" apply "$@"
}

tf_destroy() {
  terraform -chdir="${TF_DIR}" destroy "$@"
}

command="${1:-}"
if [[ -z "${command}" ]]; then
  usage
  exit 1
fi
shift || true

case "${command}" in
  plan)
    require_command terraform
    require_tf_dir
    validate_acl_allowlist
    tf_init
    tf_plan "$@"
    ;;
  apply)
    require_command terraform
    require_tf_dir
    validate_acl_allowlist
    tf_init
    tf_apply "$@"
    ;;
  rotate-key)
    validate_acl_allowlist
    cat <<EOF
rotation_checklist:
1. Create a new Vultr API key for the automation user.
2. Apply API ACL allowlist to the new key user path (${VULTR_API_ALLOWED_CIDRS}).
3. Update CI secret to use the new key and validate plan/apply.
4. Verify non-allowlisted source IPs are denied by ACL policy.
5. Revoke old key after successful overlap period.
EOF
    ;;
  rotate-runner)
    require_command terraform
    require_tf_dir
    require_control_plane_context
    require_two_person_approval
    validate_acl_allowlist
    tf_init
    tf_apply -replace="${RUNNER_RESOURCE_ADDR}" "$@"
    ;;
  destroy)
    require_command terraform
    require_tf_dir
    require_control_plane_context
    require_two_person_approval
    tf_init
    tf_destroy "$@"
    ;;
  -h|--help)
    usage
    ;;
  *)
    echo "unknown command: ${command}" >&2
    usage >&2
    exit 1
    ;;
esac
