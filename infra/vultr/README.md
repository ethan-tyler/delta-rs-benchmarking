# Vultr IaC Baseline (Wave 1)

This directory is the Terraform home used by `scripts/provision_vultr.sh`.

## Security controls implemented here

- Provider version pinning (`versions.tf`)
- Input validation for API ACL CIDRs (`variables.tf`)
- Explicit prohibition of `0.0.0.0/0` and `::/0` API ACL values

## Required operating model

- Use a **separate Vultr account** for benchmarking control-plane isolation.
- Use two users only: `automation` and `breakglass-admin`.
- Enforce MFA for all human access.
- Rotate API keys every 30 days with overlap cutover.
- Configure API Access Control allowlist to fixed CIDRs only.

## Remote state

Configure encrypted remote state before production use. Suggested options:

1. S3-compatible backend with server-side encryption and restricted IAM policy.
2. Terraform Cloud/Enterprise with RBAC and audit trail.

Do not store long-lived state in local files outside isolated development.

## Destructive operations gate

`scripts/provision_vultr.sh rotate-runner` and `destroy` enforce:

1. CI execution context by default (`CI=true`) unless breakglass override (`DELTA_BENCH_ALLOW_LOCAL_DESTRUCTIVE=1`) is set.
2. Two distinct approver IDs (`DELTA_BENCH_APPROVER_1`, `DELTA_BENCH_APPROVER_2`).
3. Immutable approval evidence (`DELTA_BENCH_APPROVAL_EVIDENCE_FILE`).
