# Cloud Runner Security and Fidelity Runbook

Checklist-oriented runbook for remote and hardened benchmark execution.
Use [user-guide.md](user-guide.md) for normal local contributor workflows.

## Baseline Assumptions

- dedicated benchmarking cloud account/project
- single dedicated runner in private network topology
- runner has no public IPv4 address
- benchmark data is synthetic and non-sensitive

## Run-Mode Checklist

Apply benchmark run mode before benchmark execution:

```bash
sudo ./scripts/security_mode.sh run-mode
export DELTA_BENCH_EGRESS_POLICY_SHA256="$(nft list ruleset | sha256sum | awk '{print $1}')"
./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy
```

In run mode, update/scan/log-heavy services configured by `security_mode.sh` are paused to reduce benchmark noise.

Switch back for maintenance operations:

```bash
sudo ./scripts/security_mode.sh maintenance-mode
```

Use maintenance mode for patching, scans, snapshots, and key rotation.

## Compare Preflight Enforcement

`compare_branch.sh` can enforce security preflight checks directly:

```bash
./scripts/compare_branch.sh \
  --enforce-run-mode \
  --require-no-public-ipv4 \
  --require-egress-policy \
  --backend-profile s3_locking_vultr \
  --runner all \
  main <candidate_ref> all
```

## Backend Profile and Secret Handling

- keep repeatable object-store + lock-table defaults in `backends/s3_locking_vultr.env`
- override sensitive or ephemeral values at execution time with `--storage-option KEY=VALUE`

## Provisioning Controls

Use `scripts/provision_runner.sh` for Terraform orchestration.

Guardrails:

- `rotate-runner` and `destroy` require CI context (`CI=true`) unless breakglass override is used
- `rotate-runner` and `destroy` require two distinct approver IDs via environment variables
- `rotate-runner` and `destroy` require `DELTA_BENCH_APPROVAL_EVIDENCE_FILE` pointing to immutable approval evidence
- API ACL allowlist must exclude `0.0.0.0/0` and `::/0`

## Result Integrity Metadata

Benchmark result `context` may capture fidelity and hardening metadata:

- `image_version`
- `hardening_profile_id`
- `hardening_profile_sha256`
- `cpu_model`
- `cpu_microcode`
- `kernel`
- `boot_params`
- `cpu_steal_pct`
- `numa_topology`
- `egress_policy_sha256`
- `run_mode`
- `maintenance_window_id`

## Related Guides

- [User Guide](user-guide.md)
- [Architecture](architecture.md)
