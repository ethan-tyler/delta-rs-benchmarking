# Cloud Runner Security and Fidelity Runbook

## Baseline assumptions

- Dedicated benchmarking cloud account or project.
- Single dedicated runner in private network topology.
- Runner has no public IPv4 address.
- Benchmark data is synthetic and non-sensitive.

## Operational modes

### Benchmark run mode

```bash
sudo ./scripts/security_mode.sh run-mode
export DELTA_BENCH_EGRESS_POLICY_SHA256="$(nft list ruleset | sha256sum | awk '{print $1}')"
./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy
```

In run mode, update/scan/log-heavy units configured by `security_mode.sh` are paused to reduce benchmark noise.

### Security maintenance mode

```bash
sudo ./scripts/security_mode.sh maintenance-mode
```

Use maintenance mode for patching, scans, snapshots, and key rotation.

## Preflight requirements before compare

`scripts/compare_branch.sh` can enforce preflight checks directly:

```bash
./scripts/compare_branch.sh \
  --enforce-run-mode \
  --require-no-public-ipv4 \
  --require-egress-policy \
  --backend-profile s3_locking_vultr \
  --runner all \
  main candidate-branch all
```

Backend profile guidance:
- Store repeatable object-store and lock-table defaults in `backends/s3_locking_vultr.env`.
- Override sensitive/ephemeral values with `--storage-option KEY=VALUE` at execution time.

## Provisioning controls

- Use `scripts/provision_runner.sh` for Terraform orchestration.
- `rotate-runner` and `destroy` require CI context by default (`CI=true`) unless breakglass override is set.
- `rotate-runner` and `destroy` require two distinct approver IDs via environment variables.
- `rotate-runner` and `destroy` require `DELTA_BENCH_APPROVAL_EVIDENCE_FILE` pointing to immutable approval evidence.
- API ACL allowlist must exclude `0.0.0.0/0` and `::/0`.

## Benchmark result integrity metadata

Each benchmark result context now captures optional fidelity fields:

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
