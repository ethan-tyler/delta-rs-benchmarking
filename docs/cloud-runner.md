# Cloud Runner

This guide covers running benchmarks on dedicated cloud infrastructure where noise isolation and security controls matter.

## When to Use a Cloud Runner

Local runs are fine for development feedback, but they are subject to interference from other processes, thermal throttling, and inconsistent hardware. Cloud runners provide a dedicated machine in a private network with reproducible system state, giving you the noise isolation needed for release decisions, longitudinal baselines, and reproducible CI results.

Use a cloud runner when benchmark accuracy matters more than convenience.

## Baseline Assumptions

The cloud runner setup assumes:

- A dedicated benchmarking cloud account or project
- A single dedicated runner in a private network topology
- The runner has no public IPv4 address
- Benchmark data is synthetic and non-sensitive

## Setting Up Run Mode

Run mode pauses update services, log collectors, and other system daemons that create background noise during benchmarks. Switch to run mode before executing benchmarks and back to maintenance mode when you need to patch, scan, or rotate credentials.

Apply benchmark run mode:

```bash
sudo ./scripts/security_mode.sh run-mode
export DELTA_BENCH_EGRESS_POLICY_SHA256="$(nft list ruleset | sha256sum | awk '{print $1}')"
./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy
```

The `security_check.sh` script validates that the runner is in the expected state before allowing benchmarks to proceed.

Switch back to maintenance mode for system operations:

```bash
sudo ./scripts/security_mode.sh maintenance-mode
```

## Preflight Enforcement on Compare

The `compare_branch.sh` script can enforce security and environment checks as part of the comparison workflow. This ensures benchmarks only run when the runner is in the correct state.

| Flag | Description |
|---|---|
| `--remote-runner <ssh-host>` | SSH target for remote execution |
| `--remote-root <path>` | Remote working directory on the runner |
| `--enforce-run-mode` | Require benchmark run mode to be active |
| `--require-no-public-ipv4` | Require the runner has no public IPv4 address |
| `--require-egress-policy` | Require a network egress policy is in place |
| `--backend-profile <name>` | Use a specific backend profile from `backends/` |

Example with all preflight checks enabled:

```bash
./scripts/compare_branch.sh \
  --remote-runner bench-runner-01 \
  --remote-root /opt/delta-rs-benchmarking \
  --enforce-run-mode \
  --require-no-public-ipv4 \
  --require-egress-policy \
  --backend-profile s3_locking_vultr \
  --runner all \
  main <candidate_ref> all
```

The self-hosted GitHub Actions workflows enforce the same contract:

- `benchmark.yml` passes `--enforce-run-mode`, `--require-no-public-ipv4`, and `--require-egress-policy` directly into `./scripts/compare_branch.sh`
- `benchmark-prerelease.yml` passes the same hardening flags into `./scripts/compare_branch.sh`
- `benchmark-nightly.yml` runs `./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy` before benchmark execution
- `longitudinal-nightly.yml` runs `./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy` before `run-matrix`
- `longitudinal-release-history.yml` runs `./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy` before `run-matrix`

Workflow variables for self-hosted runs:

- Required: `DELTA_BENCH_EGRESS_POLICY_SHA256` for the expected hash of the active nftables ruleset
- Optional: `BENCH_STORAGE_BACKEND` for benchmark jobs that read or write remote storage
- Optional: `BENCH_STORAGE_OPTIONS` for newline-delimited `KEY=VALUE` storage options
- Optional: `BENCH_BACKEND_PROFILE` for nightly and compare workflows
- Optional: `BENCH_RUNNER_MODE` for `benchmark.yml` runner selection (`rust`, `python`, or `all`)

## Backend Profile and Secret Handling

Backend profiles store repeatable object-store and lock-table defaults in `backends/<profile_name>.env`. Use these to avoid passing the same `--storage-option` flags every time.

- Keep stable configuration in the profile file (e.g., `backends/s3_locking_vultr.env`)
- Override sensitive or ephemeral values at execution time with `--storage-option KEY=VALUE`

## Provisioning Controls

The `scripts/provision_runner.sh` script wraps Terraform for runner provisioning with safety guardrails:

| Guardrail | Detail |
|---|---|
| CI context required | `rotate-runner` and `destroy` require `CI=true` unless breakglass override is used |
| Dual approver | Two distinct approver IDs via environment variables |
| Evidence file | `DELTA_BENCH_APPROVAL_EVIDENCE_FILE` must point to immutable approval evidence |
| ACL allowlist | Must exclude `0.0.0.0/0` and `::/0` (no open access) |

## Result Integrity Metadata

Benchmark results captured on cloud runners can include fidelity and hardening metadata in the `context` section of the schema v2 output. This covers image version, CPU model, kernel, NUMA topology, egress policy, and run mode. See the [Reference](reference.md#result-schema-v2) for the complete list of fidelity context fields.
