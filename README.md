# delta-rs-benchmarking

Repo-first benchmark harness for `delta-rs` with manual branch comparison.

## Quickstart

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
./scripts/bench.sh data --scale sf1 --seed 42
./scripts/bench.sh run --suite all --scale sf1 --warmup 1 --iters 5 --label local
```

Results are written to `results/<label>/<suite>.json`.

## Compare two branches

```bash
./scripts/compare_branch.sh main fix/python-dv-bug-4235 all
```

This will:
1. Update `.delta-rs-under-test`
2. Sync this repo's harness into `delta-rs` workspace
3. Benchmark base branch in that workspace
4. Benchmark candidate branch in that workspace
5. Print a markdown comparison table

Tuning options:
- `BENCH_TIMEOUT_SECONDS` (default `3600`) to cap each `bench.sh` step runtime.
- `BENCH_RETRY_ATTEMPTS` (default `2`) for transient failures.
- `BENCH_RETRY_DELAY_SECONDS` (default `5`) between retry attempts.
- `--noise-threshold` to tune no-change classification in comparison output.
- `--storage-backend` and repeatable `--storage-option KEY=VALUE` to run fixture generation + suite execution against object storage.

To include scan/rewrite metric columns in compare output:

```bash
PYTHONPATH=python python3 -m delta_bench_compare.compare \
  results/base/read_scan.json \
  results/candidate/read_scan.json \
  --format markdown \
  --include-metrics
```

New optional sample metric fields emitted by benchmark suites:
- `files_scanned`
- `files_pruned`
- `bytes_scanned`
- `scan_time_ms`
- `rewrite_time_ms`

## Cloud/object-store mode

Local remains the default backend. To run fixture-backed suites against object storage:

```bash
./scripts/bench.sh data \
  --scale sf1 \
  --seed 42 \
  --storage-backend s3 \
  --storage-option table_root=s3://bench-bucket/delta-bench \
  --storage-option AWS_REGION=us-east-1

./scripts/bench.sh run \
  --suite optimize_vacuum \
  --scale sf1 \
  --warmup 1 \
  --iters 2 \
  --label wave2-s3 \
  --storage-backend s3 \
  --storage-option table_root=s3://bench-bucket/delta-bench \
  --storage-option AWS_REGION=us-east-1
```

Notes:
- For non-local backends, `--storage-option table_root=...` is required.
- Local fixture cache (`fixtures/<scale>/rows.jsonl` + `manifest.json`) is unchanged.
- `write` suite keeps local temp-table behavior in cloud mode.

Security/fidelity options:
- `--remote-runner <ssh-host>` to execute the full workflow on a dedicated Vultr runner.
- `--remote-root <path>` to set the benchmark repo root on the remote runner.
- `--enforce-run-mode` to require benchmark run mode marker (`/etc/delta-bench/security-mode`).
- `--require-no-public-ipv4` to fail if the runner has any public IPv4 address on interfaces.
- `--require-egress-policy` to require active `nftables` hash match against `DELTA_BENCH_EGRESS_POLICY_SHA256`.

Example:

```bash
./scripts/compare_branch.sh \
  --remote-runner bench-runner-01 \
  --remote-root /opt/delta-rs-benchmarking \
  --enforce-run-mode \
  --require-no-public-ipv4 \
  --require-egress-policy \
  main feature/merge-opt all

./scripts/compare_branch.sh \
  --remote-runner bench-runner-01 \
  --remote-root /opt/delta-rs-benchmarking \
  --storage-backend s3 \
  --storage-option table_root=s3://bench-bucket/delta-bench \
  --storage-option AWS_REGION=us-east-1 \
  main feature/merge-opt optimize_vacuum

./scripts/compare_branch.sh \
  --noise-threshold 0.05 \
  main feature/merge-opt all
```

Workflow mode storage configuration:
- Optional repository variable `BENCH_STORAGE_BACKEND` (`s3`, `gcs`, or `azure`)
- Optional multi-line repository variable `BENCH_STORAGE_OPTIONS` (one `KEY=VALUE` per line; for example `table_root=...`, `AWS_REGION=...`)

## Security operations

Switch benchmark isolation mode on the runner:

```bash
sudo ./scripts/security_mode.sh run-mode
sudo ./scripts/security_mode.sh maintenance-mode
```

Run security preflight checks directly:

```bash
export DELTA_BENCH_EGRESS_POLICY_SHA256="$(nft list ruleset | sha256sum | awk '{print $1}')"
./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy
```

Provisioning helper entrypoint for Terraform:

```bash
./scripts/provision_vultr.sh plan
./scripts/provision_vultr.sh apply
./scripts/provision_vultr.sh rotate-key
```

Destructive provisioning commands (`rotate-runner`, `destroy`) require CI context by default, two distinct approver IDs, and an immutable approval evidence file.

## CLI reference

```bash
cargo run -p delta-bench -- --help
cargo run -p delta-bench -- list all
cargo run -p delta-bench -- data --scale sf1 --seed 42
cargo run -p delta-bench -- run --target all --scale sf1 --warmup 1 --iterations 5 --storage-backend local
cargo run -p delta-bench -- doctor
```

Run against the managed delta-rs checkout workspace:

```bash
DELTA_BENCH_EXEC_ROOT="$(pwd)/.delta-rs-under-test" \
DELTA_RS_DIR="$(pwd)/.delta-rs-under-test" \
./scripts/bench.sh doctor
```

## Current scope

- Implemented suites: `read_scan`, `write`, `merge_dml`, `metadata`, `optimize_vacuum`
- Implemented: suites execute real `deltalake-core` operations (read provider scans, write builders, merge builders, metadata loads)
- Implemented: deterministic fixture generation + result schema v1
- Implemented: manual comparison workflow (`scripts/compare_branch.sh` + Python compare) with advisory output
- Implemented: Option B PR benchmark workflow (`.github/workflows/benchmark.yml`) with issue-comment trigger, role-based authorization, and serialized execution
- Limitation: workflow mode currently relies on `compare_branch.sh` branch names being available in the upstream `delta-rs` checkout; fork PR heads may require manual handling
