# User Guide

This guide covers day-to-day use of the benchmark harness.

## Local setup

Prepare a managed `delta-rs` checkout and sync the harness:

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
```

## Generate fixtures and run suites

Generate deterministic fixture data:

```bash
./scripts/bench.sh data --scale sf1 --seed 42
```

Run benchmark suites:

```bash
./scripts/bench.sh run --suite all --scale sf1 --warmup 1 --iters 5 --label local
```

Outputs are written to `results/<label>/<suite>.json`.

## Compare two branches

Run sequential base-vs-candidate comparison:

```bash
./scripts/compare_branch.sh main candidate-branch all
```

The compare workflow will:

1. Update `.delta-rs-under-test`.
2. Sync this repo's harness into the `delta-rs` workspace.
3. Benchmark the base branch.
4. Benchmark the candidate branch.
5. Print a markdown comparison table.

Useful tuning options:

- `BENCH_TIMEOUT_SECONDS` (default `3600`) caps each `bench.sh` step runtime.
- `BENCH_RETRY_ATTEMPTS` (default `2`) retries transient failures.
- `BENCH_RETRY_DELAY_SECONDS` (default `5`) sets retry delay.
- `--noise-threshold` controls compare sensitivity.
- `cd python && python3 -m delta_bench_compare.compare ... --include-metrics` appends per-case metric columns.

## Object-store mode

Local is the default backend. To run fixture-backed suites against object storage:

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
- Local fixture cache (`fixtures/<scale>/rows.jsonl` and `fixtures/<scale>/manifest.json`) is unchanged.
- The `write` suite keeps local temp table behavior in cloud mode.
- The `delete_update_dml` suite seeds isolated remote tables per iteration to keep DML benchmark runs independent.

Workflow mode storage configuration:

- Optional repository variable `BENCH_STORAGE_BACKEND` (`s3`, `gcs`, or `azure`)
- Optional multi-line repository variable `BENCH_STORAGE_OPTIONS` (one `KEY=VALUE` per line)
- Benchmark workflow comments are advisory and do not gate PR merge

## Security and remote runner options

`compare_branch.sh` supports dedicated remote execution and preflight checks:

- `--remote-runner <ssh-host>`
- `--remote-root <path>`
- `--enforce-run-mode`
- `--require-no-public-ipv4`
- `--require-egress-policy`

Example:

```bash
./scripts/compare_branch.sh \
  --remote-runner bench-runner-01 \
  --remote-root /opt/delta-rs-benchmarking \
  --enforce-run-mode \
  --require-no-public-ipv4 \
  --require-egress-policy \
  main candidate-branch all
```

For run-mode operations and provisioning constraints, use the dedicated runbook: [security-runner.md](security-runner.md).

## Metrics and schema

Each sample writes normalized metrics under `cases[].samples[].metrics`.

Always present:

- `rows_processed`
- `bytes_processed`
- `operations`
- `table_version`

Optional (suite-dependent):

- `files_scanned`
- `files_pruned`
- `bytes_scanned`
- `scan_time_ms`
- `rewrite_time_ms`

See [architecture.md](architecture.md) for schema details and execution context.

## Useful CLI commands

```bash
cargo run -p delta-bench -- --help
cargo run -p delta-bench -- list all
cargo run -p delta-bench -- data --scale sf1 --seed 42
cargo run -p delta-bench -- run --target all --scale sf1 --warmup 1 --iterations 5 --storage-backend local
cargo run -p delta-bench -- doctor
```

Run against the managed `delta-rs` checkout workspace:

```bash
DELTA_BENCH_EXEC_ROOT="$(pwd)/.delta-rs-under-test" \
DELTA_RS_DIR="$(pwd)/.delta-rs-under-test" \
./scripts/bench.sh doctor
```
