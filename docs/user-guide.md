# User Guide

This guide covers day-to-day use of the benchmark harness.

## Local setup

Prepare a managed `delta-rs` checkout and sync the harness:

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
```

Use an existing local `delta-rs` clone (instead of managed `.delta-rs-under-test`):

```bash
DELTA_RS_DIR=/path/to/your/delta-rs \
DELTA_BENCH_EXEC_ROOT=/path/to/your/delta-rs \
./scripts/sync_harness_to_delta_rs.sh

DELTA_RS_DIR=/path/to/your/delta-rs \
DELTA_BENCH_EXEC_ROOT=/path/to/your/delta-rs \
./scripts/bench.sh doctor
```

## Generate fixtures and run suites

Generate deterministic fixture data:

```bash
./scripts/bench.sh data --dataset-id tiny_smoke --seed 42
```

Run benchmark suites:

```bash
./scripts/bench.sh run --suite all --runner all --dataset-id tiny_smoke --warmup 1 --iters 5 --label local
```

Outputs are written to `results/<label>/<suite>.json`.

## DuckDB-backed TPC-DS fixture profile (`tpcds_duckdb`)

Use this dataset selector when you want `fixtures/<scale>/tpcds/store_sales` generated from
DuckDB `tpcds` extension data (`dsdgen`) instead of synthetic mapping.

Requirements:

- `python3`
- `duckdb` Python package (`pip install duckdb`)

Example:

```bash
./scripts/bench.sh data --dataset-id tpcds_duckdb --seed 42
./scripts/bench.sh run --suite tpcds --runner rust --dataset-id tpcds_duckdb --warmup 1 --iters 1 --label tpcds-duckdb-smoke
```

Runtime knobs:

- `DELTA_BENCH_DUCKDB_PYTHON` (default: `python3`)
- `DELTA_BENCH_TPCDS_DUCKDB_SCRIPT` (optional script override, useful for smoke tests)
- `DELTA_BENCH_TPCDS_DUCKDB_TIMEOUT_MS` (default: `600000`)

## Marketplace datasets (document-only path)

For externally provisioned Delta tables (for example Marketplace-delivered TPC-DS data), place or
copy those tables under the fixture roots expected by suite registration (for TPC-DS this is
`fixtures/<scale>/tpcds/<table_name>`). This repository does not currently automate Marketplace
ingestion.

## Compare two refs

Run sequential base-vs-candidate comparison using branch names:

```bash
./scripts/compare_branch.sh main <candidate_ref> all
```

`<candidate_ref>` must be a real branch in `.delta-rs-under-test`.
If unsure, run `git -C .delta-rs-under-test branch -a` first.

Pin exact commits to avoid ref drift during long runs:

```bash
./scripts/compare_branch.sh \
  --base-sha 5a0c8d7f3f2d9d42fdd9414f1ce2af319e0c52e1 \
  --candidate-sha 8c6170f1de4af5e2d3336b4fce8a9896af4d9b90 \
  all
```

The compare workflow will:

1. Update `.delta-rs-under-test`.
2. Sync this repo's harness into the `delta-rs` workspace.
3. Benchmark the base ref.
4. Benchmark the candidate ref.
5. Print a markdown comparison table.

Useful tuning options:

- `BENCH_TIMEOUT_SECONDS` (default `3600`) caps each `bench.sh` step runtime.
- `BENCH_RETRY_ATTEMPTS` (default `2`) retries transient failures.
- `BENCH_RETRY_DELAY_SECONDS` (default `5`) sets retry delay.
- `--noise-threshold` controls compare sensitivity.
- `--aggregation` controls representative sample selection (`min`, `median`, `p95`; default `median`) and is forwarded to `compare.py`.
- `cd python && python3 -m delta_bench_compare.compare ... --include-metrics` appends per-case metric columns.

## Object-store mode

Local is the default backend. To run fixture-backed suites against object storage:

```bash
./scripts/bench.sh data \
  --dataset-id medium_selective \
  --seed 42 \
  --storage-backend s3 \
  --storage-option table_root=s3://bench-bucket/delta-bench \
  --storage-option AWS_REGION=us-east-1

./scripts/bench.sh run \
  --suite all \
  --runner all \
  --dataset-id medium_selective \
  --warmup 1 \
  --iters 2 \
  --label s3-smoke \
  --storage-backend s3 \
  --storage-option table_root=s3://bench-bucket/delta-bench \
  --storage-option AWS_REGION=us-east-1
```

Notes:

- For non-local backends, `--storage-option table_root=...` is required.
- Local fixture cache (`fixtures/<scale>/rows.jsonl` and `fixtures/<scale>/manifest.json`) is unchanged.
- The `write` suite currently supports only local storage; non-local backends return explicit case failures.
- The `delete_update` suite seeds isolated remote tables per iteration to keep DML benchmark runs independent.

Workflow mode storage configuration:

- Optional repository variable `BENCH_STORAGE_BACKEND` (`local` or `s3`)
- Optional multi-line repository variable `BENCH_STORAGE_OPTIONS` (one `KEY=VALUE` per line)
- Optional repository variable `BENCH_BACKEND_PROFILE` (profile name in `backends/*.env`)
- Optional repository variable `BENCH_RUNNER_MODE` (`rust`, `python`, or `all`)
- PR benchmark workflow compares immutable PR base/head SHAs to avoid moving branch refs during execution
- Benchmark workflow comments publish benchmark output without merge gating

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
  main <candidate_ref> all
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
- `result_hash`
- `schema_hash`

Optional case-level elapsed aggregates (`cases[].elapsed_stats`):

- `min_ms`
- `max_ms`
- `mean_ms`
- `median_ms`
- `stddev_ms`
- `cv_pct`

See [architecture.md](architecture.md) for schema details and execution context.

## Useful CLI commands

```bash
cargo run -p delta-bench -- --help
cargo run -p delta-bench -- list all
cargo run -p delta-bench -- data --dataset-id tiny_smoke --seed 42
cargo run -p delta-bench -- run --target all --runner all --dataset-id tiny_smoke --warmup 1 --iterations 5
cargo run -p delta-bench -- data --dataset-id tpcds_duckdb --seed 42
cargo run -p delta-bench -- doctor
```

Run against the managed `delta-rs` checkout workspace:

```bash
DELTA_BENCH_EXEC_ROOT="$(pwd)/.delta-rs-under-test" \
DELTA_RS_DIR="$(pwd)/.delta-rs-under-test" \
./scripts/bench.sh doctor
```
