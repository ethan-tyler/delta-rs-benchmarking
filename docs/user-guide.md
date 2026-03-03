# User Guide

Task-first playbook for active contributors running and comparing `delta-rs` benchmarks.

## Prerequisites and Workspace Modes

Managed checkout mode (recommended):

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
```

Use an existing local `delta-rs` clone:

```bash
DELTA_RS_DIR=/path/to/your/delta-rs \
DELTA_BENCH_EXEC_ROOT=/path/to/your/delta-rs \
./scripts/sync_harness_to_delta_rs.sh

DELTA_RS_DIR=/path/to/your/delta-rs \
DELTA_BENCH_EXEC_ROOT=/path/to/your/delta-rs \
./scripts/bench.sh doctor
```

Quick health check:

```bash
./scripts/bench.sh doctor
```

## First Benchmark Run (Happy Path)

Generate deterministic fixture data:

```bash
./scripts/bench.sh data --dataset-id tiny_smoke --seed 42
```

Run all suites across Rust and Python lanes:

```bash
./scripts/bench.sh run --suite all --runner all --dataset-id tiny_smoke --warmup 1 --iters 5 --label local
```

Expected behavior:

- terminal includes per-case summary table
- detailed results land at `results/<label>/<suite>.json`
- for quieter logs, pass `--no-summary-table`
- Rust warnings are suppressed by default; set `DELTA_BENCH_SUPPRESS_RUST_WARNINGS=0` to re-enable

## Compare Workflows

### Branch-to-branch compare

```bash
./scripts/compare_branch.sh main <candidate_ref> all
```

`<candidate_ref>` must exist in `.delta-rs-under-test`.
Use `git -C .delta-rs-under-test branch -a` to inspect available refs.

### Immutable SHA compare (recommended for long runs)

```bash
./scripts/compare_branch.sh \
  --base-sha 5a0c8d7f3f2d9d42fdd9414f1ce2af319e0c52e1 \
  --candidate-sha 8c6170f1de4af5e2d3336b4fce8a9896af4d9b90 \
  all
```

### Current checkout commit vs latest upstream main

```bash
./scripts/compare_branch.sh --current-vs-main all
```

### What compare does

1. updates `.delta-rs-under-test`
2. syncs this harness into the `delta-rs` workspace
3. refreshes deterministic fixtures with the base revision
4. optionally runs unreported prewarm iterations for both refs
5. executes measured benchmark runs for base and candidate in configured order
6. aggregates each side into one JSON payload from all measured runs
7. prints grouped report sections: `Regressions`, `Improvements`, `Stable`, `Needs Attention`

### Compare tuning

- `BENCH_TIMEOUT_SECONDS` (default `3600`) caps each benchmark step
- `BENCH_RETRY_ATTEMPTS` (default `2`) retries transient failures
- `BENCH_RETRY_DELAY_SECONDS` (default `5`) controls retry delay
- `--noise-threshold` controls sensitivity
- `--aggregation min|median|p95` selects representative sample (default `median`)
- `--warmup` sets warmup iterations per case (default `2`)
- `--iters` sets measured iterations per case per run (default `9`)
- `--prewarm-iters` sets unreported prewarm iterations per ref (default `1`)
- `--compare-runs` sets measured runs per ref before aggregation (default `3`)
- `--measure-order` controls run ordering (`base-first`, `candidate-first`, `alternate`; default `alternate`)
- `cd python && python3 -m delta_bench_compare.compare ... --include-metrics` appends metric columns

### Reliable compare protocol (recommended)

For PR and release decisions, prefer this protocol:

1. use immutable commits (`--base-sha`, `--candidate-sha`) or `--current-vs-main`
2. run on an otherwise idle machine and keep backend/profile identical between refs
3. keep `--measure-order alternate` to reduce drift from cache/thermal effects
4. keep aggregation at `median` unless you are analyzing tail latency
5. treat `cv_pct > 10` as noisy and rerun with higher `--compare-runs` or `--iters`

Example decision-grade command:

```bash
./scripts/compare_branch.sh \
  --current-vs-main \
  --warmup 2 \
  --iters 9 \
  --prewarm-iters 1 \
  --compare-runs 3 \
  --measure-order alternate \
  --aggregation median \
  all
```

## Backend and Dataset Selection

### Local backend (default)

Use local backend for quickest contributor feedback loops.

### Object-store mode (S3)

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

- for non-local backends, `--storage-option table_root=...` is required
- local fixture cache (`fixtures/<scale>/rows.jsonl`, `fixtures/<scale>/manifest.json`) is unchanged
- `write` suite currently supports only local storage
- `delete_update` seeds isolated remote tables per iteration to keep DML runs independent

Workflow variables for backend mode:

- `BENCH_STORAGE_BACKEND` (`local` or `s3`)
- `BENCH_STORAGE_OPTIONS` (multi-line `KEY=VALUE`)
- `BENCH_BACKEND_PROFILE` (`backends/*.env` profile name)
- `BENCH_RUNNER_MODE` (`rust`, `python`, or `all`)

## Cleanup and Troubleshooting

### Safe cleanup defaults

`cleanup_local.sh` is dry-run by default and does nothing destructive unless `--apply` is set.
With `--apply`, deletion is restricted to this repo root unless `--allow-outside-root` is explicitly set.

Preview cleanup:

```bash
./scripts/cleanup_local.sh --results
```

Apply targeted cleanup:

```bash
./scripts/cleanup_local.sh --apply --results --keep-last 5 --older-than-days 14
./scripts/cleanup_local.sh --apply --fixtures
./scripts/cleanup_local.sh --apply --delta-rs-under-test
./scripts/cleanup_local.sh --apply --fixtures --delta-rs-under-test
./scripts/cleanup_local.sh --apply --results --allow-outside-root
```

### High-signal troubleshooting commands

```bash
./scripts/bench.sh --help
./scripts/compare_branch.sh --help
./scripts/cleanup_local.sh --help
./scripts/longitudinal_bench.sh --help
```

If the local workspace wiring looks wrong, run `./scripts/bench.sh doctor`.

## Advanced Topics

### DuckDB-backed TPC-DS fixture profile (`tpcds_duckdb`)

Use this selector when `fixtures/<scale>/tpcds/store_sales` should come from DuckDB `tpcds` extension data (`dsdgen`).

Requirements:

- `python3`
- `duckdb` Python package (`pip install duckdb`)

Example:

```bash
./scripts/bench.sh data --dataset-id tpcds_duckdb --seed 42
./scripts/bench.sh run --suite tpcds --runner rust --dataset-id tpcds_duckdb --warmup 1 --iters 1 --label tpcds-duckdb-smoke
```

Runtime knobs:

- `DELTA_BENCH_DUCKDB_PYTHON` (default `python3`)
- `DELTA_BENCH_TPCDS_DUCKDB_SCRIPT` (script override)
- `DELTA_BENCH_TPCDS_DUCKDB_TIMEOUT_MS` (default `600000`)

### Marketplace datasets (document-only path)

Place externally provisioned Delta tables under suite-expected fixture roots (for TPC-DS: `fixtures/<scale>/tpcds/<table_name>`).
This repository does not currently automate Marketplace ingestion.

### Remote runner + security preflight options

`compare_branch.sh` supports remote execution and preflight requirements:

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

### Metrics and schema reference

Sample-level metrics live at `cases[].samples[].metrics` and always include:

- `rows_processed`
- `bytes_processed`
- `operations`
- `table_version`

Optional suite-dependent fields include:

- `files_scanned`, `files_pruned`, `bytes_scanned`, `scan_time_ms`, `rewrite_time_ms`
- `result_hash`, `schema_hash`

Optional case-level elapsed aggregates at `cases[].elapsed_stats`:

- `min_ms`, `max_ms`, `mean_ms`, `median_ms`, `stddev_ms`, `cv_pct`

For complete schema and source mapping, use [architecture.md](architecture.md).

## Related Guides

- [Longitudinal CLI Guide](longitudinal-cli.md)
- [Longitudinal Runbook](longitudinal-runbook.md)
- [Security Runner Runbook](security-runner.md)
- [Architecture Reference](architecture.md)
