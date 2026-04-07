# Getting Started

This guide walks you through setting up the benchmark harness, running your first benchmark, and understanding the output. By the end you will have a working local setup and a set of JSON results you can inspect.

## Table of Contents

- [How the Harness Works](#how-the-harness-works)
- [Prerequisites](#prerequisites)
- [Your First Benchmark Run](#your-first-benchmark-run)
- [Choosing a Dataset](#choosing-a-dataset)
- [Choosing a Backend](#choosing-a-backend)
- [Cleanup](#cleanup)
- [Troubleshooting](#troubleshooting)
- [Next Steps](#next-steps)

## How the Harness Works

This repository is a standalone benchmark harness that wraps [delta-rs](https://github.com/delta-io/delta-rs). It manages a mutable execution checkout at `.delta-rs-under-test/`, a clean source checkout for compare pinning at `.delta-rs-source/`, generates deterministic test data (fixtures), runs benchmark suites against prepared checkouts, and writes structured JSON results.

You never need to modify delta-rs source from this repo. The harness syncs itself into the mutable delta-rs workspace, runs benchmarks there, and collects results back here. Compare workflows resolve immutable refs through the clean source checkout so `.delta-rs-under-test/` can carry synced harness files without breaking SHA pinning.

The main scripts are:

| Script                        | Purpose                                                    |
| ----------------------------- | ---------------------------------------------------------- |
| `prepare_delta_rs.sh`         | Clones or updates the delta-rs checkout                    |
| `sync_harness_to_delta_rs.sh` | Copies benchmark crate and configs into the checkout       |
| `bench.sh`                    | Generates fixtures and runs benchmark suites               |
| `compare_branch.sh`           | Runs benchmarks against two revisions, compares them, and writes compare artifacts |

Branch compare writes an automation-friendly bundle under `results/compare/<suite>/<base>__<candidate>/`, including `summary.md`, versioned `comparison.json`, `hash-policy.txt`, and `manifest.json`. Immutable refs are resolved through `DELTA_RS_SOURCE_DIR` (default: `.delta-rs-source/`). If a trusted immutable SHA lives on a fork remote, pass `--candidate-fetch-url <clone-url>` or `--base-fetch-url <clone-url>` and prefer the full 40-character SHA.

## Prerequisites

### Managed checkout (recommended)

The harness can manage its own delta-rs clone. This is the simplest way to get started:

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
```

The first command clones delta-rs into `.delta-rs-under-test/`. The second copies the benchmark crate and configuration into that workspace so Cargo can find it. `compare_branch.sh` creates a separate clean source checkout at `.delta-rs-source/` on demand unless you override `DELTA_RS_SOURCE_DIR`.

### Bring your own delta-rs clone

If you already have a local delta-rs checkout you want to benchmark against, point the harness at it:

```bash
DELTA_RS_DIR=/path/to/your/delta-rs \
DELTA_BENCH_EXEC_ROOT=/path/to/your/delta-rs \
./scripts/sync_harness_to_delta_rs.sh

DELTA_RS_DIR=/path/to/your/delta-rs \
DELTA_BENCH_EXEC_ROOT=/path/to/your/delta-rs \
./scripts/bench.sh doctor
```

Relative `DELTA_BENCH_FIXTURES` and `DELTA_BENCH_RESULTS` values still stay anchored to this harness repository, even when `DELTA_BENCH_EXEC_ROOT` points at a separate delta-rs checkout. Use absolute paths if you want fixtures or results to live somewhere else.

If you want compare pinning to reuse a specific clean clone, set `DELTA_RS_SOURCE_DIR` to that checkout. Keep it unsynced and disposable; the harness uses it only to resolve immutable refs and seed per-SHA compare worktrees.

### Python interop dependencies

If you plan to run `interop_py` cases (or use `--suite all --runner all`), install Python dependencies in the interpreter used by `DELTA_BENCH_INTEROP_PYTHON` (defaults to `python3`):

```bash
python3 -m pip install -r python/requirements-audit.txt
```

Without these packages, interop cases are classified as `expected_failure` instead of `supported`. `./scripts/bench.sh doctor` now reports missing interop dependencies and an install hint.

### Health check

Run `doctor` at any time to verify that the workspace is wired up correctly:

```bash
./scripts/bench.sh doctor
```

This checks that the delta-rs checkout exists, the harness is synced, Cargo can resolve the benchmark crate, and whether Python interop dependencies are available.

### Local CI baseline

Before you push changes, run the same baseline checks enforced by `.github/workflows/ci.yml`:

```bash
cargo test --locked
(cd python && python3 -m pytest -q tests)
```

Treat these as the minimum regression screen before sending a PR. GitHub-hosted CI also runs smoke and correctness lanes; self-hosted workflows handle macro perf, decision compare, and longitudinal automation.

## Your First Benchmark Run

### Step 1: Generate fixture data

Fixtures are deterministic test datasets generated from a seed. They provide consistent input data so benchmark results are reproducible.

```bash
./scripts/bench.sh data --dataset-id tiny_smoke --seed 42
```

This creates Delta tables under `fixtures/sf1/` including narrow sales tables, partitioned tables, merge targets, and other suite-specific data. The `tiny_smoke` dataset is small and fast to generate, making it ideal for your first run.

### Step 2: Run the benchmarks

```bash
./scripts/bench.sh run \
  --suite all \
  --runner all \
  --lane smoke \
  --dataset-id tiny_smoke \
  --warmup 1 \
  --iters 5 \
  --label local
```

This runs the smoke lane across every suite (scan, write, merge, delete_update, metadata, optimize_vacuum, tpcds, interop_py) using both Rust and Python runners. Smoke runs are validation-only — they confirm each case is operational but do not produce performance data.

If you want trusted semantic validation for the correctness-backed suites (`write`, `delete_update`, `merge`, `metadata`, `optimize_vacuum`, `interop_py`), switch to the correctness lane:

```bash
./scripts/bench.sh run --suite write --runner rust --lane correctness --mode assert --dataset-id tiny_smoke --label assert-correctness
```

If you switch those same suites to `--lane macro`, the workload still runs, but the result is marked validation-only (`perf_status=validation_only`) so compare/reporting will not treat it as trusted perf evidence.

### Step 3: Read the output

You should see two things:

1. **Terminal summary table** showing each case with its median time, status, and key metrics.
2. **JSON result files** at `results/local/<suite>.json` containing full schema v5 results with lane, benchmark mode, compatibility identity, and per-run summaries.

The JSON files are the primary output. They include context metadata (host, git SHA, timestamp), per-case outcomes, and per-sample metrics like `rows_processed`, `bytes_processed`, and timing statistics. See [reference.md](reference.md) for the complete schema.

To suppress the terminal table, pass `--no-summary-table`. Rust compiler warnings are suppressed by default; set `DELTA_BENCH_SUPPRESS_RUST_WARNINGS=0` to re-enable them.

## Choosing a Dataset

Each dataset ID controls which fixtures are generated and at what scale. The seed ensures deterministic data regardless of when or where you run it.

| Dataset ID         | Scale            | Description                                                                                  |
| ------------------ | ---------------- | -------------------------------------------------------------------------------------------- |
| `tiny_smoke`       | sf1 (10K rows)   | Minimal smoke test. Fast to generate, good for validating your setup.                        |
| `medium_selective` | sf10 (100K rows) | Realistic workloads with selective query patterns.                                           |
| `small_files`      | sf1 (10K rows)   | Generates many small files for optimize/vacuum testing.                                      |
| `many_versions`    | sf1 (10K rows)   | Creates 12 commits to build a version history for time-travel tests.                         |
| `tpcds_duckdb`     | sf1 (10K rows)   | TPC-DS `store_sales` table sourced from DuckDB. Requires `python3` and `pip install duckdb`. Used by the trusted self-hosted `pr-tpcds` contract. |

See [reference.md](reference.md#datasets-and-scales) for scale factors, fixture profiles, and fixture table details.

For trusted self-hosted `pr-tpcds` runs, pre-provision the fixture root on every runner before collecting evidence. The expected path is `/var/lib/delta-bench/fixtures/sf1/tpcds/store_sales`, and `tpcds_q72` remains outside the PR decision surface.

## Choosing a Backend

### Local (default)

Local storage is the default and requires no configuration. Fixture data lives on disk under `fixtures/` and benchmark results write to `results/`. This gives the fastest feedback loop for development.

### Object-store (S3)

For benchmarking against remote storage, pass `--storage-backend s3` with the required `table_root` option:

```bash
./scripts/bench.sh data \
  --dataset-id medium_selective \
  --seed 42 \
  --storage-backend s3 \
  --storage-option table_root=s3://bench-bucket/delta-bench \
  --storage-option AWS_REGION=us-east-1

./scripts/bench.sh run \
  --suite scan \
  --runner all \
  --lane macro \
  --dataset-id medium_selective \
  --warmup 1 \
  --iters 2 \
  --label s3-smoke \
  --storage-backend s3 \
  --storage-option table_root=s3://bench-bucket/delta-bench \
  --storage-option AWS_REGION=us-east-1
```

Backend configuration can also be set through environment variables or backend profiles:

| Variable                | Description                        |
| ----------------------- | ---------------------------------- |
| `BENCH_STORAGE_BACKEND` | `local` or `s3`                    |
| `BENCH_STORAGE_OPTIONS` | Multi-line `KEY=VALUE` pairs       |
| `BENCH_BACKEND_PROFILE` | Profile name from `backends/*.env` |
| `BENCH_RUNNER_MODE`     | `rust`, `python`, or `all`         |

Notes:

- The `--storage-option table_root=...` flag is required for non-local backends.
- Local fixture cache (`fixtures/<scale>/rows.jsonl`, `fixtures/<scale>/manifest.json`) is unchanged regardless of backend.
- The `write` suite currently supports only local storage.
- The `delete_update` suite seeds isolated remote tables per iteration to keep DML runs independent.
- GitHub-hosted CI runs smoke and correctness lanes only. Self-hosted workflows run `--lane macro` on curated `scan` cases and are the only automated path for macro perf or longitudinal claims.
- Remote candidate/manual evidence surfaces are declared in `bench/evidence/registry.yaml` instead of ad hoc shell flags. The current first-class S3 surfaces are `scan_s3`, `delete_update_perf_s3`, `merge_perf_s3`, `optimize_perf_s3`, and `metadata_perf_s3`.
- Their backing methodology profiles (`scan-s3-candidate`, `write-perf-s3-candidate`, `delete-update-perf-s3-candidate`, `merge-perf-s3-candidate`, `optimize-perf-s3-candidate`, and `metadata-perf-s3-candidate`) pin `storage_backend=s3` and `backend_profile=s3_locking_vultr`. The matching `s3-candidate-manual` pack batches the declared ready-for-operator surfaces through the same compare/profile contract used by the rest of the harness.
- `write_perf_s3` is declared via `write-perf-s3-candidate` but remains explicitly gated and is intentionally excluded from `s3-candidate-manual` until non-local write throughput evidence is closed out.

## Cleanup

The `cleanup_local.sh` script removes accumulated fixtures, results, and checkout artifacts, including cached per-SHA compare worktrees under `.delta-bench-compare-checkouts/`. It is **dry-run by default** and will not delete anything unless you pass `--apply`.

Preview what would be cleaned:

```bash
./scripts/cleanup_local.sh --results
```

Apply targeted cleanup:

```bash
./scripts/cleanup_local.sh --apply --results --keep-last 5 --older-than-days 14
./scripts/cleanup_local.sh --apply --compare-checkouts --keep-last 5
./scripts/cleanup_local.sh --apply --fixtures
./scripts/cleanup_local.sh --apply --delta-rs-source
./scripts/cleanup_local.sh --apply --delta-rs-under-test
```

| Flag                    | What it targets                                        |
| ----------------------- | ------------------------------------------------------ |
| `--results`             | JSON result files under `results/`                     |
| `--compare-checkouts`   | Cached compare worktrees under `.delta-bench-compare-checkouts/` |
| `--fixtures`            | Generated fixture data under `fixtures/`               |
| `--delta-rs-source`     | The clean source checkout used for compare pinning     |
| `--delta-rs-under-test` | The managed delta-rs checkout                          |
| `--keep-last N`         | Retain the N most recent result or compare-checkout entries |
| `--older-than-days N`   | Only remove items older than N days                    |
| `--allow-outside-root`  | Allow cleanup of results stored outside this repo root |

## Troubleshooting

**Workspace looks wrong or builds fail:**

```bash
./scripts/bench.sh doctor
```

This diagnoses common issues: missing checkout, un-synced harness, Cargo resolution failures.

**Local compare runs fail early with a disk-space error:**

`compare_branch.sh` now performs a local-only free-space preflight before it starts preparing compare checkouts. By default it requires at least `20` GiB free under the repo root and fails closed when that floor is missed. Override the floor with `DELTA_BENCH_MIN_FREE_GB` only when you have a tighter local contract and understand the tradeoff.

For local branch comparisons, standardize Cargo artifacts in one shared target directory:

```bash
export CARGO_TARGET_DIR="$PWD/target"
```

If you are close to the free-space floor, clear stale `target/` trees and cached compare worktrees before retrying:

```bash
./scripts/cleanup_local.sh --apply --compare-checkouts
rm -rf target
```

**Need help with a specific command:**

| Script                  | Help command                             |
| ----------------------- | ---------------------------------------- |
| `bench.sh`              | `./scripts/bench.sh --help`              |
| `compare_branch.sh`     | `./scripts/compare_branch.sh --help`     |
| `cleanup_local.sh`      | `./scripts/cleanup_local.sh --help`      |
| `longitudinal_bench.sh` | `./scripts/longitudinal_bench.sh --help` |

**Noisy Rust warnings in output:**

Rust compiler warnings are suppressed by default. To re-enable them:

```bash
export DELTA_BENCH_SUPPRESS_RUST_WARNINGS=0
```

## Next Steps

- **Compare your branch against main** -- see [Comparing Branches](comparing-branches.md) for the primary contributor workflow.
- **Track performance over time** -- see [Longitudinal Benchmarking](longitudinal.md) for regression detection across revisions.
- **Look up suites, metrics, flags** -- see [Reference](reference.md) for the complete glossary and configuration catalog.
- **Understand the internals** -- see [Architecture](architecture.md) for component boundaries and data flow.
