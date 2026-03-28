# Architecture

How the benchmark harness is structured, how data flows through it, and what controls ensure reproducible results.

## Table of Contents

- [Key Concepts](#key-concepts)
- [Components](#components)
- [Data Flow](#data-flow)
- [Longitudinal State and Storage](#longitudinal-state-and-storage)
- [Result Schema v4](#result-schema-v4)
- [Benchmark Coverage](#benchmark-coverage)
- [Reproducibility Controls](#reproducibility-controls)
- [Advanced Fixture Profiles](#advanced-fixture-profiles)

## Key Concepts

A quick orientation on the most important terms. See [Reference](reference.md#glossary) for the full glossary.

| Term | Definition |
|---|---|
| **Suite** | A named group of benchmark cases testing one type of Delta operation (e.g., `scan`, `merge`). |
| **Case** | An individual benchmark within a suite, run for warmup + measured iterations. |
| **Runner** | Execution lane: `rust`, `python`, or `all`. |
| **Fixture** | Deterministic test data generated from a seed. Produces Delta tables and row snapshots. |
| **Schema v4** | The normalized JSON result format used by authoritative benchmark output. |
| **Manifest** | A YAML file declaring which cases to execute and what assertions to validate. |

## Components

The harness is organized into three layers: execution, comparison/analysis, and automation.

### Execution

| Component | Description |
|---|---|
| `crates/delta-bench` | Rust CLI and benchmark execution engine. Generates fixtures, runs suites, writes results. |
| `bench/manifests/*.yaml` | Benchmark catalogs declaring cases, runners, and assertions for execution planning. |
| `backends/*.env` | Backend profile defaults for storage configuration (S3, locking, region). |

### Comparison and analysis

| Component | Description |
|---|---|
| `python/delta_bench_compare` | Result comparison and rendering. Reads schema v3/v4 JSON, but decision mode requires schema v4 and strict compatibility identity. |
| `python/delta_bench_interop` | Python interop benchmark cases using pandas, polars, and pyarrow. |
| `python/delta_bench_tpcds` | DuckDB-backed `store_sales` fixture generation script for the `tpcds_duckdb` dataset. |

### Automation and workflows

| Component | Description |
|---|---|
| `scripts/prepare_delta_rs.sh` | Manages the delta-rs checkout at `.delta-rs-under-test`. |
| `scripts/sync_harness_to_delta_rs.sh` | Syncs benchmark crate and configs into the delta-rs workspace. |
| `scripts/bench.sh` | Wrapper for `delta-bench` subcommands (data, run, list, doctor). |
| `scripts/compare_branch.sh` | Multi-run base-vs-candidate orchestration with aggregation and reporting. |
| `scripts/security_mode.sh` | Toggles benchmark run mode vs maintenance mode on cloud runners. |
| `scripts/security_check.sh` | Preflight guardrails for mode, network, and egress policy. |
| `scripts/provision_runner.sh` | Terraform orchestration wrapper for runner provisioning. |
| `.github/workflows/ci.yml` | Enforces the shared Rust/Python test baseline plus dependency audit jobs on pushes and pull requests. |
| `.github/workflows/benchmark*.yml`, `.github/workflows/longitudinal-*.yml` | Self-hosted benchmark workflows that enforce runner preflight before branch comparison or `run-matrix`. |

## Data Flow

Benchmark execution follows this pipeline:

1. **Fixture generation.** `delta-bench data` generates deterministic Delta tables under `fixtures/<scale>/`. This includes narrow sales tables, partitioned tables, merge targets, and suite-specific fixtures. JSON row snapshots (`rows.jsonl`) and a manifest (`manifest.json`) are written alongside the tables.

2. **TPC-DS fixtures (optional).** For `dataset_id=tpcds_duckdb`, the `store_sales` table is sourced from DuckDB's `tpcds` extension, exported through CSV, and written as a Delta table.

3. **Suite execution.** `delta-bench run` resolves runner mode from manifest-planned cases and executes Rust suites directly and Python interop cases via subprocess. `bench.sh` defaults to the smoke lane; explicit `--lane correctness` is the trusted semantic-validation path for the stateful Rust suites; `--lane macro` is the performance lane for macro-safe cases.

4. **Result output.** Each suite writes a schema v4 JSON file to `results/<label>/<suite>.json`. Context now carries lane, fixture recipe, harness revision, and fidelity identity. Cases also carry run summaries and compatibility keys. Correctness-tagged cases requested in macro lane remain operationally runnable but are emitted with `perf_valid=false` so they cannot be compared or reported as trusted perf evidence.

5. **Comparison (optional).** `compare.py` reads baseline and candidate JSON files, rejects invalid or mismatched contexts, and supports two modes: exploratory comparison on representative samples and decision comparison on run-level summaries with schema v4 compatibility checks and explicit fail-on exit policy.

6. **Security validation.** `security_check.sh` validates fidelity invariants (run mode, network, egress). Self-hosted GitHub Actions workflows enforce this preflight before branch comparison and longitudinal execution. Current perf automation is macro-only and curated to `scan`; PR comments split into exploratory (`run benchmark scan`) and decision (`run benchmark decision scan`) paths.

7. **Report output.** The compare workflow produces grouped text output. `compare.py` also supports markdown output for CI integration.

8. **Longitudinal matrix checkpointing (optional).** `run-matrix` writes `matrix-state.json` through an atomic temp-file replace. The state file records per-cell progress plus a configuration fingerprint so resume only happens against the same suite/scale/lane/output contract.

9. **Longitudinal ingest, reporting, and retention (optional).** `ingest-results` normalizes schema v4 suite outputs into a SQLite store. Reporting keys off explicit stored compatibility identity fields plus `compatibility_key` instead of just `suite/scale/case`, and the pipeline rejects legacy `rows.jsonl` / `index.json`-only stores to avoid silent split state.

10. **Criterion decision lane (optional).** `cargo bench -p delta-bench --bench scan_phase_bench` runs phase-isolated microbenchmarks for PR-sensitive scan paths using Criterion `iter_batched` loops so setup stays outside the timed phase.

Marketplace datasets are a document-only path: place externally provisioned Delta tables under the expected `fixtures/<scale>/...` roots.

## Longitudinal State and Storage

The longitudinal pipeline persists two control-plane artifacts:

| Artifact | Format | Purpose |
|---|---|---|
| `matrix-state.json` | JSON | Resume ledger for `(revision, suite, scale)` cells. Written atomically and guarded by a stored configuration fingerprint. |
| `store.sqlite3` | SQLite | Normalized time-series store for ingested runs and case rows. Reporting and retention use the same database, and ingest deduplicates by run id. |

If a store directory still contains only legacy `rows.jsonl` or `index.json` artifacts, the current pipeline fails fast instead of silently treating that state as empty.

## Result Schema v4

Benchmark results use a normalized JSON format with three top-level sections: `context` (metadata about the run), `cases` (per-case outcomes and samples), and a `schema_version` field.

Each case contains an array of `samples`, where each sample captures the elapsed time and optional metrics for one measured iteration. Case-level `elapsed_stats` are only populated when `perf_valid=true`.

For the complete field-by-field listing of all context, fidelity, case-level, and sample-level fields, see [Reference](reference.md#result-schema-v4).

### Source mapping highlights

Different suites populate different subsets of the metrics:

| Suite | Key metrics captured |
|---|---|
| `scan` | `files_scanned`, `files_pruned`, `bytes_scanned`, `scan_time_ms` |
| `merge` | `files_scanned`, `files_pruned`, `scan_time_ms`, `rewrite_time_ms` |
| `optimize_vacuum` (optimize cases) | `files_scanned` (considered), `files_pruned` (skipped) |
| `concurrency` | `operations`, nested `metrics.contention` counters such as `ops_succeeded`, `conflict_delete_read`, `conflict_delete_delete`; `table_version` is only meaningful for shared-table cases |

## Benchmark Coverage

The harness covers these operation categories with specific contrast cases:

- **scan** includes pruning contrast: `scan_pruning_hit` vs `scan_pruning_miss` to measure the impact of partition pruning.
- **merge** includes a localized partition-aware case: `merge_localized_1pct` tests merge performance when a partition predicate narrows the scan scope.
- **optimize_vacuum** includes noop-vs-heavy contrast: `optimize_noop_already_compact` vs `optimize_heavy_compaction` to measure compaction overhead when there is nothing to do vs aggressive compaction.
- **concurrency** includes both parallel and contended workflows: fresh-path table creation, concurrent append, DML-vs-compaction races, and overlapping optimize operations. The contended cases aggregate independent fixture copies, so they intentionally leave `table_version` unset.

For the complete list of all 40 benchmark cases across 9 suites, see [Reference](reference.md#benchmark-suites-and-cases).

## Reproducibility Controls

These mechanisms ensure that benchmark results are comparable across runs:

- **Deterministic fixtures.** Seed-based data generation produces identical tables regardless of when or where you run.
- **Managed checkout locking.** Prepare/compare flows serialize access to `.delta-rs-under-test` so concurrent control-plane actions cannot corrupt the managed checkout.
- **Deterministic manifest ordering.** The `core-rust` and `core-python` manifests define a fixed case execution order.
- **Single-machine comparisons.** Branch comparisons run both refs on the same hardware to eliminate machine-to-machine variance.
- **Prewarm runs.** Optional unreported iterations stabilize caches and thermal state before measurement begins.
- **Multi-run aggregation.** Multiple measured runs per ref are aggregated (default: median) before change classification.
- **Configurable run order.** `base-first`, `candidate-first`, or `alternate` ordering to reduce systematic bias from execution order.
- **Stable thresholds.** Default no-change threshold of `0.05` (5%) prevents false positives from normal measurement noise.
- **Explicit run mode and network guardrails.** Cloud runners can enforce benchmark mode, no-public-IPv4, and egress-policy checks before self-hosted workloads start.
- **Durable longitudinal checkpoints.** Matrix state writes use atomic replacement plus fsync so resume metadata survives process interruption.
- **Configuration fingerprinting.** Longitudinal resume state is bound to the original suite/scale/warmup/iteration/output configuration and fails closed on mismatches.

## Advanced Fixture Profiles

### DuckDB-backed TPC-DS (`tpcds_duckdb`)

Use this dataset when you want TPC-DS `store_sales` data generated by DuckDB's `dsdgen` implementation rather than the standard synthetic generator.

Requirements:
- `python3`
- `duckdb` Python package (`pip install duckdb`)

```bash
./scripts/bench.sh data --dataset-id tpcds_duckdb --seed 42
./scripts/bench.sh run --suite tpcds --runner rust --dataset-id tpcds_duckdb --warmup 1 --iters 1 --label tpcds-smoke
```

Runtime configuration:

| Variable | Default | Description |
|---|---|---|
| `DELTA_BENCH_DUCKDB_PYTHON` | `python3` | Python executable for DuckDB |
| `DELTA_BENCH_TPCDS_DUCKDB_SCRIPT` | — | Override path to generation script |
| `DELTA_BENCH_TPCDS_DUCKDB_TIMEOUT_MS` | `600000` | Timeout for generation (10 minutes) |

### Marketplace datasets

Place externally provisioned Delta tables under the expected fixture roots (for TPC-DS: `fixtures/<scale>/tpcds/<table_name>`). This repository does not automate marketplace ingestion.
