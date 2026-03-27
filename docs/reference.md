# Reference

Complete lookup reference for the delta-rs benchmarking harness. This page catalogs every suite, case, metric, CLI flag, environment variable, dataset, and schema field in one place.

## Table of Contents

- [Glossary](#glossary)
- [Benchmark Suites and Cases](#benchmark-suites-and-cases)
- [Metrics Reference](#metrics-reference)
- [CLI Commands and Flags](#cli-commands-and-flags)
- [Repository Verification Baseline](#repository-verification-baseline)
- [Environment Variables](#environment-variables)
- [Longitudinal State and Store](#longitudinal-state-and-store)
- [Datasets and Scales](#datasets-and-scales)
- [Fixture Tables](#fixture-tables)
- [Result Schema v2](#result-schema-v2)
- [Manifest Format](#manifest-format)
- [Backend Profiles](#backend-profiles)

## Glossary

| Term | Definition |
|---|---|
| **Suite** | A named group of benchmark cases that test a specific Delta Lake operation (e.g., `scan`, `write`, `merge`). |
| **Case** | An individual benchmark within a suite. Each case is run for a configured number of warmup + measured iterations. |
| **Runner** | The execution lane: `rust` (native Rust implementation), `python` (Python interop via pandas/polars/pyarrow), or `all`. |
| **Dataset** | A named fixture configuration that controls which tables are generated and at what size. Identified by `dataset_id`. |
| **Scale** | The size factor for fixture data: `sf1` (10K rows), `sf10` (100K rows), `sf100` (1M rows). |
| **Fixture** | Deterministic test data generated from a seed. Includes Delta tables, JSON row snapshots, and a manifest. |
| **Fixture profile** | Controls how fixtures are generated: `Standard` (normal), `ManyVersions` (12 commits for version history), `TpcdsDuckdb` (DuckDB TPC-DS source). |
| **Label** | A run identifier used in result paths (e.g., `results/<label>/<suite>.json`). Must match `[A-Za-z0-9._-]` and cannot be `.` or `..`. |
| **Schema v2** | The normalized JSON result format that all benchmark output follows. Includes context, cases, and per-sample metrics. |
| **Manifest** | A YAML or JSON file that declares which benchmark cases to execute and what assertions to validate. |
| **Backend profile** | A `.env` file under `backends/` with storage configuration defaults (S3 bucket, locking, region). |
| **Lane** | A release-tag track for longitudinal benchmarking (e.g., `rust` lane for `rust-v*` tags, `python` lane for `python-v*`). |
| **Matrix state** | The resumable JSON checkpoint written by `run-matrix`, including per-cell execution status and a configuration fingerprint. |
| **Longitudinal store** | The SQLite database (`store.sqlite3`) that holds normalized run metadata and case rows for ingest/report/prune. |
| **Fidelity** | System-level metadata (CPU model, kernel, run mode) captured alongside results to ensure reproducibility. |
| **cv_pct** | Coefficient of variation as a percentage. Measures result noise. Below 5% is good; above 10% warrants rerunning. |

### Label Contract

Labels are validated strictly at the Rust CLI boundary and sanitized in Bash/Python helper paths to produce Rust-valid labels.

| Input | Behavior |
|---|---|
| `local-main_20260325` | valid |
| `bench.v1` | valid |
| `""`, `.`, `..`, `bad/slash`, `bad space` | invalid |
| `feature/foo` | sanitizes to `feature_foo` |
| `___` | sanitizes to `label` |
| `...` | sanitizes to `...` |

## Benchmark Suites and Cases

### scan (5 cases)

Read operations testing full scans, projections, filters, and partition pruning.

| Case | Description | Key metrics |
|---|---|---|
| `scan_full_narrow` | Full table scan of the narrow sales table with all columns | files_scanned, bytes_scanned, rows_processed |
| `scan_projection_region` | Scan with column projection (region column only) | files_scanned, bytes_scanned, rows_processed |
| `scan_filter_flag` | Scan with a predicate filter on the flag column | files_scanned, bytes_scanned, rows_processed |
| `scan_pruning_hit` | Scan with a filter that prunes most partitions (high selectivity) | files_scanned, files_pruned, scan_time_ms |
| `scan_pruning_miss` | Scan with a filter that prunes no partitions (low selectivity) | files_scanned, files_pruned, scan_time_ms |

### write (3 cases)

Write operations testing append and overwrite patterns. Local storage only.

| Case | Description | Key metrics |
|---|---|---|
| `write_append_small` | Append a small batch (128 rows) to a new table | rows_processed, bytes_processed, operations |
| `write_append_large` | Append all rows to a new table | rows_processed, bytes_processed, operations |
| `write_overwrite` | Overwrite an existing table with all rows | rows_processed, bytes_processed, operations |

### delete_update (7 cases)

DML operations testing deletes and updates at varying selectivity and data layout.

| Case | Description | Key metrics |
|---|---|---|
| `delete_1pct_localized` | Delete 1% of rows within a single partition | rows_processed, files_scanned, rewrite_time_ms |
| `delete_5pct_scattered` | Delete 5% of rows scattered across small files | rows_processed, files_scanned, rewrite_time_ms |
| `delete_50pct_broad` | Delete 50% of rows across all partitions | rows_processed, files_scanned, rewrite_time_ms |
| `update_literal_1pct_localized` | Update 1% of rows with a literal value, partition-localized | rows_processed, files_scanned, rewrite_time_ms |
| `update_literal_5pct_scattered` | Update 5% of rows with a literal value, scattered across small files | rows_processed, files_scanned, rewrite_time_ms |
| `update_expr_50pct_broad` | Update 50% of rows with an expression, across all partitions | rows_processed, files_scanned, rewrite_time_ms |
| `update_all_rows_expr` | Update all rows with an expression (full table rewrite) | rows_processed, files_scanned, rewrite_time_ms |

### merge (6 cases)

Merge (upsert/delete) operations at varying match ratios and target configurations.

| Case | Description | Key metrics |
|---|---|---|
| `merge_delete_5pct` | Merge-delete matching 5% of rows in standard table | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |
| `merge_upsert_10pct_insert_10pct` | Upsert matching 10% + insert 10% new rows | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |
| `merge_upsert_10pct` | Upsert matching 10% of rows | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |
| `merge_upsert_50pct` | Upsert matching 50% of rows | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |
| `merge_upsert_90pct` | Upsert matching 90% of rows (near-full rewrite) | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |
| `merge_localized_1pct` | Partition-aware upsert matching 1% with partition predicate | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |

### metadata (2 cases)

Metadata operations testing table load and time-travel performance.

| Case | Description | Key metrics |
|---|---|---|
| `metadata_load` | Load table metadata from the transaction log | table_version, operations |
| `metadata_time_travel_v0` | Load table metadata at version 0 (time travel to initial commit) | table_version, operations |

### optimize_vacuum (5 cases)

Table maintenance operations: file compaction and vacuum.

| Case | Description | Key metrics |
|---|---|---|
| `optimize_compact_small_files` | Compact small files into larger ones (target: 1MB) | files_scanned, files_pruned |
| `optimize_noop_already_compact` | Optimize an already-compacted table (should be a no-op) | files_scanned, files_pruned |
| `optimize_heavy_compaction` | Aggressive compaction with small target size (64KB) | files_scanned, files_pruned |
| `vacuum_dry_run_lite` | Dry-run vacuum to identify removable files without deleting | files_scanned, operations |
| `vacuum_execute_lite` | Execute vacuum to remove expired files | files_scanned, operations |

### concurrency (5 cases)

Rust-only multi-worker races for parallel table creation, concurrent appends, and overlapping maintenance/DML operations. Local storage only. Each measured sample uses fixed worker topology and fixed work; contended cases aggregate 3 independent races over pre-cloned fixture copies.

`table_version` is meaningful only for the shared-table cases (`concurrent_table_create`, `concurrent_append_multi`). The three contended cases aggregate independent fixture copies, so they intentionally emit `table_version: null`.

| Case | Description | Key metrics |
|---|---|---|
| `concurrent_table_create` | Workers race to create the same empty table in a fresh temp directory | elapsed_ms, ops_succeeded |
| `concurrent_append_multi` | Workers concurrently append fixed row batches into the same new table | elapsed_ms, ops_succeeded |
| `update_vs_compaction` | Localized update and optimize workers race on the `delete_update_small_files_delta` fixture using `region = 'us' AND id % 17 = 0` | ops_succeeded, conflict_delete_read, elapsed_ms |
| `delete_vs_compaction` | Scattered delete and optimize workers race on the `delete_update_small_files_delta` fixture using `id % 20 = 0` | ops_succeeded, conflict_delete_read, elapsed_ms |
| `optimize_vs_optimize_overlap` | Two optimize workers race on overlapping small-file compaction work | conflict_delete_delete, ops_succeeded, elapsed_ms |

### tpcds (4 queries)

TPC-DS analytical queries against the `store_sales` table. Requires `tpcds_duckdb` dataset.

| Case | Status | Description |
|---|---|---|
| `tpcds_q03` | Enabled | TPC-DS Query 3 |
| `tpcds_q07` | Enabled | TPC-DS Query 7 |
| `tpcds_q64` | Enabled | TPC-DS Query 64 |
| `tpcds_q72` | Disabled | TPC-DS Query 72 (blocked on DataFusion parity) |

### interop_py (3 cases)

Python interop benchmarks testing roundtrip and scan performance through Python libraries.

| Case | Description | Key metrics |
|---|---|---|
| `pandas_roundtrip_smoke` | Write and read-back through pandas | rows_processed, bytes_processed |
| `polars_roundtrip_smoke` | Write and read-back through polars | rows_processed, bytes_processed |
| `pyarrow_dataset_scan_perf` | Dataset scan through pyarrow | rows_processed, bytes_processed |

## Metrics Reference

All metrics are optional per-sample fields. Which metrics are populated depends on the suite and case.

### Base metrics

Emitted by all suites.

| Metric | Type | Description |
|---|---|---|
| `rows_processed` | u64 | Number of rows read or written |
| `bytes_processed` | u64 | Number of bytes read or written |
| `operations` | u64 | Number of Delta operations executed |
| `table_version` | u64 | Delta table version after the operation when the sample targets one logical table; null for aggregated multi-table races |

### Scan and rewrite metrics

Emitted by scan, delete_update, merge, and optimize_vacuum suites.

| Metric | Type | Description |
|---|---|---|
| `files_scanned` | u64 | Number of data files read |
| `files_pruned` | u64 | Number of data files skipped by pruning |
| `bytes_scanned` | u64 | Total bytes read from data files |
| `scan_time_ms` | u64 | Time spent scanning data files |
| `rewrite_time_ms` | u64 | Time spent rewriting data files |

### Runtime and I/O metrics

Optional metrics for deeper performance analysis.

| Metric | Type | Description |
|---|---|---|
| `peak_rss_mb` | u64 | Peak resident set size in MB |
| `cpu_time_ms` | u64 | Total CPU time consumed |
| `bytes_read` | u64 | Total bytes read (including metadata) |
| `bytes_written` | u64 | Total bytes written |
| `files_touched` | u64 | Number of files accessed |
| `files_skipped` | u64 | Number of files skipped |
| `spill_bytes` | u64 | Bytes spilled to disk |

### Contention metrics

Emitted by the `concurrency` suite as a nested `metrics.contention` object. These counters reflect terminal returned outcomes from public Delta operations only; they do not claim visibility into internal retry attempts.

| Metric | Type | Description |
|---|---|---|
| `worker_count` | u64 | Number of concurrent workers launched per measured sample |
| `race_count` | u64 | Number of independent races aggregated into one measured sample |
| `ops_attempted` | u64 | Total public operations attempted across all workers and races |
| `ops_succeeded` | u64 | Operations that returned success |
| `ops_failed` | u64 | Operations that returned a terminal error outcome |
| `conflict_append` | u64 | Classified append-conflict outcomes |
| `conflict_delete_read` | u64 | Classified delete-read conflict outcomes |
| `conflict_delete_delete` | u64 | Classified overlapping remove/delete conflict outcomes |
| `conflict_metadata_changed` | u64 | Classified metadata-changed conflict outcomes |
| `conflict_protocol_changed` | u64 | Classified protocol-changed conflict outcomes |
| `conflict_transaction` | u64 | Classified concurrent transaction conflict outcomes |
| `version_already_exists` | u64 | Version-collision outcomes returned by the public operation |
| `max_commit_attempts_exceeded` | u64 | Operations that exhausted the public commit-attempt budget |
| `other_errors` | u64 | Unclassified or unexpected errors; non-zero fails the benchmark case |

### Result integrity metrics

Hash-based verification of result correctness.

| Metric | Type | Description |
|---|---|---|
| `result_hash` | String | SHA256 hash of the query result data |
| `schema_hash` | String | SHA256 hash of the result schema |

### Elapsed statistics

Case-level timing aggregates computed across all measured samples.

| Metric | Type | Description |
|---|---|---|
| `min_ms` | f64 | Minimum elapsed time across samples |
| `max_ms` | f64 | Maximum elapsed time across samples |
| `mean_ms` | f64 | Mean elapsed time |
| `median_ms` | f64 | Median elapsed time |
| `stddev_ms` | f64 | Standard deviation of elapsed times |
| `cv_pct` | f64 | Coefficient of variation (stddev/mean * 100). Below 5% is stable; above 10% is noisy. |

## CLI Commands and Flags

### Global flags

These apply to all `delta-bench` subcommands and are passed through `bench.sh`:

| Flag | Env variable | Default | Description |
|---|---|---|---|
| `--fixtures-dir` | `DELTA_BENCH_FIXTURES` | `fixtures` | Path to fixture data directory |
| `--results-dir` | `DELTA_BENCH_RESULTS` | `results` | Path to result output directory |
| `--label` | `DELTA_BENCH_LABEL` | `local` | Run identifier in result paths |
| `--git-sha` | — | — | Git SHA to record in result metadata |
| `--storage-backend` | `DELTA_BENCH_STORAGE_BACKEND` | `local` | Storage backend: `local` or `s3` |
| `--storage-option` | — | — | Repeatable `KEY=VALUE` storage options |
| `--backend-profile` | `DELTA_BENCH_BACKEND_PROFILE` | — | Profile name from `backends/*.env` |

### `bench.sh data` — Generate fixtures

| Flag | Default | Description |
|---|---|---|
| `--scale` | `sf1` | Scale factor for fixture generation |
| `--dataset-id` | — | Dataset identifier (see [Datasets and Scales](#datasets-and-scales)) |
| `--seed` | `42` | RNG seed for deterministic data |
| `--force` | `false` | Regenerate even if fixtures already exist |

### `bench.sh run` — Execute benchmarks

| Flag | Default | Description |
|---|---|---|
| `--scale` | `sf1` | Scale factor |
| `--dataset-id` | — | Dataset identifier |
| `--target` | `all` | Suite to run (or `all`) |
| `--case-filter` | — | Substring filter for case names |
| `--runner` | `all` | Runner mode: `rust`, `python`, or `all` |
| `--warmup` | `1` | Warmup iterations per case (not measured) |
| `--iterations` | `5` | Measured iterations per case |
| `--no-summary-table` | `false` | Suppress terminal summary table |

### `bench.sh list` — List available cases

```bash
./scripts/bench.sh list [target]   # target defaults to "all"
```

### `bench.sh doctor` — Diagnose workspace

```bash
./scripts/bench.sh doctor
```

Checks: delta-rs checkout exists, harness is synced, Cargo can resolve the benchmark crate.

### `compare_branch.sh` — Compare two revisions

| Flag | Default | Description |
|---|---|---|
| `<base_ref> <candidate_ref>` | — | Positional: base and candidate branch/ref names |
| `--base-sha` | — | Pin base to an exact commit SHA |
| `--candidate-sha` | — | Pin candidate to an exact commit SHA |
| `--current-vs-main` | — | Compare current checkout against upstream main |
| `--warmup` | `2` | Warmup iterations per case |
| `--iters` | `9` | Measured iterations per case per run |
| `--prewarm-iters` | `1` | Unreported warmup iterations per ref |
| `--compare-runs` | `3` | Measured runs per ref before aggregation |
| `--measure-order` | `alternate` | Run interleaving: `base-first`, `candidate-first`, `alternate` |
| `--aggregation` | `median` | Aggregation method: `min`, `median`, `p95` |
| `--noise-threshold` | `0.05` | Minimum relative change to classify as regression/improvement |
| `--remote-runner` | — | SSH target for remote execution |
| `--remote-root` | — | Remote working directory |
| `--enforce-run-mode` | — | Require benchmark run mode |
| `--require-no-public-ipv4` | — | Require no public IPv4 address |
| `--require-egress-policy` | — | Require network egress policy |
| `--backend-profile` | — | Backend profile name |
| `--runner` | — | Runner mode: `rust`, `python`, `all` |

### `longitudinal_bench.sh` — Longitudinal pipeline

Subcommands: `select-revisions`, `build-artifacts`, `run-matrix`, `ingest-results`, `report`, `prune`, `orchestrate`. `run-matrix` writes a resumable `matrix-state.json` checkpoint, and `ingest-results` / `report` / `prune` operate on a SQLite `store.sqlite3` store. See [Longitudinal Benchmarking](longitudinal.md) for full flag documentation per subcommand.

### `cleanup_local.sh` — Safe artifact cleanup

| Flag | Default | Description |
|---|---|---|
| `--results` | — | Target result files |
| `--fixtures` | — | Target fixture data |
| `--delta-rs-under-test` | — | Target managed checkout |
| `--keep-last N` | — | Retain N most recent result sets |
| `--older-than-days N` | — | Only remove items older than N days |
| `--allow-outside-root` | — | Allow cleanup outside repo root |
| `--apply` | `false` | Execute deletions (dry-run without this) |

## Repository Verification Baseline

These commands are the current repo-wide baseline for code, dependency, and self-hosted control-plane verification.

| Scope | Command | Notes |
|---|---|---|
| Rust tests | `cargo test --locked` | Matches the primary CI test job. |
| Python tests | `(cd python && python3 -m pytest -q tests)` | Matches the primary CI test job. |
| Rust dependency audit | `cargo audit --ignore RUSTSEC-2026-0037 --ignore RUSTSEC-2026-0041 --ignore RUSTSEC-2026-0049` | Temporary triage for known upstream advisories; new advisories still fail the job. |
| Python dependency audit | `python3 -m pip_audit -r python/requirements-audit.txt` | Audits the actual interop/runtime dependency set. |
| Self-hosted preflight | `./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy` | Required by self-hosted benchmark and longitudinal workflows before execution. |

## Environment Variables

### CLI configuration

| Variable | Default | Description |
|---|---|---|
| `DELTA_BENCH_FIXTURES` | `fixtures` | Fixture data directory |
| `DELTA_BENCH_RESULTS` | `results` | Result output directory |
| `DELTA_BENCH_LABEL` | `local` | Run identifier |
| `DELTA_BENCH_STORAGE_BACKEND` | `local` | Storage backend (`local` or `s3`) |
| `DELTA_BENCH_BACKEND_PROFILE` | — | Backend profile from `backends/` |
| `DELTA_BENCH_SUPPRESS_RUST_WARNINGS` | `1` | Set to `0` to show Rust compiler warnings |

### TPC-DS and DuckDB

| Variable | Default | Description |
|---|---|---|
| `DELTA_BENCH_DUCKDB_PYTHON` | `python3` | Python executable for DuckDB fixture generation |
| `DELTA_BENCH_TPCDS_DUCKDB_SCRIPT` | — | Override path to TPC-DS DuckDB script |
| `DELTA_BENCH_TPCDS_DUCKDB_TIMEOUT_MS` | `600000` | Timeout for DuckDB fixture generation (10 minutes) |

### Fixture locking

| Variable | Default | Description |
|---|---|---|
| `DELTA_BENCH_FIXTURE_LOCK_TIMEOUT_MS` | `120000` | Timeout for acquiring fixture lock (2 minutes) |
| `DELTA_BENCH_FIXTURE_LOCK_RETRY_MS` | `50` | Retry interval for fixture lock acquisition |

### Python interop

| Variable | Default | Description |
|---|---|---|
| `DELTA_BENCH_INTEROP_TIMEOUT_MS` | `120000` | Timeout for Python interop cases (2 minutes) |
| `DELTA_BENCH_INTEROP_RETRIES` | `1` | Number of retries for Python interop failures |
| `DELTA_BENCH_INTEROP_PYTHON` | `python3` | Python executable for interop cases |

### Script-level configuration

| Variable | Default | Description |
|---|---|---|
| `BENCH_TIMEOUT_SECONDS` | `3600` | Per-step timeout cap for compare/longitudinal scripts |
| `BENCH_RETRY_ATTEMPTS` | `2` | Retry count for transient benchmark failures |
| `BENCH_RETRY_DELAY_SECONDS` | `5` | Delay between retries |
| `BENCH_STORAGE_BACKEND` | `local` | Storage backend for script-level workflows |
| `BENCH_STORAGE_OPTIONS` | — | Multi-line `KEY=VALUE` storage options |
| `BENCH_BACKEND_PROFILE` | — | Backend profile for script-level workflows |
| `BENCH_RUNNER_MODE` | — | Runner mode for script-level workflows (`rust`, `python`, `all`) |

### Fidelity and hardening

These are captured in result metadata when running on cloud/hardened infrastructure.

| Variable | Default | Description |
|---|---|---|
| `DELTA_BENCH_IMAGE_VERSION` | — | Container or VM image version |
| `DELTA_BENCH_HARDENING_PROFILE_ID` | — | System hardening profile identifier |
| `DELTA_BENCH_HARDENING_PROFILE_SHA256` | — | SHA256 of the hardening configuration |
| `DELTA_BENCH_HARDENING_PROFILE_PATH` | — | Path to hardening profile file |
| `DELTA_BENCH_EGRESS_POLICY_SHA256` | — | SHA256 of network egress policy |
| `DELTA_BENCH_EGRESS_POLICY_PATH` | — | Path to egress policy file |
| `DELTA_BENCH_RUN_MODE` | — | Security/execution mode |
| `DELTA_BENCH_RUN_MODE_PATH` | — | Path to run mode configuration |
| `DELTA_BENCH_MAINTENANCE_WINDOW_ID` | — | Maintenance window identifier |

## Longitudinal State and Store

The longitudinal pipeline persists two primary artifacts beyond the raw `results/<label>/<suite>.json` files:

| Artifact | Typical location | Description |
|---|---|---|
| `matrix-state.json` | `longitudinal/state/matrix-state.json` or lane-specific `longitudinal/releases/<lane>/state/matrix-state.json` | JSON checkpoint for `run-matrix`. Stores per-cell status and a top-level `config` fingerprint so resume only happens against the same suite/scale/warmup/iteration/output contract. |
| `store.sqlite3` | `longitudinal/store/store.sqlite3` or lane-specific `longitudinal/releases/<lane>/store/store.sqlite3` | SQLite database populated by `ingest-results`. Holds normalized run metadata and case rows used by `report` and `prune`. Duplicate ingests are deduplicated by run id. |

Legacy `rows.jsonl` / `index.json` stores are no longer a supported primary path. If those files exist without `store.sqlite3`, ingest/report/prune fail fast so the operator can migrate or remove the stale state intentionally.

## Datasets and Scales

### Dataset IDs

| Dataset ID | Scale | Fixture profile | Description |
|---|---|---|---|
| `tiny_smoke` | sf1 | Standard | Minimal smoke test. Fast to generate. |
| `medium_selective` | sf10 | Standard | Medium workload with selective query patterns. |
| `small_files` | sf1 | Standard | Many small files for optimize/vacuum testing. |
| `many_versions` | sf1 | ManyVersions | Creates 12 commits to build version history. |
| `tpcds_duckdb` | sf1 | TpcdsDuckdb | TPC-DS `store_sales` sourced from DuckDB. Requires `python3` + `duckdb`. |

### Scale factors

| Scale | Row count | Description |
|---|---|---|
| `sf1` | 10,000 | Small. Good for smoke tests and development. |
| `sf10` | 100,000 | Medium. Realistic for selective query patterns. |
| `sf100` | 1,000,000 | Large. For production-representative benchmarks. |

### Fixture profiles

| Profile | Used by | Behavior |
|---|---|---|
| Standard | `tiny_smoke`, `medium_selective`, `small_files` | Normal fixture generation |
| ManyVersions | `many_versions` | Creates 12 append commits to build a Delta version history |
| TpcdsDuckdb | `tpcds_duckdb` | Loads TPC-DS data via DuckDB `dsdgen`, exports through CSV, writes to Delta |

## Fixture Tables

All fixture tables are generated under `<fixtures_dir>/<scale>/`.

| Table | Directory | Purpose |
|---|---|---|
| Narrow sales | `narrow_sales_delta` | Base table for scan and read benchmarks |
| Read partitioned | `read_partitioned_delta` | Partitioned table for pruning tests |
| Merge target | `merge_target_delta` | Standard merge target |
| Merge partitioned target | `merge_partitioned_target_delta` | Partitioned merge target for localized merge |
| Delete/update small files | `delete_update_small_files_delta` | Small-file layout for scattered DML |
| Optimize small files | `optimize_small_files_delta` | Small files for compaction testing |
| Optimize compacted | `optimize_compacted_delta` | Already-compacted table for no-op optimize test |
| Vacuum ready | `vacuum_ready_delta` | Table with expired files for vacuum testing |
| TPC-DS store_sales | `tpcds/store_sales` | TPC-DS `store_sales` table |

Additional fixture artifacts:
- `rows.jsonl` — JSON-lines snapshot of the source row data
- `manifest.json` — Fixture generation metadata (schema version, seed, scale, fingerprint)

## Result Schema v2

### Top-level structure

| Field | Type | Description |
|---|---|---|
| `schema_version` | u32 | Format version (currently 2) |
| `context` | object | Host, run configuration, and fidelity metadata |
| `cases` | array | Array of benchmark case results |

### Context fields

| Field | Type | Required | Description |
|---|---|---|---|
| `schema_version` | u32 | yes | Context schema version (always `2`) |
| `host` | string | yes | Machine hostname |
| `label` | string | yes | Run label identifier |
| `git_sha` | string | no | Git SHA of the revision under test |
| `created_at` | datetime | yes | Timestamp of result creation |
| `suite` | string | yes | Benchmark suite name |
| `scale` | string | yes | Scale factor |
| `iterations` | u32 | yes | Measured iterations per case |
| `warmup` | u32 | yes | Warmup iterations per case |
| `dataset_id` | string | no | Dataset identifier |
| `dataset_fingerprint` | string | no | Hash of the fixture data |
| `runner` | string | no | Runner mode (rust/python) |
| `backend_profile` | string | no | Backend profile name |

### Fidelity and security context fields

These are populated when running on cloud/hardened infrastructure.

| Field | Type | Description |
|---|---|---|
| `image_version` | string | Container or VM image version |
| `hardening_profile_id` | string | System hardening profile ID |
| `hardening_profile_sha256` | string | SHA256 of hardening configuration |
| `cpu_model` | string | CPU model identifier |
| `cpu_microcode` | string | CPU microcode version |
| `kernel` | string | Kernel version |
| `boot_params` | string | Kernel boot parameters |
| `cpu_steal_pct` | f64 | CPU steal percentage (cloud VMs) |
| `numa_topology` | string | NUMA topology description |
| `egress_policy_sha256` | string | SHA256 of network egress policy |
| `run_mode` | string | Benchmark run mode |
| `maintenance_window_id` | string | Maintenance window identifier |

### Case-level fields

| Field | Type | Description |
|---|---|---|
| `case` | string | Case name (e.g., `scan_full_narrow`) |
| `success` | bool | Whether the case completed without error |
| `classification` | string | `supported` or `expected_failure` |
| `samples` | array | Per-iteration timing and metrics |
| `failure` | object | Failure payload with a `message` field when the case failed |
| `elapsed_stats` | object | Timing statistics across samples (see [Elapsed statistics](#elapsed-statistics)) |

### Sample-level fields

Each sample represents one measured iteration.

| Field | Type | Description |
|---|---|---|
| `elapsed_ms` | f64 | Wall-clock time for this iteration |
| `rows` | u64 | Optional row count captured directly on the sample |
| `bytes` | u64 | Optional byte count captured directly on the sample |
| `metrics` | object | Optional flat and nested metric fields (see [Metrics Reference](#metrics-reference)) |
| `metrics.contention` | object | Optional nested contention metrics domain emitted by `concurrency` |

## Manifest Format

Benchmark manifests declare which cases to execute and what assertions to validate. Core manifests live in `bench/manifests/`.

| Manifest | Runner | Description |
|---|---|---|
| `core_rust.yaml` | rust | All Rust suite cases |
| `core_python.yaml` | python | Python interop cases |

### Manifest structure

```yaml
id: <manifest-id>
description: <description>
cases:
  - id: <case-name>
    target: <suite-name>
    runner: rust|python
    enabled: true|false
    assertions:
      - type: <assertion-type>
        value: <expected-value>
```

### Assertion types

| Type | Value format | Description |
|---|---|---|
| `exact_result_hash` | `sha256:<hash>` | SHA256 of query result data must match |
| `schema_hash` | `sha256:<hash>` | SHA256 of result schema must match |
| `expected_error_contains` | `<substring>` | Error message must contain this substring |
| `version_monotonicity` | — | Table version must be monotonically increasing |

### Case classifications

| Classification | Meaning |
|---|---|
| `supported` | Normal case — expected to succeed |
| `expected_failure` | Case is expected to fail (e.g., unsupported operation). Failure is the passing state. |

## Backend Profiles

Backend profiles store storage configuration defaults in `backends/<name>.env` files.

### Available profiles

| Profile | Description |
|---|---|
| `s3_locking_vultr` | S3 storage with DynamoDB locking on Vultr |

### Profile format

Profiles use `KEY=VALUE` format, one per line:

```env
AWS_REGION=us-east-1
table_root=s3://delta-bench/private
AWS_S3_LOCKING_PROVIDER=dynamodb
DELTA_DYNAMO_TABLE_NAME=delta-bench-lock
```

Load a profile with `--backend-profile <name>` or `DELTA_BENCH_BACKEND_PROFILE=<name>`. Override individual values at runtime with `--storage-option KEY=VALUE`.
