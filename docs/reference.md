# Reference

Complete lookup reference for the delta-rs benchmarking harness. This page catalogs every suite, case, metric, CLI flag, environment variable, dataset, and schema field in one place.

## Table of Contents

- [Glossary](#glossary)
- [Benchmark Suites and Cases](#benchmark-suites-and-cases)
- [Criterion Microbench Families](#criterion-microbench-families)
- [Metrics Reference](#metrics-reference)
- [CLI Commands and Flags](#cli-commands-and-flags)
- [Repository Verification Baseline](#repository-verification-baseline)
- [Environment Variables](#environment-variables)
- [Longitudinal State and Store](#longitudinal-state-and-store)
- [Datasets and Scales](#datasets-and-scales)
- [Fixture Tables](#fixture-tables)
- [Result Schema v5](#result-schema-v5)
- [Manifest Format](#manifest-format)
- [Backend Profiles](#backend-profiles)

## Glossary

| Term                   | Definition                                                                                                                                                            |
| ---------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Suite**              | A named group of benchmark cases that test a specific Delta Lake operation (e.g., `scan`, `write`, `merge`).                                                          |
| **Case**               | An individual benchmark within a suite. Each case is run for a configured number of warmup + measured iterations.                                                     |
| **Methodology profile** | A harness-owned compare or diagnostic contract loaded from `bench/methodologies/<name>.env` so operators do not restate raw compare knobs or Criterion entrypoints by hand. |
| **Evidence registry**   | The harness-owned policy file at `bench/evidence/registry.yaml` that classifies suites and defines pack aliases such as `full -> pr-full-decision`.                   |
| **Runner**             | The execution lane: `rust` (native Rust implementation), `python` (Python interop via pandas/polars/pyarrow), or `all`.                                               |
| **Dataset**            | A named fixture configuration that controls which tables are generated and at what size. Identified by `dataset_id`.                                                  |
| **Scale**              | The size factor for fixture data: `sf1` (10K rows), `sf10` (100K rows), `sf100` (1M rows).                                                                            |
| **Fixture**            | Deterministic test data generated from a seed. Includes Delta tables, JSON row snapshots, and a manifest.                                                             |
| **Fixture profile**    | Controls how fixtures are generated: `Standard` (normal), `ManyVersions` (12 commits for version history), `TpcdsDuckdb` (DuckDB TPC-DS source).                      |
| **Label**              | A run identifier used in result paths (e.g., `results/<label>/<suite>.json`). Must match `[A-Za-z0-9._-]` and cannot be `.` or `..`.                                  |
| **Schema v5**          | The normalized JSON result format for authoritative benchmark output. Includes context identity, benchmark mode, cases, per-sample metrics, and run summaries.         |
| **Manifest**           | A YAML or JSON file that declares which benchmark cases to execute and what assertions to validate.                                                                   |
| **Backend profile**    | A `.env` file under `backends/` with storage configuration defaults (S3 bucket, locking, region).                                                                     |
| **Lane**               | A benchmark execution contract such as `smoke`, `correctness`, or `macro`. Longitudinal release history also uses separate release lanes such as `rust` and `python`. |
| **Matrix state**       | The resumable JSON checkpoint written by `run-matrix`, including per-cell execution status and a configuration fingerprint.                                           |
| **Longitudinal store** | The SQLite database (`store.sqlite3`) that holds normalized run metadata and case rows for ingest/report/prune.                                                       |
| **Fidelity**           | System-level metadata (CPU model, kernel, run mode) captured alongside results to ensure reproducibility.                                                             |
| **cv_pct**             | Coefficient of variation as a percentage. Measures result noise. Below 5% is good; above 10% warrants rerunning.                                                      |

### Label Contract

Labels are validated strictly at the Rust CLI boundary and sanitized in Bash/Python helper paths to produce Rust-valid labels.

| Input                                     | Behavior                   |
| ----------------------------------------- | -------------------------- |
| `local-main_20260325`                     | valid                      |
| `bench.v1`                                | valid                      |
| `""`, `.`, `..`, `bad/slash`, `bad space` | invalid                    |
| `feature/foo`                             | sanitizes to `feature_foo` |
| `___`                                     | sanitizes to `label`       |
| `...`                                     | sanitizes to `...`         |

## Benchmark Suites and Cases

### scan (5 cases)

Read operations testing full scans, projections, filters, and partition pruning.

| Case                     | Description                                                       | Key metrics                                  |
| ------------------------ | ----------------------------------------------------------------- | -------------------------------------------- |
| `scan_full_narrow`       | Full table scan of the narrow sales table with all columns        | files_scanned, bytes_scanned, rows_processed |
| `scan_projection_region` | Scan with column projection (region column only)                  | files_scanned, bytes_scanned, rows_processed |
| `scan_filter_flag`       | Scan with a predicate filter on the flag column                   | files_scanned, bytes_scanned, rows_processed |
| `scan_pruning_hit`       | Scan with a filter that prunes most partitions (high selectivity) | files_scanned, files_pruned, scan_time_ms    |
| `scan_pruning_miss`      | Scan with a filter that prunes no partitions (low selectivity)    | files_scanned, files_pruned, scan_time_ms    |

For phase-aware suites, use `--timing-phase load|plan|execute|validate` to select which isolated phase populates `elapsed_ms`. Case IDs stay the same regardless of the selected phase.

Authoritative decision runs use `scan_full_narrow`, `scan_projection_region`, and `scan_filter_flag` on the deterministic `medium_selective` dataset. `scan_pruning_hit` is intentionally excluded from the macro decision manifest and belongs in Criterion microbench coverage because it is routinely too small/cache-sensitive on local disk. `scan_pruning_miss` is listed for exploratory review but stays disabled in `bench/manifests/core_rust.yaml` until its exact-result assertion is requalified.

Use `scan` as the execute-phase guardrail. For scan-internal planning or execution probes, pair it with `./scripts/run_profile.sh scan-phase-criterion`. For snapshot/provider replay diagnostics, use `./scripts/run_profile.sh metadata-replay-criterion`. For log parsing or snapshot materialization internals, use `./scripts/run_profile.sh metadata-log-criterion`. Criterion output is diagnostic-only and should be reported separately from authoritative PR evidence.

### write (3 cases)

Write operations testing append and overwrite patterns. Local storage only.

| Case                 | Description                                    | Key metrics                                 |
| -------------------- | ---------------------------------------------- | ------------------------------------------- |
| `write_append_small` | Append a small batch (128 rows) to a new table | rows_processed, bytes_processed, operations |
| `write_append_large` | Append all rows to a new table                 | rows_processed, bytes_processed, operations |
| `write_overwrite`    | Overwrite an existing table with all rows      | rows_processed, bytes_processed, operations |

### delete_update (7 cases)

DML operations testing deletes and updates at varying selectivity and data layout.

| Case                            | Description                                                          | Key metrics                                    |
| ------------------------------- | -------------------------------------------------------------------- | ---------------------------------------------- |
| `delete_1pct_localized`         | Delete 1% of rows within a single partition                          | rows_processed, files_scanned, rewrite_time_ms |
| `delete_5pct_scattered`         | Delete 5% of rows scattered across small files                       | rows_processed, files_scanned, rewrite_time_ms |
| `delete_50pct_broad`            | Delete 50% of rows across all partitions                             | rows_processed, files_scanned, rewrite_time_ms |
| `update_literal_1pct_localized` | Update 1% of rows with a literal value, partition-localized          | rows_processed, files_scanned, rewrite_time_ms |
| `update_literal_5pct_scattered` | Update 5% of rows with a literal value, scattered across small files | rows_processed, files_scanned, rewrite_time_ms |
| `update_expr_50pct_broad`       | Update 50% of rows with an expression, across all partitions         | rows_processed, files_scanned, rewrite_time_ms |
| `update_all_rows_expr`          | Update all rows with an expression (full table rewrite)              | rows_processed, files_scanned, rewrite_time_ms |

`delete_update` stays correctness-backed. If you need DML compare evidence, use the dedicated `delete_update_perf` suite instead of treating macro-lane runs of `delete_update` as authoritative.
For shared delete/update file-selection internals, use `./scripts/run_profile.sh file-selection-criterion`. That Criterion family is diagnostic-only and stays separate from macro PR evidence.

### delete_update_perf (4 cases)

Perf-owned DML suite with split compare profiles. `pr-delete-update-perf` is the lighter blocking PR gate for `delete_update_perf`; `delete-update-perf-high-confidence` preserves the longer manual or nightly evidence contract. Both fix `dataset_id=medium_selective`, and the suite stays gated in `bench/evidence/registry.yaml` until same-SHA stability, delayed-canary validation, runtime signoff, and one stable case-list refresh cycle are complete.

| Case                                   | Description                                                           | Key metrics                                    |
| -------------------------------------- | --------------------------------------------------------------------- | ---------------------------------------------- |
| `delete_perf_localized_1pct`           | Perf-owned localized delete case matching 1% of rows in one partition | rows_processed, files_scanned, rewrite_time_ms |
| `delete_perf_scattered_5pct_small_files` | Perf-owned scattered delete case on small-file layout                 | rows_processed, files_scanned, rewrite_time_ms |
| `update_perf_literal_5pct_scattered`   | Perf-owned scattered literal update on small-file layout              | rows_processed, files_scanned, rewrite_time_ms |
| `update_perf_all_rows_expr`            | Perf-owned full-table expression update canary                        | rows_processed, files_scanned, rewrite_time_ms |

### merge (6 cases)

Merge (upsert/delete) operations at varying match ratios and target configurations.

| Case                              | Description                                                 | Key metrics                                                |
| --------------------------------- | ----------------------------------------------------------- | ---------------------------------------------------------- |
| `merge_delete_5pct`               | Merge-delete matching 5% of rows in standard table          | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |
| `merge_upsert_10pct_insert_10pct` | Upsert matching 10% + insert 10% new rows                   | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |
| `merge_upsert_10pct`              | Upsert matching 10% of rows                                 | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |
| `merge_upsert_50pct`              | Upsert matching 50% of rows                                 | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |
| `merge_upsert_90pct`              | Upsert matching 90% of rows (near-full rewrite)             | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |
| `merge_localized_1pct`            | Partition-aware upsert matching 1% with partition predicate | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |

`merge` stays correctness-backed. For candidate/manual merge perf evidence, use `merge_perf`.
For merge planning internals, use `./scripts/run_profile.sh merge-filter-criterion`. That Criterion family is diagnostic-only, planning-only, and stays separate from `merge_perf`.

### merge_perf (4 cases)

Perf-owned merge candidate/manual suite. The compare profile is `pr-merge-perf`, which fixes `dataset_id=medium_selective` and stays gated until same-SHA stability, delayed-canary validation, runtime signoff, and case-list stability are all closed.

| Case                        | Description                                                      | Key metrics                                                |
| --------------------------- | ---------------------------------------------------------------- | ---------------------------------------------------------- |
| `merge_perf_upsert_10pct`   | Perf-owned upsert matching 10% of rows                           | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |
| `merge_perf_upsert_50pct`   | Perf-owned upsert matching 50% of rows                           | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |
| `merge_perf_localized_1pct` | Perf-owned partition-aware upsert matching 1% of rows            | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |
| `merge_perf_delete_5pct`    | Perf-owned merge-delete matching 5% of rows                      | files_scanned, files_pruned, scan_time_ms, rewrite_time_ms |

### metadata (2 cases)

Metadata operations testing table load and time-travel performance.

| Case                      | Description                                                      | Key metrics               |
| ------------------------- | ---------------------------------------------------------------- | ------------------------- |
| `metadata_load`           | Load table metadata from the transaction log                     | table_version, operations |
| `metadata_time_travel_v0` | Load table metadata at version 0 (time travel to initial commit) | table_version, operations |

`metadata` stays correctness-backed. Do not treat it as authoritative perf evidence.

### optimize_vacuum (5 cases)

Table maintenance operations: file compaction and vacuum.

| Case                            | Description                                                 | Key metrics                 |
| ------------------------------- | ----------------------------------------------------------- | --------------------------- |
| `optimize_compact_small_files`  | Compact small files into larger ones (target: 1MB)          | files_scanned, files_pruned |
| `optimize_noop_already_compact` | Optimize an already-compacted table (should be a no-op)     | files_scanned, files_pruned |
| `optimize_heavy_compaction`     | Aggressive compaction with small target size (64KB)         | files_scanned, files_pruned |
| `vacuum_dry_run_lite`           | Dry-run vacuum to identify removable files without deleting | files_scanned, operations   |
| `vacuum_execute_lite`           | Execute vacuum to remove expired files                      | files_scanned, operations   |

`optimize_vacuum` stays correctness-backed. For candidate/manual maintenance perf evidence, use `optimize_perf`.

### optimize_perf (3 cases)

Perf-owned maintenance candidate/manual suite. The compare profile is `pr-optimize-perf`, which fixes `dataset_id=medium_selective` and stays gated until same-SHA stability, delayed-canary validation, runtime signoff, and a stable initial case set are complete.

| Case                                 | Description                                            | Key metrics                 |
| ------------------------------------ | ------------------------------------------------------ | --------------------------- |
| `optimize_perf_compact_small_files`  | Perf-owned compaction on the small-file maintenance fixture | files_scanned, files_pruned |
| `optimize_perf_noop_already_compact` | Perf-owned no-op optimize case on already compacted data   | files_scanned, files_pruned |
| `vacuum_perf_execute_lite`           | Perf-owned vacuum execute case                             | files_scanned, operations   |

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

TPC-DS analytical queries against the `store_sales` table. The trusted self-hosted compare profile is `pr-tpcds`, which requires the `tpcds_duckdb` dataset. Only `tpcds_q03`, `tpcds_q07`, and `tpcds_q64` participate in the candidate/manual evidence path today; `tpcds_q72` remains outside the PR decision surface. `tpcds` remains candidate/manual until fixture provisioning, same-SHA stability, delayed-canary validation, and runtime signoff are all closed.

| Case        | Status   | Description                                    |
| ----------- | -------- | ---------------------------------------------- |
| `tpcds_q03` | Enabled  | TPC-DS Query 3                                 |
| `tpcds_q07` | Enabled  | TPC-DS Query 7                                 |
| `tpcds_q64` | Enabled  | TPC-DS Query 64                                |
| `tpcds_q72` | Disabled | TPC-DS Query 72 (blocked on DataFusion parity) |

### interop_py (3 cases)

Python interop benchmarks testing roundtrip and scan performance through Python libraries. These cases are correctness-backed and normally validated on the GitHub-hosted correctness lane rather than used as macro PR perf evidence.

| Case                        | Description                        | Key metrics                     |
| --------------------------- | ---------------------------------- | ------------------------------- |
| `pandas_roundtrip_smoke`    | Write and read-back through pandas | rows_processed, bytes_processed |
| `polars_roundtrip_smoke`    | Write and read-back through polars | rows_processed, bytes_processed |
| `pyarrow_dataset_scan_perf` | Dataset scan through pyarrow       | rows_processed, bytes_processed |

## Criterion Microbench Families

Criterion profiles are for local or trusted self-hosted investigation only. They are diagnostic-only, never authoritative PR evidence, and do not enter `bench/evidence/registry.yaml` packs, `compare_branch.sh`, PR comment automation, or longitudinal ingest.

Use `./scripts/run_profile.sh <profile>` for committed Criterion entrypoints. Existing and planned families are listed here so the diagnostic contract stays explicit as coverage expands without widening PR decision scope.

| Family | Status | Profile | Bench file | Intended use |
| --- | --- | --- | --- | --- |
| `scan_phase` | Existing | `scan-phase-criterion` | `scan_phase_bench.rs` | Phase-isolated scan microbench for `scan_filter_flag`, `scan_projection_region`, and `scan_pruning_hit` |
| `metadata_replay` | Existing | `metadata-replay-criterion` | `metadata_replay_bench.rs` | Snapshot/provider replay probes for the dedicated metadata replay bench |
| `metadata_log` | Existing | `metadata-log-criterion` | `metadata_log_bench.rs` | Log parsing and snapshot materialization internals |
| `file_selection` | Existing | `file-selection-criterion` | `file_selection_bench.rs` | Shared delete/update file-finding and predicate-selection internals |
| `merge_filter` | Existing | `merge-filter-criterion` | `merge_filter_bench.rs` | Merge early-filter and placeholder-expansion planning |
| `optimize_plan` | Planned | `optimize-plan-criterion` (planned) | `optimize_plan_bench.rs` | Compaction planning and file-selection internals |
| `object_store_control` | Optional later | `object-store-control-criterion` (planned) | `object_store_control_bench.rs` | Local object-store control-plane helpers only |

## Metrics Reference

All metrics are optional per-sample fields. Which metrics are populated depends on the suite and case.

### Base metrics

Emitted by all suites.

| Metric            | Type | Description                                                                                              |
| ----------------- | ---- | -------------------------------------------------------------------------------------------------------- |
| `rows_processed`  | u64  | Number of rows read or written                                                                           |
| `bytes_processed` | u64  | Number of bytes read or written                                                                          |
| `operations`      | u64  | Number of Delta operations executed                                                                      |
| `table_version`   | u64  | Delta table version after the operation when the sample targets one logical table; null for aggregated multi-table races |

### Scan and rewrite metrics

Emitted by scan, delete_update, delete_update_perf, merge, merge_perf, optimize_vacuum, and optimize_perf suites.

| Metric            | Type | Description                             |
| ----------------- | ---- | --------------------------------------- |
| `files_scanned`   | u64  | Number of data files read               |
| `files_pruned`    | u64  | Number of data files skipped by pruning |
| `bytes_scanned`   | u64  | Total bytes read from data files        |
| `scan_time_ms`    | u64  | Time spent scanning data files          |
| `rewrite_time_ms` | u64  | Time spent rewriting data files         |

### Runtime and I/O metrics

Optional metrics for deeper performance analysis.

| Metric          | Type | Description                           |
| --------------- | ---- | ------------------------------------- |
| `peak_rss_mb`   | u64  | Peak resident set size in MB          |
| `cpu_time_ms`   | u64  | Total CPU time consumed               |
| `bytes_read`    | u64  | Total bytes read (including metadata) |
| `bytes_written` | u64  | Total bytes written                   |
| `files_touched` | u64  | Number of files accessed              |
| `files_skipped` | u64  | Number of files skipped               |
| `spill_bytes`   | u64  | Bytes spilled to disk                 |

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

| Metric        | Type   | Description                          |
| ------------- | ------ | ------------------------------------ |
| `result_hash` | String | SHA256 hash of the query result data |
| `schema_hash` | String | SHA256 hash of the result schema     |

### Elapsed statistics

Case-level timing aggregates computed across all measured samples.

| Metric      | Type | Description                                                                            |
| ----------- | ---- | -------------------------------------------------------------------------------------- |
| `min_ms`    | f64  | Minimum elapsed time across samples                                                    |
| `max_ms`    | f64  | Maximum elapsed time across samples                                                    |
| `mean_ms`   | f64  | Mean elapsed time                                                                      |
| `median_ms` | f64  | Median elapsed time                                                                    |
| `stddev_ms` | f64  | Standard deviation of elapsed times                                                    |
| `cv_pct`    | f64  | Coefficient of variation (stddev/mean \* 100). Below 5% is stable; above 10% is noisy. |

## CLI Commands and Flags

### Global flags

These apply to all `delta-bench` subcommands and are passed through `bench.sh`:

| Flag                 | Env variable                   | Default     | Description                                            |
| -------------------- | ------------------------------ | ----------- | ------------------------------------------------------ |
| `--fixtures-dir`     | `DELTA_BENCH_FIXTURES`         | `fixtures`  | Path to fixture data directory                         |
| `--results-dir`      | `DELTA_BENCH_RESULTS`          | `results`   | Path to result output directory                        |
| `--label`            | `DELTA_BENCH_LABEL`            | `local`     | Run identifier in result paths                         |
| `--git-sha`          | —                              | —           | Git SHA to record in result metadata                   |
| `--harness-revision` | `DELTA_BENCH_HARNESS_REVISION` | repo `HEAD` | Harness revision recorded in schema v5 identity fields |
| `--storage-backend`  | `DELTA_BENCH_STORAGE_BACKEND`  | `local`     | Storage backend: `local` or `s3`                       |
| `--storage-option`   | —                              | —           | Repeatable `KEY=VALUE` storage options                 |
| `--backend-profile`  | `DELTA_BENCH_BACKEND_PROFILE`  | —           | Profile name from `backends/*.env`                     |

Relative `DELTA_BENCH_FIXTURES` and `DELTA_BENCH_RESULTS` values are resolved against the harness repository root before `bench.sh` switches into `DELTA_BENCH_EXEC_ROOT`. Use absolute paths if you want fixture or result output somewhere else.

### `bench.sh data` — Generate fixtures

| Flag           | Default | Description                                                          |
| -------------- | ------- | -------------------------------------------------------------------- |
| `--scale`      | `sf1`   | Scale factor for fixture generation                                  |
| `--dataset-id` | —       | Dataset identifier (see [Datasets and Scales](#datasets-and-scales)) |
| `--seed`       | `42`    | RNG seed for deterministic data                                      |
| `--force`      | `false` | Regenerate even if fixtures already exist                            |

### `bench.sh run` — Execute benchmarks

| Flag                 | Default   | Description                                                                                                                                                                                                                                                                                          |
| -------------------- | --------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `--scale`            | `sf1`     | Scale factor                                                                                                                                                                                                                                                                                         |
| `--dataset-id`       | —         | Dataset identifier                                                                                                                                                                                                                                                                                   |
| `--suite`            | `all`     | Suite to run (or `all`)                                                                                                                                                                                                                                                                              |
| `--case-filter`      | —         | Substring filter for case names                                                                                                                                                                                                                                                                      |
| `--runner`           | `all`     | Runner mode: `rust`, `python`, or `all`                                                                                                                                                                                                                                                              |
| `--lane`             | `smoke`   | Benchmark lane: `smoke`, `correctness`, or `macro`. `smoke` is the default local workflow; `correctness` is the trusted semantic lane for correctness-backed suites (`write`, `delete_update`, `merge`, `metadata`, `optimize_vacuum`, `interop_py`) and optional semantic validation on the perf-owned DML/maintenance suites; `macro` is the perf lane for macro-safe cases such as `scan`, `write_perf`, `delete_update_perf`, `merge_perf`, `optimize_perf`, and `tpcds`. |
| `--mode`             | `perf`    | Benchmark mode: `perf` records measurable timings; `assert` emits validation-only artifacts and requires `--lane correctness`                                                                                                                                                                         |
| `--timing-phase`     | `execute` | For phase-aware suites, isolate and record `load`, `plan`, `execute`, or `validate` time in `elapsed_ms`                                                                                                                                                                                             |
| `--warmup`           | `1`       | Warmup iterations per case (not measured)                                                                                                                                                                                                                                                            |
| `--iterations`       | `5`       | Measured iterations per case                                                                                                                                                                                                                                                                         |
| `--no-summary-table` | `false`   | Suppress terminal summary table                                                                                                                                                                                                                                                                      |

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

| Flag                         | Default       | Description                                                                                                                                                                         |
| ---------------------------- | ------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `<base_ref> <candidate_ref>` | —             | Positional: base and candidate branch/ref names                                                                                                                                     |
| `--base-sha`                 | —             | Pin base to an exact commit SHA                                                                                                                                                     |
| `--candidate-sha`            | —             | Pin candidate to an exact commit SHA                                                                                                                                                |
| `--base-fetch-url`           | —             | Alternate remote URL used when the base SHA is not reachable from `origin`; prefer the full 40-character SHA or set `DELTA_RS_FETCH_REF` when using abbreviated SHAs             |
| `--candidate-fetch-url`      | —             | Alternate remote URL used when the candidate SHA is not reachable from `origin`; prefer the full 40-character SHA or set `DELTA_RS_FETCH_REF` when using abbreviated SHAs        |
| `--current-vs-main`          | —             | Compare current checkout against upstream main                                                                                                                                      |
| `--methodology-profile`      | —             | Load a harness-owned methodology profile from `bench/methodologies/<name>.env`. `pr-macro` is the self-hosted PR decision contract; `pr-write-perf`, `pr-delete-update-perf`, `delete-update-perf-high-confidence`, `pr-merge-perf`, `pr-optimize-perf`, and `pr-tpcds` are the main perf compare contracts; explicit CLI flags still override profile defaults. |
| `--warmup`                   | `2`           | Warmup iterations per case                                                                                                                                                          |
| `--iters`                    | `9`           | Measured iterations per case per run                                                                                                                                                |
| `--prewarm-iters`            | `1`           | Unreported warmup iterations per ref                                                                                                                                                |
| `--compare-runs`             | `3`           | Measured runs per ref before aggregation. Decision mode requires `>= 5`.                                                                                                            |
| `--measure-order`            | `alternate`   | Run interleaving: `base-first`, `candidate-first`, `alternate`                                                                                                                      |
| `--aggregation`              | `median`      | Aggregation method: `min`, `median`, `p95`                                                                                                                                          |
| `--compare-mode`             | `exploratory` | Comparison policy for `compare.py`: `exploratory` or `decision`                                                                                                                     |
| `--fail-on`                  | —             | Comma-separated statuses that should force a non-zero compare exit (`expected_failure`, `improvement`, `incomparable`, `inconclusive`, `new`, `no_change`, `regression`, `removed`) |
| `--noise-threshold`          | `0.05`        | Minimum relative change to classify as regression/improvement                                                                                                                       |
| `--remote-runner`            | —             | SSH target for remote execution                                                                                                                                                     |
| `--remote-root`              | —             | Remote working directory                                                                                                                                                            |
| `--enforce-run-mode`         | —             | Require benchmark run mode                                                                                                                                                          |
| `--require-no-public-ipv4`   | —             | Require no public IPv4 address                                                                                                                                                      |
| `--require-egress-policy`    | —             | Require network egress policy                                                                                                                                                       |
| `--backend-profile`          | —             | Backend profile name                                                                                                                                                                |
| `--runner`                   | —             | Runner mode: `rust`, `python`, `all`                                                                                                                                                |
| `--mode`                     | `perf`        | Benchmark mode forwarded to `bench.sh run`                                                                                                                                          |
| `--dataset-id`               | —             | Dataset id forwarded to fixture generation and benchmark runs                                                                                                                       |
| `--timing-phase`             | `execute`     | Timing phase forwarded to `bench.sh run`                                                                                                                                            |

Self-hosted PR automation uses `--methodology-profile pr-macro` instead of restating raw decision knobs in workflow YAML. The profile currently resolves to `dataset_id=medium_selective`, `compare_mode=decision`, `compare_runs=7`, `aggregation=median`, `spread_metric=iqr_ms`, and `sub_ms_policy=micro_only`. Narrow diagnostic work stays outside the public methodology-profile contract and instead uses `./scripts/run_profile.sh scan-phase-criterion` for phase-isolated scan probes, `./scripts/run_profile.sh metadata-replay-criterion` for replay/provider diagnostics, `./scripts/run_profile.sh metadata-log-criterion`, which resolves to `metadata_log_bench`, for metadata/log internals, `./scripts/run_profile.sh file-selection-criterion`, which resolves to `file_selection_bench`, for shared DML file-selection seams, and `./scripts/run_profile.sh merge-filter-criterion`, which resolves to `merge_filter_bench`, for planning-only merge filter seams. The older `timing_phase=plan` approximation stays investigation-grade only.

### `run_profile.sh` — Invoke committed methodology profiles

```bash
./scripts/run_profile.sh [--dry-run] <profile> [profile-args...]
```

`run_profile.sh` resolves the named profile from `bench/methodologies/` and dispatches it to the correct entrypoint:

- compare profiles resolve to `./scripts/compare_branch.sh --methodology-profile <name> ...`
- run profiles resolve to `./scripts/bench.sh run ...`
- Criterion profiles resolve to `cargo bench -p delta-bench --bench <bench>`

Current committed Criterion profiles:

- `scan-phase-criterion` -> `scan_phase_bench`
- `metadata-replay-criterion` -> `metadata_replay_bench`
- `metadata-log-criterion` -> `metadata_log_bench`
- `file-selection-criterion` -> `file_selection_bench`
- `merge-filter-criterion` -> `merge_filter_bench`

Criterion profiles are diagnostic-only and intended for local or trusted self-hosted investigation. Pass extra Criterion arguments after `--` when you need a narrower filter or a saved baseline, for example `./scripts/run_profile.sh scan-phase-criterion -- scan_pruning_hit/phase/execute --save-baseline pr-base`.

PR pack automation reads `bench/evidence/registry.yaml`. The currently defined pack alias is `full`, which resolves to `pr-full-decision`. `run benchmark decision full` is therefore a pack request, not a suite request, and full does not mean --suite all.

`pr-full-decision` contains only `readiness=ready` suites. In plain terms, it contains only readiness=ready suites. Gated perf-owned suites move to `pr-candidate-manual` so operators can refresh manual/candidate evidence without widening PR comment automation. `pr-candidate-manual` currently carries `write_perf`, `delete_update_perf`, `merge_perf`, `optimize_perf`, `metadata_perf`, and `tpcds`; `delete_update_perf` uses `delete-update-perf-high-confidence` inside that pack so manual or nightly evidence keeps the longer compare shape, and `tpcds` remains candidate/manual in that pack until its gates are fully closed.

Keep the entrypoints split:

- Ready PR comment grammar: `run benchmark scan`, `run benchmark decision scan`, `run benchmark decision full`
- Blocking PR DML gate: `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-delete-update-perf delete_update_perf`
- Candidate/manual operator compares: `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-write-perf write_perf`, `./scripts/compare_branch.sh --current-vs-main --methodology-profile delete-update-perf-high-confidence delete_update_perf`, `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-merge-perf merge_perf`, `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-optimize-perf optimize_perf`, `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-metadata-perf metadata_perf`, `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-tpcds tpcds`
- Diagnostic Criterion probes: `./scripts/run_profile.sh scan-phase-criterion`, `./scripts/run_profile.sh metadata-replay-criterion`, `./scripts/run_profile.sh metadata-log-criterion`, `./scripts/run_profile.sh file-selection-criterion`, `./scripts/run_profile.sh merge-filter-criterion`

Remote candidate/manual surfaces use the same registry/profile model. The currently declared S3 surface ids are `scan_s3`, `delete_update_perf_s3`, `merge_perf_s3`, `optimize_perf_s3`, and `metadata_perf_s3`; their backing methodology profiles pin `storage_backend=s3` and `backend_profile=s3_locking_vultr`, and the `s3-candidate-manual` pack batches those remote shards through the same compare flow. `write_perf_s3` is declared separately but remains gated until non-local write throughput evidence is complete.

Compare automation artifacts are written to `results/compare/<suite>/<base_sha>__<candidate_sha>/`. The directory includes:

| Artifact | Description |
| -------- | ----------- |
| `stdout.txt` | Plain-text compare report captured for automation |
| `summary.md` | Markdown compare report |
| `comparison.json` | Versioned JSON compare payload with `schema_version`, `metadata`, `summary`, and `rows` |
| `hash-policy.txt` | Hash/schema compatibility report for the aggregated base and candidate payloads across all observed sample hashes |
| `manifest.json` | JSON manifest with suite, SHAs, compare settings, methodology metadata, aggregated input paths, and artifact paths |

`comparison.json` schema version `1` includes top-level `schema_version`, `metadata`, `summary`, and `rows`. Each row contains `case`, `status`, `display_change`, `baseline_ms`, `candidate_ms`, `delta_pct`, `decision_scope`, `scope_reason`, `spread_metric`, `baseline_spread_ms`, and `candidate_spread_ms`. `decision_scope` is `macro` for rows that count toward the main summaries and `micro_only` when the selected methodology excludes a sub-millisecond comparison from macro evidence. Comparable `micro_only` rows move to the out-of-scope section; unresolved rows such as `inconclusive` still remain actionable.

`manifest.json` contains these top-level keys: `suite`, `base_sha`, `candidate_sha`, `base_json`, `candidate_json`, `stdout_report`, `markdown_report`, `comparison_json`, `hash_policy_report`, `compare_mode`, `aggregation`, `noise_threshold`, `methodology_profile`, `methodology_version`, and `methodology_settings`.

`methodology_settings` records the resolved compare settings used for the run: `compare_mode`, `warmup`, `iters`, `prewarm_iters`, `compare_runs`, `measure_order`, `timing_phase`, `aggregation`, `dataset_id`, `dataset_policy`, `spread_metric`, `sub_ms_threshold_ms`, `sub_ms_policy`, `storage_backend`, and `backend_profile`. For an exact `pr-macro` invocation, `methodology_profile` and `methodology_version` identify that canonical self-hosted PR macro contract. If explicit CLI overrides change any profile-owned setting, the manifest leaves those two top-level fields null and relies on `methodology_settings` to describe the effective non-canonical run, including the resolved storage contract for remote/object-store surfaces.

Pack automation writes an additional bundle under `results/compare/packs/<pack_id>/<base_sha>__<candidate_sha>/` with the same stable filenames: `summary.md`, `comparison.json`, `hash-policy.txt`, and `manifest.json`. Pack-level `comparison.json` flattens suite rows and adds a required `suite` field.

`compare_branch.sh` keeps a clean source checkout at `.delta-rs-source/` by default for branch lookup, immutable SHA pinning, and seeding per-SHA compare checkouts. `--current-vs-main` is the one exception: it seeds the candidate prepared checkout from `DELTA_RS_DIR` so the current local HEAD remains reachable even when it is not present in the clean source checkout.

Decision compare accepts only schema v5 aggregated inputs and fails closed when `--compare-runs` is below the required case minimum.

### `validate_perf_harness.sh` — Trust contract verification

```bash
./scripts/validate_perf_harness.sh [--sha <commit>] [--fetch-url <url>] [--fetch-ref <ref>] [--dataset-id <id>] [--artifact-dir <path>]
```

### `publish_contract.sh` — Publish the operator contract bundle

```bash
./scripts/publish_contract.sh [--output-dir <path>]
```

Copies the current README, key docs, manifests, and wrapper entrypoints into `results/contracts/` (or a caller-selected output directory) and writes a machine-readable `manifest.json` that records the published contract bundle and result schema version.

Runs the focused trust-contract suites used to justify trustworthy PR perf claims. The validator always reruns the scan trust contract first, then uses the stable artifact-dir basename to decide which follow-on gate family to run: `write-perf-ready` / `write-perf-gate` add only `write_perf`, `dml-maintenance-gate` adds only `delete_update_perf`, `merge_perf`, and `optimize_perf`, `metadata-perf-gate` adds only `metadata_perf`, and `tpcds-gate` adds only `tpcds` when `--dataset-id tpcds_duckdb` is present. Within `dml-maintenance-gate`, the validator uses one-pass suite coverage probes, canary-only same-SHA compares, and the dedicated `VALIDATION_DML_CANARY_ITERS` budget for the heavy DML suites so the staging path stays inside the phase timeout budget. Timestamped or otherwise unrecognized artifact dirs keep the full chain. By default the script writes a timestamped artifact tree under `results/validation/` and stores the Markdown summary at `summary.md` inside that directory. Use `--artifact-dir` to choose a stable directory such as `results/validation/latest`.

If `--sha <commit>` points at an immutable ref that `origin` does not advertise, pass `--fetch-url <trusted-clone-url>` and optionally `--fetch-ref <ref>` (or set `VALIDATION_FETCH_URL` / `VALIDATION_FETCH_REF`). The validator prepares `.delta-rs-under-test` at that SHA first, then seeds same-SHA compare pinning into `.delta-rs-source` from the prepared execution checkout.

The same validator covers the candidate/manual perf-owned DML and maintenance suites too when you target `results/validation/dml-maintenance-gate`. Their operator compare entrypoints are:

```bash
./scripts/compare_branch.sh --current-vs-main --methodology-profile delete-update-perf-high-confidence delete_update_perf
./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-merge-perf merge_perf
./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-optimize-perf optimize_perf
./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-metadata-perf metadata_perf
```

Passing `--dataset-id tpcds_duckdb` enables the dedicated TPC-DS promotion gate in the same script when the artifact dir resolves to the `tpcds` scope (for example `results/validation/tpcds-gate`). The primary scan and non-TPC-DS validation blocks remain pinned to their own contract datasets. The operator-facing invocation is:

```bash
./scripts/validate_perf_harness.sh --dataset-id tpcds_duckdb --artifact-dir results/validation/tpcds-gate
```

Use `pr-candidate-manual` when you want the harness-owned multi-suite manual/candidate pack for gated perf suites; it is intentionally separate from the `full` PR automation alias.

### `longitudinal_bench.sh` — Longitudinal pipeline

Subcommands: `select-revisions`, `build-artifacts`, `run-matrix`, `ingest-results`, `report`, `prune`, `orchestrate`. `run-matrix` writes a resumable `matrix-state.json` checkpoint, includes `--lane` in its resume fingerprint, and `ingest-results` / `report` / `prune` operate on a SQLite `store.sqlite3` store. See [Longitudinal Benchmarking](longitudinal.md) for full flag documentation per subcommand.

### `cleanup_local.sh` — Safe artifact cleanup

| Flag                    | Default | Description                              |
| ----------------------- | ------- | ---------------------------------------- |
| `--results`             | —       | Target result files                      |
| `--compare-checkouts`   | —       | Target cached compare checkouts          |
| `--fixtures`            | —       | Target fixture data                      |
| `--delta-rs-source`     | —       | Target clean source checkout             |
| `--delta-rs-under-test` | —       | Target managed checkout                  |
| `--keep-last N`         | —       | Retain N most recent result or checkout entries |
| `--older-than-days N`   | —       | Only remove items older than N days      |
| `--allow-outside-root`  | —       | Allow cleanup outside repo root          |
| `--apply`               | `false` | Execute deletions (dry-run without this) |

## Repository Verification Baseline

These commands are the current repo-wide baseline for code, dependency, and self-hosted control-plane verification.

| Scope                    | Command                                                                                           | Notes                                                                                      |
| ------------------------ | ------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------ |
| Rust tests               | `cargo test --locked`                                                                             | Matches the primary CI test job.                                                           |
| Python tests             | `(cd python && python3 -m pytest -q tests)`                                                       | Matches the primary CI test job.                                                           |
| Harness trust validation | `./scripts/validate_perf_harness.sh`                                                              | Required before using a new machine/workflow combination for authoritative PR perf claims. |
| Rust dependency audit    | `cargo audit --ignore RUSTSEC-2026-0037 --ignore RUSTSEC-2026-0041 --ignore RUSTSEC-2026-0049`    | Temporary triage for known upstream advisories; new advisories still fail the job.         |
| Python dependency audit  | `python3 -m pip_audit -r python/requirements-audit.txt`                                           | Audits the actual interop/runtime dependency set.                                          |
| Self-hosted preflight    | `./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy` | Required by self-hosted benchmark and longitudinal workflows before execution.             |

## Environment Variables

### CLI configuration

| Variable                             | Default    | Description                               |
| ------------------------------------ | ---------- | ----------------------------------------- |
| `DELTA_BENCH_FIXTURES`               | `fixtures` | Fixture data directory                    |
| `DELTA_BENCH_RESULTS`                | `results`  | Result output directory                   |
| `DELTA_BENCH_LABEL`                  | `local`    | Run identifier                            |
| `DELTA_BENCH_STORAGE_BACKEND`        | `local`    | Storage backend (`local` or `s3`)         |
| `DELTA_BENCH_BACKEND_PROFILE`        | —          | Backend profile from `backends/`          |
| `DELTA_BENCH_SUPPRESS_RUST_WARNINGS` | `1`        | Set to `0` to show Rust compiler warnings |

### TPC-DS and DuckDB

| Variable                              | Default   | Description                                        |
| ------------------------------------- | --------- | -------------------------------------------------- |
| `DELTA_BENCH_DUCKDB_PYTHON`           | `python3` | Python executable for DuckDB fixture generation    |
| `DELTA_BENCH_TPCDS_DUCKDB_SCRIPT`     | —         | Override path to TPC-DS DuckDB script              |
| `DELTA_BENCH_TPCDS_DUCKDB_TIMEOUT_MS` | `600000`  | Timeout for DuckDB fixture generation (10 minutes) |

### Fixture locking

| Variable                              | Default  | Description                                    |
| ------------------------------------- | -------- | ---------------------------------------------- |
| `DELTA_BENCH_FIXTURE_LOCK_TIMEOUT_MS` | `120000` | Timeout for acquiring fixture lock (2 minutes) |
| `DELTA_BENCH_FIXTURE_LOCK_RETRY_MS`   | `50`     | Retry interval for fixture lock acquisition    |

### Python interop

| Variable                         | Default   | Description                                   |
| -------------------------------- | --------- | --------------------------------------------- |
| `DELTA_BENCH_INTEROP_TIMEOUT_MS` | `120000`  | Timeout for Python interop cases (2 minutes)  |
| `DELTA_BENCH_INTEROP_RETRIES`    | `1`       | Number of retries for Python interop failures |
| `DELTA_BENCH_INTEROP_PYTHON`     | `python3` | Python executable for interop cases           |

### Script-level configuration

| Variable                    | Default | Description                                                                    |
| --------------------------- | ------- | ------------------------------------------------------------------------------ |
| `BENCH_TIMEOUT_SECONDS`     | `3600`  | Per-step timeout cap for compare/longitudinal scripts                          |
| `BENCH_RETRY_ATTEMPTS`      | `2`     | Retry count for transient benchmark failures                                   |
| `BENCH_RETRY_DELAY_SECONDS` | `5`     | Delay between retries                                                          |
| `BENCH_STORAGE_BACKEND`     | `local` | Storage backend for script-level workflows                                     |
| `BENCH_STORAGE_OPTIONS`     | —       | Multi-line `KEY=VALUE` storage options                                         |
| `BENCH_BACKEND_PROFILE`     | —       | Backend profile for script-level workflows                                     |
| `BENCH_RUNNER_MODE`         | —       | Runner mode for script-level workflows (`rust`, `python`, `all`)               |
| `DELTA_BENCH_BOT_DB_PATH`   | —       | Shared filesystem path for `benchmark.yml` PR bot state; must resolve on every runner that can execute queue/pack automation |
| `BENCH_BENCHMARK_MODE`      | `perf`  | Benchmark mode for script-level workflows (`perf`, `assert`)                   |
| `BENCH_COMPARE_FAIL_ON`     | —       | Default `--fail-on` statuses for `compare_branch.sh` / `compare.py` automation |
| `DELTA_RS_SOURCE_DIR`       | `${RUNNER_ROOT}/.delta-rs-source` | Clean checkout used for compare ref resolution and per-SHA checkout seeding |
| `DELTA_BENCH_MIN_FREE_GB`   | `20`    | Local-only free-space floor enforced by `compare_branch.sh` before preparing compare checkouts |
| `DELTA_BENCH_COMPARE_CHECKOUT_ROOT` | `${RUNNER_ROOT}/.delta-bench-compare-checkouts` | Root directory for per-ref prepared compare checkouts |

### delta-rs checkout preparation

`prepare_delta_rs.sh` supports immutable SHAs that live on a trusted alternate remote instead of `origin`. Prefer the full 40-character SHA when you use `DELTA_RS_FETCH_URL`.

| Variable                      | Default       | Description                                                                              |
| ----------------------------- | ------------- | ---------------------------------------------------------------------------------------- |
| `DELTA_RS_FETCH_URL`          | —             | Alternate fetch URL used when `DELTA_RS_REF` is not reachable from `origin`             |
| `DELTA_RS_FETCH_REF`          | `DELTA_RS_REF`| Optional advertised branch/ref to fetch when the checkout commit differs from the fetch target |

### Validation checkout overrides

These are validation-only controls used by `./scripts/validate_perf_harness.sh` when a trusted runner needs to prepare an immutable SHA before the same-SHA compare seeds `.delta-rs-source` from `.delta-rs-under-test`.

| Variable                      | Default          | Description                                                                 |
| ----------------------------- | ---------------- | --------------------------------------------------------------------------- |
| `VALIDATION_FETCH_URL`        | —                | Alternate fetch URL used when `VALIDATION_SHA` is not reachable from `origin` |
| `VALIDATION_FETCH_REF`        | `VALIDATION_SHA` | Optional advertised branch/ref to fetch before resolving `VALIDATION_SHA`   |

### Scan phase canary injection

These are validation-only controls used by `./scripts/validate_perf_harness.sh` and the scan phase canary tests.

| Variable                             | Default | Description                                                                         |
| ------------------------------------ | ------- | ----------------------------------------------------------------------------------- |
| `DELTA_BENCH_ALLOW_SCAN_PHASE_DELAY` | —       | Must be set to `1` before the validation-only scan delay env vars below are honored |
| `DELTA_BENCH_SCAN_DELAY_LOAD_MS`     | —       | Injects a fixed delay into scan load timing                                         |
| `DELTA_BENCH_SCAN_DELAY_PLAN_MS`     | —       | Injects a fixed delay into scan plan timing                                         |
| `DELTA_BENCH_SCAN_DELAY_EXECUTE_MS`  | —       | Injects a fixed delay into scan execute timing                                      |
| `DELTA_BENCH_SCAN_DELAY_VALIDATE_MS` | —       | Injects a fixed delay into scan validate timing                                     |

### TPC-DS canary injection

These are validation-only controls used by the dedicated `tpcds` block in `./scripts/validate_perf_harness.sh`.

| Variable                         | Default | Description                                                                  |
| -------------------------------- | ------- | ---------------------------------------------------------------------------- |
| `DELTA_BENCH_ALLOW_TPCDS_DELAY`  | —       | Must be set to `1` before the validation-only TPC-DS execute delay is honored |
| `DELTA_BENCH_TPCDS_DELAY_MS`     | —       | Injects a fixed delay into the validation canary execute path for `tpcds_q03` |

### Perf-owned DML and maintenance canary injection

These are validation-only controls used by the dedicated `delete_update_perf`, `merge_perf`, and `optimize_perf` blocks in `./scripts/validate_perf_harness.sh`.

| Variable                                   | Default | Description |
| ------------------------------------------ | ------- | ----------- |
| `DELTA_BENCH_ALLOW_DELETE_UPDATE_PERF_DELAY` | —     | Must be set to `1` before the validation-only DML perf delay is honored |
| `DELTA_BENCH_DELETE_UPDATE_PERF_DELAY_MS`  | —       | Injects a fixed delay into `delete_perf_scattered_5pct_small_files` |
| `DELTA_BENCH_ALLOW_MERGE_PERF_DELAY`       | —       | Must be set to `1` before the validation-only merge perf delay is honored |
| `DELTA_BENCH_MERGE_PERF_DELAY_MS`          | —       | Injects a fixed delay into `merge_perf_upsert_50pct` |
| `DELTA_BENCH_ALLOW_OPTIMIZE_PERF_DELAY`    | —       | Must be set to `1` before the validation-only maintenance perf delay is honored |
| `DELTA_BENCH_OPTIMIZE_PERF_DELAY_MS`       | —       | Injects a fixed delay into `optimize_perf_compact_small_files` |

### Fidelity and hardening

These are captured in result metadata when running on cloud/hardened infrastructure.

| Variable                               | Default | Description                           |
| -------------------------------------- | ------- | ------------------------------------- |
| `DELTA_BENCH_IMAGE_VERSION`            | —       | Container or VM image version         |
| `DELTA_BENCH_HARDENING_PROFILE_ID`     | —       | System hardening profile identifier   |
| `DELTA_BENCH_HARDENING_PROFILE_SHA256` | —       | SHA256 of the hardening configuration |
| `DELTA_BENCH_HARDENING_PROFILE_PATH`   | —       | Path to hardening profile file        |
| `DELTA_BENCH_EGRESS_POLICY_SHA256`     | —       | SHA256 of network egress policy       |
| `DELTA_BENCH_EGRESS_POLICY_PATH`       | —       | Path to egress policy file            |
| `DELTA_BENCH_RUN_MODE`                 | —       | Security/execution mode               |
| `DELTA_BENCH_RUN_MODE_PATH`            | —       | Path to run mode configuration        |
| `DELTA_BENCH_MAINTENANCE_WINDOW_ID`    | —       | Maintenance window identifier         |

## Longitudinal State and Store

The longitudinal pipeline persists two primary artifacts beyond the raw `results/<label>/<suite>.json` files:

| Artifact            | Typical location                                                                                               | Description                                                                                                                                                                              |
| ------------------- | -------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `matrix-state.json` | `longitudinal/state/matrix-state.json` or lane-specific `longitudinal/releases/<lane>/state/matrix-state.json` | JSON checkpoint for `run-matrix`. Stores per-cell status and a top-level `config` fingerprint so resume only happens against the same suite/scale/lane/warmup/iteration/output contract. |
| `store.sqlite3`     | `longitudinal/store/store.sqlite3` or lane-specific `longitudinal/releases/<lane>/store/store.sqlite3`         | SQLite database populated by `ingest-results`. Holds normalized run metadata and case rows used by `report` and `prune`. Duplicate ingests are deduplicated by run id.                   |

Legacy `rows.jsonl` / `index.json` stores are no longer a supported primary path. If those files exist without `store.sqlite3`, ingest/report/prune fail fast so the operator can migrate or remove the stale state intentionally.

## Datasets and Scales

### Dataset IDs

| Dataset ID         | Scale | Fixture profile | Description                                                              |
| ------------------ | ----- | --------------- | ------------------------------------------------------------------------ |
| `tiny_smoke`       | sf1   | Standard        | Minimal smoke test. Fast to generate.                                    |
| `medium_selective` | sf10  | Standard        | Medium workload with selective query patterns.                           |
| `small_files`      | sf1   | Standard        | Many small files for optimize/vacuum testing.                            |
| `tpcds_duckdb`     | sf1   | TpcdsDuckdb     | TPC-DS `store_sales` sourced from DuckDB. Requires `python3` + `duckdb`. |

`tiny_smoke` is the fast setup and smoke dataset. The self-hosted `pr-macro` compare profile deliberately upgrades branch comparison to `medium_selective` so the authoritative macro scan cases spend more time doing real scan work and less time in timer-noise territory.

Exact-result manifest hashes are authored against the default `tiny_smoke` corpus. When you run with another dataset id, the harness keeps schema validation enabled but relaxes exact-result hash assertions at runtime because scale/profile changes legitimately alter row-level digests.

### Scale factors

| Scale   | Row count | Description                                      |
| ------- | --------- | ------------------------------------------------ |
| `sf1`   | 10,000    | Small. Good for smoke tests and development.     |
| `sf10`  | 100,000   | Medium. Realistic for selective query patterns.  |
| `sf100` | 1,000,000 | Large. For production-representative benchmarks. |

### Fixture profiles

| Profile      | Used by                                         | Behavior                                                                    |
| ------------ | ----------------------------------------------- | --------------------------------------------------------------------------- |
| Standard     | `tiny_smoke`, `medium_selective`, `small_files` | Normal fixture generation                                                   |
| TpcdsDuckdb  | `tpcds_duckdb`                                  | Loads TPC-DS data via DuckDB `dsdgen`, exports through CSV, writes to Delta |

## Fixture Tables

All fixture tables are generated under `<fixtures_dir>/<scale>/`.

| Table                     | Directory                         | Purpose                                         |
| ------------------------- | --------------------------------- | ----------------------------------------------- |
| Narrow sales              | `narrow_sales_delta`              | Base table for scan and read benchmarks         |
| Read partitioned          | `read_partitioned_delta`          | Partitioned table for pruning tests             |
| Merge target              | `merge_target_delta`              | Standard merge target                           |
| Merge partitioned target  | `merge_partitioned_target_delta`  | Partitioned merge target for localized merge    |
| Delete/update small files | `delete_update_small_files_delta` | Small-file layout for scattered DML             |
| Optimize small files      | `optimize_small_files_delta`      | Small files for compaction testing              |
| Optimize compacted        | `optimize_compacted_delta`        | Already-compacted table for no-op optimize test |
| Vacuum ready              | `vacuum_ready_delta`              | Table with expired files for vacuum testing     |
| Metadata long history     | `metadata_long_history_delta`     | Long uncheckpointed metadata/log replay history |
| Metadata checkpointed     | `metadata_checkpointed_delta`     | Comparable metadata head load with checkpoint hint |
| Metadata uncheckpointed   | `metadata_uncheckpointed_delta`   | Comparable metadata head load without checkpoint hint |
| TPC-DS store_sales        | `tpcds/store_sales`               | TPC-DS `store_sales` table                      |

Additional fixture artifacts:

- `rows.jsonl` — JSON-lines snapshot of the source row data
- `manifest.json` — Fixture generation metadata (schema version, seed, scale, fingerprint)

## Result Schema v5

### Top-level structure

| Field            | Type   | Description                                    |
| ---------------- | ------ | ---------------------------------------------- |
| `schema_version` | u32    | Format version (currently 5)                   |
| `context`        | object | Host, run configuration, and fidelity metadata |
| `cases`          | array  | Array of benchmark case results                |

### Context fields

| Field                  | Type     | Required | Description                                                                             |
| ---------------------- | -------- | -------- | --------------------------------------------------------------------------------------- |
| `host`                 | string   | yes      | Machine hostname                                                                        |
| `label`                | string   | yes      | Run label identifier                                                                    |
| `git_sha`              | string   | no       | Git SHA of the revision under test                                                      |
| `created_at`           | datetime | yes      | Timestamp of result creation                                                            |
| `suite`                | string   | yes      | Benchmark suite name                                                                    |
| `scale`                | string   | yes      | Scale factor                                                                            |
| `iterations`           | u32      | yes      | Measured iterations per case                                                            |
| `warmup`               | u32      | yes      | Warmup iterations per case                                                              |
| `timing_phase`         | string   | no       | Selected timing phase (`load`, `plan`, `execute`, or `validate`) for phase-aware suites |
| `dataset_id`           | string   | no       | Dataset identifier                                                                      |
| `dataset_fingerprint`  | string   | no       | Hash of the fixture data                                                                |
| `runner`               | string   | no       | Runner mode (rust/python)                                                               |
| `storage_backend`      | string   | no       | Storage backend used for the run (`local` or `s3`)                                      |
| `benchmark_mode`       | string   | no       | Benchmark mode for the artifact (`perf` or `assert`)                                    |
| `lane`                 | string   | no       | Benchmark lane (`smoke`, `correctness`, `macro`)                                        |
| `measurement_kind`     | string   | no       | Timing contract (`end_to_end` or `phase_breakdown`)                                     |
| `validation_level`     | string   | no       | Validation contract (`operational` or `semantic`)                                       |
| `run_id`               | string   | no       | Unique id for the benchmark run                                                         |
| `harness_revision`     | string   | no       | Benchmark harness revision                                                              |
| `fixture_recipe_hash`  | string   | no       | Hash of the fixture recipe contract                                                     |
| `fidelity_fingerprint` | string   | no       | Hash of the fidelity/environment envelope                                               |
| `backend_profile`      | string   | no       | Backend profile name                                                                    |

### Fidelity and security context fields

These are populated when running on cloud/hardened infrastructure.

| Field                      | Type   | Description                       |
| -------------------------- | ------ | --------------------------------- |
| `image_version`            | string | Container or VM image version     |
| `hardening_profile_id`     | string | System hardening profile ID       |
| `hardening_profile_sha256` | string | SHA256 of hardening configuration |
| `cpu_model`                | string | CPU model identifier              |
| `cpu_microcode`            | string | CPU microcode version             |
| `kernel`                   | string | Kernel version                    |
| `boot_params`              | string | Kernel boot parameters            |
| `cpu_steal_pct`            | f64    | CPU steal percentage (cloud VMs)  |
| `numa_topology`            | string | NUMA topology description         |
| `egress_policy_sha256`     | string | SHA256 of network egress policy   |
| `run_mode`                 | string | Benchmark run mode                |
| `maintenance_window_id`    | string | Maintenance window identifier     |

### Case-level fields

| Field                    | Type   | Description                                                                                                                                      |
| ------------------------ | ------ | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `case`                   | string | Case name (e.g., `scan_full_narrow`)                                                                                                             |
| `success`                | bool   | Whether the case satisfied workload validation                                                                                                   |
| `validation_passed`      | bool   | Whether correctness/assertion validation passed                                                                                                  |
| `perf_status`            | string | Performance evidence status: `trusted`, `validation_only`, or `invalid`. Smoke, correctness, assert, and correctness-tagged macro runs are not `trusted`. |
| `classification`         | string | `supported` or `expected_failure`                                                                                                                |
| `samples`                | array  | Per-iteration timing and metrics                                                                                                                 |
| `run_summary`            | object | Run-level summary consumed by automation and decision mode                                                                                       |
| `run_summaries`          | array  | Aggregated list of run summaries when multiple runs are merged                                                                                   |
| `suite_manifest_hash`    | string | Hash of the manifest file that defined the case                                                                                                  |
| `case_definition_hash`   | string | Hash of the case definition in the manifest                                                                                                      |
| `compatibility_key`      | string | Derived key for strict comparison compatibility. In schema v5 it hashes the full comparison identity plus case-definition and decision metadata. |
| `supports_decision`      | bool   | Whether the case participates in decision-grade compare mode                                                                                     |
| `required_runs`          | u32    | Minimum runs required for decision mode                                                                                                          |
| `decision_threshold_pct` | f64    | Regression threshold for decision mode                                                                                                           |
| `decision_metric`        | string | Run summary metric used for decision mode                                                                                                        |
| `failure_kind`           | string | Failure class such as `execution_error`, `assertion_mismatch`, `context_mismatch`, or `unsupported`                                              |
| `failure`                | string | Error message if the case failed                                                                                                                 |
| `elapsed_stats`          | object | Timing statistics across samples when `perf_status=trusted` (see [Elapsed statistics](#elapsed-statistics))                                      |

### Sample-level fields

Each sample represents one measured iteration.

| Field                | Type   | Description                                                                                                 |
| -------------------- | ------ | ----------------------------------------------------------------------------------------------------------- |
| `elapsed_ms`         | f64    | Timed duration for this iteration; on phase-aware suites this reflects the selected isolated `timing_phase` |
| `metrics`            | object | Metric fields (see [Metrics Reference](#metrics-reference))                                                 |
| `metrics.contention` | object | Optional nested contention metrics domain emitted by `concurrency`                                          |

Schema v5 is the only authoritative result format. Decision mode, compare aggregation, and authoritative longitudinal workflows all require schema v5 with complete identity fields and explicit `perf_status`.

## Manifest Format

Benchmark manifests declare which cases to execute and what assertions to validate. Core manifests live in `bench/manifests/`. Assertions are interpreted against the run's `dataset_id` and `dataset_fingerprint`; if the workload context drifts, the artifact is validation-only and cannot be compared as perf.

Assertion freshness rules:

- Refresh `exact_result_hash` and `schema_hash` after intentional semantic changes to the case or fixture recipe.
- Treat any artifact produced against a different `fixture_recipe_hash`, `dataset_fingerprint`, or `compatibility_key` as stale for perf comparison.
- `assert` mode is the refresh-and-validate path; `perf` mode is the compare path. Do not mix them.

| Manifest           | Runner | Description          |
| ------------------ | ------ | -------------------- |
| `core_rust.yaml`   | rust   | All Rust suite cases |
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

| Type                      | Value format    | Description                                    |
| ------------------------- | --------------- | ---------------------------------------------- |
| `exact_result_hash`       | `sha256:<hash>` | SHA256 of query result data must match         |
| `schema_hash`             | `sha256:<hash>` | SHA256 of result schema must match             |
| `expected_error_contains` | `<substring>`   | Error message must contain this substring      |
| `version_monotonicity`    | —               | Table version must be monotonically increasing |

### Case classifications

| Classification     | Meaning                                                                               |
| ------------------ | ------------------------------------------------------------------------------------- |
| `supported`        | Normal case — expected to succeed                                                     |
| `expected_failure` | Case is expected to fail (e.g., unsupported operation). Failure is the passing state. |

## Backend Profiles

Backend profiles store storage configuration defaults in `backends/<name>.env` files.

### Available profiles

| Profile            | Description                               |
| ------------------ | ----------------------------------------- |
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
