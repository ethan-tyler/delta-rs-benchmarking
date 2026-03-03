# Architecture

Reference documentation for component boundaries, execution data flow, schema v2, and reproducibility controls.
For contributor task workflows, use [user-guide.md](user-guide.md).

## Components

- `crates/delta-bench`: Rust CLI and benchmark execution engine
- `python/delta_bench_compare`: result comparison and rendering
- `python/delta_bench_interop`: Python interop benchmark cases (`pandas`, `polars`, `pyarrow`)
- `python/delta_bench_tpcds`: DuckDB-backed `store_sales` fixture generation script for `tpcds_duckdb`
- `bench/manifests/*.yaml`: required benchmark catalogs and execution planning source
- `backends/*.env`: backend profile defaults
- `scripts/prepare_delta_rs.sh`: manages checkout at `.delta-rs-under-test`
- `scripts/sync_harness_to_delta_rs.sh`: syncs harness components into checkout workspace
- `scripts/bench.sh`: wrapper for `delta-bench` subcommands
- `scripts/compare_branch.sh`: multi-run base-vs-candidate orchestration with aggregation
- `scripts/security_mode.sh`: toggles benchmark run mode vs maintenance mode
- `scripts/security_check.sh`: preflight guardrails for mode, interface exposure, and egress policy
- `scripts/provision_runner.sh`: Terraform orchestration wrapper for runner provisioning

## Data Flow

1. `delta-bench data` generates deterministic fixtures under `fixtures/<scale>/`.
2. Fixture generation writes JSON row snapshots plus concrete Delta tables (`narrow_sales_delta`, `merge_target_delta`, `read_partitioned_delta`, `merge_partitioned_target_delta`, `optimize_small_files_delta`, `optimize_compacted_delta`, `vacuum_ready_delta`, `tpcds/store_sales`).
3. For `dataset_id=tpcds_duckdb`, `tpcds/store_sales` is sourced from DuckDB `tpcds` extension (`INSTALL tpcds; LOAD tpcds; CALL dsdgen(...)`) and exported through CSV before Delta write.
4. `delta-bench run` resolves runner mode (`rust|python|all`) from manifest-planned cases and executes Rust suites and Python interop cases.
5. `delta-bench run` prints a per-case terminal summary table and writes schema v2 JSON results to `results/<label>/<suite>.json`.
6. `compare.py` reads baseline and candidate schema v2 JSON and classifies per-case changes.
7. `security_check.sh` validates security and fidelity invariants before benchmark execution.
8. compare workflow produces grouped text output by default (and `compare.py` supports markdown output when needed).

Marketplace datasets are currently document-only: place externally provisioned Delta tables under expected `fixtures/<scale>/...` roots.

## Result Schema v2

Top-level fields:

- `schema_version`
- `context`
- `cases`

Context fields include host and run-shaping metadata:

- host, label, git SHA, suite, scale, warmup/iterations, timestamp
- optional run-shaping: `dataset_id`, `dataset_fingerprint`, `runner`, `backend_profile`

Optional fidelity/security context fields:

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

Case-level fields:

- success/failure outcome
- required classification (`supported` or `expected_failure`)
- sample timings and optional failure payload

Sample-level normalized metrics:

- base: `rows_processed`, `bytes_processed`, `operations`, `table_version`
- optional scan/rewrite: `files_scanned`, `files_pruned`, `bytes_scanned`, `scan_time_ms`, `rewrite_time_ms`
- optional runtime/io/result: `peak_rss_mb`, `cpu_time_ms`, `bytes_read`, `bytes_written`, `files_touched`, `files_skipped`, `spill_bytes`, `result_hash`, `schema_hash`
- optional case-level elapsed aggregates: `min_ms`, `max_ms`, `mean_ms`, `median_ms`, `stddev_ms`, `cv_pct`

Source mapping highlights:

- `scan`: physical-plan metrics (`files_scanned`, `files_pruned`, `bytes_scanned`, `scan_time_ms`)
- `merge`: `MergeMetrics` (`files_scanned`, `files_pruned`, `scan_time_ms`, `rewrite_time_ms`)
- `optimize_vacuum` optimize cases: considered/skipped counts (`files_scanned`, `files_pruned`)

## Benchmark Coverage Additions

- `scan` includes pruning contrast cases (`scan_pruning_hit`, `scan_pruning_miss`)
- `merge` includes localized partition-aware case (`merge_localized_1pct`)
- `optimize_vacuum` includes noop-vs-heavy compaction cases (`optimize_noop_already_compact`, `optimize_heavy_compaction`)

## Reproducibility Controls

- deterministic seed-based fixture generation
- deterministic manifest ordering for `core-rust` and `core-python` lanes
- single-machine branch comparisons with optional prewarm runs
- repeated measured runs per ref with configurable order (`base-first`, `candidate-first`, `alternate`)
- aggregation across runs before change classification
- stable default no-change threshold (`0.05`)
- explicit benchmark run mode for noise reduction

## Related Guides

- [User Guide](user-guide.md)
- [Longitudinal CLI Guide](longitudinal-cli.md)
- [Security Runbook](security-runner.md)
