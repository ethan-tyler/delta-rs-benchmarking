# Architecture

## Components

- `crates/delta-bench`: Rust CLI + benchmark execution engine.
- `python/delta_bench_compare`: result comparison and rendering.
- `python/delta_bench_interop`: targeted Python interop benchmark cases (`pandas`, `polars`, `pyarrow`).
- `bench/manifests/*.yaml`: required benchmark catalogs used as the only execution planning source.
- `backends/*.env`: backend profile defaults (provider-specific and custom profiles).
- `scripts/prepare_delta_rs.sh`: manages local checkout at `.delta-rs-under-test`.
- `scripts/sync_harness_to_delta_rs.sh`: syncs `crates/delta-bench`, benchmark manifests, backend profiles, and Python interop runner into the checked-out `delta-rs` workspace.
- `scripts/bench.sh`: wraps `delta-bench` subcommands.
- `scripts/compare_branch.sh`: sequential base vs candidate run orchestration.
- `scripts/security_mode.sh`: toggles benchmark run mode vs maintenance mode.
- `scripts/security_check.sh`: preflight guardrails for run mode, interface exposure, and egress policy drift.
- `scripts/provision_runner.sh`: Terraform orchestration wrapper for runner provisioning operations.

## Data flow

1. `delta-bench data` generates deterministic fixtures under `fixtures/<scale>/`.
2. Fixture generation writes both JSON row snapshots and concrete Delta tables (`narrow_sales_delta`, `merge_target_delta`, `read_partitioned_delta`, `merge_partitioned_target_delta`, `optimize_small_files_delta`, `optimize_compacted_delta`, `vacuum_ready_delta`).
3. `delta-bench run` resolves runner mode (`rust|python|all`) from manifest-planned cases and executes:
   - Rust suites against `deltalake-core`
   - Python interoperability suite through `python/delta_bench_interop/run_case.py`
4. Results are written as schema v2 JSON to `results/<label>/<suite>.json`.
5. `compare.py` reads baseline/candidate result JSON (schema v2 only) and classifies per-case changes.
6. `security_check.sh` runs before benchmark execution to validate security/fidelity invariants.
7. Manual compare script prints markdown output suitable for PR comments.

## Result schema v2

- Top-level: `schema_version`, `context`, `cases`
- Context: host, label, git SHA, suite, scale, warmup/iterations, timestamp
- Optional context run-shaping fields:
  - `dataset_id`
  - `dataset_fingerprint`
  - `runner`
  - `backend_profile`
- Optional context fidelity/security fields:
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
- Cases: success/failure, required classification (`supported` or `expected_failure`), sample timings, failure payload when applicable
- Samples: `elapsed_ms` plus normalized `metrics`:
  - Base fields: `rows_processed`, `bytes_processed`, `operations`, `table_version`
  - Optional scan/rewrite fields: `files_scanned`, `files_pruned`, `bytes_scanned`, `scan_time_ms`, `rewrite_time_ms`
  - Optional runtime/io/result fields: `peak_rss_mb`, `cpu_time_ms`, `bytes_read`, `bytes_written`, `files_touched`, `files_skipped`, `spill_bytes`, `result_hash`
  - Source mapping:
    - `read_scan`: DataFusion/Delta physical-plan metrics (`files_scanned`, `files_pruned`, `bytes_scanned`, `scan_time_ms`)
    - `merge_dml`: `MergeMetrics` (`files_scanned`, `files_pruned`, `scan_time_ms`, `rewrite_time_ms`)
    - `optimize_vacuum` optimize cases: considered/skipped counts (`files_scanned`, `files_pruned`)

## Benchmark coverage additions

- `read_scan` includes partition-pruning contrast cases (`read_partition_pruning_hit`, `read_partition_pruning_miss`).
- `merge_dml` includes localized partition-aware case (`merge_partition_localized_1pct`) using target/source region alignment and partition predicate.
- `optimize_vacuum` includes explicit noop-vs-heavy compaction cases (`optimize_noop_already_compact`, `optimize_heavy_compaction`).

## Reproducibility controls

- Deterministic seed-based fixture generation
- Deterministic manifest ordering for P0 (`p0-rust`, `p0-python`)
- Single-machine sequential branch comparisons
- Stable default threshold (`0.05`) for no-change classification
- Explicit benchmark run mode to suppress update/scan/log-noise during timed runs
