# Architecture

## Components

- `crates/delta-bench`: Rust CLI + benchmark execution engine.
- `python/delta_bench_compare`: result comparison and rendering.
- `scripts/prepare_delta_rs.sh`: manages local checkout at `.delta-rs-under-test`.
- `scripts/sync_harness_to_delta_rs.sh`: syncs `crates/delta-bench` into the checked-out `delta-rs` workspace.
- `scripts/bench.sh`: wraps `delta-bench` subcommands.
- `scripts/compare_branch.sh`: sequential base vs candidate run orchestration.
- `scripts/security_mode.sh`: toggles benchmark run mode vs maintenance mode.
- `scripts/security_check.sh`: preflight guardrails for run mode, interface exposure, and egress policy drift.
- `scripts/provision_vultr.sh`: Terraform orchestration wrapper for Vultr provisioning operations.

## Data flow

1. `delta-bench data` generates deterministic fixtures under `fixtures/<scale>/`.
2. Fixture generation writes both JSON row snapshots and concrete Delta tables (`narrow_sales_delta`, `merge_target_delta`).
3. `delta-bench run` executes suite cases using real `deltalake-core` read/write/merge/metadata operations and writes `results/<label>/<suite>.json`.
4. `compare.py` reads baseline/candidate result JSON and classifies per-case changes.
5. `security_check.sh` runs before benchmark execution to validate security/fidelity invariants.
6. Manual compare script prints markdown output suitable for PR comments.

## Result schema v1

- Top-level: `schema_version`, `context`, `cases`
- Context: host, label, git SHA, suite, scale, warmup/iterations, timestamp
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
- Cases: success/failure, sample timings, failure payload when applicable
- Samples: `elapsed_ms` plus normalized `metrics`:
  - Base fields: `rows_processed`, `bytes_processed`, `operations`, `table_version`
  - Optional scan/rewrite fields: `files_scanned`, `files_pruned`, `bytes_scanned`, `scan_time_ms`, `rewrite_time_ms`
  - Source mapping:
    - `read_scan`: DataFusion/Delta physical-plan metrics (`files_scanned`, `files_pruned`, `bytes_scanned`, `scan_time_ms`)
    - `merge_dml`: `MergeMetrics` (`files_scanned`, `files_pruned`, `scan_time_ms`, `rewrite_time_ms`)
    - `optimize_vacuum` optimize case: considered/skipped counts (`files_scanned`, `files_pruned`)

## Reproducibility controls

- Deterministic seed-based fixture generation
- Single-machine sequential branch comparisons
- Stable default threshold (`0.05`) for no-change classification
- Explicit benchmark run mode to suppress update/scan/log-noise during timed runs
