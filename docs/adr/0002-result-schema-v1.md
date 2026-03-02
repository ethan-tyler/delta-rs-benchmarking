# ADR 0002: Result Schema v2 (strict)

## Decision

Use JSON result schema v2 with explicit case-level samples/classification and strict v2 ingestion.

## Structure

- `schema_version`: integer (`2`)
- `context`: run metadata (host, suite, scale, git SHA, timestamp)
  - Optional run-shaping metadata:
    - `dataset_id`
    - `dataset_fingerprint`
    - `runner`
    - `backend_profile`
  - Optional fidelity/security metadata:
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
- `cases[]`:
  - `case`
  - `success`
  - required `classification` (`supported` or `expected_failure`)
  - `samples[]` (`elapsed_ms`, optional rows/bytes)
    - `metrics` normalized fields:
      - base: `rows_processed`, `bytes_processed`, `operations`, `table_version`
      - optional scan/rewrite: `files_scanned`, `files_pruned`, `bytes_scanned`, `scan_time_ms`, `rewrite_time_ms`
      - optional runtime/io/result: `peak_rss_mb`, `cpu_time_ms`, `bytes_read`, `bytes_written`, `files_touched`, `files_skipped`, `spill_bytes`, `result_hash`
  - `failure` message when unsuccessful

## Clarifications

- Suite-level skip policies (for example, TPC-DS `q72`) are represented without schema changes:
  - `success=false`
  - `samples=[]`
  - `failure.message` prefixed with `skipped: ...`
- This keeps schema v1 compatible while preserving deterministic per-case output for compare tooling.

## Rationale

- Supports rich comparisons without rerunning benchmarks.
- Handles partial failures and expected-failure compatibility lanes without invalidating whole runs.
- Keeps schema easy to consume from Python and future bot services.
- Python compare tooling enforces schema v2 for both baseline and candidate payloads.
