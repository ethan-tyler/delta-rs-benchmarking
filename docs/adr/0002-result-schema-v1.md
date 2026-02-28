# ADR 0002: Result Schema v1

## Decision

Use JSON result schema v1 with explicit case-level samples and failure payloads.

## Structure

- `schema_version`: integer
- `context`: run metadata (host, suite, scale, git SHA, timestamp)
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
  - `samples[]` (`elapsed_ms`, optional rows/bytes)
  - `samples[].metrics` normalized benchmark metrics:
    - Existing: `rows_processed`, `bytes_processed`, `operations`, `table_version`
    - Backward-compatible optional additions: `files_scanned`, `files_pruned`, `bytes_scanned`, `scan_time_ms`, `rewrite_time_ms`
  - `failure` message when unsuccessful

## Rationale

- Supports rich comparisons without rerunning benchmarks.
- Handles partial failures without invalidating whole runs.
- Keeps schema easy to consume from Python and future bot services.
