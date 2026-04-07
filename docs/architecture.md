# Architecture

How the benchmark harness is structured, how data flows through it, and what controls ensure reproducible results.

## Table of Contents

- [Key Concepts](#key-concepts)
- [Components](#components)
- [Data Flow](#data-flow)
- [Longitudinal State and Storage](#longitudinal-state-and-storage)
- [Result Schema v5](#result-schema-v5)
- [Benchmark Coverage](#benchmark-coverage)
- [Reproducibility Controls](#reproducibility-controls)
- [Advanced Fixture Profiles](#advanced-fixture-profiles)

## Key Concepts

A quick orientation on the most important terms. See [Reference](reference.md#glossary) for the full glossary.

| Term          | Definition                                                                                    |
| ------------- | --------------------------------------------------------------------------------------------- |
| **Suite**     | A named group of benchmark cases testing one type of Delta operation (e.g., `scan`, `merge`). |
| **Case**      | An individual benchmark within a suite, run for warmup + measured iterations.                 |
| **Runner**    | Execution lane: `rust`, `python`, or `all`.                                                   |
| **Fixture**   | Deterministic test data generated from a seed. Produces Delta tables and row snapshots.       |
| **Schema v5** | The normalized JSON result format used by authoritative benchmark output.                     |
| **Manifest**  | A YAML file declaring which cases to execute and what assertions to validate.                 |

## Components

The harness is organized into three layers: execution, comparison/analysis, and automation.

### Execution

| Component                | Description                                                                               |
| ------------------------ | ----------------------------------------------------------------------------------------- |
| `crates/delta-bench`     | Rust CLI and benchmark execution engine. Generates fixtures, runs suites, writes results. |
| `bench/manifests/*.yaml` | Benchmark catalogs declaring cases, runners, and assertions for execution planning.       |
| `backends/*.env`         | Backend profile defaults for storage configuration (S3, locking, region).                 |

### Comparison and analysis

| Component                    | Description                                                                                                                       |
| ---------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `bench/methodologies/*.env`  | Harness-owned compare contracts and Criterion diagnostic profiles. Criterion profiles are local or trusted self-hosted only and never authoritative PR evidence. |
| `python/delta_bench_compare` | Result comparison and rendering. Reads schema v5 JSON and enforces strict compatibility identity for compare and aggregation. |
| `bench/evidence/registry.yaml` | Harness-owned evidence policy and pack definitions used by PR comment automation.                                         |
| `python/delta_bench_interop` | Python interop benchmark cases using pandas, polars, and pyarrow.                                                                 |
| `python/delta_bench_tpcds`   | DuckDB-backed `store_sales` fixture generation script for the `tpcds_duckdb` dataset.                                             |

### Automation and workflows

| Component                                                                  | Description                                                                                                                       |
| -------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| `scripts/prepare_delta_rs.sh`                                              | Manages the delta-rs checkout at `.delta-rs-under-test`.                                                                          |
| `scripts/sync_harness_to_delta_rs.sh`                                      | Syncs benchmark crate and configs into the delta-rs workspace.                                                                    |
| `scripts/bench.sh`                                                         | Wrapper for `delta-bench` subcommands (data, run, list, doctor).                                                                  |
| `scripts/compare_branch.sh`                                                | Multi-run base-vs-candidate orchestration that pins refs once, prepares per-ref checkouts, and emits compare artifacts for automation. |
| `scripts/validate_perf_harness.sh`                                         | Runs the focused trust-contract suites and records a rerunnable Markdown artifact under `results/validation/`.                    |
| `scripts/publish_contract.sh`                                              | Publishes the current docs/manifests/script contract bundle under `results/contracts/`.                                           |
| `scripts/security_mode.sh`                                                 | Toggles benchmark run mode vs maintenance mode on cloud runners.                                                                  |
| `scripts/security_check.sh`                                                | Preflight guardrails for mode, network, and egress policy.                                                                        |
| `scripts/provision_runner.sh`                                              | Terraform orchestration wrapper for runner provisioning.                                                                          |
| `.github/workflows/ci.yml`                                                 | Enforces the shared Rust/Python test baseline and runs hosted smoke/correctness benchmark validation on pushes and pull requests. |
| `.github/workflows/benchmark*.yml`, `.github/workflows/longitudinal-*.yml` | Self-hosted benchmark workflows that enforce runner preflight before branch comparison, pack fan-out, or `run-matrix`.            |

## Data Flow

Benchmark execution follows this pipeline:

1. **Fixture generation.** `delta-bench data` generates deterministic Delta tables under `fixtures/<scale>/`. This includes narrow sales tables, partitioned tables, merge targets, and suite-specific fixtures. JSON row snapshots (`rows.jsonl`) and a manifest (`manifest.json`) are written alongside the tables.

2. **TPC-DS fixtures (optional).** For `dataset_id=tpcds_duckdb`, the `store_sales` table is sourced from DuckDB's `tpcds` extension, exported through CSV, and written as a Delta table.

3. **Suite execution.** `delta-bench run` resolves runner mode from manifest-planned cases and executes Rust suites directly and Python interop cases via subprocess. `bench.sh` defaults to the smoke lane; explicit `--lane correctness` is the trusted semantic-validation path for correctness-backed suites; `--lane macro` is the performance lane for macro-safe cases; and `--mode assert` is only valid with `--lane correctness`.

GitHub-hosted CI stays on smoke and correctness lanes. Self-hosted infrastructure is the only authorized path for macro perf, decision compare, Criterion microbench, and longitudinal ingestion.

4. **Result output.** Each suite writes a schema v5 JSON file to `results/<label>/<suite>.json`. Context carries lane, benchmark mode, fixture recipe, harness revision, and fidelity identity. Cases also carry run summaries, compatibility keys, and `perf_status`. Legacy correctness-tagged cases requested in macro lane remain operationally runnable but are emitted with `perf_status=validation_only` so they cannot be compared or reported as trusted perf evidence. Dedicated perf-owned suites such as `write_perf`, `delete_update_perf`, `merge_perf`, `optimize_perf`, and `tpcds` keep separate suite ids so they can own their own methodology and readiness gates.

5. **Comparison (optional).** `compare.py` reads baseline and candidate schema v5 JSON files, rejects invalid or mismatched contexts, and supports three render formats: text, markdown, and a versioned JSON payload with explicit machine status fields. `compare_branch.sh` prepares one checkout per pinned SHA under `.delta-bench-compare-checkouts/` so repeated runs reuse the same synced worktree. Exploratory mode can run with the default three measured runs; decision mode fails closed unless both sides provide at least five runs. Named methodology profiles let the harness pin that contract centrally; the self-hosted PR decision path uses `--methodology-profile pr-macro`, which fixes the `medium_selective` dataset, seven measured runs, 15 iterations per run, `iqr_ms` as the spread metric, and `decision_scope=micro_only` for sub-millisecond rows. Candidate/manual perf-owned DML, maintenance, and metadata surfaces (`pr-delete-update-perf`, `pr-merge-perf`, `pr-optimize-perf`, `pr-metadata-perf`) also route through the same compare path, but remain gated in the evidence registry until their same-SHA and canary evidence is refreshed. Remote object-store coverage now follows that same single-suite compare path through first-class registry surfaces such as `scan_s3`, `delete_update_perf_s3`, `merge_perf_s3`, `optimize_perf_s3`, and `metadata_perf_s3` instead of workflow-specific shell branches.

6. **Security validation.** `security_check.sh` validates fidelity invariants (run mode, network, egress). Self-hosted GitHub Actions workflows enforce this preflight before branch comparison, pack planning, shard execution, and longitudinal execution. Current automated perf collection is self-hosted, macro-only, and curated through the registry in `bench/evidence/registry.yaml`; GitHub-hosted CI is limited to smoke/correctness validation. PR comments split into exploratory (`run benchmark scan`), single-suite decision (`run benchmark decision scan`), queue inspection (`show benchmark queue`), and pack decision (`run benchmark decision full`) paths. The `full` command resolves the `pr-full-decision` pack, full does not mean --suite all, and that pack contains only `readiness=ready` suites. `pr-candidate-manual` keeps gated perf-owned suites available for operator reruns without routing them through PR comment automation; it now carries `write_perf`, `delete_update_perf`, `merge_perf`, `optimize_perf`, `metadata_perf`, and `tpcds`. The separate `s3-candidate-manual` pack does the same for declared remote surfaces, while keeping `write_perf_s3` gated outside that pack until non-local write throughput is explicitly signed off. That split is intentional: ready PR automation stays on the comment grammar, gated perf suites stay operator-only through `./scripts/compare_branch.sh --current-vs-main --methodology-profile <profile> <suite>` or the manual packs, and Criterion diagnostics stay on `./scripts/run_profile.sh <criterion-profile>`. Queue/pack state lives in a SQLite DB pointed to by `DELTA_BENCH_BOT_DB_PATH`, and that path must resolve on every runner that can pick up `benchmark.yml`.

7. **Report output.** The compare workflow produces grouped text output and a sidecar artifact bundle under `results/compare/<suite>/<base>__<candidate>/`, including `summary.md`, `comparison.json`, `hash-policy.txt`, and `manifest.json`. Pack aggregation writes the same stable filenames under `results/compare/packs/<pack_id>/<base>__<candidate>/`, with pack-level `comparison.json` flattening suite rows and adding a `suite` field.

8. **Longitudinal matrix checkpointing (optional).** `run-matrix` writes `matrix-state.json` through an atomic temp-file replace. The state file records per-cell progress plus a configuration fingerprint so resume only happens against the same suite/scale/lane/output contract.

9. **Longitudinal ingest, reporting, and retention (optional).** `ingest-results` normalizes schema v5 suite outputs into a SQLite store. Reporting uses explicit stored compatibility identity fields, `benchmark_mode`, and `compatibility_key` rather than just `suite/scale/case`, and the pipeline rejects legacy `rows.jsonl` / `index.json`-only stores to avoid silent split state.

10. **Criterion diagnostic lane (optional).** `./scripts/run_profile.sh scan-phase-criterion` and `./scripts/run_profile.sh metadata-replay-criterion` run the currently committed Criterion families, resolving to `scan_phase_bench` and `metadata_replay_bench`. These microbenches stay local or trusted self-hosted diagnostics only: they never become authoritative PR evidence, never enter `bench/evidence/registry.yaml` packs, and never route through `compare_branch.sh`, PR comment automation, or longitudinal ingest. `scan_pruning_hit` moved here because it is too small/cache-sensitive for the authoritative macro manifest, and replay/provider work belongs on the dedicated metadata/replay probe rather than on the correctness-only `metadata` suite.
11. **Trust validation (operator step).** `validate_perf_harness.sh` reruns the focused trust-contract suites before the operator treats a machine/workflow as trustworthy for PR perf claims. When the validation SHA is an immutable ref that `origin` does not advertise, the validator can fetch it into `.delta-rs-under-test` first and then seed same-SHA compare pinning into `.delta-rs-source` from that prepared execution checkout.

Marketplace datasets are a document-only path: place externally provisioned Delta tables under the expected `fixtures/<scale>/...` roots.

## Longitudinal State and Storage

The longitudinal pipeline persists two control-plane artifacts:

| Artifact            | Format | Purpose                                                                                                                                         |
| ------------------- | ------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| `matrix-state.json` | JSON   | Resume ledger for `(revision, suite, scale)` cells. Written atomically and guarded by a stored configuration fingerprint.                       |
| `store.sqlite3`     | SQLite | Normalized time-series store for ingested runs and case rows. Reporting and retention use the same database, and ingest deduplicates by run id. |

If a store directory still contains only legacy `rows.jsonl` or `index.json` artifacts, the current pipeline fails fast instead of silently treating that state as empty.

## Result Schema v5

Benchmark results use a normalized JSON format with three top-level sections: `context` (metadata about the run), `cases` (per-case outcomes and samples), and a `schema_version` field.

Each case contains an array of `samples`, where each sample captures the elapsed time and optional metrics for one measured iteration. Case-level `elapsed_stats` are only populated when `perf_status=trusted`.

For the complete field-by-field listing of all context, fidelity, case-level, and sample-level fields, see [Reference](reference.md#result-schema-v5).

### Source mapping highlights

Different suites populate different subsets of the metrics:

| Suite                              | Key metrics captured                                               |
| ---------------------------------- | ------------------------------------------------------------------ |
| `scan`                             | `files_scanned`, `files_pruned`, `bytes_scanned`, `scan_time_ms`   |
| `merge`                            | `files_scanned`, `files_pruned`, `scan_time_ms`, `rewrite_time_ms` |
| `merge_perf`                       | `files_scanned`, `files_pruned`, `scan_time_ms`, `rewrite_time_ms` |
| `delete_update_perf`               | `files_scanned`, `scan_time_ms`, `rewrite_time_ms`                 |
| `optimize_vacuum` (optimize cases) | `files_scanned` (considered), `files_pruned` (skipped)             |
| `optimize_perf`                    | `files_scanned` (considered), `files_pruned` (skipped)             |

## Benchmark Coverage

The harness covers these operation categories with specific contrast cases:

- **scan** includes pruning contrast in the suite implementation: `scan_pruning_hit` vs `scan_pruning_miss` measures the impact of partition pruning, but the authoritative macro decision manifest now enables only `scan_full_narrow`, `scan_projection_region`, and `scan_filter_flag` on `medium_selective`. `scan_pruning_hit` moved to Criterion microbench coverage because it is too small/cache-sensitive for normal macro verdicts, and `scan_pruning_miss` is disabled until its exact-result contract is requalified.
- **merge** includes a localized partition-aware case: `merge_localized_1pct` tests merge performance when a partition predicate narrows the scan scope.
- **merge_perf** freezes the initial perf-owned merge evidence set around `merge_perf_upsert_10pct`, `merge_perf_upsert_50pct`, `merge_perf_localized_1pct`, and `merge_perf_delete_5pct` so correctness runs and perf evidence stay separate.
- **delete_update_perf** freezes the initial perf-owned DML evidence set around localized delete, scattered small-file delete, scattered literal update, and full-table expression update.
- **optimize_vacuum** includes noop-vs-heavy contrast: `optimize_noop_already_compact` vs `optimize_heavy_compaction` to measure compaction overhead when there is nothing to do vs aggressive compaction.
- **optimize_perf** keeps a narrower perf-owned maintenance surface: small-file compaction, noop compact, and vacuum execute.
For the complete list of benchmark suites and cases documented for operators, see [Reference](reference.md#benchmark-suites-and-cases).

## Reproducibility Controls

These mechanisms ensure that benchmark results are comparable across runs:

- **Deterministic fixtures.** Seed-based data generation produces identical tables regardless of when or where you run.
- **Dataset separation by purpose.** `tiny_smoke` stays optimized for setup/smoke validation, while `pr-macro` intentionally promotes branch compare onto the larger deterministic `medium_selective` dataset.
- **Checkout locking.** Prepare/compare flows serialize access to the mutable `.delta-rs-under-test` checkout and the clean `.delta-rs-source` checkout so concurrent control-plane actions cannot corrupt compare pinning or checkout reuse.
- **Deterministic manifest ordering.** The `core-rust` and `core-python` manifests define a fixed case execution order.
- **Single-machine comparisons.** Branch comparisons run both refs on the same hardware to eliminate machine-to-machine variance.
- **Prewarm runs.** Optional unreported iterations stabilize caches and thermal state before measurement begins.
- **Multi-run aggregation.** Multiple measured runs per ref are aggregated (default: median) before change classification.
- **Named methodology profiles.** Harness-owned profiles such as `pr-macro` keep self-hosted PR decision settings stable across bot runs while still allowing explicit local flag overrides. Planning-focused profiles stay investigation-grade and should not be substituted for the default execute-path PR contract.
- **Separate correctness and perf suite ids.** `delete_update`, `merge`, and `optimize_vacuum` remain correctness-owned, while `delete_update_perf`, `merge_perf`, and `optimize_perf` own the candidate/manual perf compare contracts.
- **Explicit self-hosted fixture provisioning.** Promotion-grade `tpcds` evidence assumes the DuckDB-backed `tpcds_duckdb` fixture tree is already present on every trusted self-hosted runner before the compare starts.
- **Separated evidence surfaces.** `scan` remains the execute-path guardrail. Criterion profiles such as `scan-phase-criterion` and `metadata-replay-criterion` stay diagnostic-only and separate from authoritative PR evidence.
- **Configurable run order.** `base-first`, `candidate-first`, or `alternate` ordering to reduce systematic bias from execution order.
- **Stable thresholds.** Default no-change threshold of `0.05` (5%) prevents false positives from normal measurement noise.
- **Explicit decision scope.** Compare output marks sub-millisecond rows as `decision_scope=micro_only` and renders them under `Out of Scope (micro only)` instead of counting them as normal macro regressions or improvements.
- **Executable trust canaries.** The validation workflow can inject fixed scan phase delays and confirms that only the selected `timing_phase` moves while control phases stay near baseline.
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

| Variable                              | Default   | Description                         |
| ------------------------------------- | --------- | ----------------------------------- |
| `DELTA_BENCH_DUCKDB_PYTHON`           | `python3` | Python executable for DuckDB        |
| `DELTA_BENCH_TPCDS_DUCKDB_SCRIPT`     | —         | Override path to generation script  |
| `DELTA_BENCH_TPCDS_DUCKDB_TIMEOUT_MS` | `600000`  | Timeout for generation (10 minutes) |

Trusted self-hosted runner contract:

- Pre-provision the shared fixture root before collecting `pr-tpcds` evidence, typically with `DELTA_BENCH_FIXTURES=/var/lib/delta-bench/fixtures`.
- The expected table path is `/var/lib/delta-bench/fixtures/sf1/tpcds/store_sales`.
- `tpcds_q72` stays outside the PR decision surface until DataFusion parity exists.

### Marketplace datasets

Place externally provisioned Delta tables under the expected fixture roots (for TPC-DS: `fixtures/<scale>/tpcds/<table_name>`). This repository does not automate marketplace ingestion.
