# delta-rs-benchmarking

Benchmark harness for `delta-rs` with branch comparison and longitudinal trend tracking.

## Quickstart

Managed checkout mode (recommended first run):

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
./scripts/bench.sh data --dataset-id tiny_smoke --seed 42
./scripts/bench.sh run --suite all --runner all --dataset-id tiny_smoke --warmup 1 --iters 5 --label local
```

`run` prints a per-case summary table in the terminal, and detailed results are written to `results/<label>/<suite>.json` (pass `--no-summary-table` to suppress table output).
Rust compiler warnings are suppressed by default in `scripts/bench.sh` output; set `DELTA_BENCH_SUPPRESS_RUST_WARNINGS=0` to show them.

Use your existing local `delta-rs` checkout instead of `.delta-rs-under-test`:

```bash
DELTA_RS_DIR=/path/to/your/delta-rs \
DELTA_BENCH_EXEC_ROOT=/path/to/your/delta-rs \
./scripts/sync_harness_to_delta_rs.sh

DELTA_RS_DIR=/path/to/your/delta-rs \
DELTA_BENCH_EXEC_ROOT=/path/to/your/delta-rs \
./scripts/bench.sh doctor
```

TPC-DS DuckDB-backed fixture generation (subset-first):

```bash
./scripts/bench.sh data --dataset-id tpcds_duckdb --seed 42
./scripts/bench.sh run --suite tpcds --runner rust --dataset-id tpcds_duckdb --warmup 1 --iters 1 --label tpcds-duckdb-smoke
```

Dependencies for `tpcds_duckdb`: `python3` plus `duckdb` (`pip install duckdb`).
Marketplace datasets are currently a document-only path: place external Delta tables under expected `fixtures/<scale>/...` roots.

## Common workflows

Compare two refs:

```bash
./scripts/compare_branch.sh main <candidate_ref> all
```

`<candidate_ref>` must be a real branch in `.delta-rs-under-test` (or use commit SHAs below).

Pin immutable commits when needed:

```bash
./scripts/compare_branch.sh --base-sha <base_sha> --candidate-sha <candidate_sha> all
```

Compare your current checkout commit against the latest remote `main` (auto-picks `upstream`, falls back to `origin`):

```bash
./scripts/compare_branch.sh --current-vs-main all
```

Refresh committed release-history benchmark manifests:

```bash
./scripts/update_release_history_manifests.sh
```

Clean local benchmark artifacts safely (dry-run by default):

```bash
./scripts/cleanup_local.sh --results
./scripts/cleanup_local.sh --apply --results --keep-last 5 --older-than-days 14
./scripts/cleanup_local.sh --apply --fixtures --delta-rs-under-test
```

`cleanup_local.sh` never deletes anything unless `--apply` is explicitly provided.

Show CLI help:

```bash
./scripts/bench.sh --help
./scripts/compare_branch.sh --help
./scripts/longitudinal_bench.sh --help
./scripts/cleanup_local.sh --help
```

## Documentation

- [User Guide](docs/user-guide.md): day-to-day usage, suite execution, object-store mode, and metrics.
- [Longitudinal CLI Guide](docs/longitudinal-cli.md): revision selection, matrix execution, reporting, and retention.
- [Longitudinal Runbook](docs/longitudinal-runbook.md): nightly operations, release-tag history runs, and failure recovery.
- [Security Runbook (Cloud Runner)](docs/security-runner.md): runner hardening, preflight checks, and provisioning controls.
- [Architecture](docs/architecture.md): components, data flow, schema, and reproducibility controls.

## Current scope

- Suites: `scan`, `write`, `delete_update`, `merge`, `metadata`, `optimize_vacuum`, `tpcds`, `interop_py`
- Deterministic fixture generation and normalized result schema
- Manual branch-to-branch comparison with result reporting
- Longitudinal revision benchmarking pipeline with resumable execution
- Release-tag longitudinal history for `rust-v*` and `python-v*` tracks
