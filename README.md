# delta-rs-benchmarking

Benchmark harness for `delta-rs` with branch comparison and longitudinal trend tracking.

## Quickstart

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
./scripts/bench.sh data --dataset-id tiny_smoke --seed 42
./scripts/bench.sh run --suite all --runner all --dataset-id tiny_smoke --warmup 1 --iters 5 --label local
```

Results are written to `results/<label>/<suite>.json`.

TPC-DS DuckDB-backed fixture generation (subset-first):

```bash
./scripts/bench.sh data --dataset-id tpcds_duckdb --seed 42
./scripts/bench.sh run --suite tpcds --runner rust --dataset-id tpcds_duckdb --warmup 1 --iters 1 --label tpcds-duckdb-smoke
```

Dependencies for `tpcds_duckdb`: `python3` plus `duckdb` (`pip install duckdb`).
Marketplace datasets are currently a document-only path: place external Delta tables under expected `fixtures/<scale>/...` roots.

## Common workflows

Compare two branches:

```bash
./scripts/compare_branch.sh main candidate-branch all
```

Refresh committed release-history benchmark manifests:

```bash
./scripts/update_release_history_manifests.sh
```

Show CLI help:

```bash
./scripts/bench.sh --help
./scripts/compare_branch.sh --help
./scripts/longitudinal_bench.sh --help
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
- Manual branch-to-branch comparison in advisory mode
- Longitudinal revision benchmarking pipeline with resumable execution
- Release-tag longitudinal history for `rust-v*` and `python-v*` tracks
