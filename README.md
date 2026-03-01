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

## Common workflows

Compare two branches:

```bash
./scripts/compare_branch.sh main candidate-branch all
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
- [Longitudinal Runbook](docs/longitudinal-runbook.md): nightly operations and failure recovery.
- [Security Runbook (Cloud Runner)](docs/security-runner.md): runner hardening, preflight checks, and provisioning controls.
- [Architecture](docs/architecture.md): components, data flow, schema, and reproducibility controls.

## Current scope

- Suites: `read_scan`, `write`, `delete_update_dml`, `merge_dml`, `metadata`, `optimize_vacuum`, `tpcds`, `interop_py`
- Deterministic fixture generation and normalized result schema
- Manual branch-to-branch comparison in advisory mode
- Longitudinal revision benchmarking pipeline with resumable execution
