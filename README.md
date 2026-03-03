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

`run` prints a per-case summary table in the terminal, and detailed results are written to `results/<label>/<suite>.json`.

## Common tasks

Compare two refs:

```bash
./scripts/compare_branch.sh main <candidate_ref> all
```

Pin immutable commits:

```bash
./scripts/compare_branch.sh --base-sha <base_sha> --candidate-sha <candidate_sha> all
```

Compare your current checkout commit against latest remote `main`:

```bash
./scripts/compare_branch.sh --current-vs-main all
```

Refresh committed release-history benchmark manifests:

```bash
./scripts/update_release_history_manifests.sh
```

Clean local artifacts safely (dry-run by default):

```bash
./scripts/cleanup_local.sh --results
./scripts/cleanup_local.sh --apply --results --keep-last 5 --older-than-days 14
./scripts/cleanup_local.sh --apply --fixtures --delta-rs-under-test
```

`cleanup_local.sh` never deletes anything unless `--apply` is explicitly provided.
With `--apply`, deletions are restricted to this repository root unless `--allow-outside-root` is set.

Show CLI help:

```bash
./scripts/bench.sh --help
./scripts/compare_branch.sh --help
./scripts/longitudinal_bench.sh --help
./scripts/cleanup_local.sh --help
```

## Detailed guides

- [User Guide](docs/user-guide.md): local setup, dataset/fixture generation, suite execution, object-store mode, and cleanup.
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
