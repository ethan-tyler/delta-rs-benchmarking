# delta-rs-benchmarking

Benchmark harness for `delta-rs` with local branch comparison and longitudinal trend tracking.

## Start Here

First successful local run (recommended first path):

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
./scripts/bench.sh data --dataset-id tiny_smoke --seed 42
./scripts/bench.sh run --suite all --runner all --dataset-id tiny_smoke --warmup 1 --iters 5 --label local
```

The run prints a per-case summary table and writes full results to `results/<label>/<suite>.json`.

For decision-grade branch comparisons, use `compare_branch.sh` defaults:

- warmup: `2`
- measured iters: `9`
- prewarm iters per ref: `1`
- measured runs per ref: `3`
- run order: `alternate` (base-first then candidate-first on alternating runs)

```bash
./scripts/compare_branch.sh --current-vs-main all
```

## Contributor Task Router

| Task                                   | Primary page                                         | Use when                                                                      |
| -------------------------------------- | ---------------------------------------------------- | ----------------------------------------------------------------------------- |
| Run suites locally                     | [User Guide](docs/user-guide.md)                     | You need fixture generation, suite execution, backend selection, and cleanup. |
| Compare refs and SHAs                  | [User Guide](docs/user-guide.md#compare-workflows)   | You need base-vs-candidate or current-vs-main performance deltas.             |
| Run longitudinal CLI pipeline          | [Longitudinal CLI Guide](docs/longitudinal-cli.md)   | You are executing revision manifests, matrix runs, ingest, and reports.       |
| Recover failed longitudinal jobs       | [Longitudinal Runbook](docs/longitudinal-runbook.md) | Nightly/release workflow failed and you need recovery steps.                  |
| Run remote/security-controlled benches | [Security Runbook](docs/security-runner.md)          | You need run-mode checks, no-public-ipv4, and provisioning controls.          |
| Understand internals and schema        | [Architecture](docs/architecture.md)                 | You need component, data-flow, and schema v2 reference detail.                |

## Choose Your Workflow

### Run Locally

Use the happy path in [docs/user-guide.md](docs/user-guide.md#first-benchmark-run-happy-path).

### Compare Performance Between Revisions

Start with [docs/user-guide.md#compare-workflows](docs/user-guide.md#compare-workflows) for:

- branch-to-branch compare
- immutable SHA compare
- current checkout vs latest remote `main`

### Execute Longitudinal Pipelines

- command cookbook: [docs/longitudinal-cli.md](docs/longitudinal-cli.md)
- operational recovery: [docs/longitudinal-runbook.md](docs/longitudinal-runbook.md)

### Run on Hardened or Remote Runners

Use [docs/security-runner.md](docs/security-runner.md) for run-mode commands, compare preflight enforcement flags, and provisioning guardrails.

### Inspect Architecture and Result Schema

Use [docs/architecture.md](docs/architecture.md) for component map, schema v2 fields, and reproducibility controls.

## Common Command Entrypoints

```bash
./scripts/bench.sh --help
./scripts/compare_branch.sh --help
./scripts/longitudinal_bench.sh --help
./scripts/cleanup_local.sh --help
./scripts/docs_check.sh
```

## Current Scope

- Suites: `scan`, `write`, `delete_update`, `merge`, `metadata`, `optimize_vacuum`, `tpcds`, `interop_py`
- Deterministic fixture generation and normalized result schema
- Manual branch-to-branch comparison with grouped reporting
- Longitudinal revision benchmarking with resumable execution
- Release-tag longitudinal history for `rust-v*` and `python-v*` tracks
