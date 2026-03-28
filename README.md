# delta-rs-benchmarking

Standalone benchmark harness for [delta-rs](https://github.com/delta-io/delta-rs). Use it to run reproducible benchmarks against any delta-rs revision, compare a branch against `main` on the same machine, and track performance over time.

This repository is separate from `delta-rs` itself. The harness manages or targets a delta-rs checkout, syncs benchmark code into that workspace, generates deterministic fixture data, and writes normalized JSON results under `results/<label>/`.

## Quick Start

Use the managed checkout flow unless you already have a local delta-rs workspace you want to benchmark.

### 1. Prepare the managed delta-rs checkout

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
```

This clones or updates delta-rs at `.delta-rs-under-test/` and syncs the benchmark harness into that workspace so Cargo can build it.

### 2. Generate a small deterministic dataset

```bash
./scripts/bench.sh data --dataset-id tiny_smoke --seed 42
```

This creates a fast smoke-test fixture set that is stable across runs and machines.

### 3. Run your first benchmark

```bash
./scripts/bench.sh run \
  --suite scan \
  --runner rust \
  --lane smoke \
  --dataset-id tiny_smoke \
  --warmup 1 \
  --iters 5 \
  --label local
```

This writes a result file to `results/local/scan.json` and prints a terminal summary table.

`bench.sh run` defaults to `--lane smoke`. Keep that default for quick local validation, switch to `--lane correctness` for trusted semantic validation on correctness-backed suites (`write`, `delete_update`, `merge`, `metadata`, `optimize_vacuum`, `interop_py`), and use `--lane macro` only for macro-safe perf exploration.

### 4. Expand to the common workflows

Run the full local suite:

```bash
./scripts/bench.sh run \
  --suite all \
  --runner all \
  --lane smoke \
  --dataset-id tiny_smoke \
  --warmup 1 \
  --iters 5 \
  --label local
```

Compare your current delta-rs checkout against upstream `main`:

```bash
./scripts/compare_branch.sh --current-vs-main scan
```

PR automation is narrower than local exploration: `run benchmark scan` posts exploratory output, and `run benchmark decision scan` runs the explicit decision-grade path. Automated macro perf workflows are currently curated to `scan`; GitHub-hosted CI stays on smoke/correctness validation lanes, and `tpcds` remains outside automated hosted perf claims.

For a rerunnable trust-contract check, see [Validation](docs/validation.md) and run `./scripts/validate_perf_harness.sh`.

Before making a trustworthy perf claim from any local or self-hosted compare run, rerun `./scripts/validate_perf_harness.sh` and review [Validation](docs/validation.md). By default that refreshes the focused trust-contract suites and records machine-local evidence under `results/validation/<timestamp>/summary.md`; pass `--artifact-dir results/validation/latest` if you want a stable local path.

Lane policy is intentionally split by environment. GitHub-hosted CI is limited to smoke and correctness validation lanes, while self-hosted benchmark runners are the only place where macro perf, decision compare, or longitudinal ingestion should be treated as authoritative evidence.

If you want Python interop coverage (`--runner all`), install the optional Python dependencies first:

```bash
python3 -m pip install -r python/requirements-audit.txt
```

For a fuller walkthrough of datasets, backends, cleanup, and output format, start with [Getting Started](docs/getting-started.md).

## What You Can Do

| Goal                                                      | Guide                                             | When to use it                                                                            |
| --------------------------------------------------------- | ------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| Run your first benchmark end to end                       | [Getting Started](docs/getting-started.md)        | You are new to the harness and want the shortest path to a valid local result.            |
| Compare performance between two revisions                 | [Comparing Branches](docs/comparing-branches.md)  | You want to know whether a change made delta-rs faster, slower, or effectively unchanged. |
| Track performance across many revisions                   | [Longitudinal Benchmarking](docs/longitudinal.md) | You need regression detection over time, nightly runs, or release baselines.              |
| Run on dedicated cloud infrastructure                     | [Cloud Runner](docs/cloud-runner.md)              | You need lower-noise hardware, stronger controls, or reproducible self-hosted workflows.  |
| Look up suites, metrics, flags, and environment variables | [Reference](docs/reference.md)                    | You need an exact option name, metric field, schema detail, or dataset definition.        |
| Understand internals and data flow                        | [Architecture](docs/architecture.md)              | You want to see how the harness is structured and how execution moves through the repo.   |

## How the Harness Works

Most workflows follow the same shape:

1. `prepare_delta_rs.sh` creates or updates the managed checkout at `.delta-rs-under-test/`.
2. `sync_harness_to_delta_rs.sh` copies the benchmark crate and config into that workspace.
3. `bench.sh data` generates deterministic fixtures from a seed.
4. `bench.sh run` executes suites and writes schema v4 JSON results under `results/<label>/`.
5. `compare_branch.sh` or `longitudinal_bench.sh` analyze those results for branch-to-branch or time-series use cases.

You usually do not modify delta-rs source from this repository. The harness exists to drive a delta-rs checkout, not replace it.

## Command Quick Reference

| Script                            | Purpose                                                                            |
| --------------------------------- | ---------------------------------------------------------------------------------- |
| `./scripts/bench.sh`              | Generate fixtures, run suites, list benchmarks, and run workspace health checks    |
| `./scripts/compare_branch.sh`     | Compare two delta-rs revisions with repeated runs and aggregated reporting         |
| `./scripts/longitudinal_bench.sh` | Run longitudinal matrix, ingest, reporting, and retention stages                   |
| `./scripts/cleanup_local.sh`      | Clean fixtures, results, and managed checkout artifacts safely; dry-run by default |
| `./scripts/docs_check.sh`         | Run repository documentation contract checks                                       |

Pass `--help` to any script for full usage details.

## Verification and Runner Guardrails

Before opening a pull request, run the same baseline checks enforced by CI:

```bash
cargo test --locked
(cd python && python3 -m pytest -q tests)
```

GitHub-hosted CI also runs the real harness, but only on smoke and correctness validation lanes. Self-hosted workflows remain the only automated path for macro perf, decision compare, Criterion microbench work, and longitudinal ingestion.

For the dependency audit baseline that runs beside those tests:

```bash
cargo audit \
  --ignore RUSTSEC-2026-0037 \
  --ignore RUSTSEC-2026-0041 \
  --ignore RUSTSEC-2026-0049
python3 -m pip_audit -r python/requirements-audit.txt
```

Self-hosted benchmark workflows also enforce runner preflight before execution:

```bash
./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy
```

For the benchmark trust contract itself:

```bash
./scripts/validate_perf_harness.sh
```

The latest expected evidence format and rerun protocol live in [Harness Validation](docs/validation.md).

## Current Benchmark Scope

- Suites: `scan`, `write`, `delete_update`, `merge`, `metadata`, `optimize_vacuum`, `tpcds`, `interop_py`
- Coverage: 35 benchmark cases across Rust-native and Python interop paths
- Fixtures: deterministic seed-based generation with fixture recipe identity and schema v4 results
- Comparison: branch-to-branch reporting with multi-run aggregation and grouped output
- Longitudinal: resumable `matrix-state.json` checkpoints plus SQLite-backed history and trend reporting
- Automation policy: GitHub-hosted CI runs smoke plus correctness validation lanes only; self-hosted automation owns macro perf, decision compare, and longitudinal ingestion, with trusted perf workflows currently curated to `scan`
- Workflow hardening: self-hosted runs require benchmark mode, no public IPv4, and egress-policy preflight
- Release tracking: `rust-v*` and `python-v*` tag history

## Project Governance

- [Contributing](CONTRIBUTING.md)
- [Security Policy](SECURITY.md)
- [Changelog](CHANGELOG.md)
- [License](LICENSE)
