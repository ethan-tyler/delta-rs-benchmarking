# delta-rs-benchmarking

Benchmark harness for [delta-rs](https://github.com/delta-io/delta-rs). Runs reproducible performance benchmarks against any delta-rs revision with local branch comparison and longitudinal trend tracking.

## Quick Start

Get from zero to your first benchmark result:

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
./scripts/bench.sh data --dataset-id tiny_smoke --seed 42
./scripts/bench.sh run --suite all --runner all --dataset-id tiny_smoke --warmup 1 --iters 5 --label local
```

This clones delta-rs, generates test data, runs all benchmark suites, and writes results to `results/local/<suite>.json`.

To compare your branch against main with decision-grade defaults:

```bash
./scripts/compare_branch.sh --current-vs-main all
```

## What You Can Do

| Goal | Guide | When to use it |
| --- | --- | --- |
| Set up and run your first benchmark | [Getting Started](docs/getting-started.md) | You are new to this harness and want to get running. |
| Compare performance between two revisions | [Comparing Branches](docs/comparing-branches.md) | You want to know if a change made things faster or slower. |
| Track performance across many revisions | [Longitudinal Benchmarking](docs/longitudinal.md) | You need nightly regression detection or release baselines. |
| Run on dedicated cloud infrastructure | [Cloud Runner](docs/cloud-runner.md) | You need noise isolation, security controls, or reproducible CI. |
| Look up suites, metrics, flags, env vars | [Reference](docs/reference.md) | You need to find a specific configuration option or metric name. |
| Understand internals and architecture | [Architecture](docs/architecture.md) | You want to know how the harness is structured and how data flows. |

## Command Quick Reference

| Script | Purpose |
| --- | --- |
| `./scripts/bench.sh` | Generate fixtures and run benchmark suites |
| `./scripts/compare_branch.sh` | Compare two revisions with multi-run aggregation |
| `./scripts/longitudinal_bench.sh` | Run longitudinal pipeline stages |
| `./scripts/cleanup_local.sh` | Safe artifact cleanup (dry-run by default) |
| `./scripts/docs_check.sh` | Run documentation quality checks |

Pass `--help` to any script for full usage details.

## Current Benchmark Scope

| Category | Details |
| --- | --- |
| Suites | `scan`, `write`, `delete_update`, `merge`, `metadata`, `optimize_vacuum`, `tpcds`, `interop_py` |
| Cases | 35 individual benchmark cases across all suites |
| Runners | Rust native and Python interop (pandas, polars, pyarrow) |
| Fixtures | Deterministic seed-based generation with normalized result schema |
| Comparison | Branch-to-branch with multi-run aggregation and grouped reporting |
| Longitudinal | Revision benchmarking with resumable execution and trend reports |
| Release tracking | `rust-v*` and `python-v*` tag history |
