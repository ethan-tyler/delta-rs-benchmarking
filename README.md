# delta-rs-benchmarking

Reproducible benchmark harness for [delta-rs](https://github.com/delta-io/delta-rs). Measure performance against any revision, compare branches, and track regressions over time.

## Quick Start

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh

./scripts/bench.sh data --dataset-id tiny_smoke --seed 42

./scripts/bench.sh run --suite scan --runner rust --dataset-id tiny_smoke --label local

./scripts/compare_branch.sh --current-vs-main scan
```

Results go to `results/local/<suite>.json`. Pass `--help` to any script for details.

For the full setup walkthrough, see [Getting Started](docs/getting-started.md).

`bench.sh run` defaults to the `smoke` lane. Use `correctness` for correctness-backed suites such as `write`, `delete_update`, `merge`, `metadata`, `optimize_vacuum`, and `interop_py`. Use `macro` only for macro-safe perf exploration. GitHub-hosted CI stays on smoke and correctness lanes, while self-hosted workflows are the authoritative path for macro perf, decision compare, and longitudinal automation.

For Python interop coverage, install `python/requirements-audit.txt` first. For trust-contract verification, read [Validation](docs/validation.md) and run `./scripts/validate_perf_harness.sh`.

## What You Can Do

| I want to...                          | Read this                                         |
| ------------------------------------- | ------------------------------------------------- |
| Set up from scratch                   | [Getting Started](docs/getting-started.md)        |
| Compare two revisions                 | [Comparing Branches](docs/comparing-branches.md)  |
| Track performance over many revisions | [Longitudinal Benchmarking](docs/longitudinal.md) |
| Run on dedicated cloud hardware       | [Cloud Runner](docs/cloud-runner.md)              |
| Look up a flag, metric, or schema     | [Reference](docs/reference.md)                    |
| Understand how the harness works      | [Architecture](docs/architecture.md)              |
| Validate the trust contract           | [Validation](docs/validation.md)                  |

## What's Covered

35 benchmark cases across 8 suites: `scan`, `write`, `delete_update`, `merge`, `metadata`, `optimize_vacuum`, `tpcds`, and `interop_py`. `interop_py` is correctness-backed coverage for the Python runtime path in addition to the Rust-native suites.

See [Reference](docs/reference.md#benchmark-suites-and-cases) for the full listing.

## Scripts

| Script                               | Purpose                                                              |
| ------------------------------------ | -------------------------------------------------------------------- |
| `./scripts/bench.sh`                 | Generate fixtures, run suites, list benchmarks, health checks        |
| `./scripts/compare_branch.sh`        | Branch-to-branch comparison with aggregated reporting                |
| `./scripts/longitudinal_bench.sh`    | Longitudinal matrix, ingest, reporting, and retention                |
| `./scripts/cleanup_local.sh`         | Clean fixtures, results, and checkout artifacts (dry-run by default) |
| `./scripts/validate_perf_harness.sh` | Trust-contract verification for perf claims                          |

## Contributing

```bash
cargo test --locked
(cd python && python3 -m pytest -q tests)
./scripts/validate_perf_harness.sh
```

Run these before opening a PR. See [Getting Started](docs/getting-started.md#local-ci-baseline) for the full CI baseline.
