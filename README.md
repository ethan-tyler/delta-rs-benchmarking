# delta-rs-benchmarking

Benchmark harness for [delta-rs](https://github.com/delta-io/delta-rs). Run reproducible benchmarks against any delta-rs revision, compare branches on the same machine, and track performance over time.

Anyone can use this repository, but it is maintained as tooling for `delta-rs` performance work rather than as a general community project. Changes to the harness should come through reviewed pull requests that support `delta-rs` benchmarking needs.

This repository is separate from delta-rs itself. It manages a delta-rs checkout, generates deterministic test data, runs benchmark suites, and writes structured schema v5 JSON results.

## Quick Start

Use the managed checkout flow unless you already have a local `delta-rs` workspace you want to benchmark.

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
```

This prepares the managed checkout at `.delta-rs-under-test/` and syncs the harness into that workspace.

```bash
./scripts/bench.sh data --dataset-id tiny_smoke --seed 42
```

This generates a fast deterministic fixture set for local smoke validation.

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

This writes `results/local/scan.json` and prints a local summary table.

```bash
./scripts/compare_branch.sh --current-vs-main scan
```

Single-suite runs land in `results/local/scan.json`. Branch compare also writes a bot-friendly bundle under `results/compare/<suite>/<base>__<candidate>/` with `summary.md`, versioned `comparison.json`, `hash-policy.txt`, and `manifest.json`. Use `--candidate-fetch-url <clone-url>` when a trusted candidate SHA only exists on a fork remote; prefer the full 40-character SHA, or set `DELTA_RS_FETCH_REF` when the abbreviated SHA is not directly advertised. Pass `--help` to any script for full usage details.

`bench.sh run` defaults to the `smoke` lane. Use `correctness` for correctness-backed suites (`write`, `delete_update`, `merge`, `metadata`, `optimize_vacuum`, `interop_py`) and use `macro` only for macro-safe perf exploration. GitHub-hosted CI stays on smoke and correctness lanes, while self-hosted workflows are the authoritative path for macro perf, decision compare, and longitudinal automation.

When you need a portable copy of the current operator contract, publish it with `./scripts/publish_contract.sh`. That snapshots the current docs, manifests, and wrapper scripts into `results/contracts/`.

For Python interop coverage (`--runner all`), install dependencies first: `python3 -m pip install -r python/requirements-audit.txt`

## What You Can Do

| Goal                                                   | Guide                                             |
| ------------------------------------------------------ | ------------------------------------------------- |
| Full setup walkthrough, datasets, backends, cleanup    | [Getting Started](docs/getting-started.md)        |
| Compare performance between two revisions              | [Comparing Branches](docs/comparing-branches.md)  |
| Regression detection across many revisions             | [Longitudinal Benchmarking](docs/longitudinal.md) |
| Dedicated cloud infrastructure and hardened runners    | [Cloud Runner](docs/cloud-runner.md)              |
| Suites, metrics, flags, schemas, environment variables | [Reference](docs/reference.md)                    |
| Internals and data flow                                | [Architecture](docs/architecture.md)              |
| Trust contract and validation protocol                 | [Validation](docs/validation.md)                  |

## Scripts

| Script                               | Purpose                                                              |
| ------------------------------------ | -------------------------------------------------------------------- |
| `./scripts/bench.sh`                 | Generate fixtures, run suites, list benchmarks, health checks        |
| `./scripts/compare_branch.sh`        | Branch-to-branch comparison with aggregated reporting                |
| `./scripts/longitudinal_bench.sh`    | Longitudinal matrix, ingest, reporting, and retention                |
| `./scripts/cleanup_local.sh`         | Clean fixtures, results, and checkout artifacts (dry-run by default) |
| `./scripts/validate_perf_harness.sh` | Trust-contract verification for perf claims                          |
| `./scripts/publish_contract.sh`      | Publish the current operator/docs contract bundle                    |

## Benchmark Coverage

35 cases across 8 suites: `scan`, `write`, `delete_update`, `merge`, `metadata`, `optimize_vacuum`, `tpcds`, `interop_py`. Both Rust-native and Python interop paths. See [Reference](docs/reference.md#benchmark-suites-and-cases) for the full case listing.

## Contributing

Keep changes scoped and send reviewed PRs. Run the CI baseline before submitting:

```bash
cargo test --locked
(cd python && python3 -m pytest -q tests)
```

See [Getting Started](docs/getting-started.md#local-ci-baseline) for the full verification baseline including dependency audits and self-hosted preflight checks.
