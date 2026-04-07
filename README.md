# delta-rs-benchmarking

Reproducible benchmark harness for [delta-rs](https://github.com/delta-io/delta-rs). Measure performance against any revision, compare branches, and track regressions over time.

## Quick Start

Prepare the checkout, generate fixtures, run a smoke benchmark, then compare your branch against `main`:

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
./scripts/bench.sh data --dataset-id tiny_smoke --seed 42
./scripts/bench.sh run --suite scan --runner rust --dataset-id tiny_smoke --label local
./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-macro scan
```

Results go to `results/local/<suite>.json`. Pass `--help` to any script for details.

For the full setup walkthrough, see [Getting Started](docs/getting-started.md).

`bench.sh run` defaults to the `smoke` lane. Use `correctness` for correctness-backed suites such as `write`, `delete_update`, `merge`, `metadata`, `optimize_vacuum`, and `interop_py`. Use `macro` only for macro-safe perf exploration. GitHub-hosted CI stays on smoke and correctness lanes, while self-hosted workflows are the authoritative path for macro perf, decision compare, and longitudinal automation.

`tiny_smoke` stays the fast setup/smoke dataset. The PR macro compare contract is stronger: `--methodology-profile pr-macro` automatically switches branch compare onto the deterministic local-disk `medium_selective` dataset and the decision-grade 7-run / 15-iteration methodology.

Choose the benchmark surface that matches the change:

- Use `scan` plus `pr-macro` when the suspected effect is on query execution or Parquet reads.
- Use `metadata_perf` plus `pr-metadata-perf` when the suspected effect is on checkpoint loading, long-history log replay, or metadata-heavy table open paths.
- Use `tpcds` plus `pr-tpcds` on trusted self-hosted runners when the suspected effect is on analytical execute-path regressions against the DuckDB-backed `tpcds_duckdb` corpus. `tpcds_q72` remains outside the PR decision surface.
- Keep `scan` as the public execute-phase guardrail for replay-adjacent work, then pair it with `cargo bench -p delta-bench --bench metadata_replay_bench` when you need the narrower replay-state or snapshot-owned provider signal.
- Use Criterion as the primary signal when replay-state timings stay sub-millisecond or too noisy for branch-compare classification.
- Use `run benchmark decision full` only for the harness-owned `pr-full-decision` pack in `bench/evidence/registry.yaml`. `full` does not mean `--suite all`, and the bot blocks the command until every listed suite is `readiness=ready`.

In operator terms: full does not mean --suite all.

Keep the replay-state probe separate from the execute-phase guardrail:

```bash
cargo bench -p delta-bench --bench metadata_replay_bench
```

The replay-state microbench is investigation-grade. Do not substitute it for the default execute-phase guardrail:

```bash
./scripts/compare_branch.sh \
  --base-sha 61ea71b77d3322bec3ddb857685a46562925d9fd \
  --candidate-sha 385e7bd1730a2d21703001777d1368ffce5ce559 \
  --timing-phase plan \
  scan
```

```bash
./scripts/compare_branch.sh \
  --base-sha 61ea71b77d3322bec3ddb857685a46562925d9fd \
  --candidate-sha 385e7bd1730a2d21703001777d1368ffce5ce559 \
  --methodology-profile pr-macro \
  scan
```

For ad hoc historical replay-state checks, `scan --timing-phase plan` remains an approximation-only fallback. The current `scan` suite registers `table.table_provider()` from loaded eager state, so `timing_phase=plan` is not proof of the snapshot-owned replay-state path by itself.

For Python interop coverage, install `python/requirements-audit.txt` first. For trust-contract verification, read [Validation](docs/validation.md) and run `./scripts/validate_perf_harness.sh`.

## What You Can Do

- Run local smoke checks on any machine with `./scripts/bench.sh run`.
- Use GitHub-hosted CI for smoke and correctness validation, including correctness-backed suites such as `interop_py`.
- Use self-hosted runners for macro perf, decision compare, Criterion microbench, and longitudinal workflows.
- For PR macro evidence, run `./scripts/compare_branch.sh --methodology-profile pr-macro ...`; the profile fixes the decision-grade compare contract, uses `medium_selective`, and keeps sub-millisecond cases out of the normal macro verdict.
- For PR comment automation, use `run benchmark scan`, `run benchmark decision scan`, `run benchmark decision full`, and `show benchmark queue`.

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

The reference surface currently covers `scan`, `write`, `write_perf`, `delete_update`, `delete_update_perf`, `merge`, `merge_perf`, `metadata`, `metadata_perf`, `optimize_vacuum`, `optimize_perf`, `concurrency`, `tpcds`, and `interop_py`. `concurrency` covers Rust-only contention paths, and `interop_py` is correctness-backed coverage for the Python runtime path in addition to the Rust-native suites. Replay-state internals stay in the dedicated `metadata_replay_bench` engineering probe instead of a public suite contract.

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
