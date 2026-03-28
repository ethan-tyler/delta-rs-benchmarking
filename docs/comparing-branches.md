# Comparing Branches

This guide covers the primary contributor workflow: running the same benchmarks against two revisions of delta-rs and seeing what changed.

## Table of Contents

- [When to Use Branch Comparison](#when-to-use-branch-comparison)
- [Quick Start: Compare Your Branch Against Main](#quick-start-compare-your-branch-against-main)
- [Comparison Methods](#comparison-methods)
- [What Compare Does Under the Hood](#what-compare-does-under-the-hood)
- [Tuning Your Comparison](#tuning-your-comparison)
- [Reliable Comparison Protocol](#reliable-comparison-protocol)
- [Reading the Report](#reading-the-report)
- [Next Steps](#next-steps)

## When to Use Branch Comparison

Branch comparison answers the question: "did my change make things faster, slower, or about the same?" It runs every benchmark case against both a base revision and a candidate revision, then classifies each case as a regression, improvement, or stable.

Use it before merging PRs, validating optimizations, or making release decisions. For tracking performance across many revisions over time, use [Longitudinal Benchmarking](longitudinal.md) instead.

## Quick Start: Compare Your Branch Against Main

The fastest way to compare your current checkout against upstream `main`:

```bash
./scripts/compare_branch.sh --current-vs-main scan
```

This builds and benchmarks both your current checkout and the latest remote `main`, then prints a grouped report showing regressions, improvements, stable cases, and inconclusive cases. The compare pipeline always runs the macro lane in `--mode perf`. `compare_branch.sh` defaults to `--compare-mode exploratory`, which is suitable for same-machine investigation but not for automatic pass/fail decisions. Decision mode is explicit and requires schema v4 payloads with complete compatibility identity.

Automation policy is narrower than local experimentation:
- PR comments support `run benchmark scan` for exploratory output and `run benchmark decision scan` for decision-grade automation.
- Self-hosted PR and nightly perf workflows are currently curated to `scan` only.
- Stateful Rust suites remain correctness-trusted workloads. If you force them through macro lane, the result is operational but not perf-valid.

## Comparison Methods

### Current checkout vs upstream main

The simplest option. Compares whatever commit is checked out in `.delta-rs-under-test` against the latest `origin/main`:

```bash
./scripts/compare_branch.sh --current-vs-main scan
```

Use this when you are working on a branch and want to check your changes against main without specifying exact SHAs.

### Named branch-to-branch

Compare any two branches or refs that exist in the delta-rs checkout:

```bash
./scripts/compare_branch.sh main <candidate_ref> scan
```

The `<candidate_ref>` must exist in `.delta-rs-under-test`. To see available refs:

```bash
git -C .delta-rs-under-test branch -a
```

### Immutable SHA compare (recommended for long runs)

Pin both sides to exact commit SHAs for fully reproducible results:

```bash
./scripts/compare_branch.sh \
  --base-sha 5a0c8d7f3f2d9d42fdd9414f1ce2af319e0c52e1 \
  --candidate-sha 8c6170f1de4af5e2d3336b4fce8a9896af4d9b90 \
  scan
```

This is the most reproducible option because branches can move during a long run, but SHAs cannot. Prefer this for benchmarks that take more than a few minutes.

## What Compare Does Under the Hood

When you run a comparison, the script executes these steps in order:

1. **Updates the checkout** -- fetches the latest state of `.delta-rs-under-test` and resolves the base and candidate refs.
2. **Syncs the harness** -- copies the benchmark crate and configuration into the delta-rs workspace so Cargo can build it.
3. **Generates fixtures** -- creates deterministic test data using the base revision, ensuring both sides benchmark against identical input.
4. **Runs prewarm iterations** (optional) -- executes unreported warmup iterations for both refs to prime caches and stabilize thermal state.
5. **Runs measured iterations** -- executes the configured number of measured benchmark runs for base and candidate in the configured order (alternating by default).
6. **Aggregates results** -- combines all measured runs for each side into a single JSON payload using the configured aggregation method (median by default).
7. **Prints the report** -- classifies each case and prints grouped output for valid perf comparisons only. Invalid or mismatched inputs fail closed before comparison.

## Tuning Your Comparison

### Benchmark flags

These flags control the measurement itself:

| Flag | Default | Description |
|---|---|---|
| `--warmup` | `2` | Warmup iterations per case (not measured). |
| `--iters` | `9` | Measured iterations per case per run. |
| `--prewarm-iters` | `1` | Unreported warmup iterations per ref (run before any measured iterations). |
| `--compare-runs` | `3` | Number of independent measured runs per ref before aggregation. Exploratory mode can use this default; decision mode should use at least `5`. |
| `--measure-order` | `alternate` | Run interleaving: `base-first`, `candidate-first`, or `alternate`. |
| `--aggregation` | `median` | How to pick the representative sample: `min`, `median`, or `p95`. |
| `--noise-threshold` | `0.05` | Minimum relative change to classify as regression or improvement. |
| `--compare-mode` | `exploratory` | Comparison policy: `exploratory` for investigation, `decision` for run-level bootstrap classification on schema v4 payloads. |
| `--fail-on` | — | Comma-separated compare statuses that should exit non-zero after rendering (used by decision automation). |
| `--mode` | `perf` | Benchmark mode forwarded to `bench.sh run`. Branch comparison should stay on `perf`. |
| `--dataset-id` | — | Dataset id forwarded to fixture generation and benchmark runs. |
| `--timing-phase` | `execute` | Isolated timing phase for phase-aware suites. |

### Environment variables

These control execution behavior at the script level:

| Variable | Default | Description |
|---|---|---|
| `BENCH_TIMEOUT_SECONDS` | `3600` | Maximum time per benchmark step before timeout. |
| `BENCH_RETRY_ATTEMPTS` | `2` | Number of retries for transient failures. |
| `BENCH_RETRY_DELAY_SECONDS` | `5` | Delay between retries. |

### Adding metric columns to the report

To see per-case metrics (rows processed, files scanned, etc.) alongside timing data:

```bash
cd python && python3 -m delta_bench_compare.compare ... --include-metrics
```

### Concurrency suite guidance

For `target=concurrency`, change classification still uses `elapsed_ms`, but the nested contention counters are the real interpretation layer for the contended cases. Render the report with `--include-metrics` so the `metrics.contention` columns are visible when you compare runs. `table_version` is only a useful sanity metric for `concurrent_table_create` and `concurrent_append_multi`; the contended cases intentionally leave it null because each measured sample aggregates independent fixture copies.

Recommended settings for this suite:

```bash
./scripts/compare_branch.sh \
  --current-vs-main \
  --compare-runs 5 \
  --noise-threshold 0.10 \
  --aggregation median \
  --measure-order alternate \
  concurrency
```

Case interpretation:

- `concurrent_table_create`, `concurrent_append_multi`: primary signal is `elapsed_ms`; `ops_succeeded` is the secondary sanity check.
- `update_vs_compaction`: localized update-versus-compaction race; primary signals are `ops_succeeded` and `conflict_delete_read`; treat `elapsed_ms` as secondary.
- `delete_vs_compaction`: scattered delete-versus-compaction race; primary signals are `ops_succeeded` and `conflict_delete_read`; treat `elapsed_ms` as secondary.
- `optimize_vs_optimize_overlap`: primary signal is preserved overlapping-remove conflict behavior, especially `conflict_delete_delete`; treat `elapsed_ms` as secondary.

## Reliable Comparison Protocol

For PR merge decisions and release validation, follow these practices to minimize noise and maximize confidence:

1. **Use immutable refs.** Pass `--base-sha` and `--candidate-sha` (or `--current-vs-main`) so the revisions cannot shift during the run.
2. **Run on an idle machine.** Keep the system otherwise quiet and use the same backend/profile for both refs.
3. **Keep alternating order.** The default `--measure-order alternate` reduces drift from cache warming, thermal throttling, and background processes by interleaving base and candidate runs.
4. **Use median aggregation.** The default `median` is robust to outliers. Switch to `p95` only when analyzing tail latency.
5. **Watch for noise.** If `cv_pct` (coefficient of variation) exceeds 10% for a case, the measurement is noisy. Rerun with higher `--compare-runs` or `--iters` to increase sample size.

Decision-grade command with the current explicit trust contract:

```bash
./scripts/compare_branch.sh \
  --current-vs-main \
  --compare-mode decision \
  --warmup 2 \
  --iters 9 \
  --prewarm-iters 1 \
  --compare-runs 5 \
  scan
```

Use `scan` for decision-grade automation today. `tpcds` remains manual until fixture provisioning is explicit, and stateful Rust suites remain decision-invalid in macro lane because their trusted path is the correctness lane.

## Reading the Report

The comparison report groups benchmark cases into four sections:

| Section | Meaning |
|---|---|
| **Regressions** | Cases where the candidate is slower than the base beyond the noise threshold. Investigate before merging. |
| **Improvements** | Cases where the candidate is faster than the base beyond the noise threshold. |
| **Stable** | Cases where performance is within the noise threshold. No action needed. |
| **Comparison aborted** | Any invalid workload or mismatched benchmark context. Compare fails closed instead of producing a perf claim. |

Key metrics to look at:

- **Relative change (%)** -- how much faster or slower the candidate is compared to the base.
- **cv_pct** -- coefficient of variation as a percentage. Below 5% is good. Above 10% means the measurement is noisy and you should increase `--compare-runs` or `--iters`.
- **median_ms** -- the representative timing for each case.

For the complete list of metrics that may appear in the report, see [Reference](reference.md#metrics-reference).

## Next Steps

- **Track trends over time** -- see [Longitudinal Benchmarking](longitudinal.md) for regression detection across many revisions.
- **Run on dedicated hardware** -- see [Cloud Runner](cloud-runner.md) for noise-isolated benchmarks on hardened infrastructure.
- **Understand the result format** -- see [Reference](reference.md#result-schema-v4) for the complete schema v4 field listing.
