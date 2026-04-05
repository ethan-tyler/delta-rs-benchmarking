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

Choose the benchmark surface based on the path you changed:

- Use `scan` plus the self-hosted `pr-macro` profile when the suspected effect is on query execution or Parquet reads.
- Use `write_perf` plus the self-hosted `pr-write-perf` profile when the suspected effect is on write throughput, partition fanout, or file-creation cost.
- Use `delete_update_perf` plus `pr-delete-update-perf` when the suspected effect is on delete/update rewrite cost, file churn, or scattered-vs-localized DML behavior.
- Use `merge_perf` plus `pr-merge-perf` when the suspected effect is on merge upsert/delete cost, file pruning, or partition-aware merge execution.
- Use `optimize_perf` plus `pr-optimize-perf` when the suspected effect is on compaction or vacuum maintenance work.
- Use `tpcds` plus the self-hosted `pr-tpcds` profile when the suspected effect is on analytical execute-path behavior against the DuckDB-backed `store_sales` corpus.
- Use Criterion as the local diagnostic signal when replay/provider work or scan-internal timings stay sub-millisecond or too noisy for compare classification. That signal is never authoritative PR evidence.

## Quick Start: Compare Your Branch Against Main

The fastest way to compare your current checkout against upstream `main`:

```bash
./scripts/compare_branch.sh --current-vs-main scan
```

This builds and benchmarks both your current checkout and the latest remote `main`, then prints a grouped report showing regressions, improvements, stable cases, and inconclusive results.

For self-hosted PR evidence, load the harness-owned methodology profile instead of repeating raw decision knobs:

```bash
./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-macro scan
```

For write-path investigation on the current self-hosted operator contract, use:

```bash
./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-write-perf write_perf
```

For DML and maintenance investigation on the new perf-owned candidate/manual surfaces, use:

```bash
./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-delete-update-perf delete_update_perf
./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-merge-perf merge_perf
./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-optimize-perf optimize_perf
```

For TPC-DS investigation on the dedicated self-hosted operator contract, use:

```bash
./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-tpcds tpcds
```

The compare pipeline runs the macro lane in `--mode perf`. By default `compare_branch.sh` uses `--compare-mode exploratory`, which works for same-machine investigation but not automatic pass/fail decisions. `--methodology-profile pr-macro` resolves the current PR decision contract: `dataset_id=medium_selective`, `compare_mode=decision`, `warmup=2`, `iters=15`, `prewarm_iters=1`, `compare_runs=7`, `measure_order=alternate`, `timing_phase=execute`, `aggregation=median`, `spread_metric=iqr_ms`, and `sub_ms_policy=micro_only` for rows where both sides stay below the configured sub-millisecond threshold.

`--methodology-profile pr-write-perf` resolves the write-path contract: `compare_mode=decision`, `dataset_id=null`, `dataset_policy=intrinsic_case_workload`, `warmup=1`, `iters=7`, `prewarm_iters=1`, `compare_runs=5`, `measure_order=alternate`, `timing_phase=execute`, `aggregation=median`, and `spread_metric=iqr_ms`. That profile is the operator path for `write_perf`, but the registry still keeps `write_perf` gated until same-SHA stability, delayed-canary validation, and runtime signoff are complete.

`--methodology-profile pr-delete-update-perf`, `pr-merge-perf`, and `pr-optimize-perf` resolve the initial perf-owned DML and maintenance contracts. All three fix `dataset_id=medium_selective`, record `dataset_policy=shared_run_scope`, and use decision-mode compare defaults with five measured runs per side. They are candidate/manual surfaces first: the legacy `delete_update`, `merge`, and `optimize_vacuum` suites remain correctness-only, while the dedicated perf-owned counterparts stay gated until same-SHA stability, delayed-canary validation, runtime signoff, and one stable case-list refresh cycle are all complete.

`--methodology-profile pr-tpcds` resolves the TPC-DS contract: `dataset_id=tpcds_duckdb`, `compare_mode=decision`, `warmup=1`, `iters=5`, `prewarm_iters=1`, `compare_runs=5`, `measure_order=alternate`, `timing_phase=execute`, `aggregation=median`, and `spread_metric=iqr_ms`. Use it only on trusted self-hosted runners with the DuckDB-backed fixture tree provisioned ahead of time. `tpcds_q72` remains outside the PR decision surface, and the suite stays gated in `bench/evidence/registry.yaml` until fixture provisioning plus validation signoff evidence exists. Refresh the dedicated gate with `./scripts/validate_perf_harness.sh --dataset-id tpcds_duckdb --artifact-dir results/validation/tpcds-gate`.

Criterion profiles live in the same `bench/methodologies/` directory, but `compare_branch.sh` intentionally rejects `PROFILE_KIND=criterion`. Invoke them through `./scripts/run_profile.sh scan-phase-criterion` or `./scripts/run_profile.sh metadata-replay-criterion` instead. `metadata-replay-criterion` resolves to `scan_replay_bench`, and both committed profiles stay diagnostic-only rather than authoritative PR evidence.

Before treating a machine or workflow as trustworthy for perf claims, rerun `./scripts/validate_perf_harness.sh` and review [Validation](validation.md).

> **Automation scope.** PR comments support `run benchmark scan` (exploratory), `run benchmark decision scan` (decision-grade), `run benchmark decision full`, and `show benchmark queue`. The single-suite decision path runs `./scripts/compare_branch.sh --methodology-profile pr-macro ...` on self-hosted hardware. The `full` command resolves the harness-owned `pr-full-decision` pack from `bench/evidence/registry.yaml`; full does not mean --suite all, and `pr-full-decision` contains only `readiness=ready` suites. Operators can still batch gated perf suites through `pr-candidate-manual`; that candidate/manual pack now includes `write_perf`, `delete_update_perf`, `merge_perf`, `optimize_perf`, and `tpcds`. The legacy `metadata`, `delete_update`, `merge`, and `optimize_vacuum` suites stay correctness-only, so forcing those originals through macro lane still produces operational but not `perf_status=trusted` results. `scan_pruning_hit` moved to Criterion microbench coverage because it is too small/cache-sensitive to treat as a normal macro verdict row. `scan_pruning_miss` remains disabled until it is requalified. GitHub-hosted CI stays on smoke and correctness lanes.

## Replay-State Probes

Keep the execute-phase guardrail and the replay-state investigation probe separate.

Use the public `scan` contract for the product-facing PR verdict, then run the narrower replay/provider engineering probe separately:

```bash
./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-macro scan
./scripts/run_profile.sh metadata-replay-criterion
```

`metadata-replay-criterion` resolves to `scan_replay_bench`. It is investigation-grade, isolates snapshot/provider replay work behind the existing scan replay bench, and does not replace the macro `scan` compare surface or enter PR verdict logic.

Operator rule set:

- Replay/provider investigations: run `scan` with `pr-macro`, then run `./scripts/run_profile.sh metadata-replay-criterion` when the regression smells replay- or provider-local.
- Execute-path PRs: run `scan` with `pr-macro`.
- Mixed PRs: run the public compare surface first, then any relevant Criterion probes, and report the Criterion output separately.

## Comparison Methods

### Current checkout vs upstream main

The simplest option. Compares whatever commit is checked out in `.delta-rs-under-test` against the latest `origin/main`:

```bash
./scripts/compare_branch.sh --current-vs-main scan
```

Use this when you want to check your branch against main without specifying exact SHAs.

### Methodology profiles

Use `--methodology-profile <name>` to load harness-owned compare defaults from `bench/methodologies/<name>.env`. Explicit CLI flags still win, so local operators can override a profile for ad hoc investigation without editing the profile itself. If an invocation changes any profile-owned setting explicitly, `manifest.json` keeps the resolved `methodology_settings` but omits canonical `methodology_profile` and `methodology_version` so the artifact cannot be mistaken for an exact `pr-macro` run.

`pr-macro` is the current self-hosted PR decision profile. It resolves to the deterministic `medium_selective` local-disk dataset, decision mode, seven measured runs per ref, 15 measured iterations per run, median aggregation, `iqr_ms` spread reporting, and `micro_only` decision scope for sub-millisecond rows that should stay out of macro regression counts.

`pr-write-perf` is the write-path operator profile. It keeps `dataset_id` intentionally unset because the workload lives in the case ids themselves, and records `dataset_policy=intrinsic_case_workload` so the compare manifest makes that contract explicit. Use it for trusted self-hosted `write_perf` evidence and rerun `./scripts/validate_perf_harness.sh --artifact-dir results/validation/write-perf-gate` before enabling bot-default decision runs.

`pr-delete-update-perf`, `pr-merge-perf`, and `pr-optimize-perf` are the perf-owned DML and maintenance operator profiles. They all fix `dataset_id=medium_selective`, `compare_mode=decision`, `compare_runs=5`, `measure_order=alternate`, `timing_phase=execute`, `aggregation=median`, and `spread_metric=iqr_ms`. Use them for candidate/manual evidence refreshes on `delete_update_perf`, `merge_perf`, and `optimize_perf`; the underlying correctness suites remain correctness-only and are not perf evidence.

`pr-tpcds` is the TPC-DS operator profile. It fixes `dataset_id=tpcds_duckdb`, expects the fixture root to exist on trusted self-hosted runners before the compare begins, and keeps `tpcds_q72` outside the PR decision surface. Use it for dedicated TPC-DS evidence and rerun `./scripts/validate_perf_harness.sh --dataset-id tpcds_duckdb --artifact-dir results/validation/tpcds-gate` before treating the output as promotion evidence.

The PR pack registry lives in `bench/evidence/registry.yaml`. `pr-full-decision` maps the `full` alias to the ready PR decision surface only and contains only `readiness=ready` suites. `pr-candidate-manual` collects gated perf-owned suites for operator-run evidence refreshes: `write_perf`, `delete_update_perf`, `merge_perf`, `optimize_perf`, and `tpcds`. `tpcds` remains candidate/manual there and stays out of PR comment automation until its gates are closed.

### Named branch-to-branch

Compare any two branches or refs that exist in the clean source checkout used for compare pinning:

```bash
./scripts/compare_branch.sh main <candidate_ref> scan
```

The `<candidate_ref>` must exist in `.delta-rs-source` (or your overridden `DELTA_RS_SOURCE_DIR`). To see available refs:

```bash
git -C .delta-rs-source branch -a
```

### Immutable SHA compare (recommended for long runs)

Pin both sides to exact commit SHAs for fully reproducible results:

```bash
./scripts/compare_branch.sh \
  --base-sha 5a0c8d7f3f2d9d42fdd9414f1ce2af319e0c52e1 \
  --candidate-sha 8c6170f1de4af5e2d3336b4fce8a9896af4d9b90 \
  --methodology-profile pr-macro \
  scan
```

This is the most reproducible option because branches can move during a long run, but SHAs cannot. Prefer this for benchmarks that take more than a few minutes.

If a trusted PR head SHA lives on a fork remote instead of `origin`, pass the fork URL explicitly so checkout prep can fetch that immutable ref:

```bash
./scripts/compare_branch.sh \
  --base-sha 5a0c8d7f3f2d9d42fdd9414f1ce2af319e0c52e1 \
  --candidate-sha 8c6170f1de4af5e2d3336b4fce8a9896af4d9b90 \
  --candidate-fetch-url https://github.com/example/delta-rs \
  --methodology-profile pr-macro \
  scan
```

Use `--base-fetch-url` the same way when the base SHA is only reachable from a non-`origin` remote. Prefer the full 40-character SHA when you use an alternate fetch URL. If you only have an abbreviated SHA and it is not directly advertised by the alternate remote, set `DELTA_RS_FETCH_REF` to an advertised branch/ref so the checkout can fetch that history first. The lower-level checkout contract is also available through `prepare_delta_rs.sh` via `DELTA_RS_FETCH_URL` and `DELTA_RS_FETCH_REF`.

By default compare keeps two local delta-rs roots with different responsibilities:

- `.delta-rs-source/` or `DELTA_RS_SOURCE_DIR` stays clean and is used only for branch lookup, immutable SHA pinning, and seeding per-SHA compare checkouts.
- `.delta-rs-under-test/` or `DELTA_RS_DIR` is the mutable execution checkout that receives synced harness files.

This means compare no longer requires `.delta-rs-under-test/` to stay clean between runs.

`--current-vs-main` is the one exception to the seeding rule: its candidate prepared checkout is fetched from `.delta-rs-under-test/` so the current local HEAD remains reachable even when it has not been copied into `.delta-rs-source/`.

### Criterion microbench compare

Use the Criterion lane for tiny or highly cache-sensitive scan cases that should not drive the normal PR macro verdict. Today that means `scan_pruning_hit`. Run these profiles on the same local or trusted self-hosted host, treat them as diagnostic-only, and report them separately from any authoritative PR evidence.

The committed Criterion entrypoints are:

```bash
./scripts/run_profile.sh scan-phase-criterion
./scripts/run_profile.sh metadata-replay-criterion
```

Run the first command on the pinned base checkout, then run the second command on the pinned candidate checkout:

```bash
./scripts/run_profile.sh scan-phase-criterion -- scan_pruning_hit/phase/execute --save-baseline pr-base
./scripts/run_profile.sh scan-phase-criterion -- scan_pruning_hit/phase/execute --baseline pr-base
```

This keeps the micro case observable with Criterion’s own statistics while leaving `compare_branch.sh --methodology-profile pr-macro` focused on macro-safe scan coverage. Criterion baselines stay outside `bench/evidence/registry.yaml` packs, PR comment automation, and longitudinal ingest.

## What Compare Does Under the Hood

When you run a comparison, the script executes these steps in order:

1. **Pins immutable refs** -- updates `.delta-rs-source` (or `DELTA_RS_SOURCE_DIR`), and for immutable SHAs can fall back to a trusted alternate remote URL before resolving the base and candidate refs.
2. **Prepares per-ref worktrees** -- creates one synced checkout per pinned SHA under `.delta-bench-compare-checkouts/` by cloning/fetching from the clean source checkout. `--current-vs-main` keeps the candidate side reachable from `.delta-rs-under-test/`. The workflow then reuses those prepared directories for prewarm and measured runs instead of flipping one checkout back and forth.
3. **Generates fixtures** -- creates deterministic test data using the base revision, ensuring both sides benchmark against identical input.
4. **Runs prewarm iterations** (optional) -- executes unreported warmup iterations for both refs to prime caches and stabilize thermal state.
5. **Runs measured iterations** -- executes the configured number of measured benchmark runs for base and candidate in the configured order (alternating by default).
6. **Aggregates results** -- combines all measured runs for each side into a single JSON payload using the configured aggregation method (median by default).
7. **Writes compare artifacts** -- stores `stdout.txt`, `summary.md`, `comparison.json`, `hash-policy.txt`, and `manifest.json` under `results/compare/<suite>/<base>__<candidate>/`.
8. **Prints the report** -- classifies each case and prints grouped output. Exploratory mode keeps trusted cases even when one case is invalid and renders those aborted cases in a dedicated section. Decision mode still fails closed on any invalid or mismatched input.

## Tuning Your Comparison

### Benchmark flags

These flags control the measurement itself:

| Flag                | Default       | Description                                                                                                                                   |
| ------------------- | ------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `--warmup`          | `2`           | Warmup iterations per case (not measured).                                                                                                    |
| `--iters`           | `9`           | Measured iterations per case per run.                                                                                                         |
| `--prewarm-iters`   | `1`           | Unreported warmup iterations per ref (run before any measured iterations).                                                                    |
| `--compare-runs`    | `3`           | Number of independent measured runs per ref before aggregation. Exploratory mode can use this default; decision mode hard-requires at least `5`, and `pr-macro` raises it to `7`. |
| `--measure-order`   | `alternate`   | Run interleaving: `base-first`, `candidate-first`, or `alternate`.                                                                            |
| `--aggregation`     | `median`      | How to pick the representative sample: `min`, `median`, or `p95`.                                                                             |
| `--noise-threshold` | `0.05`        | Minimum relative change to classify as regression or improvement.                                                                             |
| `--methodology-profile` | —         | Load a harness-owned methodology profile such as `pr-macro`. Explicit CLI flags still override profile defaults.                              |
| `--compare-mode`    | `exploratory` | Comparison policy: `exploratory` for investigation, `decision` for run-level bootstrap classification on schema v5 payloads.                  |
| `--fail-on`         | —             | Comma-separated compare statuses that should exit non-zero after rendering (used by decision automation).                                     |
| `--mode`            | `perf`        | Benchmark mode forwarded to `bench.sh run`. Branch comparison should stay on `perf`.                                                          |
| `--dataset-id`      | —             | Dataset id forwarded to fixture generation and benchmark runs. `pr-macro` defaults this to `medium_selective`.                              |
| `--timing-phase`    | `execute`     | Isolated timing phase for phase-aware suites.                                                                                                 |

Profiles only supply defaults. If you pass `--compare-runs`, `--aggregation`, or other compare knobs explicitly, those values override the methodology profile for that invocation. Once you do that, the compare manifest still records the resolved settings but drops canonical profile identity because the run no longer matches the exact harness-owned contract.

### Environment variables

These control execution behavior at the script level:

| Variable                    | Default | Description                                     |
| --------------------------- | ------- | ----------------------------------------------- |
| `BENCH_TIMEOUT_SECONDS`     | `3600`  | Maximum time per benchmark step before timeout. |
| `BENCH_RETRY_ATTEMPTS`      | `2`     | Number of retries for transient failures.       |
| `BENCH_RETRY_DELAY_SECONDS` | `5`     | Delay between retries.                          |
| `DELTA_RS_SOURCE_DIR`       | `.delta-rs-source` | Clean checkout used for compare ref resolution and per-SHA checkout seeding. |
| `DELTA_BENCH_MIN_FREE_GB`   | `20`    | Local-only minimum free-space floor enforced before compare checkout prep. |

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
3. **Keep local disk headroom above the preflight floor.** `compare_branch.sh` fails closed below `20` GiB free by default. Clear stale `target/` trees or cached compare checkouts before long local compare sessions, or raise/lower the floor explicitly with `DELTA_BENCH_MIN_FREE_GB` when your machine contract differs.
4. **Use a shared Cargo target directory locally.** If you hit per-checkout target instability, standardize local compares on:

```bash
export CARGO_TARGET_DIR="$PWD/target"
```

5. **Keep alternating order.** The default `--measure-order alternate` reduces drift from cache warming, thermal throttling, and background processes by interleaving base and candidate runs.
6. **Use median aggregation.** The default `median` is robust to outliers. Switch to `p95` only when analyzing tail latency.
7. **Watch for noise.** If `cv_pct` (coefficient of variation) exceeds 10% for a case, the measurement is noisy. Rerun with higher `--compare-runs` or `--iters` to increase sample size.
8. **Revalidate the harness before using the output as evidence.** Run `./scripts/validate_perf_harness.sh` and inspect the refreshed `summary.md` in its artifact directory against the operator guidance in `docs/validation.md`. If you want a stable local path, use `--artifact-dir results/validation/latest`.

Decision-grade command with the current explicit trust contract:

```bash
./scripts/compare_branch.sh \
  --current-vs-main \
  --methodology-profile pr-macro \
  scan
```

Use `scan` for decision-grade automation today. `write_perf`, `delete_update_perf`, `merge_perf`, `optimize_perf`, and `tpcds` are still candidate/manual until their promotion gates close, and the legacy stateful Rust suites remain decision-invalid in macro lane because their trusted path is the correctness lane.
`pr-macro` currently resolves to `dataset_id=medium_selective`, `warmup=2`, `iters=15`, `prewarm_iters=1`, `compare_runs=7`, `measure_order=alternate`, `timing_phase=execute`, `aggregation=median`, `spread_metric=iqr_ms`, and `sub_ms_policy=micro_only`.
Within `scan`, the manifest-backed authoritative macro set is `scan_full_narrow`, `scan_projection_region`, and `scan_filter_flag`. `scan_pruning_hit` is microbench-only, and `scan_pruning_miss` remains disabled until that case is requalified; do not treat either as normal macro decision evidence.

Freshness rules for trustworthy compare output:

- If `fixture_recipe_hash`, `dataset_fingerprint`, or `compatibility_key` differs, treat the artifact pair as incomparable and rerun with fresh fixtures.
- If a case's `exact_result_hash` or `schema_hash` assertions are stale, refresh them from a trusted validation run before treating new perf artifacts as authoritative.
- `--mode assert` is for semantic validation only, and `bench.sh` requires it to run with `--lane correctness`. `compare_branch.sh` intentionally rejects assert-only artifacts because they cannot support perf conclusions.

## Reading the Report

The comparison report groups benchmark cases into four sections:

| Section                | Meaning                                                                                                       |
| ---------------------- | ------------------------------------------------------------------------------------------------------------- |
| **Regressions**        | Cases where the candidate is slower than the base beyond the noise threshold. Investigate before merging.     |
| **Improvements**       | Cases where the candidate is faster than the base beyond the noise threshold.                                 |
| **Stable**             | Cases where performance is within the noise threshold. No action needed.                                      |
| **Out of Scope (micro only)** | Comparable decision-mode rows excluded from macro evidence because both sides are below the configured sub-millisecond threshold. |
| **Comparison aborted / invalid** | Exploratory mode reports invalid workloads here without discarding the rest of the artifact. Decision mode still fails closed instead of producing a perf claim. |

Key metrics to look at:

- **Relative change (%)** -- how much faster or slower the candidate is compared to the base.
- **cv_pct** -- coefficient of variation as a percentage. Below 5% is good. Above 10% means the measurement is noisy and you should increase `--compare-runs` or `--iters`.
- **median_ms** -- the representative timing for each case.
- **iqr_ms** -- the interquartile spread computed from per-run medians in decision-mode evidence. This is the fixed spread metric used by the `pr-macro` methodology profile.
- **decision_scope** -- whether the row contributes to macro evidence. `macro` rows count toward regressions/improvements/stable summaries; comparable `micro_only` rows are rendered under **Out of Scope (micro only)** because both sides are below the sub-millisecond threshold. Rows that are still `inconclusive` or `expected_failure` remain in **Needs Attention** and still honor `--fail-on`.
- **scope_reason** -- why a row is out of scope. `sub_ms_threshold` means the `pr-macro` contract excluded the row from macro evidence.

Automation-friendly compare artifacts live next to the aggregated run JSON:

| Artifact | Purpose |
| --- | --- |
| `summary.md` | Markdown report for PR comments or artifact upload |
| `comparison.json` | Versioned machine-readable compare payload with `schema_version`, `metadata`, `summary`, and `rows`, including per-row scope and spread fields |
| `hash-policy.txt` | Hash/schema compatibility report for the aggregated payload pair across all observed sample hashes |
| `manifest.json` | Pointer file with suite, SHAs, compare settings, methodology metadata, and artifact paths |

For the complete list of metrics that may appear in the report, see [Reference](reference.md#metrics-reference).

## Next Steps

- **Track trends over time** -- see [Longitudinal Benchmarking](longitudinal.md) for regression detection across many revisions.
- **Run on dedicated hardware** -- see [Cloud Runner](cloud-runner.md) for noise-isolated benchmarks on hardened infrastructure.
- **Understand the result format** -- see [Reference](reference.md#result-schema-v5) for the complete schema v5 field listing.
