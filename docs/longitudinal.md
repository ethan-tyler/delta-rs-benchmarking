# Longitudinal Benchmarking

This guide covers tracking delta-rs performance across many revisions over time, from running your first longitudinal pipeline to operating automated nightly and release workflows.

## Table of Contents

- [When to Use Longitudinal Benchmarking](#when-to-use-longitudinal-benchmarking)
- [Prerequisites](#prerequisites)
- [Pipeline Stages Overview](#pipeline-stages-overview)
- [Quick Start: Your First Longitudinal Run](#quick-start-your-first-longitudinal-run)
- [One-Command Orchestration](#one-command-orchestration)
- [Revision Selection Strategies](#revision-selection-strategies)
- [Automated Workflows](#automated-workflows)
- [Advanced Controls](#advanced-controls)
- [Failure Recovery](#failure-recovery)
- [Directory Layout](#directory-layout)
- [Safety Notes](#safety-notes)

## When to Use Longitudinal Benchmarking

Branch comparison gives you a two-point snapshot: is revision B faster or slower than revision A? Longitudinal benchmarking gives you a time series across many revisions, letting you catch gradual regressions, validate that a performance fix holds over subsequent commits, and build release baselines.

Use it for nightly regression detection, release history analysis, or any situation where you need to see how performance trends over a range of commits or tags.

## Prerequisites

- A prepared delta-rs checkout (`./scripts/prepare_delta_rs.sh`)
- Python 3 with the `delta_bench_longitudinal` package available
- A revision manifest (created by the `select-revisions` stage or committed under `longitudinal/manifests/`)

## Pipeline Stages Overview

The longitudinal pipeline runs in six stages. Each stage is idempotent and can be rerun safely.

| Stage | Command | Purpose |
|---|---|---|
| 1. Select | `select-revisions` | Pick which commits or tags to benchmark |
| 2. Build | `build-artifacts` | Compile `delta-bench` for each revision |
| 3. Run | `run-matrix` | Execute benchmark suites across the revision/scale matrix |
| 4. Ingest | `ingest-results` | Normalize results into an append-safe time-series store |
| 5. Report | `report` | Generate markdown and HTML trend reports |
| 6. Prune | `prune` | Apply retention policies to old artifacts and data |

## Quick Start: Your First Longitudinal Run

### Select revisions

Choose which commits to benchmark. This example picks one commit per day from January 2026:

```bash
./scripts/longitudinal_bench.sh select-revisions \
  --repository .delta-rs-under-test \
  --strategy one-per-day \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --output longitudinal/manifests/jan.json
```

The output manifest is a JSON file listing the selected revision SHAs with metadata.

### Build artifacts

Compile the benchmark binary for each revision in the manifest:

```bash
./scripts/longitudinal_bench.sh build-artifacts \
  --manifest longitudinal/manifests/jan.json \
  --artifacts-dir longitudinal/artifacts
```

Already-built revisions are skipped automatically. Only new or previously failed revisions are built.

### Run the benchmark matrix

Execute benchmark suites across all revisions:

```bash
./scripts/longitudinal_bench.sh run-matrix \
  --manifest longitudinal/manifests/jan.json \
  --artifacts-dir longitudinal/artifacts \
  --state-path longitudinal/state/matrix-state.json \
  --results-dir results \
  --max-parallel 2 \
  --max-load-per-cpu 0.75 \
  --load-check-interval-seconds 10 \
  --suite scan \
  --suite metadata \
  --scale sf1 \
  --timeout-seconds 3600 \
  --max-retries 2
```

The `--state-path` file tracks which (revision, suite, scale) cells have completed, failed, or are pending. If the run is interrupted, rerunning this command resumes from where it left off.

The matrix state file also stores a fingerprint of the matrix configuration. Reuse the same `--state-path` only when `--suite`, `--scale`, warmup, iterations, fixtures directory, results directory, and label prefix are unchanged.

### Ingest results

Normalize the raw JSON results into a SQLite-backed time-series store:

```bash
./scripts/longitudinal_bench.sh ingest-results \
  --manifest longitudinal/manifests/jan.json \
  --state-path longitudinal/state/matrix-state.json \
  --results-dir results \
  --store-dir longitudinal/store
```

Duplicate ingests are deduplicated by run-id in the SQLite store, so this is safe to rerun.

### Generate reports

Create markdown and HTML trend reports from the ingested data:

```bash
./scripts/longitudinal_bench.sh report \
  --store-dir longitudinal/store \
  --markdown-path longitudinal/reports/summary.md \
  --html-path longitudinal/reports/trends.html \
  --baseline-window 7 \
  --regression-threshold 0.05 \
  --significance-method mann-whitney \
  --significance-alpha 0.05
```

### Prune old data (optional)

Apply retention policies to keep artifact and result storage bounded. Prune is **dry-run by default**; add `--apply` to execute deletions.

```bash
./scripts/longitudinal_bench.sh prune \
  --artifacts-dir longitudinal/artifacts \
  --store-dir longitudinal/store \
  --max-artifact-age-days 30 \
  --max-artifacts 120 \
  --max-run-age-days 60 \
  --max-runs 200
```

## One-Command Orchestration

If you want to run the full pipeline (build, run, ingest, report) in a single command:

```bash
./scripts/longitudinal_bench.sh orchestrate \
  --manifest longitudinal/manifests/jan.json \
  --artifacts-dir longitudinal/artifacts \
  --results-dir results \
  --state-path longitudinal/state/matrix-state.json \
  --store-dir longitudinal/store \
  --markdown-path longitudinal/reports/summary.md \
  --html-path longitudinal/reports/trends.html \
  --suite scan \
  --suite write \
  --suite delete_update \
  --suite merge \
  --suite metadata \
  --suite optimize_vacuum \
  --scale sf1
```

Use `orchestrate` when you want a hands-off run. Use the individual stages when you need to debug, retry, or customize a specific step.

## Revision Selection Strategies

The `select-revisions` stage supports three strategies for choosing which commits to benchmark:

| Strategy | Description | Use when |
|---|---|---|
| `one-per-day` | Latest commit per day in an inclusive date range | Nightly trend tracking |
| `date-window` | All commits in an inclusive date range | Dense analysis of a specific period |
| `release-tags` | Semantic version tags matching a pattern | Release history baselines |

### Release tag patterns

For Rust releases:

```bash
--release-tag-pattern '^rust-v\d+\.\d+\.\d+([+-].+)?$'
```

For Python releases:

```bash
--release-tag-pattern '^python-v\d+\.\d+\.\d+([+-].+)?$'
```

### Committed release manifests

The repository maintains pre-built release manifests at:

- `longitudinal/manifests/release-history-rust.json`
- `longitudinal/manifests/release-history-python.json`

To refresh them when new tags are published:

```bash
./scripts/update_release_history_manifests.sh
```

## Automated Workflows

### Nightly commit-window workflow

Workflow file: `.github/workflows/longitudinal-nightly.yml`

Runs automatically at 03:00 UTC daily, or manually via `workflow_dispatch` with optional `lookback_days` and `baseline_window` inputs.

The nightly workflow executes all six pipeline stages: select revisions (using `one-per-day` over the lookback window), build missing artifacts, run the matrix with retry and resume, ingest results, generate reports, apply retention, and upload `longitudinal/` as CI artifacts.

### Release-tag history workflow

Workflow file: `.github/workflows/longitudinal-release-history.yml`

Runs weekly on Monday at 04:30 UTC, or manually via `workflow_dispatch`.

This workflow processes Rust and Python release lanes independently. Each lane loads its committed release manifest, builds any missing artifacts, runs the matrix, ingests results, generates lane-specific reports, and applies retention. Results are uploaded under `longitudinal/releases/<lane>/`.

To include newly published tags, refresh the committed manifests:

```bash
./scripts/update_release_history_manifests.sh
```

## Advanced Controls

### Parallel and load guards

Control resource usage during matrix execution to avoid interference with other processes:

| Flag | Description |
|---|---|
| `--max-parallel N` | Maximum concurrent revision benchmarks |
| `--max-load-per-cpu X` | CPU load ceiling (e.g., `0.75`) before pausing new work |
| `--load-check-interval-seconds N` | How often to re-check system load |

Start conservatively and increase only after confirming low interference.

### Significance controls

Add statistical confidence labels to trend reports:

| Flag | Default | Description |
|---|---|---|
| `--significance-method` | `none` | Statistical test: `none` or `mann-whitney` |
| `--significance-alpha` | `0.05` | Significance level for the test |

Threshold-based change classification still applies; significance adds confidence labels on top.

### Retention controls

Keep storage bounded with the `prune` stage:

| Flag | Description |
|---|---|
| `--max-artifact-age-days <days>` | Remove artifacts older than this |
| `--max-artifacts <count>` | Keep at most this many artifacts |
| `--max-run-age-days <days>` | Remove run data older than this |
| `--max-runs <count>` | Keep at most this many runs |
| `--apply` | Execute deletions (dry-run without this flag) |

## Failure Recovery

Every stage is idempotent. Successful work is skipped on rerun, and failed work is retried automatically. The general recovery pattern is: diagnose the failure, fix the root cause, and rerun the same command.

### Build failures

**Symptoms:** Revision metadata shows `status: failure`, or `build-artifacts` only partially completes.

**Diagnose:** Inspect the metadata file for the failing revision:

```bash
cat longitudinal/artifacts/<revision>/metadata.json
```

Common causes include toolchain drift, checkout issues, or harness sync problems.

**Recover:** Fix the root cause and rerun `build-artifacts` with the same manifest. Successful builds are skipped; failed builds are retried.

```bash
./scripts/longitudinal_bench.sh build-artifacts \
  --manifest longitudinal/manifests/<manifest>.json \
  --artifacts-dir longitudinal/artifacts
```

### Matrix failures or timeouts

**Symptoms:** Matrix state has entries with `status: failure`, or `failure_reason` shows timeout or command errors.

**Diagnose:** Inspect the matrix state for failing cells:

```bash
cat longitudinal/state/matrix-state.json
```

Look at the `(revision, suite, scale)` keys with failure status and their `failure_reason`.

**Recover:** Fix the root cause (increase timeout for legitimately slow cases, or fix the underlying issue) and rerun `run-matrix` with the same manifest and state path. Successful cells are skipped; failed cells resume with bounded retry.

If you intentionally change matrix configuration such as suites, scales, warmup, iterations, fixtures directory, results directory, or label prefix, start with a new `--state-path` instead of reusing the old file. The runner now rejects configuration-mismatched state files to prevent partial resumes from mixing incompatible runs.

```bash
./scripts/longitudinal_bench.sh run-matrix \
  --manifest longitudinal/manifests/<manifest>.json \
  --artifacts-dir longitudinal/artifacts \
  --state-path longitudinal/state/matrix-state.json \
  --results-dir results \
  --suite scan --suite write --suite delete_update --suite merge --suite metadata --suite optimize_vacuum \
  --scale sf1 \
  --timeout-seconds 3600 \
  --max-retries 2
```

### Ingest or report gaps

**Symptoms:** Expected rows are missing from `store.sqlite3`, or the report is missing expected revisions.

**Diagnose:** Verify that the expected result files exist:

```bash
ls results/<label-prefix>-<revision>/<suite>.json
```

**Recover:** Rerun `ingest-results` (duplicates are deduplicated by run-id) and then `report`:

```bash
./scripts/longitudinal_bench.sh ingest-results \
  --manifest longitudinal/manifests/<manifest>.json \
  --state-path longitudinal/state/matrix-state.json \
  --results-dir results \
  --store-dir longitudinal/store

./scripts/longitudinal_bench.sh report \
  --store-dir longitudinal/store \
  --markdown-path longitudinal/reports/summary.md \
  --html-path longitudinal/reports/trends.html
```

## Directory Layout

```text
longitudinal/
  manifests/                  # revision manifests (JSON)
  artifacts/<sha>/            # built delta-bench binary + metadata.json per revision
  state/matrix-state.json     # resumable matrix status, config fingerprint, attempts, and failure reasons
  store/store.sqlite3         # normalized time-series runs + case rows
  reports/summary.md          # CI-friendly markdown summary
  reports/trends.html         # HTML trend report with inline charts
  releases/<lane>/            # release-tag workflow artifacts (per lane)
```

## Safety Notes

- All inputs are validated in CLI and runner paths.
- Matrix execution uses subprocess argument arrays (no shell interpolation).
- Retention deletion is gated by explicit `--apply`.
- No destructive git reset/clean actions are performed.
- Transient failures are retried with bounded attempts only.
