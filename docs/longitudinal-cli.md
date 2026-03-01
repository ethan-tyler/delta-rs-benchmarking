# Longitudinal CLI Guide

This guide covers the command-line workflow for over-time benchmarking across `delta-rs` revisions.

Operational recovery and nightly-job behavior are documented in [longitudinal-runbook.md](longitudinal-runbook.md).

## Pipeline stages

1. Select revisions.
2. Build per-revision artifacts.
3. Run resumable suite/scale matrix.
4. Ingest normalized rows.
5. Generate markdown and HTML reports.
6. Optionally prune old artifacts and runs.

## Step-by-step commands

```bash
./scripts/longitudinal_bench.sh select-revisions \
  --repository .delta-rs-under-test \
  --strategy one-per-day \
  --start-date 2026-01-01 \
  --end-date 2026-01-31 \
  --output longitudinal/manifests/jan.json

./scripts/longitudinal_bench.sh build-artifacts \
  --manifest longitudinal/manifests/jan.json \
  --artifacts-dir longitudinal/artifacts

./scripts/longitudinal_bench.sh run-matrix \
  --manifest longitudinal/manifests/jan.json \
  --artifacts-dir longitudinal/artifacts \
  --state-path longitudinal/state/matrix.json \
  --results-dir results \
  --max-parallel 2 \
  --max-load-per-cpu 0.75 \
  --load-check-interval-seconds 10 \
  --suite read_scan \
  --suite metadata \
  --scale sf1 \
  --timeout-seconds 3600 \
  --max-retries 2

./scripts/longitudinal_bench.sh ingest-results \
  --manifest longitudinal/manifests/jan.json \
  --state-path longitudinal/state/matrix.json \
  --results-dir results \
  --store-dir longitudinal/store

./scripts/longitudinal_bench.sh report \
  --store-dir longitudinal/store \
  --markdown-path longitudinal/reports/summary.md \
  --html-path longitudinal/reports/trends.html \
  --baseline-window 7 \
  --regression-threshold 0.05 \
  --significance-method mann-whitney \
  --significance-alpha 0.05

./scripts/longitudinal_bench.sh prune \
  --artifacts-dir longitudinal/artifacts \
  --store-dir longitudinal/store \
  --max-artifact-age-days 30 \
  --max-artifacts 120 \
  --max-run-age-days 60 \
  --max-runs 200
```

## End-to-end orchestration

```bash
./scripts/longitudinal_bench.sh orchestrate \
  --manifest longitudinal/manifests/jan.json \
  --artifacts-dir longitudinal/artifacts \
  --results-dir results \
  --state-path longitudinal/state/matrix.json \
  --store-dir longitudinal/store \
  --markdown-path longitudinal/reports/summary.md \
  --html-path longitudinal/reports/trends.html \
  --suite read_scan \
  --suite write \
  --suite delete_update_dml \
  --suite merge_dml \
  --suite metadata \
  --suite optimize_vacuum \
  --scale sf1
```

## Revision selection strategies

- `release-tags`: semantic version tags (`vX.Y.Z` by default)
- `date-window`: all commits in an inclusive date range
- `one-per-day`: latest commit per day in an inclusive date range

## Parallel and load-guard controls

- `run-matrix --max-parallel N`: concurrent suite/scale cells (default `1`)
- `run-matrix --max-load-per-cpu X`: pause scheduling when `loadavg_1m / cpu_count > X`
- `run-matrix --load-check-interval-seconds N`: polling interval while load guard is active

Use conservative settings first, then raise parallelism only after confirming low cross-run interference.

## Statistical significance controls

- `report --significance-method none|mann-whitney` (default `none`)
- `report --significance-alpha 0.05` sets the p-value cutoff for significance labeling

Threshold deltas still apply; significance adds confidence signals for regression highlights.

## Retention pruning controls

- `prune` is safe by default (dry-run)
- pass `--apply` to perform deletion/rewrite actions
- prune artifacts by age (`--max-artifact-age-days`) and/or count (`--max-artifacts`)
- prune store runs by age (`--max-run-age-days`) and/or count (`--max-runs`)

## Directory layout

```text
longitudinal/
  manifests/           # revision manifests
  artifacts/<sha>/     # built delta-bench binary + metadata.json per revision
  state/matrix.json    # resumable matrix state (attempts/status/reasons)
  store/rows.jsonl     # normalized append-safe time-series rows
  store/index.json     # ingested run-id dedupe index
  reports/summary.md   # CI-friendly markdown summary
  reports/trends.html  # HTML trend report with inline charts
```
