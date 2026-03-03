# Longitudinal CLI Guide

Command cookbook for running over-time benchmarks across `delta-rs` revisions.
Use [longitudinal-runbook.md](longitudinal-runbook.md) for failure recovery and workflow operations.

## Prerequisites

- prepared repository checkout (`./scripts/prepare_delta_rs.sh`)
- Python 3 environment able to run `python/delta_bench_longitudinal`
- revision manifest path under `longitudinal/manifests/`

Pipeline stages executed by this CLI:

1. select revisions
2. build per-revision artifacts
3. run resumable suite/scale matrix
4. ingest normalized rows (schema v2 only)
5. generate markdown and HTML reports
6. optionally prune old artifacts/runs

## Happy-Path Pipeline

Run the full staged sequence:

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

./scripts/longitudinal_bench.sh ingest-results \
  --manifest longitudinal/manifests/jan.json \
  --state-path longitudinal/state/matrix-state.json \
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

`prune` is dry-run by default; add `--apply` to make retention changes.

## End-to-End Orchestration

Use orchestrate when you want one command for build -> run -> ingest -> report:

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

## Advanced Controls

### Revision selection strategies

- `release-tags`: semantic tags (`vX.Y.Z` by default)
- `date-window`: all commits in inclusive date range
- `one-per-day`: latest commit per day in inclusive range

Release-tag patterns:

- Rust releases: `--release-tag-pattern '^rust-v\d+\.\d+\.\d+([+-].+)?$'`
- Python releases: `--release-tag-pattern '^python-v\d+\.\d+\.\d+([+-].+)?$'`

Repository-managed release manifests:

- `longitudinal/manifests/release-history-rust.json`
- `longitudinal/manifests/release-history-python.json`
- refresh command: `./scripts/update_release_history_manifests.sh`

### Parallel and load guards

- `run-matrix --max-parallel N`
- `run-matrix --max-load-per-cpu X`
- `run-matrix --load-check-interval-seconds N`

Start conservatively, then increase only after confirming low interference.

### Significance controls

- `report --significance-method none|mann-whitney` (default `none`)
- `report --significance-alpha 0.05`

Threshold deltas still apply; significance adds confidence labels.

### Retention controls

- `prune --max-artifact-age-days <days>` and/or `--max-artifacts <count>`
- `prune --max-run-age-days <days>` and/or `--max-runs <count>`
- add `--apply` to execute deletion/rewrite actions

## Directory Layout

```text
longitudinal/
  manifests/                  # revision manifests
  artifacts/<sha>/            # built delta-bench binary + metadata.json
  state/matrix-state.json     # resumable matrix status/attempts/reasons
  store/rows.jsonl            # normalized append-safe time-series rows
  store/index.json            # ingested run-id dedupe index
  reports/summary.md          # CI-friendly markdown summary
  reports/trends.html         # HTML trend report with inline charts
```

## Related Guides

- [Longitudinal Runbook](longitudinal-runbook.md)
- [User Guide](user-guide.md)
- [Architecture](architecture.md)
