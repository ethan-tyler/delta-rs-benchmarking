# Longitudinal Benchmarking Runbook

## Scope

This runbook covers execution-plane operation of nightly longitudinal benchmarks in this repo:

- selecting revisions
- building revision artifacts
- running benchmark matrix with resume/idempotency
- ingesting normalized rows
- generating trend reports

Control-plane authorization, queueing, and PR-bot orchestration are intentionally out of scope for this repository.

## Nightly workflow

Workflow file: `.github/workflows/longitudinal-nightly.yml`

Trigger modes:

- scheduled: nightly at `03:00 UTC`
- manual: `workflow_dispatch` with optional `lookback_days` and `baseline_window`

Nightly stages:

1. `select-revisions` (`one-per-day`, date window from `lookback_days`)
2. `build-artifacts` (build missing revisions only, metadata persisted per revision)
3. `run-matrix` (suite/scale matrix, retry + timeout + resume state)
4. `ingest-results` (append-safe normalized rows with run-id dedupe)
5. `report` (markdown summary + HTML trends, optional significance checks)
6. `prune` (retention policies for artifacts + store growth)
7. upload artifacts (`longitudinal/` subtree)

## State and artifacts

- `longitudinal/manifests/*.json`: revision sets
- `longitudinal/artifacts/<revision>/metadata.json`: build metadata, status, toolchain, timestamps
- `longitudinal/state/matrix-state.json`: per-case status/attempt/failure reason
- `longitudinal/store/rows.jsonl`: normalized time-series rows
- `longitudinal/store/index.json`: ingested run-id dedupe index
- `longitudinal/reports/summary.md`: CI markdown summary
- `longitudinal/reports/trends.html`: HTML trend report

## Performance guardrails

- `run-matrix --max-parallel` controls concurrent benchmark cells
- `run-matrix --max-load-per-cpu` blocks new dispatches when host load is above threshold
- `run-matrix --load-check-interval-seconds` controls load guard polling interval

Use conservative defaults first, then increase parallelism only after validating low benchmark interference.

## Failure recovery

### Build failures

Symptoms:

- revision metadata status is `failure`
- workflow step `build-artifacts` fails or only partially builds

Actions:

1. Inspect metadata file under `longitudinal/artifacts/<revision>/metadata.json`.
2. Fix root cause (toolchain drift, checkout issue, harness sync issue).
3. Re-run `build-artifacts` for the same manifest.

Idempotency behavior:

- successful builds are skipped automatically
- failed builds are retried on rerun

### Matrix failures/timeouts

Symptoms:

- `matrix-state.json` contains case entries with `status: failure`
- `failure_reason` includes timeout or command error

Actions:

1. Inspect `longitudinal/state/matrix-state.json` for failing `(revision,suite,scale)` keys.
2. Re-run `run-matrix` with the same manifest/state path after remediation.
3. Increase timeout only when the benchmark case is legitimately long-running.

Idempotency behavior:

- already successful cases are skipped
- failed cases resume with bounded retry policy

### Ingest/report gaps

Symptoms:

- no new rows in `rows.jsonl`
- summary/report missing expected revisions

Actions:

1. Verify expected result JSON exists at `results/<label-prefix>-<revision>/<suite>.json`.
2. Re-run `ingest-results` with the same manifest/state path.
3. Re-run `report`.

Idempotency behavior:

- duplicate ingests are deduped by run-id index; re-ingest is safe

## Manual recovery commands

```bash
./scripts/longitudinal_bench.sh build-artifacts \
  --manifest longitudinal/manifests/nightly-manifest.json \
  --artifacts-dir longitudinal/artifacts

./scripts/longitudinal_bench.sh run-matrix \
  --manifest longitudinal/manifests/nightly-manifest.json \
  --artifacts-dir longitudinal/artifacts \
  --state-path longitudinal/state/matrix-state.json \
  --results-dir results \
  --suite read_scan --suite write --suite merge_dml --suite metadata --suite optimize_vacuum \
  --scale sf1 \
  --timeout-seconds 3600 \
  --max-retries 2

./scripts/longitudinal_bench.sh ingest-results \
  --manifest longitudinal/manifests/nightly-manifest.json \
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
  --max-runs 200 \
  --apply
```

## Safety notes

- Inputs are validated in CLI and runner paths (strategy/date/token bounds).
- Matrix execution uses subprocess argument arrays (no shell interpolation).
- Retention deletion is gated by explicit `--apply`.
- No destructive git reset/clean actions are performed.
- Transient failures are retried with bounded attempts only.
