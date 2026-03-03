# Longitudinal Benchmarking Runbook

Operations guide for scheduled longitudinal workflows and recovery.
Use [longitudinal-cli.md](longitudinal-cli.md) for command-by-command normal execution.

## Scope

This runbook covers execution-plane operation in this repository:

- selecting revisions
- building artifacts
- executing resumable matrix runs
- ingesting normalized rows
- generating trend reports
- applying retention policies

Out of scope: control-plane authorization, queueing, and PR-bot orchestration.

## Nightly and Release Workflows

### Nightly commit-window workflow

Workflow file: `.github/workflows/longitudinal-nightly.yml`

Trigger modes:

- scheduled: nightly at `03:00 UTC`
- manual: `workflow_dispatch` with optional `lookback_days` and `baseline_window`

Nightly stages:

1. `select-revisions` (`one-per-day`, date window from `lookback_days`)
2. `build-artifacts` (missing revisions only)
3. `run-matrix` (retry + timeout + resume state)
4. `ingest-results` (append-safe rows with run-id dedupe)
5. `report` (markdown summary + HTML trends)
6. `prune` (retention policies)
7. upload `longitudinal/` artifacts

### Release-tag history workflow

Workflow file: `.github/workflows/longitudinal-release-history.yml`

Trigger modes:

- scheduled: weekly at `04:30 UTC` on Monday
- manual: `workflow_dispatch` with optional `baseline_window`

Release lane stages (`rust` and `python` lanes run independently):

1. load committed release manifest
2. `build-artifacts` (missing revisions only)
3. `run-matrix` (retry + timeout + resume state)
4. `ingest-results` (append-safe rows with run-id dedupe)
5. `report` (release-history baseline)
6. `prune` (lane retention)
7. upload `longitudinal/releases/<lane>/` artifacts

Refresh committed release manifests when new tags must be included:

```bash
./scripts/update_release_history_manifests.sh
```

## State and Artifacts Reference

- `longitudinal/manifests/*.json`: revision sets
- `longitudinal/artifacts/<revision>/metadata.json`: build metadata/status/toolchain/timestamps
- `longitudinal/state/matrix-state.json`: nightly per-case status/attempt/failure reason
- `longitudinal/releases/<lane>/state/matrix-state.json`: release-history lane state
- `longitudinal/store/rows.jsonl`: normalized time-series rows
- `longitudinal/store/index.json`: run-id dedupe index
- `longitudinal/reports/summary.md`: CI markdown summary
- `longitudinal/reports/trends.html`: HTML trend report

## Performance Guardrails

- `run-matrix --max-parallel`
- `run-matrix --max-load-per-cpu`
- `run-matrix --load-check-interval-seconds`

Use conservative defaults first, then increase parallelism only after validating low interference.

## Failure Recovery Playbooks

### Build failures

Symptoms:

- revision metadata has `status: failure`
- `build-artifacts` fails or only partially builds

Actions:

1. inspect `longitudinal/artifacts/<revision>/metadata.json`
2. fix root cause (toolchain drift, checkout issue, harness sync issue)
3. rerun `build-artifacts` for the same manifest

Idempotency behavior:

- successful builds are skipped
- failed builds are retried

### Matrix failures or timeouts

Symptoms:

- matrix state has entries with `status: failure`
- `failure_reason` shows timeout or command errors

Actions:

1. inspect `longitudinal/state/matrix-state.json` for failing `(revision,suite,scale)` keys
2. rerun `run-matrix` with same manifest and state path after remediation
3. increase timeout only for legitimately long-running benchmark cases

Idempotency behavior:

- successful cells are skipped
- failed cells resume with bounded retry policy

### Ingest or report gaps

Symptoms:

- expected rows missing from `rows.jsonl`
- report output missing expected revisions

Actions:

1. verify expected result JSON exists at `results/<label-prefix>-<revision>/<suite>.json`
2. rerun `ingest-results` with same manifest and state path
3. rerun `report`

Idempotency behavior:

- duplicate ingests are deduped by run-id index

## Manual Recovery Commands

```bash
./scripts/longitudinal_bench.sh build-artifacts \
  --manifest longitudinal/manifests/nightly-manifest.json \
  --artifacts-dir longitudinal/artifacts

./scripts/longitudinal_bench.sh run-matrix \
  --manifest longitudinal/manifests/nightly-manifest.json \
  --artifacts-dir longitudinal/artifacts \
  --state-path longitudinal/state/matrix-state.json \
  --results-dir results \
  --suite scan --suite write --suite delete_update --suite merge --suite metadata --suite optimize_vacuum \
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

## Safety Notes

- inputs are validated in CLI and runner paths
- matrix execution uses subprocess argument arrays (no shell interpolation)
- retention deletion is gated by explicit `--apply`
- no destructive git reset/clean actions are performed
- transient failures are retried with bounded attempts only

## Related Guides

- [Longitudinal CLI Guide](longitudinal-cli.md)
- [User Guide](user-guide.md)
