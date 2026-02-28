# delta-rs-benchmarking

Repo-first benchmark harness for `delta-rs` with manual branch comparison.

## Quickstart

```bash
./scripts/prepare_delta_rs.sh
./scripts/sync_harness_to_delta_rs.sh
./scripts/bench.sh data --scale sf1 --seed 42
./scripts/bench.sh run --suite all --scale sf1 --warmup 1 --iters 5 --label local
./scripts/bench.sh run --suite tpcds --scale sf1 --warmup 1 --iters 1 --label local
```

Results are written to `results/<label>/<suite>.json`.

## TPC-DS suite (phase 1)

- Target name: `tpcds`
- Query cases: `tpcds_q03`, `tpcds_q07`, `tpcds_q64`, `tpcds_q72`
- Skip policy: `q72` is intentionally included but emitted as `skipped` until DataFusion issue-tracker parity is resolved for that query.
- Local fixture contract: `fixtures/<scale>/tpcds/<table>/` (Delta tables)
- Non-local fixture contract: `<table_root>/<scale>/tpcds/<table>/`

`bench.sh` and `delta-bench run --target tpcds` consume pre-generated TPC-DS table fixtures; they do not generate TPC-DS data in this repository.

## Compare two branches

```bash
./scripts/compare_branch.sh main fix/python-dv-bug-4235 all
```

This will:
1. Update `.delta-rs-under-test`
2. Sync this repo's harness into `delta-rs` workspace
3. Benchmark base branch in that workspace
4. Benchmark candidate branch in that workspace
5. Print a markdown comparison table

Tuning options:
- `BENCH_TIMEOUT_SECONDS` (default `3600`) to cap each `bench.sh` step runtime.
- `BENCH_RETRY_ATTEMPTS` (default `2`) for transient failures.
- `BENCH_RETRY_DELAY_SECONDS` (default `5`) between retry attempts.
- `--noise-threshold` to tune compare sensitivity while keeping benchmark output advisory.
- `cd python && python3 -m delta_bench_compare.compare ... --include-metrics` to append per-case metric columns in compare output.
- `--storage-backend` and repeatable `--storage-option KEY=VALUE` to run fixture generation + suite execution against object storage.

## Result metrics

Each sample writes normalized metrics under `cases[].samples[].metrics`.

Always-present fields:
- `rows_processed`
- `bytes_processed`
- `operations`
- `table_version`

Optional scan/rewrite fields (suite-dependent):
- `files_scanned`
- `files_pruned`
- `bytes_scanned`
- `scan_time_ms`
- `rewrite_time_ms`

## Wave 1 cases

New pruning/compaction cases now included in existing suites:
- `read_partition_pruning_hit`
- `read_partition_pruning_miss`
- `merge_partition_localized_1pct`
- `optimize_noop_already_compact`
- `optimize_heavy_compaction`

Wave 1 fixture generation now also materializes:
- `read_partitioned_delta`
- `merge_partitioned_target_delta`
- `optimize_compacted_delta`

## Cloud/object-store mode

Local remains the default backend. To run fixture-backed suites against object storage:

```bash
./scripts/bench.sh data \
  --scale sf1 \
  --seed 42 \
  --storage-backend s3 \
  --storage-option table_root=s3://bench-bucket/delta-bench \
  --storage-option AWS_REGION=us-east-1

./scripts/bench.sh run \
  --suite optimize_vacuum \
  --scale sf1 \
  --warmup 1 \
  --iters 2 \
  --label wave2-s3 \
  --storage-backend s3 \
  --storage-option table_root=s3://bench-bucket/delta-bench \
  --storage-option AWS_REGION=us-east-1
```

Notes:
- For non-local backends, `--storage-option table_root=...` is required.
- Local fixture cache (`fixtures/<scale>/rows.jsonl` + `manifest.json`) is unchanged.
- `write` suite keeps local temp-table behavior in cloud mode.

Security/fidelity options:
- `--remote-runner <ssh-host>` to execute the full workflow on a dedicated Vultr runner.
- `--remote-root <path>` to set the benchmark repo root on the remote runner.
- `--enforce-run-mode` to require benchmark run mode marker (`/etc/delta-bench/security-mode`).
- `--require-no-public-ipv4` to fail if the runner has any public IPv4 address on interfaces.
- `--require-egress-policy` to require active `nftables` hash match against `DELTA_BENCH_EGRESS_POLICY_SHA256`.

Example:

```bash
./scripts/compare_branch.sh \
  --remote-runner bench-runner-01 \
  --remote-root /opt/delta-rs-benchmarking \
  --storage-backend s3 \
  --storage-option table_root=s3://bench-bucket/delta-bench \
  --storage-option AWS_REGION=us-east-1 \
  main feature/merge-opt optimize_vacuum

./scripts/compare_branch.sh \
  --remote-runner bench-runner-01 \
  --remote-root /opt/delta-rs-benchmarking \
  --enforce-run-mode \
  --require-no-public-ipv4 \
  --require-egress-policy \
  main feature/merge-opt all
```

Workflow mode storage configuration:
- Optional repository variable `BENCH_STORAGE_BACKEND` (`s3`, `gcs`, or `azure`)
- Optional multi-line repository variable `BENCH_STORAGE_OPTIONS` (one `KEY=VALUE` per line; for example `table_root=...`, `AWS_REGION=...`)
- Benchmark workflow comments are advisory and do not gate PR merge; CI gating remains opt-in via `--ci`.

## Security operations

Switch benchmark isolation mode on the runner:

```bash
sudo ./scripts/security_mode.sh run-mode
sudo ./scripts/security_mode.sh maintenance-mode
```

Run security preflight checks directly:

```bash
export DELTA_BENCH_EGRESS_POLICY_SHA256="$(nft list ruleset | sha256sum | awk '{print $1}')"
./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy
```

Provisioning helper entrypoint for Terraform:

```bash
./scripts/provision_vultr.sh plan
./scripts/provision_vultr.sh apply
./scripts/provision_vultr.sh rotate-key
```

Destructive provisioning commands (`rotate-runner`, `destroy`) require CI context by default, two distinct approver IDs, and an immutable approval evidence file.

## Longitudinal Benchmarking

This repo now supports DataFusion-style over-time benchmarking for `delta-rs` revisions while keeping execution logic local to benchmark infrastructure.

### Pipeline stages

1. Revision selection manifest
2. Per-revision artifact builds
3. Resumable suite/scale matrix execution
4. Append-safe normalized longitudinal store
5. Markdown + HTML trend reports
6. Optional retention pruning for artifact/store growth

### CLI usage

Use the shell wrapper:

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

Or execute end-to-end:

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
  --suite merge_dml \
  --suite metadata \
  --suite optimize_vacuum \
  --scale sf1
```

### Revision selection strategies

- `release-tags`: selects semantic version tags (`vX.Y.Z` by default)
- `date-window`: selects all commits in an inclusive date range
- `one-per-day`: selects one (latest) commit per day in an inclusive date range

### Parallel and load guard controls

- `run-matrix --max-parallel N`: cap concurrent suite/scale cells (default `1`)
- `run-matrix --max-load-per-cpu X`: pause scheduling when `loadavg_1m / cpu_count > X`
- `run-matrix --load-check-interval-seconds N`: poll interval while waiting for load guard

### Statistical significance controls

- `report --significance-method none|mann-whitney` (default `none`)
- `report --significance-alpha 0.05` controls p-value cutoff for significance labeling
- Threshold deltas still apply; significance adds an extra confidence signal to regression highlights

### Retention pruning controls

- `prune` is safe by default (dry-run)
- pass `--apply` to perform deletion/rewrite actions
- artifacts can be pruned by age (`--max-artifact-age-days`) and/or count (`--max-artifacts`)
- store can be pruned by run age (`--max-run-age-days`) and/or count (`--max-runs`)

### Directory layout

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

Operational details and failure recovery are documented in [`docs/longitudinal-runbook.md`](docs/longitudinal-runbook.md).

## CLI reference

```bash
cargo run -p delta-bench -- --help
cargo run -p delta-bench -- list all
cargo run -p delta-bench -- data --scale sf1 --seed 42
cargo run -p delta-bench -- run --target all --scale sf1 --warmup 1 --iterations 5 --storage-backend local
cargo run -p delta-bench -- doctor
```

Run against the managed delta-rs checkout workspace:

```bash
DELTA_BENCH_EXEC_ROOT="$(pwd)/.delta-rs-under-test" \
DELTA_RS_DIR="$(pwd)/.delta-rs-under-test" \
./scripts/bench.sh doctor
```

## Current scope

- Implemented suites: `read_scan`, `write`, `merge_dml`, `metadata`, `optimize_vacuum`, `tpcds` (phase 1; `q72` skipped)
- Implemented: Wave 1 pruning/compaction benchmark expansion across read, merge, and optimize suites
- Implemented: suites execute real `deltalake-core` operations (read provider scans, write builders, merge builders, metadata loads)
- Implemented: deterministic fixture generation + result schema v1
- Implemented: manual comparison workflow (`scripts/compare_branch.sh` + Python compare) in advisory mode (non-gating), with optional CI gating mode
- Implemented: Option B PR benchmark workflow (`.github/workflows/benchmark.yml`) with issue-comment trigger, role-based authorization, and serialized execution
- Limitation: workflow mode currently relies on `compare_branch.sh` branch names being available in the upstream `delta-rs` checkout; fork PR heads may require manual handling
