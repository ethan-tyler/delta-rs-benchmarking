# Validation

This page records how to validate that the benchmark harness is still producing trustworthy results after workflow, manifest, or timing changes.

## Purpose

The trust contract is split deliberately:

- GitHub-hosted CI validates only smoke and correctness lanes.
- Self-hosted workflows own macro perf, decision compare, Criterion microbench work, and longitudinal ingestion.
- Self-hosted PR decision compare uses the harness-owned `pr-macro` methodology profile. That contract fixes the deterministic `medium_selective` dataset, seven measured compare runs, 15 measured iterations per run, records `iqr_ms` spread, and renders sub-millisecond rows as `micro_only` in `Out of Scope (micro only)` instead of counting them as macro regressions.
- `write_perf` uses the separate `pr-write-perf` operator contract. That path keeps `dataset_id=null`, records `dataset_policy=intrinsic_case_workload`, and stays gated in `bench/evidence/registry.yaml` until same-SHA stability, delayed-canary validation, and runtime signoff are complete.
- `delete_update_perf` now uses two harness-owned perf contracts: the lighter blocking PR profile `pr-delete-update-perf` and the heavier manual/nightly profile `delete-update-perf-high-confidence`. `merge_perf` and `optimize_perf` keep their separate operator contracts (`pr-merge-perf`, `pr-optimize-perf`). These paths fix `dataset_id=medium_selective`, record `dataset_policy=shared_run_scope`, and stay gated until same-SHA stability, delayed-canary validation, runtime signoff, and case-list stability are complete.
- `metadata_perf` uses the separate `pr-metadata-perf` operator contract. That path fixes `dataset_id=many_versions`, records `dataset_policy=shared_run_scope`, and stays gated until same-SHA stability, delayed-canary validation, runtime signoff, and checkpoint-vs-uncheckpointed coverage signoff are complete.
- `tpcds` uses the separate `pr-tpcds` operator contract. That path fixes `dataset_id=tpcds_duckdb`, requires trusted self-hosted runners to pre-provision the DuckDB-backed fixture tree, keeps `tpcds_q72` outside the PR decision surface, and stays gated in `bench/evidence/registry.yaml` until fixture provisioning and validation signoff evidence exists.
- `pr-full-decision` contains only `readiness=ready` suites. Gated perf-owned suites move to `pr-candidate-manual` for manual/candidate reruns so the authoritative PR comment path does not widen ahead of evidence.
- Replay-state investigation uses the dedicated `metadata-replay-criterion` profile, which resolves to `metadata_replay_bench`. That surface is intentionally investigation-grade and must not replace the macro `scan` PR decision contract.
- The committed Criterion profiles `scan-phase-criterion`, `metadata-replay-criterion`, `metadata-log-criterion`, `file-selection-criterion`, and `merge-filter-criterion` are local or trusted self-hosted diagnostics only. They are diagnostic-only, never authoritative PR evidence, never widen PR verdict scope, and never route through `bench/evidence/registry.yaml` packs, `compare_branch.sh`, PR comment automation, or longitudinal ingest.
- `./scripts/validate_perf_harness.sh` reruns the scan trust contract, the `write_perf` promotion gate, the perf-owned DML/maintenance candidate gates, the `metadata_perf` candidate gate, and the dedicated `tpcds` promotion gate when invoked with `--dataset-id tpcds_duckdb`. It writes a rerunnable Markdown summary under `results/validation/<timestamp>/summary.md` by default. When the validation SHA is an immutable ref that `origin` does not advertise, pass `--sha <commit> --fetch-url <trusted-clone-url>` and optionally `--fetch-ref <ref>` (or set `VALIDATION_FETCH_URL` / `VALIDATION_FETCH_REF`) so the validator can prepare `.delta-rs-under-test` at that SHA first, then seed same-SHA compare pinning into `.delta-rs-source` from the prepared execution checkout.

Use this page as the stable operator guide, and use the generated artifact under `results/validation/` as the latest machine-local evidence.

When you need to publish the current operator-facing contract itself, run `./scripts/publish_contract.sh`. It snapshots the current docs, manifests, and wrapper entrypoints into `results/contracts/`.

The authoritative scan decision manifest is intentionally narrower than the raw suite: `scan_pruning_hit` now lives in Criterion microbench coverage, and `scan_pruning_miss` stays disabled from `core_rust.yaml` until it is requalified. Normal PR macro verdicts should therefore focus on `scan_full_narrow`, `scan_projection_region`, and `scan_filter_flag`.

## Verification Matrix

| Surface | Host class | Command or workflow | Trusted outcome |
|---|---|---|---|
| Rust/Python unit and integration tests | GitHub-hosted | `.github/workflows/ci.yml` test job | No regressions in harness logic |
| Smoke validation lane | GitHub-hosted | `.github/workflows/ci.yml` hosted benchmark validation job | All suites stay operational on cheap hosted runners |
| Correctness validation lane | GitHub-hosted | `.github/workflows/ci.yml` hosted benchmark validation job | Semantic/hash-backed assertions hold for trusted correctness suites |
| Branch compare and decision runs | Self-hosted | `benchmark.yml`, `benchmark-prerelease.yml`, `./scripts/compare_branch.sh --methodology-profile pr-macro` | Macro perf claims only on hardened runners, with sub-ms rows rendered as `micro_only` out of scope |
| Replay-state engineering probe | Self-hosted or local trusted host | `./scripts/run_profile.sh metadata-replay-criterion` | Snapshot clone, provider construction, and long-history replay internals stay observable without creating authoritative PR evidence |
| Metadata/log engineering probe | Self-hosted or local trusted host | `./scripts/run_profile.sh metadata-log-criterion` | Log parsing and snapshot materialization internals stay observable without creating authoritative PR evidence |
| DML file-selection engineering probe | Self-hosted or local trusted host | `./scripts/run_profile.sh file-selection-criterion` | Shared delete/update file-selection seams stay observable without creating authoritative PR evidence |
| Merge planning engineering probe | Self-hosted or local trusted host | `./scripts/run_profile.sh merge-filter-criterion` | Merge early-filter planning stays observable without touching the full `merge_perf` rewrite path |
| Criterion scan microbench | Self-hosted or local trusted host | `./scripts/run_profile.sh scan-phase-criterion` | PR-sensitive execute-phase scan paths stay observable without becoming authoritative PR evidence |
| Longitudinal ingest/report/prune | Self-hosted | `longitudinal-nightly.yml`, `longitudinal-release-history.yml` | Only schema v5 identity-rich results enter the store |
| Trust-contract rerun | Local operator or self-hosted runner | `./scripts/validate_perf_harness.sh` | Fresh evidence in `results/validation/<timestamp>/summary.md` or a caller-selected artifact dir |

## Latest Evidence

`pr-macro` moved to the stronger `medium_selective` / 7-run / 15-iteration contract in methodology version `2`. Any earlier local evidence predates the current macro decision profile and must not be reused as authoritative support for the new contract.

`write_perf` is not promotion-ready until the rerun artifact shows both of these on the trusted runner class:

- the same-SHA `pr-write-perf` block reports only `no_change` or `inconclusive`
- the delayed `write_perf_unpartitioned_1m` canary classifies as `regression`

`delete_update_perf`, `merge_perf`, and `optimize_perf` remain candidate/manual until the trusted self-hosted evidence bundle shows all of these:

- the same-SHA `pr-delete-update-perf`, `pr-merge-perf`, and `pr-optimize-perf` blocks report only `no_change` or `inconclusive`
- the higher-confidence `delete-update-perf-high-confidence` rerun finishes inside the trusted runner budget for manual/nightly evidence refreshes
- the delayed `delete_perf_scattered_5pct_small_files`, `merge_perf_upsert_50pct`, and `optimize_perf_compact_small_files` canaries classify as `regression`
- runtime signoff on the trusted PR runner class
- the initial case lists remain stable for at least one full evidence refresh cycle

`metadata_perf` remains candidate/manual until the trusted self-hosted evidence bundle shows all of these:

- the same-SHA `pr-metadata-perf` block reports only `no_change` or `inconclusive`
- the delayed `metadata_perf_load_head_long_history` canary classifies as `regression`
- runtime signoff on the trusted PR runner class
- the checkpointed vs uncheckpointed coverage split remains explicit in the operator contract

`tpcds` remains candidate/manual until the trusted self-hosted evidence bundle shows all of these:

- the fixture root is explicitly provisioned for `tpcds_duckdb`
- the same-SHA `pr-tpcds` block reports only `no_change` or `inconclusive`
- the delayed `tpcds_q03` canary classifies as `regression`
- `tpcds_q72` remains outside the PR decision surface

Refresh the evidence on the current machine with:

- `cargo test --locked -p delta-bench`
- `python3 -m pytest -q python/tests`
- `./scripts/validate_perf_harness.sh --dataset-id medium_selective --artifact-dir results/validation/latest`
- `./scripts/validate_perf_harness.sh --sha 3fe2fa92a1dc54c8c6b378529b449f5f4c601e39 --fetch-url https://github.com/example/delta-rs --artifact-dir results/validation/fork-sha`
- `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-write-perf write_perf`
- `./scripts/validate_perf_harness.sh --artifact-dir results/validation/write-perf-gate`
- `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-delete-update-perf delete_update_perf`
- `./scripts/compare_branch.sh --current-vs-main --methodology-profile delete-update-perf-high-confidence delete_update_perf`
- `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-merge-perf merge_perf`
- `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-optimize-perf optimize_perf`
- `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-metadata-perf metadata_perf`
- `./scripts/validate_perf_harness.sh --artifact-dir results/validation/dml-maintenance-gate`
- `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-tpcds tpcds`
- `./scripts/validate_perf_harness.sh --dataset-id tpcds_duckdb --artifact-dir results/validation/tpcds-gate`
- `./scripts/run_profile.sh scan-phase-criterion`
- `./scripts/run_profile.sh metadata-replay-criterion`
- `./scripts/run_profile.sh metadata-log-criterion`
- `./scripts/run_profile.sh file-selection-criterion`
- `./scripts/run_profile.sh merge-filter-criterion`
- Most recent committed scan phase canaries:
  - load selected `+153.853 ms`, execute control `+0.519 ms`
  - plan selected `+153.289 ms`, execute control `+1.557 ms`
  - validate selected `+152.090 ms`, execute control `-0.425 ms`
  - execute selected `+152.444 ms`, plan control `+0.819 ms`

The committed source of truth is still the protocol plus the executable entrypoint. Fresh evidence is intentionally generated locally because it depends on the machine, Python environment, and runner class used for the rerun.

To refresh the evidence on the current machine, execute:

```bash
./scripts/validate_perf_harness.sh --artifact-dir results/validation/latest
```

If the validation SHA is not reachable from `origin`, use the trusted immutable-fetch contract explicitly:

```bash
./scripts/validate_perf_harness.sh \
  --sha 3fe2fa92a1dc54c8c6b378529b449f5f4c601e39 \
  --fetch-url https://github.com/example/delta-rs \
  --artifact-dir results/validation/fork-sha
```

To collect the dedicated write-path gate before promotion, execute:

```bash
./scripts/validate_perf_harness.sh --artifact-dir results/validation/write-perf-gate
```

To collect the dedicated TPC-DS gate before promotion, execute:

```bash
./scripts/validate_perf_harness.sh --dataset-id tpcds_duckdb --artifact-dir results/validation/tpcds-gate
```

By default the script writes a timestamped artifact tree under `results/validation/` and stores the Markdown summary at `summary.md` inside that directory. The example above uses a stable artifact dir, so the summary lands at `results/validation/latest/summary.md` with exact commands, statuses, and captured output.

Historical replay-state validation commands now route through the committed Criterion profile:

```bash
./scripts/run_profile.sh metadata-replay-criterion
```

## Re-Run Protocol

1. Prepare the local Python environment so `pandas`, `polars`, and `pyarrow` are available.
2. Run `cargo test --locked` and `python3 -m pytest -q python/tests`.
3. Run `./scripts/validate_perf_harness.sh` to refresh the focused trust-contract evidence. If the target SHA is not reachable from `origin`, add `--sha <commit> --fetch-url <trusted-clone-url>` and optionally `--fetch-ref <ref>` so the validator can prepare `.delta-rs-under-test` before the same-SHA compare seeds `.delta-rs-source`.
4. When validating a PR-style macro decision run directly, use `./scripts/compare_branch.sh --methodology-profile pr-macro ...` on a self-hosted runner instead of restating the raw decision knobs by hand.
5. For write-path investigation, use `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-write-perf write_perf` and treat `dataset_policy=intrinsic_case_workload` as part of the contract, not an operator override.
6. Run `./scripts/validate_perf_harness.sh --artifact-dir results/validation/write-perf-gate` before enabling bot-default decision runs, and confirm the write_perf same-SHA block stays `no_change`/`inconclusive` while the delayed `write_perf_unpartitioned_1m` canary is `regression`.
7. Run `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-delete-update-perf delete_update_perf` when validating the blocking PR delete/update gate. Run `./scripts/compare_branch.sh --current-vs-main --methodology-profile delete-update-perf-high-confidence delete_update_perf`, `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-merge-perf merge_perf`, and `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-optimize-perf optimize_perf` when refreshing higher-confidence manual or nightly DML and maintenance evidence. Then rerun `./scripts/validate_perf_harness.sh --artifact-dir results/validation/dml-maintenance-gate` and confirm the same-SHA blocks stay `no_change`/`inconclusive` while the delayed canaries classify as `regression`. Keep the original `delete_update`, `merge`, and `optimize_vacuum` suites correctness-only.
8. Run `./scripts/compare_branch.sh --current-vs-main --methodology-profile pr-metadata-perf metadata_perf` when refreshing candidate/manual metadata evidence. Confirm the same-SHA `pr-metadata-perf` block stays `no_change`/`inconclusive`, the delayed `metadata_perf_load_head_long_history` canary is `regression`, and the operator report still distinguishes checkpointed from uncheckpointed load paths.
9. Run `./scripts/validate_perf_harness.sh --dataset-id tpcds_duckdb --artifact-dir results/validation/tpcds-gate` before promoting `tpcds`, and confirm the TPC-DS same-SHA block stays `no_change`/`inconclusive` while the delayed `tpcds_q03` canary is `regression`. Keep the DuckDB-backed fixture tree pre-provisioned on trusted self-hosted runners, and keep `tpcds_q72` outside the PR decision surface.
10. For replay-state, metadata/log, DML file-selection, merge planning, or scan microbench work, keep the public macro compare surface separate, then run `./scripts/run_profile.sh metadata-replay-criterion`, `./scripts/run_profile.sh metadata-log-criterion`, `./scripts/run_profile.sh file-selection-criterion`, `./scripts/run_profile.sh merge-filter-criterion`, or `./scripts/run_profile.sh scan-phase-criterion` separately from the macro guardrail. Pass extra Criterion arguments after `--` when you need `--save-baseline` or `--baseline` on the same host. These reruns are diagnostic-only and never authoritative PR evidence.
11. Inspect the generated `summary.md` in the chosen artifact directory before making any benchmark trust claim. `Out of Scope (micro only)` rows should remain outside the macro regression/improvement counts.
12. Treat any smoke/correctness failure, scan phase canary drift, write_perf same-SHA instability, write-path canary failure, delete/update perf same-SHA instability, delete/update perf canary failure, merge perf same-SHA instability, merge perf canary failure, optimize perf same-SHA instability, optimize perf canary failure, metadata perf same-SHA instability, metadata perf canary failure, TPC-DS same-SHA instability, TPC-DS canary failure, replay-path canary failure, schema-v5 ingest rejection, or missing methodology metadata as a release blocker until explained or fixed.
