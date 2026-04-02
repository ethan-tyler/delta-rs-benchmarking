# Validation

This page records how to validate that the benchmark harness is still producing trustworthy results after workflow, manifest, or timing changes.

## Purpose

The trust contract is split deliberately:

- GitHub-hosted CI validates only smoke and correctness lanes.
- Self-hosted workflows own macro perf, decision compare, Criterion microbench work, and longitudinal ingestion.
- Self-hosted PR decision compare uses the harness-owned `pr-macro` methodology profile. That contract fixes decision-grade compare settings, records `iqr_ms` spread, and renders sub-millisecond rows as `micro_only` in `Out of Scope (micro only)` instead of counting them as macro regressions.
- `./scripts/validate_perf_harness.sh` reruns the contract suites and writes a rerunnable Markdown summary under `results/validation/<timestamp>/summary.md` by default.

Use this page as the stable operator guide, and use the generated artifact under `results/validation/` as the latest machine-local evidence.

When you need to publish the current operator-facing contract itself, run `./scripts/publish_contract.sh`. It snapshots the current docs, manifests, and wrapper entrypoints into `results/contracts/`.

The authoritative scan decision manifest is intentionally narrower than the raw suite: `scan_pruning_miss` is currently disabled from `core_rust.yaml`, so it must not be used for authoritative perf conclusions until requalified.

## Verification Matrix

| Surface | Host class | Command or workflow | Trusted outcome |
|---|---|---|---|
| Rust/Python unit and integration tests | GitHub-hosted | `.github/workflows/ci.yml` test job | No regressions in harness logic |
| Smoke validation lane | GitHub-hosted | `.github/workflows/ci.yml` hosted benchmark validation job | All suites stay operational on cheap hosted runners |
| Correctness validation lane | GitHub-hosted | `.github/workflows/ci.yml` hosted benchmark validation job | Semantic/hash-backed assertions hold for trusted correctness suites |
| Branch compare and decision runs | Self-hosted | `benchmark.yml`, `benchmark-prerelease.yml`, `./scripts/compare_branch.sh --methodology-profile pr-macro` | Macro perf claims only on hardened runners, with sub-ms rows rendered as `micro_only` out of scope |
| Criterion scan microbench | Self-hosted or local trusted host | `cargo bench -p delta-bench --bench scan_phase_bench` | PR-sensitive scan phases stay observable |
| Longitudinal ingest/report/prune | Self-hosted | `longitudinal-nightly.yml`, `longitudinal-release-history.yml` | Only schema v5 identity-rich results enter the store |
| Trust-contract rerun | Local operator or self-hosted runner | `./scripts/validate_perf_harness.sh` | Fresh evidence in `results/validation/<timestamp>/summary.md` or a caller-selected artifact dir |

## Latest Evidence

Latest local rerun on 2026-03-28 (UTC):

- delta-rs SHA: `3fe2fa92a1dc54c8c6b378529b449f5f4c601e39`
- Dataset: `tiny_smoke`
- Commands:
  - `cargo test --locked -p delta-bench`
  - `python3 -m pytest -q python/tests`
  - `./scripts/validate_perf_harness.sh --dataset-id tiny_smoke --artifact-dir results/validation/20260328T235959Z`
- Validation artifact: `results/validation/20260328T235959Z/summary.md`
- Same-SHA decision compare via `--methodology-profile pr-macro`: `inconclusive=4`, `micro_only=0`, with no false regression/improvement classification
- Phase canaries:
  - load selected `+153.853 ms`, execute control `+0.519 ms`
  - plan selected `+153.289 ms`, execute control `+1.557 ms`
  - validate selected `+152.090 ms`, execute control `-0.425 ms`
  - execute selected `+152.444 ms`, plan control `+0.819 ms`
- Synthetic regression canary: `scan_filter_flag` classified as `regression` (`11.210 ms -> 167.255 ms`)

The committed source of truth is still the protocol plus the executable entrypoint. Fresh evidence is intentionally generated locally because it depends on the machine, Python environment, and runner class used for the rerun.

To refresh the evidence on the current machine, execute:

```bash
./scripts/validate_perf_harness.sh --artifact-dir results/validation/latest
```

By default the script writes a timestamped artifact tree under `results/validation/` and stores the Markdown summary at `summary.md` inside that directory. The example above uses a stable artifact dir, so the summary lands at `results/validation/latest/summary.md` with exact commands, statuses, and captured output.

## Re-Run Protocol

1. Prepare the local Python environment so `pandas`, `polars`, and `pyarrow` are available.
2. Run `cargo test --locked` and `python3 -m pytest -q python/tests`.
3. Run `./scripts/validate_perf_harness.sh` to refresh the focused trust-contract evidence.
4. When validating a PR-style macro decision run directly, use `./scripts/compare_branch.sh --methodology-profile pr-macro ...` on a self-hosted runner instead of restating the raw decision knobs by hand.
5. Inspect the generated `summary.md` in the chosen artifact directory before making any benchmark trust claim. `Out of Scope (micro only)` rows should remain outside the macro regression/improvement counts.
6. Treat any smoke/correctness failure, scan phase canary drift, schema-v5 ingest rejection, or missing `pr-macro` scope metadata as a release blocker until explained or fixed.
