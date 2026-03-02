# Longitudinal Next Steps Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add parallel matrix execution with load guards, retention/pruning policies, and optional statistical significance checks for longitudinal trends.

**Architecture:** Extend existing `delta_bench_longitudinal` Python package with three cohesive modules/paths: (1) concurrent matrix scheduling with host load gating, (2) explicit retention utilities with dry-run/apply behavior, and (3) reporting significance evaluation layered on top of existing threshold logic. Keep behavior opt-in and idempotent.

**Tech Stack:** Python 3.9+ stdlib (`concurrent.futures`, `datetime`, `statistics`, `math`), existing CLI/workflow/test harness.

---

### Task 1: Parallel Matrix + Host Load Guard

**Files:**
- Modify: `python/delta_bench_longitudinal/matrix_runner.py`
- Modify: `python/delta_bench_longitudinal/cli.py`
- Modify: `python/tests/test_longitudinal_matrix_runner.py`

**Step 1: Write failing tests**
- Add tests for:
  - effective parallel execution (`max_parallel > 1`)
  - load guard wait behavior before dispatch
  - validation of new parallel/load options

**Step 2: Run tests to verify RED**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_matrix_runner.py`
- Expected: FAIL for missing args/behavior.

**Step 3: Implement minimal code**
- Add `max_parallel`, `max_load_per_cpu`, and `load_check_interval_seconds` to `MatrixRunConfig`.
- Execute cells via `ThreadPoolExecutor` while preserving resume/skip/retry semantics.
- Add load gate loop using `os.getloadavg()/os.cpu_count()` when threshold configured.
- Expose new options in `run-matrix` and `orchestrate` CLI paths.

**Step 4: Run tests to verify GREEN**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_matrix_runner.py`
- Expected: PASS.

### Task 2: Retention and Pruning Policies

**Files:**
- Create: `python/delta_bench_longitudinal/retention.py`
- Modify: `python/delta_bench_longitudinal/cli.py`
- Create: `python/tests/test_longitudinal_retention.py`
- Modify: `.github/workflows/longitudinal-nightly.yml`

**Step 1: Write failing tests**
- Add tests for:
  - artifact pruning candidate selection by age/count
  - dry-run no-delete behavior
  - apply mode deletes and rewrites store/index safely

**Step 2: Run tests to verify RED**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_retention.py`
- Expected: FAIL due to missing module/command behavior.

**Step 3: Implement minimal code**
- Add artifact/store pruning helpers with explicit `apply` gating.
- Add CLI `prune` command with bounded validated parameters.
- Add nightly workflow prune step with configurable env inputs.

**Step 4: Run tests to verify GREEN**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_retention.py`
- Expected: PASS.

### Task 3: Optional Significance Checks in Reporting

**Files:**
- Modify: `python/delta_bench_longitudinal/store.py`
- Modify: `python/delta_bench_longitudinal/reporting.py`
- Modify: `python/delta_bench_longitudinal/cli.py`
- Modify: `python/tests/test_longitudinal_store.py`
- Modify: `python/tests/test_longitudinal_reporting.py`

**Step 1: Write failing tests**
- Add tests for:
  - store preserving per-case sample arrays needed for significance checks
  - report significance output when method is enabled
  - significance-disabled mode preserving current threshold-only behavior

**Step 2: Run tests to verify RED**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_store.py python/tests/test_longitudinal_reporting.py`
- Expected: FAIL due to missing fields/options.

**Step 3: Implement minimal code**
- Persist `sample_values_ms` in normalized rows.
- Add `significance_method` (`none`, `mann-whitney`) and `significance_alpha`.
- Compute p-value (normal approximation with tie-aware variance) and expose in markdown/html summaries.
- Add CLI flags for report/orchestrate and wire through workflow.

**Step 4: Run tests to verify GREEN**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_store.py python/tests/test_longitudinal_reporting.py`
- Expected: PASS.

### Task 4: Docs + Full Verification

**Files:**
- Modify: `README.md`
- Modify: `docs/longitudinal-runbook.md`
- Modify: `python/tests/test_longitudinal_workflow.py` (if needed for new workflow assertions)

**Step 1: Update docs**
- Document:
  - new matrix parallel/load options
  - prune command and safety model (`--apply`)
  - significance options and interpretation

**Step 2: Full verification**
- `cargo fmt --all --check`
- `cargo test -p delta-bench --tests`
- `PYTHONPATH=python python3 -m pytest -q python/tests`
- `for f in scripts/*.sh; do bash -n "$f"; done`

**Step 3: Final status**
- Summarize design tradeoffs, limitations, and next options.
