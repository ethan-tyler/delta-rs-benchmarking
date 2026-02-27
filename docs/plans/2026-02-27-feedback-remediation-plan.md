# Feedback Remediation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Address benchmark correctness, reproducibility, path safety, and operational hardening issues raised in review (P0/P1).

**Architecture:** Add explicit setup-vs-measure execution in the benchmark runner, enforce deterministic fixture regeneration semantics via manifest checks, and add strict CLI label validation. Update affected suites to avoid timing setup overhead and harden branch-compare orchestration with configurable command timeouts.

**Tech Stack:** Rust (`tokio`, `clap`, `deltalake-core`), Bash, pytest/cargo test.

---

### Task 1: Fixture regeneration semantics + scale validation

**Files:**
- Modify: `crates/delta-bench/src/data/fixtures.rs`
- Test: `crates/delta-bench/tests/fixtures_generation.rs`

**Step 1: Write the failing test**
- Add test that generates `sf1` seed `42`, re-runs with seed `43` without `--force`, and expects manifest seed to become `43`.
- Add test that unknown scale returns error.

**Step 2: Run test to verify it fails**
- Run: `cargo test -p delta-bench fixtures_generation -- --nocapture`
- Expected: FAIL with stale-manifest behavior and/or unknown-scale behavior.

**Step 3: Write minimal implementation**
- Add strict scale parser returning `BenchError::InvalidArgument` for unknown scales.
- When fixture root exists and `force` is false, compare existing manifest to requested params; regenerate on mismatch.

**Step 4: Run test to verify it passes**
- Run same test command and confirm PASS.

### Task 2: Runner setup/measure split

**Files:**
- Modify: `crates/delta-bench/src/runner/mod.rs`
- Test: `crates/delta-bench/tests/runner_timing.rs`

**Step 1: Write the failing test**
- Add async test where setup sleeps, op is near-noop, and measured elapsed excludes setup delay.

**Step 2: Run test to verify it fails**
- Run: `cargo test -p delta-bench runner_timing -- --nocapture`
- Expected: FAIL due missing API/behavior.

**Step 3: Write minimal implementation**
- Add `run_case_async_with_setup` API where setup happens before timer start for each warmup/iteration.

**Step 4: Run test to verify it passes**
- Run same test command and confirm PASS.

### Task 3: Label validation + clippy cleanup

**Files:**
- Modify: `crates/delta-bench/src/cli.rs`
- Modify: `crates/delta-bench/src/main.rs`
- Modify: `crates/delta-bench/src/stats.rs`
- Test: `crates/delta-bench/tests/cli_validation.rs`

**Step 1: Write the failing test**
- Add tests for accepted/rejected labels.

**Step 2: Run test to verify it fails**
- Run: `cargo test -p delta-bench cli_validation -- --nocapture`

**Step 3: Write minimal implementation**
- Add `validate_label` helper allowing safe chars only.
- Call it before result path creation.
- Replace `% 2 == 0` clippy warning in stats.

**Step 4: Run test to verify it passes**
- Run same test command and confirm PASS.

### Task 4: Suite updates to reduce timed setup overhead

**Files:**
- Modify: `crates/delta-bench/src/suites/merge_dml.rs`
- Modify: `crates/delta-bench/src/suites/write.rs`
- Modify: `crates/delta-bench/src/suites/metadata.rs`

**Step 1: Write the failing test**
- Extend/adjust runner timing test coverage to prove setup exclusion behavior used by suites.

**Step 2: Run test to verify it fails**
- Run targeted tests (runner + real ops where needed).

**Step 3: Write minimal implementation**
- Use `Arc<Vec<_>>` instead of cloning row vectors.
- Move merge fixture copy into setup phase via `run_case_async_with_setup`.
- Rename metadata load case to non-misleading name and use setup phase for cloned-path loads.

**Step 4: Run tests to verify pass**
- Run: `cargo test -p delta-bench`

### Task 5: Script hardening for branch comparison

**Files:**
- Modify: `scripts/compare_branch.sh`
- (Optional docs) Modify: `README.md`

**Step 1: Write failing verification check**
- Validate script syntax and expected usage for timeout env.

**Step 2: Implement minimal hardening**
- Add configurable timeout wrapper (`timeout`/`gtimeout` fallback).
- Wrap benchmark invocations with timeout.

**Step 3: Verify**
- Run: `bash -n scripts/compare_branch.sh`

### Task 6: Final verification

**Files:** N/A

**Step 1: Run full verification**
- `cargo fmt --all -- --check`
- `cargo clippy --all-targets -- -D warnings`
- `cargo test`
- `python3 -m pytest -q` (in `python/`)
- `bash -n scripts/*.sh`

**Step 2: Confirm outputs and summarize changes with evidence**
