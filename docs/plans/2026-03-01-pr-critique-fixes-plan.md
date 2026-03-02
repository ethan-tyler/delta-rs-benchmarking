# PR Critique High-Priority Fixes Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Close the highest-impact benchmark-trust gaps: warmup failure visibility, explicit advisory compare behavior, and honest manifest/suite scope with scenario-aware fixture generation.

**Architecture:** Preserve current harness shape while tightening guarantees: make warmup failures explicit in runner output, keep compare behavior advisory-only while surfacing deprecated CI flags clearly, and make fixture generation scenario-aware via dataset profiles so TPC-DS and many-versions workloads are actually materialized. Keep compatibility where possible by extending APIs with additive options and maintaining deterministic ordering.

**Tech Stack:** Rust (`delta-bench`, tokio, serde), Python (`delta_bench_compare`, pytest), shell scripts (`compare_branch.sh`)

---

### Task 1: Warmup Failure Handling in Runner

**Files:**
- Modify: `crates/delta-bench/src/runner/mod.rs`
- Test: `crates/delta-bench/tests/runner_timing.rs`

**Step 1: Write the failing test**
- Add tests showing warmup errors are not silently discarded (sync + async setup variants).

**Step 2: Run test to verify it fails**
- Run: `cargo test -p delta-bench runner_timing -- --nocapture`
- Expected: new warmup failure assertions fail.

**Step 3: Write minimal implementation**
- Capture warmup failures in runner helpers and return explicit failure `CaseResult` (with message indicating warmup phase).

**Step 4: Run test to verify it passes**
- Run: `cargo test -p delta-bench runner_timing -- --nocapture`
- Expected: all runner timing tests pass.

### Task 2: Advisory Compare Deprecation Path

**Files:**
- Modify: `python/delta_bench_compare/compare.py`
- Modify: `python/tests/test_compare.py`
- Modify: `scripts/compare_branch.sh`

**Step 1: Write the failing test**
- Add compare CLI tests asserting deprecated `--ci` / `--max-allowed-regressions` flags emit an explicit warning and remain advisory.

**Step 2: Run test to verify it fails**
- Run: `python3 -m pytest python/tests/test_compare.py -k deprecated_ci_flags -q`
- Expected: new CLI warning test fails before implementation.

**Step 3: Write minimal implementation**
- In `compare.py`, keep output advisory-only and print an explicit deprecation warning to `stderr` when `--ci` / `--max-allowed-regressions` are provided.
- In `compare_branch.sh`, keep `--ci` and `--max-allowed-regressions` as deprecated no-op options with warnings.

**Step 4: Run test to verify it passes**
- Run: `python3 -m pytest python/tests/test_compare.py -k deprecated_ci_flags -q`
- Expected: deprecation warning tests pass.

### Task 3: Scenario-Aware Fixture Generation (TPC-DS + Many Versions)

**Files:**
- Modify: `crates/delta-bench/src/data/fixtures.rs`
- Modify: `crates/delta-bench/src/main.rs`
- Modify: `crates/delta-bench/src/manifests.rs`
- Modify: `crates/delta-bench/tests/fixtures_generation.rs`
- Modify: `crates/delta-bench/tests/manifests.rs`
- Modify: `crates/delta-bench/tests/tpcds_runner.rs`

**Step 1: Write the failing test**
- Add fixture-generation tests asserting `fixtures/<scale>/tpcds/store_sales` exists after data generation.
- Add dataset-scenario test asserting `many_versions` generation produces multi-version table history for metadata table.

**Step 2: Run test to verify it fails**
- Run: `cargo test -p delta-bench fixtures_generation manifests tpcds_runner -- --nocapture`
- Expected: new tests fail on missing scenario artifacts.

**Step 3: Write minimal implementation**
- Introduce dataset profile input for fixture generation (default + many_versions).
- Generate deterministic minimal TPC-DS fixture table(s) required by enabled phase-1 queries.
- For many_versions profile, append deterministic commits to `narrow_sales_delta` so metadata time travel exercises log traversal.
- Update data command path to pass dataset profile to fixture generator.

**Step 4: Run test to verify it passes**
- Run: `cargo test -p delta-bench fixtures_generation manifests tpcds_runner -- --nocapture`
- Expected: scenario fixture tests pass.

### Task 4: Manifest/Scope Alignment for Default `all`

**Files:**
- Modify: `bench/manifests/p0-rust.yaml`
- Modify: `crates/delta-bench/tests/manifests.rs`
- Modify: `README.md`

**Step 1: Write the failing test**
- Add test asserting P0 Rust manifest includes all enabled TPC-DS cases currently considered runnable.

**Step 2: Run test to verify it fails**
- Run: `cargo test -p delta-bench manifests -- --nocapture`
- Expected: manifest coverage test fails.

**Step 3: Write minimal implementation**
- Add TPC-DS entries to P0 Rust manifest with assertions (or explicitly scope README to P0 subset if intentionally excluded).
- Keep README scope and default run behavior consistent.

**Step 4: Run test to verify it passes**
- Run: `cargo test -p delta-bench manifests -- --nocapture`
- Expected: manifest alignment tests pass.

### Task 5: Full Verification Sweep

**Files:**
- No new code files

**Step 1: Run targeted Rust tests**
- Run: `cargo test -p delta-bench runner_timing fixtures_generation manifests tpcds_runner execution_planning -- --nocapture`

**Step 2: Run targeted Python tests**
- Run: `python3 -m pytest python/tests/test_compare.py python/tests/test_benchmark_automation_hardening.py -q`

**Step 3: Summarize outcomes with evidence**
- Record exact commands and pass/fail outcomes before claiming completion.
