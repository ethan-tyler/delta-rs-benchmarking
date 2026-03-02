# Benchmark Follow-Ups Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Implement four benchmark harness follow-ups: read-scan setup timing split, compare aggregation/formatting improvements, variance metrics output, and metrics builder API cleanup.

**Architecture:** Keep schema compatibility by adding optional fields and preserving wrapper APIs where needed. Move expensive read-scan setup into async setup closures so timed sections only execute prebuilt plans. Add configurable compare aggregation with backward-compatible defaults and clearer change formatting.

**Tech Stack:** Rust (`delta-bench` crate, tokio, serde), Python (`delta_bench_compare`, pytest)

---

### Task 1: Read-Scan Setup Split

**Files:**
- Modify: `crates/delta-bench/src/suites/read_scan.rs`
- Test: `crates/delta-bench/tests/real_ops.rs`

1. Add failing test for prepared read-scan execution behavior.
2. Run failing test.
3. Refactor read-scan into setup + execute phases using `run_case_async_with_async_setup`.
4. Re-run read-scan tests.

### Task 2: Variance Metrics Output

**Files:**
- Modify: `crates/delta-bench/src/results.rs`
- Modify: `crates/delta-bench/src/stats.rs`
- Modify: `crates/delta-bench/src/runner/mod.rs`
- Test: `crates/delta-bench/tests/runner_timing.rs`

1. Add failing tests for per-case elapsed stats output.
2. Run failing tests.
3. Add optional elapsed stats model to `CaseResult` and compute in runner paths.
4. Re-run stats and runner tests.

### Task 3: Builder API Cleanup

**Files:**
- Modify: `crates/delta-bench/src/results.rs`
- Modify: relevant suite call sites and tests using positional builder args
- Test: `crates/delta-bench/tests/sample_metrics_helpers.rs`

1. Add failing tests for struct-based metric builders.
2. Run failing tests.
3. Add structured builder payload types and migrate call sites.
4. Re-run affected Rust tests.

### Task 4: Compare Aggregation + Formatting

**Files:**
- Modify: `python/delta_bench_compare/compare.py`
- Modify: `python/tests/test_compare.py`
- Modify: `docs/user-guide.md`

1. Add failing tests for aggregation strategy options and non-prefixed faster formatting.
2. Run failing Python tests.
3. Implement `--aggregation` strategy (`min|median|p95`) and formatting updates.
4. Re-run compare tests and update user guide.
