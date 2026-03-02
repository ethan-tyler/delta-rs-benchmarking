# Wave 1 Review Remediation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Resolve review-blocking Wave 1 issues: fixture cache invalidation/migration, optimize heavy-case distinctness, and merge fixture failure consistency.

**Architecture:** Keep existing suite boundaries and fixture generation flow, but harden fixture validity checks behind a manifest schema constant and required-table presence checks. Adjust optimize case configuration (not API shape) to create a truly heavier compaction profile, and normalize local merge fixture preflight checks to return structured fixture errors. Add focused regression tests that fail on pre-fix behavior.

**Tech Stack:** Rust (`delta-bench` crate), Tokio tests, serde_json, existing benchmark suite infrastructure.

---

### Task 1: Add failing tests for fixture migration and missing localized merge fixture behavior

**Files:**
- Modify: `crates/delta-bench/tests/fixtures_generation.rs`
- Modify: `crates/delta-bench/tests/failure_paths.rs`

1. Add a fixture-generation regression test that simulates stale/partial fixtures and expects `generate_fixtures(..., force=false)` to rebuild missing Wave 1 tables.
2. Add a failure-path test that removes only `merge_partitioned_target_delta` and expects all merge cases to return fixture-style failures (not raw IO error for one case).
3. Run targeted tests and confirm they fail before implementation.

### Task 2: Implement fixture schema/version + required-table cache validation

**Files:**
- Modify: `crates/delta-bench/src/data/fixtures.rs`

1. Introduce a fixture schema constant and use it for manifest writes/checks.
2. Gate early-return cache hit on both manifest match and required table presence.
3. Keep behavior backward-compatible by forcing regeneration for older schema versions.
4. Re-run Task 1 fixture regression test and confirm pass.

### Task 3: Differentiate heavy optimize compaction case and add regression assertion

**Files:**
- Modify: `crates/delta-bench/src/suites/optimize_vacuum.rs`
- Modify: `crates/delta-bench/tests/real_ops.rs`

1. Parameterize optimize execution target size and configure heavy compaction with a stricter target size than standard compact case.
2. Add/adjust test assertion to require heavy case operation count to exceed standard compact case operation count.
3. Run targeted optimize test to verify red->green.

### Task 4: Normalize merge local fixture preflight checks and finish verification

**Files:**
- Modify: `crates/delta-bench/src/suites/merge_dml.rs`
- Optional Modify: `README.md` (note fixture regeneration implications)

1. Add local preflight checks for both standard and partitioned merge fixture tables; if missing, return `fixture_error_cases(...)` for all merge cases.
2. Run targeted merge failure-path/real-ops tests.
3. Run full verification:
   - `cargo fmt --all -- --check`
   - `cargo clippy -p delta-bench --all-targets -- -D warnings`
   - `cargo test -p delta-bench`
   - `cd python && python3 -m pytest -q`
