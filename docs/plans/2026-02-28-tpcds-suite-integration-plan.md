# TPC-DS Suite Integration Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a first-class `tpcds` benchmark target to delta-bench that runs a curated TPC-DS query set (with explicit skips such as `q72`) and emits per-query benchmark results in the existing result schema.

**Architecture:** Introduce a new `tpcds` suite module with a query catalog + SQL asset loader, then wire it into target listing/dispatch and bench scripts. Keep phase 1 pragmatic: pre-generated TPC-DS Delta fixtures are consumed from `fixtures/<scale>/tpcds/` (or object-store root in non-local mode), rather than implementing a full TPC-DS data generator in this repo. Reuse existing `SampleMetrics` + read-path metric extraction so TPC-DS cases report both latency and scan-efficiency counters.

**Tech Stack:** Rust (`delta-bench`, DataFusion SQL execution, serde), Bash (`scripts/bench.sh`), markdown docs/tests.

---

### Task 1: Query catalog + skip policy

**Files:**
- Create: `crates/delta-bench/src/suites/tpcds/catalog.rs`
- Create: `crates/delta-bench/src/suites/tpcds/mod.rs`
- Create: `crates/delta-bench/tests/tpcds_catalog.rs`

**Step 1: Write the failing test**
- Add tests asserting:
1. query IDs are stable and sorted (for deterministic output)
2. `q72` is present in the catalog but marked skipped with explicit reason referencing DataFusion issue tracking
3. at least one executable query exists in phase 1

**Step 2: Run test to verify it fails**
- Run: `cargo test -p delta-bench --test tpcds_catalog -- --nocapture`
- Expected: FAIL because `tpcds` catalog does not exist.

**Step 3: Write minimal implementation**
- Add `TpcdsQuerySpec` struct and catalog function returning phase-1 query specs.
- Include fields: `id`, `sql_file`, `enabled`, `skip_reason`.
- Encode `q72` as disabled with explicit reason.

**Step 4: Run test to verify it passes**
- Run: `cargo test -p delta-bench --test tpcds_catalog -- --nocapture`
- Expected: PASS.

**Step 5: Commit**
```bash
git add crates/delta-bench/src/suites/tpcds/catalog.rs crates/delta-bench/src/suites/tpcds/mod.rs crates/delta-bench/tests/tpcds_catalog.rs
git commit -m "feat: add TPC-DS query catalog with explicit skip policy"
```

### Task 2: SQL assets + loader

**Files:**
- Create: `crates/delta-bench/src/suites/tpcds/sql_loader.rs`
- Create: `crates/delta-bench/src/suites/tpcds/sql/q03.sql`
- Create: `crates/delta-bench/src/suites/tpcds/sql/q07.sql`
- Create: `crates/delta-bench/src/suites/tpcds/sql/q64.sql`
- Create: `crates/delta-bench/tests/tpcds_sql_loader.rs`

**Step 1: Write the failing test**
- Add loader tests asserting:
1. loader returns SQL for enabled queries
2. missing SQL file returns actionable error including query id and path
3. disabled queries are not returned for execution

**Step 2: Run test to verify it fails**
- Run: `cargo test -p delta-bench --test tpcds_sql_loader -- --nocapture`
- Expected: FAIL because SQL assets/loader are missing.

**Step 3: Write minimal implementation**
- Implement SQL loader that resolves SQL files under `crates/delta-bench/src/suites/tpcds/sql/`.
- Add a small initial SQL corpus (3-5 queries) with stable file naming (`qXX.sql`).

**Step 4: Run test to verify it passes**
- Run: `cargo test -p delta-bench --test tpcds_sql_loader -- --nocapture`
- Expected: PASS.

**Step 5: Commit**
```bash
git add crates/delta-bench/src/suites/tpcds/sql_loader.rs crates/delta-bench/src/suites/tpcds/sql/*.sql crates/delta-bench/tests/tpcds_sql_loader.rs
git commit -m "feat: add TPC-DS SQL assets and loader"
```

### Task 3: Suite registration + CLI/script wiring

**Files:**
- Modify: `crates/delta-bench/src/suites/mod.rs`
- Modify: `crates/delta-bench/tests/suite_registry.rs`
- Modify: `scripts/bench.sh`
- Modify: `README.md`

**Step 1: Write the failing test**
- Extend suite registry tests to require `tpcds` in target list and known case names.

**Step 2: Run test to verify it fails**
- Run: `cargo test -p delta-bench --test suite_registry -- --nocapture`
- Expected: FAIL because `tpcds` target is unknown.

**Step 3: Write minimal implementation**
- Add `pub mod tpcds;` and register `tpcds` in `list_targets`, `list_cases_for_target`, and `run_target`.
- Update `scripts/bench.sh` usage string to include `tpcds` as allowed `--suite` value.
- Update README target lists/examples accordingly.

**Step 4: Run test to verify it passes**
- Run: `cargo test -p delta-bench --test suite_registry -- --nocapture`
- Expected: PASS.

**Step 5: Commit**
```bash
git add crates/delta-bench/src/suites/mod.rs crates/delta-bench/tests/suite_registry.rs scripts/bench.sh README.md
git commit -m "feat: wire TPC-DS suite into target registry and bench script"
```

### Task 4: TPC-DS runner implementation

**Files:**
- Modify: `crates/delta-bench/src/suites/tpcds/mod.rs`
- Create: `crates/delta-bench/src/suites/tpcds/registration.rs`
- Create: `crates/delta-bench/tests/tpcds_runner.rs`

**Step 1: Write the failing test**
- Add runner tests asserting:
1. enabled queries execute and emit successful `CaseResult`
2. skipped queries emit deterministic skipped/failure representation (choose one behavior and test it)
3. each successful sample includes normalized metrics and read-path scan metrics when available

**Step 2: Run test to verify it fails**
- Run: `cargo test -p delta-bench --test tpcds_runner -- --nocapture`
- Expected: FAIL because runner is not implemented.

**Step 3: Write minimal implementation**
- Implement table registration for TPC-DS tables against fixture root (`fixtures/<scale>/tpcds/<table>` for local; storage-root mapping for non-local).
- Execute each enabled SQL query via DataFusion `SessionContext`.
- Reuse scan metric extraction pattern from `read_scan` to populate `files_scanned`, `files_pruned`, `bytes_scanned`, `scan_time_ms`.

**Step 4: Run test to verify it passes**
- Run: `cargo test -p delta-bench --test tpcds_runner -- --nocapture`
- Expected: PASS.

**Step 5: Commit**
```bash
git add crates/delta-bench/src/suites/tpcds/mod.rs crates/delta-bench/src/suites/tpcds/registration.rs crates/delta-bench/tests/tpcds_runner.rs
git commit -m "feat: implement TPC-DS suite runner with scan metrics"
```

### Task 5: End-to-end smoke and compare compatibility

**Files:**
- Create/Modify: `crates/delta-bench/tests/tpcds_smoke.rs`
- Modify (if needed): `python/tests/test_compare.py`

**Step 1: Write the failing test**
- Add smoke test that runs `tpcds` target for 1 iteration with a minimal fixture and verifies output JSON shape/case naming.
- If compare behavior needs stabilization for many query cases, add focused Python assertion coverage.

**Step 2: Run test to verify it fails**
- Run:
1. `cargo test -p delta-bench --test tpcds_smoke -- --nocapture`
2. `cd python && python3 -m pytest -q tests/test_compare.py -k tpcds`
- Expected: FAIL until smoke/compare compatibility is implemented.

**Step 3: Write minimal implementation**
- Add the smallest fixture-backed smoke path for TPC-DS execution.
- Ensure per-case names remain deterministic (`tpcds_q03`, etc.) for compare table stability.

**Step 4: Run test to verify it passes**
- Run the same commands from Step 2.
- Expected: PASS.

**Step 5: Commit**
```bash
git add crates/delta-bench/tests/tpcds_smoke.rs python/tests/test_compare.py
git commit -m "test: add TPC-DS smoke and compare compatibility coverage"
```

### Task 6: Docs and verification

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/adr/0002-result-schema-v1.md` (if result semantics need clarifications)

**Step 1: Implement docs updates**
- Document:
1. `tpcds` target usage in `bench.sh` and CLI
2. fixture layout contract for TPC-DS tables
3. query skip policy (`q72`) and reason
4. advisory/non-gating benchmark policy remains unchanged

**Step 2: Run verification**
- Run:
1. `cargo fmt --all -- --check`
2. `cargo clippy -p delta-bench --all-targets -- -D warnings`
3. `cargo test -p delta-bench`
4. `cd python && python3 -m pytest -q`
5. `bash -n scripts/bench.sh scripts/compare_branch.sh`

**Step 3: Commit**
```bash
git add README.md docs/architecture.md docs/adr/0002-result-schema-v1.md
git commit -m "docs: document TPC-DS target and fixture contract"
```
