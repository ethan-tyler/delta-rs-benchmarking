# Step 3 Metric Plumbing Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add benchmark metric plumbing so delta-bench captures scan/pruning efficiency metrics and can optionally show them in baseline-vs-candidate comparisons.

**Architecture:** Extend the run-result metric schema with explicit scan/rewrite fields, then plumb those values from execution paths that already expose them (read scan physical plan metrics, merge metrics, optimize metrics). Keep rendering backward-compatible by making new comparison columns opt-in (`--include-metrics`). Validate via focused Rust and Python tests, then run full verification.

**Tech Stack:** Rust (`delta-bench`, DataFusion execution metrics, serde), Python (`delta_bench_compare`, pytest), CLI compare workflow.

---

### Task 1: Result schema + compatibility guardrails

**Files:**
- Modify: `crates/delta-bench/src/results.rs`
- Create/Modify: `crates/delta-bench/tests/result_schema_compat.rs`

**Step 1: Write the failing test**
- Add/extend schema compatibility tests that:
1. deserialize legacy samples with no new fields
2. deserialize samples with `files_scanned`, `files_pruned`, `bytes_scanned`, `scan_time_ms`, `rewrite_time_ms`
3. verify serde round-trip keeps new fields when present

**Step 2: Run test to verify it fails**
- Run: `cargo test -p delta-bench result_schema_compat -- --nocapture`
- Expected: FAIL because the new optional fields are not fully represented or round-trip-safe.

**Step 3: Write minimal implementation**
- Add the five new optional fields to `SampleMetrics` with serde defaults/skip-when-none behavior.
- Update any constructor/default conversion helpers (for example `impl From<u64> for SampleMetrics`) so they initialize the new fields safely.

**Step 4: Run test to verify it passes**
- Run: `cargo test -p delta-bench result_schema_compat -- --nocapture`
- Expected: PASS.

**Step 5: Commit**
```bash
git add crates/delta-bench/src/results.rs crates/delta-bench/tests/result_schema_compat.rs
git commit -m "feat: extend sample metrics schema for scan and rewrite counters"
```

### Task 2: Read-path physical-plan metric collection

**Files:**
- Modify: `crates/delta-bench/src/suites/read_scan.rs`
- Modify: `crates/delta-bench/tests/real_ops.rs`

**Step 1: Write the failing test**
- Add/extend read-scan real-op test assertions so at least one successful sample reports:
1. `files_scanned`
2. `bytes_scanned`
3. `scan_time_ms`
- Keep a sanity check that `files_scanned >= files_pruned` when both are present.

**Step 2: Run test to verify it fails**
- Run: `cargo test -p delta-bench read_scan_samples_include_physical_scan_metrics -- --nocapture`
- Expected: FAIL because current read path only reports row counts.

**Step 3: Write minimal implementation**
- Build query physical plan before collect.
- Traverse plan + children and aggregate scan-related metrics by known names:
1. files scanned (`files_scanned` / `count_files_scanned`)
2. files pruned (`files_pruned`, `count_files_pruned`, pruning metric variants)
3. bytes scanned (`bytes_scanned`)
4. scan elapsed time from scan-node `elapsed_compute` converted to ms
- Populate new fields in `SampleMetrics` returned by read-scan cases.

**Step 4: Run test to verify it passes**
- Run: `cargo test -p delta-bench read_scan_samples_include_physical_scan_metrics -- --nocapture`
- Expected: PASS.

**Step 5: Commit**
```bash
git add crates/delta-bench/src/suites/read_scan.rs crates/delta-bench/tests/real_ops.rs
git commit -m "feat: capture physical scan metrics in read benchmark samples"
```

### Task 3: Merge and optimize metric mapping

**Files:**
- Modify: `crates/delta-bench/src/suites/merge_dml.rs`
- Modify: `crates/delta-bench/src/suites/optimize_vacuum.rs`
- Modify: `crates/delta-bench/tests/metrics_normalization.rs`
- Modify: `crates/delta-bench/tests/real_ops.rs`

**Step 1: Write the failing test**
- Add/extend tests so merge samples assert presence of:
1. `files_scanned`
2. `files_pruned`
3. `scan_time_ms`
4. `rewrite_time_ms`
- Add/extend optimize case assertions for:
1. `files_scanned`
2. `files_pruned`
- Keep normalization checks that legacy metrics are still populated.

**Step 2: Run test to verify it fails**
- Run:
1. `cargo test -p delta-bench merge_samples_include_merge_scan_and_rewrite_metrics -- --nocapture`
2. `cargo test -p delta-bench generated_fixtures_support_optimize_vacuum_suite -- --nocapture`
3. `cargo test -p delta-bench metrics_normalization -- --nocapture`
- Expected: FAIL because merge/optimize do not yet map these metric fields.

**Step 3: Write minimal implementation**
- Map `MergeMetrics` outputs into sample metrics fields:
1. `num_target_files_scanned -> files_scanned`
2. `num_target_files_skipped_during_scan -> files_pruned`
3. `scan_time_ms -> scan_time_ms`
4. `rewrite_time_ms -> rewrite_time_ms`
- Map optimize outputs into sample metrics fields:
1. `total_considered_files -> files_scanned`
2. `total_files_skipped -> files_pruned`
- Leave unsupported values as `None` (for example bytes scanned for merge/optimize).

**Step 4: Run test to verify it passes**
- Run the three commands from Step 2.
- Expected: PASS.

**Step 5: Commit**
```bash
git add crates/delta-bench/src/suites/merge_dml.rs crates/delta-bench/src/suites/optimize_vacuum.rs crates/delta-bench/tests/metrics_normalization.rs crates/delta-bench/tests/real_ops.rs
git commit -m "feat: map merge and optimize metrics into benchmark samples"
```

### Task 4: Optional compare-table metric columns

**Files:**
- Modify: `python/delta_bench_compare/model.py`
- Modify: `python/delta_bench_compare/compare.py`
- Modify: `python/delta_bench_compare/formatting.py`
- Modify: `python/tests/test_compare.py`

**Step 1: Write the failing test**
- Add/extend Python tests that verify:
1. default render path does not show metric columns
2. `include_metrics=True` shows baseline/candidate columns for all five new metrics
3. markdown and text formats both honor the flag

**Step 2: Run test to verify it fails**
- Run: `cd python && python3 -m pytest -q tests/test_compare.py -k metrics`
- Expected: FAIL because compare model/rendering do not yet carry the new metric snapshots.

**Step 3: Write minimal implementation**
- Extend compare model with typed metric snapshot fields.
- Parse best-sample metrics from JSON results into the model.
- Add CLI flag plumbing (`--include-metrics`) and conditional columns in text + markdown renderers.
- Keep default output unchanged when flag is not provided.

**Step 4: Run test to verify it passes**
- Run: `cd python && python3 -m pytest -q tests/test_compare.py -k metrics`
- Expected: PASS.

**Step 5: Commit**
```bash
git add python/delta_bench_compare/model.py python/delta_bench_compare/compare.py python/delta_bench_compare/formatting.py python/tests/test_compare.py
git commit -m "feat: add optional metric columns to benchmark compare output"
```

### Task 5: Docs + verification

**Files:**
- Modify: `README.md`
- Modify: `docs/architecture.md`
- Modify: `docs/adr/0002-result-schema-v1.md`

**Step 1: Write the failing doc check**
- Add/update a short checklist in your notes for required doc updates:
1. list new metric fields in result schema docs
2. describe metric collection sources (read scan, merge, optimize)
3. document `--include-metrics` compare flag and expected output behavior

**Step 2: Implement minimal docs updates**
- Update the three docs with exact metric field names and examples.
- Keep wording explicit that compare metric columns are optional and non-gating.

**Step 3: Run verification**
- Run:
1. `cargo fmt --all -- --check`
2. `cargo clippy -p delta-bench --all-targets -- -D warnings`
3. `cargo test -p delta-bench`
4. `cd python && python3 -m pytest -q`

**Step 4: Commit**
```bash
git add README.md docs/architecture.md docs/adr/0002-result-schema-v1.md
git commit -m "docs: document benchmark metric plumbing and compare metric flag"
```

### Task 6: End-to-end smoke evidence

**Files:** N/A

**Step 1: Run a quick smoke benchmark + compare render**
- Run:
1. `cargo run -p delta-bench -- run --suite read_scan --scale sf1 --iterations 1 --warmup 0 --label smoke-metrics`
2. `python3 -m delta_bench_compare.compare <baseline.json> <candidate.json> --format markdown --include-metrics`
- Expected: benchmark JSON includes new metric keys where available, and compare output includes metric columns only with the flag.

**Step 2: Final commit for smoke artifacts only if intentionally tracked**
- If no artifacts are tracked in git, skip commit.
- If test fixtures/snapshots are intentionally added, commit with a focused message.
