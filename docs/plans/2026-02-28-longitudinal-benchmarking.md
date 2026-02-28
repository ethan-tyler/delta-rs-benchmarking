# Longitudinal Benchmarking Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build an idempotent, failure-tolerant longitudinal benchmarking pipeline for `delta-rs` revisions (select -> build -> run matrix -> store -> report -> nightly automation).

**Architecture:** Add a dedicated Python execution-plane package (`delta_bench_longitudinal`) with explicit JSON manifests/state files and deterministic artifact/result/report paths. Keep benchmark execution delegated to existing `delta-bench` binaries and scripts; do not add bot-control-plane logic.

**Tech Stack:** Python 3.10 stdlib, existing Rust `delta-bench` binary, Bash wrappers, GitHub Actions.

---

### Task 1: Revision Selection Manifest

**Files:**
- Create: `python/delta_bench_longitudinal/revisions.py`
- Create: `python/tests/test_longitudinal_revisions.py`
- Modify: `python/pyproject.toml`

**Step 1: Write failing tests**
- Add tests for:
  - release-tag selection
  - date-window selection
  - one-commit-per-day selection
  - manifest round-trip serialize/deserialize
  - invalid input validation (bad dates, unknown strategy)

**Step 2: Run tests to verify failures**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_revisions.py`
- Expected: FAIL due to missing module/functions.

**Step 3: Implement minimal revision selector**
- Add git-log/tag parsing helpers using `subprocess`.
- Emit deterministic manifest JSON with commit SHA + timestamp + selection source metadata.

**Step 4: Re-run targeted tests**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_revisions.py`
- Expected: PASS.

**Step 5: Commit**
- `git add python/tests/test_longitudinal_revisions.py python/delta_bench_longitudinal/revisions.py python/pyproject.toml`
- `git commit -m "feat: add longitudinal revision selection manifest"`

### Task 2: Artifact Build Pipeline + Metadata

**Files:**
- Create: `python/delta_bench_longitudinal/artifacts.py`
- Create: `python/tests/test_longitudinal_artifacts.py`

**Step 1: Write failing tests**
- Add tests for deterministic artifact path naming and metadata read/write.
- Add tests for success/failure metadata status and toolchain capture.

**Step 2: Run tests to verify failures**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_artifacts.py`
- Expected: FAIL due to missing implementation.

**Step 3: Implement artifact manager**
- Build per revision (checkout + cargo build release + copy binary).
- Write metadata JSON (revision, commit timestamp, build timestamp, toolchain, status, error).
- Skip rebuild when successful artifact already exists.

**Step 4: Re-run targeted tests**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_artifacts.py`
- Expected: PASS.

**Step 5: Commit**
- `git add python/tests/test_longitudinal_artifacts.py python/delta_bench_longitudinal/artifacts.py`
- `git commit -m "feat: add longitudinal artifact build metadata pipeline"`

### Task 3: Matrix Runner With Resume/Retry/Timeout

**Files:**
- Create: `python/delta_bench_longitudinal/matrix_runner.py`
- Create: `python/tests/test_longitudinal_matrix_runner.py`

**Step 1: Write failing tests**
- Add tests for:
  - resume from partial state
  - skipping already successful matrix cells
  - bounded retry behavior
  - timeout handling + persisted failure reason

**Step 2: Run tests to verify failures**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_matrix_runner.py`
- Expected: FAIL due to missing implementation.

**Step 3: Implement matrix runner**
- State file tracks per `(revision, suite, scale)` case with attempt counts and final status.
- Support `max_retries`, per-case timeout, and idempotent skip semantics.

**Step 4: Re-run targeted tests**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_matrix_runner.py`
- Expected: PASS.

**Step 5: Commit**
- `git add python/tests/test_longitudinal_matrix_runner.py python/delta_bench_longitudinal/matrix_runner.py`
- `git commit -m "feat: add resumable longitudinal matrix runner"`

### Task 4: Longitudinal Normalized Result Store

**Files:**
- Create: `python/delta_bench_longitudinal/store.py`
- Create: `python/tests/test_longitudinal_store.py`

**Step 1: Write failing tests**
- Add tests for append-safe JSONL writes, dedupe behavior, and normalized row fields.

**Step 2: Run tests to verify failures**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_store.py`
- Expected: FAIL due to missing implementation.

**Step 3: Implement store**
- Normalize benchmark result JSON into per-case rows with reproducibility metadata.
- Persist append-safe row log and ingest index for idempotency.

**Step 4: Re-run targeted tests**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_store.py`
- Expected: PASS.

**Step 5: Commit**
- `git add python/tests/test_longitudinal_store.py python/delta_bench_longitudinal/store.py`
- `git commit -m "feat: add append-safe longitudinal result store"`

### Task 5: Trend Reporting (Markdown + HTML)

**Files:**
- Create: `python/delta_bench_longitudinal/reporting.py`
- Create: `python/tests/test_longitudinal_reporting.py`

**Step 1: Write failing tests**
- Add fixture-driven tests for markdown summary, HTML trend output, and regression highlight windows.

**Step 2: Run tests to verify failures**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_reporting.py`
- Expected: FAIL due to missing implementation.

**Step 3: Implement reporting**
- Compute trend series and baseline-window comparisons.
- Emit CI markdown summary + HTML report with inline SVG trend charts.

**Step 4: Re-run targeted tests**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_reporting.py`
- Expected: PASS.

**Step 5: Commit**
- `git add python/tests/test_longitudinal_reporting.py python/delta_bench_longitudinal/reporting.py`
- `git commit -m "feat: add longitudinal trend report generation"`

### Task 6: Orchestrator CLI + Shell Entrypoint + E2E Smoke

**Files:**
- Create: `python/delta_bench_longitudinal/cli.py`
- Create: `python/delta_bench_longitudinal/__init__.py`
- Create: `scripts/longitudinal_bench.sh`
- Create: `python/tests/test_longitudinal_e2e_smoke.py`

**Step 1: Write failing tests**
- Add end-to-end smoke test with stub builder/runner that validates build->run->store->report flow.

**Step 2: Run tests to verify failures**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_e2e_smoke.py`
- Expected: FAIL due to missing orchestration entrypoint.

**Step 3: Implement CLI + orchestration**
- Wire subcommands:
  - `select-revisions`
  - `build-artifacts`
  - `run-matrix`
  - `ingest-results`
  - `report`
  - `orchestrate` (all steps)
- Add shell wrapper for repo-root execution.

**Step 4: Re-run targeted tests**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_e2e_smoke.py`
- Expected: PASS.

**Step 5: Commit**
- `git add python/delta_bench_longitudinal python/tests/test_longitudinal_e2e_smoke.py scripts/longitudinal_bench.sh`
- `git commit -m "feat: add longitudinal orchestration cli and smoke coverage"`

### Task 7: Scheduled Nightly Workflow

**Files:**
- Create: `.github/workflows/longitudinal-nightly.yml`
- Create: `python/tests/test_longitudinal_workflow.py`

**Step 1: Write failing tests**
- Add tests validating cron schedule and required pipeline step commands.

**Step 2: Run tests to verify failures**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_workflow.py`
- Expected: FAIL until workflow exists.

**Step 3: Implement workflow**
- Nightly + manual dispatch.
- Select revisions, build missing artifacts, run matrix, ingest, generate reports, upload artifacts.
- Keep control-plane authorization logic out.

**Step 4: Re-run targeted tests**
- Run: `PYTHONPATH=python python3 -m pytest -q python/tests/test_longitudinal_workflow.py`
- Expected: PASS.

**Step 5: Commit**
- `git add .github/workflows/longitudinal-nightly.yml python/tests/test_longitudinal_workflow.py`
- `git commit -m "ci: add nightly longitudinal benchmark workflow"`

### Task 8: Docs + Final Verification

**Files:**
- Modify: `README.md`
- Create: `docs/longitudinal-runbook.md`

**Step 1: Write/update doc assertions test (if needed)**
- Add lightweight python regex assertions in existing workflow/doc test module where useful.

**Step 2: Implement docs**
- Add README section: Longitudinal Benchmarking, CLI usage, directory layout.
- Add runbook: nightly operations + failure recovery.

**Step 3: Run full verification**
- `cargo fmt --all --check`
- `cargo test -p delta-bench --tests`
- `PYTHONPATH=python python3 -m pytest -q python/tests`
- `for f in scripts/*.sh; do bash -n "$f"; done`

**Step 4: Commit**
- `git add README.md docs/longitudinal-runbook.md python/tests`
- `git commit -m "docs: add longitudinal benchmarking usage and runbook"`
