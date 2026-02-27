# Security Feedback Execution Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Resolve the review-blocking issues in security/fidelity scripts and docs with regression tests that prove behavior.

**Architecture:** Treat shell scripts as contract-bearing components. Add Python-based black-box tests that execute scripts with controlled temp fixtures and mocked commands. Then patch scripts to make the contracts explicit: egress hash must be compared against active ruleset hash input, mode transitions must fail closed on unit-control errors, and docs must reflect actual CLI options.

**Tech Stack:** Bash, Python `pytest`, existing Rust workspace for full-project verification.

---

### Task 1: Add failing tests for egress hash contract

**Files:**
- Create: `python/tests/test_security_scripts.py`
- Verify against: `scripts/security_check.sh`

**Step 1: Write failing test**
- Add test that invokes `security_check.sh --require-egress-policy` with mocked `nft` and without expected hash.
- Assert script exits non-zero and emits guidance requiring an explicit active ruleset hash input.

**Step 2: Run test to verify it fails**
- Run: `cd python && python3 -m pytest -q tests/test_security_scripts.py::test_security_check_requires_explicit_expected_egress_hash`
- Expected: FAIL against current implementation.

### Task 2: Add failing tests for mode transition fail-closed behavior

**Files:**
- Modify: `python/tests/test_security_scripts.py`
- Verify against: `scripts/security_mode.sh`

**Step 1: Write failing test**
- Add script-structure tests that fail if `security_mode.sh` suppresses `systemctl` errors via `|| true`.
- Add script-structure tests that fail if `security_mode.sh` has no lock-based transition coordination.

**Step 2: Run test to verify it fails**
- Run: `cd python && python3 -m pytest -q tests/test_security_scripts.py`
- Expected: FAIL against current implementation.

### Task 3: Implement script and docs changes

**Files:**
- Modify: `scripts/security_check.sh`
- Modify: `scripts/security_mode.sh`
- Modify: `README.md`
- Modify: `docs/security-vultr-wave1.md` (wording accuracy around approvals)

**Step 1: Implement minimal code**
- Remove fallback behavior that derives expected hash from policy file bytes in `security_check.sh`.
- Keep comparison as active-ruleset-hash vs explicitly supplied expected hash.
- Add actionable error guidance to generate expected hash from active ruleset command.
- Add lock-based serialization for mode transitions and fail-closed unit operation handling in `security_mode.sh`.
- Update docs to include `--remote-root` and clarify approval semantics.

**Step 2: Run tests**
- Run new targeted pytest tests.
- Expected: PASS.

### Task 4: Full verification

**Files:** N/A

**Step 1: Run verification commands**
- `cargo fmt --all -- --check`
- `cargo clippy -p delta-bench --all-targets -- -D warnings`
- `cargo test -p delta-bench`
- `cd python && python3 -m pytest -q`
- `for f in scripts/*.sh; do bash -n "$f"; done`

**Step 2: Report evidence-based status**
- Summarize each command result and remaining gaps.
