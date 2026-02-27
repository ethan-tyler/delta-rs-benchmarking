from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPARE_BRANCH = REPO_ROOT / "scripts" / "compare_branch.sh"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark.yml"


def test_compare_branch_sanitizes_branch_labels_for_cli() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "sanitize_label" in script
    assert re.search(r"base_label=\"base-\$\(sanitize_label \"\$\{base_branch\}\"\)\"", script)
    assert re.search(r"cand_label=\"cand-\$\(sanitize_label \"\$\{candidate_branch\}\"\)\"", script)


def test_benchmark_workflow_defines_job_timeout() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert re.search(r"^\s*timeout-minutes:\s*\d+", workflow, flags=re.MULTILINE)


def test_benchmark_workflow_enforces_suite_allowlist() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "allowedSuites" in workflow
    assert "optimize_vacuum" in workflow
    assert "invalid command" in workflow


def test_benchmark_workflow_uses_env_vars_for_compare_refs() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert re.search(r"env:\n\s+BASE_REF:\s+\$\{\{ steps\.pr\.outputs\.base_ref \}\}", workflow)
    assert re.search(r"\s+HEAD_REF:\s+\$\{\{ steps\.pr\.outputs\.head_ref \}\}", workflow)
    assert re.search(r"\s+SUITE:\s+\$\{\{ steps\.parse\.outputs\.suite \}\}", workflow)
    assert '"$BASE_REF"' in workflow
    assert '"$HEAD_REF"' in workflow
    assert '"$SUITE"' in workflow
