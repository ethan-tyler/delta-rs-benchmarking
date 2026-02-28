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


def test_benchmark_workflow_restricts_benchmark_runs_to_same_repo_prs() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "head.repo?.full_name" in workflow
    assert "same_repo" in workflow
    assert "not allowed from fork PRs" in workflow


def test_compare_branch_supports_storage_backend_passthrough() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--storage-backend <local|s3|gcs|azure>" in script
    assert "--storage-option <KEY=VALUE>" in script
    assert re.search(r"storage_args=\(--storage-backend \"\$\{STORAGE_BACKEND\}\"\)", script)
    assert re.search(r"storage_args\+=\(--storage-option \"\$\{option\}\"\)", script)
    assert re.search(r"\./scripts/bench\.sh data .*\"\$\{storage_args\[@\]\}\"", script)
    assert re.search(r"\./scripts/bench\.sh run .*\"\$\{storage_args\[@\]\}\"", script)


def test_benchmark_workflow_accepts_optional_storage_configuration() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "BENCH_STORAGE_BACKEND" in workflow
    assert "BENCH_STORAGE_OPTIONS" in workflow
    assert "storage_args=()" in workflow
    assert re.search(r"storage_args\+=\(--storage-backend \"\$\{BENCH_STORAGE_BACKEND\}\"\)", workflow)
    assert re.search(r"storage_args\+=\(--storage-option \"\$\{opt\}\"\)", workflow)
    assert re.search(r"\./scripts/compare_branch\.sh \\\n(?:.*\n)*\s+\"\$\{storage_args\[@\]\}\" \\\n(?:.*\n)*\s+\"\$BASE_REF\"", workflow)


def test_compare_branch_does_not_offer_ci_blocking_flags() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--ci" not in script
    assert "--max-allowed-regressions" not in script
    assert "CI policy" not in script


def test_benchmark_workflow_is_advisory_only() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "--ci" not in workflow
    assert "--max-allowed-regressions" not in workflow
    assert "Fail workflow on CI regression policy violation" not in workflow
