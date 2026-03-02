from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPARE_BRANCH = REPO_ROOT / "scripts" / "compare_branch.sh"
PREPARE_DELTA_RS = REPO_ROOT / "scripts" / "prepare_delta_rs.sh"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark.yml"
NIGHTLY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark-nightly.yml"
PRERELEASE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark-prerelease.yml"


def test_compare_branch_sanitizes_branch_labels_for_cli() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "sanitize_label" in script
    assert re.search(
        r"base_label=\"base-\$\(sanitize_label \"\$\{base_ref\}\"\)\"", script
    )
    assert re.search(
        r"cand_label=\"cand-\$\(sanitize_label \"\$\{candidate_ref\}\"\)\"", script
    )


def test_benchmark_workflow_defines_job_timeout() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert re.search(r"^\s*timeout-minutes:\s*\d+", workflow, flags=re.MULTILINE)


def test_benchmark_workflow_enforces_suite_allowlist() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "allowedSuites" in workflow
    assert "optimize_vacuum" in workflow
    assert "invalid command" in workflow


def test_benchmark_workflow_uses_sha_pins_for_compare_refs() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert re.search(
        r"env:\n\s+BASE_SHA:\s+\$\{\{ steps\.pr\.outputs\.base_sha \}\}", workflow
    )
    assert re.search(
        r"\s+HEAD_SHA:\s+\$\{\{ steps\.pr\.outputs\.head_sha \}\}", workflow
    )
    assert re.search(r"\s+SUITE:\s+\$\{\{ steps\.parse\.outputs\.suite \}\}", workflow)
    assert '"$BASE_SHA"' in workflow
    assert '"$HEAD_SHA"' in workflow
    assert '"$SUITE"' in workflow


def test_compare_branch_supports_storage_backend_passthrough() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--storage-backend <local|s3>" in script
    assert "--storage-option <KEY=VALUE>" in script
    assert re.search(
        r"storage_args=\(--storage-backend \"\$\{STORAGE_BACKEND\}\"\)", script
    )
    assert re.search(r"storage_args\+=\(--storage-option \"\$\{option\}\"\)", script)
    assert re.search(r"\./scripts/bench\.sh data .*\"\$\{storage_args\[@\]\}\"", script)
    assert re.search(r"\./scripts/bench\.sh run .*\"\$\{storage_args\[@\]\}\"", script)


def test_compare_branch_supports_aggregation_passthrough() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--aggregation <min|median|p95>" in script
    assert re.search(r"AGGREGATION=\"\$\{BENCH_AGGREGATION:-median\}\"", script)
    assert re.search(
        r"compare_args=\(--noise-threshold \"\$\{NOISE_THRESHOLD\}\" --aggregation \"\$\{AGGREGATION\}\" --format markdown\)",
        script,
    )


def test_compare_branch_supports_explicit_sha_flags() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--base-sha <sha>" in script
    assert "--candidate-sha <sha>" in script
    assert re.search(r"--base-sha\)\n\s+BASE_SHA_OVERRIDE=\"\$2\"", script)
    assert re.search(r"--candidate-sha\)\n\s+CANDIDATE_SHA_OVERRIDE=\"\$2\"", script)
    assert re.search(
        r"prepare_delta_rs_ref \"\$\{base_ref\}\" \"\$\{base_ref_mode\}\"", script
    )
    assert re.search(
        r"prepare_delta_rs_ref \"\$\{candidate_ref\}\" \"\$\{candidate_ref_mode\}\"",
        script,
    )
    assert re.search(r"if \[\[ \"\$\{mode\}\" == \"commit\" \]\]; then", script)


def test_compare_branch_prefers_existing_branch_refs_before_sha_fallback() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "branch_ref_exists" in script
    assert re.search(r"show-ref --verify --quiet \"refs/heads/\$\{ref\}\"", script)
    assert re.search(
        r"show-ref --verify --quiet \"refs/remotes/origin/\$\{ref\}\"", script
    )
    assert re.search(
        r"if branch_ref_exists \"\$\{ref\}\"; then\s+run_step env DELTA_RS_BRANCH=",
        script,
        flags=re.DOTALL,
    )
    assert re.search(
        r"if is_commit_sha \"\$\{ref\}\"; then\s+run_step env DELTA_RS_REF=",
        script,
        flags=re.DOTALL,
    )


def test_prepare_delta_rs_supports_immutable_ref_checkout() -> None:
    script = PREPARE_DELTA_RS.read_text(encoding="utf-8")
    assert "DELTA_RS_REF" in script
    assert "DELTA_RS_REF_TYPE" in script
    assert "checkout --detach" in script
    assert "pull --ff-only origin" in script


def test_benchmark_workflow_accepts_optional_storage_configuration() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "BENCH_STORAGE_BACKEND" in workflow
    assert "BENCH_STORAGE_OPTIONS" in workflow
    assert "storage_args=()" in workflow
    assert re.search(
        r"storage_args\+=\(--storage-backend \"\$\{BENCH_STORAGE_BACKEND\}\"\)",
        workflow,
    )
    assert re.search(r"storage_args\+=\(--storage-option \"\$\{opt\}\"\)", workflow)
    assert re.search(
        r"\./scripts/compare_branch\.sh \\\n(?:.*\n)*\s+\"\$\{storage_args\[@\]\}\" \\\n(?:.*\n)*\s+--base-sha \"\$BASE_SHA\" \\\n(?:.*\n)*\s+--candidate-sha \"\$HEAD_SHA\"",
        workflow,
    )


def test_compare_branch_does_not_expose_ci_policy_flags() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--ci" not in script
    assert "--max-allowed-regressions" not in script


def test_non_pr_workflows_do_not_use_advisory_mode_wording() -> None:
    nightly = NIGHTLY_WORKFLOW.read_text(encoding="utf-8")
    prerelease = PRERELEASE_WORKFLOW.read_text(encoding="utf-8")
    assert "advisory" not in nightly.lower()
    assert "advisory" not in prerelease.lower()
