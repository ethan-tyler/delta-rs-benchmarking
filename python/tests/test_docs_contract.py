from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
README = REPO_ROOT / "README.md"
DOCS_DIR = REPO_ROOT / "docs"
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"
DOC_FILES = [README, *sorted(DOCS_DIR.glob("*.md"))]
DOCS_CHECK = REPO_ROOT / "scripts" / "docs_check.sh"

# Pattern intentionally simple because repository docs avoid nested markdown edge-cases.
LINK_PATTERN = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
HEADING_PATTERN = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)


REQUIRED_SECTIONS: dict[str, list[str]] = {
    "README.md": [
        "## Quick Start",
        "## What You Can Do",
    ],
    "docs/getting-started.md": [
        "## How the Harness Works",
        "## Prerequisites",
        "## Your First Benchmark Run",
        "## Choosing a Dataset",
        "## Choosing a Backend",
        "## Cleanup",
        "## Troubleshooting",
    ],
    "docs/comparing-branches.md": [
        "## When to Use Branch Comparison",
        "## Comparison Methods",
        "## What Compare Does Under the Hood",
        "## Tuning Your Comparison",
        "## Reliable Comparison Protocol",
        "## Reading the Report",
    ],
    "docs/longitudinal.md": [
        "## When to Use Longitudinal Benchmarking",
        "## Prerequisites",
        "## Pipeline Stages Overview",
        "## Automated Workflows",
        "## Failure Recovery",
    ],
    "docs/reference.md": [
        "## Glossary",
        "## Benchmark Suites and Cases",
        "## Metrics Reference",
        "## CLI Commands and Flags",
        "## Environment Variables",
        "## Datasets and Scales",
        "## Result Schema v3",
    ],
    "docs/architecture.md": [
        "## Key Concepts",
        "## Components",
        "## Data Flow",
        "## Reproducibility Controls",
    ],
    "docs/cloud-runner.md": [
        "## When to Use a Cloud Runner",
        "## Setting Up Run Mode",
        "## Preflight Enforcement on Compare",
        "## Provisioning Controls",
    ],
}


def _link_targets(markdown: str) -> list[str]:
    return [match.group(1).strip() for match in LINK_PATTERN.finditer(markdown)]


def _is_relative_link(target: str) -> bool:
    if not target:
        return False
    lower = target.lower()
    if lower.startswith(("http://", "https://", "mailto:", "tel:")):
        return False
    if target.startswith("#"):
        return False
    return True


def _assert_relative_link_resolves(source_file: Path, link_target: str) -> None:
    relative_part = link_target.split("#", maxsplit=1)[0]
    if not relative_part:
        return
    resolved = (source_file.parent / relative_part).resolve()
    assert resolved.exists(), f"broken relative link in {source_file}: {link_target}"


def _self_hosted_benchmark_workflow_names() -> list[str]:
    workflow_names = []
    for workflow_path in sorted(WORKFLOWS_DIR.glob("*.yml")):
        workflow = workflow_path.read_text(encoding="utf-8")
        if "runs-on: [self-hosted, delta-bench]" not in workflow:
            continue
        if "./scripts/" not in workflow:
            continue
        workflow_names.append(workflow_path.name)
    return workflow_names


def test_docs_check_entrypoint_exists_and_is_executable() -> None:
    assert DOCS_CHECK.exists(), "missing scripts/docs_check.sh"
    assert DOCS_CHECK.stat().st_mode & 0o111, "scripts/docs_check.sh must be executable"


def test_markdown_links_resolve_for_repo_docs() -> None:
    for doc_file in DOC_FILES:
        markdown = doc_file.read_text(encoding="utf-8")
        for target in _link_targets(markdown):
            if _is_relative_link(target):
                _assert_relative_link_resolves(doc_file, target)


def test_each_doc_has_single_h1() -> None:
    for doc_file in DOC_FILES:
        markdown = doc_file.read_text(encoding="utf-8")
        headings = HEADING_PATTERN.findall(markdown)
        h1_count = sum(1 for hashes, _ in headings if len(hashes) == 1)
        assert h1_count == 1, f"{doc_file} must have exactly one H1"


def test_heading_levels_do_not_jump() -> None:
    for doc_file in DOC_FILES:
        markdown = doc_file.read_text(encoding="utf-8")
        headings = HEADING_PATTERN.findall(markdown)
        prev_level = 0
        for hashes, text in headings:
            level = len(hashes)
            if prev_level != 0:
                assert (
                    level <= prev_level + 1
                ), f"heading level jumps in {doc_file} at heading: {text}"
            prev_level = level


def test_core_docs_include_required_sections() -> None:
    for rel_path, required_sections in REQUIRED_SECTIONS.items():
        file_path = REPO_ROOT / rel_path
        markdown = file_path.read_text(encoding="utf-8")
        for section in required_sections:
            assert section in markdown, f"{rel_path} missing section: {section}"


def test_longitudinal_state_path_uses_matrix_state_json_consistently() -> None:
    markdown_files = [README, *sorted(DOCS_DIR.glob("*.md"))]
    for file_path in markdown_files:
        content = file_path.read_text(encoding="utf-8")
        assert "matrix.json" not in content, f"{file_path} still references matrix.json"


def test_reference_docs_cover_concurrency_suite_and_contention_metrics() -> None:
    markdown = (DOCS_DIR / "reference.md").read_text(encoding="utf-8")

    assert "### concurrency (5 cases)" in markdown
    for case_name in (
        "concurrent_table_create",
        "concurrent_append_multi",
        "update_vs_compaction",
        "delete_vs_compaction",
        "optimize_vs_optimize_overlap",
    ):
        assert f"`{case_name}`" in markdown

    assert "### Contention metrics" in markdown
    for metric_name in (
        "worker_count",
        "race_count",
        "ops_attempted",
        "ops_succeeded",
        "ops_failed",
        "conflict_append",
        "conflict_delete_read",
        "conflict_delete_delete",
        "conflict_metadata_changed",
        "conflict_protocol_changed",
        "conflict_transaction",
        "version_already_exists",
        "max_commit_attempts_exceeded",
        "other_errors",
    ):
        assert f"`{metric_name}`" in markdown
    assert "table_version: null" in markdown


def test_compare_docs_cover_concurrency_guidance() -> None:
    markdown = (DOCS_DIR / "comparing-branches.md").read_text(encoding="utf-8")

    assert "### Concurrency suite guidance" in markdown
    for flag in (
        "--compare-runs 5",
        "--noise-threshold 0.10",
        "--aggregation median",
        "--measure-order alternate",
    ):
        assert flag in markdown

    assert "concurrent_table_create" in markdown
    assert "concurrent_append_multi" in markdown
    assert "update_vs_compaction" in markdown
    assert "delete_vs_compaction" in markdown
    assert "optimize_vs_optimize_overlap" in markdown
    assert "conflict_delete_read" in markdown
    assert "conflict_delete_delete" in markdown
    assert "table_version" in markdown
    assert "leave it null" in markdown


def test_cloud_runner_docs_cover_enforced_workflows_and_required_env() -> None:
    markdown = (DOCS_DIR / "cloud-runner.md").read_text(encoding="utf-8")
    workflow_names = _self_hosted_benchmark_workflow_names()
    assert workflow_names, "expected at least one self-hosted benchmark workflow"
    for workflow_name in workflow_names:
        assert workflow_name in markdown

    for env_var in (
        "DELTA_BENCH_EGRESS_POLICY_SHA256",
        "BENCH_RUNNER_MODE",
        "BENCH_STORAGE_BACKEND",
        "BENCH_STORAGE_OPTIONS",
        "BENCH_BACKEND_PROFILE",
    ):
        assert env_var in markdown

    assert (
        "./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy"
        in markdown
    )
