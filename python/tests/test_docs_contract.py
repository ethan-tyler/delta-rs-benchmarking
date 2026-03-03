from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
README = REPO_ROOT / "README.md"
DOCS_DIR = REPO_ROOT / "docs"
DOC_FILES = [README, *sorted(DOCS_DIR.glob("*.md"))]
DOCS_CHECK = REPO_ROOT / "scripts" / "docs_check.sh"

# Pattern intentionally simple because repository docs avoid nested markdown edge-cases.
LINK_PATTERN = re.compile(r"\[[^\]]+\]\(([^)]+)\)")
HEADING_PATTERN = re.compile(r"^(#{1,6})\s+(.+?)\s*$", re.MULTILINE)


REQUIRED_SECTIONS: dict[str, list[str]] = {
    "README.md": [
        "## Start Here",
        "## Contributor Task Router",
        "## Choose Your Workflow",
    ],
    "docs/user-guide.md": [
        "## Prerequisites and Workspace Modes",
        "## First Benchmark Run (Happy Path)",
        "## Compare Workflows",
        "## Backend and Dataset Selection",
        "## Cleanup and Troubleshooting",
        "## Advanced Topics",
    ],
    "docs/longitudinal-cli.md": [
        "## Prerequisites",
        "## Happy-Path Pipeline",
        "## End-to-End Orchestration",
        "## Advanced Controls",
    ],
    "docs/longitudinal-runbook.md": [
        "## Scope",
        "## Nightly and Release Workflows",
        "## Failure Recovery Playbooks",
        "## Manual Recovery Commands",
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
                assert level <= prev_level + 1, (
                    f"heading level jumps in {doc_file} at heading: {text}"
                )
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
