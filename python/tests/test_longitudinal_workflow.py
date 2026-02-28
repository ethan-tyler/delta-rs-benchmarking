from __future__ import annotations

import re
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "longitudinal-nightly.yml"
WRAPPER = REPO_ROOT / "scripts" / "longitudinal_bench.sh"


def test_workflow_has_nightly_schedule_and_manual_dispatch() -> None:
    content = WORKFLOW.read_text(encoding="utf-8")
    assert "schedule:" in content
    assert re.search(r"cron:\s*'0 3 \* \* \*'", content)
    assert "workflow_dispatch:" in content


def test_workflow_runs_longitudinal_pipeline_commands() -> None:
    content = WORKFLOW.read_text(encoding="utf-8")
    assert "select-revisions" in content
    assert "build-artifacts" in content
    assert "run-matrix" in content
    assert "ingest-results" in content
    assert "report" in content
    assert "prune" in content
    assert "actions/upload-artifact" in content


def test_workflow_uses_parallel_load_and_significance_controls() -> None:
    content = WORKFLOW.read_text(encoding="utf-8")
    assert "LONGITUDINAL_MAX_PARALLEL" in content
    assert "LONGITUDINAL_MAX_LOAD_PER_CPU" in content
    assert "--max-parallel" in content
    assert "--max-load-per-cpu" in content
    assert "--significance-method" in content
    assert "--significance-alpha" in content


def test_wrapper_prepends_repo_pythonpath() -> None:
    content = WRAPPER.read_text(encoding="utf-8")
    assert 'PYTHONPATH_DIR="${ROOT_DIR}/python${PYTHONPATH:+:${PYTHONPATH}}"' in content
