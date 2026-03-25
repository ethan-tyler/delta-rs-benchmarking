from __future__ import annotations

import re
from pathlib import Path

import pytest

from delta_bench_longitudinal import cli as longitudinal_cli
from delta_bench_longitudinal.matrix_runner import save_matrix_state


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


def test_run_matrix_cli_rejects_state_with_different_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_path = tmp_path / "matrix_state.json"
    fixtures_dir = tmp_path / "fixtures"
    results_dir = tmp_path / "results"
    save_matrix_state(
        state_path,
        {
            "schema_version": 1,
            "config": {
                "suites": ["read_scan"],
                "scales": ["sf1"],
                "warmup": 1,
                "iterations": 5,
                "fixtures_dir": str(fixtures_dir),
                "results_dir": str(results_dir),
                "label_prefix": "old-prefix",
            },
            "cases": {},
        },
    )
    monkeypatch.setattr(
        longitudinal_cli,
        "_load_manifest_artifacts",
        lambda *_args, **_kwargs: ([], {}),
    )

    with pytest.raises(
        ValueError, match="state file was created with different config"
    ):
        longitudinal_cli.main(
            [
                "run-matrix",
                "--manifest",
                str(tmp_path / "manifest.json"),
                "--artifacts-dir",
                str(tmp_path / "artifacts"),
                "--state-path",
                str(state_path),
                "--results-dir",
                str(results_dir),
                "--fixtures-dir",
                str(fixtures_dir),
                "--suite",
                "read_scan",
                "--scale",
                "sf1",
                "--label-prefix",
                "new-prefix",
            ]
        )
