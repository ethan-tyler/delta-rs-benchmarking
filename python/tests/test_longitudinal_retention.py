from __future__ import annotations

import json
import threading
import time
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path

import pytest

from delta_bench_longitudinal.retention import prune_artifacts, prune_store
from delta_bench_longitudinal.store import (
    _connect_store,
    fcntl,
    load_longitudinal_rows,
    store_db_path,
    store_lock,
)


def _write_artifact_metadata(base: Path, revision: str, build_timestamp: str) -> None:
    rev_dir = base / revision
    rev_dir.mkdir(parents=True, exist_ok=True)
    metadata = {
        "revision": revision,
        "commit_timestamp": "2026-01-01T00:00:00+00:00",
        "build_timestamp": build_timestamp,
        "rust_toolchain": "stable",
        "status": "success",
        "artifact_path": str(rev_dir / f"delta-bench-{revision}"),
        "error": None,
    }
    (rev_dir / "metadata.json").write_text(json.dumps(metadata), encoding="utf-8")


def _seed_store_runs(store_dir: Path) -> None:
    with closing(_connect_store(store_dir)) as conn:
        with conn:
            for run_id, created_at in (
                ("run-1", "2026-01-01T00:00:00+00:00"),
                ("run-2", "2026-02-01T00:00:00+00:00"),
                ("run-3", "2026-03-01T00:00:00+00:00"),
            ):
                conn.execute(
                    """
                    INSERT INTO runs (
                        run_id,
                        schema_version,
                        ingested_at,
                        revision,
                        revision_commit_timestamp,
                        benchmark_created_at,
                        label,
                        git_sha,
                        host,
                        suite,
                        scale,
                        iterations,
                        warmup,
                        source_result_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        1,
                        created_at,
                        run_id,
                        created_at,
                        created_at,
                        f"longitudinal-{run_id}",
                        run_id,
                        "bench-host",
                        "read_scan",
                        "sf1",
                        1,
                        0,
                        str(store_dir / f"{run_id}.json"),
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO case_rows (
                        run_id,
                        case_name,
                        success,
                        failure_reason,
                        sample_count,
                        sample_values_json,
                        best_ms,
                        min_ms,
                        max_ms,
                        mean_ms,
                        median_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        run_id,
                        "scan_all",
                        1,
                        None,
                        1,
                        "[100.0]",
                        100.0,
                        100.0,
                        100.0,
                        100.0,
                        100.0,
                    ),
                )


def test_prune_artifacts_dry_run_keeps_files(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    _write_artifact_metadata(artifacts, "rev-old", "2025-12-01T00:00:00+00:00")
    _write_artifact_metadata(artifacts, "rev-new", "2026-02-20T00:00:00+00:00")

    summary = prune_artifacts(
        artifacts_dir=artifacts,
        max_age_days=30,
        max_artifacts=None,
        apply=False,
        now=datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc),
    )
    assert "rev-old" in summary["candidates"]
    assert (artifacts / "rev-old").exists()


def test_prune_artifacts_apply_enforces_count_limit(tmp_path: Path) -> None:
    artifacts = tmp_path / "artifacts"
    _write_artifact_metadata(artifacts, "rev-1", "2026-01-01T00:00:00+00:00")
    _write_artifact_metadata(artifacts, "rev-2", "2026-02-01T00:00:00+00:00")
    _write_artifact_metadata(artifacts, "rev-3", "2026-03-01T00:00:00+00:00")

    summary = prune_artifacts(
        artifacts_dir=artifacts,
        max_age_days=None,
        max_artifacts=1,
        apply=True,
        now=datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc),
    )
    assert summary["removed"] == 2
    assert (artifacts / "rev-3").exists()
    assert not (artifacts / "rev-1").exists()
    assert not (artifacts / "rev-2").exists()


def test_prune_store_dry_run(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    _seed_store_runs(store_dir)
    summary = prune_store(
        store_dir=store_dir,
        max_age_days=None,
        max_runs=2,
        apply=False,
        now=datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc),
    )
    assert summary["candidate_runs"] == ["run-1"]
    assert store_db_path(store_dir).exists()
    assert len(load_longitudinal_rows(store_dir)) == 3


def test_prune_store_apply_deletes_runs_without_rows_rewrite(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    _seed_store_runs(store_dir)
    summary = prune_store(
        store_dir=store_dir,
        max_age_days=None,
        max_runs=2,
        apply=True,
        now=datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc),
    )
    assert summary["removed_runs"] == 1
    rows = load_longitudinal_rows(store_dir)
    assert {row["run_id"] for row in rows} == {"run-2", "run-3"}
    assert not (store_dir / "rows.jsonl").exists()
    assert not (store_dir / "index.json").exists()


def test_prune_store_blocks_when_store_lock_is_held(tmp_path: Path) -> None:
    if fcntl is None:
        pytest.skip("flock unavailable on this platform")

    store_dir = tmp_path / "store"
    _seed_store_runs(store_dir)
    done = threading.Event()

    def run_prune() -> None:
        prune_store(
            store_dir=store_dir,
            max_age_days=None,
            max_runs=2,
            apply=False,
            now=datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc),
        )
        done.set()

    with store_lock(store_dir):
        worker = threading.Thread(target=run_prune)
        worker.start()
        time.sleep(0.05)
        assert not done.is_set()

    worker.join(timeout=2.0)
    assert done.is_set()
