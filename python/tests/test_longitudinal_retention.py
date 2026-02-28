from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone
from pathlib import Path

import pytest

from delta_bench_longitudinal.retention import prune_artifacts, prune_store
from delta_bench_longitudinal.store import fcntl, store_lock


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


def _write_store(store_dir: Path) -> None:
    store_dir.mkdir(parents=True, exist_ok=True)
    rows = [
        {
            "run_id": "run-1",
            "benchmark_created_at": "2026-01-01T00:00:00+00:00",
            "ingested_at": "2026-01-01T01:00:00+00:00",
            "case": "a",
        },
        {
            "run_id": "run-2",
            "benchmark_created_at": "2026-02-01T00:00:00+00:00",
            "ingested_at": "2026-02-01T01:00:00+00:00",
            "case": "a",
        },
        {
            "run_id": "run-3",
            "benchmark_created_at": "2026-03-01T00:00:00+00:00",
            "ingested_at": "2026-03-01T01:00:00+00:00",
            "case": "a",
        },
    ]
    with (store_dir / "rows.jsonl").open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row))
            fh.write("\n")
    (store_dir / "index.json").write_text(
        json.dumps({"schema_version": 1, "run_ids": ["run-1", "run-2", "run-3"]}),
        encoding="utf-8",
    )


def test_prune_store_dry_run(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    _write_store(store_dir)
    summary = prune_store(
        store_dir=store_dir,
        max_age_days=None,
        max_runs=2,
        apply=False,
        now=datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc),
    )
    assert summary["candidate_runs"] == ["run-1"]
    assert len((store_dir / "rows.jsonl").read_text(encoding="utf-8").splitlines()) == 3


def test_prune_store_apply_rewrites_rows_and_index(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    _write_store(store_dir)
    summary = prune_store(
        store_dir=store_dir,
        max_age_days=None,
        max_runs=2,
        apply=True,
        now=datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc),
    )
    assert summary["removed_runs"] == 1
    rows = [json.loads(line) for line in (store_dir / "rows.jsonl").read_text(encoding="utf-8").splitlines()]
    assert {row["run_id"] for row in rows} == {"run-2", "run-3"}
    index = json.loads((store_dir / "index.json").read_text(encoding="utf-8"))
    assert index["run_ids"] == ["run-2", "run-3"]


def test_prune_store_skips_invalid_json_rows(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True, exist_ok=True)
    with (store_dir / "rows.jsonl").open("w", encoding="utf-8") as fh:
        fh.write(
            json.dumps(
                {
                    "run_id": "run-1",
                    "benchmark_created_at": "2026-01-01T00:00:00+00:00",
                    "ingested_at": "2026-01-01T01:00:00+00:00",
                    "case": "a",
                }
            )
        )
        fh.write("\n")
        fh.write("{invalid json}\n")
        fh.write(
            json.dumps(
                {
                    "run_id": "run-2",
                    "benchmark_created_at": "2026-02-01T00:00:00+00:00",
                    "ingested_at": "2026-02-01T01:00:00+00:00",
                    "case": "a",
                }
            )
        )
        fh.write("\n")
    (store_dir / "index.json").write_text(
        json.dumps({"schema_version": 1, "run_ids": ["run-1", "run-2"]}),
        encoding="utf-8",
    )

    summary = prune_store(
        store_dir=store_dir,
        max_age_days=None,
        max_runs=1,
        apply=False,
        now=datetime(2026, 3, 2, 0, 0, tzinfo=timezone.utc),
    )

    assert summary["candidate_runs"] == ["run-1"]
    assert summary["invalid_rows_skipped"] == 1


def test_prune_store_blocks_when_store_lock_is_held(tmp_path: Path) -> None:
    if fcntl is None:
        pytest.skip("flock unavailable on this platform")

    store_dir = tmp_path / "store"
    _write_store(store_dir)
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
