from __future__ import annotations

import json
import os
from pathlib import Path

from delta_bench_longitudinal.store import ingest_benchmark_result, load_longitudinal_rows


def _result_payload() -> dict:
    return {
        "schema_version": 1,
        "context": {
            "schema_version": 1,
            "label": "longitudinal-rev1",
            "git_sha": "rev1",
            "created_at": "2026-02-01T00:00:00+00:00",
            "host": "bench-host",
            "suite": "read_scan",
            "scale": "sf1",
            "iterations": 3,
            "warmup": 1,
            "run_mode": "run-mode",
            "egress_policy_sha256": "abc",
            "cpu_model": "cpu",
            "kernel": "kernel",
        },
        "cases": [
            {
                "case": "scan_all",
                "success": True,
                "samples": [
                    {"elapsed_ms": 100.0},
                    {"elapsed_ms": 120.0},
                    {"elapsed_ms": 90.0},
                ],
                "failure": None,
            },
            {
                "case": "scan_predicate",
                "success": False,
                "samples": [],
                "failure": {"message": "failed op"},
            },
        ],
    }


def test_ingest_is_append_safe_and_idempotent(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    result_path.write_text(json.dumps(_result_payload()), encoding="utf-8")
    store_dir = tmp_path / "store"

    first = ingest_benchmark_result(
        store_dir=store_dir,
        result_path=result_path,
        revision="rev1",
        commit_timestamp="2026-01-01T00:00:00+00:00",
    )
    second = ingest_benchmark_result(
        store_dir=store_dir,
        result_path=result_path,
        revision="rev1",
        commit_timestamp="2026-01-01T00:00:00+00:00",
    )

    assert first["rows_appended"] == 2
    assert second["rows_appended"] == 0
    assert len(load_longitudinal_rows(store_dir)) == 2


def test_rows_include_reproducibility_metadata(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    result_path.write_text(json.dumps(_result_payload()), encoding="utf-8")
    store_dir = tmp_path / "store"

    ingest_benchmark_result(
        store_dir=store_dir,
        result_path=result_path,
        revision="rev1",
        commit_timestamp="2026-01-01T00:00:00+00:00",
    )
    rows = load_longitudinal_rows(store_dir)
    by_case = {row["case"]: row for row in rows}
    scan_all = by_case["scan_all"]
    failed = by_case["scan_predicate"]

    assert scan_all["revision"] == "rev1"
    assert scan_all["suite"] == "read_scan"
    assert scan_all["scale"] == "sf1"
    assert scan_all["host"] == "bench-host"
    assert scan_all["run_mode"] == "run-mode"
    assert scan_all["median_ms"] == 100.0
    assert scan_all["sample_values_ms"] == [100.0, 120.0, 90.0]
    assert failed["success"] is False
    assert failed["failure_reason"] == "failed op"
    assert failed["sample_values_ms"] == []


def test_ingest_dedupes_identical_payload_from_different_paths(tmp_path: Path) -> None:
    first_path = tmp_path / "first.json"
    second_path = tmp_path / "nested" / "second.json"
    second_path.parent.mkdir(parents=True, exist_ok=True)
    payload = _result_payload()
    first_path.write_text(json.dumps(payload), encoding="utf-8")
    second_path.write_text(json.dumps(payload), encoding="utf-8")
    store_dir = tmp_path / "store"

    first = ingest_benchmark_result(
        store_dir=store_dir,
        result_path=first_path,
        revision="rev1",
        commit_timestamp="2026-01-01T00:00:00+00:00",
    )
    second = ingest_benchmark_result(
        store_dir=store_dir,
        result_path=second_path,
        revision="rev1",
        commit_timestamp="2026-01-01T00:00:00+00:00",
    )

    assert first["rows_appended"] == 2
    assert second["rows_appended"] == 0
    assert second["deduped"] is True
    assert len(load_longitudinal_rows(store_dir)) == 2


def test_ingest_recovers_when_index_missing_but_rows_exist(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    result_path.write_text(json.dumps(_result_payload()), encoding="utf-8")
    store_dir = tmp_path / "store"

    first = ingest_benchmark_result(
        store_dir=store_dir,
        result_path=result_path,
        revision="rev1",
        commit_timestamp="2026-01-01T00:00:00+00:00",
    )
    assert first["rows_appended"] == 2

    # Simulate a crash window where rows are written but index is missing.
    (store_dir / "index.json").unlink()

    second = ingest_benchmark_result(
        store_dir=store_dir,
        result_path=result_path,
        revision="rev1",
        commit_timestamp="2026-01-01T00:00:00+00:00",
    )

    assert second["rows_appended"] == 0
    assert second["deduped"] is True
    assert len(load_longitudinal_rows(store_dir)) == 2


def test_ingest_dedupes_without_rows_rescan_when_index_is_fresh(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    result_path.write_text(json.dumps(_result_payload()), encoding="utf-8")
    store_dir = tmp_path / "store"

    first = ingest_benchmark_result(
        store_dir=store_dir,
        result_path=result_path,
        revision="rev1",
        commit_timestamp="2026-01-01T00:00:00+00:00",
    )
    assert first["rows_appended"] == 2

    rows_path = store_dir / "rows.jsonl"
    try:
        rows_path.chmod(0)
        second = ingest_benchmark_result(
            store_dir=store_dir,
            result_path=result_path,
            revision="rev1",
            commit_timestamp="2026-01-01T00:00:00+00:00",
        )
    finally:
        rows_path.chmod(0o644)

    assert second["rows_appended"] == 0
    assert second["deduped"] is True
