from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from delta_bench_longitudinal.store import (
    ingest_benchmark_result,
    load_longitudinal_rows,
    store_db_path,
)


def _result_payload() -> dict:
    return {
        "schema_version": 3,
        "context": {
            "schema_version": 3,
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
                "classification": "supported",
                "success": True,
                "validation_passed": True,
                "perf_valid": True,
                "samples": [
                    {"elapsed_ms": 100.0},
                    {"elapsed_ms": 120.0},
                    {"elapsed_ms": 90.0},
                ],
                "failure": None,
            },
            {
                "case": "scan_predicate",
                "classification": "supported",
                "success": False,
                "validation_passed": False,
                "perf_valid": False,
                "samples": [],
                "failure": {"message": "failed op"},
            },
        ],
    }


def _result_payload_v4() -> dict:
    payload = _result_payload()
    payload["schema_version"] = 4
    payload["context"]["schema_version"] = 4
    payload["context"]["lane"] = "macro"
    payload["context"]["measurement_kind"] = "end_to_end"
    payload["context"]["validation_level"] = "operational"
    payload["context"]["run_id"] = "run-v4"
    payload["context"]["harness_revision"] = "harness-rev"
    payload["context"]["fixture_recipe_hash"] = "sha256:recipe"
    payload["context"]["fidelity_fingerprint"] = "sha256:fidelity"
    payload["cases"][0]["compatibility_key"] = "sha256:compat"
    payload["cases"][0]["case_definition_hash"] = "sha256:case-def"
    payload["cases"][0]["run_summary"] = {
        "sample_count": 3,
        "invalid_sample_count": 0,
        "median_ms": 100.0,
        "host_label": "bench-host",
        "fidelity_fingerprint": "sha256:fidelity",
    }
    payload["cases"][1]["compatibility_key"] = "sha256:compat-fail"
    payload["cases"][1]["case_definition_hash"] = "sha256:case-def-fail"
    payload["cases"][1]["run_summary"] = {
        "sample_count": 0,
        "invalid_sample_count": 0,
        "median_ms": None,
        "host_label": "bench-host",
        "fidelity_fingerprint": "sha256:fidelity",
    }
    return payload


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


def test_ingest_rejects_legacy_jsonl_store_until_migrated(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    result_path.write_text(json.dumps(_result_payload()), encoding="utf-8")
    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True, exist_ok=True)
    (store_dir / "rows.jsonl").write_text("{}", encoding="utf-8")
    (store_dir / "index.json").write_text(
        json.dumps({"schema_version": 1, "run_ids": []}),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="legacy longitudinal store"):
        ingest_benchmark_result(
            store_dir=store_dir,
            result_path=result_path,
            revision="rev1",
            commit_timestamp="2026-01-01T00:00:00+00:00",
        )


def test_ingest_uses_queryable_sqlite_backend(tmp_path: Path) -> None:
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

    db_path = store_db_path(store_dir)
    assert db_path.exists()
    rows = load_longitudinal_rows(store_dir)
    assert len(rows) == 2
    assert not (store_dir / "rows.jsonl").exists()
    assert not (store_dir / "index.json").exists()


def test_ingest_rejects_non_v2_payload(tmp_path: Path) -> None:
    payload = _result_payload()
    payload["schema_version"] = 1
    payload["context"]["schema_version"] = 1
    result_path = tmp_path / "legacy-result.json"
    result_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="schema_version"):
        ingest_benchmark_result(
            store_dir=tmp_path / "store",
            result_path=result_path,
            revision="rev1",
            commit_timestamp="2026-01-01T00:00:00+00:00",
        )


def test_ingest_rejects_missing_case_classification(tmp_path: Path) -> None:
    payload = _result_payload()
    payload["cases"][0].pop("classification")
    result_path = tmp_path / "missing-classification.json"
    result_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="classification"):
        ingest_benchmark_result(
            store_dir=tmp_path / "store",
            result_path=result_path,
            revision="rev1",
            commit_timestamp="2026-01-01T00:00:00+00:00",
        )


def test_store_uses_public_benchmark_schema_loader() -> None:
    module_path = (
        Path(__file__).resolve().parents[1] / "delta_bench_longitudinal" / "store.py"
    )
    source = module_path.read_text(encoding="utf-8")
    assert "from delta_bench_compare.schema import load_benchmark_payload" in source
    assert "from delta_bench_compare.compare import _load" not in source


def test_v4_rows_preserve_compatibility_identity_fields(tmp_path: Path) -> None:
    result_path = tmp_path / "result-v4.json"
    result_path.write_text(json.dumps(_result_payload_v4()), encoding="utf-8")
    store_dir = tmp_path / "store"

    ingest_benchmark_result(
        store_dir=store_dir,
        result_path=result_path,
        revision="rev1",
        commit_timestamp="2026-01-01T00:00:00+00:00",
    )
    rows = load_longitudinal_rows(store_dir)
    scan_all = next(row for row in rows if row["case"] == "scan_all")

    assert scan_all["lane"] == "macro"
    assert scan_all["measurement_kind"] == "end_to_end"
    assert scan_all["validation_level"] == "operational"
    assert scan_all["harness_revision"] == "harness-rev"
    assert scan_all["fixture_recipe_hash"] == "sha256:recipe"
    assert scan_all["compatibility_key"] == "sha256:compat"
