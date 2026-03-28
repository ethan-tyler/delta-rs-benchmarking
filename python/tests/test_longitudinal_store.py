from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path

import pytest

from delta_bench_longitudinal.store import (
    ingest_benchmark_result,
    load_longitudinal_rows,
    store_db_path,
)


def _result_payload_v3() -> dict:
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
    payload = _result_payload_v3()
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


def _create_legacy_store_sqlite(store_dir: Path) -> None:
    store_dir.mkdir(parents=True, exist_ok=True)
    db_path = store_db_path(store_dir)
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE runs (
                run_id TEXT PRIMARY KEY,
                schema_version INTEGER NOT NULL,
                ingested_at TEXT NOT NULL,
                revision TEXT NOT NULL,
                revision_commit_timestamp TEXT NOT NULL,
                benchmark_created_at TEXT,
                label TEXT,
                git_sha TEXT,
                host TEXT,
                suite TEXT,
                scale TEXT,
                iterations INTEGER,
                warmup INTEGER,
                image_version TEXT,
                hardening_profile_id TEXT,
                hardening_profile_sha256 TEXT,
                cpu_model TEXT,
                cpu_microcode TEXT,
                kernel TEXT,
                boot_params TEXT,
                cpu_steal_pct REAL,
                numa_topology TEXT,
                egress_policy_sha256 TEXT,
                run_mode TEXT,
                maintenance_window_id TEXT,
                source_result_path TEXT NOT NULL
            );

            CREATE TABLE case_rows (
                run_id TEXT NOT NULL,
                case_name TEXT NOT NULL,
                success INTEGER NOT NULL,
                failure_reason TEXT,
                sample_count INTEGER NOT NULL,
                sample_values_json TEXT NOT NULL,
                best_ms REAL,
                min_ms REAL,
                max_ms REAL,
                mean_ms REAL,
                median_ms REAL,
                PRIMARY KEY (run_id, case_name)
            );
            """
        )


def _seed_legacy_store_sqlite(store_dir: Path) -> None:
    _create_legacy_store_sqlite(store_dir)
    with sqlite3.connect(store_db_path(store_dir)) as conn:
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
                image_version,
                hardening_profile_id,
                hardening_profile_sha256,
                cpu_model,
                cpu_microcode,
                kernel,
                boot_params,
                cpu_steal_pct,
                numa_topology,
                egress_policy_sha256,
                run_mode,
                maintenance_window_id,
                source_result_path
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "legacy-run",
                1,
                "2026-01-01T00:00:00+00:00",
                "rev0",
                "2026-01-01T00:00:00+00:00",
                "2026-01-01T00:00:00+00:00",
                "legacy-label",
                "rev0",
                "bench-host",
                "scan",
                "sf1",
                1,
                0,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                str(store_dir / "legacy-run.json"),
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
                "legacy-run",
                "scan_all",
                1,
                None,
                1,
                json.dumps([100.0]),
                100.0,
                100.0,
                100.0,
                100.0,
                100.0,
            ),
        )


def test_ingest_is_append_safe_and_idempotent(tmp_path: Path) -> None:
    result_path = tmp_path / "result.json"
    result_path.write_text(json.dumps(_result_payload_v4()), encoding="utf-8")
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
    result_path.write_text(json.dumps(_result_payload_v4()), encoding="utf-8")
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
    payload = _result_payload_v4()
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
    result_path.write_text(json.dumps(_result_payload_v4()), encoding="utf-8")
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
    result_path.write_text(json.dumps(_result_payload_v4()), encoding="utf-8")
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
    payload = _result_payload_v3()
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
    payload = _result_payload_v4()
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


def test_authoritative_longitudinal_ingest_rejects_schema_v3_payloads(
    tmp_path: Path,
) -> None:
    result_path = tmp_path / "result-v3.json"
    result_path.write_text(json.dumps(_result_payload_v3()), encoding="utf-8")

    with pytest.raises(ValueError, match="schema v4"):
        ingest_benchmark_result(
            store_dir=tmp_path / "store",
            result_path=result_path,
            revision="rev1",
            commit_timestamp="2026-01-01T00:00:00+00:00",
        )


def test_authoritative_longitudinal_ingest_requires_v4_context_identity(
    tmp_path: Path,
) -> None:
    payload = _result_payload_v4()
    payload["context"].pop("fixture_recipe_hash")
    result_path = tmp_path / "missing-context.json"
    result_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="fixture_recipe_hash"):
        ingest_benchmark_result(
            store_dir=tmp_path / "store",
            result_path=result_path,
            revision="rev1",
            commit_timestamp="2026-01-01T00:00:00+00:00",
        )


def test_authoritative_longitudinal_ingest_requires_case_identity_fields(
    tmp_path: Path,
) -> None:
    payload = _result_payload_v4()
    payload["cases"][0].pop("compatibility_key")
    result_path = tmp_path / "missing-case-identity.json"
    result_path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="compatibility_key"):
        ingest_benchmark_result(
            store_dir=tmp_path / "store",
            result_path=result_path,
            revision="rev1",
            commit_timestamp="2026-01-01T00:00:00+00:00",
        )


def test_load_rows_migrates_existing_legacy_sqlite_store(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    _seed_legacy_store_sqlite(store_dir)

    rows = load_longitudinal_rows(store_dir)

    assert len(rows) == 1
    assert rows[0]["case"] == "scan_all"
    assert rows[0]["runner"] is None
    assert rows[0]["timing_phase"] is None
    assert rows[0]["lane"] is None
    assert rows[0]["measurement_kind"] is None
    assert rows[0]["validation_level"] is None
    assert rows[0]["storage_backend"] is None
    assert rows[0]["compatibility_key"] is None
    assert rows[0]["case_definition_hash"] is None


def test_ingest_v4_result_migrates_existing_legacy_sqlite_store(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    _create_legacy_store_sqlite(store_dir)
    result_path = tmp_path / "result-v4.json"
    result_path.write_text(json.dumps(_result_payload_v4()), encoding="utf-8")

    outcome = ingest_benchmark_result(
        store_dir=store_dir,
        result_path=result_path,
        revision="rev1",
        commit_timestamp="2026-01-01T00:00:00+00:00",
    )

    assert outcome["rows_appended"] == 2
    rows = load_longitudinal_rows(store_dir)
    assert any(row["case"] == "scan_all" for row in rows)
