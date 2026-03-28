from __future__ import annotations

from contextlib import closing
import json
from pathlib import Path

from delta_bench_longitudinal.reporting import generate_trend_reports
from delta_bench_longitudinal.store import (
    _connect_store,
    ingest_benchmark_result,
    store_db_path,
)


def _seed_rows(store_dir: Path, rows: list[dict]) -> None:
    with closing(_connect_store(store_dir)) as conn:
        with conn:
            for row in rows:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO runs (
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
                        lane,
                        measurement_kind,
                        validation_level,
                        harness_revision,
                        fixture_recipe_hash,
                        fidelity_fingerprint,
                        iterations,
                        warmup,
                        source_result_path
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["run_id"],
                        1,
                        row.get("ingested_at", row["benchmark_created_at"]),
                        row["revision"],
                        row.get(
                            "revision_commit_timestamp", row["benchmark_created_at"]
                        ),
                        row["benchmark_created_at"],
                        row.get("label", f"longitudinal-{row['revision']}"),
                        row.get("git_sha", row["revision"]),
                        row.get("host", "bench-host"),
                        row["suite"],
                        row["scale"],
                        row.get("lane"),
                        row.get("measurement_kind"),
                        row.get("validation_level"),
                        row.get("harness_revision"),
                        row.get("fixture_recipe_hash"),
                        row.get("fidelity_fingerprint"),
                        row.get("iterations", len(row["sample_values_ms"])),
                        row.get("warmup", 1),
                        row.get(
                            "source_result_path",
                            str(store_dir / f"{row['run_id']}.json"),
                        ),
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO case_rows (
                        run_id,
                        case_name,
                        compatibility_key,
                        case_definition_hash,
                        success,
                        failure_reason,
                        sample_count,
                        sample_values_json,
                        best_ms,
                        min_ms,
                        max_ms,
                        mean_ms,
                        median_ms
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        row["run_id"],
                        row["case"],
                        row.get("compatibility_key"),
                        row.get("case_definition_hash"),
                        int(bool(row["success"])),
                        row.get("failure_reason"),
                        len(row["sample_values_ms"]),
                        json.dumps(row["sample_values_ms"]),
                        row.get("best_ms", row["median_ms"]),
                        row.get("min_ms", row["median_ms"]),
                        row.get("max_ms", row["median_ms"]),
                        row.get("mean_ms", row["median_ms"]),
                        row["median_ms"],
                    ),
                )


def _report_rows() -> list[dict]:
    return [
        {
            "run_id": "r1",
            "revision": "rev1",
            "suite": "read_scan",
            "scale": "sf1",
            "case": "scan_all",
            "success": True,
            "median_ms": 100.0,
            "sample_values_ms": [100.0, 101.0, 99.0],
            "benchmark_created_at": "2026-01-01T00:00:00+00:00",
        },
        {
            "run_id": "r2",
            "revision": "rev2",
            "suite": "read_scan",
            "scale": "sf1",
            "case": "scan_all",
            "success": True,
            "median_ms": 102.0,
            "sample_values_ms": [102.0, 103.0, 101.0],
            "benchmark_created_at": "2026-01-02T00:00:00+00:00",
        },
        {
            "run_id": "r3",
            "revision": "rev3",
            "suite": "read_scan",
            "scale": "sf1",
            "case": "scan_all",
            "success": True,
            "median_ms": 140.0,
            "sample_values_ms": [139.0, 140.0, 141.0],
            "benchmark_created_at": "2026-01-03T00:00:00+00:00",
        },
        {
            "run_id": "r4",
            "revision": "rev3",
            "suite": "metadata",
            "scale": "sf1",
            "case": "load_metadata",
            "success": True,
            "median_ms": 50.0,
            "sample_values_ms": [49.0, 50.0, 51.0],
            "benchmark_created_at": "2026-01-03T00:00:00+00:00",
        },
    ]


def test_generate_trend_reports_outputs_markdown_and_html(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    _seed_rows(store_dir, _report_rows())
    markdown_path = tmp_path / "summary.md"
    html_path = tmp_path / "report.html"

    summary = generate_trend_reports(
        store_dir=store_dir,
        markdown_path=markdown_path,
        html_path=html_path,
        baseline_window=2,
        regression_threshold=0.05,
        significance_method="none",
        significance_alpha=0.05,
    )

    markdown = markdown_path.read_text(encoding="utf-8")
    html = html_path.read_text(encoding="utf-8")

    assert store_db_path(store_dir).exists()
    assert not (store_dir / "rows.jsonl").exists()
    assert summary["total_series"] == 2
    assert summary["regressions"] == 1
    assert "Regression Highlights" in markdown
    assert "scan_all" in markdown
    assert "<svg" in html
    assert "scan_all" in html


def test_generate_trend_reports_splits_series_by_compatibility_identity(
    tmp_path: Path,
) -> None:
    store_dir = tmp_path / "store"
    _seed_rows(
        store_dir,
        [
            {
                "run_id": "r1",
                "revision": "rev1",
                "suite": "scan",
                "scale": "sf1",
                "case": "scan_all",
                "lane": "macro",
                "measurement_kind": "phase_breakdown",
                "validation_level": "operational",
                "harness_revision": "h1",
                "fixture_recipe_hash": "sha256:recipe-a",
                "compatibility_key": "sha256:compat-a",
                "success": True,
                "median_ms": 100.0,
                "sample_values_ms": [100.0],
                "benchmark_created_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "run_id": "r2",
                "revision": "rev2",
                "suite": "scan",
                "scale": "sf1",
                "case": "scan_all",
                "lane": "macro",
                "measurement_kind": "phase_breakdown",
                "validation_level": "operational",
                "harness_revision": "h1",
                "fixture_recipe_hash": "sha256:recipe-b",
                "compatibility_key": "sha256:compat-b",
                "success": True,
                "median_ms": 101.0,
                "sample_values_ms": [101.0],
                "benchmark_created_at": "2026-01-02T00:00:00+00:00",
            },
        ],
    )
    markdown_path = tmp_path / "summary.md"
    html_path = tmp_path / "report.html"

    summary = generate_trend_reports(
        store_dir=store_dir,
        markdown_path=markdown_path,
        html_path=html_path,
        baseline_window=1,
        regression_threshold=0.05,
        significance_method="none",
        significance_alpha=0.05,
    )

    assert summary["total_series"] == 2


def test_generate_trend_reports_does_not_collapse_same_key_with_different_identity(
    tmp_path: Path,
) -> None:
    store_dir = tmp_path / "store"
    shared_key = "sha256:compat-shared"
    _seed_rows(
        store_dir,
        [
            {
                "run_id": "r1",
                "revision": "rev1",
                "suite": "scan",
                "scale": "sf1",
                "case": "scan_all",
                "runner": "rust",
                "lane": "macro",
                "measurement_kind": "phase_breakdown",
                "validation_level": "operational",
                "harness_revision": "h1",
                "fixture_recipe_hash": "sha256:recipe-a",
                "fidelity_fingerprint": "sha256:fidelity",
                "compatibility_key": shared_key,
                "success": True,
                "median_ms": 100.0,
                "sample_values_ms": [100.0],
                "benchmark_created_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "run_id": "r2",
                "revision": "rev2",
                "suite": "scan",
                "scale": "sf1",
                "case": "scan_all",
                "runner": "rust",
                "lane": "macro",
                "measurement_kind": "phase_breakdown",
                "validation_level": "operational",
                "harness_revision": "h1",
                "fixture_recipe_hash": "sha256:recipe-b",
                "fidelity_fingerprint": "sha256:fidelity",
                "compatibility_key": shared_key,
                "success": True,
                "median_ms": 101.0,
                "sample_values_ms": [101.0],
                "benchmark_created_at": "2026-01-02T00:00:00+00:00",
            },
        ],
    )

    summary = generate_trend_reports(
        store_dir=store_dir,
        markdown_path=tmp_path / "summary.md",
        html_path=tmp_path / "report.html",
        baseline_window=1,
        regression_threshold=0.05,
        significance_method="none",
        significance_alpha=0.05,
    )

    assert summary["total_series"] == 2


def test_generate_trend_reports_handles_empty_store(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True, exist_ok=True)
    markdown_path = tmp_path / "summary.md"
    html_path = tmp_path / "report.html"

    summary = generate_trend_reports(
        store_dir=store_dir,
        markdown_path=markdown_path,
        html_path=html_path,
        baseline_window=3,
        regression_threshold=0.05,
        significance_method="none",
        significance_alpha=0.05,
    )

    assert summary["total_series"] == 0
    assert summary["regressions"] == 0
    assert "No longitudinal rows found" in markdown_path.read_text(encoding="utf-8")


def test_generate_trend_reports_with_significance_metrics(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    _seed_rows(store_dir, _report_rows())
    markdown_path = tmp_path / "summary.md"
    html_path = tmp_path / "report.html"

    summary = generate_trend_reports(
        store_dir=store_dir,
        markdown_path=markdown_path,
        html_path=html_path,
        baseline_window=2,
        regression_threshold=0.05,
        significance_method="mann-whitney",
        significance_alpha=0.05,
    )
    markdown = markdown_path.read_text(encoding="utf-8")
    assert summary["regressions"] == 1
    assert summary["significant_regressions"] == 1
    assert "p-value" in markdown


def test_generate_trend_reports_handles_zero_baseline_regression(
    tmp_path: Path,
) -> None:
    store_dir = tmp_path / "store"
    _seed_rows(
        store_dir,
        [
            {
                "run_id": "r1",
                "revision": "rev1",
                "suite": "read_scan",
                "scale": "sf1",
                "case": "zero_baseline_case",
                "success": True,
                "median_ms": 0.0,
                "sample_values_ms": [0.0, 0.0],
                "benchmark_created_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "run_id": "r2",
                "revision": "rev2",
                "suite": "read_scan",
                "scale": "sf1",
                "case": "zero_baseline_case",
                "success": True,
                "median_ms": 1.0,
                "sample_values_ms": [1.0, 1.0],
                "benchmark_created_at": "2026-01-02T00:00:00+00:00",
            },
        ],
    )

    markdown_path = tmp_path / "summary.md"
    html_path = tmp_path / "report.html"
    summary = generate_trend_reports(
        store_dir=store_dir,
        markdown_path=markdown_path,
        html_path=html_path,
        baseline_window=1,
        regression_threshold=0.05,
        significance_method="none",
        significance_alpha=0.05,
    )
    assert summary["regressions"] == 1
    assert summary["invalid_rows"] == 0
    assert "zero_baseline_case" in markdown_path.read_text(encoding="utf-8")


def test_generate_trend_reports_excludes_validation_only_runs(
    tmp_path: Path,
) -> None:
    store_dir = tmp_path / "store"

    for run_id, revision, created_at, median_ms in (
        ("run-1", "rev1", "2026-01-01T00:00:00+00:00", 100.0),
        ("run-2", "rev2", "2026-01-02T00:00:00+00:00", 130.0),
    ):
        result_path = tmp_path / f"{run_id}.json"
        result_path.write_text(
            json.dumps(
                {
                    "schema_version": 4,
                    "context": {
                        "schema_version": 4,
                        "label": run_id,
                        "created_at": created_at,
                        "suite": "scan",
                        "runner": "rust",
                        "scale": "sf1",
                        "timing_phase": "execute",
                        "dataset_fingerprint": "sha256:fixture",
                        "storage_backend": "local",
                        "lane": "smoke",
                        "measurement_kind": "phase_breakdown",
                        "validation_level": "operational",
                        "run_id": run_id,
                        "harness_revision": "h1",
                        "fixture_recipe_hash": "sha256:recipe",
                        "fidelity_fingerprint": "sha256:fidelity",
                        "iterations": 1,
                        "warmup": 0,
                    },
                    "cases": [
                        {
                            "case": "scan_full_narrow",
                            "success": True,
                            "validation_passed": True,
                            "perf_valid": False,
                            "classification": "supported",
                            "samples": [{"elapsed_ms": median_ms}],
                            "compatibility_key": "sha256:scan-full-narrow",
                            "case_definition_hash": "sha256:scan-full-narrow-def",
                            "run_summary": {
                                "sample_count": 1,
                                "invalid_sample_count": 0,
                                "min_ms": median_ms,
                                "max_ms": median_ms,
                                "mean_ms": median_ms,
                                "median_ms": median_ms,
                            },
                            "failure": None,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        ingest_benchmark_result(
            store_dir=store_dir,
            result_path=result_path,
            revision=revision,
            commit_timestamp=created_at,
        )

    markdown_path = tmp_path / "summary.md"
    html_path = tmp_path / "report.html"
    summary = generate_trend_reports(
        store_dir=store_dir,
        markdown_path=markdown_path,
        html_path=html_path,
        baseline_window=1,
        regression_threshold=0.05,
        significance_method="none",
        significance_alpha=0.05,
    )

    assert summary["total_series"] == 0
    assert summary["regressions"] == 0
    assert "scan_full_narrow" not in markdown_path.read_text(encoding="utf-8")


def test_generate_trend_reports_excludes_zero_sample_legacy_rows(
    tmp_path: Path,
) -> None:
    store_dir = tmp_path / "store"
    _seed_rows(
        store_dir,
        [
            {
                "run_id": "r1",
                "revision": "rev1",
                "suite": "scan",
                "scale": "sf1",
                "case": "scan_full_narrow",
                "success": True,
                "median_ms": 100.0,
                "sample_values_ms": [],
                "benchmark_created_at": "2026-01-01T00:00:00+00:00",
            },
            {
                "run_id": "r2",
                "revision": "rev2",
                "suite": "scan",
                "scale": "sf1",
                "case": "scan_full_narrow",
                "success": True,
                "median_ms": 130.0,
                "sample_values_ms": [],
                "benchmark_created_at": "2026-01-02T00:00:00+00:00",
            },
        ],
    )

    markdown_path = tmp_path / "summary.md"
    html_path = tmp_path / "report.html"
    summary = generate_trend_reports(
        store_dir=store_dir,
        markdown_path=markdown_path,
        html_path=html_path,
        baseline_window=1,
        regression_threshold=0.05,
        significance_method="none",
        significance_alpha=0.05,
    )

    assert summary["total_series"] == 0
    assert summary["regressions"] == 0
