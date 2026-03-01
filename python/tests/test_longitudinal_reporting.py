from __future__ import annotations

import json
from pathlib import Path

from delta_bench_longitudinal.reporting import generate_trend_reports


def _write_rows(store_dir: Path) -> None:
    rows = [
        {
            "schema_version": 1,
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
            "schema_version": 1,
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
            "schema_version": 1,
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
            "schema_version": 1,
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
    store_dir.mkdir(parents=True, exist_ok=True)
    path = store_dir / "rows.jsonl"
    with path.open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row))
            fh.write("\n")


def test_generate_trend_reports_outputs_markdown_and_html(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    _write_rows(store_dir)
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

    assert summary["total_series"] == 2
    assert summary["regressions"] == 1
    assert "Regression Highlights" in markdown
    assert "scan_all" in markdown
    assert "<svg" in html
    assert "scan_all" in html


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
    _write_rows(store_dir)
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


def test_generate_trend_reports_handles_zero_baseline_regression(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    rows = [
        {
            "schema_version": 1,
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
            "schema_version": 1,
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
    ]
    store_dir.mkdir(parents=True, exist_ok=True)
    with (store_dir / "rows.jsonl").open("w", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row))
            fh.write("\n")

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
    assert "zero_baseline_case" in markdown_path.read_text(encoding="utf-8")


def test_generate_trend_reports_skips_invalid_json_rows(tmp_path: Path) -> None:
    store_dir = tmp_path / "store"
    store_dir.mkdir(parents=True, exist_ok=True)
    with (store_dir / "rows.jsonl").open("w", encoding="utf-8") as fh:
        fh.write(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": "r1",
                    "suite": "read_scan",
                    "scale": "sf1",
                    "case": "scan_all",
                    "success": True,
                    "median_ms": 100.0,
                    "sample_values_ms": [100.0, 101.0],
                    "benchmark_created_at": "2026-01-01T00:00:00+00:00",
                }
            )
        )
        fh.write("\n")
        fh.write("{invalid json}\n")
        fh.write(
            json.dumps(
                {
                    "schema_version": 1,
                    "run_id": "r2",
                    "suite": "read_scan",
                    "scale": "sf1",
                    "case": "scan_all",
                    "success": True,
                    "median_ms": 120.0,
                    "sample_values_ms": [119.0, 121.0],
                    "benchmark_created_at": "2026-01-02T00:00:00+00:00",
                }
            )
        )
        fh.write("\n")

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

    assert summary["total_series"] == 1
    assert summary["invalid_rows"] == 1
