from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest

from delta_bench_compare.compare import (
    _load,
    compare_runs,
    format_change,
)


def _run(cases: list[dict]) -> dict:
    normalized_cases: list[dict] = []
    for case in cases:
        if "classification" in case:
            normalized_cases.append(case)
            continue
        normalized_cases.append({"classification": "supported", **case})
    return {
        "schema_version": 2,
        "context": {"schema_version": 2, "label": "test"},
        "cases": normalized_cases,
    }


def test_format_change_thresholds() -> None:
    assert format_change(100.0, 90.0, 0.05) == "1.11x faster"
    assert format_change(100.0, 110.0, 0.05) == "1.10x slower"
    assert format_change(100.0, 103.0, 0.05) == "no change"


def test_compare_runs_handles_failures_and_missing_cases() -> None:
    base = _run(
        [
            {"case": "a", "success": True, "samples": [{"elapsed_ms": 100.0}]},
            {
                "case": "b",
                "success": False,
                "failure": {"message": "boom"},
                "samples": [],
            },
            {"case": "only_base", "success": True, "samples": [{"elapsed_ms": 20.0}]},
        ]
    )
    cand = _run(
        [
            {"case": "a", "success": True, "samples": [{"elapsed_ms": 90.0}]},
            {"case": "b", "success": True, "samples": [{"elapsed_ms": 120.0}]},
            {"case": "only_cand", "success": True, "samples": [{"elapsed_ms": 10.0}]},
        ]
    )

    comparison = compare_runs(base, cand, threshold=0.05)

    by_case = {row.case: row for row in comparison.rows}

    assert by_case["a"].change == "1.11x faster"
    assert by_case["b"].change == "incomparable"
    assert by_case["only_base"].change == "removed"
    assert by_case["only_cand"].change == "new"

    assert comparison.summary.faster == 1
    assert comparison.summary.incomparable == 1
    assert comparison.summary.removed == 1
    assert comparison.summary.new == 1


def test_compare_runs_marks_expected_failure_classification_explicitly() -> None:
    base = _run(
        [
            {
                "case": "dv_lane",
                "classification": "expected_failure",
                "success": True,
                "samples": [],
            }
        ]
    )
    cand = _run(
        [
            {
                "case": "dv_lane",
                "classification": "expected_failure",
                "success": True,
                "samples": [],
            }
        ]
    )

    comparison = compare_runs(base, cand, threshold=0.05)
    row = comparison.rows[0]
    assert row.change == "expected_failure"


def test_compare_rows_use_metrics_from_median_sample() -> None:
    base = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {"elapsed_ms": 100.0, "metrics": {"files_scanned": 10}},
                    {"elapsed_ms": 80.0, "metrics": {"files_scanned": 7}},
                    {"elapsed_ms": 95.0, "metrics": {"files_scanned": 8}},
                ],
            }
        ]
    )
    cand = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {"elapsed_ms": 120.0, "metrics": {"files_scanned": 12}},
                    {"elapsed_ms": 90.0, "metrics": {"files_scanned": 9}},
                    {"elapsed_ms": 115.0, "metrics": {"files_scanned": 11}},
                ],
            }
        ]
    )

    comparison = compare_runs(base, cand, threshold=0.05)
    row = comparison.rows[0]

    assert row.baseline_ms == 95.0
    assert row.candidate_ms == 115.0
    assert row.baseline_metrics is not None
    assert row.candidate_metrics is not None
    assert row.baseline_metrics.files_scanned == 8
    assert row.candidate_metrics.files_scanned == 11


def test_compare_rows_even_count_median_uses_same_sample_for_time_and_metrics() -> None:
    base = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {"elapsed_ms": 80.0, "metrics": {"files_scanned": 7}},
                    {"elapsed_ms": 100.0, "metrics": {"files_scanned": 10}},
                ],
            }
        ]
    )
    cand = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {"elapsed_ms": 90.0, "metrics": {"files_scanned": 9}},
                    {"elapsed_ms": 120.0, "metrics": {"files_scanned": 12}},
                ],
            }
        ]
    )

    comparison = compare_runs(base, cand, threshold=0.05, aggregation="median")
    row = comparison.rows[0]

    assert row.baseline_ms == 100.0
    assert row.candidate_ms == 120.0
    assert row.baseline_metrics is not None
    assert row.candidate_metrics is not None
    assert row.baseline_metrics.files_scanned == 10
    assert row.candidate_metrics.files_scanned == 12


def test_compare_rows_use_metrics_from_min_sample_when_requested() -> None:
    base = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {"elapsed_ms": 100.0, "metrics": {"files_scanned": 10}},
                    {"elapsed_ms": 80.0, "metrics": {"files_scanned": 7}},
                    {"elapsed_ms": 95.0, "metrics": {"files_scanned": 8}},
                ],
            }
        ]
    )
    cand = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {"elapsed_ms": 120.0, "metrics": {"files_scanned": 12}},
                    {"elapsed_ms": 90.0, "metrics": {"files_scanned": 9}},
                    {"elapsed_ms": 115.0, "metrics": {"files_scanned": 11}},
                ],
            }
        ]
    )

    comparison = compare_runs(base, cand, threshold=0.05, aggregation="min")
    row = comparison.rows[0]

    assert row.baseline_ms == 80.0
    assert row.candidate_ms == 90.0
    assert row.baseline_metrics is not None
    assert row.candidate_metrics is not None
    assert row.baseline_metrics.files_scanned == 7
    assert row.candidate_metrics.files_scanned == 9


def test_compare_rows_use_metrics_from_p95_sample_when_requested() -> None:
    base = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {"elapsed_ms": 10.0, "metrics": {"files_scanned": 1}},
                    {"elapsed_ms": 20.0, "metrics": {"files_scanned": 2}},
                    {"elapsed_ms": 30.0, "metrics": {"files_scanned": 3}},
                    {"elapsed_ms": 40.0, "metrics": {"files_scanned": 4}},
                    {"elapsed_ms": 100.0, "metrics": {"files_scanned": 5}},
                ],
            }
        ]
    )
    cand = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {"elapsed_ms": 15.0, "metrics": {"files_scanned": 6}},
                    {"elapsed_ms": 25.0, "metrics": {"files_scanned": 7}},
                    {"elapsed_ms": 35.0, "metrics": {"files_scanned": 8}},
                    {"elapsed_ms": 45.0, "metrics": {"files_scanned": 9}},
                    {"elapsed_ms": 55.0, "metrics": {"files_scanned": 10}},
                ],
            }
        ]
    )

    comparison = compare_runs(base, cand, threshold=0.05, aggregation="p95")
    row = comparison.rows[0]

    assert row.baseline_ms == 100.0
    assert row.candidate_ms == 55.0
    assert row.baseline_metrics is not None
    assert row.candidate_metrics is not None
    assert row.baseline_metrics.files_scanned == 5
    assert row.candidate_metrics.files_scanned == 10


def test_compare_runs_rejects_unknown_aggregation() -> None:
    base = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 100.0}]}])
    cand = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 90.0}]}])
    with pytest.raises(ValueError, match="aggregation"):
        compare_runs(base, cand, threshold=0.05, aggregation="not-a-mode")


def test_format_change_handles_zero_baseline() -> None:
    assert format_change(0.0, 0.0, 0.05) == "no change"
    assert format_change(0.0, 1.0, 0.05) == "incomparable"


def test_render_markdown_includes_summary_table() -> None:
    base = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 100.0}]}])
    cand = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 90.0}]}])
    from delta_bench_compare.compare import render_markdown

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_markdown(comparison)
    assert "| metric | value |" in out


def test_render_text_default_output_does_not_include_metric_columns() -> None:
    base = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 100.0,
                        "metrics": {"files_scanned": 10, "files_pruned": 2},
                    }
                ],
            }
        ]
    )
    cand = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 90.0,
                        "metrics": {"files_scanned": 9, "files_pruned": 1},
                    }
                ],
            }
        ]
    )
    from delta_bench_compare.compare import render_text

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_text(comparison)
    assert "Case | baseline | candidate | change" in out
    assert "files_scanned" not in out
    assert "files_pruned" not in out


def test_render_text_include_metrics_outputs_metric_columns() -> None:
    base = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 100.0,
                        "metrics": {
                            "files_scanned": 10,
                            "files_pruned": 2,
                            "bytes_scanned": 1024,
                            "scan_time_ms": 7,
                            "rewrite_time_ms": 11,
                        },
                    }
                ],
            }
        ]
    )
    cand = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 90.0,
                        "metrics": {
                            "files_scanned": 9,
                            "files_pruned": 1,
                            "bytes_scanned": 768,
                            "scan_time_ms": 5,
                            "rewrite_time_ms": 8,
                        },
                    }
                ],
            }
        ]
    )
    from delta_bench_compare.compare import render_text

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_text(comparison, include_metrics=True)
    assert "baseline_files_scanned" in out
    assert "candidate_files_scanned" in out
    assert "baseline_files_pruned" in out
    assert "candidate_files_pruned" in out
    assert "baseline_bytes_scanned" in out
    assert "candidate_bytes_scanned" in out
    assert "baseline_scan_time_ms" in out
    assert "candidate_scan_time_ms" in out
    assert "baseline_rewrite_time_ms" in out
    assert "candidate_rewrite_time_ms" in out


def test_render_markdown_include_metrics_outputs_metric_columns() -> None:
    base = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 100.0,
                        "metrics": {
                            "files_scanned": 10,
                            "files_pruned": 2,
                            "bytes_scanned": 1024,
                            "scan_time_ms": 7,
                            "rewrite_time_ms": 11,
                        },
                    }
                ],
            }
        ]
    )
    cand = _run(
        [
            {
                "case": "a",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 90.0,
                        "metrics": {
                            "files_scanned": 9,
                            "files_pruned": 1,
                            "bytes_scanned": 768,
                            "scan_time_ms": 5,
                            "rewrite_time_ms": 8,
                        },
                    }
                ],
            }
        ]
    )
    from delta_bench_compare.compare import render_markdown

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_markdown(comparison, include_metrics=True)
    assert "baseline_files_scanned" in out
    assert "candidate_rewrite_time_ms" in out


def test_compare_cli_rejects_removed_ci_policy_flags(tmp_path: Path) -> None:
    baseline = _run(
        [{"case": "a", "success": True, "samples": [{"elapsed_ms": 100.0}]}]
    )
    candidate = _run(
        [{"case": "a", "success": True, "samples": [{"elapsed_ms": 130.0}]}]
    )
    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")

    env = os.environ.copy()
    env["PYTHONPATH"] = str(Path(__file__).resolve().parents[1])
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.compare",
            str(baseline_path),
            str(candidate_path),
            "--ci",
            "--max-allowed-regressions",
            "0",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode != 0
    assert "unrecognized arguments" in result.stderr.lower()


def test_load_rejects_schema_v1_payload(tmp_path: Path) -> None:
    payload = {
        "schema_version": 1,
        "context": {"schema_version": 1, "label": "legacy"},
        "cases": [],
    }
    path = tmp_path / "legacy.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="schema_version"):
        _load(path)


def test_load_rejects_missing_case_classification(tmp_path: Path) -> None:
    payload = {
        "schema_version": 2,
        "context": {"schema_version": 2, "label": "v2"},
        "cases": [{"case": "a", "success": True, "samples": []}],
    }
    path = tmp_path / "missing-classification.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="classification"):
        _load(path)


def test_load_rejects_unknown_case_classification(tmp_path: Path) -> None:
    payload = {
        "schema_version": 2,
        "context": {"schema_version": 2, "label": "v2"},
        "cases": [
            {
                "case": "a",
                "success": True,
                "classification": "experimental",
                "samples": [],
            }
        ],
    }
    path = tmp_path / "bad-classification.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="classification"):
        _load(path)


def test_load_rejects_duplicate_case_ids(tmp_path: Path) -> None:
    payload = {
        "schema_version": 2,
        "context": {"schema_version": 2, "label": "v2"},
        "cases": [
            {
                "case": "duplicate",
                "success": True,
                "classification": "supported",
                "samples": [],
            },
            {
                "case": "duplicate",
                "success": True,
                "classification": "supported",
                "samples": [],
            },
        ],
    }
    path = tmp_path / "duplicate-case.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="duplicate case"):
        _load(path)


def test_public_schema_loader_rejects_invalid_json(tmp_path: Path) -> None:
    from delta_bench_compare.schema import load_benchmark_payload

    path = tmp_path / "invalid.json"
    path.write_text("{", encoding="utf-8")

    with pytest.raises(ValueError, match="invalid JSON"):
        load_benchmark_payload(path)


def test_compare_runs_handles_deterministic_tpcds_case_names() -> None:
    base = _run(
        [
            {"case": "tpcds_q03", "success": True, "samples": [{"elapsed_ms": 100.0}]},
            {
                "case": "tpcds_q72",
                "success": False,
                "failure": {"message": "skipped"},
                "samples": [],
            },
        ]
    )
    cand = _run(
        [
            {"case": "tpcds_q07", "success": True, "samples": [{"elapsed_ms": 95.0}]},
            {"case": "tpcds_q03", "success": True, "samples": [{"elapsed_ms": 90.0}]},
            {
                "case": "tpcds_q72",
                "success": False,
                "failure": {"message": "skipped"},
                "samples": [],
            },
        ]
    )

    comparison = compare_runs(base, cand, threshold=0.05)

    assert [row.case for row in comparison.rows] == [
        "tpcds_q03",
        "tpcds_q07",
        "tpcds_q72",
    ]
    by_case = {row.case: row for row in comparison.rows}
    assert by_case["tpcds_q07"].change == "new"
    assert by_case["tpcds_q72"].change == "incomparable"
