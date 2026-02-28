from __future__ import annotations

from delta_bench_compare.compare import (
    ci_regression_violation,
    compare_runs,
    format_change,
)


def _run(cases: list[dict]) -> dict:
    return {
        "schema_version": 1,
        "context": {"label": "test"},
        "cases": cases,
    }


def test_format_change_thresholds() -> None:
    assert format_change(100.0, 90.0, 0.05) == "+1.11x faster"
    assert format_change(100.0, 110.0, 0.05) == "1.10x slower"
    assert format_change(100.0, 103.0, 0.05) == "no change"


def test_compare_runs_handles_failures_and_missing_cases() -> None:
    base = _run(
        [
            {"case": "a", "success": True, "samples": [{"elapsed_ms": 100.0}]},
            {"case": "b", "success": False, "failure": {"message": "boom"}, "samples": []},
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

    assert by_case["a"].change == "+1.11x faster"
    assert by_case["b"].change == "incomparable"
    assert by_case["only_base"].change == "removed"
    assert by_case["only_cand"].change == "new"

    assert comparison.summary.faster == 1
    assert comparison.summary.incomparable == 1
    assert comparison.summary.removed == 1
    assert comparison.summary.new == 1

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


def test_ci_policy_fails_when_slower_cases_exceed_allowed() -> None:
    base = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 100.0}]}])
    cand = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 130.0}]}])
    comparison = compare_runs(base, cand, threshold=0.05)

    violates, message = ci_regression_violation(
        comparison, ci_enabled=True, max_allowed_regressions=0
    )
    assert violates is True
    assert "slower cases" in message


def test_ci_policy_passes_when_within_allowed_regressions() -> None:
    base = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 100.0}]}])
    cand = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 130.0}]}])
    comparison = compare_runs(base, cand, threshold=0.05)

    violates, _ = ci_regression_violation(
        comparison, ci_enabled=True, max_allowed_regressions=1
    )
    assert violates is False


def test_advisory_mode_never_violates_ci_policy() -> None:
    base = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 100.0}]}])
    cand = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 130.0}]}])
    comparison = compare_runs(base, cand, threshold=0.05)

    violates, message = ci_regression_violation(
        comparison, ci_enabled=False, max_allowed_regressions=0
    )
    assert violates is False
    assert message == ""
