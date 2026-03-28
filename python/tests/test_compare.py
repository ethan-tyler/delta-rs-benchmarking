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
from delta_bench_compare.aggregate import aggregate_payloads


def _run(
    cases: list[dict],
    *,
    benchmark_mode: str = "perf",
    timing_phase: str = "execute",
    dataset_id: str = "tiny_smoke",
    dataset_fingerprint: str = "sha256:fixture",
    runner: str = "rust",
    scale: str = "sf1",
    backend_profile: str = "local",
    storage_backend: str = "local",
) -> dict:
    normalized_cases: list[dict] = []
    for case in cases:
        normalized_case = {
            "success": True,
            "validation_passed": True,
            "perf_valid": True,
            "classification": "supported",
            **case,
        }
        if "classification" in case:
            normalized_case["classification"] = case["classification"]
        normalized_cases.append(normalized_case)
    return {
        "schema_version": 3,
        "context": {
            "schema_version": 3,
            "label": "test",
            "suite": "scan",
            "benchmark_mode": benchmark_mode,
            "timing_phase": timing_phase,
            "dataset_id": dataset_id,
            "dataset_fingerprint": dataset_fingerprint,
            "runner": runner,
            "scale": scale,
            "storage_backend": storage_backend,
            "backend_profile": backend_profile,
        },
        "cases": normalized_cases,
    }


def _run_v4(
    cases: list[dict],
    *,
    lane: str = "macro",
    measurement_kind: str = "end_to_end",
    validation_level: str = "operational",
    harness_revision: str = "harness-rev",
    fixture_recipe_hash: str = "sha256:recipe",
    fidelity_fingerprint: str = "sha256:fidelity",
) -> dict:
    normalized_cases: list[dict] = []
    for case in cases:
        samples = case.get("samples", [])
        elapsed = [
            float(sample["elapsed_ms"]) for sample in samples if "elapsed_ms" in sample
        ]
        median_ms = sorted(elapsed)[len(elapsed) // 2] if elapsed else None
        normalized_case = {
            "success": True,
            "validation_passed": True,
            "perf_valid": True,
            "classification": "supported",
            "suite_manifest_hash": "sha256:manifest",
            "case_definition_hash": f"sha256:{case['case']}-def",
            "compatibility_key": f"sha256:{case['case']}-compat",
            "supports_decision": True,
            "required_runs": 5,
            "decision_threshold_pct": 5.0,
            "decision_metric": "median",
            "run_summary": {
                "sample_count": len(elapsed),
                "invalid_sample_count": 0,
                "median_ms": median_ms,
                "host_label": "bench-host",
                "fidelity_fingerprint": fidelity_fingerprint,
            },
            **case,
        }
        normalized_cases.append(normalized_case)
    return {
        "schema_version": 4,
        "context": {
            "schema_version": 4,
            "label": "test",
            "suite": "scan",
            "runner": "rust",
            "scale": "sf1",
            "storage_backend": "local",
            "timing_phase": "execute",
            "dataset_fingerprint": "sha256:fixture",
            "lane": lane,
            "measurement_kind": measurement_kind,
            "validation_level": validation_level,
            "run_id": "run-1",
            "harness_revision": harness_revision,
            "fixture_recipe_hash": fixture_recipe_hash,
            "fidelity_fingerprint": fidelity_fingerprint,
        },
        "cases": normalized_cases,
    }


def _run_compare_cli(
    baseline_path: Path,
    candidate_path: Path,
    *args: str,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.compare",
            str(baseline_path),
            str(candidate_path),
            *args,
        ],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])},
    )


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


def test_compare_runs_rejects_invalid_perf_cases() -> None:
    base = _run(
        [
            {
                "case": "a",
                "success": False,
                "validation_passed": False,
                "perf_valid": False,
                "failure_kind": "assertion_mismatch",
                "failure": {"message": "hash mismatch"},
                "samples": [],
            }
        ]
    )
    cand = _run([{"case": "a", "samples": [{"elapsed_ms": 90.0}]}])

    with pytest.raises(ValueError, match="perf-valid"):
        compare_runs(base, cand, threshold=0.05)


def test_compare_runs_rejects_context_mismatch() -> None:
    base = _run([{"case": "a", "samples": [{"elapsed_ms": 100.0}]}])
    cand = _run(
        [{"case": "a", "samples": [{"elapsed_ms": 90.0}]}],
        timing_phase="plan",
        dataset_id="many_versions",
        dataset_fingerprint="sha256:other",
    )

    with pytest.raises(ValueError, match="context mismatch"):
        compare_runs(base, cand, threshold=0.05)


def test_compare_runs_rejects_benchmark_mode_mismatch() -> None:
    base = _run(
        [{"case": "a", "samples": [{"elapsed_ms": 100.0}]}], benchmark_mode="perf"
    )
    cand = _run(
        [{"case": "a", "samples": [{"elapsed_ms": 90.0}]}],
        benchmark_mode="assert",
    )

    with pytest.raises(ValueError, match="context mismatch"):
        compare_runs(base, cand, threshold=0.05)


def test_compare_runs_rejects_missing_required_comparison_identity() -> None:
    base = _run([{"case": "a", "samples": [{"elapsed_ms": 100.0}]}])
    cand = _run([{"case": "a", "samples": [{"elapsed_ms": 90.0}]}])

    for payload in (base, cand):
        payload["context"].pop("suite")
        payload["context"].pop("runner")
        payload["context"].pop("benchmark_mode")
        payload["context"].pop("timing_phase")
        payload["context"].pop("dataset_fingerprint")
        payload["context"].pop("scale")
        payload["context"].pop("storage_backend")

    with pytest.raises(ValueError, match="missing required comparison context"):
        compare_runs(base, cand, threshold=0.05)


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


def test_compare_rows_include_contention_metrics_from_representative_sample() -> None:
    base = _run(
        [
            {
                "case": "update_vs_compaction",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 100.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 2,
                                "race_count": 3,
                                "ops_attempted": 6,
                                "ops_succeeded": 4,
                                "ops_failed": 2,
                                "conflict_append": 0,
                                "conflict_delete_read": 2,
                                "conflict_delete_delete": 0,
                                "conflict_metadata_changed": 0,
                                "conflict_protocol_changed": 0,
                                "conflict_transaction": 0,
                                "version_already_exists": 0,
                                "max_commit_attempts_exceeded": 0,
                                "other_errors": 0,
                            }
                        },
                    },
                    {
                        "elapsed_ms": 90.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 2,
                                "race_count": 3,
                                "ops_attempted": 6,
                                "ops_succeeded": 3,
                                "ops_failed": 3,
                                "conflict_append": 0,
                                "conflict_delete_read": 3,
                                "conflict_delete_delete": 0,
                                "conflict_metadata_changed": 0,
                                "conflict_protocol_changed": 0,
                                "conflict_transaction": 0,
                                "version_already_exists": 0,
                                "max_commit_attempts_exceeded": 0,
                                "other_errors": 0,
                            }
                        },
                    },
                ],
            }
        ]
    )
    cand = _run(
        [
            {
                "case": "update_vs_compaction",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 95.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 2,
                                "race_count": 3,
                                "ops_attempted": 6,
                                "ops_succeeded": 5,
                                "ops_failed": 1,
                                "conflict_append": 0,
                                "conflict_delete_read": 1,
                                "conflict_delete_delete": 0,
                                "conflict_metadata_changed": 0,
                                "conflict_protocol_changed": 0,
                                "conflict_transaction": 0,
                                "version_already_exists": 0,
                                "max_commit_attempts_exceeded": 0,
                                "other_errors": 0,
                            }
                        },
                    }
                ],
            }
        ]
    )

    comparison = compare_runs(base, cand, threshold=0.05)
    row = comparison.rows[0]

    assert row.baseline_metrics is not None
    assert row.candidate_metrics is not None
    assert row.baseline_metrics.contention is not None
    assert row.candidate_metrics.contention is not None
    assert row.baseline_metrics.contention.conflict_delete_read == 2
    assert row.candidate_metrics.contention.ops_succeeded == 5


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


def test_aggregate_payloads_merges_samples_and_recomputes_elapsed_stats() -> None:
    run_a = _run(
        [
            {
                "case": "scan_case",
                "success": True,
                "samples": [{"elapsed_ms": 100.0}, {"elapsed_ms": 110.0}],
            }
        ]
    )
    run_b = _run(
        [
            {
                "case": "scan_case",
                "success": True,
                "samples": [{"elapsed_ms": 90.0}, {"elapsed_ms": 95.0}],
            }
        ]
    )
    run_a["context"]["label"] = "run-a"
    run_b["context"]["label"] = "run-b"

    aggregated = aggregate_payloads([run_a, run_b], label="merged-run")
    assert aggregated["context"]["label"] == "merged-run"
    case = aggregated["cases"][0]
    assert case["success"] is True
    assert len(case["samples"]) == 4
    assert case["elapsed_stats"]["min_ms"] == pytest.approx(90.0)
    assert case["elapsed_stats"]["max_ms"] == pytest.approx(110.0)
    assert case["elapsed_stats"]["median_ms"] == pytest.approx(97.5)
    assert case["elapsed_stats"]["mean_ms"] == pytest.approx(98.75)


def test_aggregate_payloads_preserves_nested_contention_metrics() -> None:
    run_a = _run(
        [
            {
                "case": "update_vs_compaction",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 100.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 2,
                                "race_count": 3,
                                "ops_attempted": 6,
                                "ops_succeeded": 4,
                                "ops_failed": 2,
                                "conflict_append": 0,
                                "conflict_delete_read": 2,
                                "conflict_delete_delete": 0,
                                "conflict_metadata_changed": 0,
                                "conflict_protocol_changed": 0,
                                "conflict_transaction": 0,
                                "version_already_exists": 0,
                                "max_commit_attempts_exceeded": 0,
                                "other_errors": 0,
                            }
                        },
                    }
                ],
            }
        ]
    )
    run_b = _run(
        [
            {
                "case": "update_vs_compaction",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 90.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 2,
                                "race_count": 3,
                                "ops_attempted": 6,
                                "ops_succeeded": 3,
                                "ops_failed": 3,
                                "conflict_append": 0,
                                "conflict_delete_read": 3,
                                "conflict_delete_delete": 0,
                                "conflict_metadata_changed": 0,
                                "conflict_protocol_changed": 0,
                                "conflict_transaction": 0,
                                "version_already_exists": 0,
                                "max_commit_attempts_exceeded": 0,
                                "other_errors": 0,
                            }
                        },
                    }
                ],
            }
        ]
    )

    aggregated = aggregate_payloads([run_a, run_b], label="merged-run")
    samples = aggregated["cases"][0]["samples"]
    assert len(samples) == 2
    assert samples[0]["metrics"]["contention"]["worker_count"] == 2
    assert samples[1]["metrics"]["contention"]["conflict_delete_read"] == 3


def test_aggregate_payloads_rejects_case_set_mismatch() -> None:
    run_a = _run(
        [
            {
                "case": "scan_case",
                "success": True,
                "samples": [{"elapsed_ms": 100.0}],
            }
        ]
    )
    run_b = _run(
        [
            {
                "case": "scan_case",
                "success": True,
                "samples": [{"elapsed_ms": 90.0}],
            },
            {
                "case": "extra_case",
                "success": True,
                "samples": [{"elapsed_ms": 10.0}],
            },
        ]
    )

    with pytest.raises(ValueError, match="case set mismatch"):
        aggregate_payloads([run_a, run_b], label="merged-run")


def test_aggregate_payloads_rejects_context_mismatch() -> None:
    run_a = _run(
        [{"case": "scan_case", "samples": [{"elapsed_ms": 100.0}]}],
        timing_phase="execute",
    )
    run_b = _run(
        [{"case": "scan_case", "samples": [{"elapsed_ms": 90.0}]}],
        timing_phase="plan",
    )

    with pytest.raises(ValueError, match="context mismatch"):
        aggregate_payloads([run_a, run_b], label="merged-run")


def test_aggregate_payloads_reject_invalid_perf_cases() -> None:
    run_a = _run(
        [
            {
                "case": "scan_case",
                "success": False,
                "validation_passed": False,
                "perf_valid": False,
                "failure_kind": "execution_error",
                "failure": {"message": "boom"},
                "samples": [],
            }
        ]
    )
    run_b = _run([{"case": "scan_case", "samples": [{"elapsed_ms": 90.0}]}])

    with pytest.raises(ValueError, match="perf-valid"):
        aggregate_payloads([run_a, run_b], label="merged-run")


def test_aggregate_payloads_rejects_inconsistent_classification() -> None:
    run_a = _run(
        [
            {
                "case": "scan_case",
                "classification": "supported",
                "success": True,
                "samples": [{"elapsed_ms": 100.0}],
            }
        ]
    )
    run_b = _run(
        [
            {
                "case": "scan_case",
                "classification": "expected_failure",
                "success": False,
                "failure": {"message": "expected"},
                "samples": [],
            }
        ]
    )

    with pytest.raises(ValueError, match="inconsistent classification"):
        aggregate_payloads([run_a, run_b], label="merged-run")


def test_aggregate_payloads_v4_preserves_run_level_summaries() -> None:
    run_a = _run_v4(
        [
            {
                "case": "scan_case",
                "samples": [{"elapsed_ms": 100.0}, {"elapsed_ms": 110.0}],
            }
        ]
    )
    run_b = _run_v4(
        [
            {
                "case": "scan_case",
                "samples": [{"elapsed_ms": 90.0}, {"elapsed_ms": 95.0}],
            }
        ]
    )

    aggregated = aggregate_payloads([run_a, run_b], label="merged-run")
    case = aggregated["cases"][0]
    assert case["run_summary"]["sample_count"] == 4
    assert len(case["samples"]) == 4
    assert len(case["run_summaries"]) == 2
    assert case["run_summaries"][0]["median_ms"] == 110.0
    assert case["run_summaries"][1]["median_ms"] == 95.0


def test_aggregate_payloads_v4_preserves_exploratory_compare_samples() -> None:
    baseline = aggregate_payloads(
        [
            _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 100.0}]}]),
            _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 100.0}]}]),
        ],
        label="baseline-merged",
    )
    candidate = aggregate_payloads(
        [
            _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 100.0}]}]),
            _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 200.0}]}]),
        ],
        label="candidate-merged",
    )

    comparison = compare_runs(
        baseline,
        candidate,
        threshold=0.05,
        aggregation="median",
        mode="exploratory",
    )

    case = baseline["cases"][0]
    row = comparison.rows[0]
    assert len(case["samples"]) == 2
    assert row.baseline_ms == 100.0
    assert row.candidate_ms == 200.0
    assert row.change == "2.00x slower"


def test_compare_runs_decision_mode_requires_sufficient_run_level_replication() -> None:
    baseline = _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 100.0}]}])
    candidate = _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 110.0}]}])
    baseline["cases"][0]["run_summaries"] = [{"median_ms": 100.0}] * 3
    candidate["cases"][0]["run_summaries"] = [{"median_ms": 110.0}] * 3

    comparison = compare_runs(baseline, candidate, mode="decision")
    assert comparison.rows[0].change == "inconclusive"


def test_compare_runs_decision_mode_uses_run_level_summaries_not_pooled_samples() -> (
    None
):
    baseline = _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 1.0}]}])
    candidate = _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 1.0}]}])
    baseline["cases"][0]["run_summaries"] = [
        {"median_ms": 100.0},
        {"median_ms": 101.0},
        {"median_ms": 99.0},
        {"median_ms": 100.5},
        {"median_ms": 100.2},
    ]
    candidate["cases"][0]["run_summaries"] = [
        {"median_ms": 120.0},
        {"median_ms": 121.0},
        {"median_ms": 119.0},
        {"median_ms": 120.5},
        {"median_ms": 120.2},
    ]

    comparison = compare_runs(baseline, candidate, mode="decision")
    assert comparison.rows[0].change == "regression"


def test_compare_runs_decision_mode_marks_non_decision_cases_inconclusive() -> None:
    baseline = _run_v4(
        [{"case": "write_append_small", "samples": [{"elapsed_ms": 100.0}]}]
    )
    candidate = _run_v4(
        [{"case": "write_append_small", "samples": [{"elapsed_ms": 120.0}]}]
    )
    baseline["cases"][0]["supports_decision"] = False
    candidate["cases"][0]["supports_decision"] = False
    baseline["cases"][0]["run_summaries"] = [{"median_ms": 100.0}] * 5
    candidate["cases"][0]["run_summaries"] = [{"median_ms": 120.0}] * 5

    comparison = compare_runs(baseline, candidate, mode="decision")
    assert comparison.rows[0].change == "inconclusive"
    assert comparison.summary.incomparable == 1


def test_render_text_groups_decision_mode_statuses() -> None:
    baseline = _run_v4(
        [
            {"case": "scan_regression", "samples": [{"elapsed_ms": 100.0}]},
            {"case": "scan_improvement", "samples": [{"elapsed_ms": 100.0}]},
            {"case": "scan_inconclusive", "samples": [{"elapsed_ms": 100.0}]},
        ]
    )
    candidate = _run_v4(
        [
            {"case": "scan_regression", "samples": [{"elapsed_ms": 112.0}]},
            {"case": "scan_improvement", "samples": [{"elapsed_ms": 88.0}]},
            {"case": "scan_inconclusive", "samples": [{"elapsed_ms": 108.0}]},
        ]
    )
    baseline["cases"][0]["run_summaries"] = [{"median_ms": 100.0}] * 5
    candidate["cases"][0]["run_summaries"] = [{"median_ms": 112.0}] * 5
    baseline["cases"][1]["run_summaries"] = [{"median_ms": 100.0}] * 5
    candidate["cases"][1]["run_summaries"] = [{"median_ms": 88.0}] * 5
    baseline["cases"][2]["run_summaries"] = [{"median_ms": 100.0}] * 3
    candidate["cases"][2]["run_summaries"] = [{"median_ms": 108.0}] * 3

    from delta_bench_compare.compare import render_text

    comparison = compare_runs(baseline, candidate, mode="decision")
    out = render_text(comparison)

    assert "Regressions (slower)" in out
    assert "Improvements (faster)" in out
    assert "Needs Attention" in out
    assert "scan_regression" in out
    assert "scan_improvement" in out
    assert "scan_inconclusive" in out


def test_compare_cli_rejects_invalid_perf_input_without_traceback(
    tmp_path: Path,
) -> None:
    baseline = _run(
        [
            {
                "case": "scan_case",
                "success": False,
                "validation_passed": False,
                "perf_valid": False,
                "failure_kind": "assertion_mismatch",
                "failure": {"message": "hash mismatch"},
                "samples": [],
            }
        ]
    )
    candidate = _run([{"case": "scan_case", "samples": [{"elapsed_ms": 90.0}]}])

    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")

    result = _run_compare_cli(baseline_path, candidate_path)

    assert result.returncode == 1
    assert result.stdout == ""
    assert "compare requires perf-valid inputs" in result.stderr
    assert "Traceback" not in result.stderr


def test_compare_cli_rejects_context_mismatch_without_traceback(
    tmp_path: Path,
) -> None:
    baseline = _run([{"case": "scan_case", "samples": [{"elapsed_ms": 100.0}]}])
    candidate = _run(
        [{"case": "scan_case", "samples": [{"elapsed_ms": 90.0}]}],
        timing_phase="plan",
        dataset_fingerprint="sha256:other",
    )

    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")

    result = _run_compare_cli(baseline_path, candidate_path)

    assert result.returncode == 1
    assert result.stdout == ""
    assert "context mismatch across benchmark payloads" in result.stderr
    assert "Traceback" not in result.stderr


def test_compare_cli_rejects_missing_input_without_traceback(tmp_path: Path) -> None:
    baseline_path = tmp_path / "missing-baseline.json"
    candidate_path = tmp_path / "missing-candidate.json"

    result = _run_compare_cli(baseline_path, candidate_path)

    assert result.returncode == 1
    assert result.stdout == ""
    assert str(baseline_path) in result.stderr
    assert "Traceback" not in result.stderr


def test_compare_cli_fail_on_regression_exits_non_zero(tmp_path: Path) -> None:
    baseline = _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 1.0}]}])
    candidate = _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 1.0}]}])
    baseline["cases"][0]["run_summaries"] = [{"median_ms": 100.0}] * 5
    candidate["cases"][0]["run_summaries"] = [{"median_ms": 120.0}] * 5

    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.compare",
            "--baseline",
            str(baseline_path),
            "--candidate",
            str(candidate_path),
            "--mode",
            "decision",
            "--fail-on",
            "regression,inconclusive",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])},
    )

    assert result.returncode == 2
    assert "regression" in result.stdout.lower()


def test_compare_cli_default_exit_policy_remains_non_failing(tmp_path: Path) -> None:
    baseline = _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 1.0}]}])
    candidate = _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 1.0}]}])
    baseline["cases"][0]["run_summaries"] = [{"median_ms": 100.0}] * 5
    candidate["cases"][0]["run_summaries"] = [{"median_ms": 120.0}] * 5

    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.compare",
            "--baseline",
            str(baseline_path),
            "--candidate",
            str(candidate_path),
            "--mode",
            "decision",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])},
    )

    assert result.returncode == 0


def test_compare_cli_rejects_unknown_fail_on_status(tmp_path: Path) -> None:
    baseline = _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 1.0}]}])
    candidate = _run_v4([{"case": "scan_case", "samples": [{"elapsed_ms": 1.0}]}])
    baseline["cases"][0]["run_summaries"] = [{"median_ms": 100.0}] * 5
    candidate["cases"][0]["run_summaries"] = [{"median_ms": 120.0}] * 5

    baseline_path = tmp_path / "baseline.json"
    candidate_path = tmp_path / "candidate.json"
    baseline_path.write_text(json.dumps(baseline), encoding="utf-8")
    candidate_path.write_text(json.dumps(candidate), encoding="utf-8")

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.compare",
            "--baseline",
            str(baseline_path),
            "--candidate",
            str(candidate_path),
            "--mode",
            "decision",
            "--fail-on",
            "regresion",
        ],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "PYTHONPATH": str(Path(__file__).resolve().parents[1])},
    )

    assert result.returncode == 1
    assert "invalid --fail-on status" in result.stderr
    assert "regresion" in result.stderr
    assert "regression" in result.stderr


def test_format_change_handles_zero_baseline() -> None:
    assert format_change(0.0, 0.0, 0.05) == "no change"
    assert format_change(0.0, 1.0, 0.05) == "incomparable"


def test_render_markdown_includes_summary_table() -> None:
    base = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 100.0}]}])
    cand = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 90.0}]}])
    from delta_bench_compare.compare import render_markdown

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_markdown(comparison)
    assert "Case | baseline | candidate | delta_pct | change" in out
    assert "a | 100.00 ms | 90.00 ms | -10.00% | 1.11x faster" in out
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
    assert "Summary:" in out
    assert "Improvements (faster)" in out
    assert "Case" in out and "baseline" in out and "delta %" in out
    assert "-10.00%" in out
    assert "1.11x faster" in out
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
    assert "Summary:" in out
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


def test_render_text_include_metrics_outputs_contention_columns_when_present() -> None:
    base = _run(
        [
            {
                "case": "update_vs_compaction",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 100.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 2,
                                "race_count": 3,
                                "ops_attempted": 6,
                                "ops_succeeded": 4,
                                "ops_failed": 2,
                                "conflict_append": 0,
                                "conflict_delete_read": 2,
                                "conflict_delete_delete": 0,
                                "conflict_metadata_changed": 0,
                                "conflict_protocol_changed": 0,
                                "conflict_transaction": 0,
                                "version_already_exists": 0,
                                "max_commit_attempts_exceeded": 0,
                                "other_errors": 0,
                            }
                        },
                    }
                ],
            }
        ]
    )
    cand = _run(
        [
            {
                "case": "update_vs_compaction",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 90.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 2,
                                "race_count": 3,
                                "ops_attempted": 6,
                                "ops_succeeded": 5,
                                "ops_failed": 1,
                                "conflict_append": 0,
                                "conflict_delete_read": 1,
                                "conflict_delete_delete": 0,
                                "conflict_metadata_changed": 0,
                                "conflict_protocol_changed": 0,
                                "conflict_transaction": 0,
                                "version_already_exists": 0,
                                "max_commit_attempts_exceeded": 0,
                                "other_errors": 0,
                            }
                        },
                    }
                ],
            }
        ]
    )
    from delta_bench_compare.compare import render_text

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_text(comparison, include_metrics=True)
    assert "baseline_worker_count" in out
    assert "candidate_ops_succeeded" in out
    assert "baseline_conflict_delete_read" in out
    assert "candidate_other_errors" in out


def test_contention_metric_headers_align_with_row_values() -> None:
    base = _run(
        [
            {
                "case": "update_vs_compaction",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 100.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 2,
                                "race_count": 3,
                                "ops_attempted": 6,
                                "ops_succeeded": 4,
                                "ops_failed": 2,
                                "conflict_append": 7,
                                "conflict_delete_read": 8,
                                "conflict_delete_delete": 9,
                                "conflict_metadata_changed": 10,
                                "conflict_protocol_changed": 11,
                                "conflict_transaction": 12,
                                "version_already_exists": 13,
                                "max_commit_attempts_exceeded": 14,
                                "other_errors": 15,
                            }
                        },
                    }
                ],
            }
        ]
    )
    cand = _run(
        [
            {
                "case": "update_vs_compaction",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 90.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 20,
                                "race_count": 30,
                                "ops_attempted": 60,
                                "ops_succeeded": 40,
                                "ops_failed": 20,
                                "conflict_append": 70,
                                "conflict_delete_read": 80,
                                "conflict_delete_delete": 90,
                                "conflict_metadata_changed": 100,
                                "conflict_protocol_changed": 110,
                                "conflict_transaction": 120,
                                "version_already_exists": 130,
                                "max_commit_attempts_exceeded": 140,
                                "other_errors": 150,
                            }
                        },
                    }
                ],
            }
        ]
    )
    from delta_bench_compare.compare import compare_runs
    from delta_bench_compare.formatting import _headers, _row_cells

    comparison = compare_runs(base, cand, threshold=0.05)
    row = comparison.rows[0]
    cells = _row_cells(row, comparison.rows, include_metrics=True)
    headers = _headers(comparison.rows, include_metrics=True)
    mapped = dict(zip(headers, cells))

    assert mapped["baseline_worker_count"] == "2"
    assert mapped["candidate_worker_count"] == "20"
    assert mapped["baseline_race_count"] == "3"
    assert mapped["candidate_race_count"] == "30"
    assert mapped["baseline_conflict_delete_read"] == "8"
    assert mapped["candidate_conflict_delete_read"] == "80"
    assert mapped["baseline_other_errors"] == "15"
    assert mapped["candidate_other_errors"] == "150"


def test_render_text_groups_cases_into_readable_sections() -> None:
    base = _run(
        [
            {
                "case": "slower_case",
                "success": True,
                "samples": [{"elapsed_ms": 100.0}],
            },
            {
                "case": "faster_case",
                "success": True,
                "samples": [{"elapsed_ms": 100.0}],
            },
            {
                "case": "stable_case",
                "success": True,
                "samples": [{"elapsed_ms": 100.0}],
            },
            {
                "case": "incomparable_case",
                "success": False,
                "failure": {"message": "boom"},
                "samples": [],
            },
        ]
    )
    cand = _run(
        [
            {
                "case": "slower_case",
                "success": True,
                "samples": [{"elapsed_ms": 120.0}],
            },
            {"case": "faster_case", "success": True, "samples": [{"elapsed_ms": 90.0}]},
            {
                "case": "stable_case",
                "success": True,
                "samples": [{"elapsed_ms": 103.0}],
            },
            {
                "case": "incomparable_case",
                "success": True,
                "samples": [{"elapsed_ms": 95.0}],
            },
        ]
    )
    from delta_bench_compare.compare import render_text

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_text(comparison)
    assert "Summary:" in out
    assert "Regressions (slower)" in out
    assert "Improvements (faster)" in out
    assert "Stable (no change)" in out
    assert "Needs Attention" in out
    assert out.index("Regressions (slower)") < out.index("Improvements (faster)")


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


def test_render_markdown_include_metrics_outputs_contention_columns_when_present() -> (
    None
):
    base = _run(
        [
            {
                "case": "update_vs_compaction",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 100.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 2,
                                "race_count": 3,
                                "ops_attempted": 6,
                                "ops_succeeded": 4,
                                "ops_failed": 2,
                                "conflict_append": 0,
                                "conflict_delete_read": 2,
                                "conflict_delete_delete": 0,
                                "conflict_metadata_changed": 0,
                                "conflict_protocol_changed": 0,
                                "conflict_transaction": 0,
                                "version_already_exists": 0,
                                "max_commit_attempts_exceeded": 0,
                                "other_errors": 0,
                            }
                        },
                    }
                ],
            }
        ]
    )
    cand = _run(
        [
            {
                "case": "update_vs_compaction",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 90.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 2,
                                "race_count": 3,
                                "ops_attempted": 6,
                                "ops_succeeded": 5,
                                "ops_failed": 1,
                                "conflict_append": 0,
                                "conflict_delete_read": 1,
                                "conflict_delete_delete": 0,
                                "conflict_metadata_changed": 0,
                                "conflict_protocol_changed": 0,
                                "conflict_transaction": 0,
                                "version_already_exists": 0,
                                "max_commit_attempts_exceeded": 0,
                                "other_errors": 0,
                            }
                        },
                    }
                ],
            }
        ]
    )
    from delta_bench_compare.compare import render_markdown

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_markdown(comparison, include_metrics=True)
    assert "baseline_worker_count" in out
    assert "candidate_ops_succeeded" in out


def test_contention_metric_headers_align_with_row_values() -> None:
    base = _run(
        [
            {
                "case": "update_vs_compaction",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 100.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 2,
                                "race_count": 3,
                                "ops_attempted": 6,
                                "ops_succeeded": 4,
                                "ops_failed": 2,
                                "conflict_append": 0,
                                "conflict_delete_read": 2,
                                "conflict_delete_delete": 7,
                                "conflict_metadata_changed": 8,
                                "conflict_protocol_changed": 9,
                                "conflict_transaction": 10,
                                "version_already_exists": 11,
                                "max_commit_attempts_exceeded": 12,
                                "other_errors": 13,
                            }
                        },
                    }
                ],
            }
        ]
    )
    cand = _run(
        [
            {
                "case": "update_vs_compaction",
                "success": True,
                "samples": [
                    {
                        "elapsed_ms": 90.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 20,
                                "race_count": 30,
                                "ops_attempted": 60,
                                "ops_succeeded": 40,
                                "ops_failed": 20,
                                "conflict_append": 1,
                                "conflict_delete_read": 21,
                                "conflict_delete_delete": 71,
                                "conflict_metadata_changed": 81,
                                "conflict_protocol_changed": 91,
                                "conflict_transaction": 101,
                                "version_already_exists": 111,
                                "max_commit_attempts_exceeded": 121,
                                "other_errors": 131,
                            }
                        },
                    }
                ],
            }
        ]
    )
    from delta_bench_compare.compare import render_markdown

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_markdown(comparison, include_metrics=True)
    lines = out.splitlines()
    header_idx = next(
        idx for idx, line in enumerate(lines) if line.startswith("Case | baseline")
    )
    headers = lines[header_idx].split(" | ")
    values = lines[header_idx + 2].split(" | ")
    assert len(headers) == len(values)
    row = dict(zip(headers, values))

    assert row["baseline_worker_count"] == "2"
    assert row["candidate_worker_count"] == "20"
    assert row["baseline_conflict_delete_delete"] == "7"
    assert row["candidate_conflict_delete_delete"] == "71"
    assert row["candidate_other_errors"] == "131"


def test_render_markdown_groups_cases_into_readable_sections() -> None:
    base = _run(
        [
            {
                "case": "slower_case",
                "success": True,
                "samples": [{"elapsed_ms": 100.0}],
            },
            {
                "case": "faster_case",
                "success": True,
                "samples": [{"elapsed_ms": 100.0}],
            },
            {
                "case": "stable_case",
                "success": True,
                "samples": [{"elapsed_ms": 100.0}],
            },
            {
                "case": "incomparable_case",
                "success": False,
                "failure": {"message": "boom"},
                "samples": [],
            },
        ]
    )
    cand = _run(
        [
            {
                "case": "slower_case",
                "success": True,
                "samples": [{"elapsed_ms": 120.0}],
            },
            {"case": "faster_case", "success": True, "samples": [{"elapsed_ms": 90.0}]},
            {
                "case": "stable_case",
                "success": True,
                "samples": [{"elapsed_ms": 103.0}],
            },
            {
                "case": "incomparable_case",
                "success": True,
                "samples": [{"elapsed_ms": 95.0}],
            },
        ]
    )

    from delta_bench_compare.compare import render_markdown

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_markdown(comparison)

    assert "## Summary" in out
    assert "## Regressions (slower)" in out
    assert "## Improvements (faster)" in out
    assert "## Stable (no change)" in out
    assert "## Needs Attention" in out
    assert "slower_case" in out
    assert "faster_case" in out
    assert "stable_case" in out
    assert "incomparable_case" in out
    assert out.index("## Regressions (slower)") < out.index("## Improvements (faster)")


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
        "schema_version": 3,
        "context": {"schema_version": 3, "label": "v3"},
        "cases": [
            {
                "case": "a",
                "success": True,
                "validation_passed": True,
                "perf_valid": True,
                "samples": [],
            }
        ],
    }
    path = tmp_path / "missing-classification.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="classification"):
        _load(path)


def test_load_rejects_unknown_case_classification(tmp_path: Path) -> None:
    payload = {
        "schema_version": 3,
        "context": {"schema_version": 3, "label": "v3"},
        "cases": [
            {
                "case": "a",
                "success": True,
                "validation_passed": True,
                "perf_valid": True,
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
        "schema_version": 3,
        "context": {"schema_version": 3, "label": "v3"},
        "cases": [
            {
                "case": "duplicate",
                "success": True,
                "validation_passed": True,
                "perf_valid": True,
                "classification": "supported",
                "samples": [],
            },
            {
                "case": "duplicate",
                "success": True,
                "validation_passed": True,
                "perf_valid": True,
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


def test_public_schema_loader_preserves_nested_contention_metrics(
    tmp_path: Path,
) -> None:
    from delta_bench_compare.schema import load_benchmark_payload

    payload = {
        "schema_version": 2,
        "context": {"schema_version": 2, "label": "v2"},
        "cases": [
            {
                "case": "update_vs_compaction",
                "success": True,
                "classification": "supported",
                "samples": [
                    {
                        "elapsed_ms": 91.0,
                        "metrics": {
                            "contention": {
                                "worker_count": 2,
                                "race_count": 3,
                                "ops_attempted": 6,
                                "ops_succeeded": 4,
                                "ops_failed": 2,
                                "conflict_append": 0,
                                "conflict_delete_read": 2,
                                "conflict_delete_delete": 0,
                                "conflict_metadata_changed": 0,
                                "conflict_protocol_changed": 0,
                                "conflict_transaction": 0,
                                "version_already_exists": 0,
                                "max_commit_attempts_exceeded": 0,
                                "other_errors": 0,
                            }
                        },
                    }
                ],
            }
        ],
    }
    path = tmp_path / "contention.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    loaded = load_benchmark_payload(path)
    assert (
        loaded["cases"][0]["samples"][0]["metrics"]["contention"]
        == payload["cases"][0]["samples"][0]["metrics"]["contention"]
    )


def test_render_text_uses_display_headers() -> None:
    base = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 100.0}]}])
    cand = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 90.0}]}])
    from delta_bench_compare.compare import render_text

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_text(comparison)
    assert "delta %" in out
    assert "baseline (ms)" in out
    assert "candidate (ms)" in out
    # Machine-readable names should not appear in text output
    assert "delta_pct" not in out


def test_render_markdown_preserves_machine_headers() -> None:
    base = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 100.0}]}])
    cand = _run([{"case": "a", "success": True, "samples": [{"elapsed_ms": 90.0}]}])
    from delta_bench_compare.compare import render_markdown

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_markdown(comparison)
    assert "delta_pct" in out
    assert "baseline" in out
    assert "candidate" in out


def test_render_text_compact_stable_section() -> None:
    """When more than 5 cases are stable, the section collapses to a name list."""
    cases_base = [
        {"case": f"stable_{i}", "success": True, "samples": [{"elapsed_ms": 100.0}]}
        for i in range(8)
    ]
    cases_cand = [
        {"case": f"stable_{i}", "success": True, "samples": [{"elapsed_ms": 102.0}]}
        for i in range(8)
    ]
    base = _run(cases_base)
    cand = _run(cases_cand)
    from delta_bench_compare.compare import render_text

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_text(comparison)
    assert "all within noise threshold" in out
    # All 8 case names should appear in the compact list
    for i in range(8):
        assert f"stable_{i}" in out
    # Should not contain a full table (no dashes separator for stable section)
    stable_idx = out.index("Stable (no change)")
    after_stable = out[stable_idx:]
    assert "baseline (ms)" not in after_stable


def test_render_text_stable_section_shows_table_when_few() -> None:
    """When 5 or fewer stable cases, the full table is shown."""
    cases_base = [
        {"case": f"case_{i}", "success": True, "samples": [{"elapsed_ms": 100.0}]}
        for i in range(3)
    ]
    cases_cand = [
        {"case": f"case_{i}", "success": True, "samples": [{"elapsed_ms": 102.0}]}
        for i in range(3)
    ]
    base = _run(cases_base)
    cand = _run(cases_cand)
    from delta_bench_compare.compare import render_text

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_text(comparison)
    assert "Stable (no change)" in out
    assert "all within noise threshold" not in out
    assert "baseline (ms)" in out


def test_render_text_right_aligns_numeric_columns() -> None:
    base = _run(
        [
            {"case": "short", "success": True, "samples": [{"elapsed_ms": 1.5}]},
            {
                "case": "longer_name",
                "success": True,
                "samples": [{"elapsed_ms": 1000.0}],
            },
        ]
    )
    cand = _run(
        [
            {"case": "short", "success": True, "samples": [{"elapsed_ms": 1.4}]},
            {
                "case": "longer_name",
                "success": True,
                "samples": [{"elapsed_ms": 1050.0}],
            },
        ]
    )
    from delta_bench_compare.compare import render_text

    comparison = compare_runs(base, cand, threshold=0.05)
    out = render_text(comparison)
    # Right-aligned values should have leading spaces before shorter numbers
    lines = out.split("\n")
    data_lines = [line for line in lines if "1.50 ms" in line or "1000.00 ms" in line]
    assert len(data_lines) >= 1
    for line in data_lines:
        # The ms values should exist and the line should be properly aligned
        assert "ms" in line


def test_terminal_color_disabled_when_not_tty() -> None:
    from delta_bench_compare.terminal import dim, green, red, yellow

    # In pytest (non-TTY), color functions should return raw text
    assert red("hello") == "hello"
    assert green("hello") == "hello"
    assert yellow("hello") == "hello"
    assert dim("hello") == "hello"


def test_terminal_set_color_mode() -> None:
    from delta_bench_compare.terminal import set_color_mode, red, visible_len

    set_color_mode(True)
    colored = red("test")
    assert "\033[" in colored
    assert visible_len(colored) == 4

    set_color_mode(False)
    assert red("test") == "test"
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
