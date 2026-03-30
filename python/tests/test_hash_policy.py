from __future__ import annotations

from delta_bench_compare.hash_policy import (
    analyze_hash_policy,
    render_hash_policy_report,
    render_hash_policy_text,
)


def _case(
    *,
    case: str,
    success: bool,
    result_hashes: list[str] | None = None,
    failure_message: str | None = None,
) -> dict:
    samples = []
    for value in result_hashes or []:
        samples.append({"elapsed_ms": 1.0, "metrics": {"result_hash": value}})
    payload: dict = {
        "case": case,
        "success": success,
        "classification": "supported",
        "samples": samples,
    }
    if failure_message is not None:
        payload["failure"] = {"message": failure_message}
    return payload


def _run(cases: list[dict]) -> dict:
    return {
        "schema_version": 2,
        "context": {"schema_version": 2, "label": "test"},
        "cases": cases,
    }


def _mismatch(expected: str, found: str) -> str:
    return f"result hash mismatch: expected '{expected}', found '{found}'"


def test_analyze_hash_policy_classifies_stale_manifest_case() -> None:
    base = _run(
        [
            _case(
                case="scan_pruning_miss",
                success=False,
                result_hashes=["sha256:new"],
                failure_message=_mismatch("sha256:old", "sha256:new"),
            )
        ]
    )
    cand = _run(
        [
            _case(
                case="scan_pruning_miss",
                success=False,
                result_hashes=["sha256:new"],
                failure_message=_mismatch("sha256:old", "sha256:new"),
            )
        ]
    )

    analysis = analyze_hash_policy(base, cand)

    assert [c.case for c in analysis.stale_manifest_cases] == ["scan_pruning_miss"]
    assert analysis.candidate_only_mismatch_cases == []
    assert analysis.nondeterministic_cases == []


def test_analyze_hash_policy_classifies_candidate_only_mismatch() -> None:
    base = _run(
        [_case(case="metadata_load", success=True, result_hashes=["sha256:ok"])]
    )
    cand = _run(
        [
            _case(
                case="metadata_load",
                success=False,
                result_hashes=["sha256:new"],
                failure_message=_mismatch("sha256:old", "sha256:new"),
            )
        ]
    )

    analysis = analyze_hash_policy(base, cand)

    assert [c.case for c in analysis.candidate_only_mismatch_cases] == ["metadata_load"]
    assert analysis.stale_manifest_cases == []
    assert analysis.nondeterministic_cases == []


def test_analyze_hash_policy_detects_nondeterministic_hashes() -> None:
    base = _run(
        [
            _case(
                case="tpcds_q07",
                success=False,
                result_hashes=["sha256:a", "sha256:b"],
                failure_message=_mismatch("sha256:old", "sha256:a"),
            )
        ]
    )
    cand = _run(
        [
            _case(
                case="tpcds_q07",
                success=False,
                result_hashes=["sha256:a"],
                failure_message=_mismatch("sha256:old", "sha256:a"),
            )
        ]
    )

    analysis = analyze_hash_policy(base, cand)

    assert analysis.stale_manifest_cases == []
    assert analysis.candidate_only_mismatch_cases == []
    assert [c.case for c in analysis.nondeterministic_cases] == ["tpcds_q07"]


def test_render_hash_policy_text_includes_actionable_sections() -> None:
    base = _run(
        [
            _case(
                case="scan_pruning_miss",
                success=False,
                result_hashes=["sha256:new"],
                failure_message=_mismatch("sha256:old", "sha256:new"),
            ),
            _case(
                case="tpcds_q07",
                success=False,
                result_hashes=["sha256:a", "sha256:b"],
                failure_message=_mismatch("sha256:old", "sha256:a"),
            ),
        ]
    )
    cand = _run(
        [
            _case(
                case="scan_pruning_miss",
                success=False,
                result_hashes=["sha256:new"],
                failure_message=_mismatch("sha256:old", "sha256:new"),
            ),
            _case(
                case="metadata_load",
                success=False,
                result_hashes=["sha256:new"],
                failure_message=_mismatch("sha256:old", "sha256:new"),
            ),
            _case(
                case="tpcds_q07",
                success=False,
                result_hashes=["sha256:a"],
                failure_message=_mismatch("sha256:old", "sha256:a"),
            ),
        ]
    )

    output = render_hash_policy_text(analyze_hash_policy(base, cand))

    assert "Hash Assertion Triage" in output
    assert "Stale manifest hash candidates" in output
    assert "Candidate-only hash mismatches" in output
    assert "Nondeterministic hash candidates" in output
    assert "scan_pruning_miss" in output
    assert "metadata_load" in output
    assert "tpcds_q07" in output
def test_hash_policy_report_detects_mismatch_in_later_samples() -> None:
    baseline = {
        "schema_version": 4,
        "context": {"schema_version": 4},
        "cases": [
            {
                "case": "scan_case",
                "samples": [
                    {
                        "metrics": {
                            "result_hash": "sha256:same",
                            "schema_hash": "sha256:schema",
                        }
                    },
                    {
                        "metrics": {
                            "result_hash": "sha256:baseline-later",
                            "schema_hash": "sha256:schema",
                        }
                    },
                ],
            }
        ],
    }
    candidate = {
        "schema_version": 4,
        "context": {"schema_version": 4},
        "cases": [
            {
                "case": "scan_case",
                "samples": [
                    {
                        "metrics": {
                            "result_hash": "sha256:same",
                            "schema_hash": "sha256:schema",
                        }
                    },
                    {
                        "metrics": {
                            "result_hash": "sha256:candidate-later",
                            "schema_hash": "sha256:schema",
                        }
                    },
                ],
            }
        ],
    }

    report = render_hash_policy_report(baseline, candidate)

    assert "scan_case" in report
    assert "baseline-later" in report
    assert "candidate-later" in report
