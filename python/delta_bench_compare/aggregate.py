from __future__ import annotations

import argparse
import copy
import json
import math
from pathlib import Path
from typing import Any

from .schema import (
    ensure_matching_contexts,
    invalid_perf_case_names,
    load_benchmark_payload,
)


def _compute_elapsed_stats(samples: list[dict[str, Any]]) -> dict[str, float] | None:
    elapsed = [
        float(sample["elapsed_ms"]) for sample in samples if "elapsed_ms" in sample
    ]
    if not elapsed:
        return None

    elapsed_sorted = sorted(elapsed)
    count = len(elapsed_sorted)
    total = sum(elapsed_sorted)
    mean_ms = total / count
    min_ms = elapsed_sorted[0]
    max_ms = elapsed_sorted[-1]
    if count % 2 == 0:
        median_ms = (elapsed_sorted[count // 2 - 1] + elapsed_sorted[count // 2]) / 2.0
    else:
        median_ms = elapsed_sorted[count // 2]
    variance = sum((value - mean_ms) ** 2 for value in elapsed_sorted) / count
    stddev_ms = math.sqrt(variance)
    cv_pct = None
    if not math.isclose(mean_ms, 0.0, abs_tol=1e-12):
        cv_pct = (stddev_ms / abs(mean_ms)) * 100.0

    result = {
        "min_ms": min_ms,
        "max_ms": max_ms,
        "mean_ms": mean_ms,
        "median_ms": median_ms,
        "stddev_ms": stddev_ms,
    }
    if cv_pct is not None:
        result["cv_pct"] = cv_pct
    return result


def _run_summary_from_case(case: dict[str, Any]) -> dict[str, Any]:
    summary = case.get("run_summary")
    if isinstance(summary, dict):
        return copy.deepcopy(summary)
    elapsed = [float(sample["elapsed_ms"]) for sample in case.get("samples") or [] if "elapsed_ms" in sample]
    stats = _compute_elapsed_stats(case.get("samples") or [])
    out: dict[str, Any] = {
        "sample_count": len(elapsed),
        "invalid_sample_count": 0,
    }
    if stats:
        out.update(
            {
                "min_ms": stats.get("min_ms"),
                "max_ms": stats.get("max_ms"),
                "mean_ms": stats.get("mean_ms"),
                "median_ms": stats.get("median_ms"),
            }
        )
    return out


def _aggregate_payloads_v4(payloads: list[dict[str, Any]], label: str) -> dict[str, Any]:
    first = copy.deepcopy(payloads[0])
    first_case_order = [case["case"] for case in first.get("cases", [])]
    first_case_set = set(first_case_order)
    for payload in payloads[1:]:
        if payload.get("schema_version") != first.get("schema_version"):
            raise ValueError("cannot aggregate payloads with different schema versions")
        if payload.get("context", {}).get("suite") != first.get("context", {}).get("suite"):
            raise ValueError("cannot aggregate payloads across different suites")
        ensure_matching_contexts(first, payload)
        payload_case_set = {case["case"] for case in payload.get("cases", [])}
        if payload_case_set != first_case_set:
            missing = sorted(first_case_set - payload_case_set)
            extra = sorted(payload_case_set - first_case_set)
            raise ValueError(
                f"case set mismatch across payloads; missing={missing}, extra={extra}"
            )

    first["context"]["label"] = label

    out_cases: list[dict[str, Any]] = []
    for case_name in first_case_order:
        variants = []
        for payload in payloads:
            lookup = {case["case"]: case for case in payload.get("cases", [])}
            variants.append(lookup[case_name])

        merged = copy.deepcopy(variants[0])
        merged["run_summaries"] = [_run_summary_from_case(variant) for variant in variants]
        out_cases.append(merged)

    first["cases"] = out_cases
    return first


def aggregate_payloads(payloads: list[dict[str, Any]], label: str) -> dict[str, Any]:
    if not payloads:
        raise ValueError("at least one payload is required for aggregation")

    invalid_cases = invalid_perf_case_names(payloads)
    if invalid_cases:
        raise ValueError(
            "aggregate requires perf-valid inputs; invalid cases present: "
            + ", ".join(invalid_cases)
        )

    if int(payloads[0].get("schema_version") or 0) >= 4:
        return _aggregate_payloads_v4(payloads, label)

    first = copy.deepcopy(payloads[0])
    first_case_order = [case["case"] for case in first.get("cases", [])]
    first_case_set = set(first_case_order)
    for payload in payloads[1:]:
        if payload.get("schema_version") != first.get("schema_version"):
            raise ValueError("cannot aggregate payloads with different schema versions")
        if payload.get("context", {}).get("suite") != first.get("context", {}).get(
            "suite"
        ):
            raise ValueError("cannot aggregate payloads across different suites")
        ensure_matching_contexts(first, payload)
        payload_case_set = {case["case"] for case in payload.get("cases", [])}
        if payload_case_set != first_case_set:
            missing = sorted(first_case_set - payload_case_set)
            extra = sorted(payload_case_set - first_case_set)
            raise ValueError(
                f"case set mismatch across payloads; missing={missing}, extra={extra}"
            )

    first["context"]["label"] = label
    first["context"]["iterations"] = sum(
        int(payload.get("context", {}).get("iterations", 0)) for payload in payloads
    )

    out_cases: list[dict[str, Any]] = []

    for case_name in first_case_order:
        variants: list[dict[str, Any]] = []
        for payload in payloads:
            lookup = {case["case"]: case for case in payload.get("cases", [])}
            if case_name not in lookup:
                raise ValueError(
                    f"case '{case_name}' missing from one or more payloads"
                )
            variants.append(lookup[case_name])

        merged = copy.deepcopy(variants[0])
        merged_samples: list[dict[str, Any]] = []
        for variant in variants:
            merged_samples.extend(variant.get("samples") or [])

        merged["samples"] = merged_samples
        merged["validation_passed"] = all(
            bool(variant.get("validation_passed")) for variant in variants
        )
        merged["perf_valid"] = all(bool(variant.get("perf_valid")) for variant in variants)
        merged["elapsed_stats"] = (
            _compute_elapsed_stats(merged_samples) if merged["perf_valid"] else None
        )

        classifications = {
            variant.get("classification", "supported") for variant in variants
        }
        if len(classifications) != 1:
            raise ValueError(
                f"case '{case_name}' has inconsistent classification across payloads: "
                f"{sorted(classifications)}"
            )
        merged["classification"] = variants[0].get("classification", "supported")
        merged["success"] = all(bool(variant.get("success")) for variant in variants)
        merged_failure_kinds = {
            variant.get("failure_kind") for variant in variants if variant.get("failure_kind")
        }
        if not merged_failure_kinds:
            merged["failure_kind"] = None
        elif len(merged_failure_kinds) == 1:
            merged["failure_kind"] = next(iter(merged_failure_kinds))
        else:
            merged["failure_kind"] = "execution_error"

        if merged["success"]:
            merged["failure"] = None
        else:
            messages = []
            for variant in variants:
                failure = variant.get("failure") or {}
                message = failure.get("message")
                if message and message not in messages:
                    messages.append(message)
            merged["failure"] = {
                "message": " | ".join(messages)
                if messages
                else "one or more aggregated runs failed"
            }

        out_cases.append(merged)

    first["cases"] = out_cases
    return first


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Aggregate multiple delta-bench JSON run payloads into one result"
    )
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--label", required=True)
    parser.add_argument("inputs", nargs="+", type=Path)
    args = parser.parse_args()

    payloads = [load_benchmark_payload(path) for path in args.inputs]
    aggregated = aggregate_payloads(payloads, label=args.label)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(aggregated, indent=2) + "\n", encoding="utf-8")
    print(f"wrote aggregated result: {args.output}")


if __name__ == "__main__":
    main()
