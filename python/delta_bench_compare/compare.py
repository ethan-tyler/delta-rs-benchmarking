from __future__ import annotations

import argparse
import math
from pathlib import Path

from .formatting import (
    render_markdown as render_markdown_output,
    render_text_report,
)
from .model import Comparison, ComparisonRow, SampleMetricSnapshot, Summary
from .schema import case_classification, load_benchmark_payload

VALID_AGGREGATIONS = {"min", "median", "p95"}


def representative_sample(case: dict, aggregation: str = "median") -> dict | None:
    if aggregation not in VALID_AGGREGATIONS:
        raise ValueError(
            f"unknown aggregation '{aggregation}'; expected one of: min, median, p95"
        )
    if not case.get("success", True):
        return None
    samples = case.get("samples") or []
    elapsed_samples = [sample for sample in samples if "elapsed_ms" in sample]
    if not elapsed_samples:
        return None
    sorted_samples = sorted(
        elapsed_samples, key=lambda sample: float(sample["elapsed_ms"])
    )
    if aggregation == "min":
        return sorted_samples[0]
    if aggregation == "median":
        return sorted_samples[len(sorted_samples) // 2]
    # Nearest-rank p95
    idx = max(
        0, min(len(sorted_samples) - 1, math.ceil(0.95 * len(sorted_samples)) - 1)
    )
    return sorted_samples[idx]


def representative_ms(case: dict, aggregation: str = "median") -> float | None:
    sample = representative_sample(case, aggregation=aggregation)
    if sample is None:
        return None
    return float(sample["elapsed_ms"])


def _metric_as_int(metrics: dict, key: str) -> int | None:
    value = metrics.get(key)
    if value is None:
        return None
    return int(value)


def best_sample_metrics(
    case: dict, aggregation: str = "median"
) -> SampleMetricSnapshot | None:
    sample = representative_sample(case, aggregation=aggregation)
    if sample is None:
        return None

    metrics = sample.get("metrics") or {}
    return SampleMetricSnapshot(
        files_scanned=_metric_as_int(metrics, "files_scanned"),
        files_pruned=_metric_as_int(metrics, "files_pruned"),
        bytes_scanned=_metric_as_int(metrics, "bytes_scanned"),
        scan_time_ms=_metric_as_int(metrics, "scan_time_ms"),
        rewrite_time_ms=_metric_as_int(metrics, "rewrite_time_ms"),
    )


def format_change(baseline_ms: float, candidate_ms: float, threshold: float) -> str:
    if baseline_ms <= 0.0:
        if candidate_ms <= 0.0:
            return "no change"
        return "incomparable"
    if candidate_ms <= 0.0:
        return "incomparable"

    delta = (candidate_ms - baseline_ms) / baseline_ms
    if abs(delta) <= threshold:
        return "no change"
    ratio = baseline_ms / candidate_ms
    if candidate_ms < baseline_ms:
        return f"{ratio:.2f}x faster"
    return f"{1 / ratio:.2f}x slower"


def compare_runs(
    baseline: dict,
    candidate: dict,
    threshold: float = 0.05,
    aggregation: str = "median",
) -> Comparison:
    if aggregation not in VALID_AGGREGATIONS:
        raise ValueError(
            f"unknown aggregation '{aggregation}'; expected one of: min, median, p95"
        )

    baseline_cases = {c["case"]: c for c in baseline.get("cases", [])}
    candidate_cases = {c["case"]: c for c in candidate.get("cases", [])}
    names = sorted(set(baseline_cases) | set(candidate_cases))

    rows: list[ComparisonRow] = []
    faster = slower = no_change = incomparable = new = removed = 0

    for name in names:
        b = baseline_cases.get(name)
        c = candidate_cases.get(name)
        baseline_classification = case_classification(b)
        candidate_classification = case_classification(c)

        if b is None and c is not None:
            new += 1
            rows.append(
                ComparisonRow(
                    name,
                    None,
                    representative_ms(c, aggregation=aggregation),
                    "new",
                    baseline_classification=baseline_classification,
                    candidate_classification=candidate_classification,
                    baseline_metrics=None,
                    candidate_metrics=best_sample_metrics(c, aggregation=aggregation),
                )
            )
            continue
        if c is None and b is not None:
            removed += 1
            rows.append(
                ComparisonRow(
                    name,
                    representative_ms(b, aggregation=aggregation),
                    None,
                    "removed",
                    baseline_classification=baseline_classification,
                    candidate_classification=candidate_classification,
                    baseline_metrics=best_sample_metrics(b, aggregation=aggregation),
                    candidate_metrics=None,
                )
            )
            continue

        if b is None or c is None:
            raise ValueError(f"inconsistent comparison state for case '{name}'")
        if (
            baseline_classification == "expected_failure"
            or candidate_classification == "expected_failure"
        ):
            incomparable += 1
            rows.append(
                ComparisonRow(
                    name,
                    representative_ms(b, aggregation=aggregation),
                    representative_ms(c, aggregation=aggregation),
                    "expected_failure",
                    baseline_classification=baseline_classification,
                    candidate_classification=candidate_classification,
                    baseline_metrics=best_sample_metrics(b, aggregation=aggregation),
                    candidate_metrics=best_sample_metrics(c, aggregation=aggregation),
                )
            )
            continue
        base_ms = representative_ms(b, aggregation=aggregation)
        cand_ms = representative_ms(c, aggregation=aggregation)

        if base_ms is None or cand_ms is None:
            incomparable += 1
            rows.append(
                ComparisonRow(
                    name,
                    base_ms,
                    cand_ms,
                    "incomparable",
                    baseline_classification=baseline_classification,
                    candidate_classification=candidate_classification,
                    baseline_metrics=best_sample_metrics(b, aggregation=aggregation),
                    candidate_metrics=best_sample_metrics(c, aggregation=aggregation),
                )
            )
            continue

        change = format_change(base_ms, cand_ms, threshold)
        if "faster" in change:
            faster += 1
        elif "slower" in change:
            slower += 1
        else:
            no_change += 1
        rows.append(
            ComparisonRow(
                name,
                base_ms,
                cand_ms,
                change,
                baseline_classification=baseline_classification,
                candidate_classification=candidate_classification,
                baseline_metrics=best_sample_metrics(b, aggregation=aggregation),
                candidate_metrics=best_sample_metrics(c, aggregation=aggregation),
            )
        )

    summary = Summary(
        faster=faster,
        slower=slower,
        no_change=no_change,
        incomparable=incomparable,
        new=new,
        removed=removed,
    )
    return Comparison(rows=rows, summary=summary)


def _load(path: Path) -> dict:
    return load_benchmark_payload(path)


def render_text(comparison: Comparison, include_metrics: bool = False) -> str:
    return render_text_report(comparison, include_metrics=include_metrics)


def render_markdown(comparison: Comparison, include_metrics: bool = False) -> str:
    return render_markdown_output(comparison, include_metrics=include_metrics)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare delta-bench JSON results")
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--noise-threshold", type=float, default=0.05)
    parser.add_argument(
        "--aggregation", choices=["min", "median", "p95"], default="median"
    )
    parser.add_argument("--format", choices=["text", "markdown"], default="text")
    parser.add_argument("--include-metrics", action="store_true")
    args = parser.parse_args()

    comparison = compare_runs(
        _load(args.baseline),
        _load(args.candidate),
        threshold=args.noise_threshold,
        aggregation=args.aggregation,
    )
    output = (
        render_markdown(comparison, include_metrics=args.include_metrics)
        if args.format == "markdown"
        else render_text(comparison, include_metrics=args.include_metrics)
    )
    print(output)


if __name__ == "__main__":
    main()
