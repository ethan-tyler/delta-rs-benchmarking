from __future__ import annotations

import argparse
import math
import random
import statistics
from pathlib import Path

from .formatting import (
    render_markdown as render_markdown_output,
    render_text_report,
)
from .model import (
    Comparison,
    ComparisonRow,
    ContentionMetricSnapshot,
    SampleMetricSnapshot,
    Summary,
)
from .schema import (
    case_classification,
    ensure_matching_contexts,
    invalid_perf_case_names,
    load_benchmark_payload,
)

VALID_AGGREGATIONS = {"min", "median", "p95"}
VALID_COMPARE_MODES = {"exploratory", "decision"}


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
    contention = metrics.get("contention") or {}
    return SampleMetricSnapshot(
        files_scanned=_metric_as_int(metrics, "files_scanned"),
        files_pruned=_metric_as_int(metrics, "files_pruned"),
        bytes_scanned=_metric_as_int(metrics, "bytes_scanned"),
        scan_time_ms=_metric_as_int(metrics, "scan_time_ms"),
        rewrite_time_ms=_metric_as_int(metrics, "rewrite_time_ms"),
        contention=(
            None
            if not contention
            else ContentionMetricSnapshot(
                worker_count=_metric_as_int(contention, "worker_count"),
                race_count=_metric_as_int(contention, "race_count"),
                ops_attempted=_metric_as_int(contention, "ops_attempted"),
                ops_succeeded=_metric_as_int(contention, "ops_succeeded"),
                ops_failed=_metric_as_int(contention, "ops_failed"),
                conflict_append=_metric_as_int(contention, "conflict_append"),
                conflict_delete_read=_metric_as_int(contention, "conflict_delete_read"),
                conflict_delete_delete=_metric_as_int(
                    contention, "conflict_delete_delete"
                ),
                conflict_metadata_changed=_metric_as_int(
                    contention, "conflict_metadata_changed"
                ),
                conflict_protocol_changed=_metric_as_int(
                    contention, "conflict_protocol_changed"
                ),
                conflict_transaction=_metric_as_int(contention, "conflict_transaction"),
                version_already_exists=_metric_as_int(
                    contention, "version_already_exists"
                ),
                max_commit_attempts_exceeded=_metric_as_int(
                    contention, "max_commit_attempts_exceeded"
                ),
                other_errors=_metric_as_int(contention, "other_errors"),
            )
        ),
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


def _case_run_summaries(case: dict) -> list[dict]:
    summaries = case.get("run_summaries")
    if isinstance(summaries, list) and summaries:
        return [summary for summary in summaries if isinstance(summary, dict)]
    summary = case.get("run_summary")
    if isinstance(summary, dict):
        return [summary]
    return []


def _run_metric_values(case: dict, metric: str) -> list[float]:
    values = []
    for summary in _case_run_summaries(case):
        value = summary.get(f"{metric}_ms")
        if value is None and metric == "median":
            value = summary.get("median_ms")
        if value is None:
            continue
        values.append(float(value))
    return values


def _bootstrap_relative_change_ci(
    baseline_values: list[float],
    candidate_values: list[float],
    *,
    iterations: int = 5000,
    seed: int = 0,
) -> tuple[float, float]:
    rng = random.Random(seed)
    changes: list[float] = []
    for _ in range(iterations):
        baseline_sample = [
            baseline_values[rng.randrange(len(baseline_values))]
            for _ in range(len(baseline_values))
        ]
        candidate_sample = [
            candidate_values[rng.randrange(len(candidate_values))]
            for _ in range(len(candidate_values))
        ]
        baseline_metric = statistics.median(baseline_sample)
        candidate_metric = statistics.median(candidate_sample)
        if baseline_metric <= 0.0:
            continue
        changes.append((candidate_metric - baseline_metric) / baseline_metric)
    if not changes:
        return float("nan"), float("nan")
    changes.sort()
    low_idx = max(0, int(0.025 * len(changes)) - 1)
    high_idx = min(len(changes) - 1, int(0.975 * len(changes)))
    return changes[low_idx], changes[high_idx]


def _decision_change(case: dict, baseline: list[float], candidate: list[float]) -> str:
    required_runs = int(case.get("required_runs") or 5)
    if len(baseline) < required_runs or len(candidate) < required_runs:
        return "inconclusive"
    threshold_pct = float(case.get("decision_threshold_pct") or 5.0)
    threshold = threshold_pct / 100.0
    low, high = _bootstrap_relative_change_ci(baseline, candidate)
    if math.isnan(low) or math.isnan(high):
        return "inconclusive"
    if low > threshold:
        return "regression"
    if high < -threshold:
        return "improvement"
    if -threshold <= low and high <= threshold:
        return "no change"
    return "inconclusive"


def compare_runs(
    baseline: dict,
    candidate: dict,
    threshold: float = 0.05,
    aggregation: str = "median",
    mode: str = "exploratory",
) -> Comparison:
    if mode not in VALID_COMPARE_MODES:
        raise ValueError(
            f"unknown compare mode '{mode}'; expected one of: exploratory, decision"
        )
    if aggregation not in VALID_AGGREGATIONS:
        raise ValueError(
            f"unknown aggregation '{aggregation}'; expected one of: min, median, p95"
        )

    ensure_matching_contexts(baseline, candidate)

    baseline_cases = {c["case"]: c for c in baseline.get("cases", [])}
    candidate_cases = {c["case"]: c for c in candidate.get("cases", [])}
    invalid_cases = invalid_perf_case_names((baseline, candidate))
    if invalid_cases:
        raise ValueError(
            "compare requires perf-valid inputs; invalid cases present: "
            + ", ".join(invalid_cases)
        )
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

        if mode == "decision":
            if baseline.get("schema_version") != 4 or candidate.get("schema_version") != 4:
                raise ValueError("decision mode requires schema v4 inputs")
            if b.get("compatibility_key") != c.get("compatibility_key"):
                raise ValueError(
                    f"compatibility mismatch for case '{name}': {b.get('compatibility_key')!r}!={c.get('compatibility_key')!r}"
                )
            metric = str(c.get("decision_metric") or "median")
            baseline_values = _run_metric_values(b, metric)
            candidate_values = _run_metric_values(c, metric)
            base_ms = statistics.median(baseline_values) if baseline_values else None
            cand_ms = statistics.median(candidate_values) if candidate_values else None
            change = _decision_change(c, baseline_values, candidate_values)
            if change == "improvement":
                faster += 1
            elif change == "regression":
                slower += 1
            elif change == "no change":
                no_change += 1
            else:
                incomparable += 1
            rows.append(
                ComparisonRow(
                    name,
                    base_ms,
                    cand_ms,
                    change,
                    baseline_classification=baseline_classification,
                    candidate_classification=candidate_classification,
                    baseline_metrics=None,
                    candidate_metrics=None,
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
        "--mode", choices=["exploratory", "decision"], default="exploratory"
    )
    parser.add_argument(
        "--aggregation", choices=["min", "median", "p95"], default="median"
    )
    parser.add_argument("--format", choices=["text", "markdown"], default="text")
    parser.add_argument("--include-metrics", action="store_true")
    parser.add_argument(
        "--color",
        choices=["auto", "always", "never"],
        default="auto",
        help="Control ANSI color output (default: auto, detects TTY)",
    )
    args = parser.parse_args()

    if args.color != "auto":
        from .terminal import set_color_mode

        set_color_mode(args.color == "always")

    comparison = compare_runs(
        _load(args.baseline),
        _load(args.candidate),
        threshold=args.noise_threshold,
        aggregation=args.aggregation,
        mode=args.mode,
    )
    output = (
        render_markdown(comparison, include_metrics=args.include_metrics)
        if args.format == "markdown"
        else render_text(comparison, include_metrics=args.include_metrics)
    )
    print(output)


if __name__ == "__main__":
    main()
