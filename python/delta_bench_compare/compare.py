from __future__ import annotations

import argparse
import json
import math
import random
import statistics
import sys
from pathlib import Path

from .formatting import (
    render_markdown as render_markdown_output,
    render_text_report,
)
from .model import (
    VALID_COMPARISON_STATUSES,
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
COMPARISON_JSON_SCHEMA_VERSION = 1
VALID_FAIL_ON_STATUSES = VALID_COMPARISON_STATUSES
FAIL_ON_STATUS_ALIASES = {"no change": "no_change"}


def classify_change(baseline_ms: float, candidate_ms: float, threshold: float) -> str:
    if baseline_ms <= 0.0:
        if candidate_ms <= 0.0:
            return "no_change"
        return "incomparable"
    if candidate_ms <= 0.0:
        return "incomparable"

    delta = (candidate_ms - baseline_ms) / baseline_ms
    if abs(delta) <= threshold:
        return "no_change"
    if candidate_ms < baseline_ms:
        return "improvement"
    return "regression"


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
    status = classify_change(baseline_ms, candidate_ms, threshold)
    if status == "incomparable":
        return "incomparable"
    if status == "no_change":
        return "no change"
    ratio = baseline_ms / candidate_ms
    if status == "improvement":
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
    if not bool(case.get("supports_decision")):
        return "inconclusive"
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
        return "no_change"
    return "inconclusive"


def _display_change_for_status(
    status: str,
    *,
    baseline_ms: float | None,
    candidate_ms: float | None,
    threshold: float,
    mode: str,
) -> str:
    if mode == "exploratory" and status in {"improvement", "regression", "no_change"}:
        if baseline_ms is None or candidate_ms is None:
            raise ValueError(
                "exploratory comparable statuses require baseline and candidate timings"
            )
        return format_change(baseline_ms, candidate_ms, threshold)
    if status == "no_change":
        return "no change"
    return status


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
            "compare requires perf_status=trusted inputs; invalid cases present: "
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
                    case=name,
                    baseline_ms=None,
                    candidate_ms=representative_ms(c, aggregation=aggregation),
                    status="new",
                    change="new",
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
                    case=name,
                    baseline_ms=representative_ms(b, aggregation=aggregation),
                    candidate_ms=None,
                    status="removed",
                    change="removed",
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
                    case=name,
                    baseline_ms=representative_ms(b, aggregation=aggregation),
                    candidate_ms=representative_ms(c, aggregation=aggregation),
                    status="expected_failure",
                    change="expected_failure",
                    baseline_classification=baseline_classification,
                    candidate_classification=candidate_classification,
                    baseline_metrics=best_sample_metrics(b, aggregation=aggregation),
                    candidate_metrics=best_sample_metrics(c, aggregation=aggregation),
                )
            )
            continue

        if mode == "decision":
            if (
                baseline.get("schema_version") != 5
                or candidate.get("schema_version") != 5
            ):
                raise ValueError("decision mode requires schema v5 inputs")
            if not b.get("compatibility_key") or not c.get("compatibility_key"):
                raise ValueError(
                    f"decision mode requires compatibility_key for case '{name}'"
                )
            if b.get("compatibility_key") != c.get("compatibility_key"):
                raise ValueError(
                    f"compatibility mismatch for case '{name}': {b.get('compatibility_key')!r}!={c.get('compatibility_key')!r}"
                )
            metric = str(c.get("decision_metric") or "median")
            baseline_values = _run_metric_values(b, metric)
            candidate_values = _run_metric_values(c, metric)
            base_ms = statistics.median(baseline_values) if baseline_values else None
            cand_ms = statistics.median(candidate_values) if candidate_values else None
            status = _decision_change(c, baseline_values, candidate_values)
            if status == "improvement":
                faster += 1
            elif status == "regression":
                slower += 1
            elif status == "no_change":
                no_change += 1
            else:
                incomparable += 1
            rows.append(
                ComparisonRow(
                    case=name,
                    baseline_ms=base_ms,
                    candidate_ms=cand_ms,
                    status=status,
                    change=_display_change_for_status(
                        status,
                        baseline_ms=base_ms,
                        candidate_ms=cand_ms,
                        threshold=threshold,
                        mode=mode,
                    ),
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
                    case=name,
                    baseline_ms=base_ms,
                    candidate_ms=cand_ms,
                    status="incomparable",
                    change="incomparable",
                    baseline_classification=baseline_classification,
                    candidate_classification=candidate_classification,
                    baseline_metrics=best_sample_metrics(b, aggregation=aggregation),
                    candidate_metrics=best_sample_metrics(c, aggregation=aggregation),
                )
            )
            continue

        status = classify_change(base_ms, cand_ms, threshold)
        if status == "improvement":
            faster += 1
        elif status == "regression":
            slower += 1
        else:
            no_change += 1
        rows.append(
            ComparisonRow(
                case=name,
                baseline_ms=base_ms,
                candidate_ms=cand_ms,
                status=status,
                change=_display_change_for_status(
                    status,
                    baseline_ms=base_ms,
                    candidate_ms=cand_ms,
                    threshold=threshold,
                    mode=mode,
                ),
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


def build_json_payload(
    comparison: Comparison,
    *,
    mode: str,
    aggregation: str,
    noise_threshold: float,
) -> dict[str, object]:
    payload = comparison.to_json_dict()
    return {
        "schema_version": COMPARISON_JSON_SCHEMA_VERSION,
        "metadata": {
            "mode": mode,
            "aggregation": aggregation,
            "noise_threshold": noise_threshold,
        },
        **payload,
    }


def _resolve_input_paths(
    parser: argparse.ArgumentParser, args: argparse.Namespace
) -> tuple[Path, Path]:
    paths = list(args.paths)
    if len(paths) > 2:
        parser.error("expected at most two positional arguments: baseline candidate")
    if paths and (args.baseline_opt is not None or args.candidate_opt is not None):
        parser.error(
            "do not mix positional baseline/candidate arguments with --baseline/--candidate"
        )

    if args.baseline_opt is not None or args.candidate_opt is not None:
        baseline_path = args.baseline_opt
        candidate_path = args.candidate_opt
    else:
        baseline_path = paths[0] if len(paths) >= 1 else None
        candidate_path = paths[1] if len(paths) >= 2 else None

    if baseline_path is None or candidate_path is None:
        parser.error("the following arguments are required: baseline, candidate")
    return baseline_path, candidate_path


def _parse_fail_on(raw: str) -> set[str]:
    statuses: set[str] = set()
    invalid: list[str] = []
    for item in raw.split(","):
        status = item.strip()
        if not status:
            continue
        canonical = FAIL_ON_STATUS_ALIASES.get(status, status)
        if canonical not in VALID_FAIL_ON_STATUSES:
            invalid.append(status)
            continue
        statuses.add(canonical)
    if invalid:
        raise ValueError(
            "invalid --fail-on status(es): "
            + ", ".join(sorted(set(invalid)))
            + " (expected one of: "
            + ", ".join(sorted(VALID_FAIL_ON_STATUSES))
            + ")"
        )
    return statuses


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare delta-bench JSON results")
    parser.add_argument("paths", nargs="*", type=Path)
    parser.add_argument("--baseline", dest="baseline_opt", type=Path)
    parser.add_argument("--candidate", dest="candidate_opt", type=Path)
    parser.add_argument("--noise-threshold", type=float, default=0.05)
    parser.add_argument(
        "--mode", choices=["exploratory", "decision"], default="exploratory"
    )
    parser.add_argument(
        "--aggregation", choices=["min", "median", "p95"], default="median"
    )
    parser.add_argument(
        "--format", choices=["text", "markdown", "json"], default="text"
    )
    parser.add_argument("--include-metrics", action="store_true")
    parser.add_argument(
        "--fail-on",
        default="",
        help="Comma-separated comparison statuses that should force exit code 2",
    )
    parser.add_argument(
        "--color",
        choices=["auto", "always", "never"],
        default="auto",
        help="Control ANSI color output (default: auto, detects TTY)",
    )
    args = parser.parse_args()
    baseline_path, candidate_path = _resolve_input_paths(parser, args)

    if args.color != "auto":
        from .terminal import set_color_mode

        set_color_mode(args.color == "always")

    try:
        fail_on_statuses = _parse_fail_on(args.fail_on)
        comparison = compare_runs(
            _load(baseline_path),
            _load(candidate_path),
            threshold=args.noise_threshold,
            aggregation=args.aggregation,
            mode=args.mode,
        )
    except (ValueError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc
    if args.format == "json":
        output = json.dumps(
            build_json_payload(
                comparison,
                mode=args.mode,
                aggregation=args.aggregation,
                noise_threshold=args.noise_threshold,
            ),
            indent=2,
        )
    elif args.format == "markdown":
        output = render_markdown(comparison, include_metrics=args.include_metrics)
    else:
        output = render_text(comparison, include_metrics=args.include_metrics)
    print(output)
    if any(row.status in fail_on_statuses for row in comparison.rows):
        raise SystemExit(2)


if __name__ == "__main__":
    main()
