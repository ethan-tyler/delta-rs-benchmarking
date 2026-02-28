from __future__ import annotations

import argparse
import json
from pathlib import Path

from .formatting import render_markdown as render_markdown_output, render_text_table
from .model import Comparison, ComparisonRow, SampleMetricSnapshot, Summary


def best_sample(case: dict) -> dict | None:
    if not case.get("success", True):
        return None
    samples = case.get("samples") or []
    elapsed_samples = [sample for sample in samples if "elapsed_ms" in sample]
    if not elapsed_samples:
        return None
    return min(elapsed_samples, key=lambda sample: float(sample["elapsed_ms"]))


def best_ms(case: dict) -> float | None:
    sample = best_sample(case)
    if sample is None:
        return None
    return float(sample["elapsed_ms"])


def _metric_as_int(metrics: dict, key: str) -> int | None:
    value = metrics.get(key)
    if value is None:
        return None
    return int(value)


def best_sample_metrics(case: dict) -> SampleMetricSnapshot | None:
    sample = best_sample(case)
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
        return f"+{ratio:.2f}x faster"
    return f"{1 / ratio:.2f}x slower"


def compare_runs(baseline: dict, candidate: dict, threshold: float = 0.05) -> Comparison:
    baseline_cases = {c["case"]: c for c in baseline.get("cases", [])}
    candidate_cases = {c["case"]: c for c in candidate.get("cases", [])}
    names = sorted(set(baseline_cases) | set(candidate_cases))

    rows: list[ComparisonRow] = []
    faster = slower = no_change = incomparable = new = removed = 0

    for name in names:
        b = baseline_cases.get(name)
        c = candidate_cases.get(name)

        if b is None and c is not None:
            new += 1
            rows.append(
                ComparisonRow(
                    name,
                    None,
                    best_ms(c),
                    "new",
                    baseline_metrics=None,
                    candidate_metrics=best_sample_metrics(c),
                )
            )
            continue
        if c is None and b is not None:
            removed += 1
            rows.append(
                ComparisonRow(
                    name,
                    best_ms(b),
                    None,
                    "removed",
                    baseline_metrics=best_sample_metrics(b),
                    candidate_metrics=None,
                )
            )
            continue

        if b is None or c is None:
            raise ValueError(f"inconsistent comparison state for case '{name}'")
        base_ms = best_ms(b)
        cand_ms = best_ms(c)

        if base_ms is None or cand_ms is None:
            incomparable += 1
            rows.append(
                ComparisonRow(
                    name,
                    base_ms,
                    cand_ms,
                    "incomparable",
                    baseline_metrics=best_sample_metrics(b),
                    candidate_metrics=best_sample_metrics(c),
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
                baseline_metrics=best_sample_metrics(b),
                candidate_metrics=best_sample_metrics(c),
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
    return json.loads(path.read_text(encoding="utf-8"))


def render_text(comparison: Comparison, include_metrics: bool = False) -> str:
    lines = [render_text_table(comparison, include_metrics=include_metrics)]
    lines.append("")
    s = comparison.summary
    lines.append(
        "summary: "
        f"faster={s.faster} slower={s.slower} no_change={s.no_change} "
        f"incomparable={s.incomparable} new={s.new} removed={s.removed}"
    )
    return "\n".join(lines)


def render_markdown(comparison: Comparison, include_metrics: bool = False) -> str:
    return render_markdown_output(comparison, include_metrics=include_metrics)


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare delta-bench JSON results")
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--noise-threshold", type=float, default=0.05)
    parser.add_argument("--format", choices=["text", "markdown"], default="text")
    parser.add_argument("--include-metrics", action="store_true")
    parser.add_argument("--ci", action="store_true")
    parser.add_argument("--max-allowed-regressions", type=int, default=0)
    args = parser.parse_args()

    comparison = compare_runs(
        _load(args.baseline),
        _load(args.candidate),
        threshold=args.noise_threshold,
    )
    output = (
        render_markdown(comparison, include_metrics=args.include_metrics)
        if args.format == "markdown"
        else render_text(comparison, include_metrics=args.include_metrics)
    )
    print(output)


if __name__ == "__main__":
    main()
