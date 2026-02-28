from __future__ import annotations

from delta_bench_compare.model import Comparison


def _fmt_ms(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f} ms"


def _fmt_metric(value: int | None) -> str:
    return "-" if value is None else str(value)


def render_text_table(comparison: Comparison, include_metrics: bool = False) -> str:
    header = ["Case", "baseline", "candidate", "change"]
    if include_metrics:
        header.extend(
            [
                "baseline_files_scanned",
                "candidate_files_scanned",
                "baseline_files_pruned",
                "candidate_files_pruned",
                "baseline_bytes_scanned",
                "candidate_bytes_scanned",
                "baseline_scan_time_ms",
                "candidate_scan_time_ms",
                "baseline_rewrite_time_ms",
                "candidate_rewrite_time_ms",
            ]
        )

    lines = [" | ".join(header), " | ".join(["---"] * len(header))]
    for row in comparison.rows:
        cells = [row.case, _fmt_ms(row.baseline_ms), _fmt_ms(row.candidate_ms), row.change]
        if include_metrics:
            baseline_metrics = row.baseline_metrics
            candidate_metrics = row.candidate_metrics
            cells.extend(
                [
                    _fmt_metric(
                        baseline_metrics.files_scanned if baseline_metrics else None
                    ),
                    _fmt_metric(
                        candidate_metrics.files_scanned if candidate_metrics else None
                    ),
                    _fmt_metric(
                        baseline_metrics.files_pruned if baseline_metrics else None
                    ),
                    _fmt_metric(
                        candidate_metrics.files_pruned if candidate_metrics else None
                    ),
                    _fmt_metric(
                        baseline_metrics.bytes_scanned if baseline_metrics else None
                    ),
                    _fmt_metric(
                        candidate_metrics.bytes_scanned if candidate_metrics else None
                    ),
                    _fmt_metric(
                        baseline_metrics.scan_time_ms if baseline_metrics else None
                    ),
                    _fmt_metric(
                        candidate_metrics.scan_time_ms if candidate_metrics else None
                    ),
                    _fmt_metric(
                        baseline_metrics.rewrite_time_ms if baseline_metrics else None
                    ),
                    _fmt_metric(
                        candidate_metrics.rewrite_time_ms if candidate_metrics else None
                    ),
                ]
            )
        lines.append(" | ".join(cells))
    return "\n".join(lines)


def render_markdown(comparison: Comparison, include_metrics: bool = False) -> str:
    table = render_text_table(comparison, include_metrics=include_metrics)
    s = comparison.summary
    summary = (
        "\n\n"
        "| metric | value |\n"
        "| --- | --- |\n"
        f"| faster | {s.faster} |\n"
        f"| slower | {s.slower} |\n"
        f"| no_change | {s.no_change} |\n"
        f"| incomparable | {s.incomparable} |\n"
        f"| new | {s.new} |\n"
        f"| removed | {s.removed} |"
    )
    return table + summary
