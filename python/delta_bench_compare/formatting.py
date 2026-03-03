from __future__ import annotations

from delta_bench_compare.model import Comparison, ComparisonRow


def _fmt_ms(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f} ms"


def _fmt_metric(value: int | None) -> str:
    return "-" if value is None else str(value)


def _fmt_delta_pct(
    baseline_ms: float | None, candidate_ms: float | None, change: str
) -> str:
    if change in {"incomparable", "expected_failure", "new", "removed"}:
        return "-"
    if baseline_ms is None or candidate_ms is None or baseline_ms <= 0.0:
        return "-"
    delta_pct = ((candidate_ms - baseline_ms) / baseline_ms) * 100.0
    return f"{delta_pct:+.2f}%"


def _headers(include_metrics: bool = False) -> list[str]:
    header = ["Case", "baseline", "candidate", "delta_pct", "change"]
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
    return header


def _row_cells(row: ComparisonRow, include_metrics: bool = False) -> list[str]:
    cells = [
        row.case,
        _fmt_ms(row.baseline_ms),
        _fmt_ms(row.candidate_ms),
        _fmt_delta_pct(row.baseline_ms, row.candidate_ms, row.change),
        row.change,
    ]
    if include_metrics:
        baseline_metrics = row.baseline_metrics
        candidate_metrics = row.candidate_metrics
        cells.extend(
            [
                _fmt_metric(baseline_metrics.files_scanned if baseline_metrics else None),
                _fmt_metric(candidate_metrics.files_scanned if candidate_metrics else None),
                _fmt_metric(baseline_metrics.files_pruned if baseline_metrics else None),
                _fmt_metric(candidate_metrics.files_pruned if candidate_metrics else None),
                _fmt_metric(baseline_metrics.bytes_scanned if baseline_metrics else None),
                _fmt_metric(candidate_metrics.bytes_scanned if candidate_metrics else None),
                _fmt_metric(baseline_metrics.scan_time_ms if baseline_metrics else None),
                _fmt_metric(candidate_metrics.scan_time_ms if candidate_metrics else None),
                _fmt_metric(
                    baseline_metrics.rewrite_time_ms if baseline_metrics else None
                ),
                _fmt_metric(
                    candidate_metrics.rewrite_time_ms if candidate_metrics else None
                ),
            ]
        )
    return cells


def _table_lines_markdown(
    rows: list[ComparisonRow], include_metrics: bool = False
) -> list[str]:
    header = _headers(include_metrics=include_metrics)
    lines = [" | ".join(header), " | ".join(["---"] * len(header))]
    for row in rows:
        cells = _row_cells(row, include_metrics=include_metrics)
        lines.append(" | ".join(cells))
    return lines


def render_text_table(comparison: Comparison, include_metrics: bool = False) -> str:
    return "\n".join(
        _table_lines_markdown(comparison.rows, include_metrics=include_metrics)
    )


def _table_lines_plain(rows: list[ComparisonRow], include_metrics: bool = False) -> list[str]:
    header = _headers(include_metrics=include_metrics)
    body = [_row_cells(row, include_metrics=include_metrics) for row in rows]

    widths = [len(column) for column in header]
    for cells in body:
        for idx, value in enumerate(cells):
            widths[idx] = max(widths[idx], len(value))

    lines = [
        "  ".join(column.ljust(widths[idx]) for idx, column in enumerate(header)),
        "  ".join("-" * widths[idx] for idx in range(len(header))),
    ]
    for cells in body:
        lines.append("  ".join(value.ljust(widths[idx]) for idx, value in enumerate(cells)))
    return lines


def _group_rows(comparison: Comparison) -> list[tuple[str, list[ComparisonRow]]]:
    slower = [row for row in comparison.rows if "slower" in row.change]
    faster = [row for row in comparison.rows if "faster" in row.change]
    stable = [row for row in comparison.rows if row.change == "no change"]
    needs_attention = [
        row
        for row in comparison.rows
        if row.change in {"incomparable", "expected_failure", "new", "removed"}
    ]
    return [
        ("Regressions (slower)", slower),
        ("Improvements (faster)", faster),
        ("Stable (no change)", stable),
        ("Needs Attention", needs_attention),
    ]


def render_text_report(comparison: Comparison, include_metrics: bool = False) -> str:
    s = comparison.summary
    lines = [
        "Summary:",
        f"  faster: {s.faster}",
        f"  slower: {s.slower}",
        f"  no_change: {s.no_change}",
        f"  incomparable: {s.incomparable}",
        f"  new: {s.new}",
        f"  removed: {s.removed}",
    ]

    for title, rows in _group_rows(comparison):
        if not rows:
            continue
        lines.extend(["", f"{title}:"])
        lines.extend(_table_lines_plain(rows, include_metrics=include_metrics))

    return "\n".join(lines)


def render_markdown(comparison: Comparison, include_metrics: bool = False) -> str:
    s = comparison.summary
    lines = [
        "## Summary",
        "",
        "| metric | value |",
        "| --- | --- |",
        f"| faster | {s.faster} |",
        f"| slower | {s.slower} |",
        f"| no_change | {s.no_change} |",
        f"| incomparable | {s.incomparable} |",
        f"| new | {s.new} |",
        f"| removed | {s.removed} |",
    ]

    for title, rows in _group_rows(comparison):
        if not rows:
            continue
        lines.extend(["", f"## {title}", ""])
        lines.extend(_table_lines_markdown(rows, include_metrics=include_metrics))

    return "\n".join(lines)
