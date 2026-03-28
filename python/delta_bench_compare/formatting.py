from __future__ import annotations

from delta_bench_compare.model import Comparison, ComparisonRow
from delta_bench_compare.terminal import bold, dim, green, red, visible_len, yellow

_COMPACT_STABLE_THRESHOLD = 5

_DISPLAY_HEADERS: dict[str, str] = {
    "baseline": "baseline (ms)",
    "candidate": "candidate (ms)",
    "delta_pct": "delta %",
}


def _fmt_ms(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f} ms"


def _fmt_metric(value: int | None) -> str:
    return "-" if value is None else str(value)


def _fmt_delta_pct(
    baseline_ms: float | None, candidate_ms: float | None, change: str
) -> str:
    if change in {"incomparable", "expected_failure", "new", "removed", "inconclusive"}:
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


def _display_headers(include_metrics: bool = False) -> list[str]:
    return [
        _DISPLAY_HEADERS.get(h, h) for h in _headers(include_metrics=include_metrics)
    ]


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
    return cells


def _colorize_cells(cells: list[str], change: str) -> list[str]:
    """Apply ANSI color to delta_pct and change columns based on classification."""
    colored = list(cells)
    if "faster" in change or change == "improvement":
        colored[3] = green(cells[3])
        colored[4] = green(cells[4])
    elif "slower" in change or change == "regression":
        colored[3] = red(cells[3])
        colored[4] = red(cells[4])
    elif change == "no change":
        colored[3] = dim(cells[3])
        colored[4] = dim(cells[4])
    elif change in {
        "incomparable",
        "expected_failure",
        "new",
        "removed",
        "inconclusive",
    }:
        colored[4] = yellow(cells[4])
    return colored


# Numeric column indices: baseline (1), candidate (2), delta_pct (3)
_RIGHT_ALIGN_INDICES = {1, 2, 3}


def _pad(value: str, width: int, right_align: bool) -> str:
    vlen = visible_len(value)
    padding = max(0, width - vlen)
    if right_align:
        return " " * padding + value
    return value + " " * padding


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


def _table_lines_plain(
    rows: list[ComparisonRow], include_metrics: bool = False
) -> list[str]:
    header = _display_headers(include_metrics=include_metrics)
    raw_body = [_row_cells(row, include_metrics=include_metrics) for row in rows]

    # Compute widths from raw (uncolored) values
    widths = [len(column) for column in header]
    for cells in raw_body:
        for idx, value in enumerate(cells):
            widths[idx] = max(widths[idx], len(value))

    # Determine which columns beyond the base set are numeric (metrics)
    right_indices = set(_RIGHT_ALIGN_INDICES)
    if include_metrics:
        right_indices.update(range(5, len(header)))

    lines = [
        "  ".join(
            _pad(column, widths[idx], idx in right_indices)
            for idx, column in enumerate(header)
        ),
        "  ".join("-" * widths[idx] for idx in range(len(header))),
    ]

    # Apply color after width computation
    for raw_cells, row in zip(raw_body, rows):
        colored = _colorize_cells(raw_cells, row.change)
        lines.append(
            "  ".join(
                _pad(colored[idx], widths[idx], idx in right_indices)
                for idx in range(len(colored))
            )
        )
    return lines


def _group_rows(comparison: Comparison) -> list[tuple[str, list[ComparisonRow]]]:
    slower = [
        row
        for row in comparison.rows
        if "slower" in row.change or row.change == "regression"
    ]
    faster = [
        row
        for row in comparison.rows
        if "faster" in row.change or row.change == "improvement"
    ]
    stable = [row for row in comparison.rows if row.change == "no change"]
    needs_attention = [
        row
        for row in comparison.rows
        if row.change
        in {"incomparable", "expected_failure", "new", "removed", "inconclusive"}
    ]
    return [
        ("Regressions (slower)", slower),
        ("Improvements (faster)", faster),
        ("Stable (no change)", stable),
        ("Needs Attention", needs_attention),
    ]


def _section_header(title: str, count: int) -> str:
    return bold(f"--- {title} ({count}) ---")


def render_text_report(comparison: Comparison, include_metrics: bool = False) -> str:
    s = comparison.summary

    parts: list[str] = []
    if s.faster:
        parts.append(green(f"{s.faster} faster"))
    if s.slower:
        parts.append(red(f"{s.slower} slower"))
    if s.no_change:
        parts.append(dim(f"{s.no_change} no change"))
    if s.incomparable:
        parts.append(yellow(f"{s.incomparable} incomparable"))
    if s.new:
        parts.append(yellow(f"{s.new} new"))
    if s.removed:
        parts.append(yellow(f"{s.removed} removed"))

    lines = [bold("Summary:"), "  " + "  |  ".join(parts)]

    for title, rows in _group_rows(comparison):
        if not rows:
            continue
        lines.append("")
        if title == "Stable (no change)" and len(rows) > _COMPACT_STABLE_THRESHOLD:
            lines.append(
                dim(f"--- {title} ({len(rows)} cases, all within noise threshold) ---")
            )
            names = ", ".join(r.case for r in rows)
            lines.append(dim(f"  {names}"))
        else:
            lines.append(_section_header(title, len(rows)))
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
