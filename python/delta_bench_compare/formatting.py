from __future__ import annotations

from delta_bench_compare.model import Comparison, ComparisonRow
from delta_bench_compare.terminal import bold, dim, green, red, visible_len, yellow

_COMPACT_STABLE_THRESHOLD = 5

_DISPLAY_HEADERS: dict[str, str] = {
    "baseline": "baseline (ms)",
    "candidate": "candidate (ms)",
    "delta_pct": "delta %",
}

_SCAN_METRIC_HEADERS = [
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

_CONTENTION_METRIC_HEADERS = [
    "baseline_worker_count",
    "candidate_worker_count",
    "baseline_race_count",
    "candidate_race_count",
    "baseline_ops_attempted",
    "candidate_ops_attempted",
    "baseline_ops_succeeded",
    "candidate_ops_succeeded",
    "baseline_ops_failed",
    "candidate_ops_failed",
    "baseline_conflict_append",
    "candidate_conflict_append",
    "baseline_conflict_delete_read",
    "candidate_conflict_delete_read",
    "baseline_conflict_delete_delete",
    "candidate_conflict_delete_delete",
    "baseline_conflict_metadata_changed",
    "candidate_conflict_metadata_changed",
    "baseline_conflict_protocol_changed",
    "candidate_conflict_protocol_changed",
    "baseline_conflict_transaction",
    "candidate_conflict_transaction",
    "baseline_version_already_exists",
    "candidate_version_already_exists",
    "baseline_max_commit_attempts_exceeded",
    "candidate_max_commit_attempts_exceeded",
    "baseline_other_errors",
    "candidate_other_errors",
]


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


def _has_scan_metrics(rows: list[ComparisonRow]) -> bool:
    for row in rows:
        for metrics in (row.baseline_metrics, row.candidate_metrics):
            if metrics is None:
                continue
            if any(
                value is not None
                for value in (
                    metrics.files_scanned,
                    metrics.files_pruned,
                    metrics.bytes_scanned,
                    metrics.scan_time_ms,
                    metrics.rewrite_time_ms,
                )
            ):
                return True
    return False


def _has_contention_metrics(rows: list[ComparisonRow]) -> bool:
    for row in rows:
        for metrics in (row.baseline_metrics, row.candidate_metrics):
            if metrics is not None and metrics.contention is not None:
                return True
    return False


def _headers(rows: list[ComparisonRow], include_metrics: bool = False) -> list[str]:
    header = ["Case", "baseline", "candidate", "delta_pct", "change"]
    if include_metrics:
        if _has_scan_metrics(rows):
            header.extend(_SCAN_METRIC_HEADERS)
        if _has_contention_metrics(rows):
            header.extend(_CONTENTION_METRIC_HEADERS)
    return header


def _display_headers(
    rows: list[ComparisonRow], include_metrics: bool = False
) -> list[str]:
    return [_DISPLAY_HEADERS.get(h, h) for h in _headers(rows, include_metrics)]


def _contention_values(metrics: object | None) -> list[str]:
    contention = getattr(metrics, "contention", None)
    return [
        _fmt_metric(contention.worker_count if contention else None),
        _fmt_metric(contention.race_count if contention else None),
        _fmt_metric(contention.ops_attempted if contention else None),
        _fmt_metric(contention.ops_succeeded if contention else None),
        _fmt_metric(contention.ops_failed if contention else None),
        _fmt_metric(contention.conflict_append if contention else None),
        _fmt_metric(contention.conflict_delete_read if contention else None),
        _fmt_metric(contention.conflict_delete_delete if contention else None),
        _fmt_metric(contention.conflict_metadata_changed if contention else None),
        _fmt_metric(contention.conflict_protocol_changed if contention else None),
        _fmt_metric(contention.conflict_transaction if contention else None),
        _fmt_metric(contention.version_already_exists if contention else None),
        _fmt_metric(contention.max_commit_attempts_exceeded if contention else None),
        _fmt_metric(contention.other_errors if contention else None),
    ]


def _interleave_metric_cells(
    baseline_values: list[str], candidate_values: list[str]
) -> list[str]:
    cells: list[str] = []
    for baseline_value, candidate_value in zip(baseline_values, candidate_values):
        cells.extend([baseline_value, candidate_value])
    return cells


def _row_cells(
    row: ComparisonRow, rows: list[ComparisonRow], include_metrics: bool = False
) -> list[str]:
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
        if _has_scan_metrics(rows):
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
        if _has_contention_metrics(rows):
            cells.extend(
                _interleave_metric_cells(
                    _contention_values(baseline_metrics),
                    _contention_values(candidate_metrics),
                )
            )
    return cells


def _colorize_cells(cells: list[str], change: str) -> list[str]:
    colored = list(cells)
    if "faster" in change:
        colored[3] = green(cells[3])
        colored[4] = green(cells[4])
    elif "slower" in change:
        colored[3] = red(cells[3])
        colored[4] = red(cells[4])
    elif change == "no change":
        colored[3] = dim(cells[3])
        colored[4] = dim(cells[4])
    elif change in {"incomparable", "expected_failure", "new", "removed"}:
        colored[4] = yellow(cells[4])
    return colored


_RIGHT_ALIGN_INDICES = {1, 2, 3}


def _pad(value: str, width: int, right_align: bool) -> str:
    vlen = visible_len(value)
    padding = max(0, width - vlen)
    if right_align:
        return " " * padding + value
    return value + " " * padding


def _table_lines_markdown(
    reference_rows: list[ComparisonRow],
    rows: list[ComparisonRow],
    include_metrics: bool = False,
) -> list[str]:
    header = _headers(reference_rows, include_metrics=include_metrics)
    lines = [" | ".join(header), " | ".join(["---"] * len(header))]
    for row in rows:
        cells = _row_cells(row, reference_rows, include_metrics=include_metrics)
        lines.append(" | ".join(cells))
    return lines


def render_text_table(comparison: Comparison, include_metrics: bool = False) -> str:
    return "\n".join(
        _table_lines_markdown(
            comparison.rows, comparison.rows, include_metrics=include_metrics
        )
    )


def _table_lines_plain(
    reference_rows: list[ComparisonRow],
    rows: list[ComparisonRow],
    include_metrics: bool = False,
) -> list[str]:
    header = _display_headers(reference_rows, include_metrics=include_metrics)
    raw_body = [
        _row_cells(row, reference_rows, include_metrics=include_metrics) for row in rows
    ]

    widths = [len(column) for column in header]
    for cells in raw_body:
        for idx, value in enumerate(cells):
            widths[idx] = max(widths[idx], len(value))

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
            lines.extend(
                _table_lines_plain(
                    comparison.rows, rows, include_metrics=include_metrics
                )
            )

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
        lines.extend(
            _table_lines_markdown(
                comparison.rows, rows, include_metrics=include_metrics
            )
        )

    return "\n".join(lines)
