from __future__ import annotations

import html
import json
import math
import statistics
from pathlib import Path
from typing import Any


def generate_trend_reports(
    *,
    store_dir: Path | str,
    markdown_path: Path | str,
    html_path: Path | str,
    baseline_window: int,
    regression_threshold: float,
    significance_method: str,
    significance_alpha: float,
) -> dict[str, int]:
    if baseline_window <= 0:
        raise ValueError("baseline_window must be > 0")
    if regression_threshold < 0:
        raise ValueError("regression_threshold must be >= 0")
    if significance_method not in {"none", "mann-whitney"}:
        raise ValueError("significance_method must be one of: none, mann-whitney")
    if not (0.0 < significance_alpha <= 1.0):
        raise ValueError("significance_alpha must be in (0, 1]")

    rows, invalid_rows = _load_rows(Path(store_dir))
    if not rows:
        markdown = "# Longitudinal Benchmark Summary\n\nNo longitudinal rows found.\n"
        html_report = _empty_html()
        _write(markdown_path, markdown)
        _write(html_path, html_report)
        return {
            "total_series": 0,
            "regressions": 0,
            "significant_regressions": 0,
            "invalid_rows": invalid_rows,
        }

    grouped: dict[tuple[str, str, str], list[dict[str, Any]]] = {}
    for row in rows:
        if not row.get("success"):
            continue
        median = row.get("median_ms")
        if median is None:
            continue
        key = (
            str(row.get("suite", "unknown")),
            str(row.get("scale", "unknown")),
            str(row.get("case", "unknown")),
        )
        grouped.setdefault(key, []).append(row)

    series_stats: list[dict[str, Any]] = []
    regressions: list[dict[str, Any]] = []
    significant_regressions = 0

    for key, series in sorted(grouped.items()):
        ordered = sorted(
            series, key=lambda row: row.get("benchmark_created_at") or row.get("ingested_at") or ""
        )
        medians = [float(row["median_ms"]) for row in ordered]
        latest = medians[-1]
        baseline_rows = ordered[-(baseline_window + 1) : -1]
        baseline_values = [float(row["median_ms"]) for row in baseline_rows if row.get("median_ms") is not None]
        baseline_median = statistics.median(baseline_values) if baseline_values else None
        change_pct = None
        status = "insufficient-baseline"
        is_regression = False

        p_value = None
        significant = None
        if significance_method != "none":
            latest_samples = _extract_samples(ordered[-1])
            baseline_samples = _combine_samples(baseline_rows)
            p_value = _mann_whitney_one_sided_p_value(
                baseline_samples=baseline_samples,
                latest_samples=latest_samples,
            )
            significant = p_value is not None and p_value <= significance_alpha

        if baseline_median is not None:
            if baseline_median > 0:
                change_pct = ((latest - baseline_median) / baseline_median) * 100.0
                if latest > baseline_median * (1.0 + regression_threshold):
                    is_regression = True
                    if significance_method == "none":
                        status = "regression"
                    else:
                        status = "regression-significant" if significant else "regression-not-significant"
                elif latest < baseline_median * (1.0 - regression_threshold):
                    status = "improvement"
                else:
                    status = "stable"
            elif latest > 0:
                change_pct = float("inf")
                is_regression = True
                if significance_method == "none":
                    status = "regression"
                else:
                    status = "regression-significant" if significant else "regression-not-significant"
            else:
                change_pct = 0.0
                status = "stable"

        item = {
            "suite": key[0],
            "scale": key[1],
            "case": key[2],
            "points": medians,
            "latest": latest,
            "baseline_median": baseline_median,
            "change_pct": change_pct,
            "status": status,
            "p_value": p_value,
            "significant": significant,
        }
        series_stats.append(item)
        if is_regression:
            regressions.append(item)
            if significance_method == "none" or significant:
                significant_regressions += 1

    markdown = _markdown_summary(
        series_stats=series_stats,
        regressions=regressions,
        significance_method=significance_method,
        invalid_rows=invalid_rows,
    )
    html_report = _html_report(
        series_stats=series_stats,
        regressions=regressions,
        significant_regressions=significant_regressions,
        regression_threshold=regression_threshold,
        significance_method=significance_method,
        significance_alpha=significance_alpha,
        invalid_rows=invalid_rows,
    )
    _write(markdown_path, markdown)
    _write(html_path, html_report)
    return {
        "total_series": len(series_stats),
        "regressions": len(regressions),
        "significant_regressions": significant_regressions,
        "invalid_rows": invalid_rows,
    }


def _extract_samples(row: dict[str, Any]) -> list[float]:
    samples = row.get("sample_values_ms")
    if isinstance(samples, list):
        return [float(v) for v in samples]
    median = row.get("median_ms")
    if median is None:
        return []
    return [float(median)]


def _combine_samples(rows: list[dict[str, Any]]) -> list[float]:
    combined: list[float] = []
    for row in rows:
        combined.extend(_extract_samples(row))
    return combined


def _mann_whitney_one_sided_p_value(
    *,
    baseline_samples: list[float],
    latest_samples: list[float],
) -> float | None:
    n1 = len(latest_samples)
    n2 = len(baseline_samples)
    if n1 < 2 or n2 < 2:
        return None

    combined = [(value, 1) for value in latest_samples] + [(value, 0) for value in baseline_samples]
    combined.sort(key=lambda item: item[0])

    rank_sum_latest = 0.0
    tie_group_sizes: list[int] = []
    idx = 0
    while idx < len(combined):
        start = idx
        current = combined[idx][0]
        while idx < len(combined) and combined[idx][0] == current:
            idx += 1
        end = idx
        group_size = end - start
        tie_group_sizes.append(group_size)
        avg_rank = (start + 1 + end) / 2.0
        for j in range(start, end):
            if combined[j][1] == 1:
                rank_sum_latest += avg_rank

    u_latest = rank_sum_latest - (n1 * (n1 + 1) / 2.0)
    total = n1 + n2
    tie_sum = sum(size**3 - size for size in tie_group_sizes)
    variance = (n1 * n2 / 12.0) * ((total + 1) - (tie_sum / (total * (total - 1))))
    if variance <= 0:
        return None

    mean_u = n1 * n2 / 2.0
    z = (u_latest - mean_u) / math.sqrt(variance)
    cdf = 0.5 * (1.0 + math.erf(z / math.sqrt(2.0)))
    p_value = 1.0 - cdf
    if p_value < 0.0:
        return 0.0
    if p_value > 1.0:
        return 1.0
    return p_value


def _load_rows(store_dir: Path) -> tuple[list[dict[str, Any]], int]:
    rows_path = store_dir / "rows.jsonl"
    if not rows_path.exists():
        return [], 0
    rows: list[dict[str, Any]] = []
    invalid_rows = 0
    for line in rows_path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                invalid_rows += 1
    return rows, invalid_rows


def _markdown_summary(
    *,
    series_stats: list[dict[str, Any]],
    regressions: list[dict[str, Any]],
    significance_method: str,
    invalid_rows: int,
) -> str:
    lines = [
        "# Longitudinal Benchmark Summary",
        "",
        f"- Total series: {len(series_stats)}",
        f"- Regressions: {len(regressions)}",
    ]
    if invalid_rows:
        lines.append(f"- Invalid rows skipped: {invalid_rows}")
    lines.extend(["", "## Regression Highlights"])
    if not regressions:
        lines.extend(["", "No regressions detected in the latest window.", ""])
        return "\n".join(lines)

    lines.append("")
    if significance_method == "none":
        lines.extend(
            [
                "| suite | scale | case | baseline median (ms) | latest (ms) | delta |",
                "| --- | --- | --- | ---: | ---: | ---: |",
            ]
        )
        for item in regressions:
            lines.append(
                "| {suite} | {scale} | {case} | {baseline:.2f} | {latest:.2f} | {delta:+.2f}% |".format(
                    suite=item["suite"],
                    scale=item["scale"],
                    case=item["case"],
                    baseline=float(item["baseline_median"] or 0.0),
                    latest=float(item["latest"]),
                    delta=float(item["change_pct"] or 0.0),
                )
            )
    else:
        lines.extend(
            [
                "| suite | scale | case | baseline median (ms) | latest (ms) | delta | p-value | significant |",
                "| --- | --- | --- | ---: | ---: | ---: | ---: | --- |",
            ]
        )
        for item in regressions:
            p_value = item.get("p_value")
            p_display = f"{float(p_value):.6f}" if p_value is not None else "n/a"
            lines.append(
                "| {suite} | {scale} | {case} | {baseline:.2f} | {latest:.2f} | {delta:+.2f}% | {p_value} | {significant} |".format(
                    suite=item["suite"],
                    scale=item["scale"],
                    case=item["case"],
                    baseline=float(item["baseline_median"] or 0.0),
                    latest=float(item["latest"]),
                    delta=float(item["change_pct"] or 0.0),
                    p_value=p_display,
                    significant="yes" if item.get("significant") else "no",
                )
            )
    lines.append("")
    return "\n".join(lines)


def _html_report(
    *,
    series_stats: list[dict[str, Any]],
    regressions: list[dict[str, Any]],
    significant_regressions: int,
    regression_threshold: float,
    significance_method: str,
    significance_alpha: float,
    invalid_rows: int,
) -> str:
    cards: list[str] = []
    for item in series_stats:
        p_val = item.get("p_value")
        p_line = ""
        if significance_method != "none":
            if p_val is None:
                p_line = "<p>p-value: n/a</p>"
            else:
                p_line = f"<p>p-value: {float(p_val):.6f}</p>"
        cards.append(
            (
                "<section class='card'>"
                f"<h2>{html.escape(item['suite'])} / {html.escape(item['scale'])} / {html.escape(item['case'])}</h2>"
                f"<p>Status: <strong>{html.escape(item['status'])}</strong></p>"
                f"<p>Latest: {item['latest']:.2f} ms</p>"
                f"{p_line}"
                f"{_sparkline_svg(item['points'])}"
                "</section>"
            )
        )

    significance_meta = ""
    if significance_method != "none":
        significance_meta = (
            f" | Significant regressions: {significant_regressions}"
            f" | Method: {significance_method} (alpha={significance_alpha:.3f})"
        )

    invalid_rows_meta = f" | Invalid rows skipped: {invalid_rows}" if invalid_rows else ""

    return f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <title>Longitudinal Benchmark Trends</title>
  <style>
    :root {{
      --bg: #f5f7fb;
      --surface: #ffffff;
      --ink: #1b2432;
      --muted: #5f6b7a;
      --accent: #145a8d;
      --warn: #b24020;
    }}
    body {{ background: linear-gradient(160deg, #eff4fb, #f9f3eb); color: var(--ink); font-family: "Iowan Old Style", "Palatino Linotype", serif; margin: 0; padding: 24px; }}
    h1 {{ margin-top: 0; }}
    .grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 16px; }}
    .card {{ background: var(--surface); border-radius: 12px; padding: 16px; box-shadow: 0 4px 18px rgba(20, 90, 141, 0.08); }}
    .meta {{ color: var(--muted); }}
    svg {{ width: 100%; height: 90px; }}
  </style>
</head>
<body>
  <h1>Longitudinal Benchmark Trends</h1>
  <p class="meta">Series: {len(series_stats)} | Regressions: {len(regressions)}{significance_meta}{invalid_rows_meta} | Threshold: {regression_threshold:.2%}</p>
  <div class="grid">
    {''.join(cards)}
  </div>
</body>
</html>
"""


def _sparkline_svg(values: list[float]) -> str:
    if not values:
        return "<svg viewBox='0 0 300 90'><text x='4' y='45'>no data</text></svg>"

    width = 300.0
    height = 90.0
    x_step = width / max(len(values) - 1, 1)
    min_v = min(values)
    max_v = max(values)
    value_range = max(max_v - min_v, 1.0)

    points: list[str] = []
    for idx, value in enumerate(values):
        x = idx * x_step
        normalized = (value - min_v) / value_range
        y = height - (normalized * (height - 10.0)) - 5.0
        points.append(f"{x:.2f},{y:.2f}")

    return (
        "<svg viewBox='0 0 300 90' role='img' aria-label='trend chart'>"
        "<polyline fill='none' stroke='#145a8d' stroke-width='2.5' points='{points}' />"
        "</svg>"
    ).format(points=" ".join(points))


def _empty_html() -> str:
    return """<!doctype html>
<html lang="en">
<head><meta charset="utf-8" /><title>Longitudinal Benchmark Trends</title></head>
<body><h1>Longitudinal Benchmark Trends</h1><p>No longitudinal rows found.</p></body>
</html>
"""


def _write(path: Path | str, content: str) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(content, encoding="utf-8")
