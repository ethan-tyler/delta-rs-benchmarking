from __future__ import annotations

from delta_bench_compare.model import Comparison


def _fmt_ms(value: float | None) -> str:
    return "-" if value is None else f"{value:.2f} ms"


def render_text_table(comparison: Comparison) -> str:
    lines = ["Case | baseline | candidate | change", "--- | --- | --- | ---"]
    for row in comparison.rows:
        lines.append(
            f"{row.case} | {_fmt_ms(row.baseline_ms)} | {_fmt_ms(row.candidate_ms)} | {row.change}"
        )
    return "\n".join(lines)


def render_markdown(comparison: Comparison) -> str:
    table = render_text_table(comparison)
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
