from __future__ import annotations

import argparse
import json
from pathlib import Path

from .formatting import render_markdown as render_markdown_output, render_text_table
from .model import Comparison, ComparisonRow, Summary


def best_ms(case: dict) -> float | None:
    if not case.get("success", True):
        return None
    samples = case.get("samples") or []
    values = [float(sample["elapsed_ms"]) for sample in samples if "elapsed_ms" in sample]
    if not values:
        return None
    return min(values)


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
            rows.append(ComparisonRow(name, None, best_ms(c), "new"))
            continue
        if c is None and b is not None:
            removed += 1
            rows.append(ComparisonRow(name, best_ms(b), None, "removed"))
            continue

        if b is None or c is None:
            raise ValueError(f"inconsistent comparison state for case '{name}'")
        base_ms = best_ms(b)
        cand_ms = best_ms(c)

        if base_ms is None or cand_ms is None:
            incomparable += 1
            rows.append(ComparisonRow(name, base_ms, cand_ms, "incomparable"))
            continue

        change = format_change(base_ms, cand_ms, threshold)
        if "faster" in change:
            faster += 1
        elif "slower" in change:
            slower += 1
        else:
            no_change += 1
        rows.append(ComparisonRow(name, base_ms, cand_ms, change))

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


def render_text(comparison: Comparison) -> str:
    lines = [render_text_table(comparison)]
    lines.append("")
    s = comparison.summary
    lines.append(
        "summary: "
        f"faster={s.faster} slower={s.slower} no_change={s.no_change} "
        f"incomparable={s.incomparable} new={s.new} removed={s.removed}"
    )
    return "\n".join(lines)


def render_markdown(comparison: Comparison) -> str:
    return render_markdown_output(comparison)


def ci_regression_violation(
    comparison: Comparison,
    ci_enabled: bool,
    max_allowed_regressions: int,
) -> tuple[bool, str]:
    if not ci_enabled:
        return False, ""

    slower_cases = comparison.summary.slower
    if slower_cases > max_allowed_regressions:
        return (
            True,
            "CI policy violated: "
            f"slower cases={slower_cases} exceeds max_allowed_regressions={max_allowed_regressions}",
        )
    return False, ""


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare delta-bench JSON results")
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--noise-threshold", type=float, default=0.05)
    parser.add_argument("--format", choices=["text", "markdown"], default="text")
    parser.add_argument("--ci", action="store_true")
    parser.add_argument("--max-allowed-regressions", type=int, default=0)
    args = parser.parse_args()
    if args.max_allowed_regressions < 0:
        raise SystemExit("--max-allowed-regressions must be >= 0")

    comparison = compare_runs(
        _load(args.baseline),
        _load(args.candidate),
        threshold=args.noise_threshold,
    )
    output = render_markdown(comparison) if args.format == "markdown" else render_text(comparison)
    print(output)

    violates, message = ci_regression_violation(
        comparison,
        ci_enabled=args.ci,
        max_allowed_regressions=args.max_allowed_regressions,
    )
    if violates:
        print(message)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
