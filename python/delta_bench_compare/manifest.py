from __future__ import annotations

import argparse
import json
from pathlib import Path


def _optional_int(value: str | None) -> int | None:
    if value in {None, ""}:
        return None
    return int(value)


def _optional_float(value: str | None) -> float | None:
    if value in {None, ""}:
        return None
    return float(value)


def build_manifest(args: argparse.Namespace) -> dict[str, object]:
    return {
        "suite": args.suite,
        "base_sha": args.base_sha,
        "candidate_sha": args.candidate_sha,
        "base_json": args.base_json,
        "candidate_json": args.candidate_json,
        "stdout_report": args.stdout_report,
        "markdown_report": args.markdown_report,
        "comparison_json": args.comparison_json,
        "hash_policy_report": args.hash_policy_report,
        "compare_mode": args.compare_mode,
        "aggregation": args.aggregation,
        "noise_threshold": args.noise_threshold,
        "methodology_profile": args.methodology_profile or None,
        "methodology_version": _optional_int(args.methodology_version),
        "methodology_settings": {
            "compare_mode": args.methodology_compare_mode,
            "warmup": int(args.methodology_warmup),
            "iters": int(args.methodology_iters),
            "prewarm_iters": int(args.methodology_prewarm_iters),
            "compare_runs": int(args.methodology_compare_runs),
            "measure_order": args.methodology_measure_order,
            "timing_phase": args.methodology_timing_phase,
            "aggregation": args.methodology_aggregation,
            "dataset_policy": args.methodology_dataset_policy or None,
            "spread_metric": args.methodology_spread_metric or None,
            "sub_ms_threshold_ms": _optional_float(
                args.methodology_sub_ms_threshold_ms
            ),
            "sub_ms_policy": args.methodology_sub_ms_policy or None,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Write compare artifact manifest")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--suite", required=True)
    parser.add_argument("--base-sha", required=True)
    parser.add_argument("--candidate-sha", required=True)
    parser.add_argument("--base-json", required=True)
    parser.add_argument("--candidate-json", required=True)
    parser.add_argument("--stdout-report", required=True)
    parser.add_argument("--markdown-report", required=True)
    parser.add_argument("--comparison-json", required=True)
    parser.add_argument("--hash-policy-report", required=True)
    parser.add_argument("--compare-mode", required=True)
    parser.add_argument("--aggregation", required=True)
    parser.add_argument("--noise-threshold", type=float, required=True)
    parser.add_argument("--methodology-profile")
    parser.add_argument("--methodology-version")
    parser.add_argument("--methodology-compare-mode", required=True)
    parser.add_argument("--methodology-warmup", required=True)
    parser.add_argument("--methodology-iters", required=True)
    parser.add_argument("--methodology-prewarm-iters", required=True)
    parser.add_argument("--methodology-compare-runs", required=True)
    parser.add_argument("--methodology-measure-order", required=True)
    parser.add_argument("--methodology-timing-phase", required=True)
    parser.add_argument("--methodology-aggregation", required=True)
    parser.add_argument("--methodology-dataset-policy")
    parser.add_argument("--methodology-spread-metric")
    parser.add_argument("--methodology-sub-ms-threshold-ms")
    parser.add_argument("--methodology-sub-ms-policy")
    args = parser.parse_args()

    args.output.write_text(
        json.dumps(build_manifest(args), indent=2) + "\n",
        encoding="utf-8",
    )


if __name__ == "__main__":
    main()
