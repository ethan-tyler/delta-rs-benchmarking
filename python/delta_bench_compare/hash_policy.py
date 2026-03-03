from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from pathlib import Path

from .schema import load_benchmark_payload

HASH_MISMATCH_PATTERN = re.compile(
    r"result hash mismatch: expected '(?P<expected>[^']+)', found '(?P<found>[^']+)'"
)


@dataclass(frozen=True)
class HashPolicyCase:
    case: str
    expected: str | None
    baseline_found: str | None
    candidate_found: str | None
    baseline_sample_hashes: tuple[str, ...]
    candidate_sample_hashes: tuple[str, ...]


@dataclass(frozen=True)
class HashPolicyAnalysis:
    stale_manifest_cases: list[HashPolicyCase]
    candidate_only_mismatch_cases: list[HashPolicyCase]
    nondeterministic_cases: list[HashPolicyCase]
    other_cases: list[HashPolicyCase]


def _case_hash_mismatch(case: dict | None) -> tuple[str, str] | None:
    if case is None:
        return None
    failure = case.get("failure") or {}
    message = failure.get("message")
    if not isinstance(message, str):
        return None
    match = HASH_MISMATCH_PATTERN.search(message)
    if match is None:
        return None
    return match.group("expected"), match.group("found")


def _sample_result_hashes(case: dict | None) -> tuple[str, ...]:
    if case is None:
        return ()
    values: set[str] = set()
    for sample in case.get("samples", []):
        metrics = sample.get("metrics") or {}
        value = metrics.get("result_hash")
        if isinstance(value, str) and value:
            values.add(value)
    return tuple(sorted(values))


def analyze_hash_policy(baseline: dict, candidate: dict) -> HashPolicyAnalysis:
    baseline_cases = {c["case"]: c for c in baseline.get("cases", [])}
    candidate_cases = {c["case"]: c for c in candidate.get("cases", [])}
    names = sorted(set(baseline_cases) | set(candidate_cases))

    stale: list[HashPolicyCase] = []
    candidate_only: list[HashPolicyCase] = []
    nondeterministic: list[HashPolicyCase] = []
    other: list[HashPolicyCase] = []

    for case_name in names:
        baseline_case = baseline_cases.get(case_name)
        candidate_case = candidate_cases.get(case_name)
        baseline_mismatch = _case_hash_mismatch(baseline_case)
        candidate_mismatch = _case_hash_mismatch(candidate_case)
        if baseline_mismatch is None and candidate_mismatch is None:
            continue

        baseline_hashes = _sample_result_hashes(baseline_case)
        candidate_hashes = _sample_result_hashes(candidate_case)

        evidence = HashPolicyCase(
            case=case_name,
            expected=(
                candidate_mismatch[0]
                if candidate_mismatch is not None
                else (baseline_mismatch[0] if baseline_mismatch is not None else None)
            ),
            baseline_found=(
                baseline_mismatch[1] if baseline_mismatch is not None else None
            ),
            candidate_found=(
                candidate_mismatch[1] if candidate_mismatch is not None else None
            ),
            baseline_sample_hashes=baseline_hashes,
            candidate_sample_hashes=candidate_hashes,
        )

        if len(baseline_hashes) > 1 or len(candidate_hashes) > 1:
            nondeterministic.append(evidence)
            continue

        if (
            baseline_mismatch is not None
            and candidate_mismatch is not None
            and baseline_mismatch == candidate_mismatch
        ):
            stale.append(evidence)
            continue

        if candidate_mismatch is not None and baseline_mismatch is None:
            candidate_only.append(evidence)
            continue

        other.append(evidence)

    return HashPolicyAnalysis(
        stale_manifest_cases=stale,
        candidate_only_mismatch_cases=candidate_only,
        nondeterministic_cases=nondeterministic,
        other_cases=other,
    )


def _render_section(title: str, rows: list[str]) -> list[str]:
    lines = [title]
    if not rows:
        lines.append("  - none")
        return lines
    lines.extend(f"  - {row}" for row in rows)
    return lines


def render_hash_policy_text(analysis: HashPolicyAnalysis) -> str:
    lines = ["Hash Assertion Triage"]
    lines.append(
        "  guidance: refresh hashes only for deterministic stale baselines; investigate candidate-only mismatches as regressions"
    )
    lines.extend(
        _render_section(
            "Stale manifest hash candidates (mismatch on both base and candidate with same found hash):",
            [
                f"{case.case} expected={case.expected} found={case.candidate_found}"
                for case in analysis.stale_manifest_cases
            ],
        )
    )
    lines.extend(
        _render_section(
            "Candidate-only hash mismatches (possible regressions):",
            [
                f"{case.case} expected={case.expected} candidate_found={case.candidate_found}"
                for case in analysis.candidate_only_mismatch_cases
            ],
        )
    )
    lines.extend(
        _render_section(
            "Nondeterministic hash candidates (multiple sample hashes; stabilize before refresh):",
            [
                f"{case.case} baseline_hashes={list(case.baseline_sample_hashes)} candidate_hashes={list(case.candidate_sample_hashes)}"
                for case in analysis.nondeterministic_cases
            ],
        )
    )
    lines.extend(
        _render_section(
            "Other hash mismatch patterns (manual review):",
            [
                f"{case.case} expected={case.expected} baseline_found={case.baseline_found} candidate_found={case.candidate_found}"
                for case in analysis.other_cases
            ],
        )
    )
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Triages benchmark exact_result_hash mismatches between baseline and candidate runs"
    )
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    args = parser.parse_args()

    baseline = load_benchmark_payload(args.baseline)
    candidate = load_benchmark_payload(args.candidate)
    print(render_hash_policy_text(analyze_hash_policy(baseline, candidate)))


if __name__ == "__main__":
    main()
