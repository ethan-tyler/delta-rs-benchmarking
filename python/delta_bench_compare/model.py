from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ComparisonRow:
    case: str
    baseline_ms: float | None
    candidate_ms: float | None
    change: str


@dataclass(frozen=True)
class Summary:
    faster: int
    slower: int
    no_change: int
    incomparable: int
    new: int
    removed: int


@dataclass(frozen=True)
class Comparison:
    rows: list[ComparisonRow]
    summary: Summary
