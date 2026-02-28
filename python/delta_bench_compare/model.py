from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SampleMetricSnapshot:
    files_scanned: int | None
    files_pruned: int | None
    bytes_scanned: int | None
    scan_time_ms: int | None
    rewrite_time_ms: int | None


@dataclass(frozen=True)
class ComparisonRow:
    case: str
    baseline_ms: float | None
    candidate_ms: float | None
    change: str
    baseline_metrics: SampleMetricSnapshot | None = None
    candidate_metrics: SampleMetricSnapshot | None = None


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
