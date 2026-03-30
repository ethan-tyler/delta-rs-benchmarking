from __future__ import annotations

from dataclasses import dataclass

VALID_COMPARISON_STATUSES = frozenset(
    {
        "expected_failure",
        "improvement",
        "incomparable",
        "inconclusive",
        "new",
        "no_change",
        "regression",
        "removed",
    }
)
COMPARABLE_COMPARISON_STATUSES = frozenset({"improvement", "regression", "no_change"})


@dataclass(frozen=True)
class SampleMetricSnapshot:
    files_scanned: int | None
    files_pruned: int | None
    bytes_scanned: int | None
    scan_time_ms: int | None
    rewrite_time_ms: int | None
    contention: "ContentionMetricSnapshot | None" = None


@dataclass(frozen=True)
class ContentionMetricSnapshot:
    worker_count: int | None
    race_count: int | None
    ops_attempted: int | None
    ops_succeeded: int | None
    ops_failed: int | None
    conflict_append: int | None
    conflict_delete_read: int | None
    conflict_delete_delete: int | None
    conflict_metadata_changed: int | None
    conflict_protocol_changed: int | None
    conflict_transaction: int | None
    version_already_exists: int | None
    max_commit_attempts_exceeded: int | None
    other_errors: int | None


@dataclass(frozen=True)
class ComparisonRow:
    case: str
    baseline_ms: float | None
    candidate_ms: float | None
    status: str
    change: str
    baseline_classification: str | None
    candidate_classification: str | None
    baseline_metrics: SampleMetricSnapshot | None = None
    candidate_metrics: SampleMetricSnapshot | None = None

    def __post_init__(self) -> None:
        if self.status not in VALID_COMPARISON_STATUSES:
            raise ValueError(f"unknown comparison status '{self.status}'")

    def delta_pct(self) -> float | None:
        if self.status not in COMPARABLE_COMPARISON_STATUSES:
            return None
        if (
            self.baseline_ms is None
            or self.candidate_ms is None
            or self.baseline_ms <= 0.0
        ):
            return None
        return ((self.candidate_ms - self.baseline_ms) / self.baseline_ms) * 100.0

    def to_json_dict(self) -> dict[str, object]:
        return {
            "case": self.case,
            "status": self.status,
            "display_change": self.change,
            "baseline_ms": self.baseline_ms,
            "candidate_ms": self.candidate_ms,
            "delta_pct": self.delta_pct(),
            "baseline_classification": self.baseline_classification,
            "candidate_classification": self.candidate_classification,
        }


@dataclass(frozen=True)
class Summary:
    faster: int
    slower: int
    no_change: int
    incomparable: int
    new: int
    removed: int

    def to_json_dict(self) -> dict[str, int]:
        return {
            "faster": self.faster,
            "slower": self.slower,
            "no_change": self.no_change,
            "incomparable": self.incomparable,
            "new": self.new,
            "removed": self.removed,
        }


@dataclass(frozen=True)
class Comparison:
    rows: list[ComparisonRow]
    summary: Summary

    def to_json_dict(self) -> dict[str, object]:
        return {
            "summary": self.summary.to_json_dict(),
            "rows": [row.to_json_dict() for row in self.rows],
        }
