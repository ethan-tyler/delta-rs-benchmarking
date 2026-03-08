from __future__ import annotations

from dataclasses import dataclass


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
    change: str
    baseline_classification: str | None
    candidate_classification: str | None
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
