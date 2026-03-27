from __future__ import annotations

import json
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .store import _connect_store, _raise_if_unmigrated_legacy_store, store_db_path, store_lock


def prune_artifacts(
    *,
    artifacts_dir: Path | str,
    max_age_days: int | None,
    max_artifacts: int | None,
    apply: bool,
    now: datetime | None = None,
) -> dict[str, Any]:
    _validate_policies(
        max_age_days=max_age_days, max_count=max_artifacts, count_name="max_artifacts"
    )
    reference = now or datetime.now(timezone.utc)
    root = Path(artifacts_dir)
    if not root.exists():
        return {"total": 0, "candidates": [], "removed": 0, "applied": apply}

    entries: list[tuple[str, datetime, Path]] = []
    for child in root.iterdir():
        if not child.is_dir():
            continue
        revision = child.name
        timestamp = _artifact_timestamp(child)
        entries.append((revision, timestamp, child))

    entries.sort(key=lambda item: item[1], reverse=True)
    candidate_revisions = _select_candidates(
        entries=[(rev, ts) for rev, ts, _ in entries],
        max_age_days=max_age_days,
        max_count=max_artifacts,
        now=reference,
    )
    removed = 0
    if apply:
        candidates_set = set(candidate_revisions)
        for revision, _timestamp, path in entries:
            if revision not in candidates_set:
                continue
            shutil.rmtree(path, ignore_errors=False)
            removed += 1

    return {
        "total": len(entries),
        "candidates": candidate_revisions,
        "removed": removed,
        "applied": apply,
    }


def prune_store(
    *,
    store_dir: Path | str,
    max_age_days: int | None,
    max_runs: int | None,
    apply: bool,
    now: datetime | None = None,
) -> dict[str, Any]:
    _validate_policies(
        max_age_days=max_age_days, max_count=max_runs, count_name="max_runs"
    )
    reference = now or datetime.now(timezone.utc)
    root = Path(store_dir)
    _raise_if_unmigrated_legacy_store(root)
    db_path = store_db_path(root)
    with store_lock(root):
        if not db_path.exists():
            return {
                "total_runs": 0,
                "candidate_runs": [],
                "removed_runs": 0,
                "remaining_runs": 0,
                "invalid_rows_skipped": 0,
                "applied": apply,
            }

        conn = _connect_store(root)
        try:
            run_timestamps = _load_run_timestamps(conn)

            ordered = sorted(
                list(run_timestamps.items()),
                key=lambda item: item[1],
                reverse=True,
            )
            candidate_runs = _select_candidates(
                entries=ordered,
                max_age_days=max_age_days,
                max_count=max_runs,
                now=reference,
            )

            if apply and candidate_runs:
                with conn:
                    conn.executemany(
                        "DELETE FROM runs WHERE run_id = ?",
                        [(run_id,) for run_id in candidate_runs],
                    )

            remaining_runs = (
                conn.execute("SELECT COUNT(*) FROM runs").fetchone()[0]
                if apply
                else len(run_timestamps)
            )

            return {
                "total_runs": len(run_timestamps),
                "candidate_runs": candidate_runs,
                "removed_runs": len(candidate_runs) if apply else 0,
                "remaining_runs": remaining_runs,
                "invalid_rows_skipped": 0,
                "applied": apply,
            }
        finally:
            conn.close()


def _validate_policies(
    *,
    max_age_days: int | None,
    max_count: int | None,
    count_name: str,
) -> None:
    if max_age_days is None and max_count is None:
        raise ValueError("at least one retention policy must be configured")
    if max_age_days is not None and max_age_days <= 0:
        raise ValueError("max_age_days must be > 0")
    if max_count is not None and max_count <= 0:
        raise ValueError(f"{count_name} must be > 0")


def _select_candidates(
    *,
    entries: list[tuple[str, datetime]],
    max_age_days: int | None,
    max_count: int | None,
    now: datetime,
) -> list[str]:
    candidates: set[str] = set()

    if max_count is not None:
        for revision, _ts in entries[max_count:]:
            candidates.add(revision)

    if max_age_days is not None:
        cutoff = now - timedelta(days=max_age_days)
        for revision, ts in entries:
            if ts < cutoff:
                candidates.add(revision)

    return sorted(candidates)


def _artifact_timestamp(path: Path) -> datetime:
    metadata_path = path / "metadata.json"
    if metadata_path.exists():
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        timestamp = _parse_datetime(payload.get("build_timestamp"))
        if timestamp is not None:
            return timestamp
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def _load_run_timestamps(conn: sqlite3.Connection) -> dict[str, datetime]:
    rows = conn.execute(
        """
        SELECT run_id, benchmark_created_at, ingested_at
        FROM runs
        """
    ).fetchall()
    runs: dict[str, datetime] = {}
    for run_id, benchmark_created_at, ingested_at in rows:
        row = {
            "benchmark_created_at": benchmark_created_at,
            "ingested_at": ingested_at,
        }
        runs[str(run_id)] = _row_timestamp(row)
    return runs


def _row_timestamp(row: dict[str, Any]) -> datetime:
    timestamp = _parse_datetime(row.get("benchmark_created_at"))
    if timestamp is not None:
        return timestamp
    timestamp = _parse_datetime(row.get("ingested_at"))
    if timestamp is not None:
        return timestamp
    return datetime.fromtimestamp(0, tz=timezone.utc)


def _parse_datetime(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
