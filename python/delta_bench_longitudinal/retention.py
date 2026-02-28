from __future__ import annotations

import json
import os
import shutil
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .store import store_lock


def prune_artifacts(
    *,
    artifacts_dir: Path | str,
    max_age_days: int | None,
    max_artifacts: int | None,
    apply: bool,
    now: datetime | None = None,
) -> dict[str, Any]:
    _validate_policies(max_age_days=max_age_days, max_count=max_artifacts, count_name="max_artifacts")
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
    _validate_policies(max_age_days=max_age_days, max_count=max_runs, count_name="max_runs")
    reference = now or datetime.now(timezone.utc)
    root = Path(store_dir)
    rows_path = root / "rows.jsonl"
    index_path = root / "index.json"
    with store_lock(root):
        if not rows_path.exists():
            return {
                "total_runs": 0,
                "candidate_runs": [],
                "removed_runs": 0,
                "remaining_runs": 0,
                "invalid_rows_skipped": 0,
                "applied": apply,
            }

        run_timestamps, invalid_rows = _scan_run_timestamps(rows_path)

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
        candidate_set = set(candidate_runs)

        if apply:
            kept_runs = _rewrite_rows_without_candidates(rows_path, candidate_set)
            _write_index(index_path, sorted(kept_runs))

        return {
            "total_runs": len(run_timestamps),
            "candidate_runs": candidate_runs,
            "removed_runs": len(candidate_runs) if apply else 0,
            "remaining_runs": len(run_timestamps) - len(candidate_runs) if apply else len(run_timestamps),
            "invalid_rows_skipped": invalid_rows,
            "applied": apply,
        }


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


def _scan_run_timestamps(path: Path) -> tuple[dict[str, datetime], int]:
    runs: dict[str, datetime] = {}
    skipped = 0
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                skipped += 1
                continue
            run_id = str(row.get("run_id", ""))
            if not run_id:
                continue
            ts = _row_timestamp(row)
            previous = runs.get(run_id)
            if previous is None or ts > previous:
                runs[run_id] = ts
    return runs, skipped


def _rewrite_rows_without_candidates(path: Path, candidate_runs: set[str]) -> set[str]:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_name(f".{path.name}.tmp")
    kept_runs: set[str] = set()
    with temp.open("w", encoding="utf-8") as fh:
        with path.open("r", encoding="utf-8") as src:
            for line in src:
                if not line.strip():
                    continue
                try:
                    row = json.loads(line)
                except json.JSONDecodeError:
                    continue
                run_id = str(row.get("run_id", ""))
                if run_id and run_id in candidate_runs:
                    continue
                if run_id:
                    kept_runs.add(run_id)
                # Normalize line format during retention rewrite.
                fh.write(json.dumps(row, sort_keys=True))
                fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())
    temp.replace(path)
    return kept_runs


def _write_index(path: Path, run_ids: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"schema_version": 1, "run_ids": run_ids}
    temp = path.with_name(f".{path.name}.tmp")
    with temp.open("w", encoding="utf-8") as fh:
        fh.write(json.dumps(payload, sort_keys=True))
        fh.flush()
        os.fsync(fh.fileno())
    temp.replace(path)


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
