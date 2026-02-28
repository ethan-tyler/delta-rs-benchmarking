from __future__ import annotations

import hashlib
import json
import os
import statistics
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import IO
from typing import Any

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]


STORE_SCHEMA_VERSION = 1


def ingest_benchmark_result(
    *,
    store_dir: Path | str,
    result_path: Path | str,
    revision: str,
    commit_timestamp: str,
) -> dict[str, Any]:
    store_root = Path(store_dir)
    source = Path(result_path)
    payload = json.loads(source.read_text(encoding="utf-8"))
    context = payload.get("context", {})
    cases = payload.get("cases", [])
    run_id = _run_id(
        revision=revision,
        commit_timestamp=commit_timestamp,
        context=context,
        payload=payload,
    )

    with store_lock(store_root):
        index = _load_index(store_root)
        if run_id in index:
            return {"run_id": run_id, "rows_appended": 0, "deduped": True}

        ingested_at = datetime.now(timezone.utc).isoformat()
        rows = [
            _normalize_case_row(
                run_id=run_id,
                ingested_at=ingested_at,
                revision=revision,
                commit_timestamp=commit_timestamp,
                context=context,
                case=case,
                source=source,
            )
            for case in cases
        ]

        _append_rows(_rows_path(store_root), rows)
        index.add(run_id)
        _save_index(store_root, index)

    return {"run_id": run_id, "rows_appended": len(rows), "deduped": False}


def load_longitudinal_rows(store_dir: Path | str) -> list[dict[str, Any]]:
    rows_path = _rows_path(Path(store_dir))
    if not rows_path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in rows_path.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        rows.append(json.loads(line))
    return rows


def _normalize_case_row(
    *,
    run_id: str,
    ingested_at: str,
    revision: str,
    commit_timestamp: str,
    context: dict[str, Any],
    case: dict[str, Any],
    source: Path,
) -> dict[str, Any]:
    samples = case.get("samples") or []
    elapsed = [float(sample["elapsed_ms"]) for sample in samples if "elapsed_ms" in sample]
    metrics = _elapsed_metrics(elapsed)
    failure = case.get("failure") or {}

    return {
        "schema_version": STORE_SCHEMA_VERSION,
        "run_id": run_id,
        "ingested_at": ingested_at,
        "revision": revision,
        "revision_commit_timestamp": commit_timestamp,
        "benchmark_created_at": context.get("created_at"),
        "label": context.get("label"),
        "git_sha": context.get("git_sha"),
        "host": context.get("host"),
        "suite": context.get("suite"),
        "scale": context.get("scale"),
        "iterations": context.get("iterations"),
        "warmup": context.get("warmup"),
        "case": case.get("case"),
        "success": bool(case.get("success", False)),
        "failure_reason": failure.get("message"),
        "sample_count": len(elapsed),
        "sample_values_ms": elapsed,
        "best_ms": metrics["best_ms"],
        "min_ms": metrics["min_ms"],
        "max_ms": metrics["max_ms"],
        "mean_ms": metrics["mean_ms"],
        "median_ms": metrics["median_ms"],
        "image_version": context.get("image_version"),
        "hardening_profile_id": context.get("hardening_profile_id"),
        "hardening_profile_sha256": context.get("hardening_profile_sha256"),
        "cpu_model": context.get("cpu_model"),
        "cpu_microcode": context.get("cpu_microcode"),
        "kernel": context.get("kernel"),
        "boot_params": context.get("boot_params"),
        "cpu_steal_pct": context.get("cpu_steal_pct"),
        "numa_topology": context.get("numa_topology"),
        "egress_policy_sha256": context.get("egress_policy_sha256"),
        "run_mode": context.get("run_mode"),
        "maintenance_window_id": context.get("maintenance_window_id"),
        "source_result_path": str(source),
    }


def _elapsed_metrics(samples: list[float]) -> dict[str, float | None]:
    if not samples:
        return {
            "best_ms": None,
            "min_ms": None,
            "max_ms": None,
            "mean_ms": None,
            "median_ms": None,
        }
    return {
        "best_ms": min(samples),
        "min_ms": min(samples),
        "max_ms": max(samples),
        "mean_ms": sum(samples) / len(samples),
        "median_ms": statistics.median(samples),
    }


def _run_id(
    *,
    revision: str,
    commit_timestamp: str,
    context: dict[str, Any],
    payload: dict[str, Any],
) -> str:
    payload_digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()
    identity = {
        "revision": revision,
        "commit_timestamp": commit_timestamp,
        "created_at": context.get("created_at"),
        "suite": context.get("suite"),
        "scale": context.get("scale"),
        "label": context.get("label"),
        "payload_sha256": payload_digest,
    }
    encoded = json.dumps(identity, sort_keys=True).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _index_path(store_dir: Path) -> Path:
    return store_dir / "index.json"


def _rows_path(store_dir: Path) -> Path:
    return store_dir / "rows.jsonl"


def _load_index(store_dir: Path) -> set[str]:
    path = _index_path(store_dir)
    rows_path = _rows_path(store_dir)
    run_ids: set[str]
    rows_mtime_ns: int | None = None
    rows_size: int | None = None
    if not path.exists():
        run_ids = set()
    else:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            payload = {}
        run_ids = set(payload.get("run_ids", []))
        mtime_candidate = payload.get("rows_mtime_ns")
        size_candidate = payload.get("rows_size")
        if isinstance(mtime_candidate, int) and isinstance(size_candidate, int):
            rows_mtime_ns = mtime_candidate
            rows_size = size_candidate

    # Reconcile with existing rows to preserve idempotency if a prior run crashed
    # after appending rows but before writing index.json. Skip full scan when index
    # is newer than rows.
    if rows_path.exists():
        should_reconcile = True
        if rows_mtime_ns is not None and rows_size is not None:
            try:
                current = rows_path.stat()
                should_reconcile = not (
                    current.st_mtime_ns == rows_mtime_ns and current.st_size == rows_size
                )
            except OSError:
                should_reconcile = True
        elif path.exists():
            try:
                should_reconcile = rows_path.stat().st_mtime > path.stat().st_mtime
            except OSError:
                should_reconcile = True
        if should_reconcile:
            run_ids.update(_scan_row_run_ids(rows_path))
    return run_ids


def _scan_row_run_ids(rows_path: Path) -> set[str]:
    run_ids: set[str] = set()
    with rows_path.open("r", encoding="utf-8") as fh:
        for line in fh:
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            run_id = row.get("run_id")
            if isinstance(run_id, str) and run_id:
                run_ids.add(run_id)
    return run_ids


def _save_index(store_dir: Path, run_ids: set[str]) -> None:
    path = _index_path(store_dir)
    store_dir.mkdir(parents=True, exist_ok=True)
    data: dict[str, Any] = {"schema_version": STORE_SCHEMA_VERSION, "run_ids": sorted(run_ids)}
    rows_path = _rows_path(store_dir)
    if rows_path.exists():
        rows_stat = rows_path.stat()
        data["rows_mtime_ns"] = rows_stat.st_mtime_ns
        data["rows_size"] = rows_stat.st_size
    temp = path.with_suffix(".tmp")
    temp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temp.replace(path)


def _append_rows(rows_path: Path, rows: list[dict[str, Any]]) -> None:
    rows_path.parent.mkdir(parents=True, exist_ok=True)
    with rows_path.open("a", encoding="utf-8") as fh:
        for row in rows:
            fh.write(json.dumps(row, sort_keys=True))
            fh.write("\n")
        fh.flush()
        os.fsync(fh.fileno())


@contextmanager
def store_lock(store_dir: Path) -> IO[str]:
    store_dir.mkdir(parents=True, exist_ok=True)
    lock_path = store_dir / ".lock"
    with lock_path.open("a+", encoding="utf-8") as lock_file:
        if fcntl is not None:
            fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
        try:
            yield lock_file
        finally:
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)


# Backward compatibility for internal imports/tests.
_store_lock = store_lock
