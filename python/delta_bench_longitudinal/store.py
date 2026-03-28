from __future__ import annotations

import hashlib
import json
import sqlite3
import statistics
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import IO
from typing import Any

from delta_bench_compare.schema import load_benchmark_payload

try:
    import fcntl
except ImportError:  # pragma: no cover - non-POSIX platforms
    fcntl = None  # type: ignore[assignment]


STORE_SCHEMA_VERSION = 2
STORE_DB_FILENAME = "store.sqlite3"

RUNS_REQUIRED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("runner", "TEXT"),
    ("timing_phase", "TEXT"),
    ("dataset_id", "TEXT"),
    ("dataset_fingerprint", "TEXT"),
    ("storage_backend", "TEXT"),
    ("backend_profile", "TEXT"),
    ("lane", "TEXT"),
    ("measurement_kind", "TEXT"),
    ("validation_level", "TEXT"),
    ("harness_revision", "TEXT"),
    ("fixture_recipe_hash", "TEXT"),
    ("fidelity_fingerprint", "TEXT"),
)

CASE_ROWS_REQUIRED_COLUMNS: tuple[tuple[str, str], ...] = (
    ("compatibility_key", "TEXT"),
    ("case_definition_hash", "TEXT"),
)


def ingest_benchmark_result(
    *,
    store_dir: Path | str,
    result_path: Path | str,
    revision: str,
    commit_timestamp: str,
) -> dict[str, Any]:
    store_root = Path(store_dir)
    _raise_if_unmigrated_legacy_store(store_root)
    source = Path(result_path)
    payload = load_benchmark_payload(source)
    _validate_authoritative_longitudinal_payload(payload, source)
    context = payload.get("context", {})
    cases = payload.get("cases", [])
    run_id = _run_id(
        revision=revision,
        commit_timestamp=commit_timestamp,
        context=context,
        payload=payload,
    )
    ingested_at = datetime.now(timezone.utc).isoformat()
    run_record = _normalize_run_record(
        run_id=run_id,
        ingested_at=ingested_at,
        revision=revision,
        commit_timestamp=commit_timestamp,
        context=context,
        source=source,
    )
    case_rows = [
        _normalize_case_row(
            case=case,
        )
        for case in cases
    ]

    with store_lock(store_root):
        with closing(_connect_store(store_root)) as conn:
            if _run_exists(conn, run_id):
                return {"run_id": run_id, "rows_appended": 0, "deduped": True}
            with conn:
                _insert_run(conn, run_record)
                if case_rows:
                    conn.executemany(
                        """
                        INSERT INTO case_rows (
                            run_id,
                            case_name,
                            compatibility_key,
                            case_definition_hash,
                            success,
                            failure_reason,
                            sample_count,
                            sample_values_json,
                            best_ms,
                            min_ms,
                            max_ms,
                            mean_ms,
                            median_ms
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        [_case_row_params(run_id=run_id, row=row) for row in case_rows],
                    )

    return {"run_id": run_id, "rows_appended": len(case_rows), "deduped": False}


def load_longitudinal_rows(store_dir: Path | str) -> list[dict[str, Any]]:
    store_root = Path(store_dir)
    _raise_if_unmigrated_legacy_store(store_root)
    db_path = store_db_path(store_root)
    if not db_path.exists():
        return []
    with closing(_connect_store(store_root)) as conn:
        rows = conn.execute(
            """
            SELECT
                r.run_id,
                r.ingested_at,
                r.revision,
                r.revision_commit_timestamp,
                r.benchmark_created_at,
                r.label,
                r.git_sha,
                r.host,
                r.suite,
                r.runner,
                r.scale,
                r.timing_phase,
                r.dataset_id,
                r.dataset_fingerprint,
                r.storage_backend,
                r.backend_profile,
                r.lane,
                r.measurement_kind,
                r.validation_level,
                r.harness_revision,
                r.fixture_recipe_hash,
                r.fidelity_fingerprint,
                r.iterations,
                r.warmup,
                r.image_version,
                r.hardening_profile_id,
                r.hardening_profile_sha256,
                r.cpu_model,
                r.cpu_microcode,
                r.kernel,
                r.boot_params,
                r.cpu_steal_pct,
                r.numa_topology,
                r.egress_policy_sha256,
                r.run_mode,
                r.maintenance_window_id,
                r.source_result_path,
                c.case_name,
                c.compatibility_key,
                c.case_definition_hash,
                c.success,
                c.failure_reason,
                c.sample_count,
                c.sample_values_json,
                c.best_ms,
                c.min_ms,
                c.max_ms,
                c.mean_ms,
                c.median_ms
            FROM runs AS r
            LEFT JOIN case_rows AS c ON c.run_id = r.run_id
            ORDER BY
                COALESCE(r.revision_commit_timestamp, ''),
                COALESCE(r.benchmark_created_at, r.ingested_at, ''),
                r.run_id,
                c.case_name
            """
        ).fetchall()
    return [_row_from_db(row) for row in rows if row["case_name"] is not None]


def _normalize_run_record(
    *,
    run_id: str,
    ingested_at: str,
    revision: str,
    commit_timestamp: str,
    context: dict[str, Any],
    source: Path,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "ingested_at": ingested_at,
        "revision": revision,
        "revision_commit_timestamp": commit_timestamp,
        "benchmark_created_at": context.get("created_at"),
        "label": context.get("label"),
        "git_sha": context.get("git_sha"),
        "host": context.get("host"),
        "suite": context.get("suite"),
        "runner": context.get("runner"),
        "scale": context.get("scale"),
        "timing_phase": context.get("timing_phase"),
        "dataset_id": context.get("dataset_id"),
        "dataset_fingerprint": context.get("dataset_fingerprint"),
        "storage_backend": context.get("storage_backend"),
        "backend_profile": context.get("backend_profile"),
        "lane": context.get("lane"),
        "measurement_kind": context.get("measurement_kind"),
        "validation_level": context.get("validation_level"),
        "harness_revision": context.get("harness_revision"),
        "fixture_recipe_hash": context.get("fixture_recipe_hash"),
        "fidelity_fingerprint": context.get("fidelity_fingerprint"),
        "iterations": context.get("iterations"),
        "warmup": context.get("warmup"),
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


def _validate_authoritative_longitudinal_payload(
    payload: dict[str, Any], source: Path
) -> None:
    if payload.get("schema_version") != 4:
        raise ValueError(
            f"{source}: authoritative longitudinal ingest requires schema v4 benchmark payloads"
        )
    context = payload.get("context") or {}
    if context.get("schema_version") != 4:
        raise ValueError(
            f"{source}: authoritative longitudinal ingest requires context.schema_version=4"
        )
    for field in (
        "lane",
        "measurement_kind",
        "validation_level",
        "harness_revision",
        "fixture_recipe_hash",
    ):
        value = context.get(field)
        if not isinstance(value, str) or not value:
            raise ValueError(
                f"{source}: context is missing required authoritative identity field '{field}'"
            )

    for case in payload.get("cases", []):
        case_name = case.get("case", "<unknown>")
        for field in ("compatibility_key", "case_definition_hash"):
            value = case.get(field)
            if not isinstance(value, str) or not value:
                raise ValueError(
                    f"{source}: case '{case_name}' is missing required longitudinal identity field '{field}'"
                )
        if not isinstance(case.get("run_summary"), dict):
            raise ValueError(
                f"{source}: case '{case_name}' must include run_summary for authoritative longitudinal ingest"
            )


def _normalize_case_row(
    *,
    case: dict[str, Any],
) -> dict[str, Any]:
    perf_valid = bool(case.get("perf_valid", True))
    summary = case.get("run_summary") or {}
    samples = case.get("samples") or []
    elapsed = (
        [float(sample["elapsed_ms"]) for sample in samples if "elapsed_ms" in sample]
        if perf_valid
        else []
    )
    metrics = _elapsed_metrics(elapsed)
    failure = case.get("failure") or {}
    if summary and perf_valid:
        metrics = {
            "best_ms": summary.get("min_ms"),
            "min_ms": summary.get("min_ms"),
            "max_ms": summary.get("max_ms"),
            "mean_ms": summary.get("mean_ms"),
            "median_ms": summary.get("median_ms"),
        }

    return {
        "case": case.get("case"),
        "compatibility_key": case.get("compatibility_key"),
        "case_definition_hash": case.get("case_definition_hash"),
        "success": bool(case.get("success", False)),
        "failure_reason": failure.get("message"),
        "sample_count": len(elapsed),
        "sample_values_ms": elapsed,
        "best_ms": metrics["best_ms"],
        "min_ms": metrics["min_ms"],
        "max_ms": metrics["max_ms"],
        "mean_ms": metrics["mean_ms"],
        "median_ms": metrics["median_ms"],
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
    explicit_run_id = context.get("run_id")
    if isinstance(explicit_run_id, str) and explicit_run_id:
        return explicit_run_id
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


def store_db_path(store_dir: Path | str) -> Path:
    return Path(store_dir) / STORE_DB_FILENAME


def _raise_if_unmigrated_legacy_store(store_dir: Path) -> None:
    db_path = store_db_path(store_dir)
    if db_path.exists():
        return
    legacy_paths = [store_dir / "rows.jsonl", store_dir / "index.json"]
    present = [path.name for path in legacy_paths if path.exists()]
    if present:
        names = ", ".join(present)
        raise ValueError(
            f"legacy longitudinal store detected at {store_dir}; migrate or remove {names} before using store.sqlite3"
        )


def _connect_store(store_dir: Path) -> sqlite3.Connection:
    db_path = store_db_path(store_dir)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    _ensure_schema(conn)
    return conn


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS runs (
            run_id TEXT PRIMARY KEY,
            schema_version INTEGER NOT NULL,
            ingested_at TEXT NOT NULL,
            revision TEXT NOT NULL,
            revision_commit_timestamp TEXT NOT NULL,
            benchmark_created_at TEXT,
            label TEXT,
            git_sha TEXT,
            host TEXT,
            suite TEXT,
            runner TEXT,
            scale TEXT,
            timing_phase TEXT,
            dataset_id TEXT,
            dataset_fingerprint TEXT,
            storage_backend TEXT,
            backend_profile TEXT,
            lane TEXT,
            measurement_kind TEXT,
            validation_level TEXT,
            harness_revision TEXT,
            fixture_recipe_hash TEXT,
            fidelity_fingerprint TEXT,
            iterations INTEGER,
            warmup INTEGER,
            image_version TEXT,
            hardening_profile_id TEXT,
            hardening_profile_sha256 TEXT,
            cpu_model TEXT,
            cpu_microcode TEXT,
            kernel TEXT,
            boot_params TEXT,
            cpu_steal_pct REAL,
            numa_topology TEXT,
            egress_policy_sha256 TEXT,
            run_mode TEXT,
            maintenance_window_id TEXT,
            source_result_path TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS case_rows (
            run_id TEXT NOT NULL,
            case_name TEXT NOT NULL,
            compatibility_key TEXT,
            case_definition_hash TEXT,
            success INTEGER NOT NULL,
            failure_reason TEXT,
            sample_count INTEGER NOT NULL,
            sample_values_json TEXT NOT NULL,
            best_ms REAL,
            min_ms REAL,
            max_ms REAL,
            mean_ms REAL,
            median_ms REAL,
            PRIMARY KEY (run_id, case_name),
            FOREIGN KEY (run_id) REFERENCES runs(run_id) ON DELETE CASCADE
        );

        CREATE INDEX IF NOT EXISTS idx_runs_ordering
        ON runs (suite, scale, revision_commit_timestamp, benchmark_created_at, ingested_at, run_id);
        """
    )
    _ensure_table_columns(conn, "runs", RUNS_REQUIRED_COLUMNS)
    _ensure_table_columns(conn, "case_rows", CASE_ROWS_REQUIRED_COLUMNS)
    conn.execute(f"PRAGMA user_version = {STORE_SCHEMA_VERSION}")


def _ensure_table_columns(
    conn: sqlite3.Connection,
    table: str,
    required_columns: tuple[tuple[str, str], ...],
) -> None:
    existing_columns = {
        str(row[1]) for row in conn.execute(f"PRAGMA table_info({table})").fetchall()
    }
    for column, column_type in required_columns:
        if column in existing_columns:
            continue
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {column_type}")


def _run_exists(conn: sqlite3.Connection, run_id: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM runs WHERE run_id = ? LIMIT 1",
        (run_id,),
    ).fetchone()
    return row is not None


def _insert_run(conn: sqlite3.Connection, row: dict[str, Any]) -> None:
    conn.execute(
        """
        INSERT INTO runs (
            run_id,
            schema_version,
            ingested_at,
            revision,
            revision_commit_timestamp,
            benchmark_created_at,
            label,
            git_sha,
            host,
            suite,
            runner,
            scale,
            timing_phase,
            dataset_id,
            dataset_fingerprint,
            storage_backend,
            backend_profile,
            lane,
            measurement_kind,
            validation_level,
            harness_revision,
            fixture_recipe_hash,
            fidelity_fingerprint,
            iterations,
            warmup,
            image_version,
            hardening_profile_id,
            hardening_profile_sha256,
            cpu_model,
            cpu_microcode,
            kernel,
            boot_params,
            cpu_steal_pct,
            numa_topology,
            egress_policy_sha256,
            run_mode,
            maintenance_window_id,
            source_result_path
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["run_id"],
            STORE_SCHEMA_VERSION,
            row["ingested_at"],
            row["revision"],
            row["revision_commit_timestamp"],
            row["benchmark_created_at"],
            row["label"],
            row["git_sha"],
            row["host"],
            row["suite"],
            row["runner"],
            row["scale"],
            row["timing_phase"],
            row["dataset_id"],
            row["dataset_fingerprint"],
            row["storage_backend"],
            row["backend_profile"],
            row["lane"],
            row["measurement_kind"],
            row["validation_level"],
            row["harness_revision"],
            row["fixture_recipe_hash"],
            row["fidelity_fingerprint"],
            row["iterations"],
            row["warmup"],
            row["image_version"],
            row["hardening_profile_id"],
            row["hardening_profile_sha256"],
            row["cpu_model"],
            row["cpu_microcode"],
            row["kernel"],
            row["boot_params"],
            row["cpu_steal_pct"],
            row["numa_topology"],
            row["egress_policy_sha256"],
            row["run_mode"],
            row["maintenance_window_id"],
            row["source_result_path"],
        ),
    )


def _case_row_params(*, run_id: str, row: dict[str, Any]) -> tuple[Any, ...]:
    return (
        run_id,
        row["case"],
        row["compatibility_key"],
        row["case_definition_hash"],
        int(bool(row["success"])),
        row["failure_reason"],
        row["sample_count"],
        json.dumps(row["sample_values_ms"]),
        row["best_ms"],
        row["min_ms"],
        row["max_ms"],
        row["mean_ms"],
        row["median_ms"],
    )


def _row_from_db(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "schema_version": STORE_SCHEMA_VERSION,
        "run_id": row["run_id"],
        "ingested_at": row["ingested_at"],
        "revision": row["revision"],
        "revision_commit_timestamp": row["revision_commit_timestamp"],
        "benchmark_created_at": row["benchmark_created_at"],
        "label": row["label"],
        "git_sha": row["git_sha"],
        "host": row["host"],
        "suite": row["suite"],
        "runner": row["runner"],
        "scale": row["scale"],
        "timing_phase": row["timing_phase"],
        "dataset_id": row["dataset_id"],
        "dataset_fingerprint": row["dataset_fingerprint"],
        "storage_backend": row["storage_backend"],
        "backend_profile": row["backend_profile"],
        "lane": row["lane"],
        "measurement_kind": row["measurement_kind"],
        "validation_level": row["validation_level"],
        "harness_revision": row["harness_revision"],
        "fixture_recipe_hash": row["fixture_recipe_hash"],
        "fidelity_fingerprint": row["fidelity_fingerprint"],
        "iterations": row["iterations"],
        "warmup": row["warmup"],
        "case": row["case_name"],
        "compatibility_key": row["compatibility_key"],
        "case_definition_hash": row["case_definition_hash"],
        "success": bool(row["success"]),
        "failure_reason": row["failure_reason"],
        "sample_count": row["sample_count"],
        "sample_values_ms": json.loads(row["sample_values_json"]),
        "best_ms": row["best_ms"],
        "min_ms": row["min_ms"],
        "max_ms": row["max_ms"],
        "mean_ms": row["mean_ms"],
        "median_ms": row["median_ms"],
        "image_version": row["image_version"],
        "hardening_profile_id": row["hardening_profile_id"],
        "hardening_profile_sha256": row["hardening_profile_sha256"],
        "cpu_model": row["cpu_model"],
        "cpu_microcode": row["cpu_microcode"],
        "kernel": row["kernel"],
        "boot_params": row["boot_params"],
        "cpu_steal_pct": row["cpu_steal_pct"],
        "numa_topology": row["numa_topology"],
        "egress_policy_sha256": row["egress_policy_sha256"],
        "run_mode": row["run_mode"],
        "maintenance_window_id": row["maintenance_window_id"],
        "source_result_path": row["source_result_path"],
    }


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
