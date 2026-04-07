from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUEST_STATUSES = {
    "accepted",
    "planning",
    "running",
    "aggregating",
    "completed",
    "failed",
    "blocked",
}
SUITE_STATUSES = {"queued", "running", "completed", "failed", "blocked", "skipped"}
ACTIVE_REQUEST_STATUSES = {"accepted", "planning", "running", "aggregating"}
DEFAULT_DB_PATH = Path("/var/lib/delta-bench/pr-bot.sqlite3")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _connect_db(db_path: Path | str) -> sqlite3.Connection:
    path = Path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA synchronous = FULL")
    _ensure_schema(conn)
    return conn


def _table_columns(conn: sqlite3.Connection, table_name: str) -> list[sqlite3.Row]:
    return conn.execute(f"PRAGMA table_info({table_name})").fetchall()


def _ensure_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS requests (
            request_id INTEGER PRIMARY KEY AUTOINCREMENT,
            repo TEXT NOT NULL,
            pr_number INTEGER NOT NULL,
            comment_id INTEGER NOT NULL UNIQUE,
            actor TEXT NOT NULL,
            command TEXT NOT NULL,
            pack TEXT,
            base_sha TEXT NOT NULL,
            candidate_sha TEXT NOT NULL,
            status TEXT NOT NULL,
            summary_comment_id INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            workflow_run_id INTEGER,
            error_message TEXT
        );

        CREATE TABLE IF NOT EXISTS suites (
            request_id INTEGER NOT NULL,
            suite TEXT NOT NULL,
            profile TEXT NOT NULL,
            status TEXT NOT NULL,
            timeout_minutes INTEGER NOT NULL,
            artifact_name TEXT,
            exit_code INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (request_id, suite, profile),
            FOREIGN KEY (request_id) REFERENCES requests(request_id) ON DELETE CASCADE
        );
        """
    )
    _migrate_legacy_suites(conn)
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_requests_status
        ON requests (repo, pr_number, candidate_sha, status, created_at, request_id);

        CREATE INDEX IF NOT EXISTS idx_requests_comment_id
        ON requests (comment_id);

        CREATE INDEX IF NOT EXISTS idx_suites_request_status
        ON suites (request_id, status, suite, profile);
        """
    )


def _migrate_legacy_suites(conn: sqlite3.Connection) -> None:
    columns = _table_columns(conn, "suites")
    if not columns:
        return
    pk_by_name = {row["name"]: int(row["pk"]) for row in columns}
    needs_migration = (
        pk_by_name.get("request_id") != 1
        or pk_by_name.get("suite") != 2
        or pk_by_name.get("profile") != 3
    )
    if not needs_migration:
        return

    conn.execute("ALTER TABLE suites RENAME TO suites_legacy")
    conn.execute(
        """
        CREATE TABLE suites (
            request_id INTEGER NOT NULL,
            suite TEXT NOT NULL,
            profile TEXT NOT NULL,
            status TEXT NOT NULL,
            timeout_minutes INTEGER NOT NULL,
            artifact_name TEXT,
            exit_code INTEGER,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            PRIMARY KEY (request_id, suite, profile),
            FOREIGN KEY (request_id) REFERENCES requests(request_id) ON DELETE CASCADE
        )
        """
    )

    legacy_columns = {row["name"] for row in _table_columns(conn, "suites_legacy")}
    profile_expr = (
        "COALESCE(NULLIF(profile, ''), suite)"
        if "profile" in legacy_columns
        else "suite"
    )
    artifact_expr = "artifact_name" if "artifact_name" in legacy_columns else "NULL"
    exit_code_expr = "exit_code" if "exit_code" in legacy_columns else "NULL"
    conn.execute(
        f"""
        INSERT INTO suites (
            request_id,
            suite,
            profile,
            status,
            timeout_minutes,
            artifact_name,
            exit_code,
            created_at,
            updated_at
        )
        SELECT
            request_id,
            suite,
            {profile_expr},
            status,
            timeout_minutes,
            {artifact_expr},
            {exit_code_expr},
            created_at,
            updated_at
        FROM suites_legacy
        """
    )
    conn.execute("DROP TABLE suites_legacy")


def initialize_db(db_path: Path | str) -> None:
    with closing(_connect_db(db_path)):
        return


def _request_exists(conn: sqlite3.Connection, request_id: int) -> bool:
    row = conn.execute(
        "SELECT 1 FROM requests WHERE request_id = ?",
        (request_id,),
    ).fetchone()
    return row is not None


def _normalize_suites_for_compare(suites: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for suite in suites:
        normalized.append(
            {
                "suite": str(suite["suite"]),
                "profile": str(suite["profile"]),
                "timeout_minutes": int(suite["timeout_minutes"]),
                "artifact_name": str(suite.get("artifact_name") or ""),
            }
        )
    return sorted(
        normalized,
        key=lambda item: (
            item["suite"],
            item["profile"],
            item["timeout_minutes"],
            item["artifact_name"],
        ),
    )


def _request_payload_matches(
    conn: sqlite3.Connection,
    request_row: sqlite3.Row,
    *,
    repo: str,
    pr_number: int,
    actor: str,
    command: str,
    pack: str | None,
    base_sha: str,
    candidate_sha: str,
    suites: list[dict[str, Any]],
) -> bool:
    existing_request = {
        "repo": str(request_row["repo"]),
        "pr_number": int(request_row["pr_number"]),
        "actor": str(request_row["actor"]),
        "command": str(request_row["command"]),
        "pack": str(request_row["pack"] or ""),
        "base_sha": str(request_row["base_sha"]),
        "candidate_sha": str(request_row["candidate_sha"]),
    }
    expected_request = {
        "repo": repo,
        "pr_number": int(pr_number),
        "actor": actor,
        "command": command,
        "pack": str(pack or ""),
        "base_sha": base_sha,
        "candidate_sha": candidate_sha,
    }
    if existing_request != expected_request:
        return False

    suite_rows = conn.execute(
        """
        SELECT suite, profile, timeout_minutes, artifact_name
        FROM suites
        WHERE request_id = ?
        ORDER BY suite, profile
        """,
        (int(request_row["request_id"]),),
    ).fetchall()
    existing_suites = [
        {
            "suite": str(row["suite"]),
            "profile": str(row["profile"]),
            "timeout_minutes": int(row["timeout_minutes"]),
            "artifact_name": str(row["artifact_name"] or ""),
        }
        for row in suite_rows
    ]
    return existing_suites == _normalize_suites_for_compare(suites)


def _insert_request(
    conn: sqlite3.Connection,
    *,
    repo: str,
    pr_number: int,
    comment_id: int,
    actor: str,
    command: str,
    pack: str | None,
    base_sha: str,
    candidate_sha: str,
    workflow_run_id: int | None,
    suites: list[dict[str, Any]],
    status: str,
) -> int:
    timestamp = _utc_now()
    cursor = conn.execute(
        """
        INSERT INTO requests (
            repo,
            pr_number,
            comment_id,
            actor,
            command,
            pack,
            base_sha,
            candidate_sha,
            status,
            created_at,
            updated_at,
            workflow_run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            repo,
            int(pr_number),
            int(comment_id),
            actor,
            command,
            pack,
            base_sha,
            candidate_sha,
            status,
            timestamp,
            timestamp,
            workflow_run_id,
        ),
    )
    request_id = int(cursor.lastrowid)
    for suite in suites:
        conn.execute(
            """
            INSERT INTO suites (
                request_id,
                suite,
                profile,
                status,
                timeout_minutes,
                artifact_name,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                request_id,
                str(suite["suite"]),
                str(suite["profile"]),
                str(suite.get("status") or "queued"),
                int(suite["timeout_minutes"]),
                suite.get("artifact_name"),
                timestamp,
                timestamp,
            ),
        )
    return request_id


def create_request(
    db_path: Path | str,
    *,
    repo: str,
    pr_number: int,
    comment_id: int,
    actor: str,
    command: str,
    pack: str | None,
    base_sha: str,
    candidate_sha: str,
    workflow_run_id: int | None,
    suites: list[dict[str, Any]],
    status: str = "planning",
) -> int:
    if status not in REQUEST_STATUSES:
        raise ValueError(f"invalid request status '{status}'")
    try:
        with closing(_connect_db(db_path)) as conn:
            with conn:
                return _insert_request(
                    conn,
                    repo=repo,
                    pr_number=pr_number,
                    comment_id=comment_id,
                    actor=actor,
                    command=command,
                    pack=pack,
                    base_sha=base_sha,
                    candidate_sha=candidate_sha,
                    workflow_run_id=workflow_run_id,
                    suites=suites,
                    status=status,
                )
    except sqlite3.IntegrityError as exc:
        if "requests.comment_id" in str(exc) or "UNIQUE constraint failed: requests.comment_id" in str(exc):
            raise ValueError(f"duplicate request for comment_id={comment_id}") from exc
        raise


def ensure_request(
    db_path: Path | str,
    *,
    repo: str,
    pr_number: int,
    comment_id: int,
    actor: str,
    command: str,
    pack: str | None,
    base_sha: str,
    candidate_sha: str,
    workflow_run_id: int | None,
    suites: list[dict[str, Any]],
    status: str = "planning",
) -> tuple[int, bool]:
    if status not in REQUEST_STATUSES:
        raise ValueError(f"invalid request status '{status}'")
    with closing(_connect_db(db_path)) as conn:
        with conn:
            existing = conn.execute(
                "SELECT * FROM requests WHERE comment_id = ?",
                (int(comment_id),),
            ).fetchone()
            if existing is not None:
                if not _request_payload_matches(
                    conn,
                    existing,
                    repo=repo,
                    pr_number=pr_number,
                    actor=actor,
                    command=command,
                    pack=pack,
                    base_sha=base_sha,
                    candidate_sha=candidate_sha,
                    suites=suites,
                ):
                    raise ValueError(
                        f"comment_id={comment_id} conflicts with an existing request payload"
                    )
                return int(existing["request_id"]), False

            request_id = _insert_request(
                conn,
                repo=repo,
                pr_number=pr_number,
                comment_id=comment_id,
                actor=actor,
                command=command,
                pack=pack,
                base_sha=base_sha,
                candidate_sha=candidate_sha,
                workflow_run_id=workflow_run_id,
                suites=suites,
                status=status,
            )
            return request_id, True


def update_request_status(
    db_path: Path | str,
    *,
    request_id: int,
    status: str,
    summary_comment_id: int | None = None,
    error_message: str | None = None,
) -> None:
    if status not in REQUEST_STATUSES:
        raise ValueError(f"invalid request status '{status}'")
    timestamp = _utc_now()
    with closing(_connect_db(db_path)) as conn:
        with conn:
            if not _request_exists(conn, request_id):
                raise ValueError(f"request_id={request_id} was not found")
            fields = ["status = ?", "updated_at = ?"]
            params: list[Any] = [status, timestamp]
            if summary_comment_id is not None:
                fields.append("summary_comment_id = ?")
                params.append(summary_comment_id)
            if error_message is not None:
                fields.append("error_message = ?")
                params.append(error_message)
            params.append(request_id)
            conn.execute(
                f"UPDATE requests SET {', '.join(fields)} WHERE request_id = ?",
                params,
            )


def list_request_suites(
    db_path: Path | str, *, request_id: int
) -> list[dict[str, Any]]:
    with closing(_connect_db(db_path)) as conn:
        rows = conn.execute(
            """
            SELECT
                suite,
                profile,
                status,
                timeout_minutes,
                artifact_name,
                exit_code,
                created_at,
                updated_at
            FROM suites
            WHERE request_id = ?
            ORDER BY suite, profile
            """,
            (int(request_id),),
        ).fetchall()
    return [dict(row) for row in rows]


def get_request_state(db_path: Path | str, *, request_id: int) -> dict[str, Any]:
    with closing(_connect_db(db_path)) as conn:
        request = conn.execute(
            """
            SELECT
                request_id,
                repo,
                pr_number,
                comment_id,
                actor,
                command,
                pack,
                base_sha,
                candidate_sha,
                status,
                summary_comment_id,
                workflow_run_id,
                error_message,
                created_at,
                updated_at
            FROM requests
            WHERE request_id = ?
            """,
            (int(request_id),),
        ).fetchone()
        if request is None:
            raise ValueError(f"request_id={request_id} was not found")
        suites = conn.execute(
            """
            SELECT
                suite,
                profile,
                status,
                timeout_minutes,
                artifact_name,
                exit_code,
                created_at,
                updated_at
            FROM suites
            WHERE request_id = ?
            ORDER BY suite, profile
            """,
            (int(request_id),),
        ).fetchall()
    payload = dict(request)
    payload["suites"] = [dict(row) for row in suites]
    return payload


def replay_request(
    db_path: Path | str,
    *,
    request_id: int,
    workflow_run_id: int | None,
    suites: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    timestamp = _utc_now()
    with closing(_connect_db(db_path)) as conn:
        with conn:
            request = conn.execute(
                "SELECT * FROM requests WHERE request_id = ?",
                (int(request_id),),
            ).fetchone()
            if request is None:
                raise ValueError(f"request_id={request_id} was not found")

            if suites is not None and not _request_payload_matches(
                conn,
                request,
                repo=str(request["repo"]),
                pr_number=int(request["pr_number"]),
                actor=str(request["actor"]),
                command=str(request["command"]),
                pack=str(request["pack"] or "") or None,
                base_sha=str(request["base_sha"]),
                candidate_sha=str(request["candidate_sha"]),
                suites=suites,
            ):
                raise ValueError(
                    f"request_id={request_id} replay plan does not match stored suite rows"
                )

            conn.execute(
                """
                UPDATE requests
                SET status = ?, workflow_run_id = ?, error_message = NULL, updated_at = ?
                WHERE request_id = ?
                """,
                ("planning", workflow_run_id, timestamp, int(request_id)),
            )
            conn.execute(
                """
                UPDATE suites
                SET status = ?, exit_code = NULL, updated_at = ?
                WHERE request_id = ?
                """,
                ("queued", timestamp, int(request_id)),
            )
    return get_request_state(db_path, request_id=request_id)


def update_suite_status(
    db_path: Path | str,
    *,
    request_id: int,
    suite: str,
    status: str,
    profile: str | None = None,
    artifact_name: str | None = None,
    exit_code: int | None = None,
) -> None:
    if status not in SUITE_STATUSES:
        raise ValueError(f"invalid suite status '{status}'")
    timestamp = _utc_now()
    with closing(_connect_db(db_path)) as conn:
        with conn:
            if profile is not None:
                matching_rows = conn.execute(
                    """
                    SELECT 1
                    FROM suites
                    WHERE request_id = ? AND suite = ? AND profile = ?
                    """,
                    (int(request_id), suite, profile),
                ).fetchall()
                where_sql = "request_id = ? AND suite = ? AND profile = ?"
                where_params: list[Any] = [int(request_id), suite, profile]
            else:
                matching_rows = conn.execute(
                    """
                    SELECT profile
                    FROM suites
                    WHERE request_id = ? AND suite = ?
                    ORDER BY profile
                    """,
                    (int(request_id), suite),
                ).fetchall()
                if len(matching_rows) > 1:
                    raise ValueError(
                        f"request_id={request_id} suite='{suite}' is ambiguous without profile"
                    )
                where_sql = "request_id = ? AND suite = ?"
                where_params = [int(request_id), suite]

            if not matching_rows:
                raise ValueError(
                    f"request_id={request_id} suite='{suite}' was not found"
                )

            fields = ["status = ?", "updated_at = ?"]
            params: list[Any] = [status, timestamp]
            if artifact_name is not None:
                fields.append("artifact_name = ?")
                params.append(artifact_name)
            if exit_code is not None:
                fields.append("exit_code = ?")
                params.append(exit_code)
            params.extend(where_params)
            conn.execute(
                f"UPDATE suites SET {', '.join(fields)} WHERE {where_sql}",
                params,
            )


def format_queue(db_path: Path | str) -> str:
    with closing(_connect_db(db_path)) as conn:
        requests = conn.execute(
            """
            SELECT
                request_id,
                repo,
                pr_number,
                actor,
                command,
                pack,
                base_sha,
                candidate_sha,
                status,
                created_at
            FROM requests
            WHERE status IN (?, ?, ?, ?)
            ORDER BY created_at, request_id
            """,
            tuple(sorted(ACTIVE_REQUEST_STATUSES)),
        ).fetchall()
        if not requests:
            return "No active benchmark requests."

        lines: list[str] = []
        for request in requests:
            lines.append(
                "request_id={request_id} repo={repo} pr=#{pr_number} actor={actor} "
                'command="{command}" pack={pack} status={status} '
                "base_sha={base_sha} candidate_sha={candidate_sha}".format(
                    request_id=request["request_id"],
                    repo=request["repo"],
                    pr_number=request["pr_number"],
                    actor=request["actor"],
                    command=request["command"],
                    pack=request["pack"] or "-",
                    status=request["status"],
                    base_sha=request["base_sha"],
                    candidate_sha=request["candidate_sha"],
                )
            )
            suite_rows = conn.execute(
                """
                SELECT suite, profile, status
                FROM suites
                WHERE request_id = ?
                ORDER BY suite, profile
                """,
                (request["request_id"],),
            ).fetchall()
            for suite_row in suite_rows:
                lines.append(
                    f"{suite_row['suite']}[{suite_row['profile']}]: {suite_row['status']}"
                )
            lines.append("")
        return "\n".join(lines).strip() + "\n"


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Persistent PR benchmark bot state")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB_PATH)
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("init", help="Initialize the bot SQLite database")
    subparsers.add_parser("queue", help="Render outstanding request queue")

    enqueue = subparsers.add_parser("enqueue", help="Insert or dedupe request rows")
    enqueue.add_argument("--repo", required=True)
    enqueue.add_argument("--pr-number", type=int, required=True)
    enqueue.add_argument("--comment-id", type=int, required=True)
    enqueue.add_argument("--actor", required=True)
    enqueue.add_argument("--request-command", required=True)
    enqueue.add_argument("--pack")
    enqueue.add_argument("--base-sha", required=True)
    enqueue.add_argument("--candidate-sha", required=True)
    enqueue.add_argument("--workflow-run-id", type=int)
    enqueue.add_argument("--plan-json", type=Path, required=True)
    enqueue.add_argument(
        "--status",
        default="planning",
        choices=sorted(REQUEST_STATUSES),
    )

    set_request = subparsers.add_parser(
        "set-request-status", help="Update request status"
    )
    set_request.add_argument("--request-id", type=int, required=True)
    set_request.add_argument("--status", choices=sorted(REQUEST_STATUSES), required=True)
    set_request.add_argument("--summary-comment-id", type=int)
    set_request.add_argument("--error-message")

    set_suite = subparsers.add_parser("set-suite-status", help="Update suite status")
    set_suite.add_argument("--request-id", type=int, required=True)
    set_suite.add_argument("--suite", required=True)
    set_suite.add_argument("--profile")
    set_suite.add_argument("--status", choices=sorted(SUITE_STATUSES), required=True)
    set_suite.add_argument("--artifact-name")
    set_suite.add_argument("--exit-code", type=int)

    request_state = subparsers.add_parser(
        "request-state", help="Render one request plus suite rows as JSON"
    )
    request_state.add_argument("--request-id", type=int, required=True)

    replay_request_parser = subparsers.add_parser(
        "replay-request",
        help="Reset an existing request so it can be replayed explicitly",
    )
    replay_request_parser.add_argument("--request-id", type=int, required=True)
    replay_request_parser.add_argument("--workflow-run-id", type=int)
    replay_request_parser.add_argument("--plan-json", type=Path)

    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        if args.command == "init":
            initialize_db(args.db)
            return
        if args.command == "queue":
            print(format_queue(args.db), end="")
            return
        if args.command == "enqueue":
            plan = json.loads(args.plan_json.read_text(encoding="utf-8"))
            suites = list(plan.get("include") or [])
            request_id, created = ensure_request(
                args.db,
                repo=args.repo,
                pr_number=args.pr_number,
                comment_id=args.comment_id,
                actor=args.actor,
                command=args.request_command,
                pack=args.pack,
                base_sha=args.base_sha,
                candidate_sha=args.candidate_sha,
                workflow_run_id=args.workflow_run_id,
                suites=suites,
                status=args.status,
            )
            print(json.dumps({"request_id": request_id, "duplicate": not created}))
            return
        if args.command == "set-request-status":
            update_request_status(
                args.db,
                request_id=args.request_id,
                status=args.status,
                summary_comment_id=args.summary_comment_id,
                error_message=args.error_message,
            )
            return
        if args.command == "set-suite-status":
            update_suite_status(
                args.db,
                request_id=args.request_id,
                suite=args.suite,
                profile=args.profile,
                status=args.status,
                artifact_name=args.artifact_name,
                exit_code=args.exit_code,
            )
            return
        if args.command == "request-state":
            print(
                json.dumps(
                    get_request_state(args.db, request_id=args.request_id),
                    indent=2,
                )
            )
            return
        if args.command == "replay-request":
            suites: list[dict[str, Any]] | None = None
            if args.plan_json is not None:
                plan = json.loads(args.plan_json.read_text(encoding="utf-8"))
                suites = list(plan.get("include") or [])
            print(
                json.dumps(
                    replay_request(
                        args.db,
                        request_id=args.request_id,
                        workflow_run_id=args.workflow_run_id,
                        suites=suites,
                    ),
                    indent=2,
                )
            )
            return
    except (OSError, ValueError, sqlite3.DatabaseError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
