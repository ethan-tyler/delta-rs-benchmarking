from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from delta_bench_compare.bot_state import (
    ensure_request,
    format_queue,
    get_request_state,
    initialize_db,
    list_request_suites,
    replay_request,
    update_request_status,
    update_suite_status,
)


def _plan_suites() -> list[dict[str, object]]:
    return [
        {
            "suite": "scan",
            "profile": "pr-macro-a",
            "timeout_minutes": 90,
            "artifact_name": "benchmark-scan-pr-macro-a",
        },
        {
            "suite": "scan",
            "profile": "pr-macro-b",
            "timeout_minutes": 90,
            "artifact_name": "benchmark-scan-pr-macro-b",
        },
        {
            "suite": "write_perf",
            "profile": "pr-write-perf",
            "timeout_minutes": 120,
            "artifact_name": "benchmark-write-perf-pr-write-perf",
        },
    ]


def test_ensure_request_is_idempotent_for_replayed_comment_id(tmp_path: Path) -> None:
    db_path = tmp_path / "pr-bot.sqlite3"
    initialize_db(db_path)

    first_request_id, first_created = ensure_request(
        db_path,
        repo="delta-io/delta-rs",
        pr_number=123,
        comment_id=456,
        actor="maintainer",
        command="run benchmark decision full",
        pack="pr-full-decision",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
        workflow_run_id=111,
        suites=_plan_suites(),
        status="planning",
    )
    second_request_id, second_created = ensure_request(
        db_path,
        repo="delta-io/delta-rs",
        pr_number=123,
        comment_id=456,
        actor="maintainer",
        command="run benchmark decision full",
        pack="pr-full-decision",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
        workflow_run_id=111,
        suites=_plan_suites(),
        status="planning",
    )

    assert first_created is True
    assert second_created is False
    assert second_request_id == first_request_id


def test_ensure_request_rejects_comment_replay_with_conflicting_payload(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pr-bot.sqlite3"
    initialize_db(db_path)
    ensure_request(
        db_path,
        repo="delta-io/delta-rs",
        pr_number=123,
        comment_id=456,
        actor="maintainer",
        command="run benchmark decision full",
        pack="pr-full-decision",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
        workflow_run_id=111,
        suites=_plan_suites(),
        status="planning",
    )

    with pytest.raises(ValueError, match="comment_id=456"):
        ensure_request(
            db_path,
            repo="delta-io/delta-rs",
            pr_number=123,
            comment_id=456,
            actor="maintainer",
            command="run benchmark decision full",
            pack="pr-full-decision",
            base_sha="a" * 40,
            candidate_sha="c" * 40,
            workflow_run_id=111,
            suites=_plan_suites(),
            status="planning",
        )


def test_suite_rows_are_keyed_by_suite_and_profile(tmp_path: Path) -> None:
    db_path = tmp_path / "pr-bot.sqlite3"
    initialize_db(db_path)
    request_id, _ = ensure_request(
        db_path,
        repo="delta-io/delta-rs",
        pr_number=123,
        comment_id=456,
        actor="maintainer",
        command="run benchmark decision full",
        pack="pr-full-decision",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
        workflow_run_id=111,
        suites=_plan_suites(),
        status="planning",
    )

    update_suite_status(
        db_path,
        request_id=request_id,
        suite="scan",
        profile="pr-macro-a",
        status="running",
    )
    suite_rows = list_request_suites(db_path, request_id=request_id)

    assert [(row["suite"], row["profile"], row["status"]) for row in suite_rows] == [
        ("scan", "pr-macro-a", "running"),
        ("scan", "pr-macro-b", "queued"),
        ("write_perf", "pr-write-perf", "queued"),
    ]


def test_suite_status_update_requires_profile_when_suite_is_ambiguous(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pr-bot.sqlite3"
    initialize_db(db_path)
    request_id, _ = ensure_request(
        db_path,
        repo="delta-io/delta-rs",
        pr_number=123,
        comment_id=456,
        actor="maintainer",
        command="run benchmark decision full",
        pack="pr-full-decision",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
        workflow_run_id=111,
        suites=_plan_suites(),
        status="planning",
    )

    with pytest.raises(ValueError, match="profile"):
        update_suite_status(
            db_path,
            request_id=request_id,
            suite="scan",
            status="running",
        )


def test_queue_format_reports_active_request_with_per_profile_suite_status(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pr-bot.sqlite3"
    initialize_db(db_path)
    request_id, _ = ensure_request(
        db_path,
        repo="delta-io/delta-rs",
        pr_number=123,
        comment_id=456,
        actor="maintainer",
        command="run benchmark decision full",
        pack="pr-full-decision",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
        workflow_run_id=111,
        suites=_plan_suites(),
        status="planning",
    )
    update_request_status(db_path, request_id=request_id, status="running")
    update_suite_status(
        db_path,
        request_id=request_id,
        suite="scan",
        profile="pr-macro-a",
        status="running",
    )
    update_suite_status(
        db_path,
        request_id=request_id,
        suite="scan",
        profile="pr-macro-b",
        status="failed",
        artifact_name="benchmark-scan-pr-macro-b",
        exit_code=124,
    )

    queue_text = format_queue(db_path)

    assert "command=\"run benchmark decision full\"" in queue_text
    assert "pack=pr-full-decision" in queue_text
    assert "status=running" in queue_text
    assert "scan[pr-macro-a]: running" in queue_text
    assert "scan[pr-macro-b]: failed" in queue_text
    assert "write_perf[pr-write-perf]: queued" in queue_text


def test_replay_request_resets_request_and_suite_rows_for_explicit_rerun(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pr-bot.sqlite3"
    initialize_db(db_path)
    request_id, _ = ensure_request(
        db_path,
        repo="delta-io/delta-rs",
        pr_number=123,
        comment_id=456,
        actor="maintainer",
        command="run benchmark decision full",
        pack="pr-full-decision",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
        workflow_run_id=111,
        suites=_plan_suites(),
        status="failed",
    )
    update_suite_status(
        db_path,
        request_id=request_id,
        suite="scan",
        profile="pr-macro-a",
        status="failed",
        exit_code=124,
    )
    update_suite_status(
        db_path,
        request_id=request_id,
        suite="scan",
        profile="pr-macro-b",
        status="completed",
        exit_code=0,
    )
    update_request_status(
        db_path,
        request_id=request_id,
        status="failed",
        summary_comment_id=999,
        error_message="old failure",
    )

    replay_request(
        db_path,
        request_id=request_id,
        workflow_run_id=222,
        suites=_plan_suites(),
    )

    request_state = get_request_state(db_path, request_id=request_id)
    assert request_state["status"] == "planning"
    assert request_state["workflow_run_id"] == 222
    assert request_state["summary_comment_id"] == 999
    assert request_state["error_message"] is None
    assert [
        (row["suite"], row["profile"], row["status"], row["exit_code"])
        for row in request_state["suites"]
    ] == [
        ("scan", "pr-macro-a", "queued", None),
        ("scan", "pr-macro-b", "queued", None),
        ("write_perf", "pr-write-perf", "queued", None),
    ]


def test_replay_request_rejects_plan_drift_against_existing_suite_rows(
    tmp_path: Path,
) -> None:
    db_path = tmp_path / "pr-bot.sqlite3"
    initialize_db(db_path)
    request_id, _ = ensure_request(
        db_path,
        repo="delta-io/delta-rs",
        pr_number=123,
        comment_id=456,
        actor="maintainer",
        command="run benchmark decision full",
        pack="pr-full-decision",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
        workflow_run_id=111,
        suites=_plan_suites(),
        status="failed",
    )

    drifted_plan = _plan_suites()
    drifted_plan[0] = {
        **drifted_plan[0],
        "artifact_name": "benchmark-scan-pr-macro-a-v2",
    }

    with pytest.raises(ValueError, match="request_id=.*replay plan"):
        replay_request(
            db_path,
            request_id=request_id,
            workflow_run_id=222,
            suites=drifted_plan,
        )


def test_schema_migration_keeps_request_and_profile_rows_queryable(tmp_path: Path) -> None:
    db_path = tmp_path / "pr-bot.sqlite3"
    with sqlite3.connect(db_path) as conn:
        conn.executescript(
            """
            CREATE TABLE requests (
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
                PRIMARY KEY (request_id, suite),
                FOREIGN KEY (request_id) REFERENCES requests(request_id) ON DELETE CASCADE
            );
            """
        )
        conn.commit()

    initialize_db(db_path)
    request_id, created = ensure_request(
        db_path,
        repo="delta-io/delta-rs",
        pr_number=123,
        comment_id=456,
        actor="maintainer",
        command="run benchmark decision full",
        pack="pr-full-decision",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
        workflow_run_id=111,
        suites=[
            {
                "suite": "scan",
                "profile": "pr-macro",
                "timeout_minutes": 90,
                "artifact_name": "benchmark-scan-pr-macro",
            }
        ],
        status="planning",
    )

    assert created is True
    assert request_id > 0
    assert "scan[pr-macro]: queued" in format_queue(db_path)

    with sqlite3.connect(db_path) as conn:
        suite_rows = conn.execute(
            "SELECT suite, profile, status FROM suites WHERE request_id = ? ORDER BY suite, profile",
            (request_id,),
        ).fetchall()

    assert suite_rows == [("scan", "pr-macro", "queued")]
