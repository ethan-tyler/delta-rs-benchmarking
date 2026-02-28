from __future__ import annotations

import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import pytest

from delta_bench_longitudinal.revisions import (
    RevisionEntry,
    RevisionManifest,
    select_revisions,
    write_manifest,
    load_manifest,
)


def _run(cmd: list[str], cwd: Path, env: dict[str, str] | None = None) -> None:
    merged = os.environ.copy()
    if env:
        merged.update(env)
    subprocess.run(
        cmd,
        cwd=cwd,
        env=merged,
        check=True,
        capture_output=True,
        text=True,
    )


def _init_repo(path: Path) -> None:
    _run(["git", "init", "-b", "main"], cwd=path)
    _run(["git", "config", "user.name", "tester"], cwd=path)
    _run(["git", "config", "user.email", "tester@example.com"], cwd=path)


def _commit(path: Path, name: str, ts: str) -> str:
    marker = path / "marker.txt"
    with marker.open("a", encoding="utf-8") as fh:
        fh.write(f"{name}\n")
    _run(["git", "add", "marker.txt"], cwd=path)
    _run(
        ["git", "commit", "-m", name],
        cwd=path,
        env={"GIT_AUTHOR_DATE": ts, "GIT_COMMITTER_DATE": ts},
    )
    out = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        cwd=path,
        check=True,
        capture_output=True,
        text=True,
    )
    return out.stdout.strip()


def test_select_release_tags(tmp_path: Path) -> None:
    _init_repo(tmp_path)
    c1 = _commit(tmp_path, "c1", "2026-01-01T10:00:00+00:00")
    _run(["git", "tag", "v0.1.0"], cwd=tmp_path)
    c2 = _commit(tmp_path, "c2", "2026-01-03T10:00:00+00:00")
    _run(["git", "tag", "v0.2.0"], cwd=tmp_path)

    manifest = select_revisions(tmp_path, strategy="release-tags")

    assert [entry.commit for entry in manifest.revisions] == [c1, c2]
    assert [entry.tag for entry in manifest.revisions] == ["v0.1.0", "v0.2.0"]
    assert manifest.strategy == "release-tags"


def test_select_date_window(tmp_path: Path) -> None:
    _init_repo(tmp_path)
    c1 = _commit(tmp_path, "c1", "2026-01-01T09:00:00+00:00")
    c2 = _commit(tmp_path, "c2", "2026-01-02T09:00:00+00:00")
    _commit(tmp_path, "c3", "2026-01-04T09:00:00+00:00")

    manifest = select_revisions(
        tmp_path,
        strategy="date-window",
        start_date="2026-01-01",
        end_date="2026-01-03",
    )

    assert [entry.commit for entry in manifest.revisions] == [c1, c2]
    assert all(entry.source == "date-window" for entry in manifest.revisions)


def test_select_one_commit_per_day(tmp_path: Path) -> None:
    _init_repo(tmp_path)
    _commit(tmp_path, "early-day1", "2026-01-01T08:00:00+00:00")
    c2 = _commit(tmp_path, "late-day1", "2026-01-01T20:00:00+00:00")
    c3 = _commit(tmp_path, "day2", "2026-01-02T12:00:00+00:00")

    manifest = select_revisions(
        tmp_path,
        strategy="one-per-day",
        start_date="2026-01-01",
        end_date="2026-01-02",
    )

    assert [entry.commit for entry in manifest.revisions] == [c2, c3]
    assert all(entry.source == "one-per-day" for entry in manifest.revisions)


def test_select_one_per_day_is_ref_scoped(tmp_path: Path) -> None:
    _init_repo(tmp_path)
    main_commit = _commit(tmp_path, "main-day1", "2026-01-01T08:00:00+00:00")

    _run(["git", "checkout", "-b", "feature"], cwd=tmp_path)
    _commit(tmp_path, "feature-day1", "2026-01-01T20:00:00+00:00")
    _run(["git", "checkout", "main"], cwd=tmp_path)

    manifest = select_revisions(
        tmp_path,
        strategy="one-per-day",
        start_date="2026-01-01",
        end_date="2026-01-01",
        ref="main",
    )

    assert [entry.commit for entry in manifest.revisions] == [main_commit]
    assert manifest.ref == "main"


def test_date_window_uses_first_parent_history(tmp_path: Path) -> None:
    _init_repo(tmp_path)
    _commit(tmp_path, "main-day1", "2026-01-01T08:00:00+00:00")

    _run(["git", "checkout", "-b", "feature"], cwd=tmp_path)
    feature_commit = _commit(tmp_path, "feature-day2", "2026-01-02T08:00:00+00:00")
    _run(["git", "checkout", "main"], cwd=tmp_path)
    _run(
        ["git", "commit", "--allow-empty", "-m", "main-day3"],
        cwd=tmp_path,
        env={
            "GIT_AUTHOR_DATE": "2026-01-03T08:00:00+00:00",
            "GIT_COMMITTER_DATE": "2026-01-03T08:00:00+00:00",
        },
    )
    _run(
        ["git", "merge", "--no-ff", "feature", "-m", "merge feature"],
        cwd=tmp_path,
        env={
            "GIT_AUTHOR_DATE": "2026-01-04T08:00:00+00:00",
            "GIT_COMMITTER_DATE": "2026-01-04T08:00:00+00:00",
        },
    )

    manifest = select_revisions(
        tmp_path,
        strategy="date-window",
        start_date="2026-01-02",
        end_date="2026-01-02",
        ref="main",
    )

    assert feature_commit not in [entry.commit for entry in manifest.revisions]
    assert manifest.revisions == []


def test_manifest_roundtrip(tmp_path: Path) -> None:
    base = RevisionManifest(
        schema_version=1,
        generated_at=datetime(2026, 2, 1, 12, 0, tzinfo=timezone.utc),
        repository=str(tmp_path),
        strategy="release-tags",
        ref=None,
        revisions=[
            RevisionEntry(
                commit="deadbeef",
                commit_timestamp="2026-01-01T00:00:00+00:00",
                source="release-tags",
                tag="v0.1.0",
            )
        ],
    )
    path = tmp_path / "manifest.json"
    write_manifest(base, path)
    loaded = load_manifest(path)
    assert loaded == base
    assert load_manifest(path.with_name("manifest.json")).schema_version == 1


def test_invalid_strategy_and_dates(tmp_path: Path) -> None:
    _init_repo(tmp_path)
    _commit(tmp_path, "c1", "2026-01-01T00:00:00+00:00")

    with pytest.raises(ValueError, match="strategy"):
        select_revisions(tmp_path, strategy="invalid")

    with pytest.raises(ValueError, match="start_date"):
        select_revisions(
            tmp_path,
            strategy="date-window",
            start_date="2026-99-01",
            end_date="2026-01-31",
        )

    with pytest.raises(ValueError, match="end_date"):
        select_revisions(
            tmp_path,
            strategy="date-window",
            start_date="2026-01-01",
            end_date="not-a-date",
        )
