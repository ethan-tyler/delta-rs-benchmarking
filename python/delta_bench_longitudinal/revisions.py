from __future__ import annotations

import json
import re
import subprocess
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Iterable


DEFAULT_RELEASE_TAG_PATTERN = r"^v\d+\.\d+\.\d+([.-].+)?$"


@dataclass(frozen=True)
class RevisionEntry:
    commit: str
    commit_timestamp: str
    source: str
    tag: str | None = None


@dataclass(frozen=True)
class RevisionManifest:
    schema_version: int
    generated_at: datetime
    repository: str
    strategy: str
    revisions: list[RevisionEntry]
    ref: str | None = None


def select_revisions(
    repository: Path | str,
    *,
    strategy: str,
    start_date: str | None = None,
    end_date: str | None = None,
    release_tag_pattern: str = DEFAULT_RELEASE_TAG_PATTERN,
    ref: str = "HEAD",
) -> RevisionManifest:
    repo = Path(repository).resolve()
    if strategy not in {"release-tags", "date-window", "one-per-day"}:
        raise ValueError(
            "strategy must be one of: release-tags, date-window, one-per-day"
        )

    if strategy == "release-tags":
        revisions = list(
            _select_release_tags(repo=repo, release_tag_pattern=release_tag_pattern)
        )
        selected_ref: str | None = None
    else:
        start = _parse_date(start_date, "start_date")
        end = _parse_date(end_date, "end_date")
        if start > end:
            raise ValueError("start_date must be <= end_date")
        if strategy == "date-window":
            revisions = list(_select_date_window(repo=repo, start=start, end=end, ref=ref))
        else:
            revisions = list(_select_one_per_day(repo=repo, start=start, end=end, ref=ref))
        selected_ref = ref

    return RevisionManifest(
        schema_version=1,
        generated_at=datetime.now(timezone.utc),
        repository=str(repo),
        strategy=strategy,
        revisions=revisions,
        ref=selected_ref,
    )


def write_manifest(manifest: RevisionManifest, path: Path | str) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    payload = asdict(manifest)
    payload["generated_at"] = manifest.generated_at.isoformat()
    destination.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def load_manifest(path: Path | str) -> RevisionManifest:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    revisions = [RevisionEntry(**entry) for entry in payload.get("revisions", [])]
    generated_at = datetime.fromisoformat(payload["generated_at"])
    return RevisionManifest(
        schema_version=int(payload["schema_version"]),
        generated_at=generated_at,
        repository=str(payload["repository"]),
        strategy=str(payload["strategy"]),
        revisions=revisions,
        ref=payload.get("ref"),
    )


def _select_release_tags(
    *,
    repo: Path,
    release_tag_pattern: str,
) -> Iterable[RevisionEntry]:
    pattern = re.compile(release_tag_pattern)
    tags = _git(repo, ["tag", "--list", "--sort=creatordate"]).splitlines()
    out: list[RevisionEntry] = []
    for tag in tags:
        if not tag or not pattern.match(tag):
            continue
        commit = _git(repo, ["rev-list", "-n", "1", tag]).strip()
        commit_ts = _git(repo, ["show", "-s", "--date=iso-strict", "--format=%cI", commit]).strip()
        out.append(
            RevisionEntry(
                commit=commit,
                commit_timestamp=commit_ts,
                source="release-tags",
                tag=tag,
            )
        )
    out.sort(key=lambda entry: (entry.commit_timestamp, entry.tag or ""))
    return out


def _select_date_window(*, repo: Path, start: date, end: date, ref: str) -> Iterable[RevisionEntry]:
    rows = _git_commit_rows(repo=repo, start=start, end=end, ref=ref)
    return [
        RevisionEntry(
            commit=commit,
            commit_timestamp=commit_ts,
            source="date-window",
            tag=None,
        )
        for commit, commit_ts in rows
    ]


def _select_one_per_day(*, repo: Path, start: date, end: date, ref: str) -> Iterable[RevisionEntry]:
    rows = _git_commit_rows(repo=repo, start=start, end=end, ref=ref)
    latest_per_day: dict[str, tuple[str, str]] = {}
    for commit, commit_ts in rows:
        day = commit_ts[:10]
        latest_per_day[day] = (commit, commit_ts)
    selected: list[RevisionEntry] = []
    for day in sorted(latest_per_day):
        commit, commit_ts = latest_per_day[day]
        selected.append(
            RevisionEntry(
                commit=commit,
                commit_timestamp=commit_ts,
                source="one-per-day",
                tag=None,
            )
        )
    return selected


def _git_commit_rows(*, repo: Path, start: date, end: date, ref: str) -> list[tuple[str, str]]:
    start_ts = f"{start.isoformat()}T00:00:00+00:00"
    end_ts = f"{end.isoformat()}T23:59:59+00:00"
    raw = _git(
        repo,
        [
            "log",
            "--first-parent",
            ref,
            "--reverse",
            "--date=iso-strict",
            "--pretty=format:%H|%cI",
            "--since",
            start_ts,
            "--until",
            end_ts,
        ],
    )
    rows: list[tuple[str, str]] = []
    for line in raw.splitlines():
        if not line:
            continue
        commit, commit_ts = line.split("|", 1)
        rows.append((commit, commit_ts))
    return rows


def _git(repo: Path, args: list[str]) -> str:
    proc = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        stderr = proc.stderr.strip() or "unknown git error"
        raise ValueError(f"git command failed: git {' '.join(args)}: {stderr}")
    return proc.stdout.strip()


def _parse_date(value: str | None, field: str) -> date:
    if value is None:
        raise ValueError(f"{field} is required for this strategy")
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field} must be YYYY-MM-DD") from exc
