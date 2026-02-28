from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence


@dataclass(frozen=True)
class ArtifactBuildMetadata:
    revision: str
    commit_timestamp: str
    build_timestamp: str
    rust_toolchain: str
    status: str
    artifact_path: str | None
    error: str | None


def artifact_binary_path(artifacts_dir: Path | str, revision: str) -> Path:
    safe_revision = _sanitize_revision(revision)
    return Path(artifacts_dir) / safe_revision / f"delta-bench-{safe_revision}"


def artifact_metadata_path(artifacts_dir: Path | str, revision: str) -> Path:
    safe_revision = _sanitize_revision(revision)
    return Path(artifacts_dir) / safe_revision / "metadata.json"


def write_artifact_metadata(path: Path | str, metadata: ArtifactBuildMetadata) -> None:
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(
        json.dumps(asdict(metadata), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def load_artifact_metadata(path: Path | str) -> ArtifactBuildMetadata:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return ArtifactBuildMetadata(**payload)


def should_skip_build(artifacts_dir: Path | str, revision: str) -> bool:
    metadata_file = artifact_metadata_path(artifacts_dir, revision)
    if not metadata_file.exists():
        return False
    metadata = load_artifact_metadata(metadata_file)
    if metadata.status != "success" or not metadata.artifact_path:
        return False
    return is_trusted_artifact_path(
        artifacts_dir=artifacts_dir,
        revision=revision,
        artifact_path=metadata.artifact_path,
    )


def is_trusted_artifact_path(
    *,
    artifacts_dir: Path | str,
    revision: str,
    artifact_path: str | Path,
) -> bool:
    expected = artifact_binary_path(artifacts_dir, revision).resolve()
    candidate = Path(artifact_path)
    try:
        candidate_resolved = candidate.resolve(strict=True)
    except OSError:
        return False
    if candidate_resolved != expected:
        return False
    if candidate.is_symlink():
        return False
    return candidate.is_file()


def build_artifact_from_checkout(
    *,
    checkout_dir: Path | str,
    revision: str,
    commit_timestamp: str,
    artifacts_dir: Path | str,
    build_command: Sequence[str] | None = None,
    rust_toolchain: str = "unknown",
) -> ArtifactBuildMetadata:
    checkout = Path(checkout_dir)
    metadata_file = artifact_metadata_path(artifacts_dir, revision)
    build_timestamp = datetime.now(timezone.utc).isoformat()
    command = list(build_command or ["cargo", "build", "-p", "delta-bench", "--release"])

    proc = subprocess.run(
        command,
        cwd=checkout,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        metadata = ArtifactBuildMetadata(
            revision=revision,
            commit_timestamp=commit_timestamp,
            build_timestamp=build_timestamp,
            rust_toolchain=rust_toolchain,
            status="failure",
            artifact_path=None,
            error=_truncate_err(proc.stderr or proc.stdout),
        )
        write_artifact_metadata(metadata_file, metadata)
        return metadata

    built_binary = checkout / "target" / "release" / "delta-bench"
    if not built_binary.exists():
        metadata = ArtifactBuildMetadata(
            revision=revision,
            commit_timestamp=commit_timestamp,
            build_timestamp=build_timestamp,
            rust_toolchain=rust_toolchain,
            status="failure",
            artifact_path=None,
            error=f"built binary not found: {built_binary}",
        )
        write_artifact_metadata(metadata_file, metadata)
        return metadata

    output_binary = artifact_binary_path(artifacts_dir, revision)
    output_binary.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(built_binary, output_binary)
    output_binary.chmod(output_binary.stat().st_mode | 0o111)

    metadata = ArtifactBuildMetadata(
        revision=revision,
        commit_timestamp=commit_timestamp,
        build_timestamp=build_timestamp,
        rust_toolchain=rust_toolchain,
        status="success",
        artifact_path=str(output_binary),
        error=None,
    )
    write_artifact_metadata(metadata_file, metadata)
    return metadata


def build_revision_artifact(
    *,
    repository: Path | str,
    revision: str,
    commit_timestamp: str,
    artifacts_dir: Path | str,
    build_command: Sequence[str] | None = None,
    sync_harness: bool = True,
) -> ArtifactBuildMetadata:
    if should_skip_build(artifacts_dir, revision):
        return load_artifact_metadata(artifact_metadata_path(artifacts_dir, revision))

    repo = Path(repository).resolve()
    metadata_file = artifact_metadata_path(artifacts_dir, revision)
    toolchain = "unknown"
    build_result: ArtifactBuildMetadata | None = None
    build_error: str | None = None
    worktree_added = False
    checkout: Path | None = None

    with tempfile.TemporaryDirectory(prefix="delta-bench-build-") as td:
        checkout = Path(td) / "checkout"
        try:
            _run(
                [
                    "git",
                    "-C",
                    str(repo),
                    "worktree",
                    "add",
                    "--detach",
                    str(checkout),
                    revision,
                ]
            )
            worktree_added = True
            if sync_harness:
                _sync_harness_to_checkout(checkout)
            toolchain = detect_rust_toolchain(checkout)
            build_result = build_artifact_from_checkout(
                checkout_dir=checkout,
                revision=revision,
                commit_timestamp=commit_timestamp,
                artifacts_dir=artifacts_dir,
                build_command=build_command,
                rust_toolchain=toolchain,
            )
        except Exception as exc:  # noqa: BLE001 - persist setup failures into metadata
            build_error = str(exc)
        finally:
            if worktree_added:
                try:
                    _run(
                        [
                            "git",
                            "-C",
                            str(repo),
                            "worktree",
                            "remove",
                            "--force",
                            str(checkout),
                        ]
                    )
                except Exception as cleanup_exc:  # noqa: BLE001 - capture cleanup errors too
                    cleanup_message = f"worktree cleanup failed: {cleanup_exc}"
                    build_error = (
                        cleanup_message
                        if build_error is None
                        else f"{build_error}; {cleanup_message}"
                    )

    if build_error is not None:
        metadata = ArtifactBuildMetadata(
            revision=revision,
            commit_timestamp=commit_timestamp,
            build_timestamp=datetime.now(timezone.utc).isoformat(),
            rust_toolchain=toolchain,
            status="failure",
            artifact_path=None,
            error=_truncate_err(build_error),
        )
        write_artifact_metadata(metadata_file, metadata)
        return metadata

    if build_result is None:
        metadata = ArtifactBuildMetadata(
            revision=revision,
            commit_timestamp=commit_timestamp,
            build_timestamp=datetime.now(timezone.utc).isoformat(),
            rust_toolchain=toolchain,
            status="failure",
            artifact_path=None,
            error="artifact build completed with no metadata result",
        )
        write_artifact_metadata(metadata_file, metadata)
        return metadata

    return build_result


def detect_rust_toolchain(checkout_dir: Path | str) -> str:
    proc = subprocess.run(
        ["rustup", "show", "active-toolchain"],
        cwd=Path(checkout_dir),
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        return "unknown"
    out = proc.stdout.strip()
    return out if out else "unknown"


def _run(command: Sequence[str]) -> None:
    proc = subprocess.run(command, check=False, capture_output=True, text=True)
    if proc.returncode != 0:
        detail = _truncate_err(proc.stderr or proc.stdout)
        raise RuntimeError(f"command failed: {' '.join(command)}: {detail}")


def _sync_harness_to_checkout(checkout_dir: Path) -> None:
    repo_root = Path(__file__).resolve().parents[2]
    sync_script = repo_root / "scripts" / "sync_harness_to_delta_rs.sh"
    if not sync_script.exists():
        return
    env = os.environ.copy()
    env["DELTA_RS_DIR"] = str(checkout_dir)
    proc = subprocess.run(
        [str(sync_script)],
        cwd=repo_root,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    if proc.returncode != 0:
        detail = _truncate_err(proc.stderr or proc.stdout)
        raise RuntimeError(f"harness sync failed: {detail}")


def _sanitize_revision(revision: str) -> str:
    safe = "".join(ch if ch.isalnum() else "_" for ch in revision).strip("_")
    if not safe:
        raise ValueError("revision must contain at least one alphanumeric character")
    return safe


def _truncate_err(value: str, limit: int = 4000) -> str:
    trimmed = value.strip()
    if len(trimmed) <= limit:
        return trimmed
    return trimmed[-limit:]
