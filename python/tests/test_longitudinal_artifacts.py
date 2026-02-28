from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

from delta_bench_longitudinal.artifacts import (
    ArtifactBuildMetadata,
    artifact_binary_path,
    artifact_metadata_path,
    build_artifact_from_checkout,
    build_revision_artifact,
    load_artifact_metadata,
    should_skip_build,
    write_artifact_metadata,
)


def test_artifact_paths_are_deterministic(tmp_path: Path) -> None:
    revision = "0123456789abcdef"
    assert artifact_binary_path(tmp_path, revision) == (
        tmp_path / revision / "delta-bench-0123456789abcdef"
    )
    assert artifact_metadata_path(tmp_path, revision) == (
        tmp_path / revision / "metadata.json"
    )


def test_artifact_metadata_roundtrip(tmp_path: Path) -> None:
    metadata = ArtifactBuildMetadata(
        revision="deadbeef",
        commit_timestamp="2026-01-01T00:00:00+00:00",
        build_timestamp="2026-02-01T00:00:00+00:00",
        rust_toolchain="stable-x86_64-apple-darwin",
        status="success",
        artifact_path="artifacts/deadbeef/delta-bench-deadbeef",
        error=None,
    )
    path = artifact_metadata_path(tmp_path, metadata.revision)
    write_artifact_metadata(path, metadata)
    assert load_artifact_metadata(path) == metadata


def test_should_skip_build_for_successful_metadata_and_binary(tmp_path: Path) -> None:
    revision = "abc123"
    bin_path = artifact_binary_path(tmp_path, revision)
    bin_path.parent.mkdir(parents=True, exist_ok=True)
    bin_path.write_bytes(b"binary")
    metadata = ArtifactBuildMetadata(
        revision=revision,
        commit_timestamp="2026-01-01T00:00:00+00:00",
        build_timestamp="2026-02-01T00:00:00+00:00",
        rust_toolchain="stable",
        status="success",
        artifact_path=str(bin_path),
        error=None,
    )
    write_artifact_metadata(artifact_metadata_path(tmp_path, revision), metadata)
    assert should_skip_build(tmp_path, revision) is True


def test_build_artifact_from_checkout_captures_failure(tmp_path: Path) -> None:
    checkout = tmp_path / "checkout"
    checkout.mkdir(parents=True, exist_ok=True)
    artifacts_dir = tmp_path / "artifacts"
    metadata = build_artifact_from_checkout(
        checkout_dir=checkout,
        revision="f00dbabe",
        commit_timestamp="2026-01-01T00:00:00+00:00",
        artifacts_dir=artifacts_dir,
        build_command=["bash", "-lc", "true"],
        rust_toolchain="stable",
    )
    assert metadata.status == "failure"
    assert metadata.error is not None
    assert "not found" in metadata.error


def test_build_revision_artifact_persists_setup_failures(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    metadata = build_revision_artifact(
        repository=tmp_path / "missing-repo",
        revision="deadbeef",
        commit_timestamp="2026-01-01T00:00:00+00:00",
        artifacts_dir=artifacts_dir,
        sync_harness=False,
    )
    metadata_file = artifact_metadata_path(artifacts_dir, "deadbeef")
    persisted = load_artifact_metadata(metadata_file)

    assert metadata.status == "failure"
    assert metadata.error is not None
    assert metadata_file.exists()
    assert persisted.status == "failure"


def test_should_skip_build_rejects_untrusted_metadata_path(tmp_path: Path) -> None:
    revision = "abc123"
    expected = artifact_binary_path(tmp_path, revision)
    expected.parent.mkdir(parents=True, exist_ok=True)
    expected.write_bytes(b"expected")

    outside_binary = tmp_path / "outside-delta-bench"
    outside_binary.write_bytes(b"outside")
    metadata = ArtifactBuildMetadata(
        revision=revision,
        commit_timestamp="2026-01-01T00:00:00+00:00",
        build_timestamp="2026-02-01T00:00:00+00:00",
        rust_toolchain="stable",
        status="success",
        artifact_path=str(outside_binary),
        error=None,
    )
    write_artifact_metadata(artifact_metadata_path(tmp_path, revision), metadata)

    assert should_skip_build(tmp_path, revision) is False
