from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from delta_bench_longitudinal.artifacts import artifact_binary_path, artifact_metadata_path
from delta_bench_longitudinal.cli import orchestrate_from_manifest
from delta_bench_longitudinal.matrix_runner import sanitize_label
from delta_bench_longitudinal.revisions import RevisionEntry, RevisionManifest, write_manifest
from delta_bench_longitudinal.store import load_longitudinal_rows


def test_smoke_build_run_report_flow(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest = RevisionManifest(
        schema_version=1,
        generated_at=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        repository=str(tmp_path),
        strategy="release-tags",
        revisions=[
            RevisionEntry(
                commit="rev-smoke-1",
                commit_timestamp="2026-01-01T00:00:00+00:00",
                source="release-tags",
                tag="v0.1.0",
            )
        ],
    )
    write_manifest(manifest, manifest_path)

    artifacts_dir = tmp_path / "artifacts"
    results_dir = tmp_path / "results"
    store_dir = tmp_path / "store"
    reports_dir = tmp_path / "reports"
    state_path = tmp_path / "matrix_state.json"

    def fake_build(**kwargs):  # type: ignore[no-untyped-def]
        revision = kwargs["revision"]
        binary = artifact_binary_path(artifacts_dir, revision)
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
        metadata = {
            "revision": revision,
            "commit_timestamp": kwargs["commit_timestamp"],
            "build_timestamp": "2026-02-01T00:00:00+00:00",
            "rust_toolchain": "stable",
            "status": "success",
            "artifact_path": str(binary),
            "error": None,
        }
        metadata_path = artifact_metadata_path(artifacts_dir, revision)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
        return metadata

    def fake_matrix_executor(artifact, suite, scale, attempt, timeout):  # type: ignore[no-untyped-def]
        label = f"longitudinal-{artifact.revision}"
        out_dir = results_dir / label
        out_dir.mkdir(parents=True, exist_ok=True)
        result = {
            "schema_version": 1,
            "context": {
                "schema_version": 1,
                "label": label,
                "git_sha": artifact.revision,
                "created_at": "2026-02-02T00:00:00+00:00",
                "host": "host",
                "suite": suite,
                "scale": scale,
                "iterations": 1,
                "warmup": 0,
            },
            "cases": [
                {
                    "case": "scan_all",
                    "success": True,
                    "samples": [{"elapsed_ms": 100.0}],
                    "failure": None,
                }
            ],
        }
        (out_dir / f"{suite}.json").write_text(json.dumps(result), encoding="utf-8")
        return 0, ""

    summary = orchestrate_from_manifest(
        manifest_path=manifest_path,
        artifacts_dir=artifacts_dir,
        results_dir=results_dir,
        state_path=state_path,
        store_dir=store_dir,
        markdown_path=reports_dir / "summary.md",
        html_path=reports_dir / "report.html",
        suites=["read_scan"],
        scales=["sf1"],
        timeout_seconds=5,
        max_retries=0,
        max_parallel=1,
        max_load_per_cpu=None,
        load_check_interval_seconds=0.01,
        baseline_window=2,
        regression_threshold=0.05,
        significance_method="none",
        significance_alpha=0.05,
        build_fn=fake_build,
        matrix_executor=fake_matrix_executor,
    )

    assert summary["built"] == 1
    assert summary["ingested_rows"] == 1
    assert (reports_dir / "summary.md").exists()
    assert (reports_dir / "report.html").exists()


def test_orchestrate_uses_sanitized_label_prefix_for_ingest(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest = RevisionManifest(
        schema_version=1,
        generated_at=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        repository=str(tmp_path),
        strategy="release-tags",
        ref=None,
        revisions=[
            RevisionEntry(
                commit="rev-smoke-2",
                commit_timestamp="2026-01-02T00:00:00+00:00",
                source="release-tags",
                tag="v0.2.0",
            )
        ],
    )
    write_manifest(manifest, manifest_path)

    artifacts_dir = tmp_path / "artifacts"
    results_dir = tmp_path / "results"
    store_dir = tmp_path / "store"
    reports_dir = tmp_path / "reports"
    state_path = tmp_path / "matrix_state.json"
    label_prefix = "nightly/bench"

    def fake_build(**kwargs):  # type: ignore[no-untyped-def]
        revision = kwargs["revision"]
        binary = artifact_binary_path(artifacts_dir, revision)
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
        metadata = {
            "revision": revision,
            "commit_timestamp": kwargs["commit_timestamp"],
            "build_timestamp": "2026-02-01T00:00:00+00:00",
            "rust_toolchain": "stable",
            "status": "success",
            "artifact_path": str(binary),
            "error": None,
        }
        metadata_path = artifact_metadata_path(artifacts_dir, revision)
        metadata_path.parent.mkdir(parents=True, exist_ok=True)
        metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
        return metadata

    def fake_matrix_executor(artifact, suite, scale, attempt, timeout):  # type: ignore[no-untyped-def]
        label = sanitize_label(f"{label_prefix}-{artifact.revision}")
        out_dir = results_dir / label
        out_dir.mkdir(parents=True, exist_ok=True)
        result = {
            "schema_version": 1,
            "context": {
                "schema_version": 1,
                "label": label,
                "git_sha": artifact.revision,
                "created_at": "2026-02-02T00:00:00+00:00",
                "host": "host",
                "suite": suite,
                "scale": scale,
                "iterations": 1,
                "warmup": 0,
            },
            "cases": [
                {
                    "case": "scan_all",
                    "success": True,
                    "samples": [{"elapsed_ms": 100.0}],
                    "failure": None,
                }
            ],
        }
        (out_dir / f"{suite}.json").write_text(json.dumps(result), encoding="utf-8")
        return 0, ""

    summary = orchestrate_from_manifest(
        manifest_path=manifest_path,
        artifacts_dir=artifacts_dir,
        results_dir=results_dir,
        state_path=state_path,
        store_dir=store_dir,
        markdown_path=reports_dir / "summary.md",
        html_path=reports_dir / "report.html",
        suites=["read_scan"],
        scales=["sf1"],
        timeout_seconds=5,
        max_retries=0,
        max_parallel=1,
        max_load_per_cpu=None,
        load_check_interval_seconds=0.01,
        baseline_window=2,
        regression_threshold=0.05,
        significance_method="none",
        significance_alpha=0.05,
        label_prefix=label_prefix,
        build_fn=fake_build,
        matrix_executor=fake_matrix_executor,
    )

    assert summary["ingested_rows"] == 1


def test_orchestrate_ingests_distinct_scales(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest = RevisionManifest(
        schema_version=1,
        generated_at=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        repository=str(tmp_path),
        strategy="release-tags",
        revisions=[
            RevisionEntry(
                commit="rev-smoke-2",
                commit_timestamp="2026-01-01T00:00:00+00:00",
                source="release-tags",
                tag="v0.1.0",
            )
        ],
    )
    write_manifest(manifest, manifest_path)

    artifacts_dir = tmp_path / "artifacts"
    results_dir = tmp_path / "results"
    store_dir = tmp_path / "store"
    reports_dir = tmp_path / "reports"
    state_path = tmp_path / "matrix_state.json"

    def fake_build(**kwargs):  # type: ignore[no-untyped-def]
        revision = kwargs["revision"]
        binary = artifact_binary_path(artifacts_dir, revision)
        binary.parent.mkdir(parents=True, exist_ok=True)
        binary.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
        return {
            "revision": revision,
            "commit_timestamp": kwargs["commit_timestamp"],
            "build_timestamp": "2026-02-01T00:00:00+00:00",
            "rust_toolchain": "stable",
            "status": "success",
            "artifact_path": str(binary),
            "error": None,
        }

    def fake_matrix_executor(artifact, suite, scale, attempt, timeout):  # type: ignore[no-untyped-def]
        label = f"longitudinal-{artifact.revision}-{scale}"
        out_dir = results_dir / label
        out_dir.mkdir(parents=True, exist_ok=True)
        result = {
            "schema_version": 1,
            "context": {
                "schema_version": 1,
                "label": label,
                "git_sha": artifact.revision,
                "created_at": "2026-02-02T00:00:00+00:00",
                "host": "host",
                "suite": suite,
                "scale": scale,
                "iterations": 1,
                "warmup": 0,
            },
            "cases": [
                {
                    "case": "scan_all",
                    "success": True,
                    "samples": [{"elapsed_ms": 100.0}],
                    "failure": None,
                }
            ],
        }
        (out_dir / f"{suite}.json").write_text(json.dumps(result), encoding="utf-8")
        return 0, ""

    summary = orchestrate_from_manifest(
        manifest_path=manifest_path,
        artifacts_dir=artifacts_dir,
        results_dir=results_dir,
        state_path=state_path,
        store_dir=store_dir,
        markdown_path=reports_dir / "summary.md",
        html_path=reports_dir / "report.html",
        suites=["read_scan"],
        scales=["sf1", "sf10"],
        timeout_seconds=5,
        max_retries=0,
        max_parallel=1,
        max_load_per_cpu=None,
        load_check_interval_seconds=0.01,
        baseline_window=2,
        regression_threshold=0.05,
        significance_method="none",
        significance_alpha=0.05,
        build_fn=fake_build,
        matrix_executor=fake_matrix_executor,
    )

    rows = load_longitudinal_rows(store_dir)
    assert summary["ingested_rows"] == 2
    assert {row["scale"] for row in rows} == {"sf1", "sf10"}


def test_orchestrate_rejects_untrusted_artifact_paths(tmp_path: Path) -> None:
    manifest_path = tmp_path / "manifest.json"
    manifest = RevisionManifest(
        schema_version=1,
        generated_at=datetime(2026, 2, 1, 0, 0, tzinfo=timezone.utc),
        repository=str(tmp_path),
        strategy="release-tags",
        revisions=[
            RevisionEntry(
                commit="rev-untrusted",
                commit_timestamp="2026-01-01T00:00:00+00:00",
                source="release-tags",
                tag="v0.1.0",
            )
        ],
    )
    write_manifest(manifest, manifest_path)

    artifacts_dir = tmp_path / "artifacts"
    results_dir = tmp_path / "results"
    store_dir = tmp_path / "store"
    reports_dir = tmp_path / "reports"
    state_path = tmp_path / "matrix_state.json"
    outside_binary = tmp_path / "outside-delta-bench"
    outside_binary.write_text("#!/usr/bin/env bash\n", encoding="utf-8")
    outside_binary.chmod(0o755)

    calls = {"count": 0}

    def fake_build(**kwargs):  # type: ignore[no-untyped-def]
        return {
            "revision": kwargs["revision"],
            "commit_timestamp": kwargs["commit_timestamp"],
            "build_timestamp": "2026-02-01T00:00:00+00:00",
            "rust_toolchain": "stable",
            "status": "success",
            "artifact_path": str(outside_binary),
            "error": None,
        }

    def fake_matrix_executor(*_args):  # type: ignore[no-untyped-def]
        calls["count"] += 1
        return 0, ""

    summary = orchestrate_from_manifest(
        manifest_path=manifest_path,
        artifacts_dir=artifacts_dir,
        results_dir=results_dir,
        state_path=state_path,
        store_dir=store_dir,
        markdown_path=reports_dir / "summary.md",
        html_path=reports_dir / "report.html",
        suites=["read_scan"],
        scales=["sf1"],
        timeout_seconds=5,
        max_retries=0,
        max_parallel=1,
        max_load_per_cpu=None,
        load_check_interval_seconds=0.01,
        baseline_window=2,
        regression_threshold=0.05,
        significance_method="none",
        significance_alpha=0.05,
        build_fn=fake_build,
        matrix_executor=fake_matrix_executor,
    )

    assert summary["built"] == 0
    assert summary["ingested_rows"] == 0
    assert calls["count"] == 0
