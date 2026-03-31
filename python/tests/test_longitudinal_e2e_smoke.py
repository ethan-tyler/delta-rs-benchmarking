from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable

from delta_bench_longitudinal.artifacts import (
    artifact_binary_path,
    artifact_metadata_path,
)
from delta_bench_longitudinal.cli import orchestrate_from_manifest
from delta_bench_longitudinal.matrix_runner import matrix_result_label
from delta_bench_longitudinal.revisions import (
    RevisionEntry,
    RevisionManifest,
    write_manifest,
)
from delta_bench_longitudinal.store import load_longitudinal_rows, store_db_path


def _make_fake_build(
    artifacts_dir: Path,
    *,
    write_metadata: bool = True,
):  # type: ignore[no-untyped-def]
    """Factory for a fake build function that can optionally persist metadata."""

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
        if write_metadata:
            metadata_path = artifact_metadata_path(artifacts_dir, revision)
            metadata_path.parent.mkdir(parents=True, exist_ok=True)
            metadata_path.write_text(json.dumps(metadata), encoding="utf-8")
        return metadata

    return fake_build


def _make_fake_executor(
    results_dir: Path,
    label_fn: Callable[..., str] | None = None,
    lane: str = "macro",
):  # type: ignore[no-untyped-def]
    """Factory for a fake matrix executor that writes result JSON files.

    Args:
        results_dir: Directory where results will be written.
        label_fn: Optional callable(artifact, scale, lane) -> str for custom labels.
            Defaults to the shared lane-scoped longitudinal label contract.
    """

    def fake_matrix_executor(artifact, suite, scale, attempt, timeout):  # type: ignore[no-untyped-def]
        if label_fn is not None:
            label = label_fn(artifact, scale, lane)
        else:
            label = matrix_result_label("longitudinal", artifact.revision, scale, lane)
        out_dir = results_dir / label
        out_dir.mkdir(parents=True, exist_ok=True)
        result = {
            "schema_version": 5,
            "context": {
                "schema_version": 5,
                "label": label,
                "git_sha": artifact.revision,
                "created_at": "2026-02-02T00:00:00+00:00",
                "host": "host",
                "suite": suite,
                "runner": "rust",
                "benchmark_mode": "perf",
                "scale": scale,
                "timing_phase": "execute",
                "dataset_id": "tiny_smoke",
                "dataset_fingerprint": "sha256:fixture",
                "storage_backend": "local",
                "iterations": 1,
                "warmup": 0,
                "lane": lane,
                "measurement_kind": "phase_breakdown",
                "validation_level": "operational",
                "run_id": f"{artifact.revision}-{scale}-{lane}",
                "harness_revision": "h1",
                "fixture_recipe_hash": "sha256:recipe",
                "fidelity_fingerprint": "sha256:fidelity",
            },
            "cases": [
                {
                    "case": "scan_all",
                    "classification": "supported",
                    "success": True,
                    "validation_passed": True,
                    "perf_status": "trusted",
                    "samples": [{"elapsed_ms": 100.0}],
                    "run_summary": {
                        "sample_count": 1,
                        "invalid_sample_count": 0,
                        "min_ms": 100.0,
                        "max_ms": 100.0,
                        "mean_ms": 100.0,
                        "median_ms": 100.0,
                    },
                    "compatibility_key": f"sha256:{suite}-{scale}-{lane}-compat",
                    "case_definition_hash": f"sha256:{suite}-scan-all",
                    "failure": None,
                }
            ],
        }
        (out_dir / f"{suite}.json").write_text(json.dumps(result), encoding="utf-8")
        return 0, ""

    return fake_matrix_executor


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
        build_fn=_make_fake_build(artifacts_dir),
        matrix_executor=_make_fake_executor(results_dir),
    )

    assert summary["built"] == 1
    assert summary["ingested_rows"] == 1
    assert store_db_path(store_dir).exists()
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
        build_fn=_make_fake_build(artifacts_dir),
        matrix_executor=_make_fake_executor(
            results_dir,
            label_fn=lambda artifact, scale, lane: matrix_result_label(
                label_prefix, artifact.revision, scale, lane
            ),
        ),
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
        build_fn=_make_fake_build(artifacts_dir, write_metadata=False),
        matrix_executor=_make_fake_executor(
            results_dir,
            label_fn=lambda artifact, scale, lane: matrix_result_label(
                "longitudinal", artifact.revision, scale, lane
            ),
        ),
    )

    rows = load_longitudinal_rows(store_dir)
    assert summary["ingested_rows"] == 2
    assert store_db_path(store_dir).exists()
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
