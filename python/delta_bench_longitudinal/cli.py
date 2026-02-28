from __future__ import annotations

import argparse
import json
from dataclasses import asdict, is_dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Union

from .artifacts import (
    ArtifactBuildMetadata,
    artifact_metadata_path,
    build_revision_artifact,
    is_trusted_artifact_path,
    load_artifact_metadata,
)
from .matrix_runner import (
    MatrixArtifact,
    MatrixRunConfig,
    load_matrix_state,
    matrix_result_label,
    run_matrix,
    sanitize_label,
)
from .retention import prune_artifacts, prune_store
from .reporting import generate_trend_reports
from .revisions import load_manifest, select_revisions, write_manifest
from .store import ingest_benchmark_result


BuildFn = Callable[..., Union[ArtifactBuildMetadata, dict[str, Any]]]


def orchestrate_from_manifest(
    *,
    manifest_path: Path | str,
    artifacts_dir: Path | str,
    results_dir: Path | str,
    state_path: Path | str,
    store_dir: Path | str,
    markdown_path: Path | str,
    html_path: Path | str,
    suites: list[str],
    scales: list[str],
    timeout_seconds: int,
    max_retries: int,
    max_parallel: int,
    max_load_per_cpu: float | None,
    load_check_interval_seconds: float,
    baseline_window: int,
    regression_threshold: float,
    significance_method: str,
    significance_alpha: float,
    build_fn: BuildFn | None = None,
    matrix_executor=None,
    label_prefix: str = "longitudinal",
) -> dict[str, int]:
    manifest = load_manifest(manifest_path)
    chosen_build = build_fn or build_revision_artifact
    build_results: list[ArtifactBuildMetadata] = []

    for revision in manifest.revisions:
        raw_meta = chosen_build(
            repository=manifest.repository,
            revision=revision.commit,
            commit_timestamp=revision.commit_timestamp,
            artifacts_dir=artifacts_dir,
        )
        build_results.append(_coerce_metadata(raw_meta))

    expected_revisions = {entry.commit for entry in manifest.revisions}
    artifacts: list[MatrixArtifact] = []
    for meta in build_results:
        if meta.status != "success" or not meta.artifact_path:
            continue
        if meta.revision not in expected_revisions:
            continue
        if not is_trusted_artifact_path(
            artifacts_dir=artifacts_dir,
            revision=meta.revision,
            artifact_path=meta.artifact_path,
        ):
            continue
        artifacts.append(
            MatrixArtifact(
                revision=meta.revision,
                commit_timestamp=meta.commit_timestamp,
                artifact_path=meta.artifact_path,
            )
        )

    matrix_state = run_matrix(
        artifacts=artifacts,
        config=MatrixRunConfig(
            suites=suites,
            scales=scales,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            state_path=state_path,
            results_dir=results_dir,
            label_prefix=label_prefix,
            max_parallel=max_parallel,
            max_load_per_cpu=max_load_per_cpu,
            load_check_interval_seconds=load_check_interval_seconds,
        ),
        executor=matrix_executor,
    )

    revision_to_ts = {entry.commit: entry.commit_timestamp for entry in manifest.revisions}
    ingested_rows = _ingest_from_state(
        matrix_state=matrix_state,
        results_dir=Path(results_dir),
        store_dir=Path(store_dir),
        label_prefix=label_prefix,
        revision_to_ts=revision_to_ts,
    )

    report_summary = generate_trend_reports(
        store_dir=store_dir,
        markdown_path=markdown_path,
        html_path=html_path,
        baseline_window=baseline_window,
        regression_threshold=regression_threshold,
        significance_method=significance_method,
        significance_alpha=significance_alpha,
    )

    return {
        "built": len(artifacts),
        "ingested_rows": ingested_rows,
        "total_series": report_summary["total_series"],
        "regressions": report_summary["regressions"],
    }


def _ingest_from_state(
    *,
    matrix_state: dict[str, Any],
    results_dir: Path,
    store_dir: Path,
    label_prefix: str,
    revision_to_ts: dict[str, str],
) -> int:
    total = 0
    for case in matrix_state.get("cases", {}).values():
        if case.get("status") != "success":
            continue
        revision = str(case.get("revision"))
        suite = str(case.get("suite"))
        scale = str(case.get("scale"))
        candidate_labels = [
            matrix_result_label(label_prefix, revision, scale),
            sanitize_label(f"{label_prefix}-{revision}"),
        ]
        result_path: Path | None = None
        for label in candidate_labels:
            candidate = results_dir / label / f"{suite}.json"
            if candidate.exists():
                result_path = candidate
                break
        if result_path is None:
            continue
        outcome = ingest_benchmark_result(
            store_dir=store_dir,
            result_path=result_path,
            revision=revision,
            commit_timestamp=revision_to_ts.get(revision, "unknown"),
        )
        total += int(outcome["rows_appended"])
    return total


def _coerce_metadata(value: ArtifactBuildMetadata | dict[str, Any]) -> ArtifactBuildMetadata:
    if isinstance(value, ArtifactBuildMetadata):
        return value
    if is_dataclass(value):
        return ArtifactBuildMetadata(**asdict(value))
    return ArtifactBuildMetadata(**value)


def _load_manifest_artifacts(
    manifest_path: Path | str,
    artifacts_dir: Path | str,
) -> tuple[list[MatrixArtifact], dict[str, str]]:
    manifest = load_manifest(manifest_path)
    revision_to_ts = {entry.commit: entry.commit_timestamp for entry in manifest.revisions}
    artifacts: list[MatrixArtifact] = []
    for entry in manifest.revisions:
        metadata_file = artifact_metadata_path(artifacts_dir, entry.commit)
        if not metadata_file.exists():
            continue
        metadata = load_artifact_metadata(metadata_file)
        if metadata.status != "success" or not metadata.artifact_path:
            continue
        if metadata.revision != entry.commit:
            continue
        if not is_trusted_artifact_path(
            artifacts_dir=artifacts_dir,
            revision=metadata.revision,
            artifact_path=metadata.artifact_path,
        ):
            continue
        artifacts.append(
            MatrixArtifact(
                revision=metadata.revision,
                commit_timestamp=metadata.commit_timestamp,
                artifact_path=metadata.artifact_path,
            )
        )
    return artifacts, revision_to_ts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Longitudinal benchmark orchestration")
    sub = parser.add_subparsers(dest="command", required=True)

    select_cmd = sub.add_parser("select-revisions", help="Generate revision manifest")
    select_cmd.add_argument("--repository", required=True, type=Path)
    select_cmd.add_argument("--strategy", required=True)
    select_cmd.add_argument("--start-date")
    select_cmd.add_argument("--end-date")
    select_cmd.add_argument("--ref", default="HEAD")
    select_cmd.add_argument("--release-tag-pattern", default=None)
    select_cmd.add_argument("--output", required=True, type=Path)

    build_cmd = sub.add_parser("build-artifacts", help="Build artifacts for manifest revisions")
    build_cmd.add_argument("--manifest", required=True, type=Path)
    build_cmd.add_argument("--artifacts-dir", required=True, type=Path)

    matrix_cmd = sub.add_parser("run-matrix", help="Run suite/scale matrix for built artifacts")
    matrix_cmd.add_argument("--manifest", required=True, type=Path)
    matrix_cmd.add_argument("--artifacts-dir", required=True, type=Path)
    matrix_cmd.add_argument("--state-path", required=True, type=Path)
    matrix_cmd.add_argument("--results-dir", required=True, type=Path)
    matrix_cmd.add_argument("--fixtures-dir", default=Path("fixtures"), type=Path)
    matrix_cmd.add_argument("--suite", action="append", required=True)
    matrix_cmd.add_argument("--scale", action="append", required=True)
    matrix_cmd.add_argument("--timeout-seconds", type=int, default=3600)
    matrix_cmd.add_argument("--max-retries", type=int, default=2)
    matrix_cmd.add_argument("--max-parallel", type=int, default=1)
    matrix_cmd.add_argument("--max-load-per-cpu", type=float, default=None)
    matrix_cmd.add_argument("--load-check-interval-seconds", type=float, default=5.0)
    matrix_cmd.add_argument("--label-prefix", default="longitudinal")

    ingest_cmd = sub.add_parser("ingest-results", help="Normalize successful matrix results")
    ingest_cmd.add_argument("--manifest", required=True, type=Path)
    ingest_cmd.add_argument("--state-path", required=True, type=Path)
    ingest_cmd.add_argument("--results-dir", required=True, type=Path)
    ingest_cmd.add_argument("--store-dir", required=True, type=Path)
    ingest_cmd.add_argument("--label-prefix", default="longitudinal")

    report_cmd = sub.add_parser("report", help="Generate trend markdown+html")
    report_cmd.add_argument("--store-dir", required=True, type=Path)
    report_cmd.add_argument("--markdown-path", required=True, type=Path)
    report_cmd.add_argument("--html-path", required=True, type=Path)
    report_cmd.add_argument("--baseline-window", type=int, default=7)
    report_cmd.add_argument("--regression-threshold", type=float, default=0.05)
    report_cmd.add_argument(
        "--significance-method",
        choices=["none", "mann-whitney"],
        default="none",
    )
    report_cmd.add_argument("--significance-alpha", type=float, default=0.05)

    prune_cmd = sub.add_parser("prune", help="Apply retention policies to artifacts/store")
    prune_cmd.add_argument("--artifacts-dir", type=Path)
    prune_cmd.add_argument("--store-dir", type=Path)
    prune_cmd.add_argument("--max-artifact-age-days", type=int, default=None)
    prune_cmd.add_argument("--max-artifacts", type=int, default=None)
    prune_cmd.add_argument("--max-run-age-days", type=int, default=None)
    prune_cmd.add_argument("--max-runs", type=int, default=None)
    prune_cmd.add_argument("--apply", action="store_true")

    orchestration_cmd = sub.add_parser("orchestrate", help="Build -> run matrix -> ingest -> report")
    orchestration_cmd.add_argument("--manifest", required=True, type=Path)
    orchestration_cmd.add_argument("--artifacts-dir", required=True, type=Path)
    orchestration_cmd.add_argument("--results-dir", required=True, type=Path)
    orchestration_cmd.add_argument("--state-path", required=True, type=Path)
    orchestration_cmd.add_argument("--store-dir", required=True, type=Path)
    orchestration_cmd.add_argument("--markdown-path", required=True, type=Path)
    orchestration_cmd.add_argument("--html-path", required=True, type=Path)
    orchestration_cmd.add_argument("--suite", action="append", required=True)
    orchestration_cmd.add_argument("--scale", action="append", required=True)
    orchestration_cmd.add_argument("--timeout-seconds", type=int, default=3600)
    orchestration_cmd.add_argument("--max-retries", type=int, default=2)
    orchestration_cmd.add_argument("--max-parallel", type=int, default=1)
    orchestration_cmd.add_argument("--max-load-per-cpu", type=float, default=None)
    orchestration_cmd.add_argument("--load-check-interval-seconds", type=float, default=5.0)
    orchestration_cmd.add_argument("--baseline-window", type=int, default=7)
    orchestration_cmd.add_argument("--regression-threshold", type=float, default=0.05)
    orchestration_cmd.add_argument(
        "--significance-method",
        choices=["none", "mann-whitney"],
        default="none",
    )
    orchestration_cmd.add_argument("--significance-alpha", type=float, default=0.05)
    orchestration_cmd.add_argument("--label-prefix", default="longitudinal")

    args = parser.parse_args(argv)
    if args.command == "select-revisions":
        manifest = select_revisions(
            args.repository,
            strategy=args.strategy,
            start_date=args.start_date,
            end_date=args.end_date,
            ref=args.ref,
            release_tag_pattern=args.release_tag_pattern
            if args.release_tag_pattern is not None
            else r"^v\d+\.\d+\.\d+([.-].+)?$",
        )
        write_manifest(manifest, args.output)
        print(str(args.output))
        return 0

    if args.command == "build-artifacts":
        manifest = load_manifest(args.manifest)
        built = 0
        for revision in manifest.revisions:
            metadata = build_revision_artifact(
                repository=manifest.repository,
                revision=revision.commit,
                commit_timestamp=revision.commit_timestamp,
                artifacts_dir=args.artifacts_dir,
            )
            if metadata.status == "success":
                built += 1
        print(json.dumps({"built": built}, sort_keys=True))
        return 0

    if args.command == "run-matrix":
        artifacts, _ = _load_manifest_artifacts(args.manifest, args.artifacts_dir)
        state = run_matrix(
            artifacts=artifacts,
            config=MatrixRunConfig(
                suites=args.suite,
                scales=args.scale,
                timeout_seconds=args.timeout_seconds,
                max_retries=args.max_retries,
                max_parallel=args.max_parallel,
                max_load_per_cpu=args.max_load_per_cpu,
                load_check_interval_seconds=args.load_check_interval_seconds,
                state_path=args.state_path,
                fixtures_dir=args.fixtures_dir,
                results_dir=args.results_dir,
                label_prefix=args.label_prefix,
            ),
        )
        print(json.dumps({"cases": len(state.get("cases", {}))}, sort_keys=True))
        return 0

    if args.command == "ingest-results":
        matrix_state = load_matrix_state(args.state_path)
        manifest = load_manifest(args.manifest)
        revision_to_ts = {
            entry.commit: entry.commit_timestamp for entry in manifest.revisions
        }
        rows = _ingest_from_state(
            matrix_state=matrix_state,
            results_dir=args.results_dir,
            store_dir=args.store_dir,
            label_prefix=args.label_prefix,
            revision_to_ts=revision_to_ts,
        )
        print(json.dumps({"ingested_rows": rows}, sort_keys=True))
        return 0

    if args.command == "report":
        summary = generate_trend_reports(
            store_dir=args.store_dir,
            markdown_path=args.markdown_path,
            html_path=args.html_path,
            baseline_window=args.baseline_window,
            regression_threshold=args.regression_threshold,
            significance_method=args.significance_method,
            significance_alpha=args.significance_alpha,
        )
        print(json.dumps(summary, sort_keys=True))
        return 0

    if args.command == "prune":
        summary: dict[str, Any] = {}
        if args.artifacts_dir is None and args.store_dir is None:
            raise SystemExit("at least one of --artifacts-dir or --store-dir is required")

        if args.artifacts_dir is not None:
            summary["artifacts"] = prune_artifacts(
                artifacts_dir=args.artifacts_dir,
                max_age_days=args.max_artifact_age_days,
                max_artifacts=args.max_artifacts,
                apply=args.apply,
            )
        if args.store_dir is not None:
            summary["store"] = prune_store(
                store_dir=args.store_dir,
                max_age_days=args.max_run_age_days,
                max_runs=args.max_runs,
                apply=args.apply,
            )
        print(json.dumps(summary, sort_keys=True))
        return 0

    if args.command == "orchestrate":
        summary = orchestrate_from_manifest(
            manifest_path=args.manifest,
            artifacts_dir=args.artifacts_dir,
            results_dir=args.results_dir,
            state_path=args.state_path,
            store_dir=args.store_dir,
            markdown_path=args.markdown_path,
            html_path=args.html_path,
            suites=args.suite,
            scales=args.scale,
            timeout_seconds=args.timeout_seconds,
            max_retries=args.max_retries,
            max_parallel=args.max_parallel,
            max_load_per_cpu=args.max_load_per_cpu,
            load_check_interval_seconds=args.load_check_interval_seconds,
            baseline_window=args.baseline_window,
            regression_threshold=args.regression_threshold,
            significance_method=args.significance_method,
            significance_alpha=args.significance_alpha,
            label_prefix=args.label_prefix,
        )
        print(json.dumps(summary, sort_keys=True))
        return 0

    raise AssertionError(f"unexpected command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
