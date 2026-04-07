from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
REGISTRY_PATH = REPO_ROOT / "bench" / "evidence" / "registry.yaml"


def _python_env() -> dict[str, str]:
    return {**os.environ, "PYTHONPATH": str(REPO_ROOT / "python")}


def _write_registry(path: Path, *, write_perf_ready: bool, tpcds_ready: bool) -> None:
    lines = [
        "schema_version: 1",
        "",
        "suites:",
        "  scan:",
        "    class: authoritative_macro",
        "    automation_tier: pr_bot",
        "    default_profile: pr-macro",
        "    readiness: ready",
        "  write_perf:",
        "    class: authoritative_macro",
        f"    automation_tier: {'pr_bot' if write_perf_ready else 'candidate_pr_bot'}",
        "    default_profile: pr-write-perf",
        f"    readiness: {'ready' if write_perf_ready else 'gated'}",
    ]
    if not write_perf_ready:
        lines.append(
            '    readiness_reason: "await same-SHA stability + regression canary + runtime signoff"'
        )
    lines.extend(
        [
            "  tpcds:",
            "    class: authoritative_macro",
            f"    automation_tier: {'pr_bot' if tpcds_ready else 'candidate_pr_bot'}",
            "    default_profile: pr-tpcds",
            f"    readiness: {'ready' if tpcds_ready else 'gated'}",
        ]
    )
    if not tpcds_ready:
        lines.append(
            '    readiness_reason: "await explicit fixture provisioning + validation signoff"'
        )
    lines.extend(
        [
            "",
            "packs:",
            "  pr-full-decision:",
            "    alias: full",
            "    pack_version: 1",
            "    compare_mode: decision",
            "    strict_mode: require_all_ready",
            "    max_parallel: 2",
            "    overall_fail_on: [regression, inconclusive]",
            "    suites:",
            "      - suite: scan",
            "        profile: pr-macro",
            "        timeout_minutes: 90",
        ]
    )
    if write_perf_ready:
        lines.extend(
            [
                "      - suite: write_perf",
                "        profile: pr-write-perf",
                "        timeout_minutes: 120",
            ]
        )
    if tpcds_ready:
        lines.extend(
            [
                "      - suite: tpcds",
                "        profile: pr-tpcds",
                "        timeout_minutes: 150",
            ]
        )
    candidate_entries: list[str] = []
    if not write_perf_ready:
        candidate_entries.extend(
            [
                "      - suite: write_perf",
                "        profile: pr-write-perf",
                "        timeout_minutes: 120",
            ]
        )
    if not tpcds_ready:
        candidate_entries.extend(
            [
                "      - suite: tpcds",
                "        profile: pr-tpcds",
                "        timeout_minutes: 150",
            ]
        )
    if candidate_entries:
        lines.extend(
            [
                "  pr-candidate-manual:",
                "    pack_version: 1",
                "    compare_mode: decision",
                "    max_parallel: 1",
                "    overall_fail_on: [regression, inconclusive]",
                "    suites:",
            ]
        )
        lines.extend(candidate_entries)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def _write_suite_artifact(
    root: Path,
    *,
    suite: str,
    profile: str,
    base_sha: str,
    candidate_sha: str,
    status: str,
    delta_pct: float,
    artifact_dir_name: str | None = None,
) -> Path:
    artifact_dir = root / (artifact_dir_name or suite)
    artifact_dir.mkdir(parents=True, exist_ok=True)
    (artifact_dir / "summary.md").write_text(
        f"## {suite} [{profile}]\n\nstatus: {status}\n",
        encoding="utf-8",
    )
    (artifact_dir / "hash-policy.txt").write_text(
        f"{suite}[{profile}]: trusted\n",
        encoding="utf-8",
    )
    comparison_payload = {
        "schema_version": 1,
        "metadata": {
            "mode": "decision",
            "aggregation": "median",
            "noise_threshold": 0.05,
        },
        "summary": {
            "faster": 0,
            "slower": 1 if status == "regression" else 0,
            "no_change": 1 if status == "no_change" else 0,
            "incomparable": 1 if status == "inconclusive" else 0,
            "new": 0,
            "removed": 0,
        },
        "rows": [
            {
                "case": f"{suite}_case",
                "profile": profile,
                "status": status,
                "display_change": status,
                "baseline_ms": 100.0,
                "candidate_ms": 100.0 * (1.0 + delta_pct / 100.0),
                "delta_pct": delta_pct,
                "baseline_classification": "supported",
                "candidate_classification": "supported",
                "decision_scope": "macro",
                "scope_reason": None,
                "spread_metric": "iqr_ms",
                "baseline_spread_ms": 1.0,
                "candidate_spread_ms": 1.0,
            }
        ],
    }
    (artifact_dir / "comparison.json").write_text(
        json.dumps(comparison_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    manifest_path = artifact_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(
            {
                "suite": suite,
                "profile": profile,
                "base_sha": base_sha,
                "candidate_sha": candidate_sha,
                "markdown_report": str(artifact_dir / "summary.md"),
                "comparison_json": str(artifact_dir / "comparison.json"),
                "hash_policy_report": str(artifact_dir / "hash-policy.txt"),
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest_path


def test_registry_file_exists_and_declares_full_alias() -> None:
    from delta_bench_compare.registry import load_registry, pack_suite_definitions, resolve_pack

    assert REGISTRY_PATH.exists(), "missing bench/evidence/registry.yaml"
    registry = load_registry(REGISTRY_PATH)
    pack_id, pack, alias = resolve_pack(registry, "full")
    assert pack_id == "pr-full-decision"
    assert alias == "full"
    assert str(pack["strict_mode"]) == "require_all_ready"

    full_suites = [
        entry["suite"] for entry in pack_suite_definitions(registry, pack)
    ]
    expected_ready_suites = ["scan"]
    if registry["suites"]["write_perf"]["readiness"] == "ready":
        expected_ready_suites.append("write_perf")
    assert full_suites == expected_ready_suites
    assert "tpcds" not in full_suites

    write_perf_registry = registry["suites"]["write_perf"]
    assert write_perf_registry["automation_tier"] == "candidate_pr_bot"
    assert write_perf_registry["readiness"] == "gated"

    tpcds_registry = registry["suites"]["tpcds"]
    assert tpcds_registry["automation_tier"] == "candidate_pr_bot"
    assert tpcds_registry["readiness"] == "gated"

    candidate_pack = registry["packs"].get("pr-candidate-manual")
    assert isinstance(candidate_pack, dict)
    candidate_suites = [
        entry["suite"] for entry in pack_suite_definitions(registry, candidate_pack)
    ]
    assert "write_perf" in candidate_suites
    assert "tpcds" in candidate_suites


def test_actual_registry_keeps_current_ready_and_candidate_scopes_explicit() -> None:
    from delta_bench_compare.registry import load_registry, pack_suite_definitions

    registry = load_registry(REGISTRY_PATH)

    full_pack = registry["packs"]["pr-full-decision"]
    assert [
        entry["suite"] for entry in pack_suite_definitions(registry, full_pack)
    ] == ["scan"]

    candidate_pack = registry["packs"]["pr-candidate-manual"]
    assert [
        entry["suite"] for entry in pack_suite_definitions(registry, candidate_pack)
    ] == [
        "write_perf",
        "delete_update_perf",
        "merge_perf",
        "optimize_perf",
        "metadata_perf",
        "tpcds",
    ]


def test_pack_plan_keeps_full_pack_ready_when_gated_suites_are_excluded(
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "registry.yaml"
    _write_registry(registry_path, write_perf_ready=False, tpcds_ready=False)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.pack",
            "plan",
            "--registry",
            str(registry_path),
            "--pack",
            "full",
            "--base-sha",
            "a" * 40,
            "--candidate-sha",
            "b" * 40,
            "--format",
            "github-matrix",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["pack_id"] == "pr-full-decision"
    assert payload["include"] == [
        {
            "suite": "scan",
            "profile": "pr-macro",
            "timeout_minutes": 90,
            "shard_key": "scan__pr-macro",
            "artifact_name": "benchmark-scan-pr-macro",
        }
    ]


def test_candidate_pack_collects_gated_perf_suites_for_manual_runs(
    tmp_path: Path,
) -> None:
    registry_path = tmp_path / "registry.yaml"
    _write_registry(registry_path, write_perf_ready=False, tpcds_ready=False)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.pack",
            "plan",
            "--registry",
            str(registry_path),
            "--pack",
            "pr-candidate-manual",
            "--base-sha",
            "a" * 40,
            "--candidate-sha",
            "b" * 40,
            "--format",
            "github-matrix",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["pack_id"] == "pr-candidate-manual"
    assert payload["include"] == [
        {
            "suite": "write_perf",
            "profile": "pr-write-perf",
            "timeout_minutes": 120,
            "shard_key": "write_perf__pr-write-perf",
            "artifact_name": "benchmark-write-perf-pr-write-perf",
        },
        {
            "suite": "tpcds",
            "profile": "pr-tpcds",
            "timeout_minutes": 150,
            "shard_key": "tpcds__pr-tpcds",
            "artifact_name": "benchmark-tpcds-pr-tpcds",
        },
    ]


def test_registry_declares_remote_candidate_manual_pack() -> None:
    from delta_bench_compare.registry import load_registry, pack_suite_definitions

    registry = load_registry(REGISTRY_PATH)
    remote_surfaces = registry.get("surfaces")
    assert isinstance(remote_surfaces, dict)
    assert remote_surfaces["write_perf_s3"]["readiness"] == "gated"
    assert "non-local write throughput" in remote_surfaces["write_perf_s3"][
        "readiness_reason"
    ]
    remote_pack = registry["packs"].get("s3-candidate-manual")
    assert isinstance(remote_pack, dict)

    suite_entries = pack_suite_definitions(registry, remote_pack)
    identities = [
        (
            entry.get("surface"),
            entry["suite"],
            entry["profile"],
            entry.get("storage_backend"),
            entry.get("backend_profile"),
        )
        for entry in suite_entries
    ]
    assert identities == [
        ("scan_s3", "scan", "scan-s3-candidate", "s3", "s3_locking_vultr"),
        (
            "delete_update_perf_s3",
            "delete_update_perf",
            "delete-update-perf-s3-candidate",
            "s3",
            "s3_locking_vultr",
        ),
        (
            "merge_perf_s3",
            "merge_perf",
            "merge-perf-s3-candidate",
            "s3",
            "s3_locking_vultr",
        ),
        (
            "optimize_perf_s3",
            "optimize_perf",
            "optimize-perf-s3-candidate",
            "s3",
            "s3_locking_vultr",
        ),
        (
            "metadata_perf_s3",
            "metadata_perf",
            "metadata-perf-s3-candidate",
            "s3",
            "s3_locking_vultr",
        ),
    ]


def test_pack_plan_emits_storage_defaults_for_remote_candidate_pack() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.pack",
            "plan",
            "--registry",
            str(REGISTRY_PATH),
            "--pack",
            "s3-candidate-manual",
            "--base-sha",
            "a" * 40,
            "--candidate-sha",
            "b" * 40,
            "--format",
            "github-matrix",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["pack_id"] == "s3-candidate-manual"
    assert payload["include"] == [
        {
            "surface": "scan_s3",
            "suite": "scan",
            "profile": "scan-s3-candidate",
            "timeout_minutes": 120,
            "shard_key": "scan__scan-s3-candidate",
            "artifact_name": "benchmark-scan-scan-s3-candidate",
            "storage_backend": "s3",
            "backend_profile": "s3_locking_vultr",
        },
        {
            "surface": "delete_update_perf_s3",
            "suite": "delete_update_perf",
            "profile": "delete-update-perf-s3-candidate",
            "timeout_minutes": 120,
            "shard_key": "delete_update_perf__delete-update-perf-s3-candidate",
            "artifact_name": "benchmark-delete-update-perf-delete-update-perf-s3-candidate",
            "storage_backend": "s3",
            "backend_profile": "s3_locking_vultr",
        },
        {
            "surface": "merge_perf_s3",
            "suite": "merge_perf",
            "profile": "merge-perf-s3-candidate",
            "timeout_minutes": 120,
            "shard_key": "merge_perf__merge-perf-s3-candidate",
            "artifact_name": "benchmark-merge-perf-merge-perf-s3-candidate",
            "storage_backend": "s3",
            "backend_profile": "s3_locking_vultr",
        },
        {
            "surface": "optimize_perf_s3",
            "suite": "optimize_perf",
            "profile": "optimize-perf-s3-candidate",
            "timeout_minutes": 120,
            "shard_key": "optimize_perf__optimize-perf-s3-candidate",
            "artifact_name": "benchmark-optimize-perf-optimize-perf-s3-candidate",
            "storage_backend": "s3",
            "backend_profile": "s3_locking_vultr",
        },
        {
            "surface": "metadata_perf_s3",
            "suite": "metadata_perf",
            "profile": "metadata-perf-s3-candidate",
            "timeout_minutes": 120,
            "shard_key": "metadata_perf__metadata-perf-s3-candidate",
            "artifact_name": "benchmark-metadata-perf-metadata-perf-s3-candidate",
            "storage_backend": "s3",
            "backend_profile": "s3_locking_vultr",
        },
    ]


def test_pack_plan_emits_explicit_suite_shards_for_ready_full_pack(tmp_path: Path) -> None:
    registry_path = tmp_path / "registry.yaml"
    _write_registry(registry_path, write_perf_ready=True, tpcds_ready=True)

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.pack",
            "plan",
            "--registry",
            str(registry_path),
            "--pack",
            "full",
            "--base-sha",
            "1" * 40,
            "--candidate-sha",
            "2" * 40,
            "--format",
            "github-matrix",
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )

    assert result.returncode == 0, result.stderr
    payload = json.loads(result.stdout)
    assert payload["pack_id"] == "pr-full-decision"
    assert payload["pack_alias"] == "full"
    assert payload["compare_mode"] == "decision"
    assert payload["max_parallel"] == 2
    assert payload["base_sha"] == "1" * 40
    assert payload["candidate_sha"] == "2" * 40
    assert payload["matrix"] == {"include": payload["include"]}
    assert payload["include"] == [
        {
            "suite": "scan",
            "profile": "pr-macro",
            "timeout_minutes": 90,
            "shard_key": "scan__pr-macro",
            "artifact_name": "benchmark-scan-pr-macro",
        },
        {
            "suite": "write_perf",
            "profile": "pr-write-perf",
            "timeout_minutes": 120,
            "shard_key": "write_perf__pr-write-perf",
            "artifact_name": "benchmark-write-perf-pr-write-perf",
        },
        {
            "suite": "tpcds",
            "profile": "pr-tpcds",
            "timeout_minutes": 150,
            "shard_key": "tpcds__pr-tpcds",
            "artifact_name": "benchmark-tpcds-pr-tpcds",
        },
    ]


def test_pack_summarize_flattens_suite_rows_and_writes_pack_artifacts(
    tmp_path: Path,
) -> None:
    base_sha = "a" * 40
    candidate_sha = "b" * 40
    registry_path = tmp_path / "registry.yaml"
    _write_registry(registry_path, write_perf_ready=True, tpcds_ready=True)

    scan_manifest = _write_suite_artifact(
        tmp_path,
        suite="scan",
        profile="pr-macro",
        base_sha=base_sha,
        candidate_sha=candidate_sha,
        status="no_change",
        delta_pct=0.0,
    )
    write_perf_manifest = _write_suite_artifact(
        tmp_path,
        suite="write_perf",
        profile="pr-write-perf",
        base_sha=base_sha,
        candidate_sha=candidate_sha,
        status="regression",
        delta_pct=8.0,
    )
    tpcds_manifest = _write_suite_artifact(
        tmp_path,
        suite="tpcds",
        profile="pr-tpcds",
        base_sha=base_sha,
        candidate_sha=candidate_sha,
        status="no_change",
        delta_pct=0.0,
    )

    output_dir = tmp_path / "pack-artifacts"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.pack",
            "summarize",
            "--registry",
            str(registry_path),
            "--pack",
            "pr-full-decision",
            "--output-dir",
            str(output_dir),
            "--suite-manifest",
            str(scan_manifest),
            "--suite-manifest",
            str(write_perf_manifest),
            "--suite-manifest",
            str(tpcds_manifest),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )

    assert result.returncode == 0, result.stderr
    assert (output_dir / "summary.md").is_file()
    assert (output_dir / "comparison.json").is_file()
    assert (output_dir / "hash-policy.txt").is_file()
    assert (output_dir / "manifest.json").is_file()

    comparison_payload = json.loads(
        (output_dir / "comparison.json").read_text(encoding="utf-8")
    )
    assert comparison_payload["schema_version"] == 1
    assert comparison_payload["metadata"]["pack_id"] == "pr-full-decision"
    assert comparison_payload["summary"]["slower"] == 1
    rows = comparison_payload["rows"]
    assert rows[0]["suite"] == "scan"
    assert rows[0]["profile"] == "pr-macro"
    assert rows[1]["suite"] == "write_perf"
    assert rows[1]["profile"] == "pr-write-perf"
    assert rows[1]["case"] == "write_perf_case"

    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["pack_id"] == "pr-full-decision"
    assert manifest["pack_version"] == 1
    assert manifest["base_sha"] == base_sha
    assert manifest["candidate_sha"] == candidate_sha
    assert manifest["suite_statuses"] == {
        "scan[pr-macro]": "completed",
        "write_perf[pr-write-perf]": "completed",
        "tpcds[pr-tpcds]": "completed",
    }
    assert len(manifest["suite_manifests"]) == 3
    assert manifest["suite_artifacts"]["scan[pr-macro]"]["profile"] == "pr-macro"


def test_pack_summarize_fails_closed_when_a_planned_suite_manifest_is_missing(
    tmp_path: Path,
) -> None:
    base_sha = "a" * 40
    candidate_sha = "b" * 40
    registry_path = tmp_path / "registry.yaml"
    _write_registry(registry_path, write_perf_ready=True, tpcds_ready=True)

    scan_manifest = _write_suite_artifact(
        tmp_path,
        suite="scan",
        profile="pr-macro",
        base_sha=base_sha,
        candidate_sha=candidate_sha,
        status="no_change",
        delta_pct=0.0,
    )
    write_perf_manifest = _write_suite_artifact(
        tmp_path,
        suite="write_perf",
        profile="pr-write-perf",
        base_sha=base_sha,
        candidate_sha=candidate_sha,
        status="no_change",
        delta_pct=0.0,
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.pack",
            "summarize",
            "--registry",
            str(registry_path),
            "--pack",
            "pr-full-decision",
            "--output-dir",
            str(tmp_path / "pack-artifacts"),
            "--suite-manifest",
            str(scan_manifest),
            "--suite-manifest",
            str(write_perf_manifest),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )

    assert result.returncode != 0
    assert "missing suite manifests" in result.stderr
    assert "tpcds" in result.stderr


def test_pack_summarize_fails_closed_when_planned_suite_manifests_are_missing(
    tmp_path: Path,
) -> None:
    base_sha = "a" * 40
    candidate_sha = "b" * 40
    registry_path = tmp_path / "registry.yaml"
    _write_registry(registry_path, write_perf_ready=True, tpcds_ready=True)

    scan_manifest = _write_suite_artifact(
        tmp_path,
        suite="scan",
        profile="pr-macro",
        base_sha=base_sha,
        candidate_sha=candidate_sha,
        status="no_change",
        delta_pct=0.0,
    )

    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.pack",
            "summarize",
            "--registry",
            str(registry_path),
            "--pack",
            "pr-full-decision",
            "--output-dir",
            str(tmp_path / "pack-artifacts"),
            "--suite-manifest",
            str(scan_manifest),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )

    assert result.returncode != 0
    assert "missing suite manifests" in result.stderr
    assert "write_perf" in result.stderr
    assert "tpcds" in result.stderr


def test_pack_summarize_uses_durable_suite_state_to_render_failed_summary(
    tmp_path: Path,
) -> None:
    base_sha = "a" * 40
    candidate_sha = "b" * 40
    registry_path = tmp_path / "registry.yaml"
    _write_registry(registry_path, write_perf_ready=True, tpcds_ready=True)

    scan_manifest = _write_suite_artifact(
        tmp_path,
        suite="scan",
        profile="pr-macro",
        base_sha=base_sha,
        candidate_sha=candidate_sha,
        status="no_change",
        delta_pct=0.0,
    )
    request_state_path = tmp_path / "request-state.json"
    request_state_path.write_text(
        json.dumps(
            {
                "request_id": 42,
                "status": "aggregating",
                "suites": [
                    {
                        "suite": "scan",
                        "profile": "pr-macro",
                        "status": "completed",
                        "artifact_name": "benchmark-scan-pr-macro",
                        "exit_code": 0,
                    },
                    {
                        "suite": "write_perf",
                        "profile": "pr-write-perf",
                        "status": "failed",
                        "artifact_name": "benchmark-write-perf-pr-write-perf",
                        "exit_code": 124,
                    },
                    {
                        "suite": "tpcds",
                        "profile": "pr-tpcds",
                        "status": "blocked",
                        "artifact_name": "benchmark-tpcds-pr-tpcds",
                        "exit_code": None,
                    },
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    output_dir = tmp_path / "pack-artifacts"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.pack",
            "summarize",
            "--registry",
            str(registry_path),
            "--pack",
            "pr-full-decision",
            "--output-dir",
            str(output_dir),
            "--request-state",
            str(request_state_path),
            "--suite-manifest",
            str(scan_manifest),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )

    assert result.returncode == 0, result.stderr
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["overall_status"] == "failed"
    assert manifest["suite_statuses"] == {
        "scan[pr-macro]": "completed",
        "write_perf[pr-write-perf]": "failed",
        "tpcds[pr-tpcds]": "blocked",
    }
    summary_markdown = (output_dir / "summary.md").read_text(encoding="utf-8")
    assert "FAILED" in summary_markdown
    assert "| write_perf | pr-write-perf | failed |" in summary_markdown
    assert "| tpcds | pr-tpcds | blocked |" in summary_markdown


def test_pack_summarize_can_render_terminal_request_state_without_suite_manifests(
    tmp_path: Path,
) -> None:
    base_sha = "a" * 40
    candidate_sha = "b" * 40
    registry_path = tmp_path / "registry.yaml"
    _write_registry(registry_path, write_perf_ready=True, tpcds_ready=True)

    request_state_path = tmp_path / "request-state.json"
    request_state_path.write_text(
        json.dumps(
            {
                "request_id": 42,
                "status": "aggregating",
                "base_sha": base_sha,
                "candidate_sha": candidate_sha,
                "suites": [
                    {
                        "suite": "scan",
                        "profile": "pr-macro",
                        "status": "failed",
                        "artifact_name": "benchmark-scan-pr-macro",
                        "exit_code": 124,
                    },
                    {
                        "suite": "write_perf",
                        "profile": "pr-write-perf",
                        "status": "blocked",
                        "artifact_name": "benchmark-write-perf-pr-write-perf",
                        "exit_code": None,
                    },
                    {
                        "suite": "tpcds",
                        "profile": "pr-tpcds",
                        "status": "skipped",
                        "artifact_name": "benchmark-tpcds-pr-tpcds",
                        "exit_code": None,
                    },
                ],
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )

    output_dir = tmp_path / "pack-artifacts"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.pack",
            "summarize",
            "--registry",
            str(registry_path),
            "--pack",
            "pr-full-decision",
            "--output-dir",
            str(output_dir),
            "--request-state",
            str(request_state_path),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )

    assert result.returncode == 0, result.stderr
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["base_sha"] == base_sha
    assert manifest["candidate_sha"] == candidate_sha
    assert manifest["overall_status"] == "failed"
    assert manifest["suite_statuses"] == {
        "scan[pr-macro]": "failed",
        "write_perf[pr-write-perf]": "blocked",
        "tpcds[pr-tpcds]": "skipped",
    }
    summary_markdown = (output_dir / "summary.md").read_text(encoding="utf-8")
    assert "FAILED" in summary_markdown
    assert "| scan | pr-macro | failed |" in summary_markdown
    assert "| write_perf | pr-write-perf | blocked |" in summary_markdown
    assert "| tpcds | pr-tpcds | skipped |" in summary_markdown


def test_pack_summarize_disambiguates_duplicate_suite_names_by_profile(
    tmp_path: Path,
) -> None:
    base_sha = "a" * 40
    candidate_sha = "b" * 40
    registry_path = tmp_path / "registry.yaml"
    registry_path.write_text(
        "\n".join(
            [
                "schema_version: 1",
                "",
                "suites:",
                "  scan:",
                "    class: authoritative_macro",
                "    automation_tier: pr_bot",
                "    default_profile: pr-macro-a",
                "    readiness: ready",
                "",
                "packs:",
                "  pr-dual-scan-decision:",
                "    alias: dual-scan",
                "    pack_version: 1",
                "    compare_mode: decision",
                "    strict_mode: require_all_ready",
                "    max_parallel: 2",
                "    overall_fail_on: [regression, inconclusive]",
                "    suites:",
                "      - suite: scan",
                "        profile: pr-macro-a",
                "        timeout_minutes: 90",
                "      - suite: scan",
                "        profile: pr-macro-b",
                "        timeout_minutes: 95",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    scan_a_manifest = _write_suite_artifact(
        tmp_path,
        suite="scan",
        profile="pr-macro-a",
        base_sha=base_sha,
        candidate_sha=candidate_sha,
        status="no_change",
        delta_pct=0.0,
        artifact_dir_name="scan-pr-macro-a",
    )
    scan_b_manifest = _write_suite_artifact(
        tmp_path,
        suite="scan",
        profile="pr-macro-b",
        base_sha=base_sha,
        candidate_sha=candidate_sha,
        status="regression",
        delta_pct=7.5,
        artifact_dir_name="scan-pr-macro-b",
    )

    output_dir = tmp_path / "pack-artifacts"
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "delta_bench_compare.pack",
            "summarize",
            "--registry",
            str(registry_path),
            "--pack",
            "pr-dual-scan-decision",
            "--output-dir",
            str(output_dir),
            "--suite-manifest",
            str(scan_a_manifest),
            "--suite-manifest",
            str(scan_b_manifest),
        ],
        check=False,
        capture_output=True,
        text=True,
        env=_python_env(),
    )

    assert result.returncode == 0, result.stderr
    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["suite_statuses"] == {
        "scan[pr-macro-a]": "completed",
        "scan[pr-macro-b]": "completed",
    }
    assert manifest["suite_artifacts"]["scan[pr-macro-a]"]["profile"] == "pr-macro-a"
    assert manifest["suite_artifacts"]["scan[pr-macro-b]"]["profile"] == "pr-macro-b"
    comparison_payload = json.loads(
        (output_dir / "comparison.json").read_text(encoding="utf-8")
    )
    assert [row["profile"] for row in comparison_payload["rows"]] == [
        "pr-macro-a",
        "pr-macro-b",
    ]
    summary_markdown = (output_dir / "summary.md").read_text(encoding="utf-8")
    assert "| scan | pr-macro-a | completed |" in summary_markdown
    assert "| scan | pr-macro-b | completed |" in summary_markdown


def test_bot_state_reports_active_queue_with_per_suite_status(tmp_path: Path) -> None:
    from delta_bench_compare.bot_state import (
        create_request,
        format_queue,
        initialize_db,
        update_suite_status,
    )

    db_path = tmp_path / "pr-bot.sqlite3"
    initialize_db(db_path)
    request_id = create_request(
        db_path,
        repo="delta-io/delta-rs",
        pr_number=123,
        comment_id=456,
        actor="delta-force-bot",
        command="run benchmark decision full",
        pack="pr-full-decision",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
        workflow_run_id=999,
        suites=[
            {
                "suite": "scan",
                "profile": "pr-macro",
                "timeout_minutes": 90,
                "artifact_name": "scan-artifact",
            },
            {
                "suite": "write_perf",
                "profile": "pr-write-perf",
                "timeout_minutes": 120,
                "artifact_name": "write-perf-artifact",
            },
        ],
    )

    update_suite_status(db_path, request_id=request_id, suite="scan", status="running")
    update_suite_status(
        db_path,
        request_id=request_id,
        suite="write_perf",
        status="queued",
    )

    queue_text = format_queue(db_path)
    assert "run benchmark decision full" in queue_text
    assert "pr-full-decision" in queue_text
    assert "status=planning" in queue_text
    assert "scan[pr-macro]: running" in queue_text
    assert "write_perf[pr-write-perf]: queued" in queue_text


def test_bot_state_deduplicates_requests_by_comment_id(tmp_path: Path) -> None:
    from delta_bench_compare.bot_state import create_request, initialize_db

    db_path = tmp_path / "pr-bot.sqlite3"
    initialize_db(db_path)
    create_request(
        db_path,
        repo="delta-io/delta-rs",
        pr_number=1,
        comment_id=777,
        actor="maintainer",
        command="run benchmark decision full",
        pack="pr-full-decision",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
        workflow_run_id=1,
        suites=[
            {
                "suite": "scan",
                "profile": "pr-macro",
                "timeout_minutes": 90,
                "artifact_name": "scan-artifact",
            }
        ],
    )

    with pytest.raises(ValueError, match="comment_id"):
        create_request(
            db_path,
            repo="delta-io/delta-rs",
            pr_number=1,
            comment_id=777,
            actor="maintainer",
            command="run benchmark decision full",
            pack="pr-full-decision",
            base_sha="a" * 40,
            candidate_sha="b" * 40,
            workflow_run_id=2,
            suites=[
                {
                    "suite": "scan",
                    "profile": "pr-macro",
                    "timeout_minutes": 90,
                    "artifact_name": "scan-artifact",
                }
            ],
        )


def test_bot_state_request_status_update_fails_for_missing_request(tmp_path: Path) -> None:
    from delta_bench_compare.bot_state import initialize_db, update_request_status

    db_path = tmp_path / "pr-bot.sqlite3"
    initialize_db(db_path)

    with pytest.raises(ValueError, match="request_id=999"):
        update_request_status(db_path, request_id=999, status="running")


def test_bot_state_suite_status_update_fails_for_missing_suite_row(tmp_path: Path) -> None:
    from delta_bench_compare.bot_state import (
        create_request,
        initialize_db,
        update_suite_status,
    )

    db_path = tmp_path / "pr-bot.sqlite3"
    initialize_db(db_path)
    request_id = create_request(
        db_path,
        repo="delta-io/delta-rs",
        pr_number=1,
        comment_id=888,
        actor="maintainer",
        command="run benchmark decision full",
        pack="pr-full-decision",
        base_sha="a" * 40,
        candidate_sha="b" * 40,
        workflow_run_id=1,
        suites=[
            {
                "suite": "scan",
                "profile": "pr-macro",
                "timeout_minutes": 90,
                "artifact_name": "scan-artifact",
            }
        ],
    )

    with pytest.raises(ValueError, match="request_id=.*write_perf"):
        update_suite_status(
            db_path,
            request_id=request_id,
            suite="write_perf",
            status="running",
        )
