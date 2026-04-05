from __future__ import annotations

import os
import subprocess
import textwrap
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[2]
METHODS_DIR = REPO_ROOT / "bench" / "methodologies"
RUN_PROFILE = REPO_ROOT / "scripts" / "run_profile.sh"


def test_run_profile_is_present_and_executable() -> None:
    assert RUN_PROFILE.exists()
    assert os.access(RUN_PROFILE, os.X_OK)


def test_run_profile_dry_run_prints_resolved_compare_command() -> None:
    result = subprocess.run(
        [str(RUN_PROFILE), "--dry-run", "--current-vs-main", "pr-macro"],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.strip() == (
        "./scripts/compare_branch.sh --current-vs-main "
        "--methodology-profile pr-macro scan"
    )


def test_run_profile_dry_run_resolves_run_profiles_to_bench_sh() -> None:
    profile_name = "test-run-profile"
    profile_path = METHODS_DIR / f"{profile_name}.env"
    profile_path.write_text(
        textwrap.dedent(
            """
            METHODOLOGY_PROFILE=test-run-profile
            METHODOLOGY_VERSION=1
            PROFILE_KIND=run
            TARGET=merge
            RUNNER=rust
            LANE=correctness
            MODE=assert
            DATASET_ID=tiny_smoke
            TIMING_PHASE=execute
            WARMUP=1
            ITERS=1
            STORAGE_BACKEND=local
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    try:
        result = subprocess.run(
            [str(RUN_PROFILE), "--dry-run", profile_name],
            check=False,
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )
    finally:
        profile_path.unlink(missing_ok=True)

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.strip() == (
        "./scripts/bench.sh run --suite merge --runner rust "
        "--lane correctness --mode assert --dataset-id tiny_smoke "
        "--timing-phase execute --warmup 1 --iters 1 --storage-backend local"
    )


def test_run_profile_dry_run_uses_criterion_metadata() -> None:
    profile_name = "test-criterion-profile"
    profile_path = METHODS_DIR / f"{profile_name}.env"
    profile_path.write_text(
        textwrap.dedent(
            """
            METHODOLOGY_PROFILE=test-criterion-profile
            METHODOLOGY_VERSION=1
            PROFILE_KIND=criterion
            TARGET=scan
            CRITERION_BENCH=scan_phase_bench
            CRITERION_FILTER=scan_filter_flag
            """
        ).strip()
        + "\n",
        encoding="utf-8",
    )
    try:
        result = subprocess.run(
            [str(RUN_PROFILE), "--dry-run", profile_name],
            check=False,
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )
    finally:
        profile_path.unlink(missing_ok=True)

    assert result.returncode == 0
    assert result.stderr == ""
    assert (
        result.stdout.strip()
        == "cargo bench -p delta-bench --bench scan_phase_bench -- scan_filter_flag"
    )


@pytest.mark.parametrize(
    ("profile_name", "expected_command"),
    [
        (
            "scan-phase-criterion",
            "cargo bench -p delta-bench --bench scan_phase_bench",
        ),
        (
            "metadata-replay-criterion",
            "cargo bench -p delta-bench --bench scan_replay_bench",
        ),
    ],
)
def test_run_profile_dry_run_resolves_committed_criterion_profiles(
    profile_name: str,
    expected_command: str,
) -> None:
    result = subprocess.run(
        [str(RUN_PROFILE), "--dry-run", profile_name],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )

    assert result.returncode == 0
    assert result.stderr == ""
    assert result.stdout.strip() == expected_command


def test_run_profile_rejects_missing_required_run_metadata() -> None:
    for missing_field in ("RUNNER", "LANE", "MODE"):
        profile_name = f"test-run-profile-missing-{missing_field.lower()}"
        profile_path = METHODS_DIR / f"{profile_name}.env"
        lines = [
            f"METHODOLOGY_PROFILE={profile_name}",
            "METHODOLOGY_VERSION=1",
            "PROFILE_KIND=run",
            "TARGET=merge",
            "RUNNER=rust",
            "LANE=correctness",
            "MODE=assert",
        ]
        profile_path.write_text(
            "\n".join(
                line for line in lines if not line.startswith(f"{missing_field}=")
            )
            + "\n",
            encoding="utf-8",
        )
        try:
            result = subprocess.run(
                [str(RUN_PROFILE), "--dry-run", profile_name],
                check=False,
                capture_output=True,
                text=True,
                cwd=REPO_ROOT,
            )
        finally:
            profile_path.unlink(missing_ok=True)

        assert result.returncode != 0
        assert result.stdout == ""
        assert f"missing {missing_field}" in result.stderr
