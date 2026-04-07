from __future__ import annotations

import re
import subprocess
import textwrap
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
METHODS_DIR = REPO_ROOT / "bench" / "methodologies"
COMPARE_BRANCH = REPO_ROOT / "scripts" / "compare_branch.sh"
PR_MACRO = METHODS_DIR / "pr-macro.env"


def _write_profile(name: str, body: str) -> Path:
    path = METHODS_DIR / f"{name}.env"
    path.write_text(textwrap.dedent(body).strip() + "\n", encoding="utf-8")
    return path


def _extract_function(script: str, name: str) -> str:
    pattern = re.compile(rf"(?ms)^{re.escape(name)}\(\) \{{\n.*?^\}}\n")
    match = pattern.search(script)
    assert match is not None, f"missing function {name}"
    return match.group(0)


def _compare_branch_runtime_script(function_names: list[str], body: str) -> str:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    functions = "\n".join(_extract_function(script, name) for name in function_names)
    runtime = "\n".join(
        [
            "set -euo pipefail",
            f'REPO_ROOT="{REPO_ROOT}"',
            'ROOT_DIR="${REPO_ROOT}"',
            'METHODOLOGY_PROFILE=""',
            'METHODOLOGY_VERSION=""',
            'DATASET_ID=""',
            'PROFILE_KIND=""',
            'TARGET=""',
            'COMPARE_MODE=""',
            'STORAGE_BACKEND=""',
            'BACKEND_PROFILE=""',
            'WARMUP=""',
            'ITERS=""',
            'PREWARM_ITERS=""',
            'COMPARE_RUNS=""',
            'MEASURE_ORDER=""',
            'TIMING_PHASE=""',
            'AGGREGATION=""',
            'DATASET_POLICY=""',
            'SPREAD_METRIC=""',
            'SUB_MS_THRESHOLD_MS=""',
            'SUB_MS_POLICY=""',
            'COMPARE_MODE_EXPLICIT=0',
            'STORAGE_BACKEND_EXPLICIT=0',
            'BACKEND_PROFILE_EXPLICIT=0',
            'BENCH_WARMUP_EXPLICIT=0',
            'BENCH_ITERS_EXPLICIT=0',
            'BENCH_PREWARM_ITERS_EXPLICIT=0',
            'BENCH_COMPARE_RUNS_EXPLICIT=0',
            'BENCH_MEASURE_ORDER_EXPLICIT=0',
            'TIMING_PHASE_EXPLICIT=0',
            'AGGREGATION_EXPLICIT=0',
            'DATASET_ID_EXPLICIT=0',
            functions,
            body,
        ]
    )
    return runtime


def _run_compare_branch(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [str(COMPARE_BRANCH), *args],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )


def test_pr_macro_profile_preserves_live_contract_values() -> None:
    result = subprocess.run(
        [
            "bash",
            "-lc",
            "set -euo pipefail; "
            "source bench/methodologies/pr-macro.env; "
            "printf '%s\\n' "
            '"${METHODOLOGY_VERSION}" "${DATASET_ID}" "${ITERS}" '
            '"${COMPARE_RUNS}" "${PROFILE_KIND}" "${TARGET}"',
        ],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )

    assert result.returncode == 0
    assert result.stdout.splitlines() == [
        "2",
        "medium_selective",
        "15",
        "7",
        "compare",
        "scan",
    ]


def test_compare_branch_rejects_non_compare_profile_kinds() -> None:
    profile_path = _write_profile(
        "test-run-profile",
        """
        METHODOLOGY_PROFILE=test-run-profile
        METHODOLOGY_VERSION=1
        PROFILE_KIND=run
        TARGET=scan
        """,
    )
    try:
        result = _run_compare_branch(
            "--current-vs-main",
            "--methodology-profile",
            "test-run-profile",
        )
    finally:
        profile_path.unlink(missing_ok=True)

    assert result.returncode != 0
    assert "rejects run or criterion profiles" in result.stderr


def test_compare_branch_honors_profile_dataset_id() -> None:
    runtime = _compare_branch_runtime_script(
        [
            "methodology_profile_path",
            "methodology_profile_names",
            "load_methodology_profile",
            "build_resolved_methodology_settings",
            "resolved_methodology_setting",
            "build_manifest_methodology_args",
        ],
        "\n".join(
            [
                "load_methodology_profile pr-macro",
                "build_resolved_methodology_settings",
                "build_manifest_methodology_args",
                "printf '%s\\n' \"$(resolved_methodology_setting dataset_id)\" \"${manifest_methodology_args[@]}\"",
            ]
        ),
    )
    result = subprocess.run(
        ["bash", "-lc", runtime],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )

    assert result.returncode == 0
    assert result.stdout.splitlines() == [
        "medium_selective",
        "--methodology-compare-mode",
        "decision",
        "--methodology-warmup",
        "2",
        "--methodology-iters",
        "15",
        "--methodology-prewarm-iters",
        "1",
        "--methodology-compare-runs",
        "7",
        "--methodology-measure-order",
        "alternate",
        "--methodology-timing-phase",
        "execute",
        "--methodology-aggregation",
        "median",
        "--methodology-profile",
        "pr-macro",
        "--methodology-version",
        "2",
        "--methodology-dataset-id",
        "medium_selective",
        "--methodology-dataset-policy",
        "shared_run_scope",
        "--methodology-spread-metric",
        "iqr_ms",
        "--methodology-sub-ms-threshold-ms",
        "1.0",
        "--methodology-sub-ms-policy",
        "micro_only",
    ]


def test_compare_branch_rejects_suite_target_mismatch() -> None:
    profile_path = _write_profile(
        "test-compare-profile",
        """
        METHODOLOGY_PROFILE=test-compare-profile
        METHODOLOGY_VERSION=1
        PROFILE_KIND=compare
        TARGET=scan
        """,
    )
    try:
        result = _run_compare_branch(
            "--current-vs-main",
            "--methodology-profile",
            "test-compare-profile",
            "write_perf",
        )
    finally:
        profile_path.unlink(missing_ok=True)

    assert result.returncode != 0
    assert "does not match methodology profile" in result.stderr


def test_compare_branch_rejects_missing_methodology_version() -> None:
    profile_path = _write_profile(
        "test-missing-version",
        """
        METHODOLOGY_PROFILE=test-missing-version
        PROFILE_KIND=compare
        TARGET=scan
        """,
    )
    try:
        runtime = _compare_branch_runtime_script(
            ["methodology_profile_path", "methodology_profile_names", "load_methodology_profile"],
            "load_methodology_profile test-missing-version",
        )
        result = subprocess.run(
            ["bash", "-lc", runtime],
            check=False,
            capture_output=True,
            text=True,
            cwd=REPO_ROOT,
        )
    finally:
        profile_path.unlink(missing_ok=True)

    assert result.returncode != 0
    assert "missing METHODOLOGY_VERSION" in result.stderr


def test_compare_branch_honors_profile_storage_defaults() -> None:
    runtime = _compare_branch_runtime_script(
        [
            "methodology_profile_path",
            "methodology_profile_names",
            "load_methodology_profile",
            "build_resolved_methodology_settings",
            "resolved_methodology_setting",
            "build_manifest_methodology_args",
        ],
        "\n".join(
            [
                "load_methodology_profile scan-s3-candidate",
                "build_resolved_methodology_settings",
                "build_manifest_methodology_args",
                "printf '%s\\n' "
                '"$(resolved_methodology_setting storage_backend)" '
                '"$(resolved_methodology_setting backend_profile)" '
                '"${STORAGE_BACKEND}" '
                '"${BACKEND_PROFILE}" '
                '"${manifest_methodology_args[@]}"',
            ]
        ),
    )
    result = subprocess.run(
        ["bash", "-lc", runtime],
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
    )

    assert result.returncode == 0
    assert result.stdout.splitlines() == [
        "s3",
        "s3_locking_vultr",
        "s3",
        "s3_locking_vultr",
        "--methodology-compare-mode",
        "decision",
        "--methodology-warmup",
        "2",
        "--methodology-iters",
        "15",
        "--methodology-prewarm-iters",
        "1",
        "--methodology-compare-runs",
        "7",
        "--methodology-measure-order",
        "alternate",
        "--methodology-timing-phase",
        "execute",
        "--methodology-aggregation",
        "median",
        "--methodology-profile",
        "scan-s3-candidate",
        "--methodology-version",
        "1",
        "--methodology-dataset-id",
        "medium_selective",
        "--methodology-dataset-policy",
        "shared_run_scope",
        "--methodology-spread-metric",
        "iqr_ms",
        "--methodology-sub-ms-threshold-ms",
        "1.0",
        "--methodology-sub-ms-policy",
        "micro_only",
        "--methodology-storage-backend",
        "s3",
        "--methodology-backend-profile",
        "s3_locking_vultr",
    ]
