from __future__ import annotations

import os
import re
import subprocess
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPARE_BRANCH = REPO_ROOT / "scripts" / "compare_branch.sh"
PREPARE_DELTA_RS = REPO_ROOT / "scripts" / "prepare_delta_rs.sh"
LOCAL_CLEANUP = REPO_ROOT / "scripts" / "cleanup_local.sh"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark.yml"
NIGHTLY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark-nightly.yml"
PRERELEASE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark-prerelease.yml"


def test_compare_branch_sanitizes_branch_labels_for_cli() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "sanitize_label" in script
    assert re.search(
        r"base_label=\"base-\$\(sanitize_label \"\$\{base_ref\}\"\)\"", script
    )
    assert re.search(
        r"cand_label=\"cand-\$\(sanitize_label \"\$\{candidate_ref\}\"\)\"", script
    )


def test_benchmark_workflow_defines_job_timeout() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert re.search(r"^\s*timeout-minutes:\s*\d+", workflow, flags=re.MULTILINE)


def test_benchmark_workflow_enforces_suite_allowlist() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "allowedSuites" in workflow
    assert "optimize_vacuum" in workflow
    assert "invalid command" in workflow


def test_benchmark_workflow_uses_sha_pins_for_compare_refs() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert re.search(
        r"env:\n\s+BASE_SHA:\s+\$\{\{ steps\.pr\.outputs\.base_sha \}\}", workflow
    )
    assert re.search(
        r"\s+HEAD_SHA:\s+\$\{\{ steps\.pr\.outputs\.head_sha \}\}", workflow
    )
    assert re.search(r"\s+SUITE:\s+\$\{\{ steps\.parse\.outputs\.suite \}\}", workflow)
    assert '"$BASE_SHA"' in workflow
    assert '"$HEAD_SHA"' in workflow
    assert '"$SUITE"' in workflow


def test_compare_branch_supports_storage_backend_passthrough() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--storage-backend <local|s3>" in script
    assert "--storage-option <KEY=VALUE>" in script
    assert re.search(
        r"storage_args=\(--storage-backend \"\$\{STORAGE_BACKEND\}\"\)", script
    )
    assert re.search(r"storage_args\+=\(--storage-option \"\$\{option\}\"\)", script)
    assert re.search(r"\./scripts/bench\.sh data .*\"\$\{storage_args\[@\]\}\"", script)
    assert re.search(r"\./scripts/bench\.sh run .*\"\$\{storage_args\[@\]\}\"", script)


def test_compare_branch_does_not_retry_benchmark_producing_steps() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "run_step_no_retry()" in script
    assert re.search(
        r"run_step_no_retry env .*?/scripts/bench\.sh data --scale sf1 --seed 42",
        script,
    )
    assert (
        len(
            re.findall(
                r"run_step_no_retry env .*?/scripts/bench\.sh run --scale sf1", script
            )
        )
        == 2
    )


def test_compare_branch_cleans_untracked_harness_overlay_on_exit() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "cleanup_harness_overlay_untracked" in script
    assert re.search(
        r"git -C \"\$\{DELTA_RS_DIR\}\" clean -fd -- \"\$\{path\}\"", script
    )
    assert "crates/delta-bench" in script
    assert "bench/manifests" in script
    assert "backends" in script
    assert "python/delta_bench_interop" in script
    assert "python/delta_bench_tpcds" in script
    assert re.search(r"trap cleanup_harness_overlay_untracked EXIT", script)


def test_compare_branch_supports_aggregation_passthrough() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--aggregation <min|median|p95>" in script
    assert re.search(r"AGGREGATION=\"\$\{BENCH_AGGREGATION:-median\}\"", script)
    assert re.search(
        r"compare_args=\(--noise-threshold \"\$\{NOISE_THRESHOLD\}\" --aggregation \"\$\{AGGREGATION\}\" --format text\)",
        script,
    )


def test_compare_branch_emits_hash_policy_triage_report() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "delta_bench_compare.hash_policy" in script
    assert re.search(
        r"python3 -m delta_bench_compare\.hash_policy \"\$\{base_json\}\" \"\$\{cand_json\}\"",
        script,
    )


def test_compare_branch_supports_explicit_sha_flags() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--base-sha <sha>" in script
    assert "--candidate-sha <sha>" in script
    assert re.search(r"--base-sha\)\n\s+BASE_SHA_OVERRIDE=\"\$2\"", script)
    assert re.search(r"--candidate-sha\)\n\s+CANDIDATE_SHA_OVERRIDE=\"\$2\"", script)
    assert re.search(
        r"prepare_delta_rs_ref \"\$\{base_ref\}\" \"\$\{base_ref_mode\}\"", script
    )
    assert re.search(
        r"prepare_delta_rs_ref \"\$\{candidate_ref\}\" \"\$\{candidate_ref_mode\}\"",
        script,
    )
    assert re.search(r"if \[\[ \"\$\{mode\}\" == \"commit\" \]\]; then", script)


def test_compare_branch_supports_working_branch_vs_upstream_main_shortcut() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--current-vs-main" in script
    assert "--working-vs-upstream-main" in script
    assert "--upstream-remote <name>" in script
    assert re.search(r"WORKING_VS_UPSTREAM_MAIN=0", script)
    assert re.search(r"UPSTREAM_REMOTE_OVERRIDE=\"\"", script)
    assert re.search(
        r"if \(\( WORKING_VS_UPSTREAM_MAIN != 0 \)\); then[\s\S]*candidate_ref=\"\$\{working_head_sha\}\"",
        script,
    )
    assert re.search(
        r"if \(\( WORKING_VS_UPSTREAM_MAIN != 0 \)\); then[\s\S]*base_ref=\"\$\{upstream_main_sha\}\"",
        script,
    )
    assert re.search(
        r"git -C \"\$\{DELTA_RS_DIR\}\" fetch \"\$\{upstream_remote\}\" main",
        script,
    )


def test_compare_branch_prefers_existing_branch_refs_before_sha_fallback() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "branch_ref_exists" in script
    assert re.search(r"show-ref --verify --quiet \"refs/heads/\$\{ref\}\"", script)
    assert re.search(
        r"show-ref --verify --quiet \"refs/remotes/origin/\$\{ref\}\"", script
    )
    assert re.search(
        r"if branch_ref_exists \"\$\{ref\}\"; then\s+run_step env DELTA_RS_BRANCH=",
        script,
        flags=re.DOTALL,
    )
    assert re.search(
        r"if is_commit_sha \"\$\{ref\}\"; then\s+run_step env DELTA_RS_REF=",
        script,
        flags=re.DOTALL,
    )


def test_compare_branch_emits_clear_missing_ref_guidance() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "benchmark ref '${ref}' not found in delta-rs checkout" in script
    assert "--candidate-sha" in script
    assert 'git -C "${DELTA_RS_DIR}" branch -a' in script
    assert re.search(
        r"ensure_known_ref_mode \"\$\{candidate_ref\}\" \"\$\{candidate_ref_mode\}\"[\s\S]*prepare_delta_rs_ref \"\$\{base_ref\}\"",
        script,
    )


def test_prepare_delta_rs_supports_immutable_ref_checkout() -> None:
    script = PREPARE_DELTA_RS.read_text(encoding="utf-8")
    assert "DELTA_RS_REF" in script
    assert "DELTA_RS_REF_TYPE" in script
    assert "checkout --detach" in script
    assert "pull --ff-only origin" in script


def test_prepare_delta_rs_cleans_untracked_harness_overlay_before_checkout() -> None:
    script = PREPARE_DELTA_RS.read_text(encoding="utf-8")
    assert "cleanup_harness_overlay_untracked" in script
    assert re.search(
        r"git -C \"\$\{DELTA_RS_DIR\}\" clean -fd -- \"\$\{path\}\"", script
    )
    assert "crates/delta-bench" in script
    assert "bench/manifests" in script
    assert "backends" in script
    assert "python/delta_bench_interop" in script
    assert "python/delta_bench_tpcds" in script
    assert re.search(
        r"cleanup_harness_overlay_untracked\s+git -C \"\$\{DELTA_RS_DIR\}\" fetch origin",
        script,
    )


def test_benchmark_workflow_accepts_optional_storage_configuration() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "BENCH_STORAGE_BACKEND" in workflow
    assert "BENCH_STORAGE_OPTIONS" in workflow
    assert "storage_args=()" in workflow
    assert re.search(
        r"storage_args\+=\(--storage-backend \"\$\{BENCH_STORAGE_BACKEND\}\"\)",
        workflow,
    )
    assert re.search(r"storage_args\+=\(--storage-option \"\$\{opt\}\"\)", workflow)
    assert re.search(
        r"\./scripts/compare_branch\.sh \\\n(?:.*\n)*\s+\"\$\{storage_args\[@\]\}\" \\\n(?:.*\n)*\s+--base-sha \"\$BASE_SHA\" \\\n(?:.*\n)*\s+--candidate-sha \"\$HEAD_SHA\"",
        workflow,
    )


def test_compare_branch_does_not_expose_ci_policy_flags() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--ci" not in script
    assert "--max-allowed-regressions" not in script


def test_non_pr_workflows_do_not_use_advisory_mode_wording() -> None:
    nightly = NIGHTLY_WORKFLOW.read_text(encoding="utf-8")
    prerelease = PRERELEASE_WORKFLOW.read_text(encoding="utf-8")
    assert "advisory" not in nightly.lower()
    assert "advisory" not in prerelease.lower()


def test_bench_wrapper_help_is_structured_and_readable() -> None:
    result = subprocess.run(
        ["bash", str(REPO_ROOT / "scripts" / "bench.sh"), "--help"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    assert "Usage:" in result.stdout
    assert "Commands:" in result.stdout
    assert "Data command options:" in result.stdout
    assert "Run command options:" in result.stdout
    assert "--no-summary-table" in result.stdout
    assert "-h, --help" in result.stdout


def test_bench_wrapper_supports_no_summary_table_passthrough() -> None:
    script = (REPO_ROOT / "scripts" / "bench.sh").read_text(encoding="utf-8")
    assert "--no-summary-table" in script
    assert re.search(r"--no-summary-table\)\s+no_summary_table=1; shift 1 ;;", script)
    assert re.search(
        r"if \(\( no_summary_table != 0 \)\); then\s+run_args\+=\(--no-summary-table\)",
        script,
    )


def test_bench_wrapper_suppresses_rust_warnings_by_default() -> None:
    script = (REPO_ROOT / "scripts" / "bench.sh").read_text(encoding="utf-8")
    assert re.search(
        r'DELTA_BENCH_SUPPRESS_RUST_WARNINGS="\$\{DELTA_BENCH_SUPPRESS_RUST_WARNINGS:-1\}"',
        script,
    )
    assert 'RUSTFLAGS="${RUSTFLAGS:-} -Awarnings"' in script
    assert "--quiet -p delta-bench --" in script


def test_cleanup_local_help_lists_all_flags() -> None:
    result = subprocess.run(
        ["bash", str(LOCAL_CLEANUP), "--help"],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0
    for flag in (
        "--apply",
        "--dry-run",
        "--results",
        "--fixtures",
        "--delta-rs-under-test",
        "--keep-last <N>",
        "--older-than-days <N>",
        "--help",
    ):
        assert flag in result.stdout


def test_cleanup_local_defaults_to_dry_run() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        results_dir = root / "results"
        label_dir = results_dir / "old-run"
        label_dir.mkdir(parents=True)
        result_file = label_dir / "scan.json"
        result_file.write_text("{}", encoding="utf-8")

        env = os.environ.copy()
        env["DELTA_BENCH_RESULTS"] = str(results_dir)

        result = subprocess.run(
            ["bash", str(LOCAL_CLEANUP), "--results"],
            cwd=REPO_ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "Mode: dry-run" in result.stdout
        assert "DRY-RUN: rm -rf" in result.stdout
        assert result_file.exists()


def test_cleanup_local_requires_apply_for_deletion() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        results_dir = root / "results"
        label_dir = results_dir / "run-a"
        label_dir.mkdir(parents=True)
        marker = label_dir / "scan.json"
        marker.write_text("{}", encoding="utf-8")

        env = os.environ.copy()
        env["DELTA_BENCH_RESULTS"] = str(results_dir)

        dry_run = subprocess.run(
            ["bash", str(LOCAL_CLEANUP), "--results"],
            cwd=REPO_ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        assert dry_run.returncode == 0
        assert marker.exists()

        apply_run = subprocess.run(
            ["bash", str(LOCAL_CLEANUP), "--apply", "--results"],
            cwd=REPO_ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
        assert apply_run.returncode == 0
        assert "Mode: apply" in apply_run.stdout
        assert "APPLY: rm -rf" in apply_run.stdout
        assert not label_dir.exists()
