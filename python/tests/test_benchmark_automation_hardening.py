from __future__ import annotations

import json
import os
import re
import shlex
import shutil
import subprocess
import tempfile
import time
from collections.abc import Callable
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPARE_BRANCH = REPO_ROOT / "scripts" / "compare_branch.sh"
BENCH_SH = REPO_ROOT / "scripts" / "bench.sh"
PREPARE_DELTA_RS = REPO_ROOT / "scripts" / "prepare_delta_rs.sh"
LOCAL_CLEANUP = REPO_ROOT / "scripts" / "cleanup_local.sh"
GITIGNORE = REPO_ROOT / ".gitignore"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark.yml"
NIGHTLY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark-nightly.yml"
PRERELEASE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark-prerelease.yml"
VALIDATION_SCRIPT = REPO_ROOT / "scripts" / "validate_perf_harness.sh"
VALIDATION_DOC = REPO_ROOT / "docs" / "validation.md"
REFERENCE_DOC = REPO_ROOT / "docs" / "reference.md"
GETTING_STARTED_DOC = REPO_ROOT / "docs" / "getting-started.md"
SCAN_PHASE_BENCH = (
    REPO_ROOT / "crates" / "delta-bench" / "benches" / "scan_phase_bench.rs"
)
LONGITUDINAL_NIGHTLY_WORKFLOW = (
    REPO_ROOT / ".github" / "workflows" / "longitudinal-nightly.yml"
)
LONGITUDINAL_RELEASE_HISTORY_WORKFLOW = (
    REPO_ROOT / ".github" / "workflows" / "longitudinal-release-history.yml"
)
LABEL_CONTRACT = REPO_ROOT / "python" / "tests" / "fixtures" / "label_contract.json"


def wait_for_condition(predicate: Callable[[], bool], timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


def symlink_command(fake_bin: Path, name: str) -> None:
    resolved = shutil.which(name)
    assert resolved is not None, f"missing system command for test: {name}"
    (fake_bin / name).symlink_to(resolved)


def write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def copy_executable(source: Path, dest: Path) -> None:
    dest.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    dest.chmod(0o755)


def assert_order(content: str, earlier: str, later: str) -> None:
    assert earlier in content, f"missing earlier marker: {earlier}"
    assert later in content, f"missing later marker: {later}"
    assert content.index(earlier) < content.index(later)


def test_compare_branch_sanitizes_branch_labels_for_cli() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "sanitize_label" in script
    assert re.search(
        r"base_label=\"base-\$\(sanitize_label \"\$\{base_ref\}\"\)\"", script
    )
    assert re.search(
        r"cand_label=\"cand-\$\(sanitize_label \"\$\{candidate_ref\}\"\)\"", script
    )


def test_bench_sh_usage_lists_write_perf_suite() -> None:
    script = BENCH_SH.read_text(encoding="utf-8")
    assert "--suite <scan|write|write_perf|" in script


def test_bench_sh_defaults_run_lane_to_smoke() -> None:
    script = BENCH_SH.read_text(encoding="utf-8")
    assert 'lane="smoke"' in script
    assert "--lane <smoke|correctness|macro>" in script
    assert re.search(r'run_args=.*--lane "\$\{lane\}"', script, flags=re.DOTALL)


def test_compare_branch_label_contract_matches_shared_fixture() -> None:
    contract = json.loads(LABEL_CONTRACT.read_text(encoding="utf-8"))
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    start = script.index("sanitize_label() {")
    end = script.index("\n}\n\nis_positive_integer", start) + 2
    function_body = script[start:end]
    runner = f'set -euo pipefail\n{function_body}\nsanitize_label "$1"\n'

    for raw, expected in contract["sanitized"].items():
        result = subprocess.run(
            ["bash", "-c", runner, "sanitize_label_test", raw],
            check=True,
            capture_output=True,
            text=True,
        )
        assert result.stdout == expected


def test_benchmark_workflow_defines_job_timeout() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert re.search(r"^\s*timeout-minutes:\s*\d+", workflow, flags=re.MULTILINE)


def test_benchmark_workflow_enforces_suite_allowlist() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "allowedSuites" in workflow
    match = re.search(
        r"const allowedSuites = new Set\(\[(?P<body>.*?)\]\);",
        workflow,
        flags=re.DOTALL,
    )
    assert match is not None
    allowlist = match.group("body")
    assert '"scan"' in allowlist
    assert '"all"' not in allowlist
    assert '"write"' not in allowlist
    assert "invalid command" in workflow


def test_benchmark_workflow_supports_decision_command_grammar() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "run benchmark decision" in workflow
    assert (
        "firstLine.match(/^run benchmark(?:\\s+(decision))?\\s+(\\S+)$/i)" in workflow
    )


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


def test_benchmark_workflows_run_security_preflight_before_compare_execution() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert_order(
        workflow,
        "- name: Enforce runner security preflight",
        "- name: Run branch compare",
    )

    prerelease = PRERELEASE_WORKFLOW.read_text(encoding="utf-8")
    assert_order(
        prerelease,
        "- name: Enforce runner security preflight",
        "- name: Run branch comparison",
    )


def test_longitudinal_workflows_run_security_preflight_before_checkout_prep() -> None:
    nightly = LONGITUDINAL_NIGHTLY_WORKFLOW.read_text(encoding="utf-8")
    assert_order(
        nightly,
        "- name: Enforce runner security preflight",
        "- name: Prepare delta-rs checkout",
    )
    assert_order(
        nightly,
        "- name: Enforce runner security preflight",
        "- name: Build missing artifacts",
    )

    release_history = LONGITUDINAL_RELEASE_HISTORY_WORKFLOW.read_text(encoding="utf-8")
    assert_order(
        release_history,
        "- name: Enforce runner security preflight",
        "- name: Prepare delta-rs checkout",
    )
    assert_order(
        release_history,
        "- name: Enforce runner security preflight",
        "- name: Build missing artifacts",
    )


def test_compare_branch_supports_storage_backend_passthrough() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--storage-backend <local|s3>" in script
    assert "--storage-option <KEY=VALUE>" in script
    assert re.search(
        r"storage_args=\(--storage-backend \"\$\{STORAGE_BACKEND\}\"\)", script
    )
    assert re.search(r"storage_args\+=\(--storage-option \"\$\{option\}\"\)", script)
    assert re.search(
        r"data_cmd=\(\./scripts/bench\.sh data --scale sf1 --seed 42\)", script
    )
    assert re.search(r"data_cmd\+=\(\"\\?\$\{storage_args\[@\]\}\"\)", script)
    assert re.search(r"run_cmd\+=\(\"\\?\$\{storage_args\[@\]\}\"\)", script)


def test_compare_branch_supports_dataset_and_timing_phase_passthrough() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--mode <perf|assert>" in script
    assert "--dataset-id <id>" in script
    assert "--timing-phase <phase>" in script
    assert re.search(r'BENCHMARK_MODE="\$\{BENCH_BENCHMARK_MODE:-perf\}"', script)
    assert re.search(r'DATASET_ID="\$\{BENCH_DATASET_ID:-\}"', script)
    assert re.search(r'TIMING_PHASE="\$\{BENCH_TIMING_PHASE:-execute\}"', script)
    assert re.search(r'--mode "\$\{BENCHMARK_MODE\}"', script)
    assert re.search(r'run_cmd\+=\(--dataset-id "\$\{DATASET_ID\}"\)', script)
    assert re.search(r'run_cmd\+=\(--timing-phase "\$\{TIMING_PHASE\}"\)', script)
    assert re.search(r'data_cmd\+=\(--dataset-id "\$\{DATASET_ID\}"\)', script)


def test_compare_branch_does_not_retry_benchmark_producing_steps() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "run_step_no_retry()" in script
    assert "data_cmd=(./scripts/bench.sh data --scale sf1 --seed 42)" in script
    assert re.search(r'run_step_no_retry env .*?"\$\{data_cmd\[@\]\}"', script)
    assert "run_benchmark_suite_for_ref" in script
    assert "./scripts/bench.sh run --scale sf1" in script


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
        r"compare_args=\(--mode \"\$\{COMPARE_MODE\}\" --noise-threshold \"\$\{NOISE_THRESHOLD\}\" --aggregation \"\$\{AGGREGATION\}\" --format text\)",
        script,
    )


def test_compare_branch_defaults_to_exploratory_mode_curated_scan_and_macro_lane() -> (
    None
):
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--compare-mode <exploratory|decision>" in script
    assert re.search(r'COMPARE_MODE="\$\{BENCH_COMPARE_MODE:-exploratory\}"', script)
    assert re.search(r'suite="\$\{positional_refs\[2\]:-scan\}"', script)
    assert re.search(r'suite="\$\{positional_refs\[0\]:-scan\}"', script)
    assert re.search(r"run_cmd=\(\./scripts/bench\.sh run .* --lane macro", script)
    assert re.search(
        r'compare_args=\(--mode "\$\{COMPARE_MODE\}" --noise-threshold "\$\{NOISE_THRESHOLD\}" --aggregation "\$\{AGGREGATION\}" --format text\)',
        script,
    )


def test_compare_branch_supports_fail_on_passthrough() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--fail-on <statuses>" in script
    assert re.search(r'COMPARE_FAIL_ON="\$\{BENCH_COMPARE_FAIL_ON:-\}"', script)
    assert re.search(r'compare_args\+=\(--fail-on "\$\{COMPARE_FAIL_ON\}"\)', script)


def test_perf_validation_workflow_entrypoint_exists_and_is_executable() -> None:
    assert VALIDATION_SCRIPT.exists(), "missing scripts/validate_perf_harness.sh"
    assert (
        VALIDATION_SCRIPT.stat().st_mode & 0o111
    ), "scripts/validate_perf_harness.sh must be executable"


def test_validation_script_exposes_artifact_dir_contract() -> None:
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")
    assert "--artifact-dir <path>" in script
    assert "SUMMARY_FILE" in script
    assert "summary.md" in script


def test_validation_script_covers_all_scan_phase_canaries() -> None:
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")

    for phase, env_var, control_phase in (
        ("load", "DELTA_BENCH_SCAN_DELAY_LOAD_MS", "execute"),
        ("plan", "DELTA_BENCH_SCAN_DELAY_PLAN_MS", "execute"),
        ("validate", "DELTA_BENCH_SCAN_DELAY_VALIDATE_MS", "execute"),
        ("execute", "DELTA_BENCH_SCAN_DELAY_EXECUTE_MS", "plan"),
    ):
        assert f"canary-{phase}-baseline" in script
        assert f"canary-{phase}-delayed" in script
        assert env_var in script
        assert f'"{phase}"' in script
        assert f'"{control_phase}"' in script


def test_validation_script_canonicalizes_artifact_dir_once() -> None:
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")
    assert "canonicalize_dir()" in script
    assert "pwd -P" in script
    assert (
        'VALIDATION_ARTIFACT_DIR="$(canonicalize_dir "${VALIDATION_ARTIFACT_DIR}")"'
        in script
    )


def test_validation_script_keeps_assert_failures_fail_closed() -> None:
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")
    assert 'note "$(assert_' not in script


def test_validation_docs_and_readme_point_to_perf_validation_workflow() -> None:
    readme = REPO_ROOT.joinpath("README.md").read_text(encoding="utf-8")
    compare_doc = REPO_ROOT.joinpath("docs", "comparing-branches.md").read_text(
        encoding="utf-8"
    )

    assert "docs/validation.md" in readme
    assert "./scripts/validate_perf_harness.sh" in readme
    assert "docs/validation.md" in compare_doc
    assert "./scripts/validate_perf_harness.sh" in compare_doc


def test_bench_docs_explain_harness_root_path_resolution_when_exec_root_differs() -> (
    None
):
    reference = REFERENCE_DOC.read_text(encoding="utf-8")
    getting_started = GETTING_STARTED_DOC.read_text(encoding="utf-8")

    assert (
        "Relative `DELTA_BENCH_FIXTURES` and `DELTA_BENCH_RESULTS` values are "
        "resolved against the harness repository root before `bench.sh` switches "
        "into `DELTA_BENCH_EXEC_ROOT`."
    ) in reference
    assert (
        "Relative `DELTA_BENCH_FIXTURES` and `DELTA_BENCH_RESULTS` values still "
        "stay anchored to this harness repository, even when "
        "`DELTA_BENCH_EXEC_ROOT` points at a separate delta-rs checkout."
    ) in getting_started


def test_scan_phase_criterion_bench_covers_multiple_pr_sensitive_cases() -> None:
    bench = SCAN_PHASE_BENCH.read_text(encoding="utf-8")
    for case_name in ("scan_filter_flag", "scan_projection_region", "scan_pruning_hit"):
        assert case_name in bench, f"criterion bench missing {case_name}"


def test_benchmark_workflow_reports_execution_status_separately_from_compare_mode() -> (
    None
):
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "status_label=EXPLORATORY" not in workflow
    assert "Benchmark EXPLORATORY" not in workflow
    assert "--compare-mode decision" in workflow
    assert "--fail-on regression,inconclusive" in workflow
    assert "status_label=PASS" in workflow
    assert "status_label=FAIL" in workflow
    assert "`Compare mode: ${compareMode}`" in workflow


def test_benchmark_workflow_fails_job_when_compare_fails() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert 'if [[ "${status}" -ne 0 ]]; then' in workflow
    assert 'exit "${status}"' in workflow


def test_benchmark_workflow_posts_results_even_after_compare_step_failure() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert (
        "if: always() && steps.parse.outputs.mode == 'run' && "
        "steps.auth.outputs.allowed == 'true' && "
        "steps.pr.outputs.same_repo == 'true'"
    ) in workflow


def test_benchmark_workflow_does_not_mask_exploratory_failures() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert (
        'if [[ "${compare_mode}" == "exploratory" ]]; then\n'
        '            echo "status_label=EXPLORATORY" >> "$GITHUB_OUTPUT"'
    ) not in workflow
    assert (
        'const heading = compareMode === "exploratory"\n'
        '              ? "### Benchmark EXPLORATORY"'
    ) not in workflow


def test_benchmark_nightly_is_explicit_macro_lane_for_curated_scan_only() -> None:
    workflow = NIGHTLY_WORKFLOW.read_text(encoding="utf-8")
    assert "--suite scan" in workflow
    assert "--lane macro" in workflow
    assert "--suite all" not in workflow
    assert "--suite write" not in workflow


def test_benchmark_prerelease_is_curated_to_scan_only() -> None:
    workflow = PRERELEASE_WORKFLOW.read_text(encoding="utf-8")
    assert re.search(r"default:\s*scan", workflow)
    assert "default: all" not in workflow
    assert "options:" in workflow
    assert re.search(r"options:\n(?:\s+- .+\n)*\s+- scan\b", workflow)
    assert "- python" not in workflow


def test_longitudinal_nightly_is_explicit_macro_lane_for_curated_scan_only() -> None:
    workflow = LONGITUDINAL_NIGHTLY_WORKFLOW.read_text(encoding="utf-8")
    assert "--suite scan" in workflow
    assert "--lane macro" in workflow
    assert "--suite write" not in workflow
    assert "--suite merge" not in workflow


def test_longitudinal_release_history_is_explicit_macro_lane_for_curated_scan_only() -> (
    None
):
    workflow = LONGITUDINAL_RELEASE_HISTORY_WORKFLOW.read_text(encoding="utf-8")
    assert "--suite scan" in workflow
    assert "--lane macro" in workflow
    assert "--suite write" not in workflow
    assert "--suite merge" not in workflow


def test_compare_branch_supports_reliable_multi_run_controls() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--warmup <N>" in script
    assert "--iters <N>" in script
    assert "--prewarm-iters <N>" in script
    assert "--compare-runs <N>" in script
    assert "--measure-order <base-first|candidate-first|alternate>" in script
    assert re.search(r'BENCH_WARMUP="\$\{BENCH_WARMUP:-\d+\}"', script)
    assert re.search(r'BENCH_ITERS="\$\{BENCH_ITERS:-\d+\}"', script)
    assert re.search(r'BENCH_PREWARM_ITERS="\$\{BENCH_PREWARM_ITERS:-\d+\}"', script)
    assert re.search(r'BENCH_COMPARE_RUNS="\$\{BENCH_COMPARE_RUNS:-\d+\}"', script)
    assert re.search(
        r'BENCH_MEASURE_ORDER="\$\{BENCH_MEASURE_ORDER:-alternate\}"', script
    )
    assert "python3 -m delta_bench_compare.aggregate" in script


def test_compare_branch_runs_security_preflight_before_initial_checkout_prep() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    start = script.index(
        'phase "${current_phase}" "${total_phases}" "Preparing delta-rs checkout and fixtures"'
    )
    end = script.index(
        'ensure_known_ref_mode "${candidate_ref}" "${candidate_ref_mode}"', start
    )
    initial_block = script[start:end]
    assert_order(
        initial_block,
        "run_security_check",
        'if ! exec_on_runner test -d "${DELTA_RS_DIR}/.git"; then',
    )
    assert (
        'run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/prepare_delta_rs.sh'
        in initial_block
    )


def test_compare_branch_does_not_fast_forward_default_branch_before_ref_pinning() -> (
    None
):
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    start = script.index(
        'phase "${current_phase}" "${total_phases}" "Preparing delta-rs checkout and fixtures"'
    )
    end = script.index(
        'base_ref="$(pin_ref_to_commit "${base_ref}" "${base_ref_mode}")"', start
    )
    initial_block = script[start:end]
    assert re.search(
        r'if ! exec_on_runner test -d "\$\{DELTA_RS_DIR\}/\.git"; then\s+run_step env DELTA_RS_DIR="\$\{DELTA_RS_DIR\}" \./scripts/prepare_delta_rs\.sh\s+fi',
        initial_block,
    )
    assert (
        'run_step env DELTA_RS_DIR="${DELTA_RS_DIR}" ./scripts/prepare_delta_rs.sh\n'
        'ensure_known_ref_mode "${base_ref}" "${base_ref_mode}"' not in initial_block
    )


def test_compare_branch_runs_security_preflight_before_per_ref_checkout() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    start = script.index("run_benchmark_suite_for_ref() {")
    end = script.index("\n}\n\nrun_order_for_iteration", start) + 2
    function_body = script[start:end]
    assert_order(
        function_body,
        "run_security_check",
        'prepare_delta_rs_ref "${ref}" "${mode}"',
    )


def test_compare_branch_pins_refs_once_before_labels_and_measured_runs() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "pin_ref_to_commit()" in script
    assert re.search(
        r'base_ref="\$\(pin_ref_to_commit \"\$\{base_ref\}\" \"\$\{base_ref_mode\}\"\)"',
        script,
    )
    assert re.search(
        r'candidate_ref="\$\(pin_ref_to_commit \"\$\{candidate_ref\}\" \"\$\{candidate_ref_mode\}\"\)"',
        script,
    )
    assert_order(
        script,
        'base_ref="$(pin_ref_to_commit "${base_ref}" "${base_ref_mode}")"',
        'base_label="base-$(sanitize_label "${base_ref}")"',
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
        r"run_benchmark_suite_for_ref \"\$\{candidate_ref\}\" \"\$\{candidate_ref_mode\}\"",
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
    assert re.search(r"checkout(?: -q)? --detach", script)
    assert re.search(r"pull(?: -q)? --ff-only origin", script)


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


def test_compare_branch_defaults_checkout_lock_outside_managed_checkout() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "default_checkout_lock_file()" in script
    assert re.search(
        r'DELTA_BENCH_CHECKOUT_LOCK_FILE="\$\{DELTA_BENCH_CHECKOUT_LOCK_FILE:-\$\(default_checkout_lock_file \"\$\{DELTA_RS_DIR\}\"\)\}"',
        script,
    )
    assert 'dirname "${checkout_dir}"' in script
    assert 'basename "${checkout_dir}"' in script
    assert ".delta_bench_checkout.lock" in script


def test_prepare_delta_rs_defaults_checkout_lock_outside_managed_checkout() -> None:
    script = PREPARE_DELTA_RS.read_text(encoding="utf-8")
    assert "default_checkout_lock_file()" in script
    assert re.search(
        r'DELTA_BENCH_CHECKOUT_LOCK_FILE="\$\{DELTA_BENCH_CHECKOUT_LOCK_FILE:-\$\(default_checkout_lock_file \"\$\{DELTA_RS_DIR\}\"\)\}"',
        script,
    )
    assert 'dirname "${checkout_dir}"' in script
    assert 'basename "${checkout_dir}"' in script
    assert ".delta_bench_checkout.lock" in script


def test_prepare_delta_rs_initial_clone_locking() -> None:
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        prepare_copy = scripts_dir / "prepare_delta_rs.sh"
        prepare_copy.write_text(
            PREPARE_DELTA_RS.read_text(encoding="utf-8"), encoding="utf-8"
        )
        prepare_copy.chmod(0o755)

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
        fake_git = fake_bin / "git"
        clone_log = temp_root / "clone.log"
        release_clone = temp_root / "release-clone"
        lock_wait_log = temp_root / "lock-wait.log"
        for command in (
            "bash",
            "basename",
            "dirname",
            "find",
            "mkdir",
            "python3",
            "rm",
            "rmdir",
        ):
            symlink_command(fake_bin, command)
        fake_sleep = fake_bin / "sleep"
        fake_sleep.write_text(
            f"""#!/usr/bin/env bash
set -euo pipefail

if [[ "${{1:-}}" == "1" ]]; then
  printf 'lock-wait\\n' >> {str(lock_wait_log)!r}
fi
exec {shutil.which("sleep")!r} "$@"
""",
            encoding="utf-8",
        )
        fake_sleep.chmod(0o755)
        fake_git.write_text(
            f"""#!/usr/bin/env bash
set -euo pipefail

clone_log={str(clone_log)!r}
release_clone={str(release_clone)!r}

if [[ "$1" == "clone" ]]; then
  printf 'clone\\n' >> "$clone_log"
  while [[ ! -f "$release_clone" ]]; do
    sleep 0.05
  done
  dest="${{@: -1}}"
  if [[ -e "$dest" ]] && [[ -n "$(find "$dest" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
    printf "fatal: destination path '%s' already exists and is not an empty directory.\\n" "$dest" >&2
    exit 128
  fi
  mkdir -p "$dest/.git"
  exit 0
fi

if [[ "$1" == "-C" ]]; then
  shift 2
  case "${{1:-}}" in
    clean|fetch|checkout|pull)
      exit 0
      ;;
    rev-parse)
      if [[ "${{*: -1}}" == "HEAD" ]]; then
        printf '0123456789abcdef0123456789abcdef01234567\\n'
        exit 0
      fi
      exit 1
      ;;
  esac
fi

printf "unexpected git invocation:" >&2
printf " %q" "$@" >&2
printf "\\n" >&2
exit 99
""",
            encoding="utf-8",
        )
        fake_git.chmod(0o755)

        checkout_dir = temp_root / "delta-rs-under-test"
        env = os.environ.copy()
        env["PATH"] = str(fake_bin)
        env["DELTA_RS_DIR"] = str(checkout_dir)

        first = subprocess.Popen(
            ["bash", str(prepare_copy)],
            cwd=temp_root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert wait_for_condition(
            lambda: clone_log.exists()
            and len(clone_log.read_text(encoding="utf-8").splitlines()) >= 1
        )

        second = subprocess.Popen(
            ["bash", str(prepare_copy)],
            cwd=temp_root,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        assert wait_for_condition(
            lambda: lock_wait_log.exists()
            and len(lock_wait_log.read_text(encoding="utf-8").splitlines()) >= 1,
            timeout=3.0,
        )
        assert len(clone_log.read_text(encoding="utf-8").splitlines()) == 1

        release_clone.write_text("ok\n", encoding="utf-8")
        first_stdout, first_stderr = first.communicate(timeout=10)
        second_stdout, second_stderr = second.communicate(timeout=10)

        clone_count = len(clone_log.read_text(encoding="utf-8").splitlines())
        assert first.returncode == 0, first_stderr or first_stdout
        assert second.returncode == 0, second_stderr or second_stdout
        assert clone_count == 1
        assert (checkout_dir / ".git").exists()


def test_prepare_delta_rs_rejects_checkout_lock_override_inside_managed_checkout_before_first_clone() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        prepare_copy = scripts_dir / "prepare_delta_rs.sh"
        prepare_copy.write_text(
            PREPARE_DELTA_RS.read_text(encoding="utf-8"), encoding="utf-8"
        )
        prepare_copy.chmod(0o755)

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
        fake_git = fake_bin / "git"
        fake_git.write_text(
            """#!/usr/bin/env bash
set -euo pipefail
printf "git should not be invoked\\n" >&2
exit 99
""",
            encoding="utf-8",
        )
        fake_git.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["DELTA_RS_DIR"] = str(temp_root / ".delta-rs-under-test")
        env["DELTA_BENCH_CHECKOUT_LOCK_FILE"] = str(
            temp_root / ".delta-rs-under-test" / ".delta_bench_checkout.lock"
        )

        result = subprocess.run(
            ["bash", str(prepare_copy)],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert (
            "DELTA_BENCH_CHECKOUT_LOCK_FILE must be outside DELTA_RS_DIR before initial clone"
            in result.stderr
        )
        assert "git should not be invoked" not in result.stderr


def test_gitignore_ignores_checkout_lock_artifacts() -> None:
    gitignore = GITIGNORE.read_text(encoding="utf-8")
    assert ".DS_Store" in gitignore
    assert "*.delta_bench_checkout.lock" in gitignore
    assert "*.delta_bench_checkout.lock.dir/" in gitignore
    assert "Best-in-Class Benchmark Suite" not in gitignore
    assert "Research Prompt: Design and Build" not in gitignore
    assert "Below is a **complete, production-qualit" not in gitignore


def test_compare_branch_default_checkout_lock_does_not_block_initial_clone() -> None:
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        prepare_copy = scripts_dir / "prepare_delta_rs.sh"
        security_copy = scripts_dir / "security_check.sh"
        compare_copy.write_text(
            COMPARE_BRANCH.read_text(encoding="utf-8"), encoding="utf-8"
        )
        prepare_copy.write_text(
            PREPARE_DELTA_RS.read_text(encoding="utf-8"), encoding="utf-8"
        )
        security_copy.write_text(
            "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n",
            encoding="utf-8",
        )
        compare_copy.chmod(0o755)
        prepare_copy.chmod(0o755)
        security_copy.chmod(0o755)

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
        fake_git = fake_bin / "git"
        fake_git.write_text(
            """#!/usr/bin/env bash
set -euo pipefail

if [[ "$1" == "clone" ]]; then
  dest="${@: -1}"
  if [[ -e "$dest" ]] && [[ -n "$(find "$dest" -mindepth 1 -maxdepth 1 -print -quit 2>/dev/null)" ]]; then
    printf "fatal: destination path '%s' already exists and is not an empty directory.\\n" "$dest" >&2
    exit 128
  fi
  mkdir -p "$dest/.git"
  exit 0
fi

if [[ "$1" == "-C" ]]; then
  shift 2
  case "${1:-}" in
    clean|fetch|checkout|pull)
      exit 0
      ;;
    rev-parse)
      if [[ "${*: -1}" == "HEAD" ]]; then
        printf '0123456789abcdef0123456789abcdef01234567\\n'
        exit 0
      fi
      exit 1
      ;;
    show-ref)
      exit 1
      ;;
  esac
fi

printf "unexpected git invocation:" >&2
printf " %q" "$@" >&2
printf "\\n" >&2
exit 99
""",
            encoding="utf-8",
        )
        fake_git.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"

        result = subprocess.run(
            ["bash", str(compare_copy), "main", "candidate", "scan"],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert "Preparing delta-rs checkout and fixtures" in result.stdout
        assert "cloning https://github.com/delta-io/delta-rs" in result.stdout
        assert "delta-rs checkout ready:" in result.stdout
        assert "fatal: destination path" not in result.stderr
        assert "benchmark ref 'main' not found" in result.stderr
        assert (temp_root / ".delta-rs-under-test" / ".git").is_dir()
        assert not (
            temp_root / ".delta-rs-under-test" / ".delta_bench_checkout.lock"
        ).exists()


def test_compare_branch_keeps_relative_results_under_runner_root_when_exec_root_differs() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        bench_copy = scripts_dir / "bench.sh"
        compare_copy.write_text(
            COMPARE_BRANCH.read_text(encoding="utf-8"), encoding="utf-8"
        )
        bench_copy.write_text(BENCH_SH.read_text(encoding="utf-8"), encoding="utf-8")
        compare_copy.chmod(0o755)
        bench_copy.chmod(0o755)

        write_executable(
            scripts_dir / "prepare_delta_rs.sh",
            """#!/usr/bin/env bash
set -euo pipefail
mkdir -p "${DELTA_RS_DIR}/.git"
ref="${DELTA_RS_REF:-${DELTA_RS_BRANCH:-}}"
if [[ -z "${ref}" ]]; then
  ref="0000000000000000000000000000000000000000"
fi
printf '%s\n' "${ref}" > "${DELTA_RS_DIR}/.bench-current-sha"
printf 'delta-rs checkout ready: %s\n' "${DELTA_RS_DIR}"
""",
        )
        write_executable(
            scripts_dir / "sync_harness_to_delta_rs.sh",
            """#!/usr/bin/env bash
set -euo pipefail
mkdir -p "${DELTA_RS_DIR}/crates/delta-bench"
: > "${DELTA_RS_DIR}/crates/delta-bench/Cargo.toml"
""",
        )
        write_executable(
            scripts_dir / "security_check.sh",
            "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n",
        )

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
        write_executable(
            fake_bin / "git",
            """#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "clone" ]]; then
  dest="${@: -1}"
  mkdir -p "${dest}/.git"
  exit 0
fi

if [[ "${1:-}" == "-C" ]]; then
  repo="$2"
  shift 2
  case "${1:-}" in
    clean|fetch|checkout|pull)
      exit 0
      ;;
    show-ref)
      exit 1
      ;;
    rev-parse)
      if [[ "$*" == *"HEAD"* ]]; then
        cat "${repo}/.bench-current-sha"
        exit 0
      fi
      ;;
  esac
fi

printf "unexpected git invocation:" >&2
printf " %q" "$@" >&2
printf "\\n" >&2
exit 99
""",
        )
        write_executable(
            fake_bin / "cargo",
            """#!/usr/bin/env python3
import json
import sys
from pathlib import Path


def value(args, flag, default=None):
    if flag not in args:
        return default
    idx = args.index(flag)
    return args[idx + 1]


args = sys.argv[1:]
cli_args = args[args.index("--") + 1 :] if "--" in args else args
cwd = Path.cwd()
command = "doctor"
for candidate in ("data", "run", "list", "doctor"):
    if candidate in cli_args:
        command = candidate
        break

fixtures_dir = Path(value(cli_args, "--fixtures-dir", "fixtures"))
if not fixtures_dir.is_absolute():
    fixtures_dir = cwd / fixtures_dir

if command == "data":
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    sys.exit(0)

if command == "run":
    results_dir = Path(value(cli_args, "--results-dir", "results"))
    if not results_dir.is_absolute():
        results_dir = cwd / results_dir
    label = value(cli_args, "--label", "local")
    target = value(cli_args, "--target", "scan")
    out_dir = results_dir / label
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{target}.json"
    out_file.write_text(
        json.dumps({"label": label, "suite": target, "cwd": str(cwd)}),
        encoding="utf-8",
    )
    print(f"wrote result: {out_file}")
    sys.exit(0)

sys.exit(0)
""",
        )

        python_pkg = temp_root / "python" / "delta_bench_compare"
        python_pkg.mkdir(parents=True)
        (python_pkg / "__init__.py").write_text("", encoding="utf-8")
        write_executable(
            python_pkg / "aggregate.py",
            """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--label", required=True)
parser.add_argument("inputs", nargs="+")
args = parser.parse_args()

payloads = [json.loads(Path(path).read_text(encoding="utf-8")) for path in args.inputs]
output_path = Path(args.output)
output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(
    json.dumps({"label": args.label, "payloads": payloads}),
    encoding="utf-8",
)
""",
        )
        write_executable(
            python_pkg / "compare.py",
            """#!/usr/bin/env python3
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("base_json")
parser.add_argument("cand_json")
parser.parse_known_args()
print("compare ok")
""",
        )
        write_executable(
            python_pkg / "hash_policy.py",
            """#!/usr/bin/env python3
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("base_json")
parser.add_argument("cand_json")
parser.parse_known_args()
print("hash policy ok")
""",
        )

        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"
        results_rel = Path("results") / "pr-4318-scan-20260328"
        checkout_dir = temp_root / ".delta-rs-pr4318-bench"

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_RS_DIR"] = str(checkout_dir)
        env["DELTA_BENCH_RESULTS"] = str(results_rel)

        result = subprocess.run(
            [
                "bash",
                str(compare_copy),
                "--base-sha",
                base_sha,
                "--candidate-sha",
                candidate_sha,
                "scan",
            ],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr
        assert "Aggregating results" in result.stdout
        assert "Comparison report" in result.stdout
        assert (temp_root / results_rel / f"base-{base_sha}" / "scan.json").is_file()
        assert (temp_root / results_rel / f"base-{base_sha}-r1" / "scan.json").is_file()
        assert (
            temp_root / results_rel / f"cand-{candidate_sha}" / "scan.json"
        ).is_file()
        assert not (checkout_dir / results_rel).exists()


def test_bench_wrapper_anchors_relative_fixture_and_result_paths_to_harness_root_when_exec_root_differs() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        bench_copy = scripts_dir / "bench.sh"
        copy_executable(BENCH_SH, bench_copy)

        exec_root = temp_root / "remote cargo root"
        (exec_root / "crates" / "delta-bench").mkdir(parents=True)
        (exec_root / "crates" / "delta-bench" / "Cargo.toml").write_text(
            "[package]\nname = 'delta-bench'\nversion = '0.0.0'\n",
            encoding="utf-8",
        )

        cargo_log = temp_root / "cargo-log.jsonl"
        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
        write_executable(
            fake_bin / "cargo",
            f"""#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path


def value(args, flag, default=None):
    if flag not in args:
        return default
    idx = args.index(flag)
    return args[idx + 1]


args = sys.argv[1:]
cli_args = args[args.index("--") + 1 :] if "--" in args else args
cwd = Path.cwd()
command = "doctor"
for candidate in ("data", "run", "list", "doctor"):
    if candidate in cli_args:
        command = candidate
        break

fixtures_dir = value(cli_args, "--fixtures-dir")
results_dir = value(cli_args, "--results-dir")
entry = {{
    "command": command,
    "cwd": str(cwd),
    "fixtures_dir": str((cwd / fixtures_dir).resolve()) if fixtures_dir else None,
    "results_dir": str((cwd / results_dir).resolve()) if results_dir else None,
    "env_exec_root": os.environ.get("DELTA_BENCH_EXEC_ROOT"),
    "env_results": os.environ.get("DELTA_BENCH_RESULTS"),
    "env_label": os.environ.get("DELTA_BENCH_LABEL"),
}}
with open({str(cargo_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(entry) + "\\n")

if command == "data":
    Path(entry["fixtures_dir"]).mkdir(parents=True, exist_ok=True)
    sys.exit(0)

if command == "run":
    resolved = Path(entry["results_dir"])
    label = value(cli_args, "--label", "local")
    target = value(cli_args, "--target", "scan")
    out_file = resolved / label / f"{{target}}.json"
    out_file.parent.mkdir(parents=True, exist_ok=True)
    out_file.write_text(
        json.dumps({{"label": label, "cwd": str(cwd), "results_dir": str(resolved)}}),
        encoding="utf-8",
    )
    sys.exit(0)

sys.exit(0)
""",
        )

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["DELTA_BENCH_EXEC_ROOT"] = str(exec_root)
        default_env = env.copy()
        relative_env = env.copy()
        relative_env["DELTA_BENCH_FIXTURES"] = "fixtures-rel"
        relative_env["DELTA_BENCH_RESULTS"] = "results-rel"
        absolute_results_dir = temp_root / "abs-results"
        absolute_env = env.copy()
        absolute_env["DELTA_BENCH_RESULTS"] = str(absolute_results_dir)

        default_data_result = subprocess.run(
            [
                "bash",
                str(bench_copy),
                "data",
                "--dataset-id",
                "tiny_smoke",
            ],
            cwd=temp_root,
            env=default_env,
            check=False,
            capture_output=True,
            text=True,
        )
        assert default_data_result.returncode == 0, default_data_result.stderr

        default_run_result = subprocess.run(
            [
                "bash",
                str(bench_copy),
                "run",
                "--suite",
                "scan",
                "--warmup",
                "1",
                "--iters",
                "1",
            ],
            cwd=temp_root,
            env=default_env,
            check=False,
            capture_output=True,
            text=True,
        )
        assert default_run_result.returncode == 0, default_run_result.stderr

        relative_data_result = subprocess.run(
            [
                "bash",
                str(bench_copy),
                "data",
                "--dataset-id",
                "tiny_smoke",
            ],
            cwd=temp_root,
            env=relative_env,
            check=False,
            capture_output=True,
            text=True,
        )
        assert relative_data_result.returncode == 0, relative_data_result.stderr

        relative_run_result = subprocess.run(
            [
                "bash",
                str(bench_copy),
                "run",
                "--suite",
                "scan",
                "--warmup",
                "1",
                "--iters",
                "1",
            ],
            cwd=temp_root,
            env=relative_env,
            check=False,
            capture_output=True,
            text=True,
        )
        assert relative_run_result.returncode == 0, relative_run_result.stderr

        absolute_run_result = subprocess.run(
            [
                "bash",
                str(bench_copy),
                "run",
                "--suite",
                "scan",
                "--warmup",
                "1",
                "--iters",
                "1",
            ],
            cwd=temp_root,
            env=absolute_env,
            check=False,
            capture_output=True,
            text=True,
        )
        assert absolute_run_result.returncode == 0, absolute_run_result.stderr

        default_fixtures_dir = temp_root / "fixtures"
        default_results_dir = temp_root / "results"
        fixtures_dir = temp_root / "fixtures-rel"
        results_dir = temp_root / "results-rel"
        assert default_fixtures_dir.is_dir()
        assert (default_results_dir / "local" / "scan.json").is_file()
        assert fixtures_dir.is_dir()
        assert (results_dir / "local" / "scan.json").is_file()
        assert (absolute_results_dir / "local" / "scan.json").is_file()
        assert not (exec_root / "fixtures").exists()
        assert not (exec_root / "results").exists()
        assert not (exec_root / "fixtures-rel").exists()
        assert not (exec_root / "results-rel").exists()
        assert not (exec_root / "abs-results").exists()

        entries = [
            json.loads(line)
            for line in cargo_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        assert len(entries) == 5

        default_entries = [entry for entry in entries if entry["env_results"] is None]
        relative_entries = [
            entry for entry in entries if entry["env_results"] == "results-rel"
        ]
        absolute_entries = [
            entry
            for entry in entries
            if entry["env_results"] == str(absolute_results_dir)
        ]

        assert len(default_entries) == 2
        assert [entry["command"] for entry in default_entries] == ["data", "run"]
        assert Path(default_entries[0]["cwd"]).resolve() == exec_root.resolve()
        assert default_entries[0]["fixtures_dir"] == str(default_fixtures_dir.resolve())
        assert default_entries[0]["env_exec_root"] == str(exec_root)
        assert Path(default_entries[1]["cwd"]).resolve() == exec_root.resolve()
        assert default_entries[1]["results_dir"] == str(default_results_dir.resolve())
        assert default_entries[1]["env_exec_root"] == str(exec_root)

        assert len(relative_entries) == 2
        assert [entry["command"] for entry in relative_entries] == ["data", "run"]
        assert Path(relative_entries[0]["cwd"]).resolve() == exec_root.resolve()
        assert relative_entries[0]["fixtures_dir"] == str(fixtures_dir.resolve())
        assert relative_entries[0]["env_exec_root"] == str(exec_root)
        assert Path(relative_entries[1]["cwd"]).resolve() == exec_root.resolve()
        assert relative_entries[1]["results_dir"] == str(results_dir.resolve())
        assert relative_entries[1]["env_exec_root"] == str(exec_root)

        assert len(absolute_entries) == 1
        assert absolute_entries[0]["command"] == "run"
        assert Path(absolute_entries[0]["cwd"]).resolve() == exec_root.resolve()
        assert absolute_entries[0]["results_dir"] == str(absolute_results_dir.resolve())
        assert absolute_entries[0]["env_exec_root"] == str(exec_root)


def test_compare_branch_remote_runner_keeps_relative_results_under_remote_root_and_owns_bench_env() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        local_root = temp_root / "local harness"
        remote_root = temp_root / "remote runner root"
        local_scripts = local_root / "scripts"
        remote_scripts = remote_root / "scripts"
        local_scripts.mkdir(parents=True)
        remote_scripts.mkdir(parents=True)

        compare_copy = local_scripts / "compare_branch.sh"
        bench_copy = remote_scripts / "bench.sh"
        copy_executable(COMPARE_BRANCH, compare_copy)
        copy_executable(BENCH_SH, bench_copy)

        write_executable(
            remote_scripts / "prepare_delta_rs.sh",
            """#!/usr/bin/env bash
set -euo pipefail
mkdir -p "${DELTA_RS_DIR}/.git"
ref="${DELTA_RS_REF:-${DELTA_RS_BRANCH:-}}"
if [[ -z "${ref}" ]]; then
  ref="0000000000000000000000000000000000000000"
fi
printf '%s\n' "${ref}" > "${DELTA_RS_DIR}/.bench-current-sha"
printf 'delta-rs checkout ready: %s\n' "${DELTA_RS_DIR}"
""",
        )
        write_executable(
            remote_scripts / "sync_harness_to_delta_rs.sh",
            """#!/usr/bin/env bash
set -euo pipefail
mkdir -p "${DELTA_RS_DIR}/crates/delta-bench"
: > "${DELTA_RS_DIR}/crates/delta-bench/Cargo.toml"
""",
        )
        write_executable(
            remote_scripts / "security_check.sh",
            "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n",
        )

        python_pkg = remote_root / "python" / "delta_bench_compare"
        python_pkg.mkdir(parents=True)
        (python_pkg / "__init__.py").write_text("", encoding="utf-8")

        compare_log = remote_root / "compare-log.jsonl"
        write_executable(
            python_pkg / "aggregate.py",
            f"""#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--label", required=True)
parser.add_argument("inputs", nargs="+")
args = parser.parse_args()

entry = {{
    "tool": "aggregate",
    "cwd": str(Path.cwd()),
    "pythonpath": os.environ.get("PYTHONPATH"),
    "output": args.output,
    "label": args.label,
    "inputs": args.inputs,
}}
with open({str(compare_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(entry) + "\\n")

payloads = [json.loads(Path(path).read_text(encoding="utf-8")) for path in args.inputs]
output_path = Path(args.output)
output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(
    json.dumps({{"label": args.label, "payloads": payloads}}),
    encoding="utf-8",
)
""",
        )
        write_executable(
            python_pkg / "compare.py",
            f"""#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("base_json")
parser.add_argument("cand_json")
args, extra = parser.parse_known_args()

entry = {{
    "tool": "compare",
    "cwd": str(Path.cwd()),
    "pythonpath": os.environ.get("PYTHONPATH"),
    "base_json": args.base_json,
    "cand_json": args.cand_json,
    "extra": extra,
}}
with open({str(compare_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(entry) + "\\n")

Path(args.base_json).read_text(encoding="utf-8")
Path(args.cand_json).read_text(encoding="utf-8")
print("remote compare ok")
""",
        )
        write_executable(
            python_pkg / "hash_policy.py",
            f"""#!/usr/bin/env python3
import argparse
import json
import os
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("base_json")
parser.add_argument("cand_json")
args, extra = parser.parse_known_args()

entry = {{
    "tool": "hash_policy",
    "cwd": str(Path.cwd()),
    "pythonpath": os.environ.get("PYTHONPATH"),
    "base_json": args.base_json,
    "cand_json": args.cand_json,
    "extra": extra,
}}
with open({str(compare_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(entry) + "\\n")

Path(args.base_json).read_text(encoding="utf-8")
Path(args.cand_json).read_text(encoding="utf-8")
print("remote hash policy ok")
""",
        )

        cargo_log = remote_root / "cargo-log.jsonl"
        ssh_log = temp_root / "ssh.log"
        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
        write_executable(
            fake_bin / "ssh",
            f"""#!/usr/bin/env python3
import json
import os
import shlex
import subprocess
import sys

host = sys.argv[1] if len(sys.argv) > 1 else ""
command = sys.argv[2] if len(sys.argv) > 2 else ""

payload = command
if command.startswith("bash -lc "):
    payload_parts = shlex.split(command)
    payload = payload_parts[2]

remote_env = {{}}
for key in ("PATH", "HOME", "TMPDIR", "TMP", "TEMP", "LANG"):
    value = os.environ.get(key)
    if value is not None:
        remote_env[key] = value
with open({str(ssh_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(
        json.dumps(
            {{
                "host": host,
                "payload": payload,
                "remote_env_exec_root": remote_env.get("DELTA_BENCH_EXEC_ROOT"),
                "remote_env_results": remote_env.get("DELTA_BENCH_RESULTS"),
                "remote_env_label": remote_env.get("DELTA_BENCH_LABEL"),
            }}
        )
        + "\\n"
    )

raise SystemExit(
    subprocess.run(["bash", "-c", payload], check=False, env=remote_env).returncode
)
""",
        )
        write_executable(
            fake_bin / "git",
            """#!/usr/bin/env bash
set -euo pipefail

if [[ "${1:-}" == "-C" ]]; then
  repo="$2"
  shift 2
  case "${1:-}" in
    clean|fetch|checkout|pull)
      exit 0
      ;;
    rev-parse)
      if [[ "$*" == *"HEAD"* ]]; then
        cat "${repo}/.bench-current-sha"
        exit 0
      fi
      ;;
  esac
fi

printf "unexpected git invocation:" >&2
printf " %q" "$@" >&2
printf "\\n" >&2
exit 99
""",
        )
        write_executable(
            fake_bin / "cargo",
            f"""#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path


def value(args, flag, default=None):
    if flag not in args:
        return default
    idx = args.index(flag)
    return args[idx + 1]


args = sys.argv[1:]
cli_args = args[args.index("--") + 1 :] if "--" in args else args
cwd = Path.cwd()
command = "doctor"
for candidate in ("data", "run", "list", "doctor"):
    if candidate in cli_args:
        command = candidate
        break

entry = {{
    "command": command,
    "cwd": str(cwd),
    "env_exec_root": os.environ.get("DELTA_BENCH_EXEC_ROOT"),
    "env_results": os.environ.get("DELTA_BENCH_RESULTS"),
    "env_label": os.environ.get("DELTA_BENCH_LABEL"),
    "fixtures_dir": value(cli_args, "--fixtures-dir"),
    "results_dir": value(cli_args, "--results-dir"),
}}
with open({str(cargo_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(entry) + "\\n")

fixtures_dir = Path(value(cli_args, "--fixtures-dir", "fixtures"))
if not fixtures_dir.is_absolute():
    fixtures_dir = cwd / fixtures_dir

if command == "data":
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    sys.exit(0)

if command == "run":
    results_dir = Path(value(cli_args, "--results-dir", "results"))
    if not results_dir.is_absolute():
        results_dir = cwd / results_dir
    label = value(cli_args, "--label", "local")
    target = value(cli_args, "--target", "scan")
    out_dir = results_dir / label
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{{target}}.json"
    out_file.write_text(
        json.dumps({{
            "label": label,
            "suite": target,
            "cwd": str(cwd),
            "env_exec_root": os.environ.get("DELTA_BENCH_EXEC_ROOT"),
            "env_results": os.environ.get("DELTA_BENCH_RESULTS"),
        }}),
        encoding="utf-8",
    )
    print(f"wrote result: {{out_file}}")
    sys.exit(0)

sys.exit(0)
""",
        )

        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"
        results_rel = Path("results") / "remote compare 20260328"
        checkout_dir = remote_root / ".delta-rs-remote-checkout"

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_RS_DIR"] = str(checkout_dir)
        env["DELTA_BENCH_RESULTS"] = str(results_rel)
        env["DELTA_BENCH_EXEC_ROOT"] = str(
            temp_root / "outer exec root should be ignored"
        )
        env["DELTA_BENCH_LABEL"] = "outer-label-should-not-leak"

        result = subprocess.run(
            [
                "bash",
                str(compare_copy),
                "--remote-runner",
                "bench-host",
                "--remote-root",
                str(remote_root),
                "--base-sha",
                base_sha,
                "--candidate-sha",
                candidate_sha,
                "scan",
            ],
            cwd=local_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr
        assert "Comparison report" in result.stdout

        remote_results_dir = remote_root / results_rel
        assert (remote_results_dir / f"base-{base_sha}" / "scan.json").is_file()
        assert (remote_results_dir / f"cand-{candidate_sha}" / "scan.json").is_file()
        assert not (local_root / results_rel).exists()
        assert not (checkout_dir / results_rel).exists()

        cargo_entries = [
            json.loads(line)
            for line in cargo_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        assert cargo_entries
        assert {Path(entry["cwd"]).resolve() for entry in cargo_entries} == {
            checkout_dir.resolve()
        }
        assert {entry["env_exec_root"] for entry in cargo_entries} == {
            str(checkout_dir)
        }
        assert {entry["env_results"] for entry in cargo_entries} == {str(results_rel)}
        assert "outer-label-should-not-leak" not in {
            entry["env_label"] for entry in cargo_entries
        }
        assert [
            entry["env_label"] for entry in cargo_entries if entry["command"] == "data"
        ] == [f"base-{base_sha}"]
        assert {
            entry["env_label"] for entry in cargo_entries if entry["command"] == "run"
        } == {f"base-{base_sha}-r1", f"cand-{candidate_sha}-r1"}

        compare_entries = [
            json.loads(line)
            for line in compare_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        assert {Path(entry["cwd"]).resolve() for entry in compare_entries} == {
            remote_root.resolve()
        }
        assert {entry["pythonpath"] for entry in compare_entries} == {
            str(remote_root / "python")
        }
        aggregate_entries = [
            entry for entry in compare_entries if entry["tool"] == "aggregate"
        ]
        assert aggregate_entries
        for entry in aggregate_entries:
            assert entry["output"].startswith(f"{results_rel}/")
            assert all(path.startswith(f"{results_rel}/") for path in entry["inputs"])

        ssh_entries = [
            json.loads(line)
            for line in ssh_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        assert ssh_entries
        assert {entry["host"] for entry in ssh_entries} == {"bench-host"}
        assert {
            (
                entry["remote_env_exec_root"],
                entry["remote_env_results"],
                entry["remote_env_label"],
            )
            for entry in ssh_entries
        } == {(None, None, None)}
        ssh_payloads = [entry["payload"] for entry in ssh_entries]
        assert any(str(remote_root) in shlex.split(payload) for payload in ssh_payloads)


def test_compare_branch_rejects_checkout_lock_override_inside_managed_checkout_before_first_clone() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        prepare_copy = scripts_dir / "prepare_delta_rs.sh"
        compare_copy.write_text(
            COMPARE_BRANCH.read_text(encoding="utf-8"), encoding="utf-8"
        )
        prepare_copy.write_text(
            PREPARE_DELTA_RS.read_text(encoding="utf-8"), encoding="utf-8"
        )
        compare_copy.chmod(0o755)
        prepare_copy.chmod(0o755)

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
        fake_git = fake_bin / "git"
        fake_git.write_text(
            """#!/usr/bin/env bash
set -euo pipefail
printf "git should not be invoked\\n" >&2
exit 99
""",
            encoding="utf-8",
        )
        fake_git.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_BENCH_CHECKOUT_LOCK_FILE"] = str(
            temp_root / ".delta-rs-under-test" / ".delta_bench_checkout.lock"
        )

        result = subprocess.run(
            ["bash", str(compare_copy), "main", "candidate", "scan"],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert (
            "DELTA_BENCH_CHECKOUT_LOCK_FILE must be outside DELTA_RS_DIR before initial clone"
            in result.stderr
        )
        assert "git should not be invoked" not in result.stderr


def test_compare_branch_rejects_untrusted_macro_compare_suites_before_checkout() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        compare_copy.write_text(
            COMPARE_BRANCH.read_text(encoding="utf-8"), encoding="utf-8"
        )
        compare_copy.chmod(0o755)

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
        fake_git = fake_bin / "git"
        fake_git.write_text(
            """#!/usr/bin/env bash
set -euo pipefail
printf "git should not be invoked\\n" >&2
exit 99
""",
            encoding="utf-8",
        )
        fake_git.chmod(0o755)

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"

        result = subprocess.run(
            ["bash", str(compare_copy), "main", "candidate", "all"],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert "suite 'all' is not supported" in result.stderr
        assert "macro-lane branch compare" in result.stderr
        assert "scan" in result.stderr
        assert "git should not be invoked" not in result.stderr


def test_compare_branch_acquires_checkout_lock_for_full_run() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "DELTA_BENCH_CHECKOUT_LOCK_FILE" in script
    assert "DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS" in script
    assert "acquire_checkout_lock" in script
    assert "DELTA_BENCH_CHECKOUT_LOCK_HELD" in script
    assert re.search(
        r"flock -w \"\$\{DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS\}\"",
        script,
    )
    assert re.search(r"export DELTA_BENCH_CHECKOUT_LOCK_HELD=1", script)


def test_prepare_delta_rs_honors_existing_checkout_lock() -> None:
    script = PREPARE_DELTA_RS.read_text(encoding="utf-8")
    assert "DELTA_BENCH_CHECKOUT_LOCK_FILE" in script
    assert "DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS" in script
    assert "DELTA_BENCH_CHECKOUT_LOCK_HELD" in script
    assert "acquire_checkout_lock" in script
    assert re.search(
        r"if \[\[ \"\$\{DELTA_BENCH_CHECKOUT_LOCK_HELD:-0\}\" == \"1\" \]\]; then",
        script,
    )
    assert re.search(
        r"flock -w \"\$\{DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS\}\"",
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
        "--allow-outside-root",
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
            [
                "bash",
                str(LOCAL_CLEANUP),
                "--apply",
                "--results",
                "--allow-outside-root",
            ],
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


def test_cleanup_local_empty_results_dir_is_noop() -> None:
    with tempfile.TemporaryDirectory() as td:
        results_dir = Path(td) / "results"
        results_dir.mkdir(parents=True)

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
        assert "No matching artifacts to clean." in result.stdout


def test_cleanup_local_older_than_days_no_matches_is_noop() -> None:
    with tempfile.TemporaryDirectory() as td:
        results_dir = Path(td) / "results"
        recent = results_dir / "recent-run"
        recent.mkdir(parents=True)
        (recent / "scan.json").write_text("{}", encoding="utf-8")

        env = os.environ.copy()
        env["DELTA_BENCH_RESULTS"] = str(results_dir)

        result = subprocess.run(
            ["bash", str(LOCAL_CLEANUP), "--results", "--older-than-days", "3650"],
            cwd=REPO_ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert "No matching artifacts to clean." in result.stdout


def test_cleanup_local_apply_refuses_outside_repo_without_override() -> None:
    with tempfile.TemporaryDirectory() as td:
        results_dir = Path(td) / "results"
        label_dir = results_dir / "run-a"
        label_dir.mkdir(parents=True)
        (label_dir / "scan.json").write_text("{}", encoding="utf-8")

        env = os.environ.copy()
        env["DELTA_BENCH_RESULTS"] = str(results_dir)

        apply_run = subprocess.run(
            ["bash", str(LOCAL_CLEANUP), "--apply", "--results"],
            cwd=REPO_ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert apply_run.returncode != 0
        assert (
            "outside repository root without --allow-outside-root" in apply_run.stderr
        )
        assert label_dir.exists()


def test_cleanup_local_apply_allows_outside_repo_with_override() -> None:
    with tempfile.TemporaryDirectory() as td:
        results_dir = Path(td) / "results"
        label_dir = results_dir / "run-a"
        label_dir.mkdir(parents=True)
        (label_dir / "scan.json").write_text("{}", encoding="utf-8")

        env = os.environ.copy()
        env["DELTA_BENCH_RESULTS"] = str(results_dir)

        apply_run = subprocess.run(
            [
                "bash",
                str(LOCAL_CLEANUP),
                "--apply",
                "--results",
                "--allow-outside-root",
            ],
            cwd=REPO_ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert apply_run.returncode == 0
        assert not label_dir.exists()


def test_cleanup_local_checkout_target_removes_root_checkout_lock_artifacts() -> None:
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        checkout_dir = root / ".delta-rs-under-test"
        checkout_dir.mkdir(parents=True)
        lock_file = root / ".delta_bench_checkout.lock"
        lock_dir = root / ".delta_bench_checkout.lock.dir"
        lock_file.write_text("", encoding="utf-8")
        lock_dir.mkdir()
        (lock_dir / "pid").write_text("123\n", encoding="utf-8")

        env = os.environ.copy()
        env["DELTA_RS_DIR"] = str(checkout_dir)
        env["DELTA_BENCH_CHECKOUT_LOCK_FILE"] = str(lock_file)

        apply_run = subprocess.run(
            [
                "bash",
                str(LOCAL_CLEANUP),
                "--apply",
                "--delta-rs-under-test",
                "--allow-outside-root",
            ],
            cwd=REPO_ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert apply_run.returncode == 0
        assert not checkout_dir.exists()
        assert not lock_file.exists()
        assert not lock_dir.exists()


def test_cleanup_local_checkout_target_defaults_lock_artifacts_from_checkout_path() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        checkout_dir = root / "alt-checkout"
        checkout_dir.mkdir(parents=True)
        lock_file = root / ".alt-checkout.delta_bench_checkout.lock"
        lock_dir = root / ".alt-checkout.delta_bench_checkout.lock.dir"
        lock_file.write_text("", encoding="utf-8")
        lock_dir.mkdir()
        (lock_dir / "pid").write_text("123\n", encoding="utf-8")

        env = os.environ.copy()
        env["DELTA_RS_DIR"] = str(checkout_dir)

        apply_run = subprocess.run(
            [
                "bash",
                str(LOCAL_CLEANUP),
                "--apply",
                "--delta-rs-under-test",
                "--allow-outside-root",
            ],
            cwd=REPO_ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert apply_run.returncode == 0
        assert not checkout_dir.exists()
        assert not lock_file.exists()
        assert not lock_dir.exists()
