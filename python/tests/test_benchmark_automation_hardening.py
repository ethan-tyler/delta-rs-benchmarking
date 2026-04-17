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

import pytest

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
PUBLISH_CONTRACT_SCRIPT = REPO_ROOT / "scripts" / "publish_contract.sh"
ACTIONLINT_SCRIPT = REPO_ROOT / "scripts" / "run_actionlint.sh"
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
PR_MACRO_PROFILE = REPO_ROOT / "bench" / "methodologies" / "pr-macro.env"
PR_WRITE_PERF_PROFILE = REPO_ROOT / "bench" / "methodologies" / "pr-write-perf.env"
PR_DELETE_UPDATE_PERF_PROFILE = (
    REPO_ROOT / "bench" / "methodologies" / "pr-delete-update-perf.env"
)
PR_MERGE_PERF_PROFILE = REPO_ROOT / "bench" / "methodologies" / "pr-merge-perf.env"
PR_OPTIMIZE_PERF_PROFILE = (
    REPO_ROOT / "bench" / "methodologies" / "pr-optimize-perf.env"
)
PR_METADATA_PERF_PROFILE = (
    REPO_ROOT / "bench" / "methodologies" / "pr-metadata-perf.env"
)
METADATA_REPLAY_CRITERION_PROFILE = (
    REPO_ROOT / "bench" / "methodologies" / "metadata-replay-criterion.env"
)
S3_CANDIDATE_PROFILES = {
    "scan": REPO_ROOT / "bench" / "methodologies" / "scan-s3-candidate.env",
    "write_perf": REPO_ROOT / "bench" / "methodologies" / "write-perf-s3-candidate.env",
    "delete_update_perf": REPO_ROOT
    / "bench"
    / "methodologies"
    / "delete-update-perf-s3-candidate.env",
    "merge_perf": REPO_ROOT / "bench" / "methodologies" / "merge-perf-s3-candidate.env",
    "optimize_perf": REPO_ROOT
    / "bench"
    / "methodologies"
    / "optimize-perf-s3-candidate.env",
    "metadata_perf": REPO_ROOT
    / "bench"
    / "methodologies"
    / "metadata-perf-s3-candidate.env",
}
COMPARE_DOC = REPO_ROOT / "docs" / "comparing-branches.md"
README_DOC = REPO_ROOT / "README.md"
EVIDENCE_REGISTRY = REPO_ROOT / "bench" / "evidence" / "registry.yaml"
WRITE_PERF_SUITE = (
    REPO_ROOT / "crates" / "delta-bench" / "src" / "suites" / "write_perf.rs"
)
DELETE_UPDATE_PERF_SUITE = (
    REPO_ROOT / "crates" / "delta-bench" / "src" / "suites" / "delete_update_perf.rs"
)
MERGE_PERF_SUITE = (
    REPO_ROOT / "crates" / "delta-bench" / "src" / "suites" / "merge_perf.rs"
)
OPTIMIZE_PERF_SUITE = (
    REPO_ROOT / "crates" / "delta-bench" / "src" / "suites" / "optimize_perf.rs"
)
METADATA_PERF_SUITE = (
    REPO_ROOT / "crates" / "delta-bench" / "src" / "suites" / "metadata_perf.rs"
)
TPCDS_SUITE = (
    REPO_ROOT / "crates" / "delta-bench" / "src" / "suites" / "tpcds" / "mod.rs"
)


def read_env_file(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, value = line.split("=", maxsplit=1)
        values[key] = value
    return values


def embedded_python_from_shell_function(script: str, function_name: str) -> str:
    marker = f"{function_name}() {{"
    start = script.index(marker)
    function_block = script[start:]
    return function_block.split("<<'PY'", maxsplit=1)[1].split("\nPY\n", maxsplit=1)[0]


def shell_function_block(script: str, function_name: str) -> str:
    marker = f"{function_name}() {{"
    start = script.index(marker)
    function_block = script[start:]
    body = function_block.split("\n}\n", maxsplit=1)[0]
    return body + "\n}\n"


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


def write_overlay_manifest(exec_root: Path, entries: list[str]) -> None:
    manifest = exec_root / "crates" / "delta-bench" / ".delta_bench_overlay_manifest"
    manifest.parent.mkdir(parents=True, exist_ok=True)
    manifest.write_text(
        "".join(f"{entry}\n" for entry in entries),
        encoding="utf-8",
    )


def copy_compare_manifest_helper(dest_pkg: Path) -> None:
    write_executable(
        dest_pkg / "manifest.py",
        (REPO_ROOT / "python" / "delta_bench_compare" / "manifest.py").read_text(
            encoding="utf-8"
        ),
    )


def relax_local_compare_disk_headroom(env: dict[str, str]) -> None:
    env["DELTA_BENCH_MIN_FREE_GB"] = "1"


def configure_git_identity(repo: Path) -> None:
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.email", "bench@example.com"],
        check=True,
    )
    subprocess.run(
        ["git", "-C", str(repo), "config", "user.name", "Bench Test"],
        check=True,
    )


def create_origin_and_fork_repos(temp_root: Path) -> tuple[Path, Path, str]:
    origin_src = temp_root / "origin-src"
    subprocess.run(["git", "init", "-q", str(origin_src)], check=True)
    configure_git_identity(origin_src)
    (origin_src / "base.txt").write_text("base\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(origin_src), "add", "base.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(origin_src), "commit", "-q", "-m", "base"],
        check=True,
    )

    origin_repo = temp_root / "origin.git"
    subprocess.run(
        ["git", "clone", "-q", "--bare", str(origin_src), str(origin_repo)],
        check=True,
    )

    fork_repo = temp_root / "fork-src"
    subprocess.run(["git", "clone", "-q", str(origin_repo), str(fork_repo)], check=True)
    configure_git_identity(fork_repo)
    (fork_repo / "candidate.txt").write_text("candidate\n", encoding="utf-8")
    subprocess.run(["git", "-C", str(fork_repo), "add", "candidate.txt"], check=True)
    subprocess.run(
        ["git", "-C", str(fork_repo), "commit", "-q", "-m", "candidate"],
        check=True,
    )
    candidate_sha = subprocess.run(
        ["git", "-C", str(fork_repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()

    return origin_repo, fork_repo, candidate_sha


def install_validation_script_stubs(
    temp_root: Path,
) -> tuple[Path, Path, Path, Path, Path]:
    scripts_dir = temp_root / "scripts"
    scripts_dir.mkdir(parents=True)

    validate_copy = scripts_dir / "validate_perf_harness.sh"
    copy_executable(VALIDATION_SCRIPT, validate_copy)

    prepare_log = temp_root / "prepare-log.jsonl"
    compare_log = temp_root / "compare-log.jsonl"
    managed_checkout = temp_root / ".delta-rs-under-test"
    (managed_checkout / ".git").mkdir(parents=True)
    fake_bin = temp_root / "bin"
    fake_bin.mkdir()

    write_executable(
        scripts_dir / "prepare_delta_rs.sh",
        f"""#!/usr/bin/env bash
set -euo pipefail
mkdir -p "${{DELTA_RS_DIR}}/.git"
if [[ -n "${{DELTA_RS_REF:-}}" ]]; then
  printf '%s\\n' "${{DELTA_RS_REF}}" > "${{DELTA_RS_DIR}}/.bench-current-sha"
fi
python3 - <<'PY'
import json
import os
from pathlib import Path

Path({str(prepare_log)!r}).open("a", encoding="utf-8").write(
    json.dumps(
        {{
            "dir": os.environ.get("DELTA_RS_DIR"),
            "ref": os.environ.get("DELTA_RS_REF"),
            "fetch_url": os.environ.get("DELTA_RS_FETCH_URL"),
            "fetch_ref": os.environ.get("DELTA_RS_FETCH_REF"),
        }}
    )
    + "\\n"
)
PY
echo "delta-rs checkout ready: ${{DELTA_RS_DIR}}"
""",
    )
    write_executable(
        scripts_dir / "sync_harness_to_delta_rs.sh",
        """#!/usr/bin/env bash
set -euo pipefail
exit 0
""",
    )
    write_executable(
        scripts_dir / "bench.sh",
        """#!/usr/bin/env bash
set -euo pipefail
exit 0
""",
    )
    write_executable(
        scripts_dir / "compare_branch.sh",
        f"""#!/usr/bin/env bash
set -euo pipefail
python3 - "$@" <<'PY'
import json
import sys
from pathlib import Path

Path({str(compare_log)!r}).open("a", encoding="utf-8").write(
    json.dumps({{"argv": sys.argv[1:]}}) + "\\n"
)
PY
exit 99
""",
    )
    write_executable(
        fake_bin / "git",
        """#!/usr/bin/env bash
set -euo pipefail
if [[ "${1:-}" == "-C" ]]; then
  repo_dir="$2"
  shift 2
else
  repo_dir="$PWD"
fi
if [[ "${1:-}" == "rev-parse" ]]; then
  shift
  if [[ "${1:-}" == "--verify" ]]; then
    shift
  fi
  if [[ "${1:-}" == "HEAD" ]]; then
    cat "${repo_dir}/.bench-current-sha"
    exit 0
  fi
fi
echo "unsupported fake git invocation: $*" >&2
exit 97
""",
    )

    return validate_copy, prepare_log, compare_log, managed_checkout, fake_bin


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
    assert "write_perf" in script


def test_bench_sh_usage_lists_new_perf_owned_suites() -> None:
    script = BENCH_SH.read_text(encoding="utf-8")

    for suite in ("delete_update_perf", "merge_perf", "optimize_perf"):
        assert suite in script


def test_bench_sh_usage_excludes_scan_planning_suite() -> None:
    script = BENCH_SH.read_text(encoding="utf-8")
    assert "scan_planning" not in script


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


def test_actionlint_entrypoint_exists_and_bootstraps_pinned_release() -> None:
    assert ACTIONLINT_SCRIPT.exists(), "missing scripts/run_actionlint.sh"
    assert (
        ACTIONLINT_SCRIPT.stat().st_mode & 0o111
    ), "scripts/run_actionlint.sh must be executable"
    script = ACTIONLINT_SCRIPT.read_text(encoding="utf-8")
    assert "ACTIONLINT_VERSION=" in script
    assert "rhysd/actionlint" in script
    assert "curl -fsSL" in script
    assert "tar -xzf" in script


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


def test_benchmark_workflow_treats_full_as_pack_alias_not_suite() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "run benchmark decision full" in workflow
    assert "pack alias" in workflow.lower()
    assert "delta_bench_compare.pack plan" in workflow
    assert '"full"' not in re.search(
        r"const allowedSuites = new Set\(\[(?P<body>.*?)\]\);",
        workflow,
        flags=re.DOTALL,
    ).group("body")


def test_benchmark_workflow_rejects_exploratory_full_command() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "run benchmark full" in workflow
    assert "run benchmark decision full" in workflow
    assert "Reject exploratory 'run benchmark full' initially." in workflow


def test_benchmark_workflow_supports_decision_command_grammar() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "run benchmark decision" in workflow
    assert (
        "firstLine.match(/^run benchmark(?:\\s+(decision))?\\s+(\\S+)$/i)" in workflow
    )


def test_benchmark_workflow_invalid_command_guidance_points_to_operator_and_diagnostic_paths() -> (
    None
):
    workflow = WORKFLOW.read_text(encoding="utf-8")

    assert "ready suites:" in workflow
    assert "Candidate/manual perf suites stay operator-only." in workflow
    assert (
        "Use `./scripts/compare_branch.sh --current-vs-main --methodology-profile <profile> <suite>`"
        in workflow
    )
    assert (
        "Use `./scripts/run_profile.sh <criterion-profile>` for diagnostic Criterion work."
        in workflow
    )


def test_benchmark_workflow_uses_sha_pins_for_compare_refs() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert re.search(
        r"env:\n\s+BASE_SHA:\s+\$\{\{ needs\.request\.outputs\.base_sha \}\}", workflow
    )
    assert re.search(
        r"\s+HEAD_SHA:\s+\$\{\{ needs\.request\.outputs\.head_sha \}\}", workflow
    )
    assert re.search(
        r"\s+SUITE:\s+\$\{\{ needs\.request\.outputs\.suite \}\}", workflow
    )
    assert '"$BASE_SHA"' in workflow
    assert '"$HEAD_SHA"' in workflow
    assert '"$SUITE"' in workflow


def test_benchmark_workflow_uses_pack_planning_matrix_and_aggregation() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "python3 -m delta_bench_compare.pack plan" in workflow
    assert "--format github-matrix" in workflow
    assert "fromJson(" in workflow
    assert "matrix.timeout_minutes" in workflow
    assert "timeout --preserve-status" in workflow
    assert "./scripts/run_profile.sh --base-sha" in workflow
    assert '"$PROFILE"' in workflow or '"$PROFILE"' in workflow
    assert "python3 -m delta_bench_compare.pack summarize" in workflow
    assert "actions/download-artifact@v4" in workflow


def test_benchmark_workflow_supports_manual_replay_for_existing_pack_request() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "workflow_dispatch:" in workflow
    assert "request_id:" in workflow
    assert "github.event.inputs.request_id" in workflow
    assert "replay-request" in workflow
    assert "request-state" in workflow


def test_benchmark_workflow_tracks_requests_in_persistent_sqlite() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "DELTA_BENCH_BOT_DB_PATH" in workflow
    assert "python3 -m delta_bench_compare.bot_state" in workflow
    assert "${{ vars.DELTA_BENCH_BOT_DB_PATH }}" in workflow
    assert "/var/lib/delta-bench/pr-bot.sqlite3" not in workflow


def test_benchmark_workflow_requires_shared_bot_db_path_for_queue_and_pack() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "Validate shared bot database path" in workflow
    assert "DELTA_BENCH_BOT_DB_READY" in workflow
    assert "shared path mounted on every runner" in workflow
    assert (
        "Benchmark queue is unavailable until DELTA_BENCH_BOT_DB_PATH is configured"
        in workflow
    )
    assert (
        "Pack automation requires DELTA_BENCH_BOT_DB_PATH to point at a shared path"
        in workflow
    )


def test_show_benchmark_queue_reads_persistent_bot_db() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "show benchmark queue" in workflow
    assert "Queue status is not available in workflow mode" not in workflow
    assert "delta_bench_compare.bot_state" in workflow
    assert "queue > benchmark_queue.txt" in workflow


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


def test_pr_macro_profile_uses_medium_selective_dataset_and_stronger_decision_defaults() -> (
    None
):
    profile = read_env_file(PR_MACRO_PROFILE)

    assert profile["METHODOLOGY_PROFILE"] == "pr-macro"
    assert profile["COMPARE_MODE"] == "decision"
    assert profile["DATASET_ID"] == "medium_selective"
    assert profile["WARMUP"] == "2"
    assert profile["ITERS"] == "15"
    assert profile["PREWARM_ITERS"] == "1"
    assert profile["COMPARE_RUNS"] == "7"
    assert profile["MEASURE_ORDER"] == "alternate"
    assert profile["TIMING_PHASE"] == "execute"
    assert profile["AGGREGATION"] == "median"
    assert profile["SPREAD_METRIC"] == "iqr_ms"
    assert profile["SUB_MS_THRESHOLD_MS"] == "1.0"
    assert profile["SUB_MS_POLICY"] == "micro_only"


def test_compare_branch_docs_describe_replay_bench_workflow() -> None:
    combined = "\n".join(
        (
            README_DOC.read_text(encoding="utf-8"),
            COMPARE_DOC.read_text(encoding="utf-8"),
            REFERENCE_DOC.read_text(encoding="utf-8"),
        )
    )

    assert "replay-state" in combined
    assert "metadata_replay_bench" in combined
    assert "scan_planning" not in combined
    assert "scan-plan" not in combined


def test_compare_branch_does_not_advertise_scan_planning_suite_or_profile() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "scan_planning" not in script
    assert "scan-plan" not in script


def test_docs_distinguish_execute_guardrail_from_planning_probe() -> None:
    combined = "\n".join(
        (
            COMPARE_DOC.read_text(encoding="utf-8"),
            REFERENCE_DOC.read_text(encoding="utf-8"),
        )
    )

    assert "execute-phase guardrail" in combined
    assert "investigation-grade" in combined
    assert "timing_phase=plan" in combined
    assert "replay-state" in combined


def test_compare_branch_usage_and_errors_include_metadata_perf_surface() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")

    assert "metadata_perf" in script
    assert "metadata_replay_bench" in script
    assert "scan_replay_bench" not in script


def test_compare_branch_does_not_retry_benchmark_producing_steps() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "run_step_no_retry()" in script
    assert "data_cmd=(./scripts/bench.sh data --scale sf1 --seed 42)" in script
    assert re.search(r'run_step_no_retry env .*?"\$\{data_cmd\[@\]\}"', script)
    assert "run_benchmark_suite_for_checkout" in script
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
        r'compare_common_args=\(--mode "\$\{compare_mode\}" --noise-threshold "\$\{NOISE_THRESHOLD\}" --aggregation "\$\{aggregation\}"\)',
        script,
    )
    assert re.search(
        r"compare_args=\(\"\\?\$\{compare_common_args\[@\]\}\" --format text\)",
        script,
    )


def test_compare_branch_supports_methodology_profile_flag() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--methodology-profile <name>" in script


def test_compare_branch_references_pr_macro_methodology_profile() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "bench/methodologies/pr-macro.env" in script


def test_compare_branch_accepts_pr_macro_methodology_profile_name() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "pr-macro" in script


def test_pr_write_perf_profile_uses_intrinsic_case_workload_policy() -> None:
    profile = read_env_file(PR_WRITE_PERF_PROFILE)

    assert profile["METHODOLOGY_PROFILE"] == "pr-write-perf"
    assert profile["TARGET"] == "write_perf"
    assert profile["DATASET_POLICY"] == "intrinsic_case_workload"
    assert "DATASET_ID" not in profile


def test_delete_update_perf_merge_perf_optimize_perf_profiles_use_medium_selective_compare_contract() -> (
    None
):
    for profile_path, expected_profile, expected_target in (
        (
            PR_DELETE_UPDATE_PERF_PROFILE,
            "pr-delete-update-perf",
            "delete_update_perf",
        ),
        (PR_MERGE_PERF_PROFILE, "pr-merge-perf", "merge_perf"),
        (PR_OPTIMIZE_PERF_PROFILE, "pr-optimize-perf", "optimize_perf"),
    ):
        profile = read_env_file(profile_path)

        assert profile["METHODOLOGY_PROFILE"] == expected_profile
        assert profile["TARGET"] == expected_target
        assert profile["PROFILE_KIND"] == "compare"
        assert profile["DATASET_ID"] == "medium_selective"
        assert profile["COMPARE_MODE"] == "decision"
        assert profile["MEASURE_ORDER"] == "alternate"
        assert profile["TIMING_PHASE"] == "execute"
        assert profile["AGGREGATION"] == "median"
        assert profile["DATASET_POLICY"] == "shared_run_scope"
        assert profile["SPREAD_METRIC"] == "iqr_ms"

    delete_update_profile = read_env_file(PR_DELETE_UPDATE_PERF_PROFILE)
    assert delete_update_profile["METHODOLOGY_VERSION"] == "2"
    assert delete_update_profile["WARMUP"] == "1"
    assert delete_update_profile["ITERS"] == "2"
    assert delete_update_profile["PREWARM_ITERS"] == "0"


def test_delete_update_perf_high_confidence_profile_preserves_longer_contract() -> None:
    profile = read_env_file(
        REPO_ROOT
        / "bench"
        / "methodologies"
        / "delete-update-perf-high-confidence.env"
    )

    assert profile["METHODOLOGY_PROFILE"] == "delete-update-perf-high-confidence"
    assert profile["TARGET"] == "delete_update_perf"
    assert profile["PROFILE_KIND"] == "compare"
    assert profile["DATASET_ID"] == "medium_selective"
    assert profile["COMPARE_MODE"] == "decision"
    assert profile["WARMUP"] == "1"
    assert profile["ITERS"] == "5"
    assert profile["PREWARM_ITERS"] == "1"
    assert profile["COMPARE_RUNS"] == "5"
    assert profile["MEASURE_ORDER"] == "alternate"
    assert profile["TIMING_PHASE"] == "execute"
    assert profile["AGGREGATION"] == "median"
    assert profile["DATASET_POLICY"] == "shared_run_scope"
    assert profile["SPREAD_METRIC"] == "iqr_ms"


def test_metadata_perf_profile_uses_many_versions_compare_contract() -> None:
    profile = read_env_file(PR_METADATA_PERF_PROFILE)

    assert profile["METHODOLOGY_PROFILE"] == "pr-metadata-perf"
    assert profile["TARGET"] == "metadata_perf"
    assert profile["PROFILE_KIND"] == "compare"
    assert profile["DATASET_ID"] == "many_versions"
    assert profile["COMPARE_MODE"] == "decision"
    assert profile["MEASURE_ORDER"] == "alternate"
    assert profile["TIMING_PHASE"] == "execute"
    assert profile["AGGREGATION"] == "median"
    assert profile["DATASET_POLICY"] == "shared_run_scope"
    assert profile["SPREAD_METRIC"] == "iqr_ms"


def test_metadata_replay_profile_stays_criterion_only() -> None:
    profile = read_env_file(METADATA_REPLAY_CRITERION_PROFILE)

    assert profile["METHODOLOGY_PROFILE"] == "metadata-replay-criterion"
    assert profile["PROFILE_KIND"] == "criterion"
    assert profile["TARGET"] == "metadata_perf"
    assert profile["CRITERION_BENCH"] == "metadata_replay_bench"


def test_remote_candidate_profiles_exist_and_pin_s3_backend_defaults() -> None:
    for profile_name, profile_path in S3_CANDIDATE_PROFILES.items():
        profile = read_env_file(profile_path)
        assert profile["PROFILE_KIND"] == "compare"
        assert profile["STORAGE_BACKEND"] == "s3"
        assert profile["BACKEND_PROFILE"] == "s3_locking_vultr"
        assert profile["COMPARE_MODE"] == "decision"
        assert profile["MEASURE_ORDER"] == "alternate"
        assert profile["TIMING_PHASE"] == "execute"
        assert profile["AGGREGATION"] == "median"
        assert profile["SPREAD_METRIC"] == "iqr_ms"


def test_perf_owned_suites_no_longer_hard_reject_non_local_backends() -> None:
    for suite_path in (
        WRITE_PERF_SUITE,
        DELETE_UPDATE_PERF_SUITE,
        MERGE_PERF_SUITE,
        OPTIMIZE_PERF_SUITE,
        METADATA_PERF_SUITE,
    ):
        source = suite_path.read_text(encoding="utf-8")
        assert "does not support non-local storage backend yet" not in source


def test_compare_branch_derives_compare_and_manifest_args_from_shared_methodology_settings() -> (
    None
):
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "build_resolved_methodology_settings()" in script
    assert "resolved_methodology_settings=(" in script
    assert "build_compare_common_args()" in script
    assert "build_manifest_methodology_args()" in script
    assert re.search(r"build_resolved_methodology_settings\s*\n", script)
    assert re.search(r"build_compare_common_args\s*\n", script)
    assert re.search(r"build_manifest_methodology_args\s*\n", script)


def test_compare_branch_forwards_spread_metric_and_sub_ms_args_to_compare() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert re.search(
        r'compare_common_args\+=\(--spread-metric "\$\{spread_metric\}"\)', script
    )
    assert re.search(
        r'compare_common_args\+=\(--sub-ms-threshold-ms "\$\{sub_ms_threshold_ms\}"\)',
        script,
    )
    assert re.search(
        r'compare_common_args\+=\(--sub-ms-policy "\$\{sub_ms_policy\}"\)', script
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
        r'compare_common_args=\(--mode "\$\{compare_mode\}" --noise-threshold "\$\{NOISE_THRESHOLD\}" --aggregation "\$\{aggregation\}"\)',
        script,
    )
    assert re.search(
        r'compare_args=\("?\$\{compare_common_args\[@\]\}"? --format text\)',
        script,
    )


def test_compare_branch_supports_fail_on_passthrough() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--fail-on <statuses>" in script
    assert re.search(r'COMPARE_FAIL_ON="\$\{BENCH_COMPARE_FAIL_ON:-\}"', script)
    assert re.search(r'compare_args\+=\(--fail-on "\$\{COMPARE_FAIL_ON\}"\)', script)


def test_compare_branch_decision_mode_requires_at_least_five_runs() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert 'if [[ "${COMPARE_MODE}" == "decision" ]]' in script
    assert "decision mode requires --compare-runs >= 5" in script


def test_bench_run_requires_correctness_lane_for_assert_mode() -> None:
    script = BENCH_SH.read_text(encoding="utf-8")
    assert (
        'if [[ "${benchmark_mode}" == "assert" && "${lane}" != "correctness" ]]'
        in script
    )
    assert "--mode assert requires --lane correctness" in script


def test_perf_validation_workflow_entrypoint_exists_and_is_executable() -> None:
    assert VALIDATION_SCRIPT.exists(), "missing scripts/validate_perf_harness.sh"
    assert (
        VALIDATION_SCRIPT.stat().st_mode & 0o111
    ), "scripts/validate_perf_harness.sh must be executable"


def test_publish_contract_entrypoint_exists_and_is_executable() -> None:
    assert PUBLISH_CONTRACT_SCRIPT.exists(), "missing scripts/publish_contract.sh"
    assert (
        PUBLISH_CONTRACT_SCRIPT.stat().st_mode & 0o111
    ), "scripts/publish_contract.sh must be executable"


def test_publish_contract_captures_current_operator_docs_and_entrypoints(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "contract"
    result = subprocess.run(
        [
            "bash",
            str(PUBLISH_CONTRACT_SCRIPT),
            "--output-dir",
            str(output_dir),
        ],
        cwd=REPO_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr or result.stdout

    manifest = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    published_files = set(manifest["files"])
    expected_files = {
        "README.md",
        *[
            str(path.relative_to(REPO_ROOT))
            for path in sorted((REPO_ROOT / "docs").glob("*.md"))
        ],
        "bench/manifests/core_python.yaml",
        "bench/manifests/core_rust.yaml",
        "scripts/bench.sh",
        "scripts/cleanup_local.sh",
        "scripts/compare_branch.sh",
        "scripts/longitudinal_bench.sh",
        "scripts/publish_contract.sh",
        "scripts/validate_perf_harness.sh",
    }

    assert expected_files <= published_files
    for relative_path in expected_files:
        assert (output_dir / relative_path).is_file(), relative_path


def test_validation_script_exposes_artifact_dir_contract() -> None:
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")
    assert "--artifact-dir <path>" in script
    assert "SUMMARY_FILE" in script
    assert "summary.md" in script


def test_validation_script_seeds_same_sha_compare_from_prepared_execution_checkout() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        validate_copy, _, compare_log, managed_checkout, fake_bin = (
            install_validation_script_stubs(temp_root)
        )

        validation_sha = "3fe2fa92a1dc54c8c6b378529b449f5f4c601e39"
        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["DELTA_RS_DIR"] = str(managed_checkout)
        env["VALIDATION_SHA"] = validation_sha
        env["VALIDATION_ARTIFACT_DIR"] = str(temp_root / "results" / "validation")

        result = subprocess.run(
            ["bash", str(validate_copy)],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 99, result.stderr or result.stdout
        compare_entries = [
            json.loads(line)
            for line in compare_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        assert compare_entries, "expected compare_branch.sh to be invoked"
        first_compare = compare_entries[0]["argv"]
        assert "--base-fetch-url" in first_compare
        assert "--candidate-fetch-url" in first_compare
        assert first_compare[first_compare.index("--base-fetch-url") + 1] == str(
            managed_checkout
        )
        assert first_compare[first_compare.index("--candidate-fetch-url") + 1] == str(
            managed_checkout
        )


def test_validation_script_threads_validation_fetch_contract_into_checkout_prep() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        validate_copy, prepare_log, _, managed_checkout, fake_bin = (
            install_validation_script_stubs(temp_root)
        )

        validation_sha = "3fe2fa92a1dc54c8c6b378529b449f5f4c601e39"
        fetch_url = str(temp_root / "fork.git")
        fetch_ref = "refs/heads/pr-head"
        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["DELTA_RS_DIR"] = str(managed_checkout)
        env["VALIDATION_SHA"] = validation_sha
        env["VALIDATION_FETCH_URL"] = fetch_url
        env["VALIDATION_FETCH_REF"] = fetch_ref
        env["VALIDATION_ARTIFACT_DIR"] = str(temp_root / "results" / "validation")

        result = subprocess.run(
            ["bash", str(validate_copy)],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 99, result.stderr or result.stdout
        prepare_entries = [
            json.loads(line)
            for line in prepare_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        assert prepare_entries, "expected prepare_delta_rs.sh to be invoked"
        assert prepare_entries[-1]["ref"] == validation_sha
        assert prepare_entries[-1]["fetch_url"] == fetch_url
        assert prepare_entries[-1]["fetch_ref"] == fetch_ref


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


def test_validation_script_covers_write_perf_same_sha_and_regression_canary() -> None:
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")

    assert "Running write_perf same-SHA branch compare..." in script
    assert "Running write_perf regression-detection canary..." in script
    assert "--methodology-profile pr-write-perf" in script
    assert "write_perf_unpartitioned_1m" in script
    assert "DELTA_BENCH_ALLOW_WRITE_PERF_DELAY=1" in script
    assert "DELTA_BENCH_WRITE_PERF_DELAY_MS" in script


def test_validation_script_covers_delete_update_perf_merge_perf_and_optimize_perf_canaries() -> (
    None
):
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")

    for banner, profile_name, case_name, allow_env, delay_env in (
        (
            "Running delete_update_perf same-SHA branch compare...",
            "pr-delete-update-perf",
            "delete_perf_scattered_5pct_small_files",
            "DELTA_BENCH_ALLOW_DELETE_UPDATE_PERF_DELAY=1",
            "DELTA_BENCH_DELETE_UPDATE_PERF_DELAY_MS",
        ),
        (
            "Running merge_perf same-SHA branch compare...",
            "pr-merge-perf",
            "merge_perf_upsert_50pct",
            "DELTA_BENCH_ALLOW_MERGE_PERF_DELAY=1",
            "DELTA_BENCH_MERGE_PERF_DELAY_MS",
        ),
        (
            "Running optimize_perf same-SHA branch compare...",
            "pr-optimize-perf",
            "optimize_perf_compact_small_files",
            "DELTA_BENCH_ALLOW_OPTIMIZE_PERF_DELAY=1",
            "DELTA_BENCH_OPTIMIZE_PERF_DELAY_MS",
        ),
    ):
        assert banner in script
        assert f"--methodology-profile {profile_name}" in script
        assert case_name in script
        assert allow_env in script
        assert delay_env in script


def test_validation_script_covers_metadata_perf_same_sha_and_regression_canary() -> (
    None
):
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")

    assert "Running metadata_perf same-SHA branch compare..." in script
    assert "Running metadata_perf regression-detection canary..." in script
    assert "--methodology-profile pr-metadata-perf" in script
    assert "metadata_perf_load_head_long_history" in script
    assert "DELTA_BENCH_ALLOW_METADATA_PERF_DELAY=1" in script
    assert "DELTA_BENCH_METADATA_PERF_DELAY_MS" in script


def test_validation_script_embedded_python_helpers_compile() -> None:
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")

    for function_name in (
        "assert_same_sha_compare_is_fail_closed",
        "assert_payload_contains_cases",
        "assert_phase_canary",
        "compute_regression_canary_delay_ms",
        "assert_regression_canary_detected",
    ):
        source = embedded_python_from_shell_function(script, function_name)
        compile(source, f"<{function_name}>", "exec")


def test_validation_script_regression_canary_helper_executes_successfully(
    tmp_path: Path,
) -> None:
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")
    function_block = shell_function_block(script, "assert_regression_canary_detected")
    case_name = "delete_perf_scattered_5pct_small_files"

    def write_payload(path: Path, *, label: str, run_medians_ms: list[float]) -> None:
        median_ms = run_medians_ms[len(run_medians_ms) // 2]
        payload = {
            "schema_version": 5,
            "context": {
                "schema_version": 5,
                "label": label,
                "suite": "delete_update_perf",
                "benchmark_mode": "perf",
                "timing_phase": "execute",
                "dataset_id": "medium_selective",
                "dataset_fingerprint": "sha256:fixture",
                "runner": "rust",
                "scale": "sf1",
                "storage_backend": "local",
                "backend_profile": "local",
                "lane": "macro",
                "measurement_kind": "phase_breakdown",
                "validation_level": "operational",
                "harness_revision": "harness-rev",
                "fixture_recipe_hash": "sha256:recipe",
                "fidelity_fingerprint": "sha256:fidelity",
            },
            "cases": [
                {
                    "case": case_name,
                    "success": True,
                    "validation_passed": True,
                    "perf_status": "trusted",
                    "classification": "supported",
                    "samples": [{"elapsed_ms": median_ms, "metrics": {}}],
                    "run_summary": {
                        "sample_count": 1,
                        "invalid_sample_count": 0,
                        "median_ms": median_ms,
                        "host_label": "bench-host",
                        "fidelity_fingerprint": "sha256:fidelity",
                    },
                    "run_summaries": [
                        {
                            "sample_count": 1,
                            "invalid_sample_count": 0,
                            "median_ms": value,
                            "host_label": "bench-host",
                            "fidelity_fingerprint": "sha256:fidelity",
                        }
                        for value in run_medians_ms
                    ],
                    "compatibility_key": "sha256:good",
                    "supports_decision": True,
                    "required_runs": 5,
                    "decision_threshold_pct": 5.0,
                    "decision_metric": "median",
                }
            ],
        }
        path.write_text(json.dumps(payload), encoding="utf-8")

    baseline_json = tmp_path / "baseline.json"
    candidate_json = tmp_path / "candidate.json"
    write_payload(
        baseline_json,
        label="baseline",
        run_medians_ms=[100.0, 101.0, 99.0, 100.5, 100.2],
    )
    write_payload(
        candidate_json,
        label="candidate",
        run_medians_ms=[120.0, 121.0, 119.0, 120.5, 120.2],
    )

    result = subprocess.run(
        [
            "bash",
            "-c",
            (
                "set -euo pipefail\n"
                f'PYTHONPATH_DIR="{REPO_ROOT / "python"}"\n'
                f"{function_block}\n"
                'assert_regression_canary_detected "$1" "$2" "$3"\n'
            ),
            "assert_regression_canary_detected_test",
            str(baseline_json),
            str(candidate_json),
            case_name,
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert f"Regression canary: {case_name} classified as regression" in result.stdout


def test_validation_script_regression_canary_delay_helper_scales_slow_baselines(
    tmp_path: Path,
) -> None:
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")
    delay_function_block = shell_function_block(
        script, "compute_regression_canary_delay_ms"
    )
    assert_function_block = shell_function_block(script, "assert_regression_canary_detected")
    case_name = "scan_filter_flag"

    def write_payload(path: Path, *, label: str, run_medians_ms: list[float]) -> None:
        median_ms = run_medians_ms[len(run_medians_ms) // 2]
        payload = {
            "schema_version": 5,
            "context": {
                "schema_version": 5,
                "label": label,
                "suite": "scan",
                "benchmark_mode": "perf",
                "timing_phase": "execute",
                "dataset_id": "medium_selective",
                "dataset_fingerprint": "sha256:fixture",
                "runner": "rust",
                "scale": "sf1",
                "storage_backend": "local",
                "backend_profile": "local",
                "lane": "macro",
                "measurement_kind": "phase_breakdown",
                "validation_level": "operational",
                "harness_revision": "harness-rev",
                "fixture_recipe_hash": "sha256:recipe",
                "fidelity_fingerprint": "sha256:fidelity",
            },
            "cases": [
                {
                    "case": case_name,
                    "success": True,
                    "validation_passed": True,
                    "perf_status": "trusted",
                    "classification": "supported",
                    "samples": [{"elapsed_ms": median_ms, "metrics": {}}],
                    "run_summary": {
                        "sample_count": 1,
                        "invalid_sample_count": 0,
                        "median_ms": median_ms,
                        "host_label": "bench-host",
                        "fidelity_fingerprint": "sha256:fidelity",
                    },
                    "run_summaries": [
                        {
                            "sample_count": 1,
                            "invalid_sample_count": 0,
                            "median_ms": value,
                            "host_label": "bench-host",
                            "fidelity_fingerprint": "sha256:fidelity",
                        }
                        for value in run_medians_ms
                    ],
                    "compatibility_key": "sha256:good",
                    "supports_decision": True,
                    "required_runs": 5,
                    "decision_threshold_pct": 5.0,
                    "decision_metric": "median",
                }
            ],
        }
        path.write_text(json.dumps(payload), encoding="utf-8")

    baseline_json = tmp_path / "baseline.json"
    candidate_json = tmp_path / "candidate.json"
    write_payload(
        baseline_json,
        label="baseline",
        run_medians_ms=[3180.0, 3190.0, 3200.0, 3210.0, 3220.0],
    )

    result = subprocess.run(
        [
            "bash",
            "-c",
            (
                "set -euo pipefail\n"
                f'PYTHONPATH_DIR="{REPO_ROOT / "python"}"\n'
                f"{delay_function_block}\n"
                f"{assert_function_block}\n"
                'delay="$(compute_regression_canary_delay_ms "$1" "$2" "$3")"\n'
                'python3 - "$1" "$2" "$delay" "$4" <<\'PY\'\n'
                "import json\n"
                "import sys\n"
                "from pathlib import Path\n"
                "\n"
                "baseline = json.loads(Path(sys.argv[1]).read_text(encoding='utf-8'))\n"
                "case_name = sys.argv[2]\n"
                "delay_ms = float(sys.argv[3])\n"
                "candidate_path = Path(sys.argv[4])\n"
                "candidate = json.loads(json.dumps(baseline))\n"
                "candidate['context']['label'] = 'candidate'\n"
                "case = candidate['cases'][0]\n"
                "case['run_summaries'] = [\n"
                "    {\n"
                "        **summary,\n"
                "        'median_ms': float(summary['median_ms']) + delay_ms,\n"
                "    }\n"
                "    for summary in case['run_summaries']\n"
                "]\n"
                "candidate_medians = [\n"
                "    float(summary['median_ms']) for summary in case['run_summaries']\n"
                "]\n"
                "case['run_summary']['median_ms'] = candidate_medians[len(candidate_medians) // 2]\n"
                "case['samples'] = [{'elapsed_ms': value, 'metrics': {}} for value in candidate_medians]\n"
                "candidate_path.write_text(json.dumps(candidate), encoding='utf-8')\n"
                "PY\n"
                'assert_regression_canary_detected "$1" "$4" "$2"\n'
                'printf "delay=%s\\n" "$delay"\n'
            ),
            "compute_regression_canary_delay_ms_test",
            str(baseline_json),
            case_name,
            "150",
            str(candidate_json),
        ],
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert f"Regression canary: {case_name} classified as regression" in result.stdout
    delay_line = next(
        line for line in result.stdout.splitlines() if line.startswith("delay=")
    )
    assert float(delay_line.removeprefix("delay=")) == pytest.approx(320.0)


def test_validation_script_keeps_scan_and_perf_owned_gates_on_their_contract_dataset() -> (
    None
):
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")

    assert 'PRIMARY_VALIDATION_DATASET_ID="medium_selective"' in script
    assert 'TPCDS_VALIDATION_DATASET_ID="tpcds_duckdb"' in script
    assert (
        'PRIMARY_FIXTURES_DIR="${VALIDATION_ARTIFACT_DIR}/fixtures-medium_selective"'
        in script
    )
    assert (
        'TPCDS_FIXTURES_DIR="${VALIDATION_ARTIFACT_DIR}/fixtures-tpcds_duckdb"'
        in script
    )
    assert '--dataset-id "${PRIMARY_VALIDATION_DATASET_ID}"' in script
    assert '--dataset-id "${TPCDS_VALIDATION_DATASET_ID}"' in script
    assert "Use --dataset-id tpcds_duckdb to enable it." in script


def test_validation_script_keeps_write_perf_canary_case_fixed() -> None:
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")

    assert "write_perf_unpartitioned_1m" in script
    assert "VALIDATION_WRITE_PERF_CASE" not in script


def test_write_perf_suite_contains_validation_only_delay_canary_contract() -> None:
    source = WRITE_PERF_SUITE.read_text(encoding="utf-8")

    assert "DELTA_BENCH_ALLOW_WRITE_PERF_DELAY" in source
    assert "DELTA_BENCH_WRITE_PERF_DELAY_MS" in source
    assert "validation-only write_perf delay injection requires" in source
    assert "write_perf_unpartitioned_1m" in source


def test_delete_update_perf_merge_perf_and_optimize_perf_suites_contain_validation_only_delay_canary_contracts() -> (
    None
):
    for source_path, allow_env, delay_env, message, case_name in (
        (
            DELETE_UPDATE_PERF_SUITE,
            "DELTA_BENCH_ALLOW_DELETE_UPDATE_PERF_DELAY",
            "DELTA_BENCH_DELETE_UPDATE_PERF_DELAY_MS",
            "validation-only delete_update_perf delay injection requires",
            "delete_perf_scattered_5pct_small_files",
        ),
        (
            MERGE_PERF_SUITE,
            "DELTA_BENCH_ALLOW_MERGE_PERF_DELAY",
            "DELTA_BENCH_MERGE_PERF_DELAY_MS",
            "validation-only merge_perf delay injection requires",
            "merge_perf_upsert_50pct",
        ),
        (
            OPTIMIZE_PERF_SUITE,
            "DELTA_BENCH_ALLOW_OPTIMIZE_PERF_DELAY",
            "DELTA_BENCH_OPTIMIZE_PERF_DELAY_MS",
            "validation-only optimize_perf delay injection requires",
            "optimize_perf_compact_small_files",
        ),
    ):
        source = source_path.read_text(encoding="utf-8")

        assert allow_env in source
        assert delay_env in source
        assert message in source
        assert case_name in source


def test_metadata_perf_suite_contains_validation_only_delay_canary_contract() -> None:
    source = METADATA_PERF_SUITE.read_text(encoding="utf-8")

    assert "DELTA_BENCH_ALLOW_METADATA_PERF_DELAY" in source
    assert "DELTA_BENCH_METADATA_PERF_DELAY_MS" in source
    assert "validation-only metadata_perf delay injection requires" in source
    assert "metadata_perf_load_head_long_history" in source


def test_validation_script_covers_tpcds_same_sha_and_regression_canary() -> None:
    script = VALIDATION_SCRIPT.read_text(encoding="utf-8")

    assert "Running tpcds same-SHA branch compare..." in script
    assert "Running tpcds regression-detection canary..." in script
    assert "--methodology-profile pr-tpcds" in script
    assert "tpcds_q03" in script
    assert "DELTA_BENCH_ALLOW_TPCDS_DELAY=1" in script
    assert "DELTA_BENCH_TPCDS_DELAY_MS" in script


def test_tpcds_suite_contains_validation_only_delay_canary_contract() -> None:
    source = TPCDS_SUITE.read_text(encoding="utf-8")

    assert "DELTA_BENCH_ALLOW_TPCDS_DELAY" in source
    assert "DELTA_BENCH_TPCDS_DELAY_MS" in source
    assert "validation-only tpcds delay injection requires" in source
    assert "tpcds_q03" in source


def test_evidence_registry_lists_delete_update_perf_merge_perf_and_optimize_perf_as_candidate_manual() -> (
    None
):
    from delta_bench_compare.registry import load_registry, pack_suite_definitions

    registry = load_registry(EVIDENCE_REGISTRY)

    for suite_name, default_profile in (
        ("delete_update_perf", "pr-delete-update-perf"),
        ("merge_perf", "pr-merge-perf"),
        ("optimize_perf", "pr-optimize-perf"),
    ):
        suite = registry["suites"][suite_name]
        assert suite["class"] == "authoritative_macro"
        assert suite["automation_tier"] == "candidate_pr_bot"
        assert suite["readiness"] == "gated"
        assert suite["default_profile"] == default_profile

    candidate_pack = registry["packs"].get("pr-candidate-manual")
    assert isinstance(candidate_pack, dict)
    candidate_entries = {
        entry["suite"]: entry for entry in pack_suite_definitions(registry, candidate_pack)
    }
    assert candidate_entries["delete_update_perf"]["profile"] == (
        "delete-update-perf-high-confidence"
    )
    assert "merge_perf" in candidate_entries
    assert "optimize_perf" in candidate_entries


def test_evidence_registry_lists_metadata_perf_as_candidate_manual() -> None:
    from delta_bench_compare.registry import load_registry, pack_suite_definitions

    registry = load_registry(EVIDENCE_REGISTRY)
    suite = registry["suites"]["metadata_perf"]
    assert suite["class"] == "authoritative_macro"
    assert suite["automation_tier"] == "candidate_pr_bot"
    assert suite["readiness"] == "gated"
    assert suite["default_profile"] == "pr-metadata-perf"

    candidate_pack = registry["packs"].get("pr-candidate-manual")
    assert isinstance(candidate_pack, dict)
    candidate_suites = [
        entry["suite"] for entry in pack_suite_definitions(registry, candidate_pack)
    ]
    assert "metadata_perf" in candidate_suites


def test_evidence_registry_replacement_surfaces_reference_real_suites() -> None:
    from delta_bench_compare.registry import load_registry

    registry = load_registry(EVIDENCE_REGISTRY)
    declared_suites = set(registry["suites"])

    for suite_name, suite in registry["suites"].items():
        replacement_surface = suite.get("replacement_surface")
        if replacement_surface is None:
            continue
        assert replacement_surface in declared_suites, (
            f"{suite_name} replacement_surface points at unknown suite "
            f"{replacement_surface!r}"
        )


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


def test_reference_docs_cover_compare_source_checkout_and_preflight_controls() -> None:
    reference = REFERENCE_DOC.read_text(encoding="utf-8")
    getting_started = GETTING_STARTED_DOC.read_text(encoding="utf-8")

    assert "DELTA_RS_SOURCE_DIR" in reference
    assert "DELTA_BENCH_MIN_FREE_GB" in reference
    assert "delta-rs-source" in getting_started


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
    assert "status_label=PASS" in workflow
    assert "status_label=FAIL" in workflow
    assert "Compare mode" in workflow


def test_benchmark_workflow_uses_pr_macro_methodology_profile_for_decision_runs() -> (
    None
):
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "profile: pr-macro" in EVIDENCE_REGISTRY.read_text(encoding="utf-8")
    assert "./scripts/run_profile.sh" in workflow


def test_benchmark_workflow_does_not_restate_pr_macro_methodology_knobs() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    for forbidden in (
        "--compare-mode decision",
        "--warmup",
        "--iters",
        "--prewarm-iters",
        "--compare-runs",
        "--measure-order",
        "--timing-phase",
        "--aggregation",
    ):
        assert forbidden not in workflow


def test_benchmark_workflow_keeps_single_suite_scan_path() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "run benchmark scan" in workflow
    assert "run benchmark decision scan" in workflow
    assert "suite == 'scan'" in workflow or 'suite === "scan"' in workflow


def test_benchmark_workflow_fails_job_when_compare_fails() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert 'if [[ "${status}" -ne 0 ]]; then' in workflow
    assert 'exit "${status}"' in workflow


def test_benchmark_workflow_posts_results_even_after_compare_step_failure() -> None:
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "- name: Post benchmark result" in workflow
    assert "if: always()" in workflow


def test_benchmark_workflow_pack_status_comment_fails_closed_when_any_shard_fails() -> (
    None
):
    workflow = WORKFLOW.read_text(encoding="utf-8")
    assert "const shardResult = `${{ needs.run_pack_shards.result }}`;" in workflow
    assert (
        "const overallStatus = `${{ steps.summarize.outputs.overall_status }}`;"
        in workflow
    )
    assert (
        'const finalStatus = shardResult === "success" && overallStatus === "passed"'
        in workflow
    )
    assert '`### Benchmark ${finalStatus ? "PASS" : "FAIL"}`' in workflow


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


def test_benchmark_nightly_uses_pack_driven_remote_candidate_path() -> None:
    workflow = NIGHTLY_WORKFLOW.read_text(encoding="utf-8")
    assert "s3-candidate-manual" in workflow
    assert "python3 -m delta_bench_compare.pack plan" in workflow
    assert "./scripts/run_profile.sh" in workflow
    assert "Run nightly object-store benchmark" not in workflow


def test_benchmark_nightly_accepts_storage_option_overrides_for_remote_pack() -> None:
    workflow = NIGHTLY_WORKFLOW.read_text(encoding="utf-8")
    assert "BENCH_STORAGE_OPTIONS" in workflow
    assert "storage_args = []" in workflow
    assert 'os.environ.get("BENCH_STORAGE_OPTIONS", "")' in workflow
    assert "command.extend(storage_args)" in workflow
    assert 'storage_args.extend(["--storage-option", opt])' in workflow


def test_benchmark_nightly_enforces_per_shard_timeouts_and_collects_failures() -> None:
    workflow = NIGHTLY_WORKFLOW.read_text(encoding="utf-8")
    assert 'timeout_minutes = int(shard["timeout_minutes"])' in workflow
    assert "timeout=timeout_minutes * 60" in workflow
    assert "check=False" in workflow
    assert "except subprocess.TimeoutExpired:" in workflow
    assert "failures.append(" in workflow
    assert "if failures:" in workflow


def test_benchmark_prerelease_exposes_declared_remote_candidate_surfaces() -> None:
    workflow = PRERELEASE_WORKFLOW.read_text(encoding="utf-8")
    assert "surface:" in workflow
    assert re.search(r"default:\s*scan_s3", workflow)
    assert "s3-candidate-manual" in workflow
    assert "prerelease-s3-pack-plan.json" in workflow
    assert "prerelease-s3-surface.json" in workflow
    for surface_name in (
        "scan_s3",
        "delete_update_perf_s3",
        "merge_perf_s3",
        "optimize_perf_s3",
        "metadata_perf_s3",
    ):
        assert surface_name in workflow
    assert "write_perf_s3" not in workflow
    assert "- python" not in workflow


def test_benchmark_prerelease_resolves_refs_with_remote_tracking_fallback() -> None:
    workflow = PRERELEASE_WORKFLOW.read_text(encoding="utf-8")
    assert "resolve_commit_ref()" in workflow
    assert 'git rev-parse --verify --quiet "${ref}^{commit}"' in workflow
    assert (
        'git rev-parse --verify --quiet "refs/remotes/origin/${ref}^{commit}"'
        in workflow
    )
    assert 'base_sha="$(resolve_commit_ref "${BASE_REF}")"' in workflow
    assert 'candidate_sha="$(resolve_commit_ref "${CANDIDATE_REF}")"' in workflow


def test_benchmark_prerelease_preserves_compare_hardening_and_storage_overrides() -> (
    None
):
    workflow = PRERELEASE_WORKFLOW.read_text(encoding="utf-8")
    assert "BENCH_STORAGE_OPTIONS" in workflow
    assert "storage_args=()" in workflow
    assert 'storage_args+=(--storage-option "${opt}")' in workflow
    assert "--enforce-run-mode \\" in workflow
    assert "--require-no-public-ipv4 \\" in workflow
    assert "--require-egress-policy \\" in workflow
    assert '"${storage_args[@]}" \\' in workflow


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
        'base_ref="$(pin_ref_to_commit "${base_ref}" "${base_ref_mode}" "${BASE_FETCH_URL}")"',
        start,
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


def test_compare_branch_runs_security_preflight_once_before_ref_pinning() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    start = script.index(
        'phase "${current_phase}" "${total_phases}" "Preparing delta-rs checkout and fixtures"'
    )
    end = script.index(
        'base_ref="$(pin_ref_to_commit "${base_ref}" "${base_ref_mode}" "${BASE_FETCH_URL}")"',
        start,
    )
    initial_block = script[start:end]
    assert initial_block.count("run_security_check") == 1
    assert_order(
        initial_block,
        "run_security_check",
        'base_requested_ref="${base_ref}"',
    )


def test_compare_branch_does_not_repeat_security_preflight_per_ref_checkout() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    start = script.index("prepare_ref_checkout_once() {")
    end = script.index("\n}\n\nrun_benchmark_suite_for_checkout", start) + 2
    function_body = script[start:end]
    assert "run_security_check" not in function_body


def test_compare_branch_pins_refs_once_before_labels_and_measured_runs() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "pin_ref_to_commit()" in script
    assert re.search(
        r'base_ref="\$\(pin_ref_to_commit \"\$\{base_ref\}\" \"\$\{base_ref_mode\}\" \"\$\{BASE_FETCH_URL\}\"\)"',
        script,
    )
    assert re.search(
        r'candidate_ref="\$\(pin_ref_to_commit \"\$\{candidate_ref\}\" \"\$\{candidate_ref_mode\}\" \"\$\{CANDIDATE_FETCH_URL\}\"\)"',
        script,
    )
    assert_order(
        script,
        'base_ref="$(pin_ref_to_commit "${base_ref}" "${base_ref_mode}" "${BASE_FETCH_URL}")"',
        'base_label="base-$(sanitize_label "${base_ref}")"',
    )


def test_compare_branch_supports_explicit_sha_flags() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--base-sha <sha>" in script
    assert "--candidate-sha <sha>" in script
    assert re.search(r"--base-sha\)\n\s+BASE_SHA_OVERRIDE=\"\$2\"", script)
    assert re.search(r"--candidate-sha\)\n\s+CANDIDATE_SHA_OVERRIDE=\"\$2\"", script)
    assert re.search(
        r'prepare_ref_checkout_once "\$\{base_ref\}" "\$\{base_checkout_dir\}" "\$\{BASE_FETCH_URL\}"',
        script,
    )
    assert re.search(
        r'run_benchmark_suite_for_checkout "\$\{candidate_checkout_dir\}" "\$\{run_label\}"',
        script,
    )
    assert re.search(r"if \[\[ \"\$\{mode\}\" == \"commit\" \]\]; then", script)


def test_compare_branch_supports_working_branch_vs_upstream_main_shortcut() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "--current-vs-main" in script
    assert "--working-vs-upstream-main" in script
    assert "--upstream-remote <name>" in script
    assert (
        'DELTA_RS_SOURCE_DIR="${DELTA_RS_SOURCE_DIR:-${RUNNER_ROOT}/.delta-rs-source}"'
        in script
    )
    assert re.search(r"WORKING_VS_UPSTREAM_MAIN=0", script)
    assert re.search(r"UPSTREAM_REMOTE_OVERRIDE=\"\"", script)
    assert re.search(
        r"if \(\(\s*WORKING_VS_UPSTREAM_MAIN != 0\s*\)\); then[\s\S]*candidate_ref=\"\$\{working_head_sha\}\"",
        script,
    )
    assert re.search(
        r"if \(\(\s*WORKING_VS_UPSTREAM_MAIN != 0\s*\)\); then[\s\S]*base_ref=\"\$\{upstream_main_sha\}\"",
        script,
    )
    assert re.search(
        r"git -C \"\$\{DELTA_RS_SOURCE_DIR\}\" fetch \"\$\{upstream_remote\}\" main",
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
    assert 'git -C "${DELTA_RS_SOURCE_DIR}" branch -a' in script
    assert re.search(
        r'ensure_known_ref_mode "\$\{candidate_ref\}" "\$\{candidate_ref_mode\}"[\s\S]*base_ref="\$\(pin_ref_to_commit "\$\{base_ref\}"',
        script,
    )


def test_compare_branch_defines_local_compare_preflight_for_disk_headroom() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert 'DELTA_BENCH_MIN_FREE_GB="${DELTA_BENCH_MIN_FREE_GB:-20}"' in script
    assert "run_local_compare_preflight" in script
    assert re.search(r'df -Pk "\$\{RUNNER_ROOT\}"', script)
    assert 'export CARGO_TARGET_DIR="$PWD/target"' in script


def test_prepare_delta_rs_supports_immutable_ref_checkout() -> None:
    script = PREPARE_DELTA_RS.read_text(encoding="utf-8")
    assert "DELTA_RS_REF" in script
    assert "DELTA_RS_REF_TYPE" in script
    assert re.search(r"checkout(?: -q)? --detach", script)
    assert re.search(r"pull(?: -q)? --ff-only origin", script)


def test_prepare_delta_rs_fetches_immutable_ref_from_alternate_remote_url() -> None:
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        prepare_copy = scripts_dir / "prepare_delta_rs.sh"
        copy_executable(PREPARE_DELTA_RS, prepare_copy)

        origin_repo, fork_repo, candidate_sha = create_origin_and_fork_repos(temp_root)

        env = os.environ.copy()
        env["DELTA_RS_DIR"] = str(temp_root / ".delta-rs-under-test")
        env["DELTA_RS_REPO_URL"] = str(origin_repo)
        env["DELTA_RS_REF"] = candidate_sha
        env["DELTA_RS_REF_TYPE"] = "commit"
        env["DELTA_RS_FETCH_URL"] = str(fork_repo)

        result = subprocess.run(
            ["bash", str(prepare_copy)],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr or result.stdout
        assert "delta-rs checkout ready:" in result.stdout
        resolved = subprocess.run(
            ["git", "-C", env["DELTA_RS_DIR"], "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        assert resolved == candidate_sha


def test_prepare_delta_rs_fetches_abbreviated_immutable_ref_from_alternate_remote_url() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        prepare_copy = scripts_dir / "prepare_delta_rs.sh"
        copy_executable(PREPARE_DELTA_RS, prepare_copy)

        origin_repo, fork_repo, candidate_sha = create_origin_and_fork_repos(temp_root)

        env = os.environ.copy()
        env["DELTA_RS_DIR"] = str(temp_root / ".delta-rs-under-test")
        env["DELTA_RS_REPO_URL"] = str(origin_repo)
        env["DELTA_RS_REF"] = candidate_sha[:12]
        env["DELTA_RS_REF_TYPE"] = "commit"
        env["DELTA_RS_FETCH_URL"] = str(fork_repo)

        result = subprocess.run(
            ["bash", str(prepare_copy)],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr or result.stdout
        resolved = subprocess.run(
            ["git", "-C", env["DELTA_RS_DIR"], "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()
        assert resolved == candidate_sha


def test_compare_branch_passes_candidate_fetch_url_for_candidate_sha() -> None:
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        copy_executable(COMPARE_BRANCH, compare_copy)

        prep_log = temp_root / "prep-log.jsonl"
        write_executable(
            scripts_dir / "prepare_delta_rs.sh",
            f"""#!/usr/bin/env python3
import json
import os
from pathlib import Path

checkout_dir = Path(os.environ["DELTA_RS_DIR"])
checkout_dir.mkdir(parents=True, exist_ok=True)
(checkout_dir / ".git").mkdir(exist_ok=True)
ref = os.environ.get("DELTA_RS_REF") or os.environ.get("DELTA_RS_BRANCH") or ""
(checkout_dir / ".bench-current-sha").write_text(ref or "0" * 40, encoding="utf-8")
with open({str(prep_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(
        json.dumps(
            {{
                "ref": os.environ.get("DELTA_RS_REF"),
                "branch": os.environ.get("DELTA_RS_BRANCH"),
                "ref_type": os.environ.get("DELTA_RS_REF_TYPE"),
                "fetch_url": os.environ.get("DELTA_RS_FETCH_URL"),
            }}
        )
        + "\\n"
    )
print(f"delta-rs checkout ready: {{checkout_dir}}")
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
        write_executable(
            scripts_dir / "bench.sh",
            """#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

args = sys.argv[1:]
command = args[0]
suite = "scan"
if "--suite" in args:
    suite = args[args.index("--suite") + 1]

results_dir = Path(os.environ["DELTA_BENCH_RESULTS"])
label = os.environ["DELTA_BENCH_LABEL"]
target_dir = results_dir / label
target_dir.mkdir(parents=True, exist_ok=True)

if command == "data":
    sys.exit(0)

if command == "run":
    (target_dir / f"{suite}.json").write_text(
        json.dumps({"label": label, "suite": suite}),
        encoding="utf-8",
    )
    sys.exit(0)

raise SystemExit(0)
""",
        )

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
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

        python_pkg = temp_root / "python" / "delta_bench_compare"
        python_pkg.mkdir(parents=True)
        (python_pkg / "__init__.py").write_text("", encoding="utf-8")
        copy_compare_manifest_helper(python_pkg)
        write_executable(
            python_pkg / "aggregate.py",
            """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--label", required=True)
parser.add_argument("--mode", default="decision")
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
print("hash policy ok")
""",
        )

        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"
        fork_repo = temp_root / "fork.git"
        fork_repo.mkdir()

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_RS_DIR"] = str(temp_root / ".delta-rs-under-test")
        env["DELTA_BENCH_RESULTS"] = str(temp_root / "results")
        relax_local_compare_disk_headroom(env)

        result = subprocess.run(
            [
                "bash",
                str(compare_copy),
                "--base-sha",
                base_sha,
                "--candidate-sha",
                candidate_sha,
                "--candidate-fetch-url",
                str(fork_repo),
                "scan",
            ],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr or result.stdout
        prep_entries = [
            json.loads(line)
            for line in prep_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        assert any(
            entry["ref"] == candidate_sha and entry["fetch_url"] == str(fork_repo)
            for entry in prep_entries
        )


def test_compare_branch_fails_closed_when_sha_pinning_misses_requested_commit() -> None:
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        copy_executable(COMPARE_BRANCH, compare_copy)

        managed_checkout = temp_root / ".delta-rs-under-test"
        (managed_checkout / ".git").mkdir(parents=True)
        (managed_checkout / "crates" / "delta-bench").mkdir(parents=True)
        (managed_checkout / "crates" / "delta-bench" / "Cargo.toml").write_text(
            "dirty overlay\n", encoding="utf-8"
        )

        stale_head = "1111111111111111111111111111111111111111"
        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"
        (managed_checkout / ".bench-current-sha").write_text(
            f"{stale_head}\n", encoding="utf-8"
        )

        write_executable(
            scripts_dir / "prepare_delta_rs.sh",
            f"""#!/usr/bin/env bash
set -euo pipefail

mkdir -p "${{DELTA_RS_DIR}}"
if [[ "${{DELTA_RS_DIR}}" == {str(managed_checkout)!r} ]] && [[ -n "${{DELTA_RS_REF:-}}" ]]; then
  echo "simulated checkout transition failure for ${{DELTA_RS_REF}}" >&2
  exit 42
fi

mkdir -p "${{DELTA_RS_DIR}}/.git"
ref="${{DELTA_RS_REF:-${{DELTA_RS_BRANCH:-}}}}"
if [[ -z "${{ref}}" ]]; then
  ref="{stale_head}"
fi
printf '%s\\n' "${{ref}}" > "${{DELTA_RS_DIR}}/.bench-current-sha"
printf 'delta-rs checkout ready: %s\\n' "${{DELTA_RS_DIR}}"
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
        write_executable(
            scripts_dir / "bench.sh",
            """#!/usr/bin/env python3
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
command = args[0]
suite = value(args, "--suite", "scan")
results_dir = Path(os.environ["DELTA_BENCH_RESULTS"])
label = os.environ["DELTA_BENCH_LABEL"]

if command == "data":
    sys.exit(0)

if command == "run":
    target_dir = results_dir / label
    target_dir.mkdir(parents=True, exist_ok=True)
    trusted_ms = 100.0 if label.startswith("base-") else 90.0
    payload = {
        "schema_version": 5,
        "context": {
            "schema_version": 5,
            "label": label,
            "suite": suite,
            "benchmark_mode": "perf",
            "timing_phase": "execute",
            "dataset_id": "tiny_smoke",
            "dataset_fingerprint": "sha256:fixture",
            "runner": "rust",
            "scale": "sf1",
            "storage_backend": "local",
            "backend_profile": "local",
            "lane": "macro",
            "measurement_kind": "phase_breakdown",
            "validation_level": "operational",
            "harness_revision": "harness-rev",
            "fixture_recipe_hash": "sha256:recipe",
            "fidelity_fingerprint": "sha256:fidelity",
        },
        "cases": [
            {
                "case": "scan_full_narrow",
                "success": True,
                "validation_passed": True,
                "perf_status": "trusted",
                "classification": "supported",
                "samples": [
                    {"elapsed_ms": trusted_ms, "metrics": {"files_scanned": 10}}
                ],
                "run_summary": {
                    "sample_count": 1,
                    "invalid_sample_count": 0,
                    "median_ms": trusted_ms,
                    "host_label": "bench-host",
                    "fidelity_fingerprint": "sha256:fidelity",
                },
                "compatibility_key": "sha256:good",
                "supports_decision": True,
                "required_runs": 5,
                "decision_threshold_pct": 5.0,
                "decision_metric": "median",
            }
        ],
    }
    (target_dir / f"{suite}.json").write_text(json.dumps(payload), encoding="utf-8")
    sys.exit(0)

raise SystemExit(0)
""",
        )

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
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

        python_pkg = temp_root / "python" / "delta_bench_compare"
        python_pkg.mkdir(parents=True)
        (python_pkg / "__init__.py").write_text("", encoding="utf-8")
        copy_compare_manifest_helper(python_pkg)
        write_executable(
            python_pkg / "aggregate.py",
            """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--label", required=True)
parser.add_argument("--mode", default="decision")
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
print("hash policy ok")
""",
        )

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_RS_DIR"] = str(managed_checkout)
        env["DELTA_RS_SOURCE_DIR"] = str(managed_checkout)
        env["DELTA_BENCH_RESULTS"] = str(temp_root / "results")
        relax_local_compare_disk_headroom(env)

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

        assert result.returncode != 0
        assert "requested commit" in result.stderr
        assert "was not pinned" in result.stderr
        assert f"Pinned base ref: {base_sha} -> {stale_head}" not in result.stdout
        assert (
            f"Pinned candidate ref: {candidate_sha} -> {stale_head}"
            not in result.stdout
        )


def test_compare_branch_accepts_abbreviated_sha_pins_when_source_checkout_resolves_full_commits() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        copy_executable(COMPARE_BRANCH, compare_copy)

        managed_checkout = temp_root / ".delta-rs-under-test"
        source_checkout = temp_root / ".delta-rs-source"
        (managed_checkout / ".git").mkdir(parents=True)
        (source_checkout / ".git").mkdir(parents=True)
        (managed_checkout / "crates" / "delta-bench").mkdir(parents=True)
        (managed_checkout / "crates" / "delta-bench" / "Cargo.toml").write_text(
            "dirty overlay\n", encoding="utf-8"
        )

        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"
        base_short = base_sha[:12]
        candidate_short = candidate_sha[:12]

        write_executable(
            scripts_dir / "prepare_delta_rs.sh",
            f"""#!/usr/bin/env python3
import os
from pathlib import Path

checkout_dir = Path(os.environ["DELTA_RS_DIR"])
checkout_dir.mkdir(parents=True, exist_ok=True)
(checkout_dir / ".git").mkdir(exist_ok=True)
ref = os.environ.get("DELTA_RS_REF") or os.environ.get("DELTA_RS_BRANCH") or ""
resolved = {{
    {base_short!r}: {base_sha!r},
    {candidate_short!r}: {candidate_sha!r},
}}.get(ref, ref or "0" * 40)
(checkout_dir / ".bench-current-sha").write_text(f"{{resolved}}\\n", encoding="utf-8")
print(f"delta-rs checkout ready: {{checkout_dir}}")
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
        write_executable(
            scripts_dir / "bench.sh",
            """#!/usr/bin/env python3
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
command = args[0]
suite = value(args, "--suite", "scan")
results_dir = Path(os.environ["DELTA_BENCH_RESULTS"])
label = os.environ["DELTA_BENCH_LABEL"]

if command == "data":
    sys.exit(0)

if command == "run":
    target_dir = results_dir / label
    target_dir.mkdir(parents=True, exist_ok=True)
    trusted_ms = 100.0 if label.startswith("base-") else 90.0
    payload = {
        "schema_version": 5,
        "context": {
            "schema_version": 5,
            "label": label,
            "suite": suite,
            "benchmark_mode": "perf",
            "timing_phase": "execute",
            "dataset_id": "tiny_smoke",
            "dataset_fingerprint": "sha256:fixture",
            "runner": "rust",
            "scale": "sf1",
            "storage_backend": "local",
            "backend_profile": "local",
            "lane": "macro",
            "measurement_kind": "phase_breakdown",
            "validation_level": "operational",
            "harness_revision": "harness-rev",
            "fixture_recipe_hash": "sha256:recipe",
            "fidelity_fingerprint": "sha256:fidelity",
        },
        "cases": [
            {
                "case": "scan_full_narrow",
                "success": True,
                "validation_passed": True,
                "perf_status": "trusted",
                "classification": "supported",
                "samples": [
                    {"elapsed_ms": trusted_ms, "metrics": {"files_scanned": 10}}
                ],
                "run_summary": {
                    "sample_count": 1,
                    "invalid_sample_count": 0,
                    "median_ms": trusted_ms,
                    "host_label": "bench-host",
                    "fidelity_fingerprint": "sha256:fidelity",
                },
                "compatibility_key": "sha256:good",
                "supports_decision": True,
                "required_runs": 5,
                "decision_threshold_pct": 5.0,
                "decision_metric": "median",
            }
        ],
    }
    (target_dir / f"{suite}.json").write_text(json.dumps(payload), encoding="utf-8")
    sys.exit(0)

raise SystemExit(0)
""",
        )

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
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
    remote)
      if [[ "${2:-}" == "get-url" ]]; then
        exit 0
      fi
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

        python_pkg = temp_root / "python" / "delta_bench_compare"
        python_pkg.mkdir(parents=True)
        (python_pkg / "__init__.py").write_text("", encoding="utf-8")
        copy_compare_manifest_helper(python_pkg)
        write_executable(
            python_pkg / "aggregate.py",
            """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--label", required=True)
parser.add_argument("--mode", default="decision")
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
print("hash policy ok")
""",
        )

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_BENCH_MIN_FREE_GB"] = "1"
        env["DELTA_RS_DIR"] = str(managed_checkout)
        env["DELTA_RS_SOURCE_DIR"] = str(source_checkout)
        env["DELTA_BENCH_RESULTS"] = str(temp_root / "results")

        result = subprocess.run(
            [
                "bash",
                str(compare_copy),
                "--base-sha",
                base_short,
                "--candidate-sha",
                candidate_short,
                "scan",
            ],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr or result.stdout
        assert f"Pinned base ref: {base_short} -> {base_sha}" in result.stdout
        assert (
            f"Pinned candidate ref: {candidate_short} -> {candidate_sha}"
            in result.stdout
        )


def test_compare_branch_prefers_hex_like_branch_refs_over_commit_interpretation_when_branch_exists() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        copy_executable(COMPARE_BRANCH, compare_copy)

        managed_checkout = temp_root / ".delta-rs-under-test"
        source_checkout = temp_root / ".delta-rs-source"
        (managed_checkout / ".git").mkdir(parents=True)
        (source_checkout / ".git").mkdir(parents=True)
        (managed_checkout / "crates" / "delta-bench").mkdir(parents=True)
        (managed_checkout / "crates" / "delta-bench" / "Cargo.toml").write_text(
            "dirty overlay\n", encoding="utf-8"
        )

        main_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        hex_branch_name = "deadbeefcafe"
        hex_branch_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"

        write_executable(
            scripts_dir / "prepare_delta_rs.sh",
            f"""#!/usr/bin/env python3
import os
from pathlib import Path

checkout_dir = Path(os.environ["DELTA_RS_DIR"])
checkout_dir.mkdir(parents=True, exist_ok=True)
(checkout_dir / ".git").mkdir(exist_ok=True)
ref = os.environ.get("DELTA_RS_REF") or os.environ.get("DELTA_RS_BRANCH") or ""
resolved = {{
    "main": {main_sha!r},
    {hex_branch_name!r}: {hex_branch_sha!r},
}}.get(ref, ref or "0" * 40)
(checkout_dir / ".bench-current-sha").write_text(f"{{resolved}}\\n", encoding="utf-8")
print(f"delta-rs checkout ready: {{checkout_dir}}")
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
        write_executable(
            scripts_dir / "bench.sh",
            """#!/usr/bin/env python3
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
command = args[0]
suite = value(args, "--suite", "scan")
results_dir = Path(os.environ["DELTA_BENCH_RESULTS"])
label = os.environ["DELTA_BENCH_LABEL"]

if command == "data":
    sys.exit(0)

if command == "run":
    target_dir = results_dir / label
    target_dir.mkdir(parents=True, exist_ok=True)
    trusted_ms = 100.0 if label.startswith("base-") else 90.0
    payload = {
        "schema_version": 5,
        "context": {
            "schema_version": 5,
            "label": label,
            "suite": suite,
            "benchmark_mode": "perf",
            "timing_phase": "execute",
            "dataset_id": "tiny_smoke",
            "dataset_fingerprint": "sha256:fixture",
            "runner": "rust",
            "scale": "sf1",
            "storage_backend": "local",
            "backend_profile": "local",
            "lane": "macro",
            "measurement_kind": "phase_breakdown",
            "validation_level": "operational",
            "harness_revision": "harness-rev",
            "fixture_recipe_hash": "sha256:recipe",
            "fidelity_fingerprint": "sha256:fidelity",
        },
        "cases": [
            {
                "case": "scan_full_narrow",
                "success": True,
                "validation_passed": True,
                "perf_status": "trusted",
                "classification": "supported",
                "samples": [{"elapsed_ms": trusted_ms, "metrics": {"files_scanned": 10}}],
                "run_summary": {
                    "sample_count": 1,
                    "invalid_sample_count": 0,
                    "median_ms": trusted_ms,
                    "host_label": "bench-host",
                    "fidelity_fingerprint": "sha256:fidelity",
                },
                "compatibility_key": "sha256:good",
                "supports_decision": True,
                "required_runs": 5,
                "decision_threshold_pct": 5.0,
                "decision_metric": "median",
            }
        ],
    }
    (target_dir / f"{suite}.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )
    sys.exit(0)

raise SystemExit(0)
""",
        )

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
        write_executable(
            fake_bin / "git",
            f"""#!/usr/bin/env bash
set -euo pipefail

if [[ "${{1:-}}" == "-C" ]]; then
  repo="$2"
  shift 2
  case "${{1:-}}" in
    clean|fetch|checkout|pull)
      exit 0
      ;;
    remote)
      if [[ "${{2:-}}" == "get-url" ]]; then
        exit 0
      fi
      ;;
    show-ref)
      case "${{@: -1}}" in
        refs/heads/main|refs/remotes/origin/main|refs/heads/{hex_branch_name}|refs/remotes/origin/{hex_branch_name})
          exit 0
          ;;
        *)
          exit 1
          ;;
      esac
      ;;
    rev-parse)
      if [[ "$*" == *"HEAD"* ]]; then
        cat "${{repo}}/.bench-current-sha"
        exit 0
      fi
      if [[ "${{@: -1}}" == "main^{{commit}}" ]]; then
        printf '%s\\n' {main_sha!r}
        exit 0
      fi
      if [[ "${{@: -1}}" == "{hex_branch_name}^{{commit}}" ]]; then
        printf '%s\\n' {hex_branch_sha!r}
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

        python_pkg = temp_root / "python" / "delta_bench_compare"
        python_pkg.mkdir(parents=True)
        (python_pkg / "__init__.py").write_text("", encoding="utf-8")
        copy_compare_manifest_helper(python_pkg)
        write_executable(
            python_pkg / "aggregate.py",
            """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--label", required=True)
parser.add_argument("--mode", default="decision")
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
print("hash policy ok")
""",
        )

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_BENCH_MIN_FREE_GB"] = "1"
        env["DELTA_RS_DIR"] = str(managed_checkout)
        env["DELTA_RS_SOURCE_DIR"] = str(source_checkout)
        env["DELTA_BENCH_RESULTS"] = str(temp_root / "results")

        result = subprocess.run(
            ["bash", str(compare_copy), "main", hex_branch_name, "scan"],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr or result.stdout
        assert f"Pinned base ref: main -> {main_sha}" in result.stdout
        assert (
            f"Pinned candidate ref: {hex_branch_name} -> {hex_branch_sha}"
            in result.stdout
        )


def test_compare_branch_uses_clean_source_checkout_for_immutable_sha_resolution() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        copy_executable(COMPARE_BRANCH, compare_copy)

        managed_checkout = temp_root / ".delta-rs-under-test"
        source_checkout = temp_root / ".delta-rs-source"
        compare_checkout_root = temp_root / ".delta-bench-compare-checkouts"
        (managed_checkout / ".git").mkdir(parents=True)
        (source_checkout / ".git").mkdir(parents=True)
        (managed_checkout / "crates" / "delta-bench").mkdir(parents=True)
        (managed_checkout / "crates" / "delta-bench" / "Cargo.toml").write_text(
            "dirty overlay\n", encoding="utf-8"
        )

        prep_log = temp_root / "prep-log.jsonl"
        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"

        write_executable(
            scripts_dir / "prepare_delta_rs.sh",
            f"""#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

checkout_dir = Path(os.environ["DELTA_RS_DIR"])
checkout_dir.mkdir(parents=True, exist_ok=True)
ref = os.environ.get("DELTA_RS_REF") or os.environ.get("DELTA_RS_BRANCH") or ""
fetch_url = os.environ.get("DELTA_RS_FETCH_URL", "")

with open({str(prep_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(
        json.dumps(
            {{
                "dir": os.environ["DELTA_RS_DIR"],
                "ref": os.environ.get("DELTA_RS_REF"),
                "branch": os.environ.get("DELTA_RS_BRANCH"),
                "fetch_url": fetch_url,
            }}
        )
        + "\\n"
    )

if checkout_dir == Path({str(managed_checkout)!r}) and ref:
    print("dirty managed checkout cannot resolve immutable refs", file=sys.stderr)
    raise SystemExit(42)

if checkout_dir.parent == Path({str(compare_checkout_root)!r}) and fetch_url != {str(source_checkout)!r}:
    print(
        "per-ref compare checkouts must clone from the clean source checkout",
        file=sys.stderr,
    )
    raise SystemExit(43)

(checkout_dir / ".git").mkdir(exist_ok=True)
resolved_ref = ref or "0000000000000000000000000000000000000000"
(checkout_dir / ".bench-current-sha").write_text(f"{{resolved_ref}}\\n", encoding="utf-8")
print(f"delta-rs checkout ready: {{checkout_dir}}")
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
        write_executable(
            scripts_dir / "bench.sh",
            """#!/usr/bin/env python3
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
command = args[0]
suite = value(args, "--suite", "scan")
results_dir = Path(os.environ["DELTA_BENCH_RESULTS"])
label = os.environ["DELTA_BENCH_LABEL"]

if command == "data":
    sys.exit(0)

if command == "run":
    target_dir = results_dir / label
    target_dir.mkdir(parents=True, exist_ok=True)
    trusted_ms = 100.0 if label.startswith("base-") else 90.0
    payload = {
        "schema_version": 5,
        "context": {
            "schema_version": 5,
            "label": label,
            "suite": suite,
            "benchmark_mode": "perf",
            "timing_phase": "execute",
            "dataset_id": "tiny_smoke",
            "dataset_fingerprint": "sha256:fixture",
            "runner": "rust",
            "scale": "sf1",
            "storage_backend": "local",
            "backend_profile": "local",
            "lane": "macro",
            "measurement_kind": "phase_breakdown",
            "validation_level": "operational",
            "harness_revision": "harness-rev",
            "fixture_recipe_hash": "sha256:recipe",
            "fidelity_fingerprint": "sha256:fidelity",
        },
        "cases": [
            {
                "case": "scan_full_narrow",
                "success": True,
                "validation_passed": True,
                "perf_status": "trusted",
                "classification": "supported",
                "samples": [{"elapsed_ms": trusted_ms, "metrics": {"files_scanned": 10}}],
                "run_summary": {
                    "sample_count": 1,
                    "invalid_sample_count": 0,
                    "median_ms": trusted_ms,
                    "host_label": "bench-host",
                    "fidelity_fingerprint": "sha256:fidelity",
                },
                "compatibility_key": "sha256:good",
                "supports_decision": True,
                "required_runs": 5,
                "decision_threshold_pct": 5.0,
                "decision_metric": "median",
            }
        ],
    }
    (target_dir / f"{suite}.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )
    sys.exit(0)

raise SystemExit(0)
""",
        )

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
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

        python_pkg = temp_root / "python" / "delta_bench_compare"
        python_pkg.mkdir(parents=True)
        (python_pkg / "__init__.py").write_text("", encoding="utf-8")
        copy_compare_manifest_helper(python_pkg)
        write_executable(
            python_pkg / "aggregate.py",
            """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--label", required=True)
parser.add_argument("--mode", default="decision")
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
print("hash policy ok")
""",
        )

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_RS_DIR"] = str(managed_checkout)
        env["DELTA_RS_SOURCE_DIR"] = str(source_checkout)
        env["DELTA_BENCH_COMPARE_CHECKOUT_ROOT"] = str(compare_checkout_root)
        env["DELTA_BENCH_RESULTS"] = str(temp_root / "results")
        relax_local_compare_disk_headroom(env)

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

        assert result.returncode == 0, result.stderr or result.stdout
        prep_entries = [
            json.loads(line)
            for line in prep_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        pin_entries = [
            entry
            for entry in prep_entries
            if entry["dir"] == str(source_checkout)
            and entry["ref"] in {base_sha, candidate_sha}
        ]
        assert [entry["ref"] for entry in pin_entries] == [base_sha, candidate_sha]
        prepared_entries = [
            entry
            for entry in prep_entries
            if Path(entry["dir"]).resolve().parent == compare_checkout_root.resolve()
        ]
        assert [entry["fetch_url"] for entry in prepared_entries] == [
            str(source_checkout),
            str(source_checkout),
        ]


def test_compare_branch_workflow_keeps_exploratory_artifacts_when_one_case_is_invalid() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        copy_executable(COMPARE_BRANCH, compare_copy)

        managed_checkout = temp_root / ".delta-rs-under-test"
        source_checkout = temp_root / ".delta-rs-source"
        compare_checkout_root = temp_root / ".delta-bench-compare-checkouts"
        (managed_checkout / ".git").mkdir(parents=True)
        (source_checkout / ".git").mkdir(parents=True)
        (managed_checkout / "crates" / "delta-bench").mkdir(parents=True)
        (managed_checkout / "crates" / "delta-bench" / "Cargo.toml").write_text(
            "dirty overlay\n", encoding="utf-8"
        )

        prep_log = temp_root / "prep-log.jsonl"
        write_executable(
            scripts_dir / "prepare_delta_rs.sh",
            f"""#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path

checkout_dir = Path(os.environ["DELTA_RS_DIR"])
checkout_dir.mkdir(parents=True, exist_ok=True)
ref = os.environ.get("DELTA_RS_REF") or os.environ.get("DELTA_RS_BRANCH") or ""
fetch_url = os.environ.get("DELTA_RS_FETCH_URL", "")

with open({str(prep_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(
        json.dumps(
            {{
                "dir": os.environ["DELTA_RS_DIR"],
                "ref": os.environ.get("DELTA_RS_REF"),
                "branch": os.environ.get("DELTA_RS_BRANCH"),
                "fetch_url": fetch_url,
            }}
        )
        + "\\n"
    )

if checkout_dir == Path({str(managed_checkout)!r}) and ref:
    print("dirty managed checkout should not be used for compare pinning", file=sys.stderr)
    raise SystemExit(42)

if checkout_dir.parent == Path({str(compare_checkout_root)!r}) and fetch_url != {str(source_checkout)!r}:
    print("per-ref compare checkout must be seeded from clean source checkout", file=sys.stderr)
    raise SystemExit(43)

(checkout_dir / ".git").mkdir(exist_ok=True)
resolved_ref = ref or "0000000000000000000000000000000000000000"
(checkout_dir / ".bench-current-sha").write_text(f"{{resolved_ref}}\\n", encoding="utf-8")
print(f"delta-rs checkout ready: {{checkout_dir}}")
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
        write_executable(
            scripts_dir / "bench.sh",
            """#!/usr/bin/env python3
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
command = args[0]
suite = value(args, "--suite", "scan")
results_dir = Path(os.environ["DELTA_BENCH_RESULTS"])
label = os.environ["DELTA_BENCH_LABEL"]

if command == "data":
    sys.exit(0)

if command == "run":
    target_dir = results_dir / label
    target_dir.mkdir(parents=True, exist_ok=True)
    trusted_ms = 100.0 if label.startswith("base-") else 90.0
    payload = {
        "schema_version": 5,
        "context": {
            "schema_version": 5,
            "label": label,
            "suite": suite,
            "benchmark_mode": "perf",
            "timing_phase": "execute",
            "dataset_id": "tiny_smoke",
            "dataset_fingerprint": "sha256:fixture",
            "runner": "rust",
            "scale": "sf1",
            "storage_backend": "local",
            "backend_profile": "local",
            "lane": "macro",
            "measurement_kind": "phase_breakdown",
            "validation_level": "operational",
            "harness_revision": "harness-rev",
            "fixture_recipe_hash": "sha256:recipe",
            "fidelity_fingerprint": "sha256:fidelity",
        },
        "cases": [
            {
                "case": "scan_full_narrow",
                "success": True,
                "validation_passed": True,
                "perf_status": "trusted",
                "classification": "supported",
                "samples": [{"elapsed_ms": trusted_ms, "metrics": {"files_scanned": 10}}],
                "run_summary": {
                    "sample_count": 1,
                    "invalid_sample_count": 0,
                    "median_ms": trusted_ms,
                    "host_label": "bench-host",
                    "fidelity_fingerprint": "sha256:fidelity",
                },
                "compatibility_key": "sha256:good",
                "supports_decision": True,
                "required_runs": 5,
                "decision_threshold_pct": 5.0,
                "decision_metric": "median",
            },
            {
                "case": "scan_pruning_hit",
                "success": False,
                "validation_passed": False,
                "perf_status": "invalid",
                "classification": "supported",
                "samples": [],
                "run_summary": {
                    "sample_count": 0,
                    "invalid_sample_count": 0,
                    "host_label": "bench-host",
                    "fidelity_fingerprint": "sha256:fidelity",
                },
                "compatibility_key": "sha256:bad",
                "supports_decision": True,
                "required_runs": 5,
                "decision_threshold_pct": 5.0,
                "decision_metric": "median",
                "failure_kind": "assertion_mismatch",
                "failure": {"message": "stale hash"},
            },
        ],
    }
    (target_dir / f"{suite}.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )
    sys.exit(0)

raise SystemExit(0)
""",
        )

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
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

        python_root = temp_root / "python"
        python_root.mkdir(parents=True)
        shutil.copytree(
            REPO_ROOT / "python" / "delta_bench_compare",
            python_root / "delta_bench_compare",
        )

        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"
        results_dir = temp_root / "results"

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_RS_DIR"] = str(managed_checkout)
        env["DELTA_RS_SOURCE_DIR"] = str(source_checkout)
        env["DELTA_BENCH_COMPARE_CHECKOUT_ROOT"] = str(compare_checkout_root)
        env["DELTA_BENCH_RESULTS"] = str(results_dir)
        env["DELTA_BENCH_MIN_FREE_GB"] = "1"

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

        assert result.returncode == 0, result.stderr or result.stdout
        assert "Traceback" not in result.stderr
        artifact_dir = results_dir / "compare" / "scan" / f"{base_sha}__{candidate_sha}"
        stdout_report = (artifact_dir / "stdout.txt").read_text(encoding="utf-8")
        markdown_report = (artifact_dir / "summary.md").read_text(encoding="utf-8")
        assert "Comparison aborted / invalid" in stdout_report
        assert "scan_pruning_hit" in stdout_report
        assert "Comparison aborted / invalid" in markdown_report
        prep_entries = [
            json.loads(line)
            for line in prep_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        pin_entries = [
            entry
            for entry in prep_entries
            if entry["dir"] == str(source_checkout)
            and entry["ref"] in {base_sha, candidate_sha}
        ]
        assert [entry["ref"] for entry in pin_entries] == [base_sha, candidate_sha]


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


def test_sync_harness_to_delta_rs_waits_for_checkout_lock_before_mutating_checkout() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        sync_copy = scripts_dir / "sync_harness_to_delta_rs.sh"
        copy_executable(
            REPO_ROOT / "scripts" / "sync_harness_to_delta_rs.sh", sync_copy
        )

        src_crate = temp_root / "crates" / "delta-bench"
        (src_crate / "src").mkdir(parents=True)
        (src_crate / "benches").mkdir(parents=True)
        (src_crate / "Cargo.toml").write_text(
            '[package]\nname = "delta-bench"\nversion = "0.1.0"\n',
            encoding="utf-8",
        )
        (src_crate / "Cargo.toml.delta-rs").write_text(
            '[package]\nname = "delta-bench"\nversion = "0.1.0"\n',
            encoding="utf-8",
        )
        (src_crate / "src" / "lib.rs").write_text("", encoding="utf-8")
        (src_crate / "benches" / "metadata_log_bench.rs").write_text(
            "fn main() {}\n", encoding="utf-8"
        )

        (temp_root / "bench" / "manifests").mkdir(parents=True)
        (temp_root / "backends").mkdir(parents=True)
        (temp_root / "python" / "delta_bench_interop").mkdir(parents=True)
        (temp_root / "python" / "delta_bench_tpcds").mkdir(parents=True)

        checkout_dir = temp_root / ".delta-rs-under-test"
        (checkout_dir / ".git").mkdir(parents=True)
        lock_file = temp_root / ".delta-rs-under-test.delta_bench_checkout.lock"
        lock_ready = temp_root / "lock-ready"
        release_lock = temp_root / "release-lock"

        python_bin = shutil.which("python3")
        assert python_bin is not None
        flock_bin = shutil.which("flock")
        if flock_bin is None:
            lock_dir = Path(f"{lock_file}.dir")
            holder = subprocess.Popen(
                [
                    python_bin,
                    "-c",
                    """
import pathlib
import sys
import time

lock_dir = pathlib.Path(sys.argv[1])
ready_path = pathlib.Path(sys.argv[2])
release_path = pathlib.Path(sys.argv[3])
lock_dir.mkdir(parents=True)
ready_path.write_text("locked\\n", encoding="utf-8")
while not release_path.exists():
    time.sleep(0.05)
(lock_dir / "pid").unlink(missing_ok=True)
lock_dir.rmdir()
""",
                    str(lock_dir),
                    str(lock_ready),
                    str(release_lock),
                ],
                cwd=temp_root,
            )
        else:
            holder = subprocess.Popen(
                [
                    python_bin,
                    "-c",
                    """
import fcntl
import pathlib
import sys
import time

lock_path = pathlib.Path(sys.argv[1])
ready_path = pathlib.Path(sys.argv[2])
release_path = pathlib.Path(sys.argv[3])
lock_path.parent.mkdir(parents=True, exist_ok=True)
with lock_path.open("w", encoding="utf-8") as handle:
    fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
    ready_path.write_text("locked\\n", encoding="utf-8")
    while not release_path.exists():
        time.sleep(0.05)
""",
                    str(lock_file),
                    str(lock_ready),
                    str(release_lock),
                ],
                cwd=temp_root,
            )
        try:
            assert wait_for_condition(lock_ready.exists)

            env = os.environ.copy()
            env["DELTA_RS_DIR"] = str(checkout_dir)
            env["DELTA_BENCH_CHECKOUT_LOCK_TIMEOUT_SECONDS"] = "5"
            sync = subprocess.Popen(
                ["bash", str(sync_copy)],
                cwd=temp_root,
                env=env,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
            try:
                time.sleep(0.3)
                assert sync.poll() is None
                assert not (
                    checkout_dir / "crates" / "delta-bench" / "Cargo.toml"
                ).exists()
            finally:
                release_lock.write_text("ok\n", encoding="utf-8")

            stdout, stderr = sync.communicate(timeout=10)
            assert sync.returncode == 0, stderr or stdout
            assert (checkout_dir / "crates" / "delta-bench" / "Cargo.toml").is_file()
            assert (
                checkout_dir
                / "crates"
                / "delta-bench"
                / "benches"
                / "metadata_log_bench.rs"
            ).is_file()
            manifest = (
                checkout_dir
                / "crates"
                / "delta-bench"
                / ".delta_bench_overlay_manifest"
            )
            assert manifest.is_file()
            manifest_entries = manifest.read_text(encoding="utf-8").splitlines()
            assert "crates/delta-bench/Cargo.toml" in manifest_entries
            assert (
                "crates/delta-bench/benches/metadata_log_bench.rs" in manifest_entries
            )
        finally:
            holder.communicate(timeout=10)


def test_gitignore_ignores_checkout_lock_artifacts() -> None:
    gitignore = GITIGNORE.read_text(encoding="utf-8")
    assert ".DS_Store" in gitignore
    assert ".delta-rs-source/" in gitignore
    assert ".delta-bench-compare-checkouts/" in gitignore
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
        relax_local_compare_disk_headroom(env)

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

if command == "validate":
    input_path = Path(value(cli_args, "--input"))
    output_path = Path(value(cli_args, "--output"))
    if not input_path.is_absolute():
        input_path = cwd / input_path
    if not output_path.is_absolute():
        output_path = cwd / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(input_path.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"validated result: {output_path}")
    sys.exit(0)

sys.exit(0)
""",
        )

        python_pkg = temp_root / "python" / "delta_bench_compare"
        python_pkg.mkdir(parents=True)
        (python_pkg / "__init__.py").write_text("", encoding="utf-8")
        copy_compare_manifest_helper(python_pkg)
        write_executable(
            python_pkg / "aggregate.py",
            """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--label", required=True)
parser.add_argument("--mode", default="decision")
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
        relax_local_compare_disk_headroom(env)

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


def test_compare_branch_writes_markdown_and_manifest_artifacts() -> None:
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        copy_executable(COMPARE_BRANCH, compare_copy)

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
        write_executable(
            scripts_dir / "bench.sh",
            """#!/usr/bin/env python3
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
command = args[0]
suite = value(args, "--suite", "scan")
results_dir = Path(os.environ["DELTA_BENCH_RESULTS"])
label = os.environ["DELTA_BENCH_LABEL"]

if command == "data":
    sys.exit(0)

if command == "run":
    target_dir = results_dir / label
    target_dir.mkdir(parents=True, exist_ok=True)
    trusted_ms = 100.0 if label.startswith("base-") else 90.0
    payload = {
        "schema_version": 5,
        "context": {
            "schema_version": 5,
            "label": label,
            "suite": suite,
            "benchmark_mode": "perf",
            "timing_phase": "execute",
            "dataset_id": "tiny_smoke",
            "dataset_fingerprint": "sha256:fixture",
            "runner": "rust",
            "scale": "sf1",
            "storage_backend": "local",
            "backend_profile": "local",
            "lane": "macro",
            "measurement_kind": "phase_breakdown",
            "validation_level": "operational",
            "harness_revision": "harness-rev",
            "fixture_recipe_hash": "sha256:recipe",
            "fidelity_fingerprint": "sha256:fidelity",
        },
        "cases": [
            {
                "case": "scan_full_narrow",
                "success": True,
                "validation_passed": True,
                "perf_status": "trusted",
                "classification": "supported",
                "samples": [{"elapsed_ms": trusted_ms, "metrics": {"files_scanned": 10}}],
                "run_summary": {
                    "sample_count": 1,
                    "invalid_sample_count": 0,
                    "median_ms": trusted_ms,
                    "host_label": "bench-host",
                    "fidelity_fingerprint": "sha256:fidelity",
                },
                "compatibility_key": "sha256:good",
                "supports_decision": True,
                "required_runs": 5,
                "decision_threshold_pct": 5.0,
                "decision_metric": "median",
            }
        ],
    }
    (target_dir / f"{suite}.json").write_text(
        json.dumps(payload),
        encoding="utf-8",
    )
    sys.exit(0)

raise SystemExit(0)
""",
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

        python_pkg = temp_root / "python" / "delta_bench_compare"
        python_pkg.mkdir(parents=True)
        (python_pkg / "__init__.py").write_text("", encoding="utf-8")
        copy_compare_manifest_helper(python_pkg)
        write_executable(
            python_pkg / "aggregate.py",
            """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--label", required=True)
parser.add_argument("--mode", default="decision")
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
import json
import os
import sys

parser = argparse.ArgumentParser()
parser.add_argument("base_json")
parser.add_argument("cand_json")
parser.add_argument("--format", choices=["text", "markdown", "json"], default="text")
parser.parse_known_args()
args, _ = parser.parse_known_args()

log_path = os.environ.get("COMPARE_ARG_LOG")
if log_path:
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(sys.argv[1:]) + "\\n")

if args.format == "markdown":
    print("# Compare Summary")
elif args.format == "json":
    print(
        json.dumps(
            {
                "summary": {
                    "faster": 1,
                    "slower": 0,
                    "no_change": 0,
                    "incomparable": 0,
                    "new": 0,
                    "removed": 0,
                },
                "rows": [
                    {
                        "case": "a",
                        "change": "1.11x faster",
                        "baseline_ms": 100.0,
                        "candidate_ms": 90.0,
                        "delta_pct": -10.0,
                    }
                ],
            }
        )
    )
else:
    print("compare ok")
""",
        )
        write_executable(
            python_pkg / "hash_policy.py",
            """#!/usr/bin/env python3
print("hash policy ok")
""",
        )

        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"
        results_dir = temp_root / "results"
        home_dir = temp_root / "home"
        home_dir.mkdir()
        (home_dir / ".bashrc").write_text(
            'printf "startup-noise-bashrc\\n" >&2\n',
            encoding="utf-8",
        )
        (home_dir / ".profile").write_text(
            'printf "startup-noise-profile\\n" >&2\n',
            encoding="utf-8",
        )

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["HOME"] = str(home_dir)
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_RS_DIR"] = str(temp_root / ".delta-rs-under-test")
        env["DELTA_BENCH_RESULTS"] = str(results_dir)
        relax_local_compare_disk_headroom(env)

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

        assert result.returncode == 0, result.stderr or result.stdout
        assert "startup-noise-bashrc" not in result.stderr
        assert "startup-noise-profile" not in result.stderr
        artifact_dir = results_dir / "compare" / "scan" / f"{base_sha}__{candidate_sha}"
        manifest = json.loads(
            (artifact_dir / "manifest.json").read_text(encoding="utf-8")
        )
        assert manifest["profile"] == "scan"
        assert manifest["compare_mode"] == "exploratory"
        assert manifest["aggregation"] == "median"
        assert manifest["noise_threshold"] == 0.05
        assert Path(manifest["stdout_report"]).is_file()
        assert Path(manifest["markdown_report"]).is_file()
        assert Path(manifest["comparison_json"]).is_file()
        assert Path(manifest["hash_policy_report"]).is_file()


def test_compare_branch_manifest_records_overridden_methodology_settings_without_canonical_profile_identity() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        copy_executable(COMPARE_BRANCH, compare_copy)

        methodology_dir = temp_root / "bench" / "methodologies"
        methodology_dir.mkdir(parents=True)
        methodology_dir.joinpath("pr-macro.env").write_text(
            (REPO_ROOT / "bench" / "methodologies" / "pr-macro.env").read_text(
                encoding="utf-8"
            ),
            encoding="utf-8",
        )

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
        write_executable(
            scripts_dir / "bench.sh",
            """#!/usr/bin/env python3
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
command = args[0]
suite = value(args, "--suite", "scan")
results_dir = Path(os.environ["DELTA_BENCH_RESULTS"])
label = os.environ["DELTA_BENCH_LABEL"]

if command == "data":
    sys.exit(0)

if command == "run":
    target_dir = results_dir / label
    target_dir.mkdir(parents=True, exist_ok=True)
    trusted_ms = 100.0 if label.startswith("base-") else 90.0
    payload = {
        "schema_version": 5,
        "context": {
            "schema_version": 5,
            "label": label,
            "suite": suite,
            "benchmark_mode": "perf",
            "timing_phase": "execute",
            "dataset_id": "tiny_smoke",
            "dataset_fingerprint": "sha256:fixture",
            "runner": "rust",
            "scale": "sf1",
            "storage_backend": "local",
            "backend_profile": "local",
            "lane": "macro",
            "measurement_kind": "phase_breakdown",
            "validation_level": "operational",
            "harness_revision": "harness-rev",
            "fixture_recipe_hash": "sha256:recipe",
            "fidelity_fingerprint": "sha256:fidelity",
        },
        "cases": [
            {
                "case": "scan_full_narrow",
                "success": True,
                "validation_passed": True,
                "perf_status": "trusted",
                "classification": "supported",
                "samples": [{"elapsed_ms": trusted_ms, "metrics": {"files_scanned": 10}}],
                "run_summary": {
                    "sample_count": 1,
                    "invalid_sample_count": 0,
                    "median_ms": trusted_ms,
                    "host_label": "bench-host",
                    "fidelity_fingerprint": "sha256:fidelity",
                },
                "compatibility_key": "sha256:good",
                "supports_decision": True,
                "required_runs": 5,
                "decision_threshold_pct": 5.0,
                "decision_metric": "median",
            }
        ],
    }
    (target_dir / f"{suite}.json").write_text(json.dumps(payload), encoding="utf-8")
    sys.exit(0)

raise SystemExit(0)
""",
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

        python_pkg = temp_root / "python" / "delta_bench_compare"
        python_pkg.mkdir(parents=True)
        (python_pkg / "__init__.py").write_text("", encoding="utf-8")
        copy_compare_manifest_helper(python_pkg)
        write_executable(
            python_pkg / "aggregate.py",
            """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--label", required=True)
parser.add_argument("--mode", default="decision")
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
import json
import os
import sys

parser = argparse.ArgumentParser()
parser.add_argument("base_json")
parser.add_argument("cand_json")
parser.add_argument("--format", choices=["text", "markdown", "json"], default="text")
parser.parse_known_args()
args, _ = parser.parse_known_args()

log_path = os.environ.get("COMPARE_ARG_LOG")
if log_path:
    with open(log_path, "a", encoding="utf-8") as handle:
        handle.write(json.dumps(sys.argv[1:]) + "\\n")

if args.format == "markdown":
    print("# Compare Summary")
elif args.format == "json":
    print(
        json.dumps(
            {
                "summary": {
                    "faster": 1,
                    "slower": 0,
                    "no_change": 0,
                    "incomparable": 0,
                    "new": 0,
                    "removed": 0,
                },
                "rows": [
                    {
                        "case": "a",
                        "change": "1.11x faster",
                        "baseline_ms": 100.0,
                        "candidate_ms": 90.0,
                        "delta_pct": -10.0,
                    }
                ],
            }
        )
    )
else:
    print("compare ok")
""",
        )
        write_executable(
            python_pkg / "hash_policy.py",
            """#!/usr/bin/env python3
print("hash policy ok")
""",
        )

        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"
        results_dir = temp_root / "results"
        compare_arg_log = temp_root / "compare-argv.jsonl"
        home_dir = temp_root / "home"
        home_dir.mkdir()
        (home_dir / ".bashrc").write_text(
            'printf "startup-noise-bashrc\\n" >&2\n',
            encoding="utf-8",
        )
        (home_dir / ".profile").write_text(
            'printf "startup-noise-profile\\n" >&2\n',
            encoding="utf-8",
        )

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["HOME"] = str(home_dir)
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_RS_DIR"] = str(temp_root / ".delta-rs-under-test")
        env["DELTA_BENCH_RESULTS"] = str(results_dir)
        env["COMPARE_ARG_LOG"] = str(compare_arg_log)
        relax_local_compare_disk_headroom(env)

        result = subprocess.run(
            [
                "bash",
                str(compare_copy),
                "--methodology-profile",
                "pr-macro",
                "--warmup",
                "7",
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

        assert result.returncode == 0, result.stderr or result.stdout
        artifact_dir = results_dir / "compare" / "scan" / f"{base_sha}__{candidate_sha}"
        manifest = json.loads(
            (artifact_dir / "manifest.json").read_text(encoding="utf-8")
        )
        assert manifest["profile"] == "pr-macro"
        assert manifest["methodology_profile"] is None
        assert manifest["methodology_version"] is None
        assert manifest["methodology_settings"] == {
            "compare_mode": "decision",
            "warmup": 7,
            "iters": 15,
            "prewarm_iters": 1,
            "compare_runs": 7,
            "measure_order": "alternate",
            "timing_phase": "execute",
            "aggregation": "median",
            "dataset_id": "medium_selective",
            "dataset_policy": "shared_run_scope",
            "spread_metric": "iqr_ms",
            "sub_ms_threshold_ms": 1.0,
            "sub_ms_policy": "micro_only",
            "storage_backend": "local",
            "backend_profile": None,
        }
        compare_invocations = [
            json.loads(line)
            for line in compare_arg_log.read_text(encoding="utf-8").splitlines()
        ]
        assert len(compare_invocations) == 4
        for argv in compare_invocations:
            assert "--spread-metric" in argv
            assert argv[argv.index("--spread-metric") + 1] == "iqr_ms"
            assert "--sub-ms-threshold-ms" in argv
            assert argv[argv.index("--sub-ms-threshold-ms") + 1] == "1.0"
            assert "--sub-ms-policy" in argv
            assert argv[argv.index("--sub-ms-policy") + 1] == "micro_only"


def test_compare_branch_prepares_each_pinned_ref_once_and_reuses_checkout() -> None:
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        copy_executable(COMPARE_BRANCH, compare_copy)

        prep_log = temp_root / "prep-log.jsonl"
        sync_log = temp_root / "sync-log.jsonl"
        write_executable(
            scripts_dir / "prepare_delta_rs.sh",
            f"""#!/usr/bin/env python3
import json
import os
from pathlib import Path

checkout_dir = Path(os.environ["DELTA_RS_DIR"])
checkout_dir.mkdir(parents=True, exist_ok=True)
(checkout_dir / ".git").mkdir(exist_ok=True)
ref = os.environ.get("DELTA_RS_REF") or os.environ.get("DELTA_RS_BRANCH") or ""
(checkout_dir / ".bench-current-sha").write_text(ref or "0" * 40, encoding="utf-8")
with open({str(prep_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(
        json.dumps(
            {{
                "ref": os.environ.get("DELTA_RS_REF"),
                "branch": os.environ.get("DELTA_RS_BRANCH"),
                "dir": os.environ["DELTA_RS_DIR"],
            }}
        )
        + "\\n"
    )
print(f"delta-rs checkout ready: {{checkout_dir}}")
""",
        )
        write_executable(
            scripts_dir / "sync_harness_to_delta_rs.sh",
            f"""#!/usr/bin/env python3
import json
import os
from pathlib import Path

checkout_dir = Path(os.environ["DELTA_RS_DIR"])
checkout_dir.mkdir(parents=True, exist_ok=True)
with open({str(sync_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(json.dumps({{"dir": os.environ["DELTA_RS_DIR"]}}) + "\\n")
""",
        )
        write_executable(
            scripts_dir / "security_check.sh",
            "#!/usr/bin/env bash\nset -euo pipefail\nexit 0\n",
        )
        write_executable(
            scripts_dir / "bench.sh",
            """#!/usr/bin/env python3
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
command = args[0]
suite = value(args, "--suite", "scan")
results_dir = Path(os.environ["DELTA_BENCH_RESULTS"])
label = os.environ["DELTA_BENCH_LABEL"]

if command == "data":
    sys.exit(0)

if command == "run":
    target_dir = results_dir / label
    target_dir.mkdir(parents=True, exist_ok=True)
    (target_dir / f"{suite}.json").write_text(
        json.dumps({"label": label, "suite": suite}),
        encoding="utf-8",
    )
    sys.exit(0)

raise SystemExit(0)
""",
        )

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
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

        python_pkg = temp_root / "python" / "delta_bench_compare"
        python_pkg.mkdir(parents=True)
        (python_pkg / "__init__.py").write_text("", encoding="utf-8")
        copy_compare_manifest_helper(python_pkg)
        write_executable(
            python_pkg / "aggregate.py",
            """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--label", required=True)
parser.add_argument("--mode", default="decision")
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
print("compare ok")
""",
        )
        write_executable(
            python_pkg / "hash_policy.py",
            """#!/usr/bin/env python3
print("hash policy ok")
""",
        )

        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"
        checkout_root = temp_root / "prepared-checkouts"

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "1"
        env["BENCH_COMPARE_RUNS"] = "2"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_RS_DIR"] = str(temp_root / ".delta-rs-under-test")
        env["DELTA_BENCH_RESULTS"] = str(temp_root / "results")
        env["DELTA_BENCH_COMPARE_CHECKOUT_ROOT"] = str(checkout_root)
        relax_local_compare_disk_headroom(env)

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

        assert result.returncode == 0, result.stderr or result.stdout
        prep_entries = [
            json.loads(line)
            for line in prep_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        prepared_entries = [
            entry
            for entry in prep_entries
            if Path(entry["dir"]).resolve().parent == checkout_root.resolve()
        ]
        assert [entry["ref"] for entry in prepared_entries] == [
            base_sha,
            candidate_sha,
        ]
        sync_entries = [
            json.loads(line)
            for line in sync_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        assert [entry["dir"] for entry in sync_entries] == [
            str(checkout_root / base_sha),
            str(checkout_root / candidate_sha),
        ]


def test_compare_branch_runs_doctor_preflight_for_each_pinned_checkout_before_data() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        copy_executable(COMPARE_BRANCH, compare_copy)

        prep_log = temp_root / "prep-log.jsonl"
        bench_log = temp_root / "bench-log.jsonl"
        write_executable(
            scripts_dir / "prepare_delta_rs.sh",
            f"""#!/usr/bin/env python3
import json
import os
from pathlib import Path

checkout_dir = Path(os.environ["DELTA_RS_DIR"])
checkout_dir.mkdir(parents=True, exist_ok=True)
(checkout_dir / ".git").mkdir(exist_ok=True)
ref = os.environ.get("DELTA_RS_REF") or os.environ.get("DELTA_RS_BRANCH") or ""
(checkout_dir / ".bench-current-sha").write_text(ref or "0" * 40, encoding="utf-8")
with open({str(prep_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(
        json.dumps(
            {{
                "ref": os.environ.get("DELTA_RS_REF"),
                "branch": os.environ.get("DELTA_RS_BRANCH"),
                "dir": os.environ["DELTA_RS_DIR"],
            }}
        )
        + "\\n"
    )
print(f"delta-rs checkout ready: {{checkout_dir}}")
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
        write_executable(
            scripts_dir / "bench.sh",
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
command = args[0]
cwd = Path.cwd()
entry = {{
    "command": command,
    "cwd": str(cwd),
    "label": os.environ.get("DELTA_BENCH_LABEL"),
    "exec_root": os.environ.get("DELTA_BENCH_EXEC_ROOT"),
}}
with open({str(bench_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(json.dumps(entry) + "\\n")

if command == "doctor":
    print("delta-bench doctor")
    sys.exit(0)

if command == "data":
    sys.exit(0)

if command == "run":
    results_dir = Path(os.environ["DELTA_BENCH_RESULTS"])
    label = os.environ["DELTA_BENCH_LABEL"]
    suite = value(args, "--suite", "scan")
    out_dir = results_dir / label
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{{suite}}.json").write_text(
        json.dumps({{"label": label, "suite": suite}}),
        encoding="utf-8",
    )
    sys.exit(0)

raise SystemExit(0)
""",
        )

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
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

        python_pkg = temp_root / "python" / "delta_bench_compare"
        python_pkg.mkdir(parents=True)
        (python_pkg / "__init__.py").write_text("", encoding="utf-8")
        copy_compare_manifest_helper(python_pkg)
        write_executable(
            python_pkg / "aggregate.py",
            """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--label", required=True)
parser.add_argument("--mode", default="decision")
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
print("compare ok")
""",
        )
        write_executable(
            python_pkg / "hash_policy.py",
            """#!/usr/bin/env python3
print("hash policy ok")
""",
        )

        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"
        checkout_root = temp_root / "prepared-checkouts"

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_RS_DIR"] = str(temp_root / ".delta-rs-under-test")
        env["DELTA_BENCH_RESULTS"] = str(temp_root / "results")
        env["DELTA_BENCH_COMPARE_CHECKOUT_ROOT"] = str(checkout_root)
        relax_local_compare_disk_headroom(env)

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

        assert result.returncode == 0, result.stderr or result.stdout
        bench_entries = [
            json.loads(line)
            for line in bench_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        assert [entry["command"] for entry in bench_entries[:2]] == ["doctor", "doctor"]
        assert [Path(entry["exec_root"]).resolve() for entry in bench_entries[:2]] == [
            (checkout_root / base_sha).resolve(),
            (checkout_root / candidate_sha).resolve(),
        ]
        assert bench_entries[2]["command"] == "data"


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
        (exec_root / "crates" / "delta-bench" / "Cargo.toml.delta-rs").write_text(
            "[package]\nname = 'delta-bench'\nversion = '0.0.0'\n",
            encoding="utf-8",
        )
        (exec_root / "crates" / "delta-bench" / "benches").mkdir(parents=True)
        (
            exec_root / "crates" / "delta-bench" / "benches" / "metadata_log_bench.rs"
        ).write_text(
            "fn main() {}\n",
            encoding="utf-8",
        )
        (exec_root / "crates" / "delta-bench" / "src").mkdir(parents=True)
        (exec_root / "crates" / "delta-bench" / "src" / "validation.rs").write_text(
            "pub fn validation_contract() {}\n",
            encoding="utf-8",
        )
        (exec_root / "bench" / "manifests").mkdir(parents=True)
        (exec_root / "bench" / "manifests" / "core_rust.yaml").write_text(
            "runner: rust\n",
            encoding="utf-8",
        )
        (exec_root / "bench" / "manifests" / "core_python.yaml").write_text(
            "runner: python\n",
            encoding="utf-8",
        )
        (exec_root / "backends").mkdir(parents=True)
        (exec_root / "backends" / "s3_locking_vultr.env").write_text(
            "PROFILE=s3_locking_vultr\n",
            encoding="utf-8",
        )
        (exec_root / "python" / "delta_bench_interop").mkdir(parents=True)
        (exec_root / "python" / "delta_bench_interop" / "run_case.py").write_text(
            "print('interop')\n",
            encoding="utf-8",
        )
        (exec_root / "python" / "delta_bench_tpcds").mkdir(parents=True)
        (
            exec_root / "python" / "delta_bench_tpcds" / "generate_store_sales_csv.py"
        ).write_text(
            "print('tpcds')\n",
            encoding="utf-8",
        )
        write_overlay_manifest(
            exec_root,
            [
                "crates/delta-bench/Cargo.toml",
                "crates/delta-bench/benches/metadata_log_bench.rs",
                "crates/delta-bench/src/validation.rs",
                "bench/manifests/core_rust.yaml",
                "bench/manifests/core_python.yaml",
                "backends/s3_locking_vultr.env",
                "python/delta_bench_interop/run_case.py",
                "python/delta_bench_tpcds/generate_store_sales_csv.py",
            ],
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


def test_bench_wrapper_resyncs_when_overlay_manifest_references_missing_file_selection_bench() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        bench_copy = scripts_dir / "bench.sh"
        copy_executable(BENCH_SH, bench_copy)

        exec_root = temp_root / ".delta-rs-under-test"
        crate_dir = exec_root / "crates" / "delta-bench"
        (crate_dir / "benches").mkdir(parents=True)
        (crate_dir / "src").mkdir(parents=True)
        (crate_dir / "Cargo.toml").write_text(
            "[package]\nname = 'delta-bench'\nversion = '0.0.0'\n",
            encoding="utf-8",
        )
        (crate_dir / "Cargo.toml.delta-rs").write_text(
            "[package]\nname = 'delta-bench'\nversion = '0.0.0'\n",
            encoding="utf-8",
        )
        (crate_dir / "benches" / "metadata_log_bench.rs").write_text(
            "fn main() {}\n",
            encoding="utf-8",
        )
        (crate_dir / "src" / "validation.rs").write_text(
            "pub fn validation_contract() {}\n",
            encoding="utf-8",
        )
        (exec_root / "bench" / "manifests").mkdir(parents=True)
        (exec_root / "bench" / "manifests" / "core_rust.yaml").write_text(
            "runner: rust\n",
            encoding="utf-8",
        )
        (exec_root / "bench" / "manifests" / "core_python.yaml").write_text(
            "runner: python\n",
            encoding="utf-8",
        )
        (exec_root / "backends").mkdir(parents=True)
        (exec_root / "backends" / "s3_locking_vultr.env").write_text(
            "PROFILE=s3_locking_vultr\n",
            encoding="utf-8",
        )
        (exec_root / "python" / "delta_bench_interop").mkdir(parents=True)
        (exec_root / "python" / "delta_bench_interop" / "run_case.py").write_text(
            "print('interop')\n",
            encoding="utf-8",
        )
        (exec_root / "python" / "delta_bench_tpcds").mkdir(parents=True)
        (
            exec_root / "python" / "delta_bench_tpcds" / "generate_store_sales_csv.py"
        ).write_text(
            "print('tpcds')\n",
            encoding="utf-8",
        )
        manifest_entries = [
            "crates/delta-bench/Cargo.toml",
            "crates/delta-bench/benches/metadata_log_bench.rs",
            "crates/delta-bench/benches/file_selection_bench.rs",
            "crates/delta-bench/src/validation.rs",
            "bench/manifests/core_rust.yaml",
            "bench/manifests/core_python.yaml",
            "backends/s3_locking_vultr.env",
            "python/delta_bench_interop/run_case.py",
            "python/delta_bench_tpcds/generate_store_sales_csv.py",
        ]
        manifest_text = "".join(f"{entry}\n" for entry in manifest_entries)
        write_overlay_manifest(exec_root, manifest_entries)

        sync_log = temp_root / "sync-log.jsonl"
        write_executable(
            scripts_dir / "sync_harness_to_delta_rs.sh",
            f"""#!/usr/bin/env python3
import json
import os
from pathlib import Path

checkout_dir = Path(os.environ["DELTA_RS_DIR"])
crate_dir = checkout_dir / "crates" / "delta-bench"
crate_dir.mkdir(parents=True, exist_ok=True)
(crate_dir / "benches").mkdir(parents=True, exist_ok=True)
(crate_dir / "benches" / "file_selection_bench.rs").write_text(
    "fn main() {{}}\\n",
    encoding="utf-8",
)
(crate_dir / ".delta_bench_overlay_manifest").write_text(
    {manifest_text!r},
    encoding="utf-8",
)
with open({str(sync_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(json.dumps({{"checkout": str(checkout_dir)}}) + "\\n")
""",
        )

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
        write_executable(
            fake_bin / "cargo",
            """#!/usr/bin/env python3
import sys
from pathlib import Path

crate_dir = Path.cwd() / "crates" / "delta-bench"
required = [
    crate_dir / "Cargo.toml",
    crate_dir / "benches" / "metadata_log_bench.rs",
    crate_dir / "benches" / "file_selection_bench.rs",
]
missing = [str(path) for path in required if not path.is_file()]
if missing:
    print("missing overlay files: " + ", ".join(missing), file=sys.stderr)
    raise SystemExit(23)

print("cargo ok")
""",
        )

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["DELTA_RS_DIR"] = str(exec_root)
        env["DELTA_BENCH_EXEC_ROOT"] = str(exec_root)

        result = subprocess.run(
            ["bash", str(bench_copy), "doctor"],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0, result.stderr or result.stdout
        sync_entries = [
            json.loads(line)
            for line in sync_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        assert sync_entries == [{"checkout": str(exec_root)}]
        assert (crate_dir / "benches" / "file_selection_bench.rs").is_file()


def test_bench_wrapper_resyncs_managed_checkout_after_compare_cleanup_removes_manifested_overlay_files() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        bench_copy = scripts_dir / "bench.sh"
        copy_executable(COMPARE_BRANCH, compare_copy)
        copy_executable(BENCH_SH, bench_copy)

        managed_checkout = temp_root / ".delta-rs-under-test"
        source_checkout = temp_root / ".delta-rs-source"
        compare_checkout_root = temp_root / ".delta-bench-compare-checkouts"
        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"

        tracked_entries = [
            "crates/delta-bench/Cargo.toml",
            "crates/delta-bench/Cargo.toml.delta-rs",
            "crates/delta-bench/benches/metadata_log_bench.rs",
            "crates/delta-bench/src/validation.rs",
            "bench/manifests/core_rust.yaml",
            "bench/manifests/core_python.yaml",
            "backends/s3_locking_vultr.env",
            "python/delta_bench_interop/run_case.py",
            "python/delta_bench_tpcds/generate_store_sales_csv.py",
        ]
        manifest_entries = tracked_entries + [
            "crates/delta-bench/benches/file_selection_bench.rs",
        ]
        manifest_text = "".join(f"{entry}\n" for entry in manifest_entries)
        for relative_path in tracked_entries:
            path = managed_checkout / relative_path
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("tracked\n", encoding="utf-8")
        (
            managed_checkout
            / "crates"
            / "delta-bench"
            / "benches"
            / "file_selection_bench.rs"
        ).write_text(
            "untracked overlay\n",
            encoding="utf-8",
        )
        (managed_checkout / ".git").mkdir(parents=True)
        (managed_checkout / ".bench-current-sha").write_text(
            f"{candidate_sha}\n",
            encoding="utf-8",
        )
        write_overlay_manifest(managed_checkout, manifest_entries)

        sync_log = temp_root / "sync-log.jsonl"
        write_executable(
            scripts_dir / "prepare_delta_rs.sh",
            """#!/usr/bin/env python3
import os
from pathlib import Path

checkout_dir = Path(os.environ["DELTA_RS_DIR"])
checkout_dir.mkdir(parents=True, exist_ok=True)
(checkout_dir / ".git").mkdir(exist_ok=True)
ref = os.environ.get("DELTA_RS_REF") or os.environ.get("DELTA_RS_BRANCH") or ""
(checkout_dir / ".bench-current-sha").write_text((ref or "0" * 40) + "\\n", encoding="utf-8")
print(f"delta-rs checkout ready: {checkout_dir}")
""",
        )
        write_executable(
            scripts_dir / "sync_harness_to_delta_rs.sh",
            f"""#!/usr/bin/env python3
import json
import os
from pathlib import Path

checkout_dir = Path(os.environ["DELTA_RS_DIR"])
entries = {manifest_entries!r}
for relative_path in entries:
    path = checkout_dir / relative_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("synced\\n", encoding="utf-8")
(checkout_dir / "crates" / "delta-bench" / ".delta_bench_overlay_manifest").write_text(
    {manifest_text!r},
    encoding="utf-8",
)
with open({str(sync_log)!r}, "a", encoding="utf-8") as handle:
    handle.write(json.dumps({{"checkout": str(checkout_dir)}}) + "\\n")
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
            f"""#!/usr/bin/env bash
set -euo pipefail

if [[ "${{1:-}}" == "-C" ]]; then
  repo="$2"
  shift 2
  case "${{1:-}}" in
    clean)
      shift
      target="${{@: -1}}"
      if [[ "$repo" == {str(managed_checkout)!r} && "$target" == "crates/delta-bench" ]]; then
        rm -f "$repo/crates/delta-bench/.delta_bench_overlay_manifest"
        rm -f "$repo/crates/delta-bench/benches/file_selection_bench.rs"
      fi
      exit 0
      ;;
    fetch|checkout|pull)
      exit 0
      ;;
    show-ref)
      exit 1
      ;;
    rev-parse)
      if [[ "$*" == *"HEAD"* ]]; then
        cat "$repo/.bench-current-sha"
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

required = cwd / "crates" / "delta-bench" / "benches" / "file_selection_bench.rs"
if not required.is_file():
    print(f"missing overlay file: {required}", file=sys.stderr)
    raise SystemExit(23)

if command == "data":
    fixtures_dir = Path(value(cli_args, "--fixtures-dir", "fixtures"))
    if not fixtures_dir.is_absolute():
        fixtures_dir = cwd / fixtures_dir
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    raise SystemExit(0)

if command == "run":
    results_dir = Path(value(cli_args, "--results-dir", "results"))
    if not results_dir.is_absolute():
        results_dir = cwd / results_dir
    label = value(cli_args, "--label", "local")
    target = value(cli_args, "--target", "scan")
    out_dir = results_dir / label
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / f"{target}.json").write_text(
        json.dumps({"label": label, "suite": target}),
        encoding="utf-8",
    )
    raise SystemExit(0)

raise SystemExit(0)
""",
        )

        python_pkg = temp_root / "python" / "delta_bench_compare"
        python_pkg.mkdir(parents=True)
        (python_pkg / "__init__.py").write_text("", encoding="utf-8")
        copy_compare_manifest_helper(python_pkg)
        write_executable(
            python_pkg / "aggregate.py",
            """#!/usr/bin/env python3
import argparse
import json
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--output", required=True)
parser.add_argument("--label", required=True)
parser.add_argument("--mode", default="decision")
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
print("hash policy ok")
""",
        )

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["DELTA_RS_DIR"] = str(managed_checkout)
        env["DELTA_RS_SOURCE_DIR"] = str(source_checkout)
        env["DELTA_BENCH_COMPARE_CHECKOUT_ROOT"] = str(compare_checkout_root)
        env["DELTA_BENCH_RESULTS"] = str(temp_root / "results")
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "0"
        env["BENCH_COMPARE_RUNS"] = "1"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_BENCH_MIN_FREE_GB"] = "1"

        compare_result = subprocess.run(
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

        assert compare_result.returncode == 0, (
            compare_result.stderr or compare_result.stdout
        )
        assert not (
            managed_checkout
            / "crates"
            / "delta-bench"
            / ".delta_bench_overlay_manifest"
        ).exists()
        assert not (
            managed_checkout
            / "crates"
            / "delta-bench"
            / "benches"
            / "file_selection_bench.rs"
        ).exists()

        doctor_env = env.copy()
        doctor_env["DELTA_BENCH_EXEC_ROOT"] = str(managed_checkout)
        doctor_result = subprocess.run(
            ["bash", str(bench_copy), "doctor"],
            cwd=temp_root,
            env=doctor_env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert doctor_result.returncode == 0, (
            doctor_result.stderr or doctor_result.stdout
        )
        sync_entries = [
            json.loads(line)
            for line in sync_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        assert sync_entries[-1] == {"checkout": str(managed_checkout)}
        assert (
            managed_checkout
            / "crates"
            / "delta-bench"
            / "benches"
            / "file_selection_bench.rs"
        ).is_file()


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
        copy_compare_manifest_helper(python_pkg)

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
parser.add_argument("--mode", default="decision")
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

if command == "validate":
    input_path = Path(value(cli_args, "--input"))
    output_path = Path(value(cli_args, "--output"))
    if not input_path.is_absolute():
        input_path = cwd / input_path
    if not output_path.is_absolute():
        output_path = cwd / output_path
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(input_path.read_text(encoding="utf-8"), encoding="utf-8")
    print(f"validated result: {{output_path}}")
    sys.exit(0)

sys.exit(0)
""",
        )

        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"
        results_rel = Path("results") / "remote compare 20260328"
        checkout_dir = remote_root / ".delta-rs-remote-checkout"
        compare_checkout_root = remote_root / ".delta-bench-compare-checkouts"
        base_checkout_dir = compare_checkout_root / base_sha
        candidate_checkout_dir = compare_checkout_root / candidate_sha

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
            base_checkout_dir.resolve(),
            candidate_checkout_dir.resolve(),
        }
        assert {entry["env_exec_root"] for entry in cargo_entries} == {
            str(base_checkout_dir),
            str(candidate_checkout_dir),
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


@pytest.mark.parametrize("suite", ["all", "interop_py", "scan_planning"])
def test_compare_branch_rejects_untrusted_macro_compare_suites_before_checkout(
    suite: str,
) -> None:
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
            ["bash", str(compare_copy), "main", "candidate", suite],
            cwd=temp_root,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert result.returncode != 0
        assert f"suite '{suite}' is not supported" in result.stderr
        assert "compare_branch.sh" in result.stderr
        assert "curated compare suites" in result.stderr
        assert "scan" in result.stderr
        assert "write_perf" in result.stderr
        assert "tpcds" in result.stderr
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


def test_compare_branch_acquires_source_checkout_lock_for_full_run() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE" in script
    assert "acquire_source_checkout_lock" in script
    assert "release_source_checkout_lock" in script
    assert re.search(
        r'DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE="\$\{DELTA_BENCH_SOURCE_CHECKOUT_LOCK_FILE:-\$\(default_checkout_lock_file \"\$\{DELTA_RS_SOURCE_DIR\}\"\)\}"',
        script,
    )


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
    assert re.search(
        r"--no-summary-table\)\s+no_summary_table=1\s+shift 1\s+;;", script
    )
    assert re.search(
        r"if \(\(\s*no_summary_table != 0\s*\)\); then\s+run_args\+=\(--no-summary-table\)",
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
        "--compare-checkouts",
        "--fixtures",
        "--delta-rs-source",
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


def test_cleanup_local_compare_checkout_target_honors_keep_last() -> None:
    with tempfile.TemporaryDirectory() as td:
        checkout_root = Path(td) / ".delta-bench-compare-checkouts"
        oldest = checkout_root / "1111111"
        middle = checkout_root / "2222222"
        newest = checkout_root / "3333333"
        for checkout_dir in (oldest, middle, newest):
            checkout_dir.mkdir(parents=True)
            (checkout_dir / "marker.txt").write_text(
                checkout_dir.name, encoding="utf-8"
            )

        now = time.time()
        os.utime(oldest, (now - 300, now - 300))
        os.utime(middle, (now - 200, now - 200))
        os.utime(newest, (now - 100, now - 100))

        env = os.environ.copy()
        env["DELTA_BENCH_COMPARE_CHECKOUT_ROOT"] = str(checkout_root)

        apply_run = subprocess.run(
            [
                "bash",
                str(LOCAL_CLEANUP),
                "--apply",
                "--compare-checkouts",
                "--keep-last",
                "2",
                "--allow-outside-root",
            ],
            cwd=REPO_ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert apply_run.returncode == 0, apply_run.stderr or apply_run.stdout
        assert not oldest.exists()
        assert middle.exists()
        assert newest.exists()
        assert "compare checkouts" in apply_run.stdout


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


def test_cleanup_local_source_checkout_target_removes_root_checkout_lock_artifacts() -> (
    None
):
    with tempfile.TemporaryDirectory() as td:
        root = Path(td)
        source_dir = root / ".delta-rs-source"
        source_dir.mkdir(parents=True)
        lock_file = root / ".delta-rs-source.delta_bench_checkout.lock"
        lock_dir = root / ".delta-rs-source.delta_bench_checkout.lock.dir"
        lock_file.write_text("", encoding="utf-8")
        lock_dir.mkdir()
        (lock_dir / "pid").write_text("123\n", encoding="utf-8")

        env = os.environ.copy()
        env["DELTA_RS_SOURCE_DIR"] = str(source_dir)

        apply_run = subprocess.run(
            [
                "bash",
                str(LOCAL_CLEANUP),
                "--apply",
                "--delta-rs-source",
                "--allow-outside-root",
            ],
            cwd=REPO_ROOT,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )

        assert apply_run.returncode == 0
        assert not source_dir.exists()
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
