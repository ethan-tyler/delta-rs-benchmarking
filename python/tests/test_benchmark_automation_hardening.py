from __future__ import annotations

import os
import re
import subprocess
import tempfile
import time
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
COMPARE_BRANCH = REPO_ROOT / "scripts" / "compare_branch.sh"
PREPARE_DELTA_RS = REPO_ROOT / "scripts" / "prepare_delta_rs.sh"
LOCAL_CLEANUP = REPO_ROOT / "scripts" / "cleanup_local.sh"
GITIGNORE = REPO_ROOT / ".gitignore"
WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark.yml"
NIGHTLY_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark-nightly.yml"
PRERELEASE_WORKFLOW = REPO_ROOT / ".github" / "workflows" / "benchmark-prerelease.yml"


def wait_for_condition(predicate: callable, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.05)
    return predicate()


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
    assert re.search(r"run_cmd\+=\(\"\\?\$\{storage_args\[@\]\}\"\)", script)


def test_compare_branch_does_not_retry_benchmark_producing_steps() -> None:
    script = COMPARE_BRANCH.read_text(encoding="utf-8")
    assert "run_step_no_retry()" in script
    assert re.search(
        r"run_step_no_retry env .*?/scripts/bench\.sh data --scale sf1 --seed 42",
        script,
    )
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
        r"compare_args=\(--noise-threshold \"\$\{NOISE_THRESHOLD\}\" --aggregation \"\$\{AGGREGATION\}\" --format text\)",
        script,
    )


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
        prepare_copy.write_text(PREPARE_DELTA_RS.read_text(encoding="utf-8"), encoding="utf-8")
        prepare_copy.chmod(0o755)

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
        fake_git = fake_bin / "git"
        clone_log = temp_root / "clone.log"
        release_clone = temp_root / "release-clone"
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
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
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
        wait_for_condition(
            lambda: clone_log.exists()
            and len(clone_log.read_text(encoding="utf-8").splitlines()) >= 2,
            timeout=3.0,
        )

        release_clone.write_text("ok\n", encoding="utf-8")
        first_stdout, first_stderr = first.communicate(timeout=10)
        second_stdout, second_stderr = second.communicate(timeout=10)

        clone_count = len(clone_log.read_text(encoding="utf-8").splitlines())
        assert first.returncode == 0, first_stderr or first_stdout
        assert second.returncode == 0, second_stderr or second_stdout
        assert clone_count == 1
        assert (checkout_dir / ".git").exists()


def test_gitignore_ignores_checkout_lock_artifacts() -> None:
    gitignore = GITIGNORE.read_text(encoding="utf-8")
    assert "*.delta_bench_checkout.lock" in gitignore
    assert "*.delta_bench_checkout.lock.dir/" in gitignore


def test_compare_branch_default_checkout_lock_does_not_block_initial_clone() -> None:
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        prepare_copy = scripts_dir / "prepare_delta_rs.sh"
        compare_copy.write_text(COMPARE_BRANCH.read_text(encoding="utf-8"), encoding="utf-8")
        prepare_copy.write_text(PREPARE_DELTA_RS.read_text(encoding="utf-8"), encoding="utf-8")
        compare_copy.chmod(0o755)
        prepare_copy.chmod(0o755)

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
            ["bash", str(compare_copy), "main", "candidate", "all"],
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
        assert not (temp_root / ".delta-rs-under-test" / ".delta_bench_checkout.lock").exists()


def test_compare_branch_rejects_checkout_lock_override_inside_managed_checkout_before_first_clone() -> None:
    with tempfile.TemporaryDirectory() as td:
        temp_root = Path(td)
        scripts_dir = temp_root / "scripts"
        scripts_dir.mkdir(parents=True)
        compare_copy = scripts_dir / "compare_branch.sh"
        prepare_copy = scripts_dir / "prepare_delta_rs.sh"
        compare_copy.write_text(COMPARE_BRANCH.read_text(encoding="utf-8"), encoding="utf-8")
        prepare_copy.write_text(PREPARE_DELTA_RS.read_text(encoding="utf-8"), encoding="utf-8")
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
            ["bash", str(compare_copy), "main", "candidate", "all"],
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


def test_cleanup_local_checkout_target_defaults_lock_artifacts_from_checkout_path() -> None:
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
