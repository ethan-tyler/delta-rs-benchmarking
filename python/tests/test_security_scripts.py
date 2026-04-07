from __future__ import annotations

import os
import re
import subprocess
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts"
WORKFLOWS_DIR = REPO_ROOT / ".github" / "workflows"
CI_WORKFLOW = WORKFLOWS_DIR / "ci.yml"
DEPENDABOT = REPO_ROOT / ".github" / "dependabot.yml"
AUDIT_REQUIREMENTS = REPO_ROOT / "python" / "requirements-audit.txt"


def _run_script(
    name: str, args: list[str], *, env: dict[str, str] | None = None
) -> subprocess.CompletedProcess[str]:
    cmd = ["bash", str(SCRIPTS_DIR / name), *args]
    merged_env = os.environ.copy()
    if env:
        merged_env.update(env)
    return subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        cwd=REPO_ROOT,
        env=merged_env,
    )


def _self_hosted_benchmark_workflows() -> list[Path]:
    workflows = []
    for workflow_path in sorted(WORKFLOWS_DIR.glob("*.yml")):
        workflow = workflow_path.read_text(encoding="utf-8")
        if "runs-on: [self-hosted, delta-bench]" not in workflow:
            continue
        if "./scripts/" not in workflow:
            continue
        workflows.append(workflow_path)
    return workflows


def _github_hosted_workflows() -> list[Path]:
    workflows = []
    for workflow_path in sorted(WORKFLOWS_DIR.glob("*.yml")):
        workflow = workflow_path.read_text(encoding="utf-8")
        if "runs-on: ubuntu-latest" not in workflow:
            continue
        workflows.append(workflow_path)
    return workflows


def test_security_check_requires_explicit_expected_egress_hash() -> None:
    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        ruleset = (
            "table inet filter {\n"
            "\tchain output {\n"
            "\t\ttype filter hook output priority 0; policy drop;\n"
            "\t}\n"
            "}\n"
        )
        policy_file = tmp / "nftables.conf"
        policy_file.write_text(ruleset, encoding="utf-8")

        nft = tmp / "nft"
        nft.write_text(
            "#!/usr/bin/env bash\n"
            'if [[ "$1" == "list" && "$2" == "ruleset" ]]; then\n'
            "  cat <<'EOF'\n"
            f"{ruleset}"
            "EOF\n"
            "  exit 0\n"
            "fi\n"
            "exit 1\n",
            encoding="utf-8",
        )
        nft.chmod(0o755)

        result = _run_script(
            "security_check.sh",
            ["--require-egress-policy", "--egress-policy-path", str(policy_file)],
            env={
                "PATH": f"{tmp}:{os.environ['PATH']}",
                "DELTA_BENCH_FORBIDDEN_PROCESSES": "",
                "DELTA_BENCH_EGRESS_POLICY_SHA256": "",
            },
        )

        assert result.returncode != 0
        assert "must be provided explicitly" in result.stderr
        assert "nft list ruleset" in result.stderr


def test_security_mode_does_not_suppress_systemctl_failures() -> None:
    script = (SCRIPTS_DIR / "security_mode.sh").read_text(encoding="utf-8")
    assert re.search(r"systemctl [^\n]+\|\| true", script) is None


def test_security_mode_uses_locking_for_state_transitions() -> None:
    script = (SCRIPTS_DIR / "security_mode.sh").read_text(encoding="utf-8")
    assert "flock" in script
    assert "security-mode.lock" in script


def test_self_hosted_benchmark_workflows_enforce_runner_preflight() -> None:
    required_flags = (
        "--enforce-run-mode",
        "--require-no-public-ipv4",
        "--require-egress-policy",
    )
    explicit_preflight = "./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy"

    workflows = _self_hosted_benchmark_workflows()
    assert workflows, "expected at least one self-hosted benchmark workflow"

    for workflow_path in workflows:
        workflow = workflow_path.read_text(encoding="utf-8")
        for flag in required_flags:
            assert flag in workflow, f"{workflow_path.name} missing {flag}"
        assert (
            "DELTA_BENCH_EGRESS_POLICY_SHA256" in workflow
        ), f"{workflow_path.name} missing DELTA_BENCH_EGRESS_POLICY_SHA256 wiring"
        if "./scripts/compare_branch.sh" in workflow:
            continue
        assert (
            explicit_preflight in workflow
        ), f"{workflow_path.name} missing explicit security_check preflight"
        preflight_index = workflow.index(explicit_preflight)
        run_index = len(workflow)
        for benchmark_cmd in (
            "./scripts/bench.sh run",
            "./scripts/longitudinal_bench.sh run-matrix",
        ):
            if benchmark_cmd in workflow:
                run_index = min(run_index, workflow.index(benchmark_cmd))
        assert (
            preflight_index < run_index
        ), f"{workflow_path.name} runs benchmark execution before security_check preflight"


def test_ci_workflow_runs_only_hosted_smoke_and_correctness_validation_lanes() -> None:
    ci = CI_WORKFLOW.read_text(encoding="utf-8")

    assert "./scripts/bench.sh run" in ci
    assert "--lane smoke" in ci
    assert "--lane correctness" in ci
    assert "--lane macro" not in ci
    assert "./scripts/compare_branch.sh" not in ci
    assert "./scripts/longitudinal_bench.sh" not in ci


def test_github_hosted_workflows_do_not_run_macro_perf_or_criterion_benches() -> None:
    hosted = _github_hosted_workflows()
    assert hosted, "expected at least one GitHub-hosted workflow"

    for workflow_path in hosted:
        workflow = workflow_path.read_text(encoding="utf-8")
        assert "cargo bench" not in workflow, (
            f"{workflow_path.name} must not run Criterion microbenches on GitHub-hosted runners"
        )
        assert "--lane macro" not in workflow, (
            f"{workflow_path.name} must not run macro-lane benchmark workflows on GitHub-hosted runners"
        )
        assert "./scripts/compare_branch.sh" not in workflow, (
            f"{workflow_path.name} must not run branch compare on GitHub-hosted runners"
        )


def test_longitudinal_ingest_is_reserved_for_dedicated_self_hosted_workflows() -> None:
    ingest_workflows = []
    for workflow_path in sorted(WORKFLOWS_DIR.glob("*.yml")):
        workflow = workflow_path.read_text(encoding="utf-8")
        if "ingest-results" not in workflow:
            continue
        ingest_workflows.append(workflow_path)
        assert "runs-on: [self-hosted, delta-bench]" in workflow, (
            f"{workflow_path.name} must stay self-hosted for longitudinal ingest"
        )

    assert [workflow.name for workflow in ingest_workflows] == [
        "longitudinal-nightly.yml",
        "longitudinal-release-history.yml",
    ]


def test_ci_workflow_configures_dependency_audits_and_dependabot() -> None:
    ci = CI_WORKFLOW.read_text(encoding="utf-8")
    dependabot = DEPENDABOT.read_text(encoding="utf-8")
    requirements = AUDIT_REQUIREMENTS.read_text(encoding="utf-8")

    assert "dependency-audit:" in ci
    assert "cargo install cargo-audit --locked" in ci
    assert "cargo audit" in ci
    assert "RUSTSEC-2026-0037" in ci
    assert "RUSTSEC-2026-0041" in ci
    assert "RUSTSEC-2026-0049" in ci
    assert "python3 -m pip install pip-audit" in ci
    assert "python3 -m pip_audit -r python/requirements-audit.txt" in ci

    assert 'package-ecosystem: "cargo"' in dependabot
    assert 'package-ecosystem: "pip"' in dependabot

    for package in ("pandas", "polars", "pyarrow", "duckdb", "PyYAML"):
        assert package in requirements


def test_audit_requirements_pin_interop_runtime_versions() -> None:
    requirements = AUDIT_REQUIREMENTS.read_text(encoding="utf-8").splitlines()
    package_lines = [
        line.strip()
        for line in requirements
        if line.strip() and not line.lstrip().startswith("#")
    ]

    assert package_lines, "expected pinned runtime requirements"
    for line in package_lines:
        assert "==" in line, f"interop runtime requirement must be pinned: {line}"
