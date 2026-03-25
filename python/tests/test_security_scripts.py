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
    explicit_preflight = (
        "./scripts/security_check.sh --enforce-run-mode --require-no-public-ipv4 --require-egress-policy"
    )

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
        assert explicit_preflight in workflow, (
            f"{workflow_path.name} missing explicit security_check preflight"
        )
        preflight_index = workflow.index(explicit_preflight)
        run_index = len(workflow)
        for benchmark_cmd in ("./scripts/bench.sh run", "./scripts/longitudinal_bench.sh run-matrix"):
            if benchmark_cmd in workflow:
                run_index = min(run_index, workflow.index(benchmark_cmd))
        assert preflight_index < run_index, (
            f"{workflow_path.name} runs benchmark execution before security_check preflight"
        )


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

    for package in ("pandas", "polars", "pyarrow", "duckdb"):
        assert package in requirements
