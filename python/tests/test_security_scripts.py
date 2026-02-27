from __future__ import annotations

import os
import re
import subprocess
import tempfile
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPTS_DIR = REPO_ROOT / "scripts"


def _run_script(name: str, args: list[str], *, env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
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
            "if [[ \"$1\" == \"list\" && \"$2\" == \"ruleset\" ]]; then\n"
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
