from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
COMPARE_BRANCH = REPO_ROOT / "scripts" / "compare_branch.sh"
FAKE_GIT_SCRIPT = """#!/usr/bin/env bash
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
"""


def write_executable(path: Path, content: str) -> None:
    path.write_text(content, encoding="utf-8")
    path.chmod(0o755)


def copy_executable(source: Path, dest: Path) -> None:
    dest.write_text(source.read_text(encoding="utf-8"), encoding="utf-8")
    dest.chmod(0o755)


def read_jsonl(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line
    ]


def install_prepare_sync_security_scripts(scripts_dir: Path, prep_log: Path) -> None:
    write_executable(
        scripts_dir / "prepare_delta_rs.sh",
        f"""#!/usr/bin/env python3
import json
import os
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


def copy_compare_package(root: Path) -> None:
    python_root = root / "python"
    python_root.mkdir(parents=True)
    shutil.copytree(
        REPO_ROOT / "python" / "delta_bench_compare",
        python_root / "delta_bench_compare",
    )


def fake_bench_script(run_log: Path) -> str:
    return f"""#!/usr/bin/env python3
import json
import os
import sys
from pathlib import Path


def value(args, flag, default=None):
    if flag not in args:
        return default
    idx = args.index(flag)
    return args[idx + 1]


def trusted_case(name, compatibility_key, elapsed_ms):
    return {{
        "case": name,
        "success": True,
        "validation_passed": True,
        "perf_status": "trusted",
        "classification": "supported",
        "samples": [{{"elapsed_ms": elapsed_ms}}],
        "run_summary": {{
            "sample_count": 1,
            "invalid_sample_count": 0,
            "host_label": "bench-host",
            "fidelity_fingerprint": "sha256:fidelity",
        }},
        "compatibility_key": compatibility_key,
        "supports_decision": True,
        "required_runs": 5,
        "decision_threshold_pct": 5.0,
        "decision_metric": "median",
        "failure_kind": None,
        "failure": None,
    }}


def invalid_case(name, compatibility_key):
    return {{
        "case": name,
        "success": False,
        "validation_passed": False,
        "perf_status": "invalid",
        "classification": "supported",
        "samples": [],
        "run_summary": {{
            "sample_count": 0,
            "invalid_sample_count": 0,
            "host_label": "bench-host",
            "fidelity_fingerprint": "sha256:fidelity",
        }},
        "compatibility_key": compatibility_key,
        "supports_decision": True,
        "required_runs": 5,
        "decision_threshold_pct": 5.0,
        "decision_metric": "median",
        "failure_kind": "assertion_mismatch",
        "failure": {{"message": "schema hash mismatch"}},
    }}


def cases_for_label(label):
    mode = os.environ.get("FAKE_BENCH_PAYLOAD_MODE", "all-invalid")
    base_cases = [
        invalid_case("scan_full_narrow", "sha256:case-a"),
        invalid_case("scan_projection_region", "sha256:case-b"),
    ]
    trusted_cases = [
        trusted_case("scan_full_narrow", "sha256:case-a", 100.0),
        trusted_case("scan_projection_region", "sha256:case-b", 90.0),
    ]
    if mode == "all-invalid":
        return base_cases
    if mode == "base-invalid":
        return base_cases if label.startswith("base-") else trusted_cases
    if mode == "candidate-invalid":
        return trusted_cases if label.startswith("base-") else base_cases
    raise SystemExit(f"unknown FAKE_BENCH_PAYLOAD_MODE={{mode}}")


args = sys.argv[1:]
command = args[0]
suite = value(args, "--suite", "scan")
results_dir = Path(os.environ["DELTA_BENCH_RESULTS"])
label = os.environ["DELTA_BENCH_LABEL"]

if command == "doctor":
    sys.exit(0)

if command == "data":
    sys.exit(0)

if command == "run":
    with open({str(run_log)!r}, "a", encoding="utf-8") as handle:
        handle.write(json.dumps({{"label": label}}) + "\\n")

    target_dir = results_dir / label
    target_dir.mkdir(parents=True, exist_ok=True)
    payload = {{
        "schema_version": 5,
        "context": {{
            "schema_version": 5,
            "label": label,
            "suite": suite,
            "benchmark_mode": "perf",
            "timing_phase": "execute",
            "dataset_id": "medium_selective",
            "dataset_fingerprint": "sha256:fixture",
            "runner": "rust",
            "scale": "sf10",
            "storage_backend": "local",
            "backend_profile": "local",
            "lane": "macro",
            "measurement_kind": "phase_breakdown",
            "validation_level": "operational",
            "harness_revision": "harness-rev",
            "fixture_recipe_hash": "sha256:recipe",
            "fidelity_fingerprint": "sha256:fidelity",
        }},
        "cases": cases_for_label(label),
    }}
    (target_dir / f"{{suite}}.json").write_text(json.dumps(payload), encoding="utf-8")
    sys.exit(0)

raise SystemExit(0)
"""


def test_compare_branch_decision_mode_fails_fast_when_suite_has_no_trusted_cases() -> (
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

        prep_log = temp_root / "prep-log.jsonl"
        run_log = temp_root / "run-log.jsonl"

        write_executable(
            scripts_dir / "prepare_delta_rs.sh",
            f"""#!/usr/bin/env python3
import json
import os
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
suite = value(args, "--suite", "scan")
results_dir = Path(os.environ["DELTA_BENCH_RESULTS"])
label = os.environ["DELTA_BENCH_LABEL"]

if command == "doctor":
    sys.exit(0)

if command == "data":
    sys.exit(0)

if command == "run":
    with open({str(run_log)!r}, "a", encoding="utf-8") as handle:
        handle.write(json.dumps({{"label": label}}) + "\\n")

    target_dir = results_dir / label
    target_dir.mkdir(parents=True, exist_ok=True)
    payload = {{
        "schema_version": 5,
        "context": {{
            "schema_version": 5,
            "label": label,
            "suite": suite,
            "benchmark_mode": "perf",
            "timing_phase": "execute",
            "dataset_id": "medium_selective",
            "dataset_fingerprint": "sha256:fixture",
            "runner": "rust",
            "scale": "sf10",
            "storage_backend": "local",
            "backend_profile": "local",
            "lane": "macro",
            "measurement_kind": "phase_breakdown",
            "validation_level": "operational",
            "harness_revision": "harness-rev",
            "fixture_recipe_hash": "sha256:recipe",
            "fidelity_fingerprint": "sha256:fidelity",
        }},
        "cases": [
            {{
                "case": "scan_full_narrow",
                "success": False,
                "validation_passed": False,
                "perf_status": "invalid",
                "classification": "supported",
                "samples": [],
                "run_summary": {{
                    "sample_count": 0,
                    "invalid_sample_count": 0,
                    "host_label": "bench-host",
                    "fidelity_fingerprint": "sha256:fidelity",
                }},
                "compatibility_key": "sha256:case-a",
                "supports_decision": True,
                "required_runs": 5,
                "decision_threshold_pct": 5.0,
                "decision_metric": "median",
                "failure_kind": "assertion_mismatch",
                "failure": {{"message": "schema hash mismatch"}},
            }},
            {{
                "case": "scan_projection_region",
                "success": False,
                "validation_passed": False,
                "perf_status": "invalid",
                "classification": "supported",
                "samples": [],
                "run_summary": {{
                    "sample_count": 0,
                    "invalid_sample_count": 0,
                    "host_label": "bench-host",
                    "fidelity_fingerprint": "sha256:fidelity",
                }},
                "compatibility_key": "sha256:case-b",
                "supports_decision": True,
                "required_runs": 5,
                "decision_threshold_pct": 5.0,
                "decision_metric": "median",
                "failure_kind": "assertion_mismatch",
                "failure": {{"message": "schema hash mismatch"}},
            }},
        ],
    }}
    (target_dir / f"{{suite}}.json").write_text(json.dumps(payload), encoding="utf-8")
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
        env["BENCH_PREWARM_ITERS"] = "1"
        env["BENCH_COMPARE_RUNS"] = "5"
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
                "--compare-mode",
                "decision",
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

        assert result.returncode == 1, result.stderr or result.stdout
        assert "produced no trusted cases" in result.stderr
        assert "Traceback" not in result.stderr

        run_entries = [
            json.loads(line)
            for line in run_log.read_text(encoding="utf-8").splitlines()
            if line
        ]
        assert [entry["label"] for entry in run_entries] == [
            f"base-{base_sha}-prewarm",
            f"cand-{candidate_sha}-prewarm",
        ]

        artifact_dir = results_dir / "compare" / "scan" / f"{base_sha}__{candidate_sha}"
        assert not artifact_dir.exists()


def test_compare_branch_decision_mode_fails_fast_when_only_base_has_no_trusted_cases() -> (
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

        prep_log = temp_root / "prep-log.jsonl"
        run_log = temp_root / "run-log.jsonl"
        install_prepare_sync_security_scripts(scripts_dir, prep_log)
        write_executable(scripts_dir / "bench.sh", fake_bench_script(run_log))

        fake_bin = temp_root / "bin"
        fake_bin.mkdir()
        write_executable(fake_bin / "git", FAKE_GIT_SCRIPT)
        copy_compare_package(temp_root)

        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"
        results_dir = temp_root / "results"

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "1"
        env["BENCH_COMPARE_RUNS"] = "5"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_RS_DIR"] = str(managed_checkout)
        env["DELTA_RS_SOURCE_DIR"] = str(source_checkout)
        env["DELTA_BENCH_COMPARE_CHECKOUT_ROOT"] = str(compare_checkout_root)
        env["DELTA_BENCH_RESULTS"] = str(results_dir)
        env["DELTA_BENCH_MIN_FREE_GB"] = "1"
        env["FAKE_BENCH_PAYLOAD_MODE"] = "base-invalid"

        result = subprocess.run(
            [
                "bash",
                str(compare_copy),
                "--compare-mode",
                "decision",
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

        assert result.returncode == 1, result.stderr or result.stdout
        assert "produced no trusted cases on base" in result.stderr
        assert "Traceback" not in result.stderr

        run_entries = read_jsonl(run_log)
        assert [entry["label"] for entry in run_entries] == [
            f"base-{base_sha}-prewarm",
            f"cand-{candidate_sha}-prewarm",
        ]

        artifact_dir = results_dir / "compare" / "scan" / f"{base_sha}__{candidate_sha}"
        assert not artifact_dir.exists()


def test_compare_branch_remote_runner_decision_mode_fails_fast_without_local_result_reads() -> (
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
        copy_executable(COMPARE_BRANCH, compare_copy)

        prep_log = remote_root / "prep-log.jsonl"
        run_log = remote_root / "run-log.jsonl"
        install_prepare_sync_security_scripts(remote_scripts, prep_log)
        write_executable(remote_scripts / "bench.sh", fake_bench_script(run_log))
        copy_compare_package(remote_root)

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
    handle.write(json.dumps({{"host": host, "payload": payload}}) + "\\n")

raise SystemExit(
    subprocess.run(["bash", "-c", payload], check=False, env=remote_env).returncode
)
""",
        )
        write_executable(fake_bin / "git", FAKE_GIT_SCRIPT)

        base_sha = "de04240bfae85a86dd73519b41e05b9be7a5924f"
        candidate_sha = "c12fd57876c5f07e5fc2c3ade1ce4408de45a2f9"
        results_rel = Path("results") / "remote fail fast"
        checkout_dir = remote_root / ".delta-rs-remote-checkout"
        source_checkout = remote_root / ".delta-rs-source"
        compare_checkout_root = remote_root / ".delta-bench-compare-checkouts"

        env = os.environ.copy()
        env["PATH"] = f"{fake_bin}:{env['PATH']}"
        env["BENCH_RETRY_ATTEMPTS"] = "1"
        env["BENCH_PREWARM_ITERS"] = "1"
        env["BENCH_COMPARE_RUNS"] = "5"
        env["BENCH_WARMUP"] = "1"
        env["BENCH_ITERS"] = "1"
        env["DELTA_RS_DIR"] = str(checkout_dir)
        env["DELTA_RS_SOURCE_DIR"] = str(source_checkout)
        env["DELTA_BENCH_COMPARE_CHECKOUT_ROOT"] = str(compare_checkout_root)
        env["DELTA_BENCH_RESULTS"] = str(results_rel)
        env["FAKE_BENCH_PAYLOAD_MODE"] = "all-invalid"

        result = subprocess.run(
            [
                "bash",
                str(compare_copy),
                "--compare-mode",
                "decision",
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

        assert result.returncode == 1, result.stderr or result.stdout
        assert "produced no trusted cases" in result.stderr
        assert "Traceback" not in result.stderr
        assert "FileNotFoundError" not in result.stderr

        run_entries = read_jsonl(run_log)
        assert [entry["label"] for entry in run_entries] == [
            f"base-{base_sha}-prewarm",
            f"cand-{candidate_sha}-prewarm",
        ]

        remote_results_dir = remote_root / results_rel
        artifact_dir = (
            remote_results_dir / "compare" / "scan" / f"{base_sha}__{candidate_sha}"
        )
        assert not artifact_dir.exists()
        assert not (local_root / results_rel).exists()
