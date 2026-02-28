from __future__ import annotations

import concurrent.futures
import json
import os
import re
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional


SAFE_TOKEN = re.compile(r"^[A-Za-z0-9._-]+$")


@dataclass(frozen=True)
class MatrixArtifact:
    revision: str
    commit_timestamp: str
    artifact_path: str


@dataclass(frozen=True)
class MatrixRunConfig:
    suites: list[str]
    scales: list[str]
    timeout_seconds: int
    max_retries: int
    state_path: Path | str
    fixtures_dir: Path | str = "fixtures"
    results_dir: Path | str = "results"
    warmup: int = 1
    iterations: int = 5
    label_prefix: str = "longitudinal"
    max_parallel: int = 1
    max_load_per_cpu: float | None = None
    load_check_interval_seconds: float = 5.0


Executor = Callable[[MatrixArtifact, str, str, int, int], tuple[int, str]]


def load_matrix_state(path: Path | str) -> dict:
    state_path = Path(path)
    if not state_path.exists():
        return {"schema_version": 1, "cases": {}}
    return json.loads(state_path.read_text(encoding="utf-8"))


def save_matrix_state(path: Path | str, data: dict) -> None:
    state_path = Path(path)
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def run_matrix(
    *,
    artifacts: Iterable[MatrixArtifact],
    config: MatrixRunConfig,
    executor: Executor | None = None,
    load_provider: Callable[[], Optional[float]] | None = None,
    sleep_fn: Callable[[float], None] | None = None,
) -> dict:
    if config.timeout_seconds <= 0:
        raise ValueError("timeout_seconds must be > 0")
    if config.max_retries < 0:
        raise ValueError("max_retries must be >= 0")
    if config.warmup < 0 or config.iterations <= 0:
        raise ValueError("warmup must be >= 0 and iterations must be > 0")
    if config.max_parallel <= 0:
        raise ValueError("max_parallel must be > 0")
    if config.max_load_per_cpu is not None and config.max_load_per_cpu <= 0:
        raise ValueError("max_load_per_cpu must be > 0 when configured")
    if config.load_check_interval_seconds <= 0:
        raise ValueError("load_check_interval_seconds must be > 0")
    _validate_tokens(config.suites, "suite")
    _validate_tokens(config.scales, "scale")

    state = load_matrix_state(config.state_path)
    cases = state.setdefault("cases", {})
    run_exec = executor or (lambda a, s, sc, at, to: _default_executor(a, s, sc, at, to, config))
    get_load = load_provider or _system_load_per_cpu
    sleep = sleep_fn or time.sleep
    max_attempts = config.max_retries + 1
    pending: list[tuple[str, MatrixArtifact, str, str, int]] = []

    for artifact in artifacts:
        _validate_tokens([artifact.revision], "revision")
        for suite in config.suites:
            for scale in config.scales:
                key = _case_key(artifact.revision, suite, scale)
                existing = cases.get(key)
                if existing and existing.get("status") == "success":
                    continue
                # Retry budget is per invocation, not lifetime cumulative, so
                # failed cells are retried from attempt 1 on a new run_matrix call.
                attempts = 0
                pending.append((key, artifact, suite, scale, attempts))

    if not pending:
        return state

    in_flight: dict[concurrent.futures.Future, tuple[str, MatrixArtifact, str, str]] = {}
    next_idx = 0
    with concurrent.futures.ThreadPoolExecutor(max_workers=config.max_parallel) as pool:
        while next_idx < len(pending) or in_flight:
            while next_idx < len(pending) and len(in_flight) < config.max_parallel:
                _wait_for_load_guard(config, get_load, sleep)
                key, artifact, suite, scale, start_attempts = pending[next_idx]
                next_idx += 1
                future = pool.submit(
                    _execute_case,
                    artifact,
                    suite,
                    scale,
                    start_attempts,
                    run_exec,
                    max_attempts,
                    config.timeout_seconds,
                )
                in_flight[future] = (key, artifact, suite, scale)

            if not in_flight:
                continue

            done = next(concurrent.futures.as_completed(in_flight))
            key, artifact, suite, scale = in_flight.pop(done)
            try:
                status, attempts, failure_reason = done.result()
            except Exception as exc:  # noqa: BLE001 - persist worker errors
                status = "failure"
                attempts = max_attempts
                failure_reason = f"worker exception: {exc}"

            cases[key] = {
                "revision": artifact.revision,
                "suite": suite,
                "scale": scale,
                "status": status,
                "attempts": attempts,
                "failure_reason": failure_reason,
                "updated_at": datetime.now(timezone.utc).isoformat(),
            }
            save_matrix_state(config.state_path, state)

    return state


def _execute_case(
    artifact: MatrixArtifact,
    suite: str,
    scale: str,
    start_attempts: int,
    run_exec: Executor,
    max_attempts: int,
    timeout_seconds: int,
) -> tuple[str, int, str | None]:
    attempts = start_attempts
    status = "failure"
    failure_reason: str | None = "unknown failure"

    while attempts < max_attempts:
        attempt_number = attempts + 1
        try:
            exit_code, reason = run_exec(
                artifact,
                suite,
                scale,
                attempt_number,
                timeout_seconds,
            )
        except subprocess.TimeoutExpired:
            exit_code = 124
            reason = f"timeout after {timeout_seconds}s"
        except Exception as exc:  # noqa: BLE001 - persist failure details
            exit_code = 1
            reason = f"executor exception: {exc}"

        attempts = attempt_number
        if exit_code == 0:
            return "success", attempts, None
        status = "failure"
        failure_reason = reason or f"exit code {exit_code}"

    return status, attempts, failure_reason


def _default_executor(
    artifact: MatrixArtifact,
    suite: str,
    scale: str,
    _attempt: int,
    timeout_seconds: int,
    config: MatrixRunConfig,
) -> tuple[int, str]:
    artifact_binary = Path(artifact.artifact_path)
    if not artifact_binary.exists():
        return 1, f"artifact binary not found: {artifact_binary}"

    label = matrix_result_label(config.label_prefix, artifact.revision, scale)
    cmd = [
        str(artifact_binary),
        "--fixtures-dir",
        str(config.fixtures_dir),
        "--results-dir",
        str(config.results_dir),
        "--label",
        label,
        "--git-sha",
        artifact.revision,
        "run",
        "--scale",
        scale,
        "--target",
        suite,
        "--warmup",
        str(config.warmup),
        "--iterations",
        str(config.iterations),
    ]
    proc = subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout_seconds,
    )
    message = (proc.stderr.strip() or proc.stdout.strip()) if proc.returncode != 0 else ""
    return proc.returncode, message


def _validate_tokens(values: Iterable[str], field: str) -> None:
    for value in values:
        if not SAFE_TOKEN.match(value):
            raise ValueError(
                f"{field} '{value}' contains invalid characters; allowed [A-Za-z0-9._-]"
            )


def _case_key(revision: str, suite: str, scale: str) -> str:
    return f"{revision}|{suite}|{scale}"


def matrix_result_label(label_prefix: str, revision: str, scale: str) -> str:
    return sanitize_label(f"{label_prefix}-{revision}-{scale}")


def sanitize_label(value: str) -> str:
    out = "".join(ch if SAFE_TOKEN.match(ch) else "_" for ch in value)
    trimmed = out.strip("_")
    if not trimmed or trimmed in {".", ".."}:
        return "longitudinal"
    return trimmed


def _wait_for_load_guard(
    config: MatrixRunConfig,
    load_provider: Callable[[], Optional[float]],
    sleep_fn: Callable[[float], None],
) -> None:
    if config.max_load_per_cpu is None:
        return
    while True:
        current = load_provider()
        if current is None or current <= config.max_load_per_cpu:
            return
        sleep_fn(config.load_check_interval_seconds)


def _system_load_per_cpu() -> Optional[float]:
    try:
        load_one = os.getloadavg()[0]
    except (AttributeError, OSError):
        return None
    cpus = os.cpu_count() or 1
    if cpus <= 0:
        return None
    return load_one / float(cpus)
