from __future__ import annotations

import subprocess
import threading
import time
from pathlib import Path

import pytest

from delta_bench_longitudinal.matrix_runner import (
    MatrixArtifact,
    MatrixRunConfig,
    load_matrix_state,
    run_matrix,
    save_matrix_state,
)


def test_resume_skips_successful_cases(tmp_path: Path) -> None:
    state_path = tmp_path / "matrix_state.json"
    save_matrix_state(
        state_path,
        {
            "schema_version": 1,
            "cases": {
                "revA|read_scan|sf1": {
                    "revision": "revA",
                    "suite": "read_scan",
                    "scale": "sf1",
                    "status": "success",
                    "attempts": 1,
                    "failure_reason": None,
                }
            },
        },
    )

    called: list[tuple[str, str, str, int]] = []

    def fake_executor(artifact: MatrixArtifact, suite: str, scale: str, attempt: int, timeout: int):
        called.append((artifact.revision, suite, scale, attempt))
        return 0, ""

    config = MatrixRunConfig(
        suites=["read_scan"],
        scales=["sf1"],
        timeout_seconds=1,
        max_retries=0,
        state_path=state_path,
    )
    run_matrix(
        artifacts=[MatrixArtifact(revision="revA", commit_timestamp="t", artifact_path="/tmp/a")],
        config=config,
        executor=fake_executor,
    )
    assert called == []


def test_retry_until_success_within_bound(tmp_path: Path) -> None:
    attempts = {"count": 0}

    def flaky_executor(artifact: MatrixArtifact, suite: str, scale: str, attempt: int, timeout: int):
        attempts["count"] += 1
        if attempts["count"] < 3:
            return 1, "transient failure"
        return 0, ""

    state_path = tmp_path / "matrix_state.json"
    config = MatrixRunConfig(
        suites=["read_scan"],
        scales=["sf1"],
        timeout_seconds=1,
        max_retries=2,
        state_path=state_path,
    )
    state = run_matrix(
        artifacts=[MatrixArtifact(revision="revB", commit_timestamp="t", artifact_path="/tmp/b")],
        config=config,
        executor=flaky_executor,
    )
    case = state["cases"]["revB|read_scan|sf1"]
    assert case["status"] == "success"
    assert case["attempts"] == 3
    assert attempts["count"] == 3


def test_timeout_is_recorded_as_failure_reason(tmp_path: Path) -> None:
    def timeout_executor(
        artifact: MatrixArtifact,
        suite: str,
        scale: str,
        attempt: int,
        timeout: int,
    ):
        raise subprocess.TimeoutExpired(cmd=["delta-bench"], timeout=timeout)

    state_path = tmp_path / "matrix_state.json"
    config = MatrixRunConfig(
        suites=["metadata"],
        scales=["sf1"],
        timeout_seconds=2,
        max_retries=1,
        state_path=state_path,
    )

    state = run_matrix(
        artifacts=[MatrixArtifact(revision="revC", commit_timestamp="t", artifact_path="/tmp/c")],
        config=config,
        executor=timeout_executor,
    )
    case = state["cases"]["revC|metadata|sf1"]
    assert case["status"] == "failure"
    assert case["attempts"] == 2
    assert "timeout" in (case["failure_reason"] or "")


def test_state_roundtrip(tmp_path: Path) -> None:
    state_path = tmp_path / "matrix_state.json"
    data = {"schema_version": 1, "cases": {}}
    save_matrix_state(state_path, data)
    assert load_matrix_state(state_path) == data


def test_failed_case_is_retried_on_subsequent_run(tmp_path: Path) -> None:
    state_path = tmp_path / "matrix_state.json"
    config = MatrixRunConfig(
        suites=["read_scan"],
        scales=["sf1"],
        timeout_seconds=1,
        max_retries=1,
        state_path=state_path,
    )
    artifact = MatrixArtifact(revision="revD", commit_timestamp="t", artifact_path="/tmp/d")

    def always_fail(
        _artifact: MatrixArtifact,
        _suite: str,
        _scale: str,
        _attempt: int,
        _timeout: int,
    ) -> tuple[int, str]:
        return 1, "fail"

    second_run_attempts: list[int] = []

    def then_succeed(
        _artifact: MatrixArtifact,
        _suite: str,
        _scale: str,
        attempt: int,
        _timeout: int,
    ) -> tuple[int, str]:
        second_run_attempts.append(attempt)
        return 0, ""

    first_state = run_matrix(artifacts=[artifact], config=config, executor=always_fail)
    assert first_state["cases"]["revD|read_scan|sf1"]["status"] == "failure"
    assert first_state["cases"]["revD|read_scan|sf1"]["attempts"] == 2

    second_state = run_matrix(artifacts=[artifact], config=config, executor=then_succeed)
    case = second_state["cases"]["revD|read_scan|sf1"]
    assert case["status"] == "success"
    assert case["failure_reason"] is None
    assert second_run_attempts == [1]


def test_parallel_execution_uses_multiple_workers(tmp_path: Path) -> None:
    state_path = tmp_path / "matrix_state.json"
    lock = threading.Lock()
    active = {"count": 0, "max": 0}

    def slow_executor(
        artifact: MatrixArtifact, suite: str, scale: str, attempt: int, timeout: int
    ):
        with lock:
            active["count"] += 1
            active["max"] = max(active["max"], active["count"])
        time.sleep(0.05)
        with lock:
            active["count"] -= 1
        return 0, ""

    config = MatrixRunConfig(
        suites=["read_scan"],
        scales=["sf1"],
        timeout_seconds=5,
        max_retries=0,
        state_path=state_path,
        max_parallel=2,
    )
    run_matrix(
        artifacts=[
            MatrixArtifact(revision="revP1", commit_timestamp="t", artifact_path="/tmp/p1"),
            MatrixArtifact(revision="revP2", commit_timestamp="t", artifact_path="/tmp/p2"),
        ],
        config=config,
        executor=slow_executor,
    )
    assert active["max"] >= 2


def test_load_guard_waits_before_dispatch(tmp_path: Path) -> None:
    state_path = tmp_path / "matrix_state.json"
    load_values = [2.0, 1.5, 0.4]
    sleep_calls: list[float] = []

    def fake_load() -> float:
        if load_values:
            return load_values.pop(0)
        return 0.1

    def fake_sleep(seconds: float) -> None:
        sleep_calls.append(seconds)

    config = MatrixRunConfig(
        suites=["read_scan"],
        scales=["sf1"],
        timeout_seconds=1,
        max_retries=0,
        state_path=state_path,
        max_parallel=1,
        max_load_per_cpu=1.0,
        load_check_interval_seconds=0.01,
    )

    run_matrix(
        artifacts=[MatrixArtifact(revision="revL", commit_timestamp="t", artifact_path="/tmp/l")],
        config=config,
        executor=lambda *_args: (0, ""),
        load_provider=fake_load,
        sleep_fn=fake_sleep,
    )
    assert sleep_calls == [0.01, 0.01]


def test_invalid_parallelism_is_rejected(tmp_path: Path) -> None:
    state_path = tmp_path / "matrix_state.json"
    config = MatrixRunConfig(
        suites=["read_scan"],
        scales=["sf1"],
        timeout_seconds=1,
        max_retries=0,
        state_path=state_path,
        max_parallel=0,
    )
    with pytest.raises(ValueError, match="max_parallel"):
        run_matrix(
            artifacts=[MatrixArtifact(revision="revBad", commit_timestamp="t", artifact_path="/tmp/b")],
            config=config,
            executor=lambda *_args: (0, ""),
        )
