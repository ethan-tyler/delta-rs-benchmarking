from __future__ import annotations

import json
from pathlib import Path

VALID_CLASSIFICATIONS = {"supported", "expected_failure"}
V2_COMPARISON_CONTEXT_KEYS: tuple[str, ...] = ()
V2_REQUIRED_COMPARISON_CONTEXT_KEYS: tuple[str, ...] = ()
V3_COMPARISON_CONTEXT_KEYS = (
    "suite",
    "runner",
    "benchmark_mode",
    "timing_phase",
    "dataset_id",
    "dataset_fingerprint",
    "scale",
    "storage_backend",
    "backend_profile",
)
V3_REQUIRED_COMPARISON_CONTEXT_KEYS = (
    "suite",
    "runner",
    "benchmark_mode",
    "timing_phase",
    "dataset_fingerprint",
    "scale",
    "storage_backend",
)
V4_COMPARISON_CONTEXT_KEYS = (
    "suite",
    "runner",
    "timing_phase",
    "dataset_id",
    "dataset_fingerprint",
    "scale",
    "storage_backend",
    "backend_profile",
    "lane",
    "measurement_kind",
    "validation_level",
    "harness_revision",
    "fixture_recipe_hash",
    "fidelity_fingerprint",
)
V4_REQUIRED_COMPARISON_CONTEXT_KEYS = (
    "suite",
    "runner",
    "timing_phase",
    "dataset_fingerprint",
    "scale",
    "storage_backend",
    "lane",
    "measurement_kind",
    "validation_level",
    "harness_revision",
    "fixture_recipe_hash",
)


def _schema_version(payload: dict) -> int:
    return int(payload.get("schema_version") or 0)


def _comparison_context_keys(payload: dict) -> tuple[str, ...]:
    version = _schema_version(payload)
    if version >= 4:
        return V4_COMPARISON_CONTEXT_KEYS
    if version == 3:
        return V3_COMPARISON_CONTEXT_KEYS
    return V2_COMPARISON_CONTEXT_KEYS


def _required_comparison_context_keys(payload: dict) -> tuple[str, ...]:
    version = _schema_version(payload)
    if version >= 4:
        return V4_REQUIRED_COMPARISON_CONTEXT_KEYS
    if version == 3:
        return V3_REQUIRED_COMPARISON_CONTEXT_KEYS
    return V2_REQUIRED_COMPARISON_CONTEXT_KEYS


def case_classification(case: dict | None) -> str | None:
    if not case:
        return None
    value = case.get("classification")
    case_name = case.get("case", "<unknown>")
    if value is None:
        raise ValueError(f"case '{case_name}' is missing required classification")
    if not isinstance(value, str):
        raise ValueError(f"case '{case_name}' has non-string classification")
    if value not in VALID_CLASSIFICATIONS:
        raise ValueError(
            f"case '{case_name}' has invalid classification '{value}'; "
            "expected one of: supported, expected_failure"
        )
    return value


def case_perf_valid(case: dict | None) -> bool:
    if not case:
        return False
    value = case.get("perf_valid")
    if not isinstance(value, bool):
        raise ValueError(
            f"case '{case.get('case', '<unknown>')}' is missing required boolean perf_valid"
        )
    return value


def invalid_perf_case_names(payloads: tuple[dict, ...] | list[dict]) -> list[str]:
    return sorted(
        {
            case["case"]
            for payload in payloads
            for case in payload.get("cases", [])
            if _schema_version(payload) >= 3 and not case_perf_valid(case)
        }
    )


def _comparison_context_value(payload: dict, key: str, *, required: bool) -> str | None:
    context = payload.get("context") or {}
    value = context.get(key)
    if value is None:
        if required:
            label = context.get("label", "<unknown>")
            raise ValueError(
                f"payload '{label}' is missing required comparison context '{key}'"
            )
        return None
    if not isinstance(value, str) or not value:
        raise ValueError(
            f"comparison context '{key}' must be a non-empty string when present"
        )
    return value


def comparison_identity(payload: dict) -> dict[str, object | None]:
    identity = {
        key: _comparison_context_value(payload, key, required=True)
        for key in _required_comparison_context_keys(payload)
    }
    for key in _comparison_context_keys(payload):
        if key in identity:
            continue
        identity[key] = _comparison_context_value(payload, key, required=False)
    return identity


def ensure_matching_contexts(baseline: dict, candidate: dict) -> None:
    baseline_identity = comparison_identity(baseline)
    candidate_identity = comparison_identity(candidate)
    keys = sorted(
        set(_comparison_context_keys(baseline))
        | set(_comparison_context_keys(candidate))
    )
    mismatched = [
        key for key in keys if baseline_identity.get(key) != candidate_identity.get(key)
    ]
    if mismatched:
        raise ValueError(
            "context mismatch across benchmark payloads: "
            + ", ".join(
                f"{key}={baseline_identity.get(key)!r}!={candidate_identity.get(key)!r}"
                for key in mismatched
            )
        )


def load_benchmark_payload(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path}: invalid JSON ({exc.msg})") from exc
    if payload.get("schema_version") not in {2, 3, 4}:
        raise ValueError(
            f"{path}: top-level schema_version must be 2, 3, or 4 (found {payload.get('schema_version')!r})"
        )
    context = payload.get("context")
    if not isinstance(context, dict):
        raise ValueError(f"{path}: context must be an object")
    if context.get("schema_version") not in {2, 3, 4}:
        raise ValueError(
            f"{path}: context.schema_version must be 2, 3, or 4 (found {context.get('schema_version')!r})"
        )
    cases = payload.get("cases")
    if not isinstance(cases, list):
        raise ValueError(f"{path}: cases must be an array")
    seen_case_names: set[str] = set()
    for index, case in enumerate(cases):
        if not isinstance(case, dict):
            raise ValueError(f"{path}: each case entry must be an object")
        case_name = case.get("case")
        if not isinstance(case_name, str) or not case_name:
            raise ValueError(
                f"{path}: case at index {index} must define a non-empty string case id"
            )
        if case_name in seen_case_names:
            raise ValueError(f"{path}: duplicate case id '{case_name}' in cases array")
        seen_case_names.add(case_name)
        case_classification(case)
        if _schema_version(payload) >= 3:
            case_perf_valid(case)
    return payload
