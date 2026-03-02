from __future__ import annotations

import json
from pathlib import Path

VALID_CLASSIFICATIONS = {"supported", "expected_failure"}


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


def load_benchmark_payload(path: Path) -> dict:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError(f"{path}: invalid JSON ({exc.msg})") from exc
    if payload.get("schema_version") != 2:
        raise ValueError(
            f"{path}: top-level schema_version must be 2 (found {payload.get('schema_version')!r})"
        )
    context = payload.get("context")
    if not isinstance(context, dict):
        raise ValueError(f"{path}: context must be an object")
    if context.get("schema_version") != 2:
        raise ValueError(
            f"{path}: context.schema_version must be 2 (found {context.get('schema_version')!r})"
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
    return payload
