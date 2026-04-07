from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

from .registry import (
    DEFAULT_REGISTRY_PATH,
    load_registry,
    pack_suite_definitions,
    readiness_blockers,
    resolve_pack,
)


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _resolve_path(manifest_path: Path, raw: str | None) -> Path | None:
    if raw in {None, ""}:
        return None
    candidate = Path(raw)
    if candidate.is_absolute():
        return candidate
    return (manifest_path.parent / candidate).resolve()


def _slug(value: str) -> str:
    return value.replace("_", "-")


def _shard_key(*, suite: str, profile: str) -> str:
    return f"{suite}__{profile}"


def _artifact_name(*, suite: str, profile: str) -> str:
    return f"benchmark-{_slug(suite)}-{_slug(profile)}"


def _suite_profile_label(*, suite: str, profile: str) -> str:
    return f"{suite}[{profile}]"


def _planned_suite_definitions(
    registry: dict[str, Any],
    pack: dict[str, Any],
    pack_manifest_payload: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    expected_entries = pack_suite_definitions(registry, pack)
    if pack_manifest_payload is None:
        return expected_entries

    raw_include = pack_manifest_payload.get("include")
    if not isinstance(raw_include, list) or not raw_include:
        return expected_entries

    expected_by_identity = {
        (str(entry["suite"]), str(entry["profile"])): entry for entry in expected_entries
    }
    expected_by_surface = {
        str(entry["surface"]): entry
        for entry in expected_entries
        if entry.get("surface") not in {None, ""}
    }
    expected_count_by_suite: dict[str, int] = {}
    for entry in expected_entries:
        suite = str(entry["suite"])
        expected_count_by_suite[suite] = expected_count_by_suite.get(suite, 0) + 1

    planned_entries: list[dict[str, Any]] = []
    seen_identities: set[tuple[str, str]] = set()

    for index, raw_entry in enumerate(raw_include):
        if not isinstance(raw_entry, dict):
            raise ValueError(f"pack manifest include row {index} must be a mapping")
        raw_surface = raw_entry.get("surface")
        expected: dict[str, Any] | None = None
        if raw_surface not in {None, ""}:
            if not isinstance(raw_surface, str):
                raise ValueError(
                    f"pack manifest include row {index} has invalid surface"
                )
            expected = expected_by_surface.get(raw_surface)
            if expected is None:
                raise ValueError(
                    f"pack manifest references unknown surface '{raw_surface}'"
                )
            suite = str(raw_entry.get("suite") or expected["suite"])
            if suite != str(expected["suite"]):
                raise ValueError(
                    f"pack manifest surface '{raw_surface}' does not match suite '{suite}'"
                )
            raw_profile = raw_entry.get("profile")
            profile = str(expected["profile"])
            if raw_profile not in {None, "", profile}:
                raise ValueError(
                    f"pack manifest surface '{raw_surface}' does not match profile '{raw_profile}'"
                )
        else:
            suite = raw_entry.get("suite")
            if not isinstance(suite, str) or not suite:
                raise ValueError(f"pack manifest include row {index} is missing suite")
            raw_profile = raw_entry.get("profile")
            if raw_profile in {None, ""}:
                if expected_count_by_suite.get(suite, 0) != 1:
                    raise ValueError(
                        f"pack manifest include row {index} must include profile for suite '{suite}'"
                    )
                profile = next(
                    str(entry["profile"])
                    for entry in expected_entries
                    if str(entry["suite"]) == suite
                )
            else:
                profile = str(raw_profile)
            expected = expected_by_identity.get((suite, profile))
        identity = (suite, profile)
        if identity in seen_identities:
            raise ValueError(
                f"pack manifest includes duplicate suite/profile '{suite}[{profile}]'"
            )
        if expected is None:
            raise ValueError(
                f"pack manifest references unknown suite/profile '{suite}[{profile}]'"
            )
        timeout_minutes = raw_entry.get(
            "timeout_minutes", expected["timeout_minutes"]
        )
        storage_backend = raw_entry.get(
            "storage_backend", expected.get("storage_backend")
        )
        backend_profile = raw_entry.get(
            "backend_profile", expected.get("backend_profile")
        )
        shard_key = raw_entry.get(
            "shard_key", _shard_key(suite=suite, profile=str(profile))
        )
        artifact_name = raw_entry.get(
            "artifact_name", _artifact_name(suite=suite, profile=str(profile))
        )
        if not isinstance(profile, str) or not profile:
            raise ValueError(f"pack manifest suite '{suite}' is missing a profile")
        if not isinstance(timeout_minutes, int):
            raise ValueError(
                f"pack manifest suite '{suite}' is missing integer timeout_minutes"
            )
        if not isinstance(shard_key, str) or not shard_key:
            raise ValueError(f"pack manifest suite '{suite}' is missing shard_key")
        if not isinstance(artifact_name, str) or not artifact_name:
            raise ValueError(f"pack manifest suite '{suite}' is missing artifact_name")
        planned_entries.append(
            {
                "suite": suite,
                "surface": (
                    str(raw_surface)
                    if raw_surface not in {None, ""}
                    else expected.get("surface")
                ),
                "profile": profile,
                "timeout_minutes": timeout_minutes,
                "shard_key": shard_key,
                "artifact_name": artifact_name,
                "storage_backend": (
                    str(storage_backend) if storage_backend not in {None, ""} else None
                ),
                "backend_profile": (
                    str(backend_profile) if backend_profile not in {None, ""} else None
                ),
                "suite_registry": expected["suite_registry"],
            }
        )
        seen_identities.add(identity)

    return planned_entries


def plan_pack(
    *,
    registry_path: Path | None,
    pack_ref: str,
    base_sha: str,
    candidate_sha: str,
) -> dict[str, Any]:
    registry = load_registry(registry_path)
    pack_id, pack, alias = resolve_pack(registry, pack_ref)
    blockers = readiness_blockers(registry, pack)
    if blockers:
        details = "\n".join(
            f"- {blocker['suite']}: {blocker['reason'] or blocker['readiness']}"
            for blocker in blockers
        )
        raise ValueError(
            f"pack '{pack_id}' is blocked by gated suites:\n{details}"
        )

    include = []
    for suite_entry in pack_suite_definitions(registry, pack):
        entry = {
            "suite": suite_entry["suite"],
            "profile": suite_entry["profile"],
            "timeout_minutes": suite_entry["timeout_minutes"],
            "shard_key": _shard_key(
                suite=suite_entry["suite"], profile=suite_entry["profile"]
            ),
            "artifact_name": _artifact_name(
                suite=suite_entry["suite"], profile=suite_entry["profile"]
            ),
        }
        if suite_entry.get("surface") is not None:
            entry["surface"] = suite_entry["surface"]
        if suite_entry.get("storage_backend") is not None:
            entry["storage_backend"] = suite_entry["storage_backend"]
        if suite_entry.get("backend_profile") is not None:
            entry["backend_profile"] = suite_entry["backend_profile"]
        include.append(entry)
    return {
        "pack_id": pack_id,
        "pack_alias": alias,
        "pack_version": int(pack.get("pack_version") or 0),
        "compare_mode": str(pack.get("compare_mode") or ""),
        "strict_mode": str(pack.get("strict_mode") or ""),
        "max_parallel": int(pack.get("max_parallel") or 1),
        "overall_fail_on": list(pack.get("overall_fail_on") or []),
        "base_sha": base_sha,
        "candidate_sha": candidate_sha,
        "include": include,
    }


def _render_plan_payload(
    payload: dict[str, Any], output_format: str
) -> dict[str, Any]:
    rendered = dict(payload)
    if output_format == "github-matrix":
        rendered["matrix"] = {"include": payload["include"]}
    return rendered


def _require_matching_shas(
    manifests: list[dict[str, Any]],
) -> tuple[str, str]:
    first = manifests[0]
    base_sha = str(first.get("base_sha") or "")
    candidate_sha = str(first.get("candidate_sha") or "")
    if not base_sha or not candidate_sha:
        raise ValueError("suite manifests must include base_sha and candidate_sha")
    for manifest in manifests[1:]:
        current_base = str(manifest.get("base_sha") or "")
        current_candidate = str(manifest.get("candidate_sha") or "")
        if current_base != base_sha or current_candidate != candidate_sha:
            raise ValueError("suite manifests disagree on base_sha/candidate_sha")
    return base_sha, candidate_sha


def _sha_pair_from_payload(
    payload: dict[str, Any] | None, source_name: str
) -> tuple[str, str] | None:
    if payload is None:
        return None
    base_sha = str(payload.get("base_sha") or "")
    candidate_sha = str(payload.get("candidate_sha") or "")
    if not base_sha and not candidate_sha:
        return None
    if not base_sha or not candidate_sha:
        raise ValueError(
            f"{source_name} must include both base_sha and candidate_sha when either is present"
        )
    return base_sha, candidate_sha


def _resolve_base_candidate_shas(
    *,
    manifests: list[dict[str, Any]],
    pack_manifest_payload: dict[str, Any] | None,
    request_state_payload: dict[str, Any] | None,
) -> tuple[str, str]:
    candidates: list[tuple[str, str]] = []
    if manifests:
        candidates.append(_require_matching_shas(manifests))
    for source_name, payload in (
        ("pack manifest", pack_manifest_payload),
        ("request-state", request_state_payload),
    ):
        pair = _sha_pair_from_payload(payload, source_name)
        if pair is not None:
            candidates.append(pair)
    if not candidates:
        raise ValueError(
            "unable to determine base_sha/candidate_sha; provide suite manifests, pack manifest, or request-state with pinned SHAs"
        )

    base_sha, candidate_sha = candidates[0]
    for current_base, current_candidate in candidates[1:]:
        if current_base != base_sha or current_candidate != candidate_sha:
            raise ValueError(
                "suite manifests, pack manifest, and request-state disagree on base_sha/candidate_sha"
            )
    return base_sha, candidate_sha


def _flatten_rows(
    *,
    suite: str,
    profile: str,
    comparison_payload: dict[str, Any],
) -> list[dict[str, Any]]:
    rows = comparison_payload.get("rows")
    if not isinstance(rows, list):
        raise ValueError(f"suite '{suite}' comparison rows must be a list")
    flattened: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError(f"suite '{suite}' comparison row must be a mapping")
        flattened.append({"suite": suite, "profile": profile, **row})
    return flattened


def _sum_summary(payloads: list[dict[str, Any]]) -> dict[str, int]:
    out = {
        "faster": 0,
        "slower": 0,
        "no_change": 0,
        "incomparable": 0,
        "new": 0,
        "removed": 0,
    }
    for payload in payloads:
        summary = payload.get("summary")
        if not isinstance(summary, dict):
            raise ValueError("comparison summary must be a mapping")
        for key in out:
            out[key] += int(summary.get(key) or 0)
    return out


def _overall_status(flattened_rows: list[dict[str, Any]], fail_on: list[str]) -> str:
    statuses = {str(row.get("status") or "") for row in flattened_rows}
    return "failed" if any(status in statuses for status in fail_on) else "passed"


def _overall_status_with_suite_states(
    *,
    flattened_rows: list[dict[str, Any]],
    suite_statuses: dict[str, str],
    fail_on: list[str],
) -> str:
    if any(status in {"failed", "blocked"} for status in suite_statuses.values()):
        return "failed"
    return _overall_status(flattened_rows, fail_on)


def _suite_state_by_identity(
    request_state_payload: dict[str, Any] | None,
) -> dict[tuple[str, str], dict[str, Any]]:
    if request_state_payload is None:
        return {}
    raw_suites = request_state_payload.get("suites")
    if not isinstance(raw_suites, list):
        raise ValueError("request-state suites must be a list")
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for index, raw_entry in enumerate(raw_suites):
        if not isinstance(raw_entry, dict):
            raise ValueError(f"request-state suite row {index} must be a mapping")
        suite = str(raw_entry.get("suite") or "")
        if not suite:
            raise ValueError(f"request-state suite row {index} is missing suite")
        profile = str(raw_entry.get("profile") or "")
        if not profile:
            raise ValueError(f"request-state suite row {index} is missing profile")
        identity = (suite, profile)
        if identity in out:
            raise ValueError(
                f"request-state includes duplicate suite/profile '{suite}[{profile}]'"
            )
        out[identity] = raw_entry
    return out


def _render_summary_markdown(
    *,
    pack_id: str,
    compare_mode: str,
    overall_status: str,
    suite_rows: list[dict[str, Any]],
    summary: dict[str, int],
) -> str:
    lines = [
        f"# Pack Summary: {pack_id}",
        "",
        f"- Overall: {overall_status.upper()}",
        f"- Compare mode: {compare_mode}",
        f"- Shards summarized: {len(suite_rows)}",
        "",
        "| Suite | Profile | Status | Faster | Slower | No Change | Incomparable |",
        "| --- | --- | --- | --- | --- | --- | --- |",
    ]
    for row in suite_rows:
        lines.append(
            "| {suite} | {profile} | {status} | {faster} | {slower} | {no_change} | {incomparable} |".format(
                **row
            )
        )
    lines.extend(
        [
            "",
            "## Combined Summary",
            "",
            f"- Faster: {summary['faster']}",
            f"- Slower: {summary['slower']}",
            f"- No change: {summary['no_change']}",
            f"- Incomparable: {summary['incomparable']}",
            f"- New: {summary['new']}",
            f"- Removed: {summary['removed']}",
        ]
    )
    return "\n".join(lines) + "\n"


def summarize_pack(
    *,
    registry_path: Path | None,
    pack_ref: str,
    suite_manifest_paths: list[Path],
    output_dir: Path | None,
    pack_manifest_path: Path | None = None,
    request_state_path: Path | None = None,
) -> dict[str, Any]:
    if not suite_manifest_paths and request_state_path is None:
        raise ValueError(
            "at least one --suite-manifest path or --request-state is required"
        )

    registry = load_registry(registry_path)
    pack_ref_to_resolve = pack_ref
    pack_manifest_payload: dict[str, Any] | None = None
    request_state_payload: dict[str, Any] | None = None
    if pack_manifest_path is not None:
        pack_manifest_payload = _load_json(pack_manifest_path)
        pack_ref_to_resolve = str(pack_manifest_payload.get("pack_id") or pack_ref)
    if request_state_path is not None:
        request_state_payload = _load_json(request_state_path)
    pack_id, pack, alias = resolve_pack(registry, pack_ref_to_resolve)
    expected_suite_entries = _planned_suite_definitions(
        registry, pack, pack_manifest_payload
    )
    expected_count_by_suite: dict[str, int] = {}
    for entry in expected_suite_entries:
        suite = str(entry["suite"])
        expected_count_by_suite[suite] = expected_count_by_suite.get(suite, 0) + 1

    manifest_pairs: list[tuple[Path, dict[str, Any]]] = []
    actual_by_identity: dict[tuple[str, str], tuple[Path, dict[str, Any]]] = {}
    for manifest_path in suite_manifest_paths:
        manifest = _load_json(manifest_path)
        suite = str(manifest.get("suite") or "")
        if not suite:
            raise ValueError(f"{manifest_path}: suite manifest is missing suite")
        raw_profile = str(
            manifest.get("profile") or manifest.get("methodology_profile") or ""
        )
        if not raw_profile:
            if expected_count_by_suite.get(suite, 0) != 1:
                raise ValueError(
                    f"{manifest_path}: suite manifest for '{suite}' must include profile"
                )
            raw_profile = next(
                str(entry["profile"])
                for entry in expected_suite_entries
                if str(entry["suite"]) == suite
            )
        identity = (suite, raw_profile)
        if identity in actual_by_identity:
            raise ValueError(
                f"duplicate suite manifest provided for '{suite}[{raw_profile}]'"
            )
        actual_by_identity[identity] = (manifest_path, manifest)

    expected_suite_order = [
        (str(entry["suite"]), str(entry["profile"])) for entry in expected_suite_entries
    ]
    expected_suite_names = set(expected_suite_order)
    actual_suite_names = set(actual_by_identity)
    missing_suites = [
        identity for identity in expected_suite_order if identity not in actual_suite_names
    ]
    unexpected_suites = sorted(actual_suite_names - expected_suite_names)
    if missing_suites or unexpected_suites:
        suite_state_by_name = _suite_state_by_identity(request_state_payload)
        unresolved_missing = [
            identity for identity in missing_suites if identity not in suite_state_by_name
        ]
        missing_without_terminal_state = [
            identity
            for identity in missing_suites
            if identity in suite_state_by_name
            and str(suite_state_by_name[identity].get("status") or "")
            not in {"failed", "blocked", "skipped"}
        ]
        if unexpected_suites or unresolved_missing or missing_without_terminal_state:
            render_identities = lambda identities: ", ".join(
                _suite_profile_label(suite=suite, profile=profile)
                for suite, profile in identities
            )
            details: list[str] = []
            if missing_suites:
                details.append(
                    "missing suite manifests: " + render_identities(missing_suites)
                )
            if missing_without_terminal_state:
                details.append(
                    "missing suites without terminal request-state rows: "
                    + render_identities(missing_without_terminal_state)
                )
            if unexpected_suites:
                details.append(
                    "unexpected suite manifests: " + render_identities(unexpected_suites)
                )
            raise ValueError("; ".join(details))

    manifest_pairs.extend(
        actual_by_identity[identity]
        for identity in expected_suite_order
        if identity in actual_by_identity
    )
    manifests = [manifest for _, manifest in manifest_pairs]
    base_sha, candidate_sha = _resolve_base_candidate_shas(
        manifests=manifests,
        pack_manifest_payload=pack_manifest_payload,
        request_state_payload=request_state_payload,
    )
    resolved_output_dir = (
        output_dir
        if output_dir is not None
        else Path("results")
        / "compare"
        / "packs"
        / pack_id
        / f"{base_sha}__{candidate_sha}"
    )
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    suite_rows: list[dict[str, Any]] = []
    comparison_payloads: list[dict[str, Any]] = []
    flattened_rows: list[dict[str, Any]] = []
    suite_statuses: dict[str, str] = {}
    suite_artifacts: dict[str, dict[str, str]] = {}
    hash_sections: list[str] = []
    suite_state_by_name = _suite_state_by_identity(request_state_payload)

    for manifest_path, manifest in manifest_pairs:
        suite = str(manifest.get("suite") or "")
        profile = str(
            manifest.get("profile") or manifest.get("methodology_profile") or ""
        )
        if not profile:
            if expected_count_by_suite.get(suite, 0) != 1:
                raise ValueError(
                    f"{manifest_path}: suite manifest for '{suite}' must include profile"
                )
            profile = next(
                str(entry["profile"])
                for entry in expected_suite_entries
                if str(entry["suite"]) == suite
            )
        comparison_json_path = _resolve_path(
            manifest_path, str(manifest.get("comparison_json") or "")
        )
        hash_policy_path = _resolve_path(
            manifest_path, str(manifest.get("hash_policy_report") or "")
        )
        markdown_path = _resolve_path(
            manifest_path, str(manifest.get("markdown_report") or "")
        )
        if comparison_json_path is None or not comparison_json_path.is_file():
            raise ValueError(f"{manifest_path}: missing comparison_json artifact")
        comparison_payload = _load_json(comparison_json_path)
        comparison_payloads.append(comparison_payload)
        flattened_rows.extend(
            _flatten_rows(
                suite=suite,
                profile=profile,
                comparison_payload=comparison_payload,
            )
        )
        summary = comparison_payload.get("summary") or {}
        suite_label = _suite_profile_label(suite=suite, profile=profile)
        suite_rows.append(
            {
                "suite": suite,
                "profile": profile,
                "status": "completed",
                "faster": int(summary.get("faster") or 0),
                "slower": int(summary.get("slower") or 0),
                "no_change": int(summary.get("no_change") or 0),
                "incomparable": int(summary.get("incomparable") or 0),
            }
        )
        suite_statuses[suite_label] = "completed"
        suite_artifacts[suite_label] = {
            "manifest": str(manifest_path.resolve()),
            "comparison_json": str(comparison_json_path),
            "hash_policy_report": str(hash_policy_path) if hash_policy_path else "",
            "markdown_report": str(markdown_path) if markdown_path else "",
            "profile": profile,
        }
        if hash_policy_path is not None and hash_policy_path.is_file():
            hash_sections.append(
                f"[{suite_label}]\n{hash_policy_path.read_text(encoding='utf-8').strip()}"
            )

    for expected_suite, expected_profile in expected_suite_order:
        suite_label = _suite_profile_label(
            suite=expected_suite, profile=expected_profile
        )
        if suite_label in suite_statuses:
            continue
        suite_state = suite_state_by_name.get((expected_suite, expected_profile))
        if suite_state is None:
            continue
        status = str(suite_state.get("status") or "blocked")
        suite_rows.append(
            {
                "suite": expected_suite,
                "profile": expected_profile,
                "status": status,
                "faster": 0,
                "slower": 0,
                "no_change": 0,
                "incomparable": 0,
            }
        )
        suite_statuses[suite_label] = status
        suite_artifacts[suite_label] = {
            "manifest": "",
            "comparison_json": "",
            "hash_policy_report": "",
            "markdown_report": "",
            "artifact_name": str(suite_state.get("artifact_name") or ""),
            "profile": expected_profile,
        }

    combined_summary = _sum_summary(comparison_payloads)
    fail_on = [str(item) for item in pack.get("overall_fail_on") or []]
    overall_status = _overall_status_with_suite_states(
        flattened_rows=flattened_rows,
        suite_statuses=suite_statuses,
        fail_on=fail_on,
    )
    summary_markdown = _render_summary_markdown(
        pack_id=pack_id,
        compare_mode=str(pack.get("compare_mode") or ""),
        overall_status=overall_status,
        suite_rows=suite_rows,
        summary=combined_summary,
    )
    summary_path = resolved_output_dir / "summary.md"
    summary_path.write_text(summary_markdown, encoding="utf-8")

    comparison_payload = {
        "schema_version": 1,
        "metadata": {
            "pack_id": pack_id,
            "pack_alias": alias,
            "pack_version": int(pack.get("pack_version") or 0),
            "compare_mode": str(pack.get("compare_mode") or ""),
            "overall_fail_on": fail_on,
            "base_sha": base_sha,
            "candidate_sha": candidate_sha,
        },
        "summary": combined_summary,
        "rows": flattened_rows,
    }
    comparison_path = resolved_output_dir / "comparison.json"
    comparison_path.write_text(
        json.dumps(comparison_payload, indent=2) + "\n",
        encoding="utf-8",
    )

    hash_policy_path = resolved_output_dir / "hash-policy.txt"
    hash_policy_path.write_text(
        ("\n\n".join(hash_sections) + "\n") if hash_sections else "",
        encoding="utf-8",
    )

    manifest_payload = {
        "pack_id": pack_id,
        "pack_alias": alias,
        "pack_version": int(pack.get("pack_version") or 0),
        "base_sha": base_sha,
        "candidate_sha": candidate_sha,
        "compare_mode": str(pack.get("compare_mode") or ""),
        "overall_fail_on": fail_on,
        "overall_status": overall_status,
        "suite_manifests": [str(path.resolve()) for path in suite_manifest_paths],
        "suite_statuses": suite_statuses,
        "suite_artifacts": suite_artifacts,
        "summary_md": str(summary_path.resolve()),
        "comparison_json": str(comparison_path.resolve()),
        "hash_policy_report": str(hash_policy_path.resolve()),
    }
    manifest_path = resolved_output_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest_payload, indent=2) + "\n",
        encoding="utf-8",
    )
    return manifest_payload


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Pack planning and aggregation helpers")
    subparsers = parser.add_subparsers(dest="command", required=True)

    plan_parser = subparsers.add_parser("plan", help="Resolve a pack into suite shards")
    plan_parser.add_argument("--registry", type=Path, default=DEFAULT_REGISTRY_PATH)
    plan_parser.add_argument("--pack", required=True)
    plan_parser.add_argument("--base-sha", required=True)
    plan_parser.add_argument("--candidate-sha", required=True)
    plan_parser.add_argument(
        "--format",
        choices=["json", "github-matrix"],
        default="json",
    )
    plan_parser.add_argument("--output", type=Path)

    summarize_parser = subparsers.add_parser(
        "summarize", help="Aggregate suite artifacts into a pack artifact bundle"
    )
    summarize_parser.add_argument(
        "--registry", type=Path, default=DEFAULT_REGISTRY_PATH
    )
    summarize_parser.add_argument("--pack")
    summarize_parser.add_argument("--pack-manifest", type=Path)
    summarize_parser.add_argument("--request-state", type=Path)
    summarize_parser.add_argument("--output-dir", type=Path)
    summarize_parser.add_argument(
        "--suite-manifest",
        dest="suite_manifests",
        type=Path,
        action="append",
    )
    return parser


def main() -> None:
    parser = _build_parser()
    args = parser.parse_args()

    try:
        if args.command == "plan":
            payload = plan_pack(
                registry_path=args.registry,
                pack_ref=args.pack,
                base_sha=args.base_sha,
                candidate_sha=args.candidate_sha,
            )
            rendered_payload = _render_plan_payload(payload, args.format)
            output = json.dumps(rendered_payload, indent=2) + "\n"
            if args.output is not None:
                args.output.write_text(output, encoding="utf-8")
            print(output, end="")
            return

        if not args.pack and args.pack_manifest is None:
            parser.error("summarize requires --pack or --pack-manifest")
        manifest = summarize_pack(
            registry_path=args.registry,
            pack_ref=args.pack or "",
            suite_manifest_paths=args.suite_manifests or [],
            output_dir=args.output_dir,
            pack_manifest_path=args.pack_manifest,
            request_state_path=args.request_state,
        )
        print(json.dumps(manifest, indent=2))
    except (OSError, ValueError, RuntimeError) as exc:
        print(str(exc), file=sys.stderr)
        raise SystemExit(1) from exc


if __name__ == "__main__":
    main()
