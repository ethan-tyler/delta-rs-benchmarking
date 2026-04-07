from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    import yaml
except ImportError:  # pragma: no cover - PyYAML is expected in benchmark environments
    yaml = None  # type: ignore[assignment]


def repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


DEFAULT_REGISTRY_PATH = repo_root() / "bench" / "evidence" / "registry.yaml"
DEFAULT_METHODOLOGY_DIR = repo_root() / "bench" / "methodologies"


def load_methodology_profile_env(
    profile: str, methodology_dir: Path | None = None
) -> dict[str, str]:
    profile_path = (methodology_dir or DEFAULT_METHODOLOGY_DIR) / f"{profile}.env"
    if not profile_path.exists():
        return {}

    values: dict[str, str] = {}
    for raw_line in profile_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key:
            continue
        values[key] = value.strip()
    return values


def load_registry(path: Path | None = None) -> dict[str, Any]:
    registry_path = path or DEFAULT_REGISTRY_PATH
    if yaml is None:  # pragma: no cover - covered only when dependency missing
        raise RuntimeError(
            "PyYAML is required to load bench/evidence/registry.yaml; install 'PyYAML' in the benchmark Python environment"
        )
    payload = yaml.safe_load(registry_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{registry_path}: registry must be a mapping")
    if int(payload.get("schema_version") or 0) != 1:
        raise ValueError(
            f"{registry_path}: schema_version must be 1 (found {payload.get('schema_version')!r})"
        )
    suites = payload.get("suites")
    surfaces = payload.get("surfaces")
    packs = payload.get("packs")
    if not isinstance(suites, dict) or not suites:
        raise ValueError(f"{registry_path}: suites must be a non-empty mapping")
    if surfaces is not None and not isinstance(surfaces, dict):
        raise ValueError(f"{registry_path}: surfaces must be a mapping when present")
    if not isinstance(packs, dict) or not packs:
        raise ValueError(f"{registry_path}: packs must be a non-empty mapping")
    return payload


def resolve_pack(
    registry: dict[str, Any], pack_ref: str
) -> tuple[str, dict[str, Any], str | None]:
    packs = registry["packs"]
    if pack_ref in packs:
        pack = packs[pack_ref]
        if not isinstance(pack, dict):
            raise ValueError(f"pack '{pack_ref}' must be a mapping")
        alias = pack.get("alias")
        return pack_ref, pack, str(alias) if alias is not None else None

    matches = []
    for pack_id, pack in packs.items():
        if isinstance(pack, dict) and str(pack.get("alias") or "") == pack_ref:
            matches.append((pack_id, pack))
    if not matches:
        raise ValueError(f"unknown pack '{pack_ref}'")
    if len(matches) > 1:
        ids = ", ".join(sorted(pack_id for pack_id, _ in matches))
        raise ValueError(f"pack alias '{pack_ref}' is ambiguous across: {ids}")
    pack_id, pack = matches[0]
    return pack_id, pack, str(pack.get("alias") or "")


def pack_suite_definitions(
    registry: dict[str, Any], pack: dict[str, Any]
) -> list[dict[str, Any]]:
    suites = registry["suites"]
    surfaces = registry.get("surfaces") or {}
    raw_suite_entries = pack.get("suites")
    if not isinstance(raw_suite_entries, list) or not raw_suite_entries:
        raise ValueError("pack suites must be a non-empty list")

    resolved: list[dict[str, Any]] = []
    for index, entry in enumerate(raw_suite_entries):
        if not isinstance(entry, dict):
            raise ValueError(f"pack suite at index {index} must be a mapping")
        surface_name = entry.get("surface")
        surface_registry: dict[str, Any] | None = None
        if surface_name not in {None, ""}:
            if not isinstance(surface_name, str):
                raise ValueError(f"pack suite at index {index} has invalid surface")
            surface_registry = surfaces.get(surface_name)
            if not isinstance(surface_registry, dict):
                raise ValueError(f"pack references unknown surface '{surface_name}'")
            suite_name = surface_registry.get("suite")
            if not isinstance(suite_name, str) or not suite_name:
                raise ValueError(f"surface '{surface_name}' is missing suite")
            explicit_suite = entry.get("suite")
            if explicit_suite not in {None, "", suite_name}:
                raise ValueError(
                    f"pack surface '{surface_name}' does not match suite '{explicit_suite}'"
                )
        else:
            suite_name = entry.get("suite")
        if not isinstance(suite_name, str) or not suite_name:
            raise ValueError(f"pack suite at index {index} is missing suite")
        suite_registry = suites.get(suite_name)
        if not isinstance(suite_registry, dict):
            raise ValueError(f"pack references unknown suite '{suite_name}'")
        profile = (
            entry.get("profile")
            or (surface_registry or {}).get("profile")
            or suite_registry.get("default_profile")
        )
        if not isinstance(profile, str) or not profile:
            raise ValueError(f"suite '{suite_name}' is missing a profile")
        profile_env = load_methodology_profile_env(profile)
        storage_backend = (
            entry.get("storage_backend")
            or (surface_registry or {}).get("storage_backend")
            or profile_env.get("STORAGE_BACKEND")
        )
        backend_profile = (
            entry.get("backend_profile")
            or (surface_registry or {}).get("backend_profile")
            or profile_env.get("BACKEND_PROFILE")
        )
        timeout_minutes = entry.get("timeout_minutes")
        if not isinstance(timeout_minutes, int):
            raise ValueError(
                f"suite '{suite_name}' is missing integer timeout_minutes"
            )
        resolved.append(
            {
                "suite": suite_name,
                "surface": (
                    str(surface_name) if surface_name not in {None, ""} else None
                ),
                "profile": profile,
                "timeout_minutes": timeout_minutes,
                "storage_backend": (
                    str(storage_backend) if storage_backend not in {None, ""} else None
                ),
                "backend_profile": (
                    str(backend_profile) if backend_profile not in {None, ""} else None
                ),
                "suite_registry": suite_registry,
                "surface_registry": surface_registry,
            }
        )
    return resolved


def readiness_blockers(
    registry: dict[str, Any], pack: dict[str, Any]
) -> list[dict[str, str]]:
    strict_mode = str(pack.get("strict_mode") or "")
    if strict_mode != "require_all_ready":
        return []
    blockers: list[dict[str, str]] = []
    for suite_entry in pack_suite_definitions(registry, pack):
        suite_name = suite_entry["suite"]
        suite_registry = suite_entry["suite_registry"]
        surface_name = suite_entry.get("surface")
        surface_registry = suite_entry.get("surface_registry") or {}
        readiness = str(surface_registry.get("readiness") or suite_registry.get("readiness") or "")
        if readiness == "ready":
            continue
        blockers.append(
            {
                "suite": str(surface_name or suite_name),
                "readiness": readiness or "unknown",
                "reason": str(
                    surface_registry.get("readiness_reason")
                    or suite_registry.get("readiness_reason")
                    or ""
                ),
            }
        )
    return blockers
