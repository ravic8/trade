#!/usr/bin/env python3
"""Check that canonical Phase 0 documentation matches repository structure."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MANIFEST_PATH = ROOT / "docs/current_state_manifest.json"
ALLOWED_STATUSES = {
    "current",
    "historical",
    "partially_implemented",
    "proposed",
    "superseded",
}
REQUIRED_METADATA = {
    "document_status",
    "last_verified_commit",
    "last_verified_date",
    "owner",
    "replaced_by",
}


def _frontmatter(path: Path) -> dict[str, str]:
    lines = path.read_text(encoding="utf-8").splitlines()
    if not lines or lines[0] != "---":
        raise ValueError("missing YAML frontmatter")
    try:
        end = lines.index("---", 1)
    except ValueError as exc:
        raise ValueError("unterminated YAML frontmatter") from exc

    values: dict[str, str] = {}
    for line in lines[1:end]:
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        key, separator, value = line.partition(":")
        if not separator:
            raise ValueError(f"invalid frontmatter line: {line!r}")
        values[key.strip()] = value.strip().strip("\"'")
    return values


def _compose_services(path: Path) -> list[str]:
    services: list[str] = []
    in_services = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line == "services:":
            in_services = True
            continue
        if not in_services:
            continue
        if line and not line.startswith(" "):
            break
        match = re.match(r"^  ([A-Za-z0-9_-]+):\s*$", line)
        if match:
            services.append(match.group(1))
    return services


def _dagster_schedule_statuses(path: Path) -> dict[str, str]:
    text = path.read_text(encoding="utf-8")
    blocks = re.findall(
        r"\w+\s*=\s*ScheduleDefinition\(\n(.*?)\n\)",
        text,
        flags=re.DOTALL,
    )
    schedules: dict[str, str] = {}
    for block in blocks:
        name_match = re.search(r'name="([^"]+)"', block)
        status_match = re.search(
            r"default_status=DefaultScheduleStatus\.([A-Z_]+)",
            block,
        )
        if name_match and status_match:
            schedules[name_match.group(1)] = status_match.group(1)
    return schedules


def check() -> list[str]:
    errors: list[str] = []
    manifest = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    baseline = manifest["baseline_commit"]
    verified_date = manifest["verified_date"]

    for relative_path, expected_status in manifest["documents"].items():
        path = ROOT / relative_path
        if not path.is_file():
            errors.append(f"{relative_path}: missing")
            continue
        try:
            metadata = _frontmatter(path)
        except ValueError as exc:
            errors.append(f"{relative_path}: {exc}")
            continue

        missing = REQUIRED_METADATA - metadata.keys()
        if missing:
            errors.append(
                f"{relative_path}: missing metadata {sorted(missing)}"
            )
        status = metadata.get("document_status")
        if status not in ALLOWED_STATUSES:
            errors.append(f"{relative_path}: invalid status {status!r}")
        if status != expected_status:
            errors.append(
                f"{relative_path}: expected status {expected_status!r}, "
                f"found {status!r}"
            )
        if metadata.get("last_verified_commit") != baseline:
            errors.append(f"{relative_path}: baseline commit drift")
        if metadata.get("last_verified_date") != verified_date:
            errors.append(f"{relative_path}: verification date drift")

        replacement = metadata.get("replaced_by")
        if replacement and replacement != "null" and not (ROOT / replacement).is_file():
            errors.append(
                f"{relative_path}: replacement does not exist: {replacement}"
            )

    compose_expectations = (
        ("docker-compose.yml", manifest["local_compose_services"]),
        ("docker-compose.prod.yml", manifest["production_compose_services"]),
    )
    for relative_path, expected in compose_expectations:
        actual = _compose_services(ROOT / relative_path)
        if actual != expected:
            errors.append(
                f"{relative_path}: service drift; "
                f"expected {expected}, found {actual}"
            )

    schedules = _dagster_schedule_statuses(
        ROOT / "src/trade_research/dagster/definitions.py"
    )
    expected_schedules = set(manifest["dagster_schedules"])
    actual_schedules = set(schedules)
    if actual_schedules != expected_schedules:
        errors.append(
            "Dagster schedule drift; "
            f"missing={sorted(expected_schedules - actual_schedules)}, "
            f"unexpected={sorted(actual_schedules - expected_schedules)}"
        )
    non_stopped = sorted(
        name for name, status in schedules.items() if status != "STOPPED"
    )
    if non_stopped:
        errors.append(
            "Dagster code defaults must remain stopped: "
            + ", ".join(non_stopped)
        )

    for relative_path, claims in manifest["forbidden_current_claims"].items():
        text = (ROOT / relative_path).read_text(encoding="utf-8")
        for claim in claims:
            if claim in text:
                errors.append(
                    f"{relative_path}: obsolete current claim remains: {claim!r}"
                )

    current_state = (ROOT / "docs/current_state.md").read_text(encoding="utf-8")
    for relative_path, status in manifest["documents"].items():
        if status == "current" and relative_path.startswith("docs/phase0_"):
            if relative_path not in current_state:
                errors.append(
                    f"docs/current_state.md: missing companion {relative_path}"
                )

    return errors


def main() -> int:
    errors = check()
    if errors:
        print("Current-state documentation check failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print("Current-state documentation check passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
