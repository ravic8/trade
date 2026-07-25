#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import time
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_EXPECTED_RULES = {
    "LensFilingWorkflowFailure",
    "LensFilingBlockingValidationDefect",
    "LensFilingTelemetryExporterDown",
}
_PROBE_ALERT_NAME = "LensM1AlertDeliveryProbe"
_PROBE_RECEIVER = "filing-alert-audit"


class DrillFailure(RuntimeError):
    pass


def _timestamp(value: datetime | None = None) -> str:
    current = value or datetime.now(timezone.utc)  # noqa: UP017
    return current.isoformat().replace("+00:00", "Z")


def _log(message: str) -> None:
    print(f"[trade-alert-delivery] {message}", flush=True)


def _command(arguments: list[str], *, label: str) -> str:
    completed = subprocess.run(
        arguments,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        if completed.stderr.strip():
            print(completed.stderr.strip(), file=sys.stderr)
        raise DrillFailure(f"{label} failed")
    return completed.stdout.strip()


def _json_command(arguments: list[str], *, label: str) -> Any:
    output = _command(arguments, label=label)
    try:
        return json.loads(output)
    except json.JSONDecodeError as exc:
        raise DrillFailure(f"{label} returned invalid JSON") from exc


def _wait_for(
    operation: Callable[[], dict[str, Any]],
    predicate: Callable[[dict[str, Any]], bool],
    *,
    timeout_seconds: int,
    description: str,
) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        latest = operation()
        if predicate(latest):
            return latest
        time.sleep(1)
    raise DrillFailure(f"timed out waiting for {description}")


def _write_report(path: Path, report: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def _service_request(
    compose: list[str],
    *,
    method: str,
    url: str,
    payload: Any | None = None,
    expect_json: bool = True,
) -> Any:
    command = [
        *compose,
        "exec",
        "-T",
        "api",
        "curl",
        "--fail-with-body",
        "-sS",
        "-X",
        method,
    ]
    if payload is not None:
        command.extend(
            [
                "-H",
                "Content-Type: application/json",
                "--data",
                json.dumps(payload, separators=(",", ":")),
            ]
        )
    if not expect_json:
        command.extend(["-o", "/dev/null"])
    command.append(url)
    if expect_json:
        return _json_command(command, label=f"{method} {url}")
    _command(command, label=f"{method} {url}")
    return None


def _unauthenticated_webhook_status(compose: list[str]) -> int:
    output = _command(
        [
            *compose,
            "exec",
            "-T",
            "api",
            "curl",
            "-sS",
            "-o",
            "/dev/null",
            "-w",
            "%{http_code}",
            "-X",
            "POST",
            "-H",
            "Content-Type: application/json",
            "--data",
            "{}",
            "http://localhost:8000/api/filings/alerts/webhook",
        ],
        label="unauthenticated alert webhook probe",
    )
    try:
        return int(output)
    except ValueError as exc:
        raise DrillFailure(
            "unauthenticated webhook probe returned an invalid status"
        ) from exc


def _alert_status_command(
    compose: list[str],
    *,
    workspace_id: str,
    drill_id: str,
) -> list[str]:
    return [
        *compose,
        "exec",
        "-T",
        "api",
        "python",
        "-m",
        "trade_research.filings.alerts",
        "status",
        "--workspace-id",
        workspace_id,
        "--drill-id",
        drill_id,
    ]


def _prometheus_alert_names(payload: dict[str, Any]) -> set[str]:
    if payload.get("status") != "success":
        raise DrillFailure("Prometheus rules API did not report success")
    groups = payload.get("data", {}).get("groups", [])
    return {
        str(rule.get("name"))
        for group in groups
        for rule in group.get("rules", [])
        if rule.get("type") == "alerting" and rule.get("name")
    }


def _probe_alert(
    *,
    drill_id: str,
    workspace_id: str,
    starts_at: str,
    ends_at: str,
) -> list[dict[str, Any]]:
    return [
        {
            "labels": {
                "alertname": _PROBE_ALERT_NAME,
                "severity": "critical",
                "service": "filing-intelligence",
                "workspace_id": workspace_id,
                "drill_id": drill_id,
            },
            "annotations": {
                "summary": "M1 filing alert delivery acceptance probe",
                "description": (
                    "Synthetic bounded alert used to verify authenticated "
                    "Alertmanager delivery and durable receipt."
                ),
            },
            "startsAt": starts_at,
            "endsAt": ends_at,
            "generatorURL": "http://prometheus:9090/alerts",
        }
    ]


def run_drill(arguments: argparse.Namespace) -> tuple[dict[str, Any], int]:
    app_dir = Path(arguments.app_dir).expanduser().resolve()
    env_file = Path(arguments.env_file).expanduser().resolve()
    compose_file = app_dir / "docker-compose.prod.yml"
    if not env_file.is_file():
        raise DrillFailure(f"environment file is missing: {env_file}")
    if not compose_file.is_file():
        raise DrillFailure(f"production Compose file is missing: {compose_file}")
    if not _IDENTIFIER_PATTERN.fullmatch(arguments.workspace_id):
        raise DrillFailure("invalid workspace identifier")
    if not 10 <= arguments.timeout_seconds <= 300:
        raise DrillFailure("timeout must contain between 10 and 300 seconds")

    drill_id = (
        f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-"  # noqa: UP017
        f"{uuid4().hex[:12]}"
    )
    report_path = Path(arguments.report_dir).expanduser().resolve() / f"{drill_id}.json"
    compose = [
        "docker",
        "compose",
        "--env-file",
        str(env_file),
        "-f",
        str(compose_file),
    ]
    report: dict[str, Any] = {
        "schema_version": 1,
        "drill_id": drill_id,
        "status": "failed",
        "stage": "initializing",
        "exit_code": 1,
        "error": None,
        "started_at": _timestamp(),
        "finished_at": None,
        "workspace_id": arguments.workspace_id,
        "readiness_passed": False,
        "topology": {
            "alertmanager_ready": False,
            "prometheus_rules_verified": False,
            "loaded_rule_names": [],
            "unauthenticated_webhook_rejected": False,
        },
        "delivery": {
            "alertname": _PROBE_ALERT_NAME,
            "severity": "critical",
            "receiver": _PROBE_RECEIVER,
            "firing_count": 0,
            "firing_received_at": None,
            "durable_receipt_verified": False,
        },
        "resolution": {
            "resolved_count": 0,
            "resolved_received_at": None,
            "durable_receipt_verified": False,
        },
    }
    exit_code = 1
    try:
        report["stage"] = "readiness"
        _log("verifying filing production readiness")
        readiness = _json_command(
            [
                *compose,
                "exec",
                "-T",
                "api",
                "trade-research",
                "verify-filing-production",
            ],
            label="filing production readiness",
        )
        if readiness.get("passed") is not True:
            raise DrillFailure("filing production readiness did not pass")
        report["readiness_passed"] = True

        report["stage"] = "topology_verification"
        _log("verifying Alertmanager, Prometheus rules, and webhook authentication")
        ready = _command(
            [
                *compose,
                "exec",
                "-T",
                "api",
                "curl",
                "--fail-with-body",
                "-sS",
                "http://alertmanager:9093/-/ready",
            ],
            label="Alertmanager readiness",
        )
        report["topology"]["alertmanager_ready"] = ready.strip().upper() == "OK"
        if report["topology"]["alertmanager_ready"] is not True:
            raise DrillFailure("Alertmanager readiness response was unexpected")
        rules_payload = _service_request(
            compose,
            method="GET",
            url="http://prometheus:9090/api/v1/rules?type=alert",
        )
        loaded_rules = _prometheus_alert_names(rules_payload)
        report["topology"]["loaded_rule_names"] = sorted(loaded_rules)
        missing_rules = _EXPECTED_RULES - loaded_rules
        if missing_rules:
            raise DrillFailure(
                "Prometheus is missing required filing alert rules: "
                + ", ".join(sorted(missing_rules))
            )
        report["topology"]["prometheus_rules_verified"] = True
        unauthorized_status = _unauthenticated_webhook_status(compose)
        report["topology"]["unauthenticated_webhook_rejected"] = (
            unauthorized_status == 401
        )
        if report["topology"]["unauthenticated_webhook_rejected"] is not True:
            raise DrillFailure(
                "alert webhook did not reject an unauthenticated request"
            )

        report["stage"] = "firing_delivery"
        _log("injecting a bounded critical alert through Alertmanager")
        started_at = datetime.now(timezone.utc)  # noqa: UP017
        _service_request(
            compose,
            method="POST",
            url="http://alertmanager:9093/api/v2/alerts",
            payload=_probe_alert(
                drill_id=drill_id,
                workspace_id=arguments.workspace_id,
                starts_at=_timestamp(started_at),
                ends_at=_timestamp(started_at + timedelta(minutes=10)),
            ),
            expect_json=False,
        )

        def delivery_status() -> dict[str, Any]:
            return _json_command(
                _alert_status_command(
                    compose,
                    workspace_id=arguments.workspace_id,
                    drill_id=drill_id,
                ),
                label="alert delivery receipt status",
            )

        firing = _wait_for(
            delivery_status,
            lambda payload: int(payload.get("firing_count", 0)) >= 1,
            timeout_seconds=arguments.timeout_seconds,
            description="durable firing-alert receipt",
        )
        report["delivery"].update(
            {
                "firing_count": int(firing.get("firing_count", 0)),
                "firing_received_at": firing.get("firing_received_at"),
                "durable_receipt_verified": (
                    firing.get("actor_ids") == ["alertmanager"]
                    and firing.get("alertnames") == [_PROBE_ALERT_NAME]
                    and firing.get("severities") == ["critical"]
                    and firing.get("receivers") == [_PROBE_RECEIVER]
                    and firing.get("firing_received_at") is not None
                ),
            }
        )
        if report["delivery"]["durable_receipt_verified"] is not True:
            raise DrillFailure("firing alert receipt did not match the probe")

        report["stage"] = "resolved_delivery"
        _log("resolving the probe and verifying resolved delivery")
        resolved_at = datetime.now(timezone.utc)  # noqa: UP017
        _service_request(
            compose,
            method="POST",
            url="http://alertmanager:9093/api/v2/alerts",
            payload=_probe_alert(
                drill_id=drill_id,
                workspace_id=arguments.workspace_id,
                starts_at=_timestamp(started_at),
                ends_at=_timestamp(resolved_at),
            ),
            expect_json=False,
        )
        resolved = _wait_for(
            delivery_status,
            lambda payload: int(payload.get("resolved_count", 0)) >= 1,
            timeout_seconds=arguments.timeout_seconds,
            description="durable resolved-alert receipt",
        )
        report["resolution"].update(
            {
                "resolved_count": int(resolved.get("resolved_count", 0)),
                "resolved_received_at": resolved.get("resolved_received_at"),
                "durable_receipt_verified": (
                    resolved.get("actor_ids") == ["alertmanager"]
                    and resolved.get("alertnames") == [_PROBE_ALERT_NAME]
                    and resolved.get("receivers") == [_PROBE_RECEIVER]
                    and resolved.get("resolved_received_at") is not None
                ),
            }
        )
        if report["resolution"]["durable_receipt_verified"] is not True:
            raise DrillFailure("resolved alert receipt did not match the probe")

        report["status"] = "passed"
        report["stage"] = "completed"
        report["exit_code"] = 0
        exit_code = 0
        _log("alert delivery and resolution drill passed")
    except (DrillFailure, KeyError, TypeError, ValueError) as exc:
        report["error"] = {
            "type": type(exc).__name__,
            "message": str(exc)[:500],
        }
        print(f"[trade-alert-delivery] {exc}", file=sys.stderr)
    except KeyboardInterrupt:
        report["stage"] = "interrupted"
        report["error"] = {
            "type": "KeyboardInterrupt",
            "message": "drill interrupted by operator",
        }
        print("[trade-alert-delivery] drill interrupted", file=sys.stderr)
    finally:
        report["finished_at"] = _timestamp()
        _write_report(report_path, report)
        _log(f"alert delivery report: {report_path}")
    return report, exit_code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Verify private Prometheus rules and authenticated, durable "
            "Alertmanager firing and resolution delivery."
        )
    )
    parser.add_argument("--workspace-id", default="default")
    parser.add_argument("--timeout-seconds", type=int, default=90)
    parser.add_argument(
        "--app-dir",
        default=os.environ.get("TRADE_APP_DIR", "/opt/trade/app"),
    )
    parser.add_argument(
        "--env-file",
        default=os.environ.get("TRADE_ENV_FILE", "/opt/trade/.env"),
    )
    parser.add_argument(
        "--report-dir",
        default=os.environ.get(
            "PROD_ALERT_REPORT_DIR",
            "/opt/trade/alert-reports",
        ),
    )
    return parser


def main() -> None:
    try:
        _report, exit_code = run_drill(_parser().parse_args())
    except DrillFailure as exc:
        print(f"[trade-alert-delivery] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
