from __future__ import annotations

import argparse
import hmac
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from trade_research.config import Settings
from trade_research.filings.runtime import FilingRuntime, get_filing_runtime
from trade_research.filings.telemetry import current_trace_id

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_ALERT_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,127}$")


class AlertWebhookConfigurationError(RuntimeError):
    pass


class AlertWebhookAuthenticationError(RuntimeError):
    pass


class AlertmanagerAlert(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    status: Literal["firing", "resolved"]
    labels: dict[str, str] = Field(min_length=1, max_length=64)
    annotations: dict[str, str] = Field(default_factory=dict, max_length=64)
    starts_at: datetime | None = Field(default=None, alias="startsAt")
    ends_at: datetime | None = Field(default=None, alias="endsAt")
    generator_url: str = Field(default="", alias="generatorURL", max_length=2_000)
    fingerprint: str = Field(default="", max_length=256)


class AlertmanagerWebhook(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    version: str = Field(max_length=16)
    status: Literal["firing", "resolved"]
    receiver: str = Field(min_length=1, max_length=256)
    group_key: str = Field(default="", alias="groupKey", max_length=2_000)
    truncated_alerts: int = Field(default=0, alias="truncatedAlerts", ge=0)
    alerts: list[AlertmanagerAlert] = Field(min_length=1, max_length=100)


def verify_alert_webhook_token(
    settings: Settings,
    *,
    authorization: str | None,
) -> None:
    token_file = settings.filing_alert_webhook_token_file
    if token_file is None:
        raise AlertWebhookConfigurationError(
            "alert webhook token file is not configured"
        )
    try:
        expected = Path(token_file).read_text(encoding="utf-8").strip()
    except OSError as exc:
        raise AlertWebhookConfigurationError(
            "alert webhook token file is unavailable"
        ) from exc
    if len(expected) < 32:
        raise AlertWebhookConfigurationError(
            "alert webhook token must contain at least 32 characters"
        )
    scheme, separator, supplied = (authorization or "").partition(" ")
    if (
        not separator
        or scheme.lower() != "bearer"
        or not supplied
        or not hmac.compare_digest(supplied.strip(), expected)
    ):
        raise AlertWebhookAuthenticationError("invalid alert webhook credentials")


def record_alertmanager_delivery(
    runtime: FilingRuntime,
    *,
    webhook: AlertmanagerWebhook,
) -> dict[str, Any]:
    event_ids: list[str] = []
    target_ids: list[str] = []
    for alert in webhook.alerts:
        alert_name = alert.labels.get("alertname", "").strip()
        if not _ALERT_NAME_PATTERN.fullmatch(alert_name):
            raise ValueError("alertname is missing or invalid")
        workspace_id = alert.labels.get(
            "workspace_id",
            runtime.settings.filing_default_workspace_id,
        ).strip()
        if not _IDENTIFIER_PATTERN.fullmatch(workspace_id):
            raise ValueError("alert workspace identifier is invalid")
        target_id = (
            alert.labels.get("drill_id", "").strip()
            or alert.fingerprint.strip()
        )
        if not _IDENTIFIER_PATTERN.fullmatch(target_id):
            raise ValueError("alert delivery target identifier is invalid")
        delivery_status = alert.status
        event_id = runtime.store.record_audit_event(
            workspace_id=workspace_id,
            actor_id="alertmanager",
            action=f"alert.delivery.{delivery_status}",
            target_type="alert_delivery",
            target_id=target_id,
            after_payload={
                "delivery_status": delivery_status,
                "alertname": alert_name,
                "severity": alert.labels.get("severity"),
                "service": alert.labels.get("service"),
                "receiver": webhook.receiver,
                "fingerprint": alert.fingerprint or None,
                "starts_at": (
                    alert.starts_at.isoformat() if alert.starts_at else None
                ),
                "ends_at": alert.ends_at.isoformat() if alert.ends_at else None,
                "labels": _bounded_mapping(alert.labels),
                "annotations": _bounded_mapping(alert.annotations),
                "truncated_alerts": webhook.truncated_alerts,
            },
            reason="authenticated Alertmanager webhook delivery",
            trace_id=current_trace_id(),
        )
        event_ids.append(event_id)
        target_ids.append(target_id)
    return {
        "accepted": True,
        "recorded": len(event_ids),
        "event_ids": event_ids,
        "target_ids": target_ids,
    }


def alert_delivery_status(
    runtime: FilingRuntime,
    *,
    workspace_id: str,
    drill_id: str,
) -> dict[str, Any]:
    if not _IDENTIFIER_PATTERN.fullmatch(workspace_id):
        raise ValueError("invalid workspace identifier")
    if not _IDENTIFIER_PATTERN.fullmatch(drill_id):
        raise ValueError("invalid alert drill identifier")
    events = runtime.store.audit_events(
        workspace_id=workspace_id,
        target_type="alert_delivery",
        target_id=drill_id,
        limit=100,
    )
    firing = [
        event
        for event in events
        if event["action"] == "alert.delivery.firing"
    ]
    resolved = [
        event
        for event in events
        if event["action"] == "alert.delivery.resolved"
    ]
    payloads = [event.get("after_payload") or {} for event in events]
    return {
        "workspace_id": workspace_id,
        "drill_id": drill_id,
        "event_count": len(events),
        "firing_count": len(firing),
        "resolved_count": len(resolved),
        "actor_ids": sorted({str(event["actor_id"]) for event in events}),
        "actions": sorted({str(event["action"]) for event in events}),
        "alertnames": sorted(
            {
                str(payload.get("alertname"))
                for payload in payloads
                if payload.get("alertname")
            }
        ),
        "severities": sorted(
            {
                str(payload.get("severity"))
                for payload in payloads
                if payload.get("severity")
            }
        ),
        "receivers": sorted(
            {
                str(payload.get("receiver"))
                for payload in payloads
                if payload.get("receiver")
            }
        ),
        "firing_received_at": _latest_created_at(firing),
        "resolved_received_at": _latest_created_at(resolved),
    }


def _bounded_mapping(values: dict[str, str]) -> dict[str, str]:
    return {
        str(key)[:128]: str(value)[:1_000]
        for key, value in values.items()
    }


def _latest_created_at(events: list[dict[str, Any]]) -> str | None:
    if not events:
        return None
    value = events[0].get("created_at")
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _print_json(payload: Any) -> None:
    print(json.dumps(payload, separators=(",", ":"), default=str))


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Internal verification for filing alert delivery."
    )
    subparsers = parser.add_subparsers(dest="operation", required=True)
    status = subparsers.add_parser("status")
    status.add_argument("--workspace-id", default="default")
    status.add_argument("--drill-id", required=True)
    arguments = parser.parse_args()
    if arguments.operation == "status":
        _print_json(
            alert_delivery_status(
                get_filing_runtime(),
                workspace_id=arguments.workspace_id,
                drill_id=arguments.drill_id,
            )
        )


if __name__ == "__main__":
    main()
