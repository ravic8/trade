from __future__ import annotations

import argparse
import importlib.util
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from fastapi.testclient import TestClient

from trade_research.api.app import app
from trade_research.config import Settings, get_settings
from trade_research.filings.alerts import (
    AlertmanagerWebhook,
    AlertWebhookAuthenticationError,
    alert_delivery_status,
    record_alertmanager_delivery,
    verify_alert_webhook_token,
)
from trade_research.filings.api import filing_runtime_dependency

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
DRILL_PATH = REPOSITORY_ROOT / "deploy/filing-alert-delivery-drill.py"


class _AlertStore:
    def __init__(self) -> None:
        self.recorded: list[dict[str, Any]] = []
        self.events: list[dict[str, Any]] = []

    def record_audit_event(self, **values: Any) -> str:
        self.recorded.append(values)
        return f"event-{len(self.recorded)}"

    def audit_events(self, **_: Any) -> list[dict[str, Any]]:
        return self.events


def _runtime(store: _AlertStore | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        settings=SimpleNamespace(filing_default_workspace_id="default"),
        store=store or _AlertStore(),
    )


def _webhook(status: str = "firing") -> AlertmanagerWebhook:
    return AlertmanagerWebhook.model_validate(
        {
            "version": "4",
            "status": status,
            "receiver": "filing-alert-audit",
            "groupKey": "{}:{alertname=\"LensM1AlertDeliveryProbe\"}",
            "truncatedAlerts": 0,
            "alerts": [
                {
                    "status": status,
                    "labels": {
                        "alertname": "LensM1AlertDeliveryProbe",
                        "severity": "critical",
                        "service": "filing-intelligence",
                        "workspace_id": "alpha",
                        "drill_id": "drill-1",
                    },
                    "annotations": {
                        "summary": "probe",
                        "description": "x" * 1_100,
                    },
                    "startsAt": "2026-07-26T00:00:00Z",
                    "endsAt": "2026-07-26T00:10:00Z",
                    "generatorURL": "http://prometheus:9090/alerts",
                    "fingerprint": "abc123",
                }
            ],
        }
    )


def _load_drill() -> Any:
    specification = importlib.util.spec_from_file_location(
        "filing_alert_delivery_drill",
        DRILL_PATH,
    )
    assert specification and specification.loader
    module = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(module)
    return module


def test_alert_webhook_token_is_file_backed_and_compared_securely(
    tmp_path: Path,
) -> None:
    token_file = tmp_path / "alert-token"
    token_file.write_text("a" * 64, encoding="utf-8")
    settings = Settings(filing_alert_webhook_token_file=token_file)

    verify_alert_webhook_token(settings, authorization=f"Bearer {'a' * 64}")

    try:
        verify_alert_webhook_token(settings, authorization=f"Bearer {'b' * 64}")
    except AlertWebhookAuthenticationError:
        pass
    else:
        raise AssertionError("incorrect alert webhook credentials must be rejected")


def test_alert_delivery_is_persisted_as_bounded_audit_evidence() -> None:
    store = _AlertStore()

    result = record_alertmanager_delivery(
        _runtime(store),
        webhook=_webhook(),
    )

    assert result == {
        "accepted": True,
        "recorded": 1,
        "event_ids": ["event-1"],
        "target_ids": ["drill-1"],
    }
    event = store.recorded[0]
    assert event["workspace_id"] == "alpha"
    assert event["actor_id"] == "alertmanager"
    assert event["action"] == "alert.delivery.firing"
    assert event["target_id"] == "drill-1"
    assert event["after_payload"]["alertname"] == "LensM1AlertDeliveryProbe"
    assert len(event["after_payload"]["annotations"]["description"]) == 1_000
    assert "authorization" not in json.dumps(event)


def test_alert_webhook_rejects_missing_auth_and_accepts_configured_token(
    tmp_path: Path,
) -> None:
    token_file = tmp_path / "alert-token"
    token_file.write_text("c" * 64, encoding="utf-8")
    settings = Settings(filing_alert_webhook_token_file=token_file)
    store = _AlertStore()
    runtime = _runtime(store)
    app.dependency_overrides[get_settings] = lambda: settings
    app.dependency_overrides[filing_runtime_dependency] = lambda: runtime
    client = TestClient(app)
    payload = _webhook().model_dump(by_alias=True, mode="json")
    try:
        unauthorized = client.post("/api/filings/alerts/webhook", json=payload)
        accepted = client.post(
            "/api/filings/alerts/webhook",
            json=payload,
            headers={"Authorization": f"Bearer {'c' * 64}"},
        )
    finally:
        app.dependency_overrides.clear()

    assert unauthorized.status_code == 401
    assert accepted.status_code == 202
    assert accepted.json()["recorded"] == 1


def test_alert_acceptance_status_is_workspace_and_drill_scoped() -> None:
    store = _AlertStore()
    store.events = [
        {
            "actor_id": "alertmanager",
            "action": "alert.delivery.resolved",
            "after_payload": {
                "alertname": "LensM1AlertDeliveryProbe",
                "severity": "critical",
                "receiver": "filing-alert-audit",
            },
            "created_at": datetime(2026, 7, 26, 0, 1, tzinfo=UTC),
        },
        {
            "actor_id": "alertmanager",
            "action": "alert.delivery.firing",
            "after_payload": {
                "alertname": "LensM1AlertDeliveryProbe",
                "severity": "critical",
                "receiver": "filing-alert-audit",
            },
            "created_at": datetime(2026, 7, 26, 0, 0, tzinfo=UTC),
        },
    ]

    result = alert_delivery_status(
        _runtime(store),
        workspace_id="alpha",
        drill_id="drill-1",
    )

    assert result["event_count"] == 2
    assert result["firing_count"] == 1
    assert result["resolved_count"] == 1
    assert result["actor_ids"] == ["alertmanager"]
    assert result["receivers"] == ["filing-alert-audit"]


def test_alert_delivery_drill_emits_a_passing_secret_free_report(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    drill = _load_drill()
    env_file = tmp_path / ".env"
    env_file.write_text("SAFE=true\n", encoding="utf-8")
    (tmp_path / "docker-compose.prod.yml").write_text(
        "services: {}\n",
        encoding="utf-8",
    )
    report_dir = tmp_path / "reports"
    status_calls = 0

    def fake_json_command(arguments: list[str], *, label: str) -> dict[str, Any]:
        nonlocal status_calls
        joined = " ".join(arguments)
        if "verify-filing-production" in joined:
            return {"passed": True}
        if "trade_research.filings.alerts status" in joined:
            status_calls += 1
            return {
                "firing_count": 1,
                "resolved_count": 1 if status_calls > 1 else 0,
                "actor_ids": ["alertmanager"],
                "alertnames": ["LensM1AlertDeliveryProbe"],
                "severities": ["critical"],
                "receivers": ["filing-alert-audit"],
                "firing_received_at": "2026-07-26T00:00:01Z",
                "resolved_received_at": (
                    "2026-07-26T00:00:02Z" if status_calls > 1 else None
                ),
            }
        raise AssertionError(f"unexpected JSON command: {label}")

    def fake_command(arguments: list[str], *, label: str) -> str:
        if "%{http_code}" in arguments:
            return "401"
        if "http://alertmanager:9093/-/ready" in arguments:
            return "OK"
        raise AssertionError(f"unexpected command: {label}")

    def fake_service_request(
        _compose: list[str],
        *,
        method: str,
        url: str,
        payload: Any | None = None,
        expect_json: bool = True,
    ) -> Any:
        if method == "GET":
            return {
                "status": "success",
                "data": {
                    "groups": [
                        {
                            "rules": [
                                {"type": "alerting", "name": name}
                                for name in sorted(drill._EXPECTED_RULES)
                            ]
                        }
                    ]
                },
            }
        assert method == "POST"
        assert url == "http://alertmanager:9093/api/v2/alerts"
        assert payload[0]["labels"]["drill_id"]
        assert expect_json is False
        return None

    monkeypatch.setattr(drill, "_json_command", fake_json_command)
    monkeypatch.setattr(drill, "_command", fake_command)
    monkeypatch.setattr(drill, "_service_request", fake_service_request)
    arguments = argparse.Namespace(
        workspace_id="default",
        timeout_seconds=30,
        app_dir=str(tmp_path),
        env_file=str(env_file),
        report_dir=str(report_dir),
    )

    report, exit_code = drill.run_drill(arguments)

    assert exit_code == 0
    assert report["status"] == "passed"
    assert report["topology"]["unauthenticated_webhook_rejected"] is True
    assert report["delivery"]["durable_receipt_verified"] is True
    assert report["resolution"]["durable_receipt_verified"] is True
    report_files = list(report_dir.glob("*.json"))
    assert len(report_files) == 1
    serialized = report_files[0].read_text(encoding="utf-8")
    assert "token" not in serialized.lower()


def test_alert_delivery_drill_is_executable() -> None:
    assert os.access(DRILL_PATH, os.X_OK)
