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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

_IDENTIFIER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_EMAIL_PATTERN = re.compile(r"^[^@\s]+@[^@\s]+$")
_HEADER_PATTERN = re.compile(r"^[A-Za-z0-9-]+$")
_TERMINAL_RUN_STATUSES = {"completed", "failed", "cancelled"}


class DrillFailure(RuntimeError):
    pass


def _timestamp() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")  # noqa: UP017


def _log(message: str) -> None:
    print(f"[trade-human-review] {message}", flush=True)


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


def _internal_command(
    compose: list[str],
    operation: str,
    *arguments: str,
) -> list[str]:
    return [
        *compose,
        "exec",
        "-T",
        "api",
        "python",
        "-m",
        "trade_research.filings.human_review_acceptance",
        operation,
        *arguments,
    ]


def _api_request(
    compose: list[str],
    *,
    method: str,
    path: str,
    workspace_id: str,
    actor_header: str | None = None,
    actor_email: str | None = None,
    payload: dict[str, Any] | None = None,
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
        "-H",
        f"X-Workspace-ID: {workspace_id}",
    ]
    if actor_header and actor_email:
        command.extend(["-H", f"{actor_header}: {actor_email}"])
    if payload is not None:
        command.extend(
            [
                "-H",
                "Content-Type: application/json",
                "--data",
                json.dumps(payload, separators=(",", ":")),
            ]
        )
    command.append(f"http://localhost:8000{path}")
    return _json_command(command, label=f"{method} {path}")


def run_drill(arguments: argparse.Namespace) -> tuple[dict[str, Any], int]:
    app_dir = Path(arguments.app_dir).expanduser().resolve()
    env_file = Path(arguments.env_file).expanduser().resolve()
    compose_file = app_dir / "docker-compose.prod.yml"
    reviewer_email = arguments.reviewer_email.strip().lower()
    actor_header = arguments.actor_header.strip()
    if not env_file.is_file():
        raise DrillFailure(f"environment file is missing: {env_file}")
    if not compose_file.is_file():
        raise DrillFailure(f"production Compose file is missing: {compose_file}")
    if not _IDENTIFIER_PATTERN.fullmatch(arguments.workspace_id):
        raise DrillFailure("invalid workspace identifier")
    if not _IDENTIFIER_PATTERN.fullmatch(arguments.filing_id):
        raise DrillFailure("invalid filing identifier")
    if not _EMAIL_PATTERN.fullmatch(reviewer_email):
        raise DrillFailure("reviewer identity must be a valid email address")
    if not _HEADER_PATTERN.fullmatch(actor_header):
        raise DrillFailure("actor header contains unsupported characters")
    if not 3 <= len(arguments.reason) <= 2_000:
        raise DrillFailure("review reason must contain between 3 and 2000 characters")

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
        "filing_id": arguments.filing_id,
        "readiness_passed": False,
        "identity": {
            "reviewer_email": reviewer_email,
            "actor_header": actor_header.lower(),
            "header_configured": False,
            "reviewer_allowlisted": False,
        },
        "interrupt": {
            "run_id": None,
            "review_id": None,
            "status": None,
            "current_node": None,
            "attempt_count": 0,
            "packet_candidate_count": 0,
            "packet_evidence_count": 0,
            "packet_defect_count": 0,
            "worker_lease_released": False,
        },
        "decision": {
            "status": None,
            "reviewer_id": None,
            "reason_recorded": False,
            "audit_event_count": 0,
            "audit_verified": False,
        },
        "resume": {
            "status": None,
            "current_node": None,
            "attempt_count": 0,
            "approved_fact_count": 0,
            "unique_approved_fact_ids": 0,
            "reviewer_approved_fact_count": 0,
            "validation_defect_count": 0,
            "worker_lease_released": False,
            "trace_id": None,
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

        report["stage"] = "identity_verification"
        _log("verifying the reviewer identity and authentication header")
        identity = _json_command(
            _internal_command(
                compose,
                "identity",
                "--reviewer-email",
                reviewer_email,
                "--actor-header",
                actor_header,
            ),
            label="reviewer identity verification",
        )
        if (
            identity.get("header_configured") is not True
            or identity.get("reviewer_allowlisted") is not True
        ):
            raise DrillFailure("reviewer identity verification did not pass")
        report["identity"].update(identity)

        report["stage"] = "review_run_submission"
        _log("submitting a forced-review filing run")
        submission = _api_request(
            compose,
            method="POST",
            path="/api/filings/runs",
            workspace_id=arguments.workspace_id,
            actor_header=actor_header,
            actor_email=reviewer_email,
            payload={
                "filing_id": arguments.filing_id,
                "idempotency_key": f"m1-human-review:{drill_id}",
                "force_review": True,
                "max_attempts": 3,
            },
        )
        if submission.get("accepted") is not True:
            raise DrillFailure("forced-review filing run was not accepted")
        run_id = str(submission["run"]["run_id"])
        report["interrupt"]["run_id"] = run_id

        report["stage"] = "review_interrupt"
        waiting = _wait_for(
            lambda: _api_request(
                compose,
                method="GET",
                path=f"/api/filings/runs/{run_id}",
                workspace_id=arguments.workspace_id,
            ),
            lambda value: value.get("status") in {"waiting_review", *_TERMINAL_RUN_STATUSES},
            timeout_seconds=180,
            description="the durable human-review interrupt",
        )
        if waiting.get("status") != "waiting_review":
            raise DrillFailure("forced-review filing run did not enter waiting_review")
        review_id = str(waiting.get("output_payload", {}).get("review_id") or "")
        if not review_id:
            raise DrillFailure("waiting review run did not expose a review identifier")
        report["interrupt"].update(
            {
                "review_id": review_id,
                "status": waiting.get("status"),
                "current_node": waiting.get("current_node"),
                "attempt_count": int(waiting.get("attempt_count", 0)),
                "worker_lease_released": (
                    waiting.get("worker_id") is None and waiting.get("lease_expires_at") is None
                ),
            }
        )
        if waiting.get("current_node") != "human_review":
            raise DrillFailure("waiting review run stopped at an unexpected node")
        if report["interrupt"]["attempt_count"] != 1:
            raise DrillFailure("human-review interrupt has an unexpected attempt count")
        if report["interrupt"]["worker_lease_released"] is not True:
            raise DrillFailure("human-review interrupt retained its worker lease")

        review = _api_request(
            compose,
            method="GET",
            path=f"/api/filings/reviews/{review_id}",
            workspace_id=arguments.workspace_id,
        )
        packet = review.get("payload", {})
        packet_candidates = len(packet.get("candidate_facts", []))
        packet_evidence = len(packet.get("evidence", []))
        packet_defects = len(packet.get("defects", []))
        report["interrupt"].update(
            {
                "packet_candidate_count": packet_candidates,
                "packet_evidence_count": packet_evidence,
                "packet_defect_count": packet_defects,
            }
        )
        if review.get("status") != "pending":
            raise DrillFailure("review packet was not pending")
        if review.get("run_id") != run_id:
            raise DrillFailure("review packet is bound to an unexpected run")
        if packet_candidates != arguments.expected_facts:
            raise DrillFailure("review packet has an unexpected candidate count")
        if packet_evidence < arguments.expected_facts:
            raise DrillFailure("review packet has insufficient evidence")
        if packet_defects != 0:
            raise DrillFailure("review packet contains validation defects")
        _log(
            f"review interrupt persisted {packet_candidates} candidates "
            f"and {packet_evidence} evidence records"
        )

        report["stage"] = "review_decision"
        _log(f"recording approval by {reviewer_email}")
        _api_request(
            compose,
            method="POST",
            path=f"/api/filings/reviews/{review_id}/decision",
            workspace_id=arguments.workspace_id,
            actor_header=actor_header,
            actor_email=reviewer_email,
            payload={
                "decision": "approve",
                "reason": arguments.reason,
            },
        )

        report["stage"] = "workflow_resume"
        completed = _wait_for(
            lambda: _api_request(
                compose,
                method="GET",
                path=f"/api/filings/runs/{run_id}",
                workspace_id=arguments.workspace_id,
            ),
            lambda value: value.get("status") in _TERMINAL_RUN_STATUSES,
            timeout_seconds=180,
            description="the human-reviewed workflow resume",
        )
        if completed.get("status") != "completed":
            raise DrillFailure("human-reviewed filing run did not complete")

        acceptance = _json_command(
            _internal_command(
                compose,
                "status",
                "--review-id",
                review_id,
                "--workspace-id",
                arguments.workspace_id,
            ),
            label="human-review acceptance verification",
        )
        report["decision"].update(
            {
                "status": acceptance.get("review_status"),
                "reviewer_id": acceptance.get("reviewer_id"),
                "reason_recorded": acceptance.get("reason") == arguments.reason,
                "audit_event_count": int(acceptance.get("audit_event_count", 0)),
                "audit_verified": (
                    acceptance.get("audit_event_count") == 1
                    and acceptance.get("audit_actor_ids") == [reviewer_email]
                    and acceptance.get("audit_actions") == ["review.approve"]
                    and acceptance.get("audit_reason_matches") is True
                ),
            }
        )
        report["resume"].update(
            {
                "status": acceptance.get("run_status"),
                "current_node": acceptance.get("current_node"),
                "attempt_count": int(acceptance.get("attempt_count", 0)),
                "approved_fact_count": int(acceptance.get("approved_fact_count", 0)),
                "unique_approved_fact_ids": int(acceptance.get("unique_approved_fact_ids", 0)),
                "reviewer_approved_fact_count": int(
                    acceptance.get("reviewer_approved_fact_count", 0)
                ),
                "validation_defect_count": int(acceptance.get("validation_defect_count", 0)),
                "worker_lease_released": (
                    acceptance.get("worker_id") is None
                    and acceptance.get("lease_expires_at") is None
                ),
                "trace_id": acceptance.get("trace_id"),
            }
        )
        if report["decision"]["status"] != "approved":
            raise DrillFailure("review decision was not persisted as approved")
        if report["decision"]["reviewer_id"] != reviewer_email:
            raise DrillFailure("persisted reviewer identity does not match")
        if report["decision"]["reason_recorded"] is not True:
            raise DrillFailure("review reason was not persisted")
        if report["decision"]["audit_verified"] is not True:
            raise DrillFailure("review audit event did not match the decision")
        if acceptance.get("decided_at") is None:
            raise DrillFailure("review decision has no decision timestamp")
        if acceptance.get("output_review_id") != review_id:
            raise DrillFailure("completed run references an unexpected review")
        if acceptance.get("output_review_status") != "approved":
            raise DrillFailure("completed run has an unexpected review status")
        if report["resume"]["status"] != "completed":
            raise DrillFailure("accepted filing run did not finish successfully")
        if report["resume"]["current_node"] != "completed":
            raise DrillFailure("accepted filing run finished at an unexpected node")
        if acceptance.get("waiting_review_at") is None:
            raise DrillFailure("accepted filing run has no durable waiting-review timestamp")
        if report["resume"]["attempt_count"] != 2:
            raise DrillFailure("reviewed workflow did not resume on its second claim")
        if report["resume"]["approved_fact_count"] != arguments.expected_facts:
            raise DrillFailure("reviewed workflow approved an unexpected fact count")
        if report["resume"]["unique_approved_fact_ids"] != arguments.expected_facts:
            raise DrillFailure("reviewed workflow produced duplicate approved facts")
        if report["resume"]["reviewer_approved_fact_count"] != arguments.expected_facts:
            raise DrillFailure("approved facts do not carry the reviewer identity")
        if acceptance.get("review_status_counts") != {"approved": arguments.expected_facts}:
            raise DrillFailure("approved facts have unexpected review statuses")
        if report["resume"]["validation_defect_count"] != 0:
            raise DrillFailure("reviewed workflow produced validation defects")
        if report["resume"]["worker_lease_released"] is not True:
            raise DrillFailure("completed reviewed workflow retained its worker lease")
        if not report["resume"]["trace_id"]:
            raise DrillFailure("reviewed workflow has no telemetry trace")

        report["status"] = "passed"
        report["stage"] = "completed"
        report["exit_code"] = 0
        exit_code = 0
        _log("human-review interrupt and resume drill passed")
    except (DrillFailure, KeyError, TypeError, ValueError) as exc:
        report["error"] = {
            "type": type(exc).__name__,
            "message": str(exc)[:500],
        }
        print(f"[trade-human-review] {exc}", file=sys.stderr)
    except KeyboardInterrupt:
        report["stage"] = "interrupted"
        report["error"] = {
            "type": "KeyboardInterrupt",
            "message": "drill interrupted by operator",
        }
        print("[trade-human-review] drill interrupted", file=sys.stderr)
    finally:
        report["finished_at"] = _timestamp()
        _write_report(report_path, report)
        _log(f"human-review report: {report_path}")
    return report, exit_code


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Force a durable filing review interrupt, approve it through the "
            "production API, and verify audited workflow resume."
        )
    )
    parser.add_argument("filing_id", help="Registered XBRL filing used by the drill.")
    parser.add_argument("--reviewer-email", required=True)
    parser.add_argument(
        "--actor-header",
        default="cf-access-authenticated-user-email",
    )
    parser.add_argument("--workspace-id", default="default")
    parser.add_argument("--expected-facts", type=int, default=59)
    parser.add_argument(
        "--reason",
        default=(
            "M1 production human-review resume drill; packet values are covered "
            "by the locked INFY golden dataset."
        ),
    )
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
            "PROD_HUMAN_REVIEW_REPORT_DIR",
            "/opt/trade/human-review-reports",
        ),
    )
    return parser


def main() -> None:
    arguments = _parser().parse_args()
    if arguments.expected_facts < 1:
        raise SystemExit("--expected-facts must be positive")
    try:
        _report, exit_code = run_drill(arguments)
    except DrillFailure as exc:
        print(f"[trade-human-review] {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
    raise SystemExit(exit_code)


if __name__ == "__main__":
    main()
