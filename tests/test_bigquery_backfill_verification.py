from __future__ import annotations

import json
from datetime import date
from types import SimpleNamespace
from typing import Any

from typer.testing import CliRunner

from trade_research import cli
from trade_research.analytics.bigquery import (
    evaluate_bigquery_backfill_reconciliation,
)


class BackfillState:
    def __init__(
        self,
        runs: list[dict[str, Any]],
        partitions: dict[str, list[dict[str, Any]]],
    ) -> None:
        self.runs = runs
        self.partitions = partitions

    def bigquery_sync_runs(self, *, limit: int) -> list[dict[str, Any]]:
        return self.runs[:limit]

    def bigquery_sync_partitions(
        self,
        *,
        run_id: str,
        limit: int,
    ) -> list[dict[str, Any]]:
        return self.partitions.get(run_id, [])[:limit]


def _run(year: int, run_id: str, *, status: str = "completed") -> dict[str, Any]:
    return {
        "run_id": run_id,
        "status": status,
        "exchange": "US",
        "year": year,
        "entities": ["ohlcv_daily"],
        "error_details": None,
    }


def _partition(
    year: int,
    run_id: str,
    *,
    destination_rows: int = 100,
    destination_watermark: str | None = None,
) -> dict[str, Any]:
    maximum_date = date(year, 12, 31)
    watermark = str(maximum_date)
    return {
        "run_id": run_id,
        "entity": "ohlcv_daily",
        "exchange": "US",
        "partition_start": date(year, 1, 1),
        "partition_end": date(year, 12, 31),
        "status": "completed",
        "source_row_count": 100,
        "destination_row_count": destination_rows,
        "count_difference": 100 - destination_rows,
        "rejected_rows": 0,
        "duplicate_business_key_count": 0,
        "source_min_date": date(year, 1, 2),
        "source_max_date": maximum_date,
        "destination_min_date": date(year, 1, 2),
        "destination_max_date": maximum_date,
        "source_watermark": watermark,
        "destination_watermark": destination_watermark or watermark,
        "bigquery_job_id": f"bq-{run_id}",
        "schema_drift": {},
        "error_details": None,
    }


def test_backfill_verification_passes_complete_year_range() -> None:
    state = BackfillState(
        runs=[_run(2017, "run-2017"), _run(2016, "run-2016")],
        partitions={
            "run-2017": [_partition(2017, "run-2017")],
            "run-2016": [_partition(2016, "run-2016")],
        },
    )

    result = evaluate_bigquery_backfill_reconciliation(
        state,  # type: ignore[arg-type]
        exchange="us",
        start_year=2016,
        end_year=2017,
    )

    assert result.reconciled is True
    assert [year.year for year in result.years] == [2016, 2017]
    assert all(year.reconciled for year in result.years)


def test_backfill_verification_fails_closed_on_missing_or_mismatched_evidence() -> None:
    state = BackfillState(
        runs=[_run(2016, "run-2016")],
        partitions={
            "run-2016": [
                _partition(
                    2016,
                    "run-2016",
                    destination_rows=99,
                    destination_watermark="2016-12-29",
                )
            ]
        },
    )

    result = evaluate_bigquery_backfill_reconciliation(
        state,  # type: ignore[arg-type]
        exchange="US",
        start_year=2016,
        end_year=2017,
    )

    assert result.reconciled is False
    assert "Source and destination row counts do not match." in result.years[0].issues
    assert "Source and destination watermarks do not match." in result.years[0].issues
    assert result.years[1].issues == ("No synchronization run evidence was found.",)


def test_backfill_verification_can_require_equivalent_rerun() -> None:
    state = BackfillState(
        runs=[_run(2016, "second"), _run(2016, "first")],
        partitions={
            "second": [_partition(2016, "second")],
            "first": [_partition(2016, "first")],
        },
    )

    result = evaluate_bigquery_backfill_reconciliation(
        state,  # type: ignore[arg-type]
        exchange="US",
        start_year=2016,
        end_year=2016,
        require_idempotent_rerun=True,
    )

    assert result.reconciled is True
    assert result.years[0].run_id == "second"
    assert result.years[0].compared_run_id == "first"


def test_backfill_verification_requires_second_run_for_idempotency() -> None:
    state = BackfillState(
        runs=[_run(2016, "only")],
        partitions={"only": [_partition(2016, "only")]},
    )

    result = evaluate_bigquery_backfill_reconciliation(
        state,  # type: ignore[arg-type]
        exchange="US",
        start_year=2016,
        end_year=2016,
        require_idempotent_rerun=True,
    )

    assert result.reconciled is False
    assert "A second run is required to verify idempotency." in result.years[0].issues


def test_verify_bigquery_backfill_cli_emits_json_and_success_exit_code(monkeypatch) -> None:
    state = BackfillState(
        runs=[_run(2016, "run-2016")],
        partitions={"run-2016": [_partition(2016, "run-2016")]},
    )
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: SimpleNamespace(database_url="postgresql://not-opened"),
    )
    monkeypatch.setattr(cli, "TimescaleStore", lambda _database_url: state)

    result = CliRunner().invoke(
        cli.app,
        [
            "verify-bigquery-backfill",
            "US",
            "--start-year",
            "2016",
            "--end-year",
            "2016",
            "--json",
        ],
    )

    assert result.exit_code == 0
    payload = json.loads(result.output)
    assert payload["read_only"] is True
    assert payload["reconciled"] is True
    assert payload["years"][0]["bigquery_job_id"] == "bq-run-2016"


def test_verify_bigquery_backfill_cli_returns_failure_for_incomplete_range(
    monkeypatch,
) -> None:
    state = BackfillState(runs=[], partitions={})
    monkeypatch.setattr(
        cli,
        "get_settings",
        lambda: SimpleNamespace(database_url="postgresql://not-opened"),
    )
    monkeypatch.setattr(cli, "TimescaleStore", lambda _database_url: state)

    result = CliRunner().invoke(
        cli.app,
        [
            "verify-bigquery-backfill",
            "US",
            "--start-year",
            "2016",
            "--end-year",
            "2016",
        ],
    )

    assert result.exit_code == 1
    assert "overall=FAIL" in result.output
