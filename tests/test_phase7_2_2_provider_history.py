from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

from trade_research.config import Settings
from trade_research.data.daily_work import DailyInstrument, DailyWorkPlanner
from trade_research.data.provider_history import (
    build_provider_daily_history_evidence,
    provider_history_is_quarantined,
    verified_provider_coverage_windows,
    verified_provider_history_start,
)
from trade_research.pipelines import provider_history as provider_history_pipeline
from trade_research.pipelines import yfinance_work_queue

NOW = datetime(2026, 7, 18, 12, tzinfo=UTC)


def _work_item(
    *,
    work_item_id: str = "work-1",
    symbol: str = "AKT-A.TO",
    listing_effective_at: datetime | None = None,
) -> dict:
    return {
        "work_item_id": work_item_id,
        "provider": "yfinance",
        "exchange": "TSX",
        "canonical_instrument_id": f"eq_{symbol}",
        "provider_symbol": symbol,
        "provider_instrument_key": f"YF|{symbol}",
        "interval": "1d",
        "work_type": "initial_backfill",
        "window_start": date(2025, 1, 1),
        "window_end": date(2026, 7, 17),
        "listing_status": "active",
        "listing_status_effective_at": listing_effective_at,
    }


def test_sparse_established_listing_is_quarantined() -> None:
    sessions = [date(2025, 1, 1) + timedelta(days=index) for index in range(300)]
    work = _work_item(
        listing_effective_at=datetime(1993, 1, 8, tzinfo=UTC),
    )

    evidence = build_provider_daily_history_evidence(
        work,
        expected_sessions=sessions,
        observed_dates=[sessions[-1]],
        run_id="run-1",
        at=NOW,
    )

    assert evidence is not None
    assert evidence.classification == "quarantined_sparse"
    assert evidence.quarantine_reason == (
        "implausibly_sparse_history_for_established_listing"
    )
    assert provider_history_is_quarantined([evidence.as_row()]) is True
    assert verified_provider_coverage_windows([evidence.as_row()]) == []


def test_sparse_history_without_old_listing_boundary_is_verified_provider_evidence() -> None:
    sessions = [date(2025, 1, 1) + timedelta(days=index) for index in range(300)]
    work = _work_item(symbol="SHOT", listing_effective_at=None)

    evidence = build_provider_daily_history_evidence(
        work,
        expected_sessions=sessions,
        observed_dates=[sessions[-1]],
        run_id="run-shot",
        at=NOW,
    )

    assert evidence is not None
    assert evidence.classification == "verified_partial"
    assert evidence.first_available_date == sessions[-1]
    assert verified_provider_history_start([evidence.as_row()]) == sessions[-1]
    assert verified_provider_coverage_windows([evidence.as_row()]) == [
        (sessions[0], sessions[-1])
    ]


def test_verified_provider_window_stops_repeat_backfill_but_not_freshness_work() -> None:
    sessions = [date(2026, 7, day) for day in (13, 14, 15, 16, 17)]
    instrument = DailyInstrument(
        canonical_instrument_id="eq_partial",
        provider_symbol="PARTIAL.TO",
        exchange="TSX",
        provider_history_start_date=sessions[0],
    )
    planner = DailyWorkPlanner()

    backfill = planner.plan_initial_backfill(
        [instrument],
        sessions,
        {instrument.instrument_key: {sessions[0], sessions[4]}},
        covered_windows={instrument.instrument_key: [(sessions[0], sessions[4])]},
        now=NOW,
    )
    incremental = planner.plan_incremental(
        [instrument],
        sessions,
        {instrument.instrument_key: sessions[2]},
        overlap_sessions=1,
        now=NOW,
    )

    assert backfill == []
    assert [(item.window_start, item.window_end) for item in incremental] == [
        (sessions[1], sessions[4])
    ]


class _PlannerEvidenceStore:
    def __init__(self) -> None:
        self.cancelled_ids: list[str] = []
        self.cancelled_verified = 2

    def initialize(self) -> None:
        pass

    def latest_provider_eligible_exchange_session(self, *_args, **_kwargs) -> dict:
        return {"session_date": date(2026, 7, 17)}

    def exchange_sessions(self, *_args, **_kwargs) -> list[dict]:
        return [
            {
                "session_date": value,
                "is_trading_day": True,
                "validation_status": "valid",
            }
            for value in (date(2026, 7, 16), date(2026, 7, 17))
        ]

    def cancel_pipeline_work_items_before_listing(self, **_kwargs) -> int:
        return 0

    def cancel_pipeline_work_items_covered_by_provider_history(
        self, **_kwargs
    ) -> int:
        return self.cancelled_verified

    def active_yfinance_daily_instruments(self, _exchange: str) -> list[dict]:
        return [
            {
                "canonical_instrument_id": "eq_aab",
                "provider_symbol": "AAB.TO",
                "provider_instrument_key": "YF|AAB.TO",
                "listing_status": "active",
                "listing_status_effective_at": None,
                "pipeline_eligibility": "incremental",
                "reconciliation_status": "official_eligible",
            },
            {
                "canonical_instrument_id": "eq_akt",
                "provider_symbol": "AKT-A.TO",
                "provider_instrument_key": "YF|AKT-A.TO",
                "listing_status": "active",
                "listing_status_effective_at": datetime(1993, 1, 8, tzinfo=UTC),
                "pipeline_eligibility": "incremental",
                "reconciliation_status": "official_eligible",
            },
        ]

    def provider_daily_history_evidence(self, *_args, **_kwargs) -> dict:
        base = {
            "status": "active",
            "work_type": "initial_backfill",
            "coverage_start": date(2026, 7, 16),
            "coverage_end": date(2026, 7, 17),
            "first_available_date": date(2026, 7, 16),
        }
        return {
            "YF|AAB.TO": [{**base, "classification": "verified_complete"}],
            "YF|AKT-A.TO": [{**base, "classification": "quarantined_sparse"}],
        }

    def cancel_pending_pipeline_work_for_instruments(
        self, canonical_ids, **_kwargs
    ) -> int:
        self.cancelled_ids = list(canonical_ids)
        return len(self.cancelled_ids)

    def latest_daily_ohlcv_dates(self, *_args, **_kwargs) -> dict:
        return {"YF|AAB.TO": date(2026, 7, 17)}

    def daily_ohlcv_dates_by_instrument(self, *_args, **_kwargs) -> dict:
        return {"YF|AAB.TO": {date(2026, 7, 16)}}

    def enqueue_pipeline_work_items(self, work) -> int:
        return len(list(work))

    def pipeline_work_queue_summary(self) -> dict:
        return {}


def test_guarded_planner_consumes_evidence_and_cancels_quarantined_work(
    monkeypatch,
) -> None:
    store = _PlannerEvidenceStore()
    settings = Settings(
        _env_file=None,
        yfinance_provider_history_evidence_enabled=True,
        yfinance_full_tsx_enabled=True,
    )
    monkeypatch.setattr(yfinance_work_queue, "get_settings", lambda: settings)
    monkeypatch.setattr(yfinance_work_queue, "TimescaleStore", lambda _url: store)

    result = yfinance_work_queue.run_yfinance_daily_work_planner(
        exchanges=("TSX",),
        include_incremental=True,
        include_initial_backfill=True,
        enqueue=True,
        at=NOW,
    )

    assert result.metrics["work_generated"] == 0
    assert result.metrics["provider_quarantined_symbols"] == 1
    assert result.metrics["provider_quarantined_work_cancelled"] == 1
    assert result.metrics["work_cancelled_by_provider_history"] == 2
    assert store.cancelled_ids == ["eq_akt"]
    assert result.metrics["exchanges"]["TSX"]["provider_quarantined_symbols"] == [
        "AKT-A.TO"
    ]


class _BootstrapEvidenceStore:
    def __init__(self) -> None:
        self.evidence_rows: list[dict] = []
        self.cancelled_ids: list[str] = []
        self.cancelled_verified = 3
        self.sessions = [
            date(2025, 1, 1) + timedelta(days=index) for index in range(300)
        ]

    def initialize(self) -> None:
        pass

    def active_yfinance_daily_instruments(self, _exchange: str) -> list[dict]:
        return [
            {
                "canonical_instrument_id": "eq_akt",
                "provider_symbol": "AKT-A.TO",
                "provider_instrument_key": "YF|AKT-A.TO",
                "reconciliation_status": "official_eligible",
            }
        ]

    def succeeded_daily_backfill_work_items(self, _ids) -> list[dict]:
        return [
            {
                **_work_item(
                    work_item_id="akt-backfill",
                    symbol="AKT-A.TO",
                    listing_effective_at=datetime(1993, 1, 8, tzinfo=UTC),
                ),
                "window_start": self.sessions[0],
                "window_end": self.sessions[-1],
                "run_id": "akt-run",
            }
        ]

    def exchange_sessions(self, *_args, **_kwargs) -> list[dict]:
        return [
            {
                "session_date": value,
                "is_trading_day": True,
                "validation_status": "valid",
            }
            for value in self.sessions
        ]

    def daily_ohlcv_dates_by_instrument(self, *_args, **_kwargs) -> dict:
        return {"YF|AKT-A.TO": {self.sessions[-1]}}

    def upsert_provider_daily_history_evidence(self, rows) -> int:
        self.evidence_rows = list(rows)
        return len(self.evidence_rows)

    def cancel_pending_pipeline_work_for_instruments(
        self, canonical_ids, **_kwargs
    ) -> int:
        self.cancelled_ids = list(canonical_ids)
        return len(self.cancelled_ids)

    def cancel_pipeline_work_items_covered_by_provider_history(
        self, **_kwargs
    ) -> int:
        return self.cancelled_verified


def test_bootstrap_classifies_existing_success_and_cancels_quarantine(
    monkeypatch,
) -> None:
    store = _BootstrapEvidenceStore()
    monkeypatch.setattr(
        provider_history_pipeline,
        "get_settings",
        lambda: Settings(_env_file=None),
    )
    monkeypatch.setattr(
        provider_history_pipeline,
        "TimescaleStore",
        lambda _url: store,
    )

    result = (
        provider_history_pipeline.run_yfinance_provider_history_evidence_bootstrap(
            "TSX",
            symbol_limit=1,
            at=NOW,
        )
    )

    assert result.status == "warn"
    assert result.metrics["classifications"] == {"quarantined_sparse": 1}
    assert result.metrics["quarantined_symbols"] == ["AKT-A.TO"]
    assert result.metrics["pending_work_cancelled"] == 4
    assert result.metrics["quarantined_work_cancelled"] == 1
    assert result.metrics["verified_work_cancelled"] == 3
    assert store.cancelled_ids == ["eq_AKT-A.TO"]


class _EvidenceWriteStore:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def exchange_sessions(self, *_args, **_kwargs) -> list[dict]:
        return [
            {
                "session_date": date(2026, 7, 16),
                "is_trading_day": True,
                "validation_status": "valid",
            },
            {
                "session_date": date(2026, 7, 17),
                "is_trading_day": True,
                "validation_status": "valid",
            },
        ]

    def upsert_provider_daily_history_evidence(self, rows) -> int:
        self.rows = list(rows)
        return len(self.rows)

    def daily_ohlcv_dates_by_instrument(self, *_args, **_kwargs) -> dict:
        return {"YF|AAB.TO": {date(2026, 7, 17)}}


def test_successful_backfill_records_provider_history_evidence() -> None:
    store = _EvidenceWriteStore()
    work = _work_item(symbol="AAB.TO")
    work.update(
        {
            "window_start": date(2026, 7, 16),
            "window_end": date(2026, 7, 17),
        }
    )

    written = yfinance_work_queue._record_successful_provider_history_evidence(
        db=store,
        settings=Settings(_env_file=None),
        exchange="TSX",
        work_items=[work],
        ticker_outcomes=[{"work_item_id": "work-1", "status": "success"}],
        run_id="run-1",
    )

    assert written == 1
    assert store.rows[0]["classification"] == "verified_partial"
    assert store.rows[0]["missing_rows"] == 1


def test_phase7_2_2_migration_creates_provider_history_evidence_table(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'phase7-2-2.sqlite'}"
    monkeypatch.setenv("DATABASE_URL", database_url)
    command.upgrade(Config("alembic.ini"), "head")
    engine = create_engine(database_url)

    columns = {
        column["name"]
        for column in inspect(engine).get_columns("provider_daily_history_evidence")
    }
    with engine.connect() as connection:
        revision = connection.execute(
            text("SELECT version_num FROM alembic_version")
        ).scalar_one()

    assert revision == "20260726_0012"
    assert {
        "instrument_key",
        "coverage_start",
        "coverage_end",
        "first_available_date",
        "classification",
        "quarantine_reason",
        "evidence_run_id",
    }.issubset(columns)


def test_provider_history_feature_is_fail_closed_by_default() -> None:
    settings = Settings(_env_file=None)

    assert settings.yfinance_provider_history_evidence_enabled is False
    assert settings.yfinance_sparse_history_minimum_expected_rows == 220
    assert settings.yfinance_sparse_history_maximum_observed_rows == 5
