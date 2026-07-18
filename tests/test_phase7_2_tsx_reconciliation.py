from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text

from trade_research.config import Settings
from trade_research.pipelines import yfinance_work_queue
from trade_research.pipelines.base import PipelineRunResult
from trade_research.schemas import Symbol
from trade_research.storage.timescale import TimescaleStore
from trade_research.universe.persisted import reconcile_universe_snapshot
from trade_research.universe.tsx_reconciliation import (
    ReconciledTSXUniverseProvider,
    TMXDirectoryEntry,
    TMXIssuer,
    TMXOfficialSnapshot,
    classify_tsx_security,
)


class _CandidateProvider:
    exchange = "TSX"
    source = "test_candidates"

    def fetch(self) -> list[Symbol]:
        return [
            _candidate("SHOP"),
            _candidate("BBD.B"),
            _candidate("AP.UN"),
            _candidate("AAPL"),
            _candidate("CU.PJ"),
            _candidate("IIP.UN"),
            _candidate("NEW"),
            _candidate("UNKNOWN"),
        ]

    def diagnostics(self) -> dict[str, int]:
        return {"source_rows": 8, "tsx_rows": 8}


class _OfficialProvider:
    issuer_url = "https://official.test/tsx.xlsx"

    def fetch(self) -> TMXOfficialSnapshot:
        observed_at = datetime(2026, 7, 18, tzinfo=UTC)
        return TMXOfficialSnapshot(
            issuers=(
                _issuer("SHOP", "SHOP1", "Technology"),
                _issuer("BBD", "BBD1", "Industrial Products & Services"),
                _issuer("AP", "AP1", "Real Estate", security_type="Income Trust"),
                _issuer("AAPL", "AAPL1", "CDR", security_type="CDR"),
                _issuer("CU", "CU1", "Utilities & Pipelines"),
                _issuer("IIP", "IIP1", "Real Estate", security_type="Income Trust"),
                _issuer("MISSING", "MISSING1", "Mining"),
            ),
            recently_listed={
                "NEW": TMXDirectoryEntry("NEW", "New Company", observed_at),
            },
            recently_delisted={
                "IIP": TMXDirectoryEntry("IIP", "InterRent", observed_at),
            },
            suspended={},
            checked_at=observed_at,
            source_updated_at=observed_at,
        )


def test_reconciled_tsx_provider_applies_official_policy_and_lifecycle() -> None:
    provider = ReconciledTSXUniverseProvider(
        candidate_provider=_CandidateProvider(),
        official_provider=_OfficialProvider(),
    )

    rows = {item.symbol: item for item in provider.fetch()}

    assert rows["SHOP"].pipeline_eligibility == "incremental"
    assert rows["SHOP"].reconciliation_status == "official_eligible"
    assert rows["SHOP"].source_identity == "tmx:SHOP1:ROOT"
    assert rows["BBD.B"].instrument_type == "common_equity_class"
    assert rows["BBD.B"].source_identity == "tmx:BBD1:B"
    assert rows["AP.UN"].instrument_type == "reit_unit"
    assert rows["AP.UN"].pipeline_eligibility == "incremental"
    assert rows["AAPL"].reconciliation_reason == "excluded_product_cdr"
    assert rows["CU.PJ"].instrument_type == "preferred_share"
    assert rows["IIP.UN"].listing_status == "delisted"
    assert rows["IIP.UN"].pipeline_eligibility == "none"
    assert rows["NEW"].reconciliation_status == "official_recent_unclassified"
    assert rows["UNKNOWN"].reconciliation_status == "candidate_only"
    assert all(
        rows[symbol].pipeline_eligibility == "none"
        for symbol in ("AAPL", "CU.PJ", "IIP.UN", "NEW", "UNKNOWN")
    )

    diagnostics = provider.diagnostics()
    assert diagnostics["candidate_symbols"] == 8
    assert diagnostics["eligible_symbols"] == 3
    assert diagnostics["provider_unmapped_official_issuers"] == 1
    assert diagnostics["provider_unmapped_sample"] == ["MISSING"]


def test_tsx_classification_excludes_non_equity_security_suffixes() -> None:
    business = _issuer("ABC", "ABC1", "Technology")
    real_estate = _issuer("REI", "REI1", "Real Estate")

    assert classify_tsx_security("ABC", business) == (
        True,
        "common_equity",
        "eligible_common_equity",
    )
    assert classify_tsx_security("ABC.B", business)[0] is True
    assert classify_tsx_security("REI.UN", real_estate)[0] is True
    assert classify_tsx_security("ABC.UN", business)[0] is False
    assert classify_tsx_security("ABC.U", business)[0] is False
    assert classify_tsx_security("ABC.PA", business)[1] == "preferred_share"
    assert classify_tsx_security("ABC.DBH", business)[1] == "debt_security"


def test_reconciliation_only_creates_backfills_for_pipeline_eligible_symbols() -> None:
    provider = ReconciledTSXUniverseProvider(
        candidate_provider=_CandidateProvider(),
        official_provider=_OfficialProvider(),
    )
    observed_at = datetime(2026, 7, 18, tzinfo=UTC)

    plan = reconcile_universe_snapshot(
        provider.fetch(),
        [],
        exchange="TSX",
        snapshot_id="phase7-2",
        fetched_at=observed_at,
    )

    assert {item.provider_symbol for item in plan.work_items} == {
        "SHOP.TO",
        "BBD-B.TO",
        "AP-UN.TO",
    }


def test_tsx_canary_is_bounded_and_requires_explicit_enqueue_flag(monkeypatch) -> None:
    settings = Settings(
        _env_file=None,
        yfinance_tsx_canary_enabled=False,
        yfinance_tsx_canary_max_symbols=25,
    )
    monkeypatch.setattr(yfinance_work_queue, "get_settings", lambda: settings)

    with pytest.raises(ValueError, match="canary enqueue is disabled"):
        yfinance_work_queue.run_yfinance_tsx_canary_planner(symbol_limit=1, enqueue=True)
    with pytest.raises(ValueError, match="exceeds configured maximum"):
        yfinance_work_queue.run_yfinance_tsx_canary_planner(symbol_limit=26)

    captured: dict[str, object] = {}

    def fake_planner(**kwargs):
        captured.update(kwargs)
        return PipelineRunResult(
            name="yfinance_daily_work_planner",
            status="pass",
            metrics={"exchanges": {"TSX": {}}, "queue": {}},
        )

    monkeypatch.setattr(yfinance_work_queue, "run_yfinance_daily_work_planner", fake_planner)
    result = yfinance_work_queue.run_yfinance_tsx_canary_planner(
        symbol_limit=5,
        enqueue=False,
    )

    assert captured["exchanges"] == ("TSX",)
    assert captured["instrument_limit_per_exchange"] == 5
    assert captured["enqueue"] is False
    assert captured["allow_disabled_exchanges"] is True
    assert result.metrics["canary"] is True


def test_general_planner_cannot_bypass_disabled_exchange_flags(monkeypatch) -> None:
    monkeypatch.setattr(
        yfinance_work_queue,
        "get_settings",
        lambda: Settings(_env_file=None),
    )
    monkeypatch.setattr(
        yfinance_work_queue,
        "TimescaleStore",
        lambda _url: pytest.fail("disabled exchange must fail before opening the database"),
    )

    with pytest.raises(ValueError, match="exchange flags are disabled for: TSX"):
        yfinance_work_queue.run_yfinance_daily_work_planner(exchanges=("TSX",))


def test_phase7_2_1_migration_adds_columns_and_cancels_prelisting_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_url = f"sqlite:///{tmp_path / 'phase7-2.sqlite'}"
    monkeypatch.setenv("DATABASE_URL", database_url)
    engine = create_engine(database_url)
    with engine.begin() as connection:
        connection.execute(
            text(
                "CREATE TABLE symbols ("
                "symbol VARCHAR NOT NULL, "
                "exchange VARCHAR NOT NULL, "
                "yahoo_symbol VARCHAR, "
                "is_active BOOLEAN NOT NULL DEFAULT true, "
                "PRIMARY KEY (symbol, exchange))"
            )
        )

    config = Config("alembic.ini")
    command.upgrade(config, "20260718_0004")

    with engine.begin() as connection:
        connection.execute(
            text(
                "INSERT INTO symbols ("
                "symbol, exchange, yahoo_symbol, is_active, canonical_instrument_id, "
                "listing_status, listing_status_effective_at"
                ") VALUES "
                "('AAUC', 'TSX', 'AAUC.TO', true, 'eq_aauc', 'active', "
                "'2023-09-11 00:00:00'), "
                "('ABXX', 'TSX', 'ABXX.TO', true, 'eq_abxx', 'active', "
                "'2023-01-01 00:00:00')"
            )
        )
        connection.execute(
            text(
                "INSERT INTO pipeline_work_items ("
                "work_item_id, idempotency_key, work_type, provider, exchange, "
                "canonical_instrument_id, provider_symbol, interval, window_start, "
                "window_end, priority, status, attempt_count, max_attempts, "
                "next_attempt_at, created_at, updated_at"
                ") VALUES "
                "('old-aauc', 'old-aauc', 'initial_backfill', 'yfinance', 'TSX', "
                "'eq_aauc', 'AAUC.TO', '1d', '2016-07-18', '2023-09-08', 50, "
                "'queued', 0, 9, '2026-07-18 09:44:27', "
                "'2026-07-18 09:44:27', '2026-07-18 09:44:27'), "
                "('valid-abxx', 'valid-abxx', 'initial_backfill', 'yfinance', 'TSX', "
                "'eq_abxx', 'ABXX.TO', '1d', '2023-01-03', '2026-07-17', 50, "
                "'queued', 0, 9, '2026-07-18 09:44:27', "
                "'2026-07-18 09:44:27', '2026-07-18 09:44:27')"
            )
        )

    command.upgrade(config, "head")

    columns = {
        column["name"]
        for column in inspect(engine).get_columns("symbols")
    }
    with engine.connect() as connection:
        revision = connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
        work = {
            row[0]: (row[1], row[2])
            for row in connection.execute(
                text(
                    "SELECT provider_symbol, status, last_error_code "
                    "FROM pipeline_work_items ORDER BY provider_symbol"
                )
            ).all()
        }

    assert {
        "instrument_type",
        "reconciliation_status",
        "reconciliation_reason",
        "official_sector",
        "official_security_type",
        "official_source_updated_at",
    }.issubset(columns)
    assert revision == "20260718_0005"
    assert work["AAUC.TO"] == ("cancelled", "outside_listing_window")
    assert work["ABXX.TO"] == ("queued", None)

    with engine.begin() as connection:
        connection.execute(
            text(
                "UPDATE pipeline_work_items SET status = 'queued', "
                "last_error_code = NULL, last_error_message = NULL, "
                "completed_at = NULL WHERE provider_symbol = 'AAUC.TO'"
            )
        )
    cancelled = TimescaleStore(database_url).cancel_pipeline_work_items_before_listing(
        exchange="TSX",
        at=datetime(2026, 7, 18, tzinfo=UTC),
    )
    with engine.connect() as connection:
        runtime_status = connection.execute(
            text(
                "SELECT status, last_error_code FROM pipeline_work_items "
                "WHERE provider_symbol = 'AAUC.TO'"
            )
        ).one()

    assert cancelled == 1
    assert runtime_status == ("cancelled", "outside_listing_window")


def test_production_compose_passes_guarded_tsx_canary_settings() -> None:
    compose = (
        Path(__file__).resolve().parents[1] / "docker-compose.prod.yml"
    ).read_text(encoding="utf-8")

    assert compose.count("\n      YFINANCE_TSX_CANARY_ENABLED:") == 3
    assert compose.count("\n      YFINANCE_TSX_CANARY_MAX_SYMBOLS:") == 3
    assert compose.count("\n      TSX_OFFICIAL_ISSUER_URL:") == 3
    assert compose.count("\n      TSX_OFFICIAL_DIRECTORY_BASE_URL:") == 3


def _candidate(symbol: str) -> Symbol:
    return Symbol(
        symbol=symbol,
        exchange="TSX",
        yahoo_symbol=f"{symbol.replace('.', '-')}.TO",
        source="candidate",
    )


def _issuer(
    root: str,
    source_identity: str,
    sector: str,
    *,
    security_type: str | None = None,
) -> TMXIssuer:
    return TMXIssuer(
        source_identity=source_identity,
        root_ticker=root,
        name=f"{root} Company",
        exchange="TSX",
        sector=sector,
        sub_sector=None,
        security_type=security_type,
        listing_date=datetime(2020, 1, 1, tzinfo=UTC),
    )
