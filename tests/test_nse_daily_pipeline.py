import csv
import io
import zipfile
from datetime import UTC, date, datetime
from decimal import Decimal

import pytest
from sqlalchemy import create_engine, func, select

from trade_data.config import Settings
from trade_data.data.nse_bhavcopy import SOURCE, parse_bhavcopy
from trade_data.pipelines import nse_daily as pipeline
from trade_data.storage.nse import nse_sessions
from trade_data.storage.timescale import TimescaleStore, ohlcv_daily_table, symbols_table

DAY = date(2026, 10, 23)


def archive(rows=None, *, day=DAY, member=None):
    row = dict(
        TradDt=str(day),
        Sgmt="CM",
        Src="NSE",
        FinInstrmTp="STK",
        TckrSymb="AAA",
        ISIN="INE002A01018",
        SctySrs="EQ",
        FinInstrmNm="AAA LIMITED",
        OpnPric="10.25",
        HghPric="12.50",
        LwPric="9.00",
        ClsPric="11.75",
        TtlTradgVol="9007199254740993",
    )
    records = [{**row, **r} for r in (rows if rows is not None else [{}])]
    text = io.StringIO()
    writer = csv.DictWriter(text, fieldnames=list(row))
    writer.writeheader()
    writer.writerows(records)
    raw = io.BytesIO()
    with zipfile.ZipFile(raw, "w") as z:
        z.writestr(member or f"BhavCopy_NSE_CM_0_0_0_{day:%Y%m%d}_F_0000.csv", text.getvalue())
    return raw.getvalue()


def test_parser_keeps_exact_integer_volume_and_decimal_prices():
    source = parse_bhavcopy(archive(), DAY)
    assert source.frame.iloc[0].volume == 9007199254740993
    assert source.frame.iloc[0].open == Decimal("10.25")
    assert len(source.sha256) == 64


@pytest.mark.parametrize(
    "changes",
    [
        {"TradDt": "2026-10-22"},
        {"Src": "BSE"},
        {"Sgmt": "FO"},
        {"TtlTradgVol": "1.1"},
        {"TtlTradgVol": "-1"},
        {"TtlTradgVol": "9223372036854775808"},
        {"OpnPric": "NaN"},
        {"LwPric": "11"},
        {"ClsPric": "99"},
    ],
)
def test_parser_fails_closed_on_wrong_identity_date_or_invalid_ohlcv(changes):
    with pytest.raises(ValueError):
        parse_bhavcopy(archive([changes]), DAY)


def test_parser_accounts_for_non_stock_exclusions_and_keeps_be_bz_sme():
    rows = [{"SctySrs": s, "TckrSymb": s} for s in ("EQ", "BE", "BZ", "SM", "ST")]
    rows += [
        {"TckrSymb": "FUND", "ISIN": "INF002A01018"},
        {"TckrSymb": "BOND", "SctySrs": "N1"},
        {"TckrSymb": "AAA-RE"},
        {"TckrSymb": "AAA-PP"},
    ]
    source = parse_bhavcopy(archive(rows), DAY)
    assert len(source.frame) == 5
    assert source.total_records == 9
    assert sum(source.excluded.values()) == 4


def test_parser_rejects_duplicate_series_and_wrong_archive():
    with pytest.raises(ValueError):
        parse_bhavcopy(archive([{}, {"SctySrs": "BE"}]), DAY)
    with pytest.raises(ValueError):
        parse_bhavcopy(archive(member="../wrong.csv"), DAY)


def test_zero_volume_is_an_explicit_flag_not_invalidity():
    assert parse_bhavcopy(archive([{"TtlTradgVol": "0"}]), DAY).frame.iloc[0].volume == 0


@pytest.fixture
def storage():
    engine = create_engine("sqlite:///:memory:")
    for table in (symbols_table, ohlcv_daily_table, nse_sessions):
        table.create(engine)
    store = TimescaleStore.__new__(TimescaleStore)
    store.engine = engine
    with engine.begin() as c:
        c.execute(
            symbols_table.insert().values(
                symbol="AAA",
                exchange="NSE",
                source="legacy",
                source_identity="INE002A01018",
                canonical_instrument_id="id-aaa",
                provider_instrument_key="legacy|AAA",
                yahoo_symbol="AAA.NS",
                fetched_at=datetime.now(UTC),
            )
        )
    return store


def memory_replica(monkeypatch, *, corrupt=False):
    records = {}

    def publish(settings, batch, **kwargs):
        for candle in batch.candles:
            row = {
                k: getattr(candle, k)
                for k in (
                    "instrument_id",
                    "provider_symbol",
                    "symbol",
                    "exchange",
                    "session_date",
                    "open",
                    "high",
                    "low",
                    "close",
                    "volume",
                    "provider_timestamp",
                )
            }
            row["source"] = SOURCE
            if corrupt:
                row["volume"] += 1
            records[candle.identity] = row

    class Replica:
        def __init__(self, *args, **kwargs):
            pass

        def read_daily_partition(self, **kwargs):
            return list(records.values())

    monkeypatch.setattr(pipeline, "replicate_validated_batch", publish)
    monkeypatch.setattr(pipeline, "create_clickhouse_client", lambda settings: None)
    monkeypatch.setattr(pipeline, "ClickHouseMarketDataRepository", Replica)


def test_ingest_proves_exchange_coverage_and_idempotent_replay_without_touching_legacy(
    monkeypatch,
    storage,
):
    memory_replica(monkeypatch)
    source = parse_bhavcopy(archive(), DAY)
    settings = Settings(_env_file=None)
    # A different source with different values must remain untouched.
    with storage.engine.begin() as c:
        c.execute(
            ohlcv_daily_table.insert().values(
                instrument_key="legacy|AAA",
                source="legacy",
                date=DAY,
                symbol="AAA.NS",
                exchange="NSE",
                open=1,
                high=1,
                low=1,
                close=1,
                volume=7,
                fetched_at=datetime.now(UTC),
                quality_status="ok",
            )
        )
    first = pipeline._ingest(storage, settings, source)
    second = pipeline._ingest(storage, settings, source)
    assert first["digest"] == second["digest"]
    with storage.engine.connect() as c:
        assert c.execute(select(func.count()).select_from(ohlcv_daily_table)).scalar() == 2
        assert c.execute(select(nse_sessions.c.status)).scalar() == "complete"
        assert (
            c.execute(
                select(ohlcv_daily_table.c.volume).where(ohlcv_daily_table.c.source == "legacy")
            ).scalar()
            == 7
        )


def test_volume_difference_cannot_be_reported_complete(monkeypatch, storage):
    memory_replica(monkeypatch, corrupt=True)
    with pytest.raises(ValueError, match="source/replica"):
        pipeline._ingest(storage, Settings(_env_file=None), parse_bhavcopy(archive(), DAY))
    with storage.engine.connect() as c:
        assert c.execute(select(nse_sessions.c.status)).scalar() == "mismatch"


def test_new_ticker_identity_mismatch_fails_closed(storage):
    source = parse_bhavcopy(archive([{"ISIN": "INE003A01018"}]), DAY)
    with pytest.raises(ValueError, match="identity changed"):
        pipeline._identities(storage, source)


def test_new_security_can_be_discovered_without_a_legacy_snapshot(storage):
    source = parse_bhavcopy(archive([{"TckrSymb": "NEW", "ISIN": "INE003A01018"}]), DAY)
    ids = pipeline._identities(storage, source)
    assert ids["NEW"]["provider_instrument_key"] == "NSE|INE003A01018"
    assert ids["NEW"]["source_identity"] == "INE003A01018"


def test_replication_bounds_instrument_parameters_for_a_full_exchange_file(monkeypatch):
    from types import SimpleNamespace

    from trade_data.market_data import ingestion
    from trade_data.market_data.replication import candle_batch_summary

    source = parse_bhavcopy(archive(), DAY)
    candles = []
    from trade_data.market_data.contracts import CandleInterval, MarketCandle

    for i in range(601):
        candles.append(
            MarketCandle(
                instrument_id=f"id-{i}",
                provider_symbol=f"S{i}",
                symbol=f"S{i}",
                exchange="NSE",
                session_date=DAY,
                open=Decimal(10),
                high=Decimal(12),
                low=Decimal(9),
                close=Decimal(11),
                volume=7,
                currency="INR",
                provider=SOURCE,
                provider_timestamp=datetime.now(UTC),
                request_id="test",
                adapter_version="test",
                interval=CandleInterval.ONE_DAY,
            )
        )
    seen = []

    class Repository:
        def __init__(self, *a, **k):
            self.candles = []

        def insert_validated(self, rows, **kwargs):
            self.candles = rows
            seen.append((len(rows), kwargs["source_run_id"]))
            return len(rows)

        def batch_summary(self, **kwargs):
            assert len(kwargs["instrument_ids"]) <= 200
            return candle_batch_summary(self.candles)

    monkeypatch.setattr(ingestion, "ClickHouseMarketDataRepository", Repository)
    monkeypatch.setattr(ingestion, "create_clickhouse_client", lambda settings: None)
    batch = ingestion.ValidatedMarketDataBatch(
        SimpleNamespace(exchange="NSE", interval=CandleInterval.ONE_DAY),
        source.frame,
        tuple(candles),
        None,
        None,
    )
    settings = Settings(
        _env_file=None,
        clickhouse_enabled=True,
        clickhouse_write_enabled=True,
        clickhouse_password="test",
    )
    assert (
        ingestion.replicate_validated_batch(settings, batch, source_run_id="whole-file", version=1)
        == 601
    )
    assert [n for n, _ in seen] == [200, 200, 200, 1]
    assert len({run for _, run in seen}) == 4


def test_same_issuer_equity_serial_change_preserves_raw_prices_and_discloses_binding(storage):
    source = parse_bhavcopy(archive([{"ISIN": "INE002A01026"}]), DAY)
    binding = pipeline._identities(storage, source)["AAA"]
    assert binding["canonical_instrument_id"] == "id-aaa"
    assert binding["source_identity"] == "INE002A01026"
    assert binding["anchor_isin"] == "INE002A01018"
    assert binding["identity_reason"] == "same_issuer_equity_serial"
    assert source.frame.iloc[0].open == Decimal("10.25")


def test_missing_legacy_isin_is_established_from_official_source(storage):
    with storage.engine.begin() as c:
        c.execute(symbols_table.update().values(source_identity=None))
    binding = pipeline._identities(storage, parse_bhavcopy(archive(), DAY))["AAA"]
    assert binding["identity_reason"] == "official_identity_established"
    with storage.engine.connect() as c:
        assert c.execute(select(symbols_table.c.source_identity)).scalar() == "INE002A01018"


def test_renamed_ticker_alias_does_not_duplicate_the_same_stored_candle(monkeypatch, storage):
    memory_replica(monkeypatch)
    with storage.engine.begin() as c:
        c.execute(
            symbols_table.insert().values(
                symbol="OLDNAME",
                exchange="NSE",
                source=SOURCE,
                source_identity="INE002A01018",
                canonical_instrument_id="id-aaa",
                provider_instrument_key="legacy|AAA",
                yahoo_symbol="OLDNAME.NS",
                fetched_at=datetime.now(UTC),
            )
        )
    result = pipeline._ingest(storage, Settings(_env_file=None), parse_bhavcopy(archive(), DAY))
    assert result["rows"] == 1
    assert result["status"] == "complete"


@pytest.mark.parametrize(
    "end,start", [(date(2026, 10, 27), date(2025, 10, 28)), (date(2024, 2, 29), date(2023, 3, 1))]
)
def test_rolling_year_matches_frozen_snapshot_date_convention(end, start):
    from trade_data.data.nse_bhavcopy import rolling_year_start

    assert rolling_year_start(end) == start


def test_official_rename_reuses_known_isin_stock_instead_of_creating_an_extra_stock(storage):
    source = parse_bhavcopy(archive([{"TckrSymb": "OLDNAME"}]), DAY)
    binding = pipeline._identities(storage, source)["OLDNAME"]
    assert binding["provider_instrument_key"] == "legacy|AAA"
    assert binding["provider_symbol"] == "AAA.NS"
    assert binding["identity_reason"] == "official_ticker_alias"
    assert binding["exchange_symbol"] == "OLDNAME"
    with storage.engine.connect() as c:
        assert c.execute(select(func.count()).select_from(symbols_table)).scalar() == 1


def test_new_publication_preserves_earlier_official_imports_without_counting_their_aliases(
    monkeypatch,
    storage,
):
    memory_replica(monkeypatch)
    with storage.engine.begin() as c:
        c.execute(
            ohlcv_daily_table.insert().values(
                instrument_key="old-import-key",
                source="nse_official_bhavcopy",
                date=DAY,
                symbol="OLDALIAS",
                exchange="NSE",
                open=1,
                high=1,
                low=1,
                close=1,
                volume=8,
                fetched_at=datetime.now(UTC),
                quality_status="ok",
            )
        )
    result = pipeline._ingest(storage, Settings(_env_file=None), parse_bhavcopy(archive(), DAY))
    assert result["status"] == "complete"
    assert result["rows"] == 1
    with storage.engine.connect() as c:
        assert (
            c.execute(
                select(ohlcv_daily_table.c.volume).where(
                    ohlcv_daily_table.c.source == "nse_official_bhavcopy"
                )
            ).scalar()
            == 8
        )


def test_closed_day_probe_distinguishes_absent_file_from_access_failure():
    from trade_data.data.nse_bhavcopy import BhavcopyUnavailable, probe_special_session

    def missing(day):
        raise BhavcopyUnavailable("not published", not_found=True)

    def denied(day):
        raise BhavcopyUnavailable("access denied")

    assert probe_special_session(DAY, loader=missing) is None
    with pytest.raises(BhavcopyUnavailable):
        probe_special_session(DAY, loader=denied)
    source = parse_bhavcopy(archive(), DAY)
    assert probe_special_session(DAY, loader=lambda day: source) is source


def test_daily_worker_retries_failed_session_without_starving_later_sessions(monkeypatch, storage):
    from trade_data.storage.timescale import exchange_sessions_table

    exchange_sessions_table.create(storage.engine)
    day2 = date(2026, 10, 26)
    with storage.engine.begin() as c:
        for day in (DAY, day2):
            c.execute(
                exchange_sessions_table.insert().values(
                    exchange="NSE",
                    session_date=day,
                    is_trading_day=True,
                    source_url="official-test",
                    calendar_version="test",
                    validation_status="valid",
                    generated_at=datetime.now(UTC),
                )
            )
    monkeypatch.setattr(pipeline, "TimescaleStore", lambda url: storage)
    monkeypatch.setattr(pipeline, "latest_nse_session", lambda *a, **k: {"session_date": day2})
    monkeypatch.setattr(pipeline, "sleep", lambda n: None)
    monkeypatch.setattr(pipeline, "probe_special_session", lambda day: None)
    called = []

    def ingest(day, **kwargs):
        called.append(day)
        if day == DAY:
            raise ValueError("source not yet available")
        return {"session": str(day), "status": "complete"}

    monkeypatch.setattr(pipeline, "ingest_session", ingest)
    with pytest.raises(RuntimeError, match="sessions need attention"):
        pipeline.run_daily(settings=Settings(_env_file=None))
    assert called == [DAY, day2]
