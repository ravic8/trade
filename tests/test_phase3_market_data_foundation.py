from __future__ import annotations

import io
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from types import SimpleNamespace

import pandas as pd
import pytest
from sqlalchemy import create_engine, select

import trade_research.market_data.ingestion as market_data_ingestion
from trade_research.config import Settings
from trade_research.control_plane.tables import (
    market_data_quality_outcomes_table,
    market_data_replication_checkpoints_table,
)
from trade_research.market_data.adapters import yfinance_frame_to_candles
from trade_research.market_data.aggregation import (
    IntradayAggregationRequest,
    aggregate_nse_minute_candles,
    nse_bucket_expected_minutes,
    nse_bucket_start,
)
from trade_research.market_data.contracts import CandleInterval, MarketCandle, ProviderRequest
from trade_research.market_data.ingestion import (
    ValidatedMarketDataBatch,
    prepare_yfinance_batch,
    replicate_validated_batch,
)
from trade_research.market_data.quality import (
    DailyExpectedWindow,
    MarketDataQualityRepository,
    MarketDataQualityStatus,
    daily_missing_quality_outcomes,
    nse_minute_missing_quality_outcomes,
    validation_quality_outcomes,
)
from trade_research.market_data.raw_snapshots import RawSnapshotWriter
from trade_research.market_data.replication import (
    MarketDataReplicationError,
    assert_replication_matches,
    candle_batch_summary,
)
from trade_research.market_data.validation import (
    MarketDataValidationError,
    validate_candle_batch,
)
from trade_research.storage.clickhouse import ClickHouseMarketDataRepository
from trade_research.storage.object_store import ArtifactNamespace, ObjectArtifactStore


def _request(
    *,
    interval: CandleInterval = CandleInterval.ONE_DAY,
    retrieved_at: datetime = datetime(2026, 9, 5, 12, tzinfo=UTC),
) -> ProviderRequest:
    return ProviderRequest(
        request_id="request-1",
        provider="yfinance",
        exchange="NSE",
        interval=interval,
        window_start=datetime(2026, 9, 4, tzinfo=UTC),
        window_end=datetime(2026, 9, 6, tzinfo=UTC),
        provider_symbols=("RELIANCE.NS",),
        retrieved_at=retrieved_at,
        adapter_version="yfinance-v1",
        parameters={"auto_adjust": False},
    )


def _candle(
    request: ProviderRequest,
    *,
    high: str = "101",
    low: str = "99",
    timestamp: datetime | None = None,
) -> MarketCandle:
    return MarketCandle(
        instrument_id="NSE_EQ|RELIANCE",
        provider_symbol="RELIANCE.NS",
        symbol="RELIANCE",
        exchange="NSE",
        session_date=date(2026, 9, 5),
        open=Decimal("100"),
        high=Decimal(high),
        low=Decimal(low),
        close=Decimal("100.5"),
        volume=1000,
        currency="INR",
        provider="yfinance",
        provider_timestamp=request.retrieved_at,
        request_id=request.request_id,
        raw_artifact_id="artifact-1",
        adapter_version=request.adapter_version,
        interval=request.interval,
        timestamp=timestamp,
    )


def test_daily_validation_accepts_one_exact_duplicate_idempotently() -> None:
    request = _request()
    candle = _candle(request)

    result = validate_candle_batch(
        request,
        [candle, candle],
        eligible_sessions={date(2026, 9, 5)},
        observed_at=request.retrieved_at,
    )

    assert result.valid is True
    assert result.accepted == (candle,)
    assert result.rejected == (candle,)
    assert result.duplicate_count == 1


def test_daily_validation_rejects_invalid_ohlc_and_ineligible_session() -> None:
    request = _request()
    candle = _candle(request, high="99.5")

    result = validate_candle_batch(
        request,
        [candle],
        eligible_sessions={date(2026, 9, 4)},
        observed_at=request.retrieved_at,
    )

    assert result.valid is False
    assert result.accepted == ()
    assert {issue.code for issue in result.issues} == {
        "invalid_ohlc_range",
        "ineligible_session",
    }


def test_nse_minute_validation_enforces_timezone_and_regular_session() -> None:
    request = _request(interval=CandleInterval.ONE_MINUTE)
    valid = _candle(request, timestamp=datetime(2026, 9, 5, 3, 45, tzinfo=UTC))
    outside = _candle(request, timestamp=datetime(2026, 9, 5, 10, 0, tzinfo=UTC))

    valid_result = validate_candle_batch(
        request,
        [valid],
        eligible_sessions={date(2026, 9, 5)},
        observed_at=request.retrieved_at,
    )
    invalid_result = validate_candle_batch(
        request,
        [outside],
        eligible_sessions={date(2026, 9, 5)},
        observed_at=request.retrieved_at,
    )

    assert valid_result.valid is True
    assert invalid_result.valid is False
    assert [issue.code for issue in invalid_result.issues] == ["outside_exchange_session"]


def test_yfinance_adapter_produces_provider_independent_candles() -> None:
    request = _request(interval=CandleInterval.ONE_MINUTE)
    frame = pd.DataFrame(
        [
            {
                "Timestamp": datetime(2026, 9, 5, 3, 45, tzinfo=UTC),
                "InstrumentKey": "NSE_EQ|RELIANCE",
                "TradingSymbol": "RELIANCE.NS",
                "Symbol": "RELIANCE",
                "Exchange": "NSE",
                "Open": 100.0,
                "High": 101.0,
                "Low": 99.0,
                "Close": 100.5,
                "Volume": 1000,
            }
        ]
    )

    candles = yfinance_frame_to_candles(
        frame,
        request,
        currency="INR",
        raw_artifact_id="artifact-1",
    )

    assert len(candles) == 1
    assert candles[0].interval is CandleInterval.ONE_MINUTE
    assert candles[0].raw_artifact_id == "artifact-1"
    assert candles[0].currency == "INR"


class _MissingObject(RuntimeError):
    response = {
        "ResponseMetadata": {"HTTPStatusCode": 404},
        "Error": {"Code": "NoSuchKey"},
    }


class _ObjectClient:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], dict] = {}

    def head_object(self, *, Bucket: str, Key: str) -> dict:
        try:
            return self.objects[(Bucket, Key)]["head"]
        except KeyError as error:
            raise _MissingObject from error

    def put_object(self, *, Bucket: str, Key: str, Body, **kwargs) -> dict:
        content = Body.read()
        self.objects[(Bucket, Key)] = {
            "content": content,
            "head": {
                "ContentLength": len(content),
                "ContentType": kwargs["ContentType"],
                "Metadata": kwargs["Metadata"],
                "VersionId": "version-1",
            },
        }
        return {"VersionId": "version-1"}

    def get_object(self, *, Bucket: str, Key: str, **kwargs) -> dict:
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)]["content"])}


class _Registrar:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    def register(self, **kwargs) -> dict:
        self.calls.append(kwargs)
        return {"artifact_manifest_id": "manifest-1"}


def test_raw_snapshot_is_content_addressed_and_registered() -> None:
    client = _ObjectClient()
    registrar = _Registrar()
    store = ObjectArtifactStore(
        client,
        buckets={namespace: f"trade-{namespace.value}" for namespace in ArtifactNamespace},
        write_enabled=True,
    )
    request = _request()

    snapshot = RawSnapshotWriter(store, registrar=registrar).write(
        request,
        [{"symbol": "RELIANCE.NS", "close": 100.5}],
    )

    assert snapshot.artifact_manifest_id == "manifest-1"
    assert snapshot.sha256 in snapshot.storage_uri
    assert snapshot.storage_uri.startswith("s3://trade-raw/yfinance/nse/1d/2026/09/05/")
    assert registrar.calls[0]["artifact_type"] == "market_data_raw_response"


class _ClickHouseClient:
    def __init__(self) -> None:
        self.inserts: list[tuple[str, list[list[object]], list[str]]] = []

    def insert(self, table: str, data, column_names) -> None:
        self.inserts.append((table, data, column_names))

    def query(self, query: str, parameters=None) -> SimpleNamespace:
        return SimpleNamespace(result_rows=[(2,)], column_names=["count()"])

    def command(self, command: str, parameters=None) -> None:
        return None


def test_clickhouse_market_data_repository_routes_daily_and_minute_rows() -> None:
    client = _ClickHouseClient()
    repository = ClickHouseMarketDataRepository(client, write_enabled=True)
    daily_request = _request()
    minute_request = _request(interval=CandleInterval.ONE_MINUTE)

    inserted = repository.insert_validated(
        [
            _candle(daily_request),
            _candle(
                minute_request,
                timestamp=datetime(2026, 9, 5, 3, 45, tzinfo=UTC),
            ),
        ],
        source_run_id="run-1",
        version=1,
    )

    assert inserted == 2
    assert [call[0] for call in client.inserts] == [
        "research.ohlcv_daily",
        "research.ohlcv_intraday",
    ]
    assert client.inserts[1][2] == list(repository.INTRADAY_COLUMNS)
    assert repository.count_rows(
        exchange="NSE",
        interval="1m",
        source_run_id="run-1",
    ) == 2


def test_provider_request_rejects_naive_datetimes_and_invalid_window() -> None:
    aware = datetime(2026, 9, 5, tzinfo=UTC)
    try:
        ProviderRequest(
            request_id="request-1",
            provider="yfinance",
            exchange="NSE",
            interval=CandleInterval.ONE_DAY,
            window_start=datetime(2026, 9, 4),
            window_end=aware + timedelta(days=1),
            provider_symbols=("RELIANCE.NS",),
            retrieved_at=aware,
            adapter_version="v1",
        )
    except ValueError as exc:
        assert "window_start must be timezone-aware" in str(exc)
    else:
        raise AssertionError("Expected naive datetime to be rejected")


def test_phase3_settings_require_both_write_planes() -> None:
    with pytest.raises(ValueError, match="object-store and ClickHouse writes"):
        Settings(
            _env_file=None,
            phase3_market_data_enabled=True,
            clickhouse_enabled=True,
            clickhouse_write_enabled=True,
            clickhouse_password="writer-password",
        )


def test_quality_ledger_is_idempotent_and_preserves_validation_reasons() -> None:
    engine = create_engine("sqlite://")
    market_data_quality_outcomes_table.create(engine)
    request = _request()
    candle = _candle(request)
    validation = validate_candle_batch(
        request,
        [candle, candle],
        eligible_sessions={date(2026, 9, 5)},
        observed_at=request.retrieved_at,
    )
    outcomes = validation_quality_outcomes(
        request=request,
        source_run_id="run-1",
        result=validation,
    )
    repository = MarketDataQualityRepository(engine)

    assert repository.record(outcomes) == 2
    assert repository.record(outcomes) == 2
    with engine.connect() as connection:
        rows = connection.execute(select(market_data_quality_outcomes_table)).mappings().all()

    assert len(rows) == 2
    assert {row["status"] for row in rows} == {"valid", "duplicate"}
    assert {row["reason_code"] for row in rows} == {"accepted", "duplicate"}


def test_invalid_provider_batch_persists_quality_before_failing_closed() -> None:
    engine = create_engine("sqlite://")
    market_data_quality_outcomes_table.create(engine)
    frame = pd.DataFrame(
        [
            {
                "Date": date(2026, 9, 5),
                "InstrumentKey": "YF|RELIANCE.NS",
                "TradingSymbol": "RELIANCE.NS",
                "Symbol": "RELIANCE",
                "Exchange": "NSE",
                "Open": 100.0,
                "High": 99.0,
                "Low": 98.0,
                "Close": 100.5,
                "Volume": 1000,
            }
        ]
    )

    with pytest.raises(MarketDataValidationError, match="invalid_ohlc_range"):
        prepare_yfinance_batch(
            settings=Settings(_env_file=None),
            database_engine=engine,
            frame=frame,
            exchange="NSE",
            interval=CandleInterval.ONE_DAY,
            run_id="invalid-run",
            window_start=date(2026, 9, 5),
            window_end=date(2026, 9, 5),
            provider_symbols=("RELIANCE.NS",),
            canonical_instrument_ids={"RELIANCE.NS": "NSE_EQ|RELIANCE"},
            eligible_sessions={date(2026, 9, 5)},
            retrieved_at=datetime(2026, 9, 5, 12, tzinfo=UTC),
        )

    with engine.connect() as connection:
        quality = connection.execute(
            select(market_data_quality_outcomes_table)
        ).mappings().one()
    assert quality["status"] == "invalid"
    assert quality["reason_code"] == "invalid_ohlc_range"


def test_daily_missing_quality_classifies_provider_availability() -> None:
    request = _request()
    accepted = _candle(request)
    outcomes = daily_missing_quality_outcomes(
        request=request,
        source_run_id="run-1",
        windows=(
            DailyExpectedWindow(
                instrument_id=accepted.instrument_id,
                provider_symbol=accepted.provider_symbol,
                window_start=date(2026, 9, 4),
                window_end=date(2026, 9, 5),
            ),
            DailyExpectedWindow(
                instrument_id="NSE_EQ|MISSING",
                provider_symbol="MISSING.NS",
                window_start=date(2026, 9, 4),
                window_end=date(2026, 9, 4),
                provider_available=False,
                reason_code="empty_response",
            ),
        ),
        eligible_sessions={date(2026, 9, 4), date(2026, 9, 5)},
        accepted=(accepted,),
    )

    assert len(outcomes) == 2
    assert {outcome.status for outcome in outcomes} == {
        MarketDataQualityStatus.MISSING,
        MarketDataQualityStatus.PROVIDER_UNAVAILABLE,
    }
    assert {outcome.reason_code for outcome in outcomes} == {
        "candle_absent",
        "empty_response",
    }


def test_nse_minute_quality_records_each_missing_expected_candle() -> None:
    request = ProviderRequest(
        request_id="minute-request",
        provider="yfinance",
        exchange="NSE",
        interval=CandleInterval.ONE_MINUTE,
        window_start=datetime(2026, 9, 5, 3, 45, tzinfo=UTC),
        window_end=datetime(2026, 9, 5, 3, 47, tzinfo=UTC),
        provider_symbols=("RELIANCE.NS",),
        retrieved_at=datetime(2026, 9, 5, 12, tzinfo=UTC),
        adapter_version="yfinance-v1",
    )
    accepted = _candle(request, timestamp=request.window_start)

    outcomes = nse_minute_missing_quality_outcomes(
        request=request,
        source_run_id="run-1",
        canonical_instrument_ids={"RELIANCE.NS": accepted.instrument_id},
        eligible_sessions={date(2026, 9, 5)},
        accepted=(accepted,),
    )

    assert len(outcomes) == 1
    assert outcomes[0].candle_timestamp == datetime(2026, 9, 5, 3, 46, tzinfo=UTC)
    assert outcomes[0].status is MarketDataQualityStatus.MISSING


def test_replication_summary_detects_count_digest_and_watermark_mismatch() -> None:
    request = _request()
    source = candle_batch_summary([_candle(request)])

    with pytest.raises(MarketDataReplicationError, match="row_count"):
        assert_replication_matches(
            source,
            {
                "row_count": 0,
                "digest": "0" * 64,
                "watermark": date(2026, 9, 4),
            },
        )


def test_validated_batch_retains_request_identity_for_quality_and_replication() -> None:
    request = _request()
    candle = _candle(request)
    validation = validate_candle_batch(request, [candle])
    batch = ValidatedMarketDataBatch(
        request=request,
        frame=pd.DataFrame(),
        candles=(candle,),
        validation=validation,
        raw_snapshot=None,
    )

    assert batch.request.request_id == "request-1"


class _ReconcilingClickHouseClient:
    def __init__(self) -> None:
        self.rows: list[dict] = []

    def insert(self, _table: str, data, column_names) -> None:
        self.rows.extend(dict(zip(column_names, row, strict=True)) for row in data)

    def query(self, _query: str, parameters=None) -> SimpleNamespace:
        source_run_id = parameters["source_run_id"]
        rows = [row for row in self.rows if row["source_run_id"] == source_run_id]
        return SimpleNamespace(
            result_rows=[(row["content_sha256"], row["session_date"]) for row in rows],
            column_names=["content_sha256", "session_date"],
        )

    def command(self, _command: str, parameters=None) -> None:
        return None


def test_replication_records_reconciled_count_digest_and_watermark(monkeypatch) -> None:
    engine = create_engine("sqlite://")
    market_data_replication_checkpoints_table.create(engine)
    client = _ReconcilingClickHouseClient()
    monkeypatch.setattr(
        market_data_ingestion,
        "create_clickhouse_client",
        lambda _settings: client,
    )
    request = _request()
    candle = _candle(request)
    batch = ValidatedMarketDataBatch(
        request=request,
        frame=pd.DataFrame(),
        candles=(candle,),
        validation=validate_candle_batch(request, [candle]),
        raw_snapshot=None,
    )
    settings = SimpleNamespace(
        clickhouse_write_enabled=True,
        clickhouse_database="research",
    )

    inserted = replicate_validated_batch(
        settings,
        batch,
        database_engine=engine,
        source_run_id="run-1",
        source_store="postgresql",
        version=1,
    )

    assert inserted == 1
    with engine.connect() as connection:
        checkpoint = connection.execute(
            select(market_data_replication_checkpoints_table)
        ).mappings().one()
    assert checkpoint["status"] == "reconciled"
    assert checkpoint["source_store"] == "postgresql"
    assert checkpoint["source_row_count"] == checkpoint["destination_row_count"] == 1
    assert checkpoint["source_digest"] == checkpoint["destination_digest"]
    assert checkpoint["watermark_lag_seconds"] == 0


def test_replication_mismatch_is_persisted_and_fails_closed(monkeypatch) -> None:
    class MissingReplicaClient(_ReconcilingClickHouseClient):
        def query(self, _query: str, parameters=None) -> SimpleNamespace:
            return SimpleNamespace(
                result_rows=[],
                column_names=["content_sha256", "session_date"],
            )

    engine = create_engine("sqlite://")
    market_data_replication_checkpoints_table.create(engine)
    monkeypatch.setattr(
        market_data_ingestion,
        "create_clickhouse_client",
        lambda _settings: MissingReplicaClient(),
    )
    request = _request()
    candle = _candle(request)
    batch = ValidatedMarketDataBatch(
        request=request,
        frame=pd.DataFrame(),
        candles=(candle,),
        validation=validate_candle_batch(request, [candle]),
        raw_snapshot=None,
    )

    with pytest.raises(MarketDataReplicationError, match="row_count"):
        replicate_validated_batch(
            SimpleNamespace(
                clickhouse_write_enabled=True,
                clickhouse_database="research",
            ),
            batch,
            database_engine=engine,
            source_run_id="run-mismatch",
            source_store="postgresql",
            version=1,
        )

    with engine.connect() as connection:
        checkpoint = connection.execute(
            select(market_data_replication_checkpoints_table)
        ).mappings().one()
    assert checkpoint["status"] == "mismatch"
    assert checkpoint["destination_row_count"] == 0
    assert checkpoint["destination_digest"] is not None
    assert "row_count" in checkpoint["error_message"]


def _minute_candles(count: int, *, start: datetime) -> list[MarketCandle]:
    provider_request = ProviderRequest(
        request_id="minute-source",
        provider="yfinance",
        exchange="NSE",
        interval=CandleInterval.ONE_MINUTE,
        window_start=start,
        window_end=start + timedelta(minutes=count),
        provider_symbols=("RELIANCE.NS",),
        retrieved_at=start + timedelta(hours=8),
        adapter_version="yfinance-v1",
    )
    base = _candle(provider_request, timestamp=start)
    candles: list[MarketCandle] = []
    for offset in range(count):
        open_price = Decimal(100 + offset)
        candles.append(
            replace(
                base,
                timestamp=start + timedelta(minutes=offset),
                open=open_price,
                high=open_price + Decimal("2"),
                low=open_price - Decimal("1"),
                close=open_price + Decimal("1"),
                volume=offset + 1,
            )
        )
    return candles


def test_python_golden_aggregation_uses_nse_session_anchor_and_ohlcv_rules() -> None:
    start = datetime(2026, 9, 5, 3, 45, tzinfo=UTC)
    request = IntradayAggregationRequest(
        instrument_id="NSE_EQ|RELIANCE",
        interval=CandleInterval.FIVE_MINUTES,
        window_start=start,
        window_end=start + timedelta(minutes=5),
    )

    result = aggregate_nse_minute_candles(request, _minute_candles(5, start=start))

    assert len(result) == 1
    candle = result[0]
    assert candle.candle_timestamp == start
    assert candle.open == Decimal("100")
    assert candle.high == Decimal("106")
    assert candle.low == Decimal("99")
    assert candle.close == Decimal("105")
    assert candle.volume == 15
    assert candle.source_rows == candle.expected_source_rows == 5
    assert candle.complete is True


def test_python_aggregation_excludes_or_marks_a_bucket_with_a_missing_minute() -> None:
    start = datetime(2026, 9, 5, 3, 45, tzinfo=UTC)
    candles = _minute_candles(5, start=start)
    complete_only = IntradayAggregationRequest(
        instrument_id="NSE_EQ|RELIANCE",
        interval=CandleInterval.FIVE_MINUTES,
        window_start=start,
        window_end=start + timedelta(minutes=5),
    )
    include_partial = replace(complete_only, complete_only=False)

    assert aggregate_nse_minute_candles(complete_only, candles[:-1]) == ()
    partial = aggregate_nse_minute_candles(include_partial, candles[:-1])

    assert len(partial) == 1
    assert partial[0].complete is False
    assert partial[0].source_rows == 4
    assert partial[0].expected_source_rows == 5


def test_final_nse_hour_bucket_expects_only_fifteen_minutes() -> None:
    final_bucket = datetime(2026, 9, 5, 9, 45, tzinfo=UTC)

    assert nse_bucket_start(final_bucket, CandleInterval.ONE_HOUR) == final_bucket
    assert nse_bucket_expected_minutes(final_bucket, CandleInterval.ONE_HOUR) == 15

    request = IntradayAggregationRequest(
        instrument_id="NSE_EQ|RELIANCE",
        interval=CandleInterval.ONE_HOUR,
        window_start=final_bucket,
        window_end=final_bucket + timedelta(minutes=15),
    )
    result = aggregate_nse_minute_candles(
        request,
        _minute_candles(15, start=final_bucket),
    )
    assert len(result) == 1
    assert result[0].complete is True
    assert result[0].expected_source_rows == 15


@pytest.mark.parametrize(
    ("interval", "expected"),
    [
        (CandleInterval.FIVE_MINUTES, datetime(2026, 9, 5, 4, 10, tzinfo=UTC)),
        (CandleInterval.FIFTEEN_MINUTES, datetime(2026, 9, 5, 4, 0, tzinfo=UTC)),
        (CandleInterval.THIRTY_MINUTES, datetime(2026, 9, 5, 3, 45, tzinfo=UTC)),
        (CandleInterval.ONE_HOUR, datetime(2026, 9, 5, 3, 45, tzinfo=UTC)),
    ],
)
def test_all_supported_intervals_anchor_to_nse_open(
    interval: CandleInterval,
    expected: datetime,
) -> None:
    assert nse_bucket_start(
        datetime(2026, 9, 5, 4, 14, tzinfo=UTC),
        interval,
    ) == expected


class _AggregateClickHouseClient:
    def __init__(self) -> None:
        self.query_text = ""
        self.parameters: dict = {}

    def query(self, query: str, parameters=None) -> SimpleNamespace:
        self.query_text = query
        self.parameters = parameters
        return SimpleNamespace(
            column_names=[
                "workspace_id",
                "instrument_id",
                "exchange",
                "symbol",
                "provider_symbol",
                "currency",
                "candle_timestamp",
                "session_date",
                "interval",
                "open",
                "high",
                "low",
                "close",
                "volume",
                "provider",
                "provider_timestamp",
                "source_rows",
                "expected_source_rows",
                "complete",
                "source_content_digests",
                "source_run_ids",
                "raw_artifact_ids",
            ],
            result_rows=[
                (
                    "default",
                    "NSE_EQ|RELIANCE",
                    "NSE",
                    "RELIANCE",
                    "RELIANCE.NS",
                    "INR",
                    datetime(2026, 9, 5, 3, 45, tzinfo=UTC),
                    date(2026, 9, 5),
                    "15m",
                    Decimal("100"),
                    Decimal("105"),
                    Decimal("99"),
                    Decimal("104"),
                    1000,
                    "yfinance",
                    datetime(2026, 9, 5, 12, tzinfo=UTC),
                    15,
                    15,
                    1,
                    ["a" * 64],
                    ["run-1"],
                    ["artifact-1"],
                )
            ],
        )

    def insert(self, _table: str, _data, _column_names) -> None:
        return None

    def command(self, _command: str, parameters=None) -> None:
        return None


def test_clickhouse_aggregation_is_on_demand_from_validated_one_minute_rows() -> None:
    client = _AggregateClickHouseClient()
    repository = ClickHouseMarketDataRepository(client)
    start = datetime(2026, 9, 5, 3, 45, tzinfo=UTC)
    request = IntradayAggregationRequest(
        instrument_id="NSE_EQ|RELIANCE",
        interval=CandleInterval.FIFTEEN_MINUTES,
        window_start=start,
        window_end=start + timedelta(hours=1),
    )

    rows = repository.aggregate_nse_intraday(request)

    assert len(rows) == 1
    assert rows[0].interval is CandleInterval.FIFTEEN_MINUTES
    assert rows[0].complete is True
    assert rows[0].source_run_ids == ("run-1",)
    assert "FROM research.ohlcv_intraday FINAL" in client.query_text
    assert "interval = '1m'" in client.query_text
    assert "09:15:00" in client.query_text
    assert client.parameters["bucket_minutes"] == 15
    assert client.parameters["complete_only"] == 1
