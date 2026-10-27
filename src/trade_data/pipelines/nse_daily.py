"""Session-atomic NSE ingestion with durable source/replica completeness checks."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal
from time import sleep
from uuid import NAMESPACE_URL, uuid4, uuid5

from sqlalchemy import select, text
from sqlalchemy.dialects.postgresql import insert

from trade_data.config import Settings, get_settings
from trade_data.data.nse_bhavcopy import SOURCE, Bhavcopy, fetch_bhavcopy, rolling_year_start
from trade_data.market_data.ingestion import replicate_validated_batch
from trade_data.market_data.partition_reconciliation import (
    DailyReplicaRow,
    PostgreSQLDailyPartitionRepository,
    compare_daily_partitions,
)
from trade_data.operations.nse_authority import prepare_nse_batch
from trade_data.storage.clickhouse import ClickHouseMarketDataRepository, create_clickhouse_client
from trade_data.storage.nse import nse_sessions
from trade_data.storage.timescale import TimescaleStore, symbols_table


def _identities(store: TimescaleStore, source: Bhavcopy) -> dict:
    """Reuse stable IDs; never silently attach a reused ticker to a different issuer."""
    with store.engine.connect() as c:
        rows = c.execute(select(symbols_table).where(symbols_table.c.exchange == "NSE")).mappings()
        symbols = {str(r["symbol"]).removesuffix(".NS"): dict(r) for r in rows}
    prior = PostgreSQLDailyPartitionRepository(store.engine).read(
        window_start=source.session, window_end=source.session, provider=SOURCE, exchange="NSE"
    )
    by_symbol = {r.provider_symbol.removesuffix(".NS"): r for r in prior}
    identities = {}
    for row in source.frame.itertuples():
        existing = symbols.get(row.exchange_symbol)
        if existing and existing.get("canonical_instrument_id"):
            anchor = existing.get("source_identity")
            reason = "exact_isin"
            if not anchor:
                # A missing legacy identity is established from the actual official
                # equity record, not inferred from another data provider.
                with store.engine.begin() as c:
                    c.execute(
                        symbols_table.update()
                        .where(symbols_table.c.symbol == existing["symbol"])
                        .where(symbols_table.c.exchange == "NSE")
                        .where(symbols_table.c.source_identity.is_(None))
                        .values(
                            source_identity=row.isin,
                            official_security_type="fully_paid_equity:" + row.series,
                        )
                    )
                reason = "official_identity_established"
            elif anchor != row.isin:
                # NSDL issuer code is positions 4–7. Preserve raw dated ISINs and
                # prices while retaining a stock alias across same-issuer equity
                # serial changes. Never attach a reused ticker to another issuer.
                same_issuer_equity = (
                    anchor[:7] == row.isin[:7] and anchor[7:9] == row.isin[7:9] == "01"
                )
                old = by_symbol.get(row.exchange_symbol)
                audited = (
                    old
                    and old.volume == row.volume
                    and all(
                        abs(Decimal(str(getattr(row, f))) - getattr(old, f))
                        <= Decimal("0.00000001")
                        for f in ("open", "high", "low", "close")
                    )
                )
                if not same_issuer_equity and not audited:
                    raise ValueError(f"NSE identity changed; review ISIN: {row.exchange_symbol}")
                reason = (
                    "same_issuer_equity_serial" if same_issuer_equity else "prior_audited_identity"
                )
            key = existing["provider_instrument_key"]
            canonical = existing["canonical_instrument_id"]
        else:
            reason = "new_official_equity"
            anchor = None
            key = "NSE|" + row.isin
            canonical = str(uuid5(NAMESPACE_URL, "nse-equity:" + row.isin))
            # New listings/previously uncollected equities are discovered from the
            # actual file, independent of the legacy accepted snapshot denominator.
            with store.engine.begin() as c:
                stmt = insert(symbols_table).values(
                    symbol=row.exchange_symbol,
                    exchange="NSE",
                    yahoo_symbol=row.exchange_symbol,
                    name=row.name,
                    currency="INR",
                    source=SOURCE,
                    source_identity=row.isin,
                    canonical_instrument_id=canonical,
                    provider_instrument_key=key,
                    is_active=True,
                    instrument_type="common_stock",
                    fetched_at=datetime.now(UTC),
                    first_seen_at=datetime.now(UTC),
                    last_seen_at=datetime.now(UTC),
                    official_security_type="fully_paid_equity:" + row.series,
                )
                c.execute(stmt.on_conflict_do_nothing(index_elements=["symbol", "exchange"]))
        identities[row.exchange_symbol] = {
            "provider_instrument_key": key,
            "canonical_instrument_id": canonical,
            "source_identity": row.isin,
            "anchor_isin": anchor,
            "identity_reason": reason,
            "provider_symbol": existing.get("yahoo_symbol") if existing else row.exchange_symbol,
        }
    if len({r["canonical_instrument_id"] for r in identities.values()}) != len(identities):
        raise ValueError("NSE file maps multiple securities to the same canonical identity")
    return identities


def _replica_rows(rows):
    return [
        DailyReplicaRow(
            **{
                **{
                    k: r[k]
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
                },
                "provider": r["source"],
            }
        )
        for r in rows
    ]


def ingest_session(session: date, *, settings: Settings | None = None, loader=fetch_bhavcopy):
    settings = settings or get_settings()
    if not settings.clickhouse_write_enabled:
        raise ValueError("NSE publication requires PostgreSQL and ClickHouse writes")
    store = TimescaleStore(settings.database_url)
    try:
        # Session-level lock prevents concurrent historical/daily imports and conflicting proofs.
        with store.engine.connect() as lock:
            locked = lock.execute(
                text("SELECT pg_try_advisory_lock(:key)"), {"key": 726000000 + session.toordinal()}
            ).scalar()
            if not locked:
                return {"session": str(session), "status": "already_running"}
            try:
                return _ingest(store, settings, loader(session))
            except Exception as exc:
                _checkpoint(
                    store,
                    session,
                    "failed",
                    details={"error_type": type(exc).__name__, "message": str(exc)[:400]},
                )
                raise
            finally:
                lock.execute(
                    text("SELECT pg_advisory_unlock(:key)"),
                    {"key": 726000000 + session.toordinal()},
                )

    finally:
        store.engine.dispose()


def _checkpoint(store, session, status, **values):
    with store.engine.begin() as c:
        stmt = insert(nse_sessions).values(
            session_date=session, status=status, updated_at=datetime.now(UTC), **values
        )
        c.execute(
            stmt.on_conflict_do_update(
                index_elements=["session_date"],
                set_={k: getattr(stmt.excluded, k) for k in ("status", "updated_at", *values)},
            )
        )


def _ingest(store, settings, source):
    identities = _identities(store, source)
    frame = source.frame.copy()
    frame["symbol"] = frame.exchange_symbol.map(lambda s: identities[s]["provider_symbol"] or s)
    identities = {(v["provider_symbol"] or k): v for k, v in identities.items()}
    frame["current_isin"] = frame["isin"]
    frame["exchange"] = "NSE"
    frame["source"] = SOURCE
    frame["price_basis"] = "as_traded"
    frame["volume_basis"] = "exchange_reported_shares"
    frame["evidence_url"] = source.url
    frame["evidence_sha256"] = source.sha256
    now = datetime.now(UTC)
    run = "nse-" + str(uuid4())
    batch = prepare_nse_batch(
        frame,
        identities=identities,
        settings=settings,
        database_engine=store.engine,
        run_id=run,
        retrieved_at=now,
        eligible_sessions={source.session},
    )
    details = {
        "total_source_records": source.total_records,
        "excluded": source.excluded,
        "series_counts": source.frame.series.value_counts().to_dict(),
        "zero_volume_records": int(source.frame.volume.eq(0).sum()),
        "source_run_id": run,
        "identity_bindings": [
            {
                "symbol": symbol,
                "anchor_isin": value["anchor_isin"],
                "source_isin": value["source_identity"],
                "reason": value["identity_reason"],
            }
            for symbol, value in identities.items()
            if value["identity_reason"] != "exact_isin"
        ],
    }
    _checkpoint(
        store,
        source.session,
        "publishing",
        source_url=source.url,
        source_sha256=source.sha256,
        expected_rows=len(frame),
        details=details,
    )
    # Existing source-labelled history is retained. UPSERT only official-source identities.
    store.upsert_daily_ohlcv(batch.frame, exchange="NSE", source=SOURCE)
    replicate_validated_batch(
        settings,
        batch,
        database_engine=store.engine,
        source_run_id=run,
        source_store="postgresql",
        version=int(now.timestamp() * 1_000_000),
    )
    pg = PostgreSQLDailyPartitionRepository(store.engine).read(
        window_start=source.session, window_end=source.session, provider=SOURCE, exchange="NSE"
    )
    ch = ClickHouseMarketDataRepository(
        create_clickhouse_client(settings), database=settings.clickhouse_database
    )
    replica = _replica_rows(
        ch.read_daily_partition(
            workspace_id="default",
            provider=SOURCE,
            exchange="NSE",
            window_start=source.session,
            window_end=source.session,
        )
    )
    # Compare BOTH replicas against the downloaded exchange records, not only each other.
    expected = [
        DailyReplicaRow(
            instrument_id=c.instrument_id,
            provider_symbol=c.provider_symbol,
            symbol=c.symbol,
            exchange=c.exchange,
            session_date=c.session_date,
            open=c.open,
            high=c.high,
            low=c.low,
            close=c.close,
            volume=c.volume,
            provider=c.provider,
            provider_timestamp=c.provider_timestamp,
        )
        for c in batch.candles
    ]
    p, h = compare_daily_partitions(expected, pg), compare_daily_partitions(expected, replica)
    status = "complete" if p.reconciled and h.reconciled else "mismatch"
    details["postgres_divergent"] = len(p.divergent_identities)
    details["clickhouse_divergent"] = len(h.divergent_identities)
    details["postgres_unexpected"] = len(p.unexpected_identities)
    details["clickhouse_unexpected"] = len(h.unexpected_identities)
    _checkpoint(
        store,
        source.session,
        status,
        source_url=source.url,
        source_sha256=source.sha256,
        source_digest=p.source_digest,
        expected_rows=len(expected),
        postgres_rows=len(pg),
        clickhouse_rows=len(replica),
        details=details,
    )
    if status != "complete":
        raise ValueError(f"NSE source/replica coverage mismatch on {source.session}")
    return {
        "session": str(source.session),
        "status": status,
        "rows": len(expected),
        "source_sha256": source.sha256,
        "digest": p.source_digest,
    }


def run_daily(*, settings: Settings | None = None):
    settings = settings or get_settings()
    store = TimescaleStore(settings.database_url)
    last = store.latest_provider_eligible_exchange_session(
        "NSE", provider_grace_minutes=settings.nse_publication_grace_minutes
    )
    if not last:
        raise ValueError("No validated completed NSE exchange session; refresh calendar")
    end = last["session_date"]
    # Catch missed/special sessions as well as retrying recent source corrections.
    start = end - timedelta(days=settings.nse_retry_lookback_days)
    with store.engine.connect() as c:
        sessions = (
            c.execute(
                text("""SELECT session_date FROM exchange_sessions
            WHERE exchange='NSE' AND is_trading_day AND validation_status LIKE 'valid%'
            AND session_date BETWEEN :start AND :end ORDER BY session_date"""),
                {"start": start, "end": end},
            )
            .scalars()
            .all()
        )
    results, failures = [], []
    for day in sessions:
        with store.engine.connect() as c:
            status = c.execute(
                select(nse_sessions.c.status).where(nse_sessions.c.session_date == day)
            ).scalar()
        if status == "complete" and day < end:
            continue
        try:
            results.append(ingest_session(day, settings=settings))
        except Exception:
            failures.append(str(day))
        sleep(settings.nse_request_interval_seconds)
    if failures:
        raise RuntimeError("NSE sessions need attention: " + ", ".join(failures))
    return results


def repair_year(*, settings: Settings | None = None):
    settings = settings or get_settings()
    store = TimescaleStore(settings.database_url)
    end = store.latest_provider_eligible_exchange_session(
        "NSE", provider_grace_minutes=settings.nse_publication_grace_minutes
    )["session_date"]
    start = rolling_year_start(end)
    with store.engine.connect() as c:
        days = (
            c.execute(
                text("""SELECT session_date FROM exchange_sessions
            WHERE exchange='NSE' AND is_trading_day AND validation_status LIKE 'valid%'
            AND session_date BETWEEN :start AND :end ORDER BY session_date"""),
                {"start": start, "end": end},
            )
            .scalars()
            .all()
        )
    failures = []
    for day in days:
        with store.engine.connect() as c:
            status = c.execute(
                select(nse_sessions.c.status).where(nse_sessions.c.session_date == day)
            ).scalar()
        if status == "complete":
            continue
        try:
            result = ingest_session(day, settings=settings)
        except Exception as exc:
            failures.append(str(day))
            result = {"session": str(day), "status": "failed", "error_type": type(exc).__name__}
        print(json.dumps(result), flush=True)
        sleep(settings.nse_request_interval_seconds)

    if failures:
        raise RuntimeError("NSE year repair incomplete: " + ", ".join(failures))
