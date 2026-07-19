from __future__ import annotations

import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid4, uuid5

from sqlalchemy import Table, func, select

from trade_research.config import Settings, get_settings
from trade_research.storage.timescale import (
    TimescaleStore,
    adaptive_rate_state_table,
    exchange_sessions_table,
    ingestion_runs_table,
    ohlcv_daily_table,
    pipeline_work_items_table,
    symbol_lifecycle_events_table,
    symbols_table,
)

DEFAULT_BIGQUERY_ENTITIES = (
    "ohlcv_daily",
    "symbols",
    "exchange_sessions",
    "pipeline_health",
    "ingestion_runs",
    "provider_health",
    "universe_lifecycle",
)


@dataclass(frozen=True)
class FieldSpec:
    name: str
    field_type: str
    mode: str = "NULLABLE"


@dataclass(frozen=True)
class EntitySpec:
    name: str
    source_table: Table
    natural_keys: tuple[str, ...]
    fields: tuple[FieldSpec, ...]
    exchange_field: str | None = "exchange"
    date_field: str | None = None
    watermark_field: str | None = None
    partition_field: str | None = None
    cluster_fields: tuple[str, ...] = ()
    require_partition_filter: bool = False


ENTITY_SPECS: dict[str, EntitySpec] = {
    "ohlcv_daily": EntitySpec(
        name="ohlcv_daily",
        source_table=ohlcv_daily_table,
        natural_keys=("instrument_key", "source", "date"),
        date_field="date",
        watermark_field="date",
        partition_field="date",
        cluster_fields=("exchange", "instrument_key", "source"),
        require_partition_filter=True,
        fields=tuple(
            FieldSpec(name, field_type, mode)
            for name, field_type, mode in (
                ("instrument_key", "STRING", "REQUIRED"),
                ("source", "STRING", "REQUIRED"),
                ("date", "DATE", "REQUIRED"),
                ("symbol", "STRING", "REQUIRED"),
                ("exchange", "STRING", "REQUIRED"),
                ("open", "FLOAT64", "REQUIRED"),
                ("high", "FLOAT64", "REQUIRED"),
                ("low", "FLOAT64", "REQUIRED"),
                ("close", "FLOAT64", "REQUIRED"),
                ("volume", "INT64", "REQUIRED"),
                ("open_interest", "INT64", "NULLABLE"),
                ("fetched_at", "TIMESTAMP", "REQUIRED"),
                ("quality_status", "STRING", "REQUIRED"),
            )
        ),
    ),
    "symbols": EntitySpec(
        name="symbols",
        source_table=symbols_table,
        natural_keys=("symbol", "exchange"),
        watermark_field="fetched_at",
        cluster_fields=("exchange", "canonical_instrument_id", "source"),
        fields=tuple(
            FieldSpec(name, field_type, mode)
            for name, field_type, mode in (
                ("symbol", "STRING", "REQUIRED"),
                ("exchange", "STRING", "REQUIRED"),
                ("yahoo_symbol", "STRING", "NULLABLE"),
                ("name", "STRING", "NULLABLE"),
                ("currency", "STRING", "NULLABLE"),
                ("source", "STRING", "REQUIRED"),
                ("canonical_instrument_id", "STRING", "NULLABLE"),
                ("provider_instrument_key", "STRING", "NULLABLE"),
                ("is_active", "BOOL", "REQUIRED"),
                ("listing_status", "STRING", "REQUIRED"),
                ("pipeline_eligibility", "STRING", "REQUIRED"),
                ("provider_status", "STRING", "REQUIRED"),
                ("instrument_type", "STRING", "REQUIRED"),
                ("reconciliation_status", "STRING", "REQUIRED"),
                ("first_seen_at", "TIMESTAMP", "NULLABLE"),
                ("last_seen_at", "TIMESTAMP", "NULLABLE"),
                ("inactive_at", "TIMESTAMP", "NULLABLE"),
                ("fetched_at", "TIMESTAMP", "REQUIRED"),
            )
        ),
    ),
    "exchange_sessions": EntitySpec(
        name="exchange_sessions",
        source_table=exchange_sessions_table,
        natural_keys=("exchange", "session_date"),
        date_field="session_date",
        watermark_field="session_date",
        cluster_fields=("exchange", "is_trading_day"),
        fields=tuple(
            FieldSpec(name, field_type, mode)
            for name, field_type, mode in (
                ("exchange", "STRING", "REQUIRED"),
                ("session_date", "DATE", "REQUIRED"),
                ("is_trading_day", "BOOL", "REQUIRED"),
                ("market_open_utc", "TIMESTAMP", "NULLABLE"),
                ("market_close_utc", "TIMESTAMP", "NULLABLE"),
                ("is_early_close", "BOOL", "REQUIRED"),
                ("source_url", "STRING", "REQUIRED"),
                ("calendar_version", "STRING", "REQUIRED"),
                ("validation_status", "STRING", "REQUIRED"),
                ("generated_at", "TIMESTAMP", "REQUIRED"),
            )
        ),
    ),
    "pipeline_health": EntitySpec(
        name="pipeline_health",
        source_table=pipeline_work_items_table,
        natural_keys=("work_item_id",),
        date_field="window_end",
        watermark_field="updated_at",
        cluster_fields=("exchange", "provider", "status"),
        fields=tuple(
            FieldSpec(name, field_type, mode)
            for name, field_type, mode in (
                ("work_item_id", "STRING", "REQUIRED"),
                ("work_type", "STRING", "REQUIRED"),
                ("provider", "STRING", "REQUIRED"),
                ("exchange", "STRING", "REQUIRED"),
                ("canonical_instrument_id", "STRING", "REQUIRED"),
                ("provider_symbol", "STRING", "REQUIRED"),
                ("interval", "STRING", "REQUIRED"),
                ("window_start", "DATE", "REQUIRED"),
                ("window_end", "DATE", "REQUIRED"),
                ("status", "STRING", "REQUIRED"),
                ("attempt_count", "INT64", "REQUIRED"),
                ("max_attempts", "INT64", "REQUIRED"),
                ("run_id", "STRING", "NULLABLE"),
                ("last_error_code", "STRING", "NULLABLE"),
                ("last_error_message", "STRING", "NULLABLE"),
                ("created_at", "TIMESTAMP", "REQUIRED"),
                ("updated_at", "TIMESTAMP", "REQUIRED"),
                ("completed_at", "TIMESTAMP", "NULLABLE"),
            )
        ),
    ),
    "ingestion_runs": EntitySpec(
        name="ingestion_runs",
        source_table=ingestion_runs_table,
        natural_keys=("run_id",),
        watermark_field="started_at",
        cluster_fields=("exchange", "source", "status"),
        fields=tuple(
            FieldSpec(name, field_type, mode)
            for name, field_type, mode in (
                ("run_id", "STRING", "REQUIRED"),
                ("job_name", "STRING", "REQUIRED"),
                ("status", "STRING", "REQUIRED"),
                ("exchange", "STRING", "REQUIRED"),
                ("source", "STRING", "REQUIRED"),
                ("started_at", "TIMESTAMP", "REQUIRED"),
                ("finished_at", "TIMESTAMP", "NULLABLE"),
                ("items_requested", "INT64", "REQUIRED"),
                ("items_processed", "INT64", "REQUIRED"),
                ("items_succeeded", "INT64", "REQUIRED"),
                ("items_failed", "INT64", "REQUIRED"),
                ("error_message", "STRING", "NULLABLE"),
                ("run_metadata", "JSON", "REQUIRED"),
            )
        ),
    ),
    "provider_health": EntitySpec(
        name="provider_health",
        source_table=adaptive_rate_state_table,
        natural_keys=("provider",),
        exchange_field=None,
        watermark_field="updated_at",
        cluster_fields=("provider", "circuit_state"),
        fields=tuple(
            FieldSpec(name, field_type, mode)
            for name, field_type, mode in (
                ("provider", "STRING", "REQUIRED"),
                ("current_rpm", "INT64", "REQUIRED"),
                ("last_safe_rpm", "INT64", "NULLABLE"),
                ("minimum_rpm", "INT64", "REQUIRED"),
                ("maximum_rpm", "INT64", "REQUIRED"),
                ("current_concurrency", "INT64", "REQUIRED"),
                ("consecutive_healthy_windows", "INT64", "REQUIRED"),
                ("circuit_state", "STRING", "REQUIRED"),
                ("cooldown_until", "TIMESTAMP", "NULLABLE"),
                ("last_429_at", "TIMESTAMP", "NULLABLE"),
                ("recent_error_rate", "FLOAT64", "REQUIRED"),
                ("latency_baseline_ms", "FLOAT64", "NULLABLE"),
                ("updated_at", "TIMESTAMP", "REQUIRED"),
            )
        ),
    ),
    "universe_lifecycle": EntitySpec(
        name="universe_lifecycle",
        source_table=symbol_lifecycle_events_table,
        natural_keys=("event_id",),
        watermark_field="created_at",
        cluster_fields=("exchange", "event_type", "canonical_instrument_id"),
        fields=tuple(
            FieldSpec(name, field_type, mode)
            for name, field_type, mode in (
                ("event_id", "STRING", "REQUIRED"),
                ("canonical_instrument_id", "STRING", "REQUIRED"),
                ("exchange", "STRING", "REQUIRED"),
                ("event_type", "STRING", "REQUIRED"),
                ("old_value", "JSON", "NULLABLE"),
                ("new_value", "JSON", "NULLABLE"),
                ("snapshot_id", "STRING", "NULLABLE"),
                ("created_at", "TIMESTAMP", "REQUIRED"),
            )
        ),
    ),
}


@dataclass(frozen=True)
class MergeResult:
    job_id: str
    inserted_rows: int
    updated_rows: int
    rejected_rows: int = 0


@dataclass(frozen=True)
class ReconciliationResult:
    row_count: int
    watermark: str | None
    schema_drift: dict[str, Any] = field(default_factory=dict)


class BigQueryGateway(Protocol):
    def ensure_foundation(self, specs: Iterable[EntitySpec]) -> None: ...

    def merge_rows(
        self,
        spec: EntitySpec,
        rows: Sequence[Mapping[str, Any]],
        *,
        load_id: str,
    ) -> MergeResult: ...

    def reconcile(
        self,
        spec: EntitySpec,
        *,
        exchange: str | None,
        start_date: date | None,
        end_date: date | None,
    ) -> ReconciliationResult: ...


@dataclass(frozen=True)
class BigQuerySyncResult:
    run_id: str
    status: str
    source_row_count: int = 0
    destination_row_count: int = 0
    count_difference: int = 0
    inserted_rows: int = 0
    updated_rows: int = 0
    rejected_rows: int = 0
    retry_count: int = 0
    bigquery_job_id: str | None = None
    error_details: str | None = None
    partition_statuses: dict[str, str] = field(default_factory=dict)


class GoogleBigQueryGateway:
    """Real BigQuery staging/MERGE client. Imported lazily behind the feature flag."""

    def __init__(self, settings: Settings) -> None:
        from google.cloud import bigquery

        credentials = None
        if settings.bigquery_auth_method == "service_account_file":
            from google.oauth2 import service_account

            credentials = service_account.Credentials.from_service_account_file(
                str(settings.bigquery_credentials_path)
            )
        self._bigquery = bigquery
        self._client = bigquery.Client(
            project=settings.bigquery_project_id,
            credentials=credentials,
            location=settings.bigquery_location,
        )
        self.project_id = str(settings.bigquery_project_id)
        self.dataset = settings.bigquery_dataset
        self.location = settings.bigquery_location

    @property
    def dataset_id(self) -> str:
        return f"{self.project_id}.{self.dataset}"

    def ensure_foundation(self, specs: Iterable[EntitySpec]) -> None:
        from google.api_core.exceptions import NotFound

        try:
            self._client.get_dataset(self.dataset_id)
        except NotFound as exc:
            raise RuntimeError(
                f"BigQuery dataset {self.dataset_id} does not exist; create it in "
                f"{self.location} before enabling synchronization."
            ) from exc
        for spec in specs:
            table = self._bigquery.Table(
                f"{self.dataset_id}.{spec.name}",
                schema=[
                    self._bigquery.SchemaField(field.name, field.field_type, mode=field.mode)
                    for field in spec.fields
                ],
            )
            if spec.partition_field:
                table.time_partitioning = self._bigquery.TimePartitioning(
                    type_=self._bigquery.TimePartitioningType.MONTH,
                    field=spec.partition_field,
                    require_partition_filter=spec.require_partition_filter,
                )
            if spec.cluster_fields:
                table.clustering_fields = list(spec.cluster_fields)
            self._client.create_table(table, exists_ok=True)

    def merge_rows(
        self,
        spec: EntitySpec,
        rows: Sequence[Mapping[str, Any]],
        *,
        load_id: str,
    ) -> MergeResult:
        if not rows:
            return MergeResult(job_id="no-op", inserted_rows=0, updated_rows=0)
        staging_name = f"_{spec.name}_staging_{_safe_job_suffix(load_id)}"
        staging_id = f"{self.dataset_id}.{staging_name}"
        target_id = f"{self.dataset_id}.{spec.name}"
        schema = [
            self._bigquery.SchemaField(field.name, field.field_type, mode=field.mode)
            for field in spec.fields
        ]
        staging = self._bigquery.Table(staging_id, schema=schema)
        staging.expires = datetime.now(UTC) + timedelta(days=1)
        self._client.create_table(staging, exists_ok=True)
        load_config = self._bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=self._bigquery.WriteDisposition.WRITE_TRUNCATE,
        )
        normalized = [_json_compatible(dict(row)) for row in rows]
        try:
            load_job = self._client.load_table_from_json(
                normalized,
                staging_id,
                job_config=load_config,
                location=self.location,
            )
            load_job.result()
            key_match = " AND ".join(f"T.`{key}` = S.`{key}`" for key in spec.natural_keys)
            classification = self._client.query(
                f"""
                SELECT COUNTIF(T.`{spec.natural_keys[0]}` IS NULL) AS inserted_rows,
                       COUNTIF(T.`{spec.natural_keys[0]}` IS NOT NULL) AS updated_rows
                FROM `{staging_id}` AS S
                LEFT JOIN `{target_id}` AS T ON {key_match}
                """,
                location=self.location,
            ).result()
            counts = next(iter(classification))
            columns = [field.name for field in spec.fields]
            updates = [column for column in columns if column not in spec.natural_keys]
            update_sql = ", ".join(f"`{column}` = S.`{column}`" for column in updates)
            insert_columns = ", ".join(f"`{column}`" for column in columns)
            insert_values = ", ".join(f"S.`{column}`" for column in columns)
            merge_job = self._client.query(
                f"""
                MERGE `{target_id}` AS T
                USING `{staging_id}` AS S
                ON {key_match}
                WHEN MATCHED THEN UPDATE SET {update_sql}
                WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
                """,
                location=self.location,
            )
            merge_job.result()
            return MergeResult(
                job_id=str(merge_job.job_id),
                inserted_rows=int(counts["inserted_rows"]),
                updated_rows=int(counts["updated_rows"]),
            )
        finally:
            self._client.delete_table(staging_id, not_found_ok=True)

    def reconcile(
        self,
        spec: EntitySpec,
        *,
        exchange: str | None,
        start_date: date | None,
        end_date: date | None,
    ) -> ReconciliationResult:
        clauses: list[str] = []
        parameters: list[Any] = []
        if exchange and spec.exchange_field:
            clauses.append(f"`{spec.exchange_field}` = @exchange")
            parameters.append(self._bigquery.ScalarQueryParameter("exchange", "STRING", exchange))
        if spec.date_field and start_date and end_date:
            clauses.append(f"`{spec.date_field}` BETWEEN @start_date AND @end_date")
            parameters.extend(
                [
                    self._bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
                    self._bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
                ]
            )
        where = f" WHERE {' AND '.join(clauses)}" if clauses else ""
        watermark = (
            f"CAST(MAX(`{spec.watermark_field}`) AS STRING)"
            if spec.watermark_field
            else "CAST(NULL AS STRING)"
        )
        job_config = self._bigquery.QueryJobConfig(query_parameters=parameters)
        result = self._client.query(
            f"SELECT COUNT(*) AS row_count, {watermark} AS watermark "
            f"FROM `{self.dataset_id}.{spec.name}`{where}",
            job_config=job_config,
            location=self.location,
        ).result()
        row = next(iter(result))
        actual = self._client.get_table(f"{self.dataset_id}.{spec.name}")
        expected_schema = {field.name: field.field_type for field in spec.fields}
        actual_schema = {field.name: field.field_type for field in actual.schema}
        drift = _schema_drift(expected_schema, actual_schema)
        return ReconciliationResult(
            row_count=int(row["row_count"]),
            watermark=row["watermark"],
            schema_drift=drift,
        )


def run_bigquery_sync(
    *,
    settings: Settings | None = None,
    store: TimescaleStore | None = None,
    gateway: BigQueryGateway | None = None,
    exchange: str | None = None,
    year: int | None = None,
    entities: Sequence[str] = DEFAULT_BIGQUERY_ENTITIES,
    trigger: str = "dagster",
    run_id: str | None = None,
    today: date | None = None,
) -> BigQuerySyncResult:
    settings = settings or get_settings()
    selected_run_id = run_id or str(uuid4())
    if not settings.bigquery_enabled:
        return BigQuerySyncResult(run_id=selected_run_id, status="disabled")
    selected_specs = _selected_specs(entities)
    canonical_exchange = exchange.upper() if exchange else None
    if canonical_exchange and canonical_exchange not in {"NSE", "TSX", "US"}:
        raise ValueError("exchange must be NSE, TSX, or US")
    if year is not None and not 1990 <= year <= 2200:
        raise ValueError("year must be between 1990 and 2200")
    store = store or TimescaleStore(settings.database_url)
    gateway = gateway or GoogleBigQueryGateway(settings)
    now = datetime.now(UTC)
    start_date, end_date = _sync_window(year=year, today=today or now.date())
    run_row = _new_run_row(
        selected_run_id,
        settings=settings,
        trigger=trigger,
        exchange=canonical_exchange,
        year=year,
        entities=tuple(spec.name for spec in selected_specs),
        at=now,
    )
    store.upsert_bigquery_sync_run(run_row)
    existing_partitions = {
        row["entity"]: row
        for row in store.bigquery_sync_partitions(run_id=selected_run_id)
    }
    statuses: dict[str, str] = {}
    totals = _empty_totals()
    source_watermark: str | None = None
    destination_watermark: str | None = None
    bigquery_job_id: str | None = None
    schema_drift: dict[str, Any] = {}
    try:
        gateway.ensure_foundation(selected_specs)
        for spec in selected_specs:
            existing = existing_partitions.get(spec.name)
            if existing and existing["status"] == "completed":
                statuses[spec.name] = "completed"
                _add_partition_totals(totals, existing)
                source_watermark = existing.get("source_watermark") or source_watermark
                destination_watermark = (
                    existing.get("destination_watermark") or destination_watermark
                )
                bigquery_job_id = existing.get("bigquery_job_id") or bigquery_job_id
                schema_drift.update(existing.get("schema_drift") or {})
                continue
            if existing:
                partition = dict(existing)
            else:
                partition = _new_partition_row(
                    selected_run_id,
                    spec=spec,
                    exchange=canonical_exchange,
                    start_date=start_date,
                    end_date=end_date,
                    at=datetime.now(UTC),
                )
            partition.update(
                status="pending",
                error_details=None,
                completed_at=None,
                updated_at=datetime.now(UTC),
            )
            store.upsert_bigquery_sync_partition(partition)
            try:
                completed = _sync_entity(
                    store,
                    gateway,
                    spec,
                    partition,
                    exchange=canonical_exchange,
                    start_date=start_date,
                    end_date=end_date,
                    chunk_size=settings.bigquery_backfill_chunk_size,
                    retry_attempts=settings.bigquery_retry_attempts,
                )
                statuses[spec.name] = "completed"
                _add_partition_totals(totals, completed)
                source_watermark = completed.get("source_watermark") or source_watermark
                destination_watermark = (
                    completed.get("destination_watermark") or destination_watermark
                )
                bigquery_job_id = completed.get("bigquery_job_id") or bigquery_job_id
                schema_drift.update(completed.get("schema_drift") or {})
            except Exception as exc:
                failed_at = datetime.now(UTC)
                partition.update(
                    status="failed",
                    error_details=str(exc),
                    completed_at=failed_at,
                    updated_at=failed_at,
                    duration_seconds=(
                        failed_at - _as_utc_datetime(partition["created_at"])
                    ).total_seconds(),
                )
                store.upsert_bigquery_sync_partition(partition)
                statuses[spec.name] = "failed"
                raise
        finished = datetime.now(UTC)
        run_row.update(
            status="completed",
            finished_at=finished,
            duration_seconds=(finished - now).total_seconds(),
            last_successful_sync_at=finished,
            source_watermark=source_watermark,
            destination_watermark=destination_watermark,
            bigquery_job_id=bigquery_job_id,
            schema_drift=schema_drift,
            updated_at=finished,
            **totals,
        )
        store.upsert_bigquery_sync_run(run_row)
        return BigQuerySyncResult(
            run_id=selected_run_id,
            status="completed",
            partition_statuses=statuses,
            bigquery_job_id=bigquery_job_id,
            **{key: totals[key] for key in _result_total_keys()},
        )
    except Exception as exc:
        failed_at = datetime.now(UTC)
        run_row.update(
            status="failed",
            finished_at=failed_at,
            duration_seconds=(failed_at - now).total_seconds(),
            error_details=str(exc),
            source_watermark=source_watermark,
            destination_watermark=destination_watermark,
            bigquery_job_id=bigquery_job_id,
            schema_drift=schema_drift,
            updated_at=failed_at,
            **totals,
        )
        store.upsert_bigquery_sync_run(run_row)
        return BigQuerySyncResult(
            run_id=selected_run_id,
            status="failed",
            error_details=str(exc),
            partition_statuses=statuses,
            bigquery_job_id=bigquery_job_id,
            **{key: totals[key] for key in _result_total_keys()},
        )


def _sync_entity(
    store: TimescaleStore,
    gateway: BigQueryGateway,
    spec: EntitySpec,
    partition: dict[str, Any],
    *,
    exchange: str | None,
    start_date: date,
    end_date: date,
    chunk_size: int,
    retry_attempts: int,
) -> dict[str, Any]:
    partition["status"] = "running"
    partition["attempt_count"] = int(partition["attempt_count"]) + 1
    partition["updated_at"] = datetime.now(UTC)
    store.upsert_bigquery_sync_partition(partition)
    source_count, source_watermark = _source_metrics(
        store, spec, exchange=exchange, start_date=start_date, end_date=end_date
    )
    inserted = updated = rejected = retries = 0
    last_job_id: str | None = None
    for chunk_index, rows in enumerate(
        _extract_batches(
            store,
            spec,
            exchange=exchange,
            start_date=start_date,
            end_date=end_date,
            chunk_size=chunk_size,
        )
    ):
        for attempt in range(1, retry_attempts + 1):
            try:
                merge = gateway.merge_rows(
                    spec,
                    rows,
                    load_id=f"{partition['partition_id']}-{chunk_index}",
                )
                inserted += merge.inserted_rows
                updated += merge.updated_rows
                rejected += merge.rejected_rows
                last_job_id = merge.job_id
                break
            except Exception:
                if attempt >= retry_attempts:
                    raise
                retries += 1
                time.sleep(min(2 ** (attempt - 1), 8))
    destination = gateway.reconcile(
        spec,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
    )
    difference = source_count - destination.row_count
    finished = datetime.now(UTC)
    partition.update(
        status="completed" if difference == 0 and not destination.schema_drift else "failed",
        source_row_count=source_count,
        destination_row_count=destination.row_count,
        count_difference=difference,
        inserted_rows=inserted,
        updated_rows=updated,
        rejected_rows=rejected,
        source_watermark=source_watermark,
        destination_watermark=destination.watermark,
        bigquery_job_id=last_job_id,
        duration_seconds=(
            finished - _as_utc_datetime(partition["created_at"])
        ).total_seconds(),
        schema_drift=destination.schema_drift,
        error_details=(
            "Reconciliation failed: source/destination counts or schemas differ."
            if difference != 0 or destination.schema_drift
            else None
        ),
        updated_at=finished,
        completed_at=finished,
        retry_count=retries,
    )
    # retry_count is run-level only and is not stored on the partition row.
    retry_count = int(partition.pop("retry_count"))
    store.upsert_bigquery_sync_partition(partition)
    if partition["status"] != "completed":
        raise RuntimeError(str(partition["error_details"]))
    completed = dict(partition)
    completed["retry_count"] = retry_count
    return completed


def _source_query(
    spec: EntitySpec,
    *,
    exchange: str | None,
    start_date: date,
    end_date: date,
):
    columns = [spec.source_table.c[field.name] for field in spec.fields]
    query = select(*columns)
    if exchange and spec.exchange_field:
        query = query.where(spec.source_table.c[spec.exchange_field] == exchange)
    if spec.date_field:
        query = query.where(spec.source_table.c[spec.date_field] >= start_date).where(
            spec.source_table.c[spec.date_field] <= end_date
        )
    return query


def _extract_batches(
    store: TimescaleStore,
    spec: EntitySpec,
    *,
    exchange: str | None,
    start_date: date,
    end_date: date,
    chunk_size: int,
) -> Iterator[list[dict[str, Any]]]:
    query = _source_query(
        spec, exchange=exchange, start_date=start_date, end_date=end_date
    ).order_by(*(spec.source_table.c[key] for key in spec.natural_keys))
    with store.engine.connect() as connection:
        result = connection.execution_options(stream_results=True).execute(query).mappings()
        while True:
            chunk = [dict(row) for row in result.fetchmany(chunk_size)]
            if not chunk:
                return
            yield chunk


def _source_metrics(
    store: TimescaleStore,
    spec: EntitySpec,
    *,
    exchange: str | None,
    start_date: date,
    end_date: date,
) -> tuple[int, str | None]:
    scoped = _source_query(
        spec,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
    ).subquery()
    watermark = (
        func.max(scoped.c[spec.watermark_field]) if spec.watermark_field else func.null()
    )
    with store.engine.begin() as connection:
        row = connection.execute(
            select(
                func.count().label("row_count"),
                watermark.label("watermark"),
            ).select_from(scoped)
        ).mappings().one()
    return int(row["row_count"]), str(row["watermark"]) if row["watermark"] is not None else None


def _selected_specs(entities: Sequence[str]) -> list[EntitySpec]:
    selected: list[EntitySpec] = []
    for entity in dict.fromkeys(entities):
        if entity not in ENTITY_SPECS:
            raise ValueError(f"Unsupported BigQuery entity: {entity}")
        selected.append(ENTITY_SPECS[entity])
    if not selected:
        raise ValueError("At least one BigQuery entity is required.")
    return selected


def _sync_window(*, year: int | None, today: date) -> tuple[date, date]:
    if year is not None:
        return date(year, 1, 1), date(year, 12, 31)
    return today - timedelta(days=7), today


def _new_run_row(
    run_id: str,
    *,
    settings: Settings,
    trigger: str,
    exchange: str | None,
    year: int | None,
    entities: tuple[str, ...],
    at: datetime,
) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "trigger": trigger,
        "status": "running",
        "project_id": str(settings.bigquery_project_id),
        "dataset": settings.bigquery_dataset,
        "location": settings.bigquery_location,
        "exchange": exchange,
        "year": year,
        "entities": list(entities),
        "started_at": at,
        "finished_at": None,
        **_empty_totals(),
        "duration_seconds": None,
        "source_watermark": None,
        "destination_watermark": None,
        "last_successful_sync_at": None,
        "bigquery_job_id": None,
        "schema_drift": {},
        "error_details": None,
        "created_at": at,
        "updated_at": at,
    }


def _new_partition_row(
    run_id: str,
    *,
    spec: EntitySpec,
    exchange: str | None,
    start_date: date,
    end_date: date,
    at: datetime,
) -> dict[str, Any]:
    partition_id = str(
        uuid5(
            NAMESPACE_URL,
            f"bigquery:{run_id}:{spec.name}:{exchange}:{start_date}:{end_date}",
        )
    )
    return {
        "partition_id": partition_id,
        "run_id": run_id,
        "entity": spec.name,
        "exchange": exchange,
        "partition_start": start_date if spec.date_field else None,
        "partition_end": end_date if spec.date_field else None,
        "status": "pending",
        "attempt_count": 0,
        "source_row_count": 0,
        "destination_row_count": 0,
        "count_difference": 0,
        "inserted_rows": 0,
        "updated_rows": 0,
        "rejected_rows": 0,
        "source_watermark": None,
        "destination_watermark": None,
        "bigquery_job_id": None,
        "duration_seconds": None,
        "schema_drift": {},
        "error_details": None,
        "created_at": at,
        "updated_at": at,
        "completed_at": None,
    }


def _empty_totals() -> dict[str, int]:
    return {
        "source_row_count": 0,
        "destination_row_count": 0,
        "count_difference": 0,
        "inserted_rows": 0,
        "updated_rows": 0,
        "rejected_rows": 0,
        "retry_count": 0,
    }


def _result_total_keys() -> tuple[str, ...]:
    return tuple(_empty_totals())


def _add_partition_totals(totals: dict[str, int], partition: Mapping[str, Any]) -> None:
    for key in _result_total_keys():
        totals[key] += int(partition.get(key) or 0)


def _safe_job_suffix(value: str) -> str:
    return "".join(character if character.isalnum() else "_" for character in value)[-900:]


def _as_utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _json_compatible(value: Any) -> Any:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, Mapping):
        return {key: _json_compatible(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_compatible(item) for item in value]
    return value


def _schema_drift(expected: Mapping[str, str], actual: Mapping[str, str]) -> dict[str, Any]:
    missing = sorted(set(expected) - set(actual))
    unexpected = sorted(set(actual) - set(expected))
    changed = {
        name: {"expected": expected[name], "actual": actual[name]}
        for name in sorted(set(expected) & set(actual))
        if _canonical_bq_type(expected[name]) != _canonical_bq_type(actual[name])
    }
    return {
        key: value
        for key, value in {
            "missing_fields": missing,
            "unexpected_fields": unexpected,
            "type_changes": changed,
        }.items()
        if value
    }


def _canonical_bq_type(value: str) -> str:
    normalized = value.upper()
    return {
        "INT64": "INTEGER",
        "FLOAT64": "FLOAT",
        "BOOL": "BOOLEAN",
    }.get(normalized, normalized)
