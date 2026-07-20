from __future__ import annotations

import os
import stat
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import UTC, date, datetime, timedelta
from typing import Any, Protocol
from uuid import NAMESPACE_URL, uuid4, uuid5

from sqlalchemy import DateTime as SQLDateTime
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
        date_field="started_at",
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
        date_field="created_at",
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
    staging_row_count: int = 0
    merged_row_count: int = 0


@dataclass(frozen=True)
class ReconciliationResult:
    row_count: int
    watermark: str | None
    duplicate_business_key_count: int = 0
    minimum_date: date | None = None
    maximum_date: date | None = None
    schema_drift: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SourceMetrics:
    row_count: int
    watermark: str | None
    duplicate_business_key_count: int
    minimum_date: date | None
    maximum_date: date | None


@dataclass(frozen=True)
class BigQueryEnvironmentVerification:
    authenticated_principal: str | None
    project_id: str
    core_dataset: str
    core_dataset_location: str
    reporting_dataset: str
    reporting_dataset_location: str


class BigQueryGateway(Protocol):
    def ensure_foundation(
        self, specs: Iterable[EntitySpec]
    ) -> BigQueryEnvironmentVerification | None: ...

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
    staging_row_count: int = 0
    merged_row_count: int = 0
    duplicate_business_key_count: int = 0
    retry_count: int = 0
    bigquery_job_id: str | None = None
    error_details: str | None = None
    partition_statuses: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class BigQueryCanaryReadiness:
    ready_for_production: bool
    year: int
    successful_run_ids: tuple[str, ...]
    reason: str


@dataclass(frozen=True)
class BigQueryBackfillYearVerification:
    year: int
    reconciled: bool
    run_id: str | None = None
    run_status: str | None = None
    partition_status: str | None = None
    source_row_count: int | None = None
    destination_row_count: int | None = None
    count_difference: int | None = None
    rejected_rows: int | None = None
    duplicate_business_key_count: int | None = None
    source_min_date: date | None = None
    source_max_date: date | None = None
    destination_min_date: date | None = None
    destination_max_date: date | None = None
    source_watermark: str | None = None
    destination_watermark: str | None = None
    bigquery_job_id: str | None = None
    compared_run_id: str | None = None
    issues: tuple[str, ...] = ()


@dataclass(frozen=True)
class BigQueryBackfillVerification:
    exchange: str
    entity: str
    start_year: int
    end_year: int
    require_idempotent_rerun: bool
    years: tuple[BigQueryBackfillYearVerification, ...]

    @property
    def reconciled(self) -> bool:
        return bool(self.years) and all(year.reconciled for year in self.years)


class GoogleBigQueryGateway:
    """Real BigQuery staging/MERGE client. Imported lazily behind the feature flag."""

    def __init__(self, settings: Settings) -> None:
        from google.cloud import bigquery

        credentials = None
        authenticated_principal = None
        if settings.bigquery_auth_method == "service_account_file":
            from google.oauth2 import service_account

            _validate_credentials_file(settings.bigquery_credentials_path)
            credentials = service_account.Credentials.from_service_account_file(
                str(settings.bigquery_credentials_path)
            )
            authenticated_principal = credentials.service_account_email
            if authenticated_principal != settings.bigquery_expected_service_account_email:
                raise RuntimeError(
                    "BigQuery credentials do not belong to the configured exporter identity."
                )
        self._bigquery = bigquery
        self._client = bigquery.Client(
            project=settings.bigquery_project_id,
            credentials=credentials,
            location=settings.bigquery_location,
        )
        self.project_id = str(settings.bigquery_project_id)
        self.core_dataset = settings.bigquery_core_dataset
        self.reporting_dataset = settings.bigquery_reporting_dataset
        self.location = settings.bigquery_location
        self.authenticated_principal = authenticated_principal

    @property
    def dataset_id(self) -> str:
        return f"{self.project_id}.{self.core_dataset}"

    @property
    def reporting_dataset_id(self) -> str:
        return f"{self.project_id}.{self.reporting_dataset}"

    def verify_environment(self) -> BigQueryEnvironmentVerification:
        from google.api_core.exceptions import NotFound

        datasets = []
        for purpose, dataset_id in (
            ("core", self.dataset_id),
            ("reporting", self.reporting_dataset_id),
        ):
            try:
                dataset = self._client.get_dataset(
                    dataset_id,
                    dataset_view=self._bigquery.enums.DatasetView.METADATA,
                )
            except NotFound as exc:
                raise RuntimeError(
                    f"Configured BigQuery {purpose} dataset does not exist."
                ) from exc
            except Exception as exc:
                raise RuntimeError(
                    f"Unable to verify the configured BigQuery {purpose} dataset via the API."
                ) from exc
            actual_location = str(dataset.location)
            if actual_location.upper() != self.location.upper():
                raise RuntimeError(
                    f"BigQuery {purpose} dataset location is {actual_location}; "
                    f"expected {self.location}."
                )
            datasets.append(actual_location)
        return BigQueryEnvironmentVerification(
            authenticated_principal=self.authenticated_principal,
            project_id=self.project_id,
            core_dataset=self.core_dataset,
            core_dataset_location=datasets[0],
            reporting_dataset=self.reporting_dataset,
            reporting_dataset_location=datasets[1],
        )

    def ensure_foundation(
        self, specs: Iterable[EntitySpec]
    ) -> BigQueryEnvironmentVerification:
        # This must remain the first operation: no table or staging write is allowed
        # until both pre-existing datasets have been verified through BigQuery's API.
        verification = self.verify_environment()
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
                )
                table.require_partition_filter = spec.require_partition_filter
            if spec.cluster_fields:
                table.clustering_fields = list(spec.cluster_fields)
            self._client.create_table(table, exists_ok=True)
        return verification

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
            merge_parameters: list[Any] = []
            if spec.partition_field:
                partition_values = [
                    parsed
                    for row in rows
                    if row.get(spec.partition_field) is not None
                    and (parsed := _as_date(row[spec.partition_field])) is not None
                ]
                if partition_values:
                    key_match += (
                        f" AND T.`{spec.partition_field}` BETWEEN "
                        "@merge_start_date AND @merge_end_date"
                    )
                    merge_parameters.extend(
                        [
                            self._bigquery.ScalarQueryParameter(
                                "merge_start_date", "DATE", min(partition_values)
                            ),
                            self._bigquery.ScalarQueryParameter(
                                "merge_end_date", "DATE", max(partition_values)
                            ),
                        ]
                    )
            merge_job_config = self._bigquery.QueryJobConfig(
                query_parameters=merge_parameters
            )
            classification = self._client.query(
                f"""
                SELECT COUNTIF(T.`{spec.natural_keys[0]}` IS NULL) AS inserted_rows,
                       COUNTIF(T.`{spec.natural_keys[0]}` IS NOT NULL) AS updated_rows
                FROM `{staging_id}` AS S
                LEFT JOIN `{target_id}` AS T ON {key_match}
                """,
                job_config=merge_job_config,
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
                job_config=merge_job_config,
                location=self.location,
            )
            merge_job.result()
            return MergeResult(
                job_id=str(merge_job.job_id),
                inserted_rows=int(counts["inserted_rows"]),
                updated_rows=int(counts["updated_rows"]),
                staging_row_count=len(rows),
                merged_row_count=(
                    int(counts["inserted_rows"]) + int(counts["updated_rows"])
                ),
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
            if _bigquery_field_type(spec, spec.date_field) == "TIMESTAMP":
                clauses.append(
                    f"`{spec.date_field}` >= @start_timestamp "
                    f"AND `{spec.date_field}` < @end_timestamp"
                )
                parameters.extend(
                    [
                        self._bigquery.ScalarQueryParameter(
                            "start_timestamp",
                            "TIMESTAMP",
                            datetime.combine(start_date, datetime.min.time(), tzinfo=UTC),
                        ),
                        self._bigquery.ScalarQueryParameter(
                            "end_timestamp",
                            "TIMESTAMP",
                            datetime.combine(
                                end_date + timedelta(days=1),
                                datetime.min.time(),
                                tzinfo=UTC,
                            ),
                        ),
                    ]
                )
            else:
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
        minimum_date = (
            f"CAST(MIN(`{spec.date_field}`) AS STRING)"
            if spec.date_field
            else "CAST(NULL AS STRING)"
        )
        maximum_date = (
            f"CAST(MAX(`{spec.date_field}`) AS STRING)"
            if spec.date_field
            else "CAST(NULL AS STRING)"
        )
        natural_keys = ", ".join(f"`{key}`" for key in spec.natural_keys)
        job_config = self._bigquery.QueryJobConfig(query_parameters=parameters)
        result = self._client.query(
            f"""
            WITH scoped AS (
              SELECT * FROM `{self.dataset_id}.{spec.name}`{where}
            ), duplicate_keys AS (
              SELECT COUNT(*) - 1 AS excess_rows
              FROM scoped
              GROUP BY {natural_keys}
              HAVING COUNT(*) > 1
            )
            SELECT
              (SELECT COUNT(*) FROM scoped) AS row_count,
              (SELECT {watermark} FROM scoped) AS watermark,
              (SELECT {minimum_date} FROM scoped) AS minimum_date,
              (SELECT {maximum_date} FROM scoped) AS maximum_date,
              COALESCE((SELECT SUM(excess_rows) FROM duplicate_keys), 0)
                AS duplicate_business_key_count
            """,
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
            duplicate_business_key_count=int(row["duplicate_business_key_count"]),
            minimum_date=_as_date(row["minimum_date"]),
            maximum_date=_as_date(row["maximum_date"]),
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
    mode: str = "production",
) -> BigQuerySyncResult:
    settings = settings or get_settings()
    selected_run_id = run_id or str(uuid4())
    if not settings.bigquery_enabled:
        return BigQuerySyncResult(run_id=selected_run_id, status="disabled")
    if mode not in {"canary", "production"}:
        raise ValueError("mode must be canary or production")
    if mode == "canary" and not settings.bigquery_canary_enabled:
        return BigQuerySyncResult(run_id=selected_run_id, status="gated")
    if mode == "production" and not settings.bigquery_production_sync_enabled:
        return BigQuerySyncResult(run_id=selected_run_id, status="gated")
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
        # Establish the complete Phase 9.2B table contract while extraction remains
        # limited to the explicitly selected canary or production entities.
        verification = gateway.ensure_foundation(ENTITY_SPECS.values())
        if verification is not None:
            run_row.update(
                authenticated_principal=verification.authenticated_principal,
                reporting_dataset=verification.reporting_dataset,
                updated_at=datetime.now(UTC),
            )
            store.upsert_bigquery_sync_run(run_row)
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
                _add_partition_totals(totals, partition)
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
            source_row_count=totals["source_row_count"],
            destination_row_count=totals["destination_row_count"],
            count_difference=totals["count_difference"],
            inserted_rows=totals["inserted_rows"],
            updated_rows=totals["updated_rows"],
            rejected_rows=totals["rejected_rows"],
            staging_row_count=totals["staging_row_count"],
            merged_row_count=totals["merged_row_count"],
            duplicate_business_key_count=totals["duplicate_business_key_count"],
            retry_count=totals["retry_count"],
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
            source_row_count=totals["source_row_count"],
            destination_row_count=totals["destination_row_count"],
            count_difference=totals["count_difference"],
            inserted_rows=totals["inserted_rows"],
            updated_rows=totals["updated_rows"],
            rejected_rows=totals["rejected_rows"],
            staging_row_count=totals["staging_row_count"],
            merged_row_count=totals["merged_row_count"],
            duplicate_business_key_count=totals["duplicate_business_key_count"],
            retry_count=totals["retry_count"],
        )


def run_bigquery_tsx_canary(
    *,
    settings: Settings | None = None,
    store: TimescaleStore | None = None,
    gateway: BigQueryGateway | None = None,
    today: date | None = None,
) -> BigQuerySyncResult:
    """Run the fixed, bounded TSX latest-completed-year OHLCV canary."""
    current_date = today or datetime.now(UTC).date()
    return run_bigquery_sync(
        settings=settings,
        store=store,
        gateway=gateway,
        exchange="TSX",
        year=current_date.year - 1,
        entities=("ohlcv_daily",),
        trigger="dagster_tsx_canary",
        today=current_date,
        mode="canary",
    )


def evaluate_bigquery_tsx_canary_readiness(
    store: TimescaleStore,
    *,
    today: date | None = None,
) -> BigQueryCanaryReadiness:
    """Require two reconciled, equivalent canary runs before production promotion."""
    year = (today or datetime.now(UTC).date()).year - 1
    candidates = [
        run
        for run in store.bigquery_sync_runs(limit=100)
        if run.get("trigger") == "dagster_tsx_canary"
        and run.get("exchange") == "TSX"
        and int(run.get("year") or 0) == year
        and run.get("entities") == ["ohlcv_daily"]
        and run.get("status") == "completed"
    ][:2]
    if len(candidates) < 2:
        return BigQueryCanaryReadiness(
            ready_for_production=False,
            year=year,
            successful_run_ids=tuple(str(run["run_id"]) for run in candidates),
            reason="Two completed TSX canary runs are required.",
        )
    partition_sets = [
        store.bigquery_sync_partitions(run_id=str(run["run_id"]), limit=10)
        for run in candidates
    ]
    if any(not rows for rows in partition_sets):
        return BigQueryCanaryReadiness(
            ready_for_production=False,
            year=year,
            successful_run_ids=tuple(str(run["run_id"]) for run in candidates),
            reason="A completed TSX canary run is missing partition evidence.",
        )
    partitions = [rows[0] for rows in partition_sets]
    required_zero_fields = (
        "count_difference",
        "rejected_rows",
        "duplicate_business_key_count",
    )
    comparable_fields = (
        "source_row_count",
        "destination_row_count",
        "source_watermark",
        "destination_watermark",
        "source_min_date",
        "source_max_date",
        "destination_min_date",
        "destination_max_date",
    )
    reconciled = all(
        partition.get("status") == "completed"
        and not partition.get("schema_drift")
        and all(int(partition.get(field) or 0) == 0 for field in required_zero_fields)
        and partition.get("source_row_count") == partition.get("destination_row_count")
        and partition.get("source_watermark") == partition.get("destination_watermark")
        and partition.get("source_min_date") == partition.get("destination_min_date")
        and partition.get("source_max_date") == partition.get("destination_max_date")
        for partition in partitions
    )
    identical = all(
        partitions[0].get(field) == partitions[1].get(field)
        for field in comparable_fields
    )
    ready = reconciled and identical
    return BigQueryCanaryReadiness(
        ready_for_production=ready,
        year=year,
        successful_run_ids=tuple(str(run["run_id"]) for run in candidates),
        reason=(
            "Two reconciled, equivalent TSX canary runs passed."
            if ready
            else "The two latest TSX canary runs are not reconciled and equivalent."
        ),
    )


def evaluate_bigquery_backfill_reconciliation(
    store: TimescaleStore,
    *,
    exchange: str,
    start_year: int,
    end_year: int,
    entity: str = "ohlcv_daily",
    require_idempotent_rerun: bool = False,
) -> BigQueryBackfillVerification:
    """Evaluate exchange/year backfills using durable PostgreSQL evidence only."""
    normalized_exchange = exchange.strip().upper()
    if normalized_exchange not in {"NSE", "TSX", "US"}:
        raise ValueError("exchange must be NSE, TSX, or US")
    if start_year > end_year:
        raise ValueError("start_year must be less than or equal to end_year")
    if start_year < 1900 or end_year > 9999:
        raise ValueError("years must be between 1900 and 9999")
    spec = ENTITY_SPECS.get(entity)
    if spec is None:
        raise ValueError(f"Unknown BigQuery entity: {entity}")
    if spec.exchange_field is None or spec.date_field is None:
        raise ValueError(
            f"BigQuery entity {entity} does not support exchange/year backfill verification"
        )

    requested_years = tuple(range(start_year, end_year + 1))
    # Runs are returned newest first. The generous bounded read keeps this command
    # read-only while allowing repeated attempts without hiding recent evidence.
    run_limit = max(1000, len(requested_years) * 20)
    runs = [
        run
        for run in store.bigquery_sync_runs(limit=run_limit)
        if str(run.get("exchange") or "").upper() == normalized_exchange
        and start_year <= int(run.get("year") or 0) <= end_year
        and entity in tuple(run.get("entities") or ())
    ]

    verified_years: list[BigQueryBackfillYearVerification] = []
    for year in requested_years:
        candidates = [run for run in runs if int(run.get("year") or 0) == year]
        if not candidates:
            verified_years.append(
                BigQueryBackfillYearVerification(
                    year=year,
                    reconciled=False,
                    issues=("No synchronization run evidence was found.",),
                )
            )
            continue

        primary = _verify_bigquery_backfill_run(
            store,
            candidates[0],
            exchange=normalized_exchange,
            year=year,
            entity=entity,
        )
        if require_idempotent_rerun:
            rerun_issues: list[str] = []
            compared_run_id: str | None = None
            if len(candidates) < 2:
                rerun_issues.append("A second run is required to verify idempotency.")
            else:
                secondary = _verify_bigquery_backfill_run(
                    store,
                    candidates[1],
                    exchange=normalized_exchange,
                    year=year,
                    entity=entity,
                )
                compared_run_id = secondary.run_id
                rerun_issues.extend(
                    f"Compared run {secondary.run_id}: {issue}" for issue in secondary.issues
                )
                comparable_fields = (
                    "source_row_count",
                    "destination_row_count",
                    "count_difference",
                    "rejected_rows",
                    "duplicate_business_key_count",
                    "source_min_date",
                    "source_max_date",
                    "destination_min_date",
                    "destination_max_date",
                    "source_watermark",
                    "destination_watermark",
                )
                changed_fields = [
                    field_name
                    for field_name in comparable_fields
                    if getattr(primary, field_name) != getattr(secondary, field_name)
                ]
                if changed_fields:
                    rerun_issues.append(
                        "Idempotent rerun evidence differs for: "
                        + ", ".join(changed_fields)
                        + "."
                    )
            if rerun_issues:
                primary = replace(
                    primary,
                    reconciled=False,
                    compared_run_id=compared_run_id,
                    issues=primary.issues + tuple(rerun_issues),
                )
            else:
                primary = replace(primary, compared_run_id=compared_run_id)
        verified_years.append(primary)

    return BigQueryBackfillVerification(
        exchange=normalized_exchange,
        entity=entity,
        start_year=start_year,
        end_year=end_year,
        require_idempotent_rerun=require_idempotent_rerun,
        years=tuple(verified_years),
    )


def _verify_bigquery_backfill_run(
    store: TimescaleStore,
    run: Mapping[str, Any],
    *,
    exchange: str,
    year: int,
    entity: str,
) -> BigQueryBackfillYearVerification:
    run_id = str(run.get("run_id") or "")
    run_status = str(run.get("status") or "") or None
    issues: list[str] = []
    if run_status != "completed":
        issues.append(f"Run status is {run_status or 'missing'}, expected completed.")
    if run.get("error_details"):
        issues.append("Run contains error details.")

    partitions = [
        partition
        for partition in store.bigquery_sync_partitions(run_id=run_id, limit=100)
        if partition.get("entity") == entity
        and str(partition.get("exchange") or "").upper() == exchange
    ]
    if not partitions:
        return BigQueryBackfillYearVerification(
            year=year,
            reconciled=False,
            run_id=run_id,
            run_status=run_status,
            issues=tuple(issues + ["Matching partition evidence was not found."]),
        )

    partition = partitions[0]
    partition_status = str(partition.get("status") or "") or None
    source_rows = int(partition.get("source_row_count") or 0)
    destination_rows = int(partition.get("destination_row_count") or 0)
    count_difference = int(partition.get("count_difference") or 0)
    rejected_rows = int(partition.get("rejected_rows") or 0)
    duplicate_keys = int(partition.get("duplicate_business_key_count") or 0)
    source_min_date = partition.get("source_min_date")
    source_max_date = partition.get("source_max_date")
    destination_min_date = partition.get("destination_min_date")
    destination_max_date = partition.get("destination_max_date")
    source_watermark = partition.get("source_watermark")
    destination_watermark = partition.get("destination_watermark")
    bigquery_job_id = str(partition.get("bigquery_job_id") or "") or None

    if partition_status != "completed":
        issues.append(
            f"Partition status is {partition_status or 'missing'}, expected completed."
        )
    if partition.get("partition_start") != date(year, 1, 1):
        issues.append("Partition start does not match the requested calendar year.")
    if partition.get("partition_end") != date(year, 12, 31):
        issues.append("Partition end does not match the requested calendar year.")
    if source_rows <= 0:
        issues.append("Source row count must be greater than zero.")
    if source_rows != destination_rows:
        issues.append("Source and destination row counts do not match.")
    if count_difference != 0:
        issues.append("Count difference is not zero.")
    if rejected_rows != 0:
        issues.append("Rejected row count is not zero.")
    if duplicate_keys != 0:
        issues.append("Duplicate business-key count is not zero.")
    if source_min_date is None or source_max_date is None:
        issues.append("Source date bounds are missing.")
    if destination_min_date is None or destination_max_date is None:
        issues.append("Destination date bounds are missing.")
    if (source_min_date, source_max_date) != (
        destination_min_date,
        destination_max_date,
    ):
        issues.append("Source and destination date bounds do not match.")
    if not source_watermark or not destination_watermark:
        issues.append("Source or destination watermark is missing.")
    if source_watermark != destination_watermark:
        issues.append("Source and destination watermarks do not match.")
    if partition.get("schema_drift"):
        issues.append("Schema drift was recorded.")
    if partition.get("error_details"):
        issues.append("Partition contains error details.")
    if not bigquery_job_id:
        issues.append("BigQuery job ID is missing.")

    return BigQueryBackfillYearVerification(
        year=year,
        reconciled=not issues,
        run_id=run_id,
        run_status=run_status,
        partition_status=partition_status,
        source_row_count=source_rows,
        destination_row_count=destination_rows,
        count_difference=count_difference,
        rejected_rows=rejected_rows,
        duplicate_business_key_count=duplicate_keys,
        source_min_date=source_min_date,
        source_max_date=source_max_date,
        destination_min_date=destination_min_date,
        destination_max_date=destination_max_date,
        source_watermark=source_watermark,
        destination_watermark=destination_watermark,
        bigquery_job_id=bigquery_job_id,
        issues=tuple(issues),
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
    source = _source_metrics(
        store, spec, exchange=exchange, start_date=start_date, end_date=end_date
    )
    partition.update(
        source_row_count=source.row_count,
        source_watermark=source.watermark,
        source_min_date=source.minimum_date,
        source_max_date=source.maximum_date,
        duplicate_business_key_count=source.duplicate_business_key_count,
        updated_at=datetime.now(UTC),
    )
    store.upsert_bigquery_sync_partition(partition)
    if source.duplicate_business_key_count:
        raise RuntimeError("Source contains duplicate BigQuery business keys.")
    inserted = updated = rejected = retries = staging = merged = 0
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
                staging += merge.staging_row_count
                merged += merge.merged_row_count
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
    difference = source.row_count - destination.row_count
    duplicate_count = max(
        source.duplicate_business_key_count,
        destination.duplicate_business_key_count,
    )
    reconciliation_failed = bool(
        difference
        or destination.schema_drift
        or duplicate_count
        or rejected
        or _normalized_watermark(source.watermark)
        != _normalized_watermark(destination.watermark)
        or source.minimum_date != destination.minimum_date
        or source.maximum_date != destination.maximum_date
    )
    finished = datetime.now(UTC)
    partition.update(
        status="failed" if reconciliation_failed else "completed",
        source_row_count=source.row_count,
        destination_row_count=destination.row_count,
        count_difference=difference,
        inserted_rows=inserted,
        updated_rows=updated,
        rejected_rows=rejected,
        staging_row_count=staging,
        merged_row_count=merged,
        duplicate_business_key_count=duplicate_count,
        source_min_date=source.minimum_date,
        source_max_date=source.maximum_date,
        destination_min_date=destination.minimum_date,
        destination_max_date=destination.maximum_date,
        source_watermark=source.watermark,
        destination_watermark=destination.watermark,
        bigquery_job_id=last_job_id,
        duration_seconds=(
            finished - _as_utc_datetime(partition["created_at"])
        ).total_seconds(),
        schema_drift=destination.schema_drift,
        error_details=(
            "Reconciliation failed: source/destination counts or schemas differ."
            if reconciliation_failed
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
        source_column = spec.source_table.c[spec.date_field]
        if isinstance(source_column.type, SQLDateTime):
            query = query.where(
                source_column
                >= datetime.combine(start_date, datetime.min.time(), tzinfo=UTC)
            ).where(
                source_column
                < datetime.combine(
                    end_date + timedelta(days=1),
                    datetime.min.time(),
                    tzinfo=UTC,
                )
            )
        else:
            query = query.where(source_column >= start_date).where(source_column <= end_date)
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
) -> SourceMetrics:
    scoped = _source_query(
        spec,
        exchange=exchange,
        start_date=start_date,
        end_date=end_date,
    ).subquery()
    watermark = (
        func.max(scoped.c[spec.watermark_field]) if spec.watermark_field else func.null()
    )
    minimum_date = func.min(scoped.c[spec.date_field]) if spec.date_field else func.null()
    maximum_date = func.max(scoped.c[spec.date_field]) if spec.date_field else func.null()
    duplicate_groups = (
        select((func.count() - 1).label("excess_rows"))
        .select_from(scoped)
        .group_by(*(scoped.c[key] for key in spec.natural_keys))
        .having(func.count() > 1)
        .subquery()
    )
    duplicate_count = select(
        func.coalesce(func.sum(duplicate_groups.c.excess_rows), 0)
    ).scalar_subquery()
    with store.engine.begin() as connection:
        row = connection.execute(
            select(
                func.count().label("row_count"),
                watermark.label("watermark"),
                minimum_date.label("minimum_date"),
                maximum_date.label("maximum_date"),
                duplicate_count.label("duplicate_business_key_count"),
            ).select_from(scoped)
        ).mappings().one()
    return SourceMetrics(
        row_count=int(row["row_count"]),
        watermark=str(row["watermark"]) if row["watermark"] is not None else None,
        duplicate_business_key_count=int(row["duplicate_business_key_count"]),
        minimum_date=_as_date(row["minimum_date"]),
        maximum_date=_as_date(row["maximum_date"]),
    )


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
        "dataset": settings.bigquery_core_dataset,
        "reporting_dataset": settings.bigquery_reporting_dataset,
        "authenticated_principal": None,
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
        "staging_row_count": 0,
        "merged_row_count": 0,
        "duplicate_business_key_count": 0,
        "source_min_date": None,
        "source_max_date": None,
        "destination_min_date": None,
        "destination_max_date": None,
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
        "staging_row_count": 0,
        "merged_row_count": 0,
        "duplicate_business_key_count": 0,
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


def _validate_credentials_file(path: Any) -> None:
    """Validate the mounted key without reading or exposing its contents."""
    if path is None:
        raise RuntimeError("BigQuery credential file is not configured.")
    try:
        metadata = os.stat(path)
    except OSError as exc:
        raise RuntimeError("BigQuery credential file is missing or unreadable.") from exc
    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size <= 0 or not os.access(path, os.R_OK):
        raise RuntimeError("BigQuery credential file is missing or unreadable.")
    if metadata.st_mode & 0o077:
        raise RuntimeError("BigQuery credential file permissions must be 0600.")


def _as_date(value: Any) -> date | None:
    if value is None or isinstance(value, date):
        return value.date() if isinstance(value, datetime) else value
    normalized = str(value).replace("Z", "+00:00")
    try:
        return date.fromisoformat(normalized)
    except ValueError:
        return datetime.fromisoformat(normalized).date()


def _bigquery_field_type(spec: EntitySpec, field_name: str) -> str:
    return next(field.field_type for field in spec.fields if field.name == field_name)


def _normalized_watermark(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = value.strip().replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        try:
            return date.fromisoformat(normalized).isoformat()
        except ValueError:
            return normalized
    if parsed.tzinfo is not None:
        parsed = parsed.astimezone(UTC)
    return parsed.isoformat()
