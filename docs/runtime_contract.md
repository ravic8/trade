# Runtime and hosting contract

Trade consumes configured services through explicit adapters. Application code
does not choose infrastructure based on a cloud provider, machine name or local
Docker runtime. The operator supplies endpoints, credentials, storage, encryption,
network policy, backups and monitoring.

| Boundary | Application responsibility | Operator responsibility |
| --- | --- | --- |
| PostgreSQL/TimescaleDB | canonical daily data, durable work, migrations and audit state | compatible service, credentials, volumes and recovery |
| Redis | provider request budgets | reachable endpoint and availability |
| yfinance / dormant Upstox daily | request mapping, normalization, quality and provenance | authorized access, entitlement and operating limits |
| ClickHouse | daily replica and reconciliation proofs | users, capacity, durable storage and recovery |
| Object storage | immutable raw evidence and manifests | S3 endpoint, identities, buckets, versioning and encryption keys |
| Dagster | daily jobs, desired schedules and guards | instance storage, processes and maintenance coordination |

Missing data remains empty or explicitly unavailable. Required infrastructure
failures return errors; no response substitutes invented observations. PostgreSQL
owns leases and retries; Redis is not a second durable queue. ClickHouse and raw
objects do not replace canonical PostgreSQL state.

The daily pipelines consume `DailyIngestionStore`; the runtime composition point
supplies adapters and rate-limit factories. Database implementations remain in
storage modules. Frontend interaction uses HTTP; research consumes versioned file
snapshots. Neither requires source imports from this repository.

Local Compose is a reference deployment. External endpoints use the
[external-services overlay](external_services.md). [Recovery](core_recovery.md)
and [fresh setup](fresh_setup.md) have isolated verifiers. Source tests and
configuration validation do not establish live infrastructure acceptance.
