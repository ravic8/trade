# Daily backend recovery

The operator owns durable database and object-store data, runtime volumes,
credentials, encryption keys, retention and restore targets. Preserve release
images and source alongside recovery evidence. Historical table retention is
independent of which application modules remain in this repository.

`deploy/backup.sh` creates a checksummed PostgreSQL dump and archives configured
data/artifact/Dagster/MinIO/ClickHouse/CloudBeaver directories. It briefly quiesces
owned workflow and mutable storage services, restarting only those that were
running. Managed KES configuration, certificates, keys and secrets are included
when present. Keep bundles private. Application encryption keys and externally
managed KMS recovery must be preserved through the operator's secret procedure.
This script does not archive or stop discarded filing/Qdrant services.

For an isolated PostgreSQL-only rehearsal:

```sh
./deploy/restore-drill.sh /path/to/backup --report /path/to/new-report.json \
  --image timescale/timescaledb:2.23.1-pg16
```

Use an image compatible with the backup's PostgreSQL and Timescale versions;
for server rehearsals, use the server's existing immutable image ID. The tool
verifies checksums before Docker access, uses uniquely named resources, caps the
container at 1 CPU/1 GB RAM, disables networking and publishes no ports. It
restores with error checking, records daily row counts and migration revisions,
and removes only its own container and volume. It never restores into production.

A PostgreSQL restore does not prove MinIO/KES or ClickHouse restoration, fresh/upgrade
application migrations, snapshot concurrency or production recovery time.
`python scripts/verify_core_recovery.py` and its `--external` variant separately
exercise isolated core recovery, schema upgrades and volume retention. Current
execution evidence and outstanding checks are tracked in
[the implementation journal](repository_split_progress.md).

## Storage image reproducibility

The former MinIO server/client Docker Hub and Quay references are unavailable.
Local Compose and new production defaults build the preserved community releases
from immutable upstream commits in `minio/Dockerfile.minio` and
`minio/Dockerfile.mc`. These builds preserve upstream AGPL license files. Existing
production storage is not upgraded as part of the application split. Explicitly
configured cached images remain supported; archive them with `docker save`,
record their immutable IDs and retain private backups outside the server.

The 17 October cutover checkpoint includes a private image archive for the exact
MinIO and KES images used by the successful encrypted recovery rehearsal. KES
remains operator-managed; preserve that archive and its configuration, certificates
and key files together. Changing image defaults cannot reconstruct encryption keys.
Upstream source: [MinIO container instructions](https://github.com/minio/minio/blob/master/docs/docker/README.md).
