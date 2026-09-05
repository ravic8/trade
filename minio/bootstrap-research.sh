#!/bin/sh
set -eu

mc alias set trade http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD"
for bucket in "$RAW_BUCKET" "$DATASETS_BUCKET" "$MODELS_BUCKET" "$EXPERIMENTS_BUCKET" "$EXPORTS_BUCKET"; do
  mc mb --ignore-existing "trade/$bucket"
  mc version enable "trade/$bucket"
done

bucket_resources=""
object_resources=""
for bucket in "$RAW_BUCKET" "$DATASETS_BUCKET" "$MODELS_BUCKET" "$EXPERIMENTS_BUCKET" "$EXPORTS_BUCKET"; do
  if [ -n "$bucket_resources" ]; then
    bucket_resources="$bucket_resources,"
    object_resources="$object_resources,"
  fi
  bucket_resources="$bucket_resources\"arn:aws:s3:::$bucket\""
  object_resources="$object_resources\"arn:aws:s3:::$bucket/*\""
done

cat > /tmp/research-read.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetBucketLocation","s3:GetBucketVersioning","s3:ListBucket"],"Resource":[$bucket_resources]},{"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion"],"Resource":[$object_resources]}]}
EOF
cat > /tmp/research-write.json <<EOF
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":["s3:GetBucketLocation","s3:GetBucketVersioning","s3:ListBucket"],"Resource":[$bucket_resources]},{"Effect":"Allow","Action":["s3:GetObject","s3:GetObjectVersion","s3:PutObject"],"Resource":[$object_resources]}]}
EOF

mc admin policy create trade research-read /tmp/research-read.json
mc admin policy create trade research-write /tmp/research-write.json
if ! mc admin user info trade "$DAGSTER_ACCESS_KEY" >/dev/null 2>&1; then
  mc admin user add trade "$DAGSTER_ACCESS_KEY" "$DAGSTER_SECRET_KEY"
fi
if ! mc admin user info trade "$API_ACCESS_KEY" >/dev/null 2>&1; then
  mc admin user add trade "$API_ACCESS_KEY" "$API_SECRET_KEY"
fi
mc admin policy attach trade research-write --user "$DAGSTER_ACCESS_KEY"
mc admin policy attach trade research-read --user "$API_ACCESS_KEY"

printf 'Research object-store buckets and identities are reconciled.\n'
