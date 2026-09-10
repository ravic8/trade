#!/usr/bin/env bash
set -Eeuo pipefail

APP_DIR="${TRADE_APP_DIR:-/opt/trade/app}"
ENV_FILE="${TRADE_ENV_FILE:-/opt/trade/.env}"
KES_IMAGE="${PROD_KES_IMAGE:-minio/kes:latest}"

log() {
  printf '[managed-kes] %s\n' "$*"
}

fail() {
  printf '[managed-kes] %s\n' "$*" >&2
  exit 1
}

require_command() {
  command -v "$1" >/dev/null 2>&1 || fail "missing required command: $1"
}

require_regular_path() {
  local path="$1"
  if [[ -L "$path" ]]; then
    fail "refusing symbolic link path: $path"
  fi
}

set_environment_value() {
  local key="$1"
  local value="$2"
  local directory temporary
  directory="$(dirname "$ENV_FILE")"
  temporary="$(mktemp "$directory/.managed-kes-env.XXXXXX")"
  chmod 0600 "$temporary"
  awk -v key="$key" -v value="$value" '
    BEGIN { replaced = 0 }
    index($0, key "=") == 1 {
      if (!replaced) {
        print key "=" value
        replaced = 1
      }
      next
    }
    { print }
    END {
      if (!replaced) print key "=" value
    }
  ' "$ENV_FILE" > "$temporary"
  mv "$temporary" "$ENV_FILE"
  printf -v "$key" '%s' "$value"
  export "$key"
}

parse_api_key() {
  awk '/kes:v1:/{ print $1; exit }'
}

parse_identity() {
  awk '/^[[:space:]]*[0-9a-f]{64}[[:space:]]*$/ { gsub(/[[:space:]]/, ""); print; exit }'
}

require_file() {
  [[ -f "$1" ]] || fail "missing required file: $1"
}

load_environment() {
  require_file "$ENV_FILE"
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
}

write_kes_config() {
  local config_dir="$1"
  local identity="$2"
  local temporary
  temporary="$(mktemp "$config_dir/.config.XXXXXX")"
  chmod 0600 "$temporary"
  cat > "$temporary" <<EOF
address: 0.0.0.0:7373

admin:
  identity: disabled

tls:
  key: /certs/server.key
  cert: /certs/server.crt

policy:
  minio:
    allow:
      - /v1/key/create/*
      - /v1/key/generate/*
      - /v1/key/decrypt/*
      - /v1/key/bulk/decrypt
      - /v1/key/list/*
      - /v1/status
      - /v1/ready
      - /v1/metrics
    identities:
      - $identity

keystore:
  fs:
    path: /keys
EOF
  mv "$temporary" "$config_dir/config.yaml"
}

prepare_managed_kes() {
  load_environment
  [[ "${PROD_SELF_MANAGED_KES_ENABLED:-false}" == "true" ]] || \
    fail "self-managed KES is not enabled"

  require_command docker
  require_command openssl
  require_command awk
  require_command install

  local config_dir="${PROD_KES_CONFIG_DIR:-/opt/trade/kes/config}"
  local cert_dir="${PROD_KES_CERT_DIR:-/opt/trade/kes/certs}"
  local key_dir="${PROD_KES_KEY_DIR:-/opt/trade/kes/keys}"
  local secret_dir="${PROD_KES_SECRET_DIR:-/opt/trade/kes/secrets}"

  for directory in "$config_dir" "$cert_dir" "$key_dir" "$secret_dir"; do
    require_regular_path "$directory"
    install -m 0700 -d "$directory"
  done

  local server_key="$cert_dir/server.key"
  local server_cert="$cert_dir/server.crt"
  require_regular_path "$server_key"
  require_regular_path "$server_cert"
  if [[ ! -f "$server_key" || ! -f "$server_cert" ]]; then
    local openssl_config="$cert_dir/server-openssl.cnf"
    cat > "$openssl_config" <<EOF
[req]
distinguished_name = dn
x509_extensions = v3_req
prompt = no

[dn]
CN = trade-managed-kes

[v3_req]
subjectAltName = @alt_names

[alt_names]
DNS.1 = kes
DNS.2 = localhost
IP.1 = 127.0.0.1
EOF
    chmod 0600 "$openssl_config"
    openssl req \
      -x509 \
      -newkey rsa:4096 \
      -sha256 \
      -days "${PROD_KES_CERT_DAYS:-3650}" \
      -nodes \
      -config "$openssl_config" \
      -keyout "$server_key" \
      -out "$server_cert" >/dev/null 2>&1
    chmod 0600 "$server_key"
    chmod 0644 "$server_cert"
    log "generated self-signed KES server certificate"
  fi

  local api_key="${PROD_MINIO_KMS_API_KEY:-}"
  local identity_file="$secret_dir/minio.identity"
  local identity=""
  if [[ -z "$api_key" || "$api_key" == replace-* ]]; then
    local identity_output
    identity_output="$(docker run --rm "$KES_IMAGE" identity new)"
    api_key="$(printf '%s\n' "$identity_output" | parse_api_key)"
    identity="$(printf '%s\n' "$identity_output" | parse_identity)"
    [[ -n "$api_key" && -n "$identity" ]] || fail "unable to generate KES identity"
    set_environment_value PROD_MINIO_KMS_API_KEY "$api_key"
    printf '%s\n' "$identity" > "$identity_file"
    chmod 0600 "$identity_file"
    log "generated persistent KES API key for MinIO"
  elif [[ -f "$identity_file" ]]; then
    identity="$(tr -d '[:space:]' < "$identity_file")"
  else
    identity="$(docker run --rm "$KES_IMAGE" identity of "$api_key" | parse_identity)"
    [[ -n "$identity" ]] || fail "unable to derive KES identity from configured API key"
    printf '%s\n' "$identity" > "$identity_file"
    chmod 0600 "$identity_file"
    log "recorded KES identity for configured MinIO API key"
  fi

  [[ "$identity" =~ ^[0-9a-f]{64}$ ]] || fail "KES identity is invalid"

  set_environment_value PROD_MINIO_KMS_ENABLED true
  set_environment_value PROD_MINIO_KMS_SERVER "${PROD_MINIO_KMS_SERVER:-https://kes:7373}"
  set_environment_value PROD_MINIO_KMS_ENCLAVE "${PROD_MINIO_KMS_ENCLAVE:-trade-production}"
  set_environment_value PROD_MINIO_KMS_SSE_KEY "${PROD_MINIO_KMS_SSE_KEY:-trade-research-sse}"
  set_environment_value PROD_KES_CONFIG_DIR "$config_dir"
  set_environment_value PROD_KES_CERT_DIR "$cert_dir"
  set_environment_value PROD_KES_KEY_DIR "$key_dir"
  set_environment_value PROD_KES_SECRET_DIR "$secret_dir"

  require_regular_path "$config_dir/config.yaml"
  write_kes_config "$config_dir" "$identity"
  log "rendered KES policy and filesystem keystore configuration"
}

case "${1:-prepare}" in
  prepare)
    prepare_managed_kes
    ;;
  *)
    fail "unsupported operation: $1"
    ;;
esac
