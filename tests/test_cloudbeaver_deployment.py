from __future__ import annotations

import json
from pathlib import Path

import yaml

REPOSITORY_ROOT = Path(__file__).resolve().parents[1]


def test_cloudbeaver_is_internal_pinned_and_hardened() -> None:
    compose = yaml.safe_load(
        (REPOSITORY_ROOT / "docker-compose.prod.yml").read_text(encoding="utf-8")
    )
    service = compose["services"]["cloudbeaver"]

    assert service["image"] == "${PROD_CLOUDBEAVER_IMAGE:-dbeaver/cloudbeaver:26.1.2}"
    assert "ports" not in service
    assert service["expose"] == ["8978"]
    assert service["networks"] == ["analytics_access"]
    assert compose["networks"]["analytics_access"]["internal"] is True
    assert "analytics_access" in compose["services"]["web"]["networks"]
    assert "analytics_access" in compose["services"]["postgres"]["networks"]
    assert service["depends_on"]["postgres"]["condition"] == "service_healthy"
    assert service["environment"] == {
        "CLOUDBEAVER_APP_ANONYMOUS_ACCESS_ENABLED": "false",
        "CLOUDBEAVER_APP_GRANT_CONNECTIONS_ACCESS_TO_ANONYMOUS_TEAM": "false",
        "CLOUDBEAVER_APP_SUPPORTS_CUSTOM_CONNECTIONS": "false",
        "CLOUDBEAVER_APP_PUBLIC_CREDENTIALS_SAVE_ENABLED": "false",
        "CLOUDBEAVER_APP_ADMIN_CREDENTIALS_SAVE_ENABLED": "false",
        "CLOUDBEAVER_APP_FORWARD_PROXY": "true",
        "CLOUDBEAVER_APP_READ_ONLY_CONNECTION_INFO": "true",
        "CLOUDBEAVER_DEVEL_MODE": "false",
        "CLOUDBEAVER_EXPIRE_SESSION_AFTER_PERIOD": (
            "${PROD_CLOUDBEAVER_SESSION_IDLE_MS:-1800000}"
        ),
    }


def test_cloudbeaver_connection_is_shared_without_credentials() -> None:
    path = REPOSITORY_ROOT / "deploy/cloudbeaver/data-sources.json"
    config = json.loads(path.read_text(encoding="utf-8"))
    connection = config["connections"]["trade-analytics-postgres"]
    database = connection["configuration"]

    assert database["host"] == "postgres"
    assert database["port"] == "5432"
    assert database["database"] == "trade_research"
    assert database["bootstrap"]["autocommit"] is True
    assert "auth-properties" not in database
    assert "user" not in database
    assert "password" not in database

    permissions_path = (
        REPOSITORY_ROOT / "deploy/cloudbeaver/data-sources-permissions.json"
    )
    permissions = json.loads(permissions_path.read_text(encoding="utf-8"))
    assert permissions == {"trade-analytics-postgres": ["user"]}


def test_caddy_routes_only_the_cloudbeaver_hostname_and_drops_identity_headers() -> None:
    caddyfile = (REPOSITORY_ROOT / "deploy/caddy/Caddyfile").read_text(encoding="utf-8")

    host_match = caddyfile.index("@cloudbeaver host {$CLOUDBEAVER_HOST:sql.example.com}")
    proxy = caddyfile.index("reverse_proxy cloudbeaver:8978")
    app_api = caddyfile.index("handle /api/*")
    assert host_match < proxy < app_api
    assert "header_up -Cf-Access-Authenticated-User-Email" in caddyfile
    assert "header_up -Cf-Access-Jwt-Assertion" in caddyfile


def test_deploy_installs_connection_policy_into_persistent_workspace() -> None:
    script = (REPOSITORY_ROOT / "deploy/deploy.sh").read_text(encoding="utf-8")

    assert "PROD_CLOUDBEAVER_WORKSPACE_DIR" in script
    assert 'deploy/cloudbeaver/data-sources.json"' in script
    assert 'deploy/cloudbeaver/data-sources-permissions.json"' in script
    assert '"$cloudbeaver_connections_dir/data-sources.json"' in script
    assert '"$cloudbeaver_connections_dir/data-sources-permissions.json"' in script
    assert "cloudbeaver_policy_changed" in script
    assert '"${compose[@]}" restart cloudbeaver' in script
    assert 'curl -fsS -H "Host: $cloudbeaver_host"' in script


def test_backup_stops_cloudbeaver_before_archiving_workspace() -> None:
    script = (REPOSITORY_ROOT / "deploy/backup.sh").read_text(encoding="utf-8")

    stop = script.index('stop cloudbeaver')
    archive = script.index('"cloudbeaver.tgz"')
    restart = script.rindex("\nrestart_quiesced_services\n")
    assert stop < archive < restart
