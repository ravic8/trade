from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text


def test_opportunity_target_migration_creates_durable_table(
    tmp_path: Path,
    monkeypatch,
) -> None:
    database_path = tmp_path / "opportunity-migration.sqlite"
    database_url = f"sqlite:///{database_path}"
    monkeypatch.setenv("DATABASE_URL", database_url)

    command.upgrade(Config("alembic.ini"), "head")

    engine = create_engine(database_url)
    inspector = inspect(engine)
    columns = {
        column["name"] for column in inspector.get_columns("opportunity_targets_daily")
    }
    assert {
        "instrument_key",
        "source",
        "date",
        "target_version",
        "previous_close",
        "session_return",
        "gap",
        "true_return",
        "upside",
        "downside",
        "giveback",
        "recovery",
        "session_range",
        "true_upside",
        "true_downside",
        "true_range",
    } <= columns
    assert set(inspector.get_pk_constraint("opportunity_targets_daily")["constrained_columns"]) == {
        "instrument_key",
        "source",
        "date",
        "target_version",
    }
    assert {
        "evaluation_id",
        "analysis_id",
        "workspace_id",
        "dataset_id",
        "evaluator_version",
        "status",
        "score",
        "report_payload",
        "trace_id",
        "created_at",
    } == {
        column["name"]
        for column in inspector.get_columns("filing_investigation_evaluations")
    }
    with engine.begin() as connection:
        assert (
            connection.execute(text("SELECT version_num FROM alembic_version")).scalar_one()
            == "20260904_0013"
        )
