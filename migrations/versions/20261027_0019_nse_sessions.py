"""Add official NSE per-session source and replica coverage proofs."""

from alembic import op

from trade_data.storage.nse import nse_sessions

revision = "20261027_0019"
down_revision = "20260906_0018"
branch_labels = None
depends_on = None


def upgrade():
    nse_sessions.create(op.get_bind(), checkfirst=True)


def downgrade():
    # Market data and source proofs are intentionally retained on application rollback.
    pass
