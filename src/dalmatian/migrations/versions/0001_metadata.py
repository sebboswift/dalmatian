"""Create Dalmatian metadata tables."""

import sqlalchemy as sa
from alembic import op

revision = "0001_metadata"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "dalmatian_invocations",
        sa.Column("id", sa.String(length=64), primary_key=True),
        sa.Column("kind", sa.String(length=16), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("request_json", sa.Text(), nullable=False),
        sa.Column("output_mode", sa.String(length=16)),
        sa.Column("output_format", sa.String(length=16)),
        sa.Column("output_location", sa.String(length=128)),
        sa.Column("output_path", sa.Text()),
        sa.Column("cached", sa.Boolean()),
        sa.Column("rows_returned", sa.BigInteger()),
        sa.Column("bytes_returned", sa.BigInteger()),
        sa.Column("truncated", sa.Boolean()),
        sa.Column("manifest_uri", sa.Text()),
        sa.Column("data_uri", sa.Text()),
        sa.Column("source_versions_json", sa.Text()),
        sa.Column("error_class", sa.String(length=255)),
        sa.Column("error_message", sa.Text()),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("started_at", sa.DateTime(timezone=True)),
        sa.Column("finished_at", sa.DateTime(timezone=True)),
        sa.Column("duration_ms", sa.BigInteger()),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_table(
        "dalmatian_storage_locations",
        sa.Column("name", sa.String(length=128), primary_key=True),
        sa.Column("uri", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
    )


def downgrade() -> None:
    op.drop_table("dalmatian_storage_locations")
    op.drop_table("dalmatian_invocations")
