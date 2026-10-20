"""Initial task_records migration.

Revision ID: 001_initial
Revises:
Create Date: 2026-10-21 00:00:00.000000

"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "001_initial"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Create task_records table."""
    op.create_table(
        "task_records",
        sa.Column("id", sa.String(length=36), nullable=False),
        sa.Column("celery_task_id", sa.String(length=255), nullable=False),
        sa.Column("task_name", sa.String(length=100), nullable=False),
        sa.Column("task_type", sa.String(length=50), nullable=False),
        sa.Column("status", sa.String(length=30), nullable=False, server_default="pending"),
        sa.Column("priority", sa.String(length=20), nullable=False, server_default="medium"),
        sa.Column("priority_num", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("args", sa.JSON(), nullable=False),
        sa.Column("kwargs", sa.JSON(), nullable=False),
        sa.Column("result", sa.JSON(), nullable=True),
        sa.Column("error_message", sa.Text(), nullable=True),
        sa.Column("retry_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("max_retries", sa.Integer(), nullable=False, server_default="3"),
        sa.Column("scheduled_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("duration_seconds", sa.Float(), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(op.f("ix_task_records_celery_task_id"), "task_records", ["celery_task_id"], unique=True)
    op.create_index(op.f("ix_task_records_priority"), "task_records", ["priority"], unique=False)
    op.create_index(op.f("ix_task_records_status"), "task_records", ["status"], unique=False)
    op.create_index(op.f("ix_task_records_task_name"), "task_records", ["task_name"], unique=False)
    op.create_index(op.f("ix_task_records_task_type"), "task_records", ["task_type"], unique=False)


def downgrade() -> None:
    """Drop task_records table."""
    op.drop_index(op.f("ix_task_records_task_type"), table_name="task_records")
    op.drop_index(op.f("ix_task_records_task_name"), table_name="task_records")
    op.drop_index(op.f("ix_task_records_status"), table_name="task_records")
    op.drop_index(op.f("ix_task_records_priority"), table_name="task_records")
    op.drop_index(op.f("ix_task_records_celery_task_id"), table_name="task_records")
    op.drop_table("task_records")
