"""scrutiny_decisions + canonical_entities.surface_mode / public_alias.

Revision ID: 0002_scrutiny_and_surface_mode
Revises: 0001_baseline
Create Date: 2026-07-17
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0002_scrutiny_and_surface_mode"
down_revision = "0001_baseline"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add scrutiny audit table + surface_mode + public_alias columns."""
    op.add_column(
        "canonical_entities",
        sa.Column(
            "surface_mode",
            sa.String(16),
            nullable=False,
            server_default="open",
        ),
    )
    op.add_column(
        "canonical_entities",
        sa.Column("public_alias", sa.Text, nullable=True),
    )

    op.create_table(
        "scrutiny_decisions",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "canonical_id",
            sa.String(36),
            sa.ForeignKey("canonical_entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("classification", sa.String(16), nullable=False),
        sa.Column("decision", sa.String(16), nullable=False),
        sa.Column("signals_used", sa.Text, nullable=False),
        sa.Column("reason", sa.Text, nullable=False),
        sa.Column("decided_by", sa.String(64), nullable=False),
        sa.Column(
            "decided_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index("ix_scrutiny_canonical", "scrutiny_decisions", ["canonical_id"])


def downgrade() -> None:
    """Reverse the P1 additions (dev-only)."""
    op.drop_index("ix_scrutiny_canonical", "scrutiny_decisions")
    op.drop_table("scrutiny_decisions")
    op.drop_column("canonical_entities", "public_alias")
    op.drop_column("canonical_entities", "surface_mode")
