"""P3.4 mechanism — curated individual allowlist. Surfaces nothing yet.

Revision ID: 0014_aircraft_individual_allowlist
Revises: 0013_aircraft_publish_mechanism
Create Date: 2026-08-30

P3.1 measured ~90% false positives on individual name matching, so the
blanket rule became "no individual surfaces". This table is the narrow,
curated exception: an explicit per-tail-number approval, carrying the
evidence that justified it and who added it.

**The table ships EMPTY.** Nothing is proposed into it by this
migration and nothing promotes from it — the gate requires
``status='approved'``, which only Tony sets.

Design notes:

  * Keyed on ``n_number`` + ``canonical_id`` — approval is per AIRCRAFT,
    not per person. Approving "Sam Graves is a public figure" must not
    silently sweep in a second aircraft that appears in a later FAA
    snapshot under the same name.
  * ``evidence`` and ``source`` are NOT NULL and CHECK-ed non-empty. An
    entry no one can justify later is exactly what this table exists to
    prevent, and it is the same discipline as the promotion audit.
  * ``status`` defaults to ``proposed``. Fail-closed: the gate reads
    only ``approved``.
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0014_aircraft_individual_allowlist"
down_revision = "0013_aircraft_publish_mechanism"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the (empty) curated individual allowlist."""
    op.create_table(
        "aircraft_individual_allowlist",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("n_number", sa.String(10), nullable=False),
        sa.Column("registrant_name", sa.Text(), nullable=False),
        sa.Column(
            "canonical_id",
            sa.String(36),
            sa.ForeignKey("canonical_entities.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("evidence", sa.Text(), nullable=False),
        sa.Column("source", sa.Text(), nullable=False),
        sa.Column("added_by", sa.Text(), nullable=False),
        sa.Column("status", sa.String(16), nullable=False, server_default="proposed"),
        sa.Column("approved_by", sa.Text(), nullable=True),
        sa.Column("approved_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("n_number", "canonical_id", name="uq_aircraft_allowlist_pair"),
        sa.CheckConstraint(
            "status IN ('proposed','approved','rejected')",
            name="ck_aircraft_allowlist_status",
        ),
        sa.CheckConstraint(
            "length(evidence) > 0 and length(source) > 0 and length(added_by) > 0",
            name="ck_aircraft_allowlist_justified",
        ),
        # An approved row must say who approved it and when.
        sa.CheckConstraint(
            "status <> 'approved' or (approved_by is not null and approved_at is not null)",
            name="ck_aircraft_allowlist_approval_attributed",
        ),
    )
    op.create_index(
        "ix_aircraft_allowlist_status", "aircraft_individual_allowlist", ["status"]
    )


def downgrade() -> None:
    """Drop the allowlist. Fails if rows exist — deliberately."""
    op.drop_index("ix_aircraft_allowlist_status", table_name="aircraft_individual_allowlist")
    op.drop_table("aircraft_individual_allowlist")
