"""P2 — aircraft REGISTERS edges, fenced closed and structurally cited.

Revision ID: 0012_aircraft_registration_edges
Revises: 0011_aircraft_asset_layer
Create Date: 2026-08-29

One table, ``aircraft_registration_edges``: canonical entity ->
aircraft, relation ``registers``.

NOT a ``canonical_edges`` row. A canonical edge needs both endpoints
in ``canonical_entities``, and P1/P2 deliberately do not make aircraft
an ``EntityType`` — so the graph form is unavailable without a schema
change nobody approved. This is the join-table form (design doc
option (a)).

**Citation is structural.** ``snapshot_id`` / ``source_url`` /
``source_sha256`` are NOT NULL, plus a CHECK that the url is non-empty
and the digest is a full 64 hex chars — so an uncited edge cannot be
written at all. That is the table-level analogue of the
0-uncited-edges invariant, enforced by the schema rather than by a
sweep that finds violations after they exist.

The fence is pinned exactly as on ``aircraft`` (migration 0011):
``ck_aircraft_reg_edge_suppress`` and ``ck_aircraft_reg_edge_staged``.
Publishing is P3 and is Tony's call, and requires dropping a named
constraint in a follow-up migration.
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0012_aircraft_registration_edges"
down_revision = "0011_aircraft_asset_layer"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the fenced, structurally-cited REGISTERS join table."""

    op.create_table(
        "aircraft_registration_edges",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "canonical_id",
            sa.String(36),
            sa.ForeignKey("canonical_entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "aircraft_id",
            sa.String(36),
            sa.ForeignKey("aircraft.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("relation", sa.String(16), nullable=False, server_default="registers"),
        sa.Column("match_tier", sa.String(24), nullable=False),
        sa.Column("match_score", sa.Float(), nullable=False),
        sa.Column("matched_via", sa.Text(), nullable=True),
        sa.Column("registrant_name_raw", sa.Text(), nullable=False),
        # citation — all NOT NULL
        sa.Column(
            "snapshot_id",
            sa.String(36),
            sa.ForeignKey("aircraft_source_snapshots.id", ondelete="RESTRICT"),
            nullable=False,
        ),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_sha256", sa.String(64), nullable=False),
        # the fence
        sa.Column("surface_mode", sa.String(16), nullable=False, server_default="suppress"),
        sa.Column(
            "publication_state", sa.String(16), nullable=False, server_default="staged"
        ),
        sa.Column("batch_id", sa.String(64), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("canonical_id", "aircraft_id", name="uq_aircraft_reg_edge_pair"),
        sa.CheckConstraint("relation = 'registers'", name="ck_aircraft_reg_edge_relation"),
        sa.CheckConstraint("surface_mode = 'suppress'", name="ck_aircraft_reg_edge_suppress"),
        sa.CheckConstraint(
            "publication_state = 'staged'", name="ck_aircraft_reg_edge_staged"
        ),
        sa.CheckConstraint(
            "length(source_url) > 0 and length(source_sha256) = 64",
            name="ck_aircraft_reg_edge_cited",
        ),
    )
    op.create_index(
        "ix_aircraft_reg_edge_canonical", "aircraft_registration_edges", ["canonical_id"]
    )
    op.create_index(
        "ix_aircraft_reg_edge_aircraft", "aircraft_registration_edges", ["aircraft_id"]
    )
    op.create_index(
        "ix_aircraft_reg_edge_batch", "aircraft_registration_edges", ["batch_id"]
    )


def downgrade() -> None:
    """Drop the REGISTERS table. P2 altered no existing table."""

    op.drop_index("ix_aircraft_reg_edge_batch", table_name="aircraft_registration_edges")
    op.drop_index("ix_aircraft_reg_edge_aircraft", table_name="aircraft_registration_edges")
    op.drop_index("ix_aircraft_reg_edge_canonical", table_name="aircraft_registration_edges")
    op.drop_table("aircraft_registration_edges")
