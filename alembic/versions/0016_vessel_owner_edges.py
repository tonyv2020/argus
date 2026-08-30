"""Vessels P3 — vessel→owner ownership edges, fenced closed.

Revision ID: 0016_vessel_owner_edges
Revises: 0015_vessel_asset_layer
Create Date: 2026-08-30

The join between the standalone ``vessels`` table and Argus canonicals.
Mirrors ``aircraft_registration_edges`` exactly, including the lessons
that table was retrofitted with:

  * **Fence pinned by equality CHECK** — P3 stages, it does not publish.
    Publishing is a separate Tony-gated step and, as with aircraft
    0013, will require a migration that drops a named constraint. That
    is deliberate: it makes surfacing a reviewable schema change rather
    than an UPDATE anyone can run.
  * **Structural citation** — ``snapshot_id``/``source_url``/
    ``source_sha256`` NOT NULL plus a CHECK that the url is non-empty
    and the digest is a full 64 hex chars. An uncited ownership claim
    is unrepresentable.
  * **Relation pinned** to ``owns`` — a second kind of claim needs its
    own review.
  * ``ofac_relation`` records WHICH OFAC relationship produced the edge
    ("Owned or Controlled By", "Property in the interest of", …), so a
    reader can see the strength of the underlying assertion.
  * Unique on ``(canonical_id, vessel_id)`` so a re-run cannot fan out
    duplicate claims.
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0016_vessel_owner_edges"
down_revision = "0015_vessel_asset_layer"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the fenced, structurally-cited vessel→owner edge table."""
    op.create_table(
        "vessel_ownership_edges",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "canonical_id", sa.String(36),
            sa.ForeignKey("canonical_entities.id", ondelete="CASCADE"), nullable=False,
        ),
        sa.Column(
            "vessel_id", sa.String(36),
            sa.ForeignKey("vessels.id", ondelete="CASCADE"), nullable=False,
        ),
        sa.Column("relation", sa.String(16), nullable=False, server_default="owns"),
        sa.Column("ofac_relation", sa.Text(), nullable=False),
        sa.Column("owner_name_raw", sa.Text(), nullable=False),
        sa.Column("ofac_owner_id", sa.String(32), nullable=False),
        # citation
        sa.Column(
            "snapshot_id", sa.String(36),
            sa.ForeignKey("vessel_source_snapshots.id", ondelete="RESTRICT"), nullable=False,
        ),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_sha256", sa.String(64), nullable=False),
        # the fence
        sa.Column("surface_mode", sa.String(16), nullable=False, server_default="suppress"),
        sa.Column("publication_state", sa.String(16), nullable=False, server_default="staged"),
        sa.Column("batch_id", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.func.now(), nullable=False,
        ),
        sa.UniqueConstraint("canonical_id", "vessel_id", name="uq_vessel_owner_pair"),
        sa.CheckConstraint("relation = 'owns'", name="ck_vessel_owner_relation"),
        sa.CheckConstraint("surface_mode = 'suppress'", name="ck_vessel_owner_suppress"),
        sa.CheckConstraint("publication_state = 'staged'", name="ck_vessel_owner_staged"),
        sa.CheckConstraint(
            "length(source_url) > 0 and length(source_sha256) = 64",
            name="ck_vessel_owner_cited",
        ),
    )
    op.create_index("ix_vessel_owner_canonical", "vessel_ownership_edges", ["canonical_id"])
    op.create_index("ix_vessel_owner_vessel", "vessel_ownership_edges", ["vessel_id"])
    op.create_index("ix_vessel_owner_batch", "vessel_ownership_edges", ["batch_id"])


def downgrade() -> None:
    """Drop the edge table. P3 altered no existing table."""
    for ix in ("ix_vessel_owner_batch", "ix_vessel_owner_vessel", "ix_vessel_owner_canonical"):
        op.drop_index(ix, table_name="vessel_ownership_edges")
    op.drop_table("vessel_ownership_edges")
