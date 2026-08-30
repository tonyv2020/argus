"""Vessels P1 — standalone vessel asset layer, fenced closed.

Revision ID: 0015_vessel_asset_layer
Revises: 0014_aircraft_allowlist
Create Date: 2026-08-30

Design: helen-k3s/docs/argus-vessels-asset-layer-design.md. Second
physical-asset class after aircraft. P1 is **ingest + fence only** — no
read path, no Neo4j, no entity resolution, nothing surfaces.

Every aircraft lesson is baked in from the start rather than retrofitted:

  * **Structural fence.** ``surface_mode``/``publication_state`` pinned by
    equality CHECK, exactly as aircraft P1 (migration 0011). Vessels P3
    relaxes them to membership checks alongside an audited promotion op,
    the way aircraft 0013 did. Pinning first means P1 cannot surface even
    by accident.
  * **Structural citation.** ``snapshot_id``/``source_url``/
    ``source_sha256`` NOT NULL plus a CHECK that the url is non-empty and
    the digest is a full 64 hex chars — an uncited vessel is
    unrepresentable, not merely discouraged.
  * **Source-agnostic.** ``source`` + ``source_key`` is the natural key,
    so USCG documentation slots in beside OFAC without a schema change.
  * **Owner PII is stored for matching only** and never surfaced. P1 has
    no read path at all, which is the strongest form of that guarantee.
  * Migration id is 23 chars — the aircraft 0014 attempt was 34 against
    ``alembic_version.version_num varchar(32)`` and CrashLoopBackOff'd.
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0015_vessel_asset_layer"
down_revision = "0014_aircraft_allowlist"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the vessel snapshot + vessel tables, fenced closed."""
    op.create_table(
        "vessel_source_snapshots",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("bytes_len", sa.BigInteger(), nullable=False),
        sa.Column("source_last_modified", sa.Text(), nullable=True),
        sa.Column(
            "fetched_at", sa.DateTime(timezone=True),
            server_default=sa.func.now(), nullable=False,
        ),
        sa.Column("batch_id", sa.String(64), nullable=False),
        sa.Column("rows_ingested", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint("batch_id", name="uq_vessel_snapshot_batch"),
        sa.CheckConstraint(
            "source IN ('ofac_sdn','uscg_nvdc')", name="ck_vessel_snapshot_source"
        ),
    )
    op.create_index("ix_vessel_snapshot_sha256", "vessel_source_snapshots", ["sha256"])

    op.create_table(
        "vessels",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("source", sa.String(32), nullable=False),
        sa.Column("source_key", sa.String(64), nullable=False),
        sa.Column("vessel_name", sa.Text(), nullable=False),
        sa.Column("imo_number", sa.String(16), nullable=True),
        sa.Column("call_sign", sa.String(32), nullable=True),
        sa.Column("flag", sa.Text(), nullable=True),
        sa.Column("vessel_type", sa.Text(), nullable=True),
        sa.Column("tonnage", sa.String(32), nullable=True),
        sa.Column("gross_tonnage", sa.String(32), nullable=True),
        sa.Column("hull_number", sa.String(64), nullable=True),
        # ── owner (PII-bearing; stored for matching, never surfaced) ──
        sa.Column("owner_name_raw", sa.Text(), nullable=True),
        sa.Column("owner_street", sa.Text(), nullable=True),
        sa.Column("owner_city", sa.Text(), nullable=True),
        sa.Column("owner_state", sa.String(64), nullable=True),
        sa.Column("owner_postal_code", sa.String(32), nullable=True),
        sa.Column("owner_country", sa.String(64), nullable=True),
        # ── sanctions (OFAC) ──
        sa.Column("sanctions_program", sa.Text(), nullable=True),
        sa.Column("sanctions_remarks", sa.Text(), nullable=True),
        sa.Column("is_sanctioned", sa.Boolean(), nullable=False, server_default=sa.false()),
        # ── the fence ──
        sa.Column("surface_mode", sa.String(16), nullable=False, server_default="suppress"),
        sa.Column("publication_state", sa.String(16), nullable=False, server_default="staged"),
        # ── structural citation ──
        sa.Column(
            "snapshot_id", sa.String(36),
            sa.ForeignKey("vessel_source_snapshots.id", ondelete="RESTRICT"), nullable=False,
        ),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("source_sha256", sa.String(64), nullable=False),
        sa.Column("batch_id", sa.String(64), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.func.now(), nullable=False,
        ),
        sa.Column(
            "updated_at", sa.DateTime(timezone=True),
            server_default=sa.func.now(), nullable=False,
        ),
        sa.UniqueConstraint("source", "source_key", name="uq_vessel_source_key"),
        sa.CheckConstraint("source IN ('ofac_sdn','uscg_nvdc')", name="ck_vessel_source"),
        # THE FENCE — P1 pins both gates closed.
        sa.CheckConstraint("surface_mode = 'suppress'", name="ck_vessel_p1_suppress"),
        sa.CheckConstraint("publication_state = 'staged'", name="ck_vessel_p1_staged"),
        # Citation is unrepresentable if absent.
        sa.CheckConstraint(
            "length(source_url) > 0 and length(source_sha256) = 64",
            name="ck_vessel_cited",
        ),
        sa.CheckConstraint("length(vessel_name) > 0", name="ck_vessel_named"),
    )
    op.create_index("ix_vessel_name", "vessels", ["vessel_name"])
    op.create_index("ix_vessel_imo", "vessels", ["imo_number"])
    op.create_index("ix_vessel_owner_name", "vessels", ["owner_name_raw"])
    op.create_index("ix_vessel_batch", "vessels", ["batch_id"])
    op.create_index("ix_vessel_sanctioned", "vessels", ["is_sanctioned"])


def downgrade() -> None:
    """Drop the vessel tables. P1 altered no existing table."""
    for ix in ("ix_vessel_sanctioned", "ix_vessel_batch", "ix_vessel_owner_name",
               "ix_vessel_imo", "ix_vessel_name"):
        op.drop_index(ix, table_name="vessels")
    op.drop_table("vessels")
    op.drop_index("ix_vessel_snapshot_sha256", table_name="vessel_source_snapshots")
    op.drop_table("vessel_source_snapshots")
