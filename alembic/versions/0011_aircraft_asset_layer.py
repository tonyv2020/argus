"""P1 aircraft asset layer — FAA registry tables, fenced closed.

Revision ID: 0011_aircraft_asset_layer
Revises: 0010_anchor_external_ids
Create Date: 2026-08-29

Three standalone PG-truth tables for the FAA Releasable Aircraft
Database (helen decision doc, 2026-08-29):

  * ``aircraft_source_snapshots`` — one row per download (provenance)
  * ``aircraft_reference``        — ACFTREF.txt, manufacturer/model
  * ``aircraft``                  — MASTER.txt, one registration

Deliberately NOT touched: ``EntityType`` gains no member, no
``canonical_entities`` / ``canonical_edges`` rows are written, the
Neo4j projection is not extended, and the public read path does not
learn these tables exist. P1 is ingest + fence only; resolution and
surfacing are P2 and are Tony's call.

THE FENCE IS STRUCTURAL. ``MASTER.txt`` contains the home street
address of every individual registrant. Rather than trusting an
application default, both gates are pinned by CHECK constraint:

  * ``ck_aircraft_p1_suppress``          surface_mode = 'suppress'
  * ``ck_aircraft_p1_staged``            publication_state = 'staged'
  * ``ck_aircraft_reference_p1_staged``  publication_state = 'staged'

An UPDATE that tries to surface a row therefore FAILS at the
database. Opening the fence means dropping a named constraint in a
follow-up migration — a reviewable schema change rather than a
one-line UPDATE. That is the intent: make surfacing hard to do by
accident.

``aircraft_reference`` takes only the lifecycle gate — it holds
manufacturer/model reference data with no personal information, and
a ``surface_mode`` column there would imply a privacy claim it is
not making.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0011_aircraft_asset_layer"
down_revision = "0010_anchor_external_ids"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create the three aircraft tables with the fence pinned closed."""

    op.create_table(
        "aircraft_source_snapshots",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("source_url", sa.Text(), nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("bytes_len", sa.BigInteger(), nullable=False),
        sa.Column("source_last_modified", sa.Text(), nullable=True),
        sa.Column(
            "fetched_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("batch_id", sa.String(64), nullable=False),
        sa.Column("master_rows", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("acftref_rows", sa.Integer(), nullable=False, server_default="0"),
        sa.UniqueConstraint("batch_id", name="uq_aircraft_snapshot_batch"),
    )
    op.create_index("ix_aircraft_snapshot_sha256", "aircraft_source_snapshots", ["sha256"])

    op.create_table(
        "aircraft_reference",
        sa.Column("code", sa.String(7), primary_key=True),
        sa.Column("mfr", sa.Text(), nullable=True),
        sa.Column("model", sa.Text(), nullable=True),
        sa.Column("type_acft", sa.String(8), nullable=True),
        sa.Column("type_eng", sa.String(8), nullable=True),
        sa.Column("ac_cat", sa.String(8), nullable=True),
        sa.Column("build_cert_ind", sa.String(8), nullable=True),
        sa.Column("no_eng", sa.Integer(), nullable=True),
        sa.Column("no_seats", sa.Integer(), nullable=True),
        sa.Column("ac_weight", sa.String(16), nullable=True),
        sa.Column("speed", sa.Integer(), nullable=True),
        sa.Column("tc_data_sheet", sa.Text(), nullable=True),
        sa.Column("tc_data_holder", sa.Text(), nullable=True),
        sa.Column(
            "publication_state", sa.String(16), nullable=False, server_default="staged"
        ),
        sa.Column(
            "snapshot_id",
            sa.String(36),
            sa.ForeignKey("aircraft_source_snapshots.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("batch_id", sa.String(64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "publication_state = 'staged'", name="ck_aircraft_reference_p1_staged"
        ),
    )
    op.create_index("ix_aircraft_reference_mfr", "aircraft_reference", ["mfr"])

    op.create_table(
        "aircraft",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("unique_id", sa.String(16), nullable=False),
        sa.Column("n_number", sa.String(10), nullable=False),
        sa.Column("serial_number", sa.Text(), nullable=True),
        sa.Column("mfr_mdl_code", sa.String(7), nullable=True),
        sa.Column("eng_mfr_mdl", sa.String(7), nullable=True),
        sa.Column("year_mfr", sa.Integer(), nullable=True),
        # ── registrant (PII) ──
        sa.Column("type_registrant", sa.String(2), nullable=True),
        sa.Column("registrant_name", sa.Text(), nullable=True),
        sa.Column("street", sa.Text(), nullable=True),
        sa.Column("street2", sa.Text(), nullable=True),
        sa.Column("city", sa.Text(), nullable=True),
        sa.Column("state", sa.String(4), nullable=True),
        sa.Column("zip_code", sa.String(16), nullable=True),
        sa.Column("region", sa.String(4), nullable=True),
        sa.Column("county", sa.String(8), nullable=True),
        sa.Column("country", sa.String(4), nullable=True),
        sa.Column(
            "other_names",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default="[]",
        ),
        # ── registration facts ──
        sa.Column("last_action_date", sa.Date(), nullable=True),
        sa.Column("cert_issue_date", sa.Date(), nullable=True),
        sa.Column("certification", sa.Text(), nullable=True),
        sa.Column("type_aircraft", sa.String(4), nullable=True),
        sa.Column("type_engine", sa.String(4), nullable=True),
        sa.Column("status_code", sa.String(4), nullable=True),
        sa.Column("mode_s_code", sa.String(16), nullable=True),
        sa.Column("mode_s_code_hex", sa.String(16), nullable=True),
        sa.Column("fract_owner", sa.Boolean(), nullable=True),
        sa.Column("air_worth_date", sa.Date(), nullable=True),
        sa.Column("expiration_date", sa.Date(), nullable=True),
        sa.Column("kit_mfr", sa.Text(), nullable=True),
        sa.Column("kit_model", sa.Text(), nullable=True),
        # ── the fence ──
        sa.Column("surface_mode", sa.String(16), nullable=False, server_default="suppress"),
        sa.Column(
            "publication_state", sa.String(16), nullable=False, server_default="staged"
        ),
        # ── provenance ──
        sa.Column(
            "snapshot_id",
            sa.String(36),
            sa.ForeignKey("aircraft_source_snapshots.id", ondelete="SET NULL"),
            nullable=True,
        ),
        sa.Column("batch_id", sa.String(64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.UniqueConstraint("unique_id", name="uq_aircraft_unique_id"),
        sa.CheckConstraint("surface_mode = 'suppress'", name="ck_aircraft_p1_suppress"),
        sa.CheckConstraint("publication_state = 'staged'", name="ck_aircraft_p1_staged"),
    )
    op.create_index("ix_aircraft_n_number", "aircraft", ["n_number"])
    op.create_index("ix_aircraft_mfr_mdl_code", "aircraft", ["mfr_mdl_code"])
    op.create_index("ix_aircraft_batch_id", "aircraft", ["batch_id"])
    op.create_index("ix_aircraft_registrant_name", "aircraft", ["registrant_name"])


def downgrade() -> None:
    """Drop the aircraft tables. P1 added no columns to existing tables."""

    op.drop_index("ix_aircraft_registrant_name", table_name="aircraft")
    op.drop_index("ix_aircraft_batch_id", table_name="aircraft")
    op.drop_index("ix_aircraft_mfr_mdl_code", table_name="aircraft")
    op.drop_index("ix_aircraft_n_number", table_name="aircraft")
    op.drop_table("aircraft")

    op.drop_index("ix_aircraft_reference_mfr", table_name="aircraft_reference")
    op.drop_table("aircraft_reference")

    op.drop_index("ix_aircraft_snapshot_sha256", table_name="aircraft_source_snapshots")
    op.drop_table("aircraft_source_snapshots")
