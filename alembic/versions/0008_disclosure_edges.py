"""D2 — disclosure edges (metadata on canonical_edges + citation page anchors).

Revision ID: 0008_disclosure_edges
Revises: 0007_disclosure_ingestion
Create Date: 2026-08-05

Adds the structural bits D2 needs to emit cited, band-metadata-carrying
disclosure edges without breaking any existing edge type:

  * ``canonical_edges.edge_metadata`` (JSONB, default {})
        Holds per-edge structured attributes (value_band, band_low,
        band_high, income_type, income_band, account_group, eif,
        position, org_type, year_incurred, rate, term, ...).  D2 stores
        bands as bands + numeric band_low/band_high derivations — never
        a point value on top of a band.
  * ``source_citations.disclosure_row_id`` (VARCHAR(36), FK, NULL)
        Points at the disclosure_rows row that produced this citation.
        Ledger-to-edge reconciliation runs off this column.
  * ``source_citations.page`` (INTEGER, NULL)
        The 1-based PDF page the row was tokenized on. Used by the
        page-anchor URL renderer + helen's row-vs-page spot check.

The two ``source_citations`` columns are nullable so pre-existing
citations (FEC / USAspending / news_permalink / senate_lda / …) don't
need backfill; only D2's OGE citations set them.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0008_disclosure_edges"
down_revision = "0007_disclosure_ingestion"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add ``edge_metadata`` on edges + ``disclosure_row_id`` / ``page``
    on citations."""

    op.add_column(
        "canonical_edges",
        sa.Column(
            "edge_metadata",
            postgresql.JSONB().with_variant(sa.JSON(), "sqlite"),
            nullable=False,
            server_default="{}",
        ),
    )
    op.add_column(
        "source_citations",
        sa.Column("disclosure_row_id", sa.String(36), nullable=True),
    )
    op.add_column(
        "source_citations",
        sa.Column("page", sa.Integer, nullable=True),
    )
    op.create_foreign_key(
        "fk_source_citations_disclosure_row_id",
        "source_citations",
        "disclosure_rows",
        ["disclosure_row_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_index(
        "ix_source_citations_disclosure_row_id",
        "source_citations",
        ["disclosure_row_id"],
    )


def downgrade() -> None:
    """Roll back D2 additive schema."""
    op.drop_index(
        "ix_source_citations_disclosure_row_id",
        table_name="source_citations",
    )
    op.drop_constraint(
        "fk_source_citations_disclosure_row_id",
        "source_citations",
        type_="foreignkey",
    )
    op.drop_column("source_citations", "page")
    op.drop_column("source_citations", "disclosure_row_id")
    op.drop_column("canonical_edges", "edge_metadata")
