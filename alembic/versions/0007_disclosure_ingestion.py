"""D1 — financial-disclosure ingestion ledger.

Revision ID: 0007_disclosure_ingestion
Revises: 0006_cw_survive
Create Date: 2026-08-05

Creates the two D1 tables that make PDF ingestion auditable:

* ``disclosure_documents`` — one row per archived source PDF
  (form_type, oge_url, sha256, filed_date, page_count, storage_path).
  The PDF bytes themselves live at ``storage_path`` on the container
  volume (no LFS, no S3 for D1 — just a mount).

* ``disclosure_rows`` — one row per parsed source line.  This is the
  audit ledger between the PDF page and any downstream graph edge
  D2+ emits.  ``parse_confidence`` in {'high','low'}, ``parse_method``
  in {'layout','vision','human'}; D1 only emits ``layout``.

No graph writes.  No changes to any existing table.  Both new tables
are additive; existing indices / FKs untouched.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0007_disclosure_ingestion"
down_revision = "0006_cw_survive"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Create ``disclosure_documents`` + ``disclosure_rows``."""

    op.create_table(
        "disclosure_documents",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("form_type", sa.String(16), nullable=False),  # oge_278e / oge_278t
        sa.Column("filer_name", sa.Text, nullable=False),
        sa.Column("oge_url", sa.Text, nullable=False),
        sa.Column("sha256", sa.String(64), nullable=False),
        sa.Column("filed_date", sa.Date, nullable=True),
        sa.Column("period_start", sa.Date, nullable=True),
        sa.Column("period_end", sa.Date, nullable=True),
        sa.Column("page_count", sa.Integer, nullable=False),
        sa.Column("storage_path", sa.Text, nullable=False),
        sa.Column("bytes_len", sa.BigInteger, nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.UniqueConstraint("sha256", name="uq_disclosure_documents_sha256"),
        sa.Index("ix_disclosure_documents_form_type", "form_type"),
        sa.Index("ix_disclosure_documents_filed_date", "filed_date"),
        sa.CheckConstraint(
            "form_type IN ('oge_278e', 'oge_278t')",
            name="ck_disclosure_documents_form_type",
        ),
    )

    op.create_table(
        "disclosure_rows",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "doc_id",
            sa.String(36),
            sa.ForeignKey("disclosure_documents.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("part", sa.String(32), nullable=False),
        sa.Column("row_index", sa.Integer, nullable=True),
        sa.Column("account_group", sa.Integer, nullable=True),
        sa.Column("page", sa.Integer, nullable=False),
        sa.Column("raw_text", sa.Text, nullable=False),
        sa.Column(
            "parsed",
            postgresql.JSONB().with_variant(sa.JSON(), "sqlite"),
            nullable=False,
            server_default="{}",
        ),
        sa.Column("parse_confidence", sa.String(8), nullable=False),
        sa.Column("parse_method", sa.String(16), nullable=False),
        sa.Column("reason", sa.String(64), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.CheckConstraint(
            "parse_confidence IN ('high', 'low')",
            name="ck_disclosure_rows_parse_confidence",
        ),
        sa.CheckConstraint(
            "parse_method IN ('layout', 'vision', 'human')",
            name="ck_disclosure_rows_parse_method",
        ),
        sa.Index("ix_disclosure_rows_doc_id", "doc_id"),
        sa.Index("ix_disclosure_rows_part", "part"),
        sa.Index("ix_disclosure_rows_parse_confidence", "parse_confidence"),
        sa.Index(
            "ix_disclosure_rows_doc_part_page",
            "doc_id",
            "part",
            "page",
        ),
    )


def downgrade() -> None:
    """Drop D1 tables (no cascade beyond the CASCADE FK)."""
    op.drop_table("disclosure_rows")
    op.drop_table("disclosure_documents")
