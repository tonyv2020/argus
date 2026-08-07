"""RG1 — read-gate hardening: publication_state + batch_id on canonicals + edges.

Revision ID: 0009_read_gate_publication_state
Revises: 0008_disclosure_edges
Create Date: 2026-08-07

Adds the content-lifecycle gate for the Argus public read path
(design doc: helen-k3s/docs/argus-read-gate-hardening-design.md).

The COLUMN default is ``published`` (NOT NULL server_default) so
migrating the live corpus leaves every existing row surfaceable —
zero read-path change on deploy day. Individual emitters may
override; only the bulk-disclosure ingester (RG3) writes ``staged``.

  * ``canonical_edges.publication_state``     VARCHAR(16) NOT NULL DEFAULT 'published'
  * ``canonical_edges.batch_id``              VARCHAR(64) NULL
  * ``canonical_entities.publication_state``  VARCHAR(16) NOT NULL DEFAULT 'published'
  * ``canonical_entities.batch_id``           VARCHAR(64) NULL

Indexes:
  * partial ix on ``batch_id`` on both tables (WHERE batch_id IS NOT NULL) —
    keeps the admin publish/unpublish batch scan cheap without paying an
    index for the ~100 % of rows that will never carry a batch id.
  * partial ix on ``canonical_edges`` WHERE publication_state='staged' —
    staged rows are a tiny minority; the ix accelerates "list what's dark"
    admin scans without inflating the hot published-only read path.

Privacy is orthogonal: ``surface_mode`` is untouched. Both gates AND
at read time (a published + suppress entity is still 404).
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0009_read_gate_publication_state"
down_revision = "0008_disclosure_edges"
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Add publication_state + batch_id columns + partial indexes."""

    op.add_column(
        "canonical_edges",
        sa.Column(
            "publication_state",
            sa.String(16),
            nullable=False,
            server_default="published",
        ),
    )
    op.add_column(
        "canonical_edges",
        sa.Column("batch_id", sa.String(64), nullable=True),
    )

    op.add_column(
        "canonical_entities",
        sa.Column(
            "publication_state",
            sa.String(16),
            nullable=False,
            server_default="published",
        ),
    )
    op.add_column(
        "canonical_entities",
        sa.Column("batch_id", sa.String(64), nullable=True),
    )

    op.create_index(
        "ix_canonical_edges_batch_id",
        "canonical_edges",
        ["batch_id"],
        postgresql_where=sa.text("batch_id IS NOT NULL"),
    )
    op.create_index(
        "ix_canonical_entities_batch_id",
        "canonical_entities",
        ["batch_id"],
        postgresql_where=sa.text("batch_id IS NOT NULL"),
    )
    op.create_index(
        "ix_canonical_edges_staged",
        "canonical_edges",
        ["publication_state"],
        postgresql_where=sa.text("publication_state = 'staged'"),
    )


def downgrade() -> None:
    """Reverse RG1 additive schema."""
    op.drop_index("ix_canonical_edges_staged", table_name="canonical_edges")
    op.drop_index("ix_canonical_entities_batch_id", table_name="canonical_entities")
    op.drop_index("ix_canonical_edges_batch_id", table_name="canonical_edges")
    op.drop_column("canonical_entities", "batch_id")
    op.drop_column("canonical_entities", "publication_state")
    op.drop_column("canonical_edges", "batch_id")
    op.drop_column("canonical_edges", "publication_state")
