"""baseline — canonical entities + edges + citations + aliases.

Revision ID: 0001_baseline
Revises:
Create Date: 2026-07-17
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0001_baseline"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    """Enable pgvector, then create the argus schema."""
    op.execute("CREATE EXTENSION IF NOT EXISTS vector")

    op.create_table(
        "canonical_entities",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("canonical_name", sa.Text, nullable=False),
        sa.Column("canonical_name_normalized", sa.Text, nullable=False),
        sa.Column("type", sa.String(32), nullable=False),
        sa.Column("embedding", sa.dialects.postgresql.ARRAY(sa.Float), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("projected_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.execute(
        "ALTER TABLE canonical_entities "
        "ALTER COLUMN embedding TYPE vector(1024) USING embedding::vector(1024)"
    )
    op.create_index(
        "ix_canonical_entities_type_norm",
        "canonical_entities",
        ["type", "canonical_name_normalized"],
    )
    op.execute(
        "CREATE INDEX ix_canonical_entities_embedding_hnsw "
        "ON canonical_entities USING hnsw (embedding vector_cosine_ops) "
        "WITH (m = 16, ef_construction = 64)"
    )

    op.create_table(
        "entity_aliases",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "canonical_id",
            sa.String(36),
            sa.ForeignKey("canonical_entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("source_system", sa.String(32), nullable=False),
        sa.Column("source_id", sa.String(64), nullable=False),
        sa.Column("surface_name", sa.Text, nullable=False),
        sa.Column("surface_name_normalized", sa.Text, nullable=False),
        sa.Column("kind_hint", sa.String(32), nullable=True),
        sa.Column("role", sa.String(32), nullable=True),
        sa.Column("confidence", sa.Float, nullable=True),
    )
    op.create_index("ix_aliases_norm", "entity_aliases", ["surface_name_normalized"])
    op.create_index(
        "ix_aliases_source", "entity_aliases", ["source_system", "source_id"], unique=True
    )

    op.create_table(
        "canonical_edges",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "source_id",
            sa.String(36),
            sa.ForeignKey("canonical_entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column(
            "target_id",
            sa.String(36),
            sa.ForeignKey("canonical_entities.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("relation", sa.String(32), nullable=False),
        sa.Column("weight", sa.Float, nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("projected_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_edges_source_relation", "canonical_edges", ["source_id", "relation"])
    op.create_index("ix_edges_target_relation", "canonical_edges", ["target_id", "relation"])
    op.create_index(
        "ix_edges_unique",
        "canonical_edges",
        ["source_id", "target_id", "relation"],
        unique=True,
    )

    op.create_table(
        "source_citations",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column(
            "edge_id",
            sa.String(36),
            sa.ForeignKey("canonical_edges.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("kind", sa.String(32), nullable=False),
        sa.Column("citation_url", sa.Text, nullable=False),
        sa.Column("citation_ref", sa.Text, nullable=True),
        sa.Column(
            "seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
    )
    op.create_index("ix_citations_edge", "source_citations", ["edge_id"])


def downgrade() -> None:
    """Reverse baseline (dev-only convenience — prod is forward-only)."""
    op.drop_index("ix_citations_edge", "source_citations")
    op.drop_table("source_citations")
    op.drop_index("ix_edges_unique", "canonical_edges")
    op.drop_index("ix_edges_target_relation", "canonical_edges")
    op.drop_index("ix_edges_source_relation", "canonical_edges")
    op.drop_table("canonical_edges")
    op.drop_index("ix_aliases_source", "entity_aliases")
    op.drop_index("ix_aliases_norm", "entity_aliases")
    op.drop_table("entity_aliases")
    op.execute("DROP INDEX IF EXISTS ix_canonical_entities_embedding_hnsw")
    op.drop_index("ix_canonical_entities_type_norm", "canonical_entities")
    op.drop_table("canonical_entities")
