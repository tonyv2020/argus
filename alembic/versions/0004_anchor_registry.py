"""P4 — shared anchor registry (single source of truth for ingester targets).

Revision ID: 0004_anchor_registry
Revises: 0003_llm_usage
Create Date: 2026-07-19

Replaces the 4 per-module hardcoded anchor constants (DETENTION_INDUSTRY_PACS
in fec.py, DETENTION_INDUSTRY_RECIPIENTS in usaspending.py,
DETENTION_INDUSTRY_LDA_CLIENTS in senate_lda.py, DEFAULT_ANCHORS in
sec_edgar.py) with one row per curated anchor.

Keying on EXTERNAL IDs (fec_committee_id, sec_cik, fec_candidate_id) rather
than names is the P4 correctness argument (helen 2026-07-19): name-matching
gave us "AMERICA PAC"=FXAIX-fund + "Anduril"=concept + noisy Aventiv fragments.

Adding a new domain becomes a data edit (INSERT into anchor_registry) rather
than a code change; the roster ingester (P1.5-folded) writes Congress members
here; the seed script bootstraps the P1 anchors + Thiel/Musk/surveillance domains.

Columns:
    * ``label`` — human-friendly canonical hint (e.g. "GEO Group",
      "Peter Thiel"), used for logging + as the canonical fallback when
      no external ID resolves.
    * ``entity_type`` — organization / person / pac / committee / bill.
    * ``priority_domain`` — free-text tag ("detention_operators",
      "prison_telecom", "congress", "surveillance", "musk_network") so the
      priority-set-driven ingestion can filter.
    * ``fec_committee_ids`` — JSONB list of FEC committee IDs (a company
      may have multiple: PAC + super-PAC + affiliated committees).
    * ``fec_candidate_ids`` — JSONB list of FEC candidate IDs (person /
      congress members from the roster).
    * ``sec_cik`` — SEC CIK (integer, 10-digit-zero-padded by caller).
    * ``usaspending_recipient_names`` — JSONB list of USAspending recipient
      strings (multiple legal entity variants).
    * ``lda_client_names`` — JSONB list of LDA client_name strings.
    * ``surface_mode`` — open / alias / suppress (defaults 'open' since P4's
      curated set is public entities; person entries via P1.6/P1.7 = open;
      any private-person entry would override).
    * ``notes`` — free-text audit trail (why anchor exists, whose ask).
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision = "0004_anchor_registry"
down_revision = "0003_llm_usage"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "anchor_registry",
        sa.Column("id", postgresql.UUID(as_uuid=True),
                  server_default=sa.text("gen_random_uuid()"),
                  primary_key=True),
        sa.Column("label", sa.Text(), nullable=False),
        sa.Column("entity_type", sa.Text(), nullable=False),
        sa.Column("priority_domain", sa.Text(), nullable=True),
        sa.Column("fec_committee_ids", postgresql.JSONB(),
                  nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("fec_candidate_ids", postgresql.JSONB(),
                  nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("sec_cik", sa.BigInteger(), nullable=True),
        sa.Column("usaspending_recipient_names", postgresql.JSONB(),
                  nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("lda_client_names", postgresql.JSONB(),
                  nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("name_variants", postgresql.JSONB(),
                  nullable=False, server_default=sa.text("'[]'::jsonb")),
        sa.Column("surface_mode", sa.Text(),
                  nullable=False, server_default=sa.text("'open'")),
        sa.Column("canonical_id", sa.String(36),
                  sa.ForeignKey("canonical_entities.id",
                                ondelete="SET NULL"),
                  nullable=True),
        sa.Column("notes", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        sa.UniqueConstraint("label", "entity_type",
                            name="uq_anchor_registry_label_type"),
    )
    op.create_index(
        "ix_anchor_registry_priority_domain",
        "anchor_registry",
        ["priority_domain"],
    )
    op.create_index(
        "ix_anchor_registry_entity_type",
        "anchor_registry",
        ["entity_type"],
    )
    op.create_index(
        "ix_anchor_registry_sec_cik",
        "anchor_registry",
        ["sec_cik"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index("ix_anchor_registry_sec_cik", table_name="anchor_registry")
    op.drop_index("ix_anchor_registry_entity_type",
                  table_name="anchor_registry")
    op.drop_index("ix_anchor_registry_priority_domain",
                  table_name="anchor_registry")
    op.drop_table("anchor_registry")
