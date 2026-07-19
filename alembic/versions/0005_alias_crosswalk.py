"""P2 — alias_crosswalk table for the merge/dedup pass.

Revision ID: 0005_alias_crosswalk
Revises: 0004_anchor_registry
Create Date: 2026-07-19

Fragmentation is real (helen 2026-07-19 20:23Z sweep): CoreCivic 4
fragments, Aventiv 2, ViaPath 3, Palantir 7 (5 of which are legit
lobbying firms, 2 real dupes). Merge decisions cannot be fully
automated — a lobbying firm 'X ON BEHALF OF PALANTIR' is a real
distinct entity, not a Palantir alias.

Design (helen spec §2 P2 + privacy guardrail §3):

Every merge is a CURATED ROW in ``alias_crosswalk``:
    * ``from_id`` — the canonical to be merged AWAY
    * ``to_id`` — the surviving canonical
    * ``reason`` — free-text audit trail (SEC former-name, explicit
      curation, embedding-similarity above threshold, etc.)
    * ``applied_at`` — set when the merge runs; NULL means pending

The merge runner (``app/services/ingest/merge_pass.py``) reads pending
rows + repoints edges + citations + aliases + anchor_registry rows +
sets ``applied_at``. It's dry-runnable + idempotent (a re-applied row
is a no-op).

Fail-closed on surface_mode (spec §3): the surviving canonical
inherits the MOST-protected surface_mode. NEVER let a merge surface
a suppressed identity.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0005_alias_crosswalk"
down_revision = "0004_anchor_registry"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "alias_crosswalk",
        sa.Column("id", sa.String(36), primary_key=True,
                  server_default=sa.text(
                      "gen_random_uuid()::text"
                  )),
        sa.Column("from_id", sa.String(36),
                  sa.ForeignKey("canonical_entities.id",
                                ondelete="CASCADE"),
                  nullable=False),
        sa.Column("to_id", sa.String(36),
                  sa.ForeignKey("canonical_entities.id",
                                ondelete="CASCADE"),
                  nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column("applied_at", sa.DateTime(timezone=True),
                  nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True),
                  server_default=sa.text("now()"), nullable=False),
        # A canonical may be merged AWAY at most once — after that its
        # id is gone.
        sa.UniqueConstraint("from_id", name="uq_alias_crosswalk_from"),
        # Guard rail — never a self-merge.
        sa.CheckConstraint("from_id <> to_id",
                           name="ck_alias_crosswalk_not_self"),
    )
    op.create_index(
        "ix_alias_crosswalk_to_id",
        "alias_crosswalk",
        ["to_id"],
    )
    op.create_index(
        "ix_alias_crosswalk_applied_at",
        "alias_crosswalk",
        ["applied_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_alias_crosswalk_applied_at",
                  table_name="alias_crosswalk")
    op.drop_index("ix_alias_crosswalk_to_id",
                  table_name="alias_crosswalk")
    op.drop_table("alias_crosswalk")
