"""P2 fix — alias_crosswalk audit rows survive canonical delete.

Revision ID: 0006_alias_crosswalk_survive_delete
Revises: 0005_alias_crosswalk
Create Date: 2026-07-19

The initial 0005 declared ``from_id`` / ``to_id`` as ``ON DELETE CASCADE``
which cascade-wiped the audit row when the merge ran (the whole point is
that ``from_id`` is deleted). That produced a StaleDataError on the
concurrent ``applied_at`` UPDATE from a running merge (2026-07-19 20:37Z
live incident).

Change to ``ON DELETE SET NULL`` — the audit row survives the merge with
NULL FKs but the reason + applied_at + timestamps intact. Add columns
``from_id_frozen`` / ``to_id_frozen`` (plain UUIDs, no FK) that capture
the original references so post-merge audit can still trace the chain.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0006_cw_survive"
down_revision = "0005_alias_crosswalk"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # Add the frozen-copy columns (nullable initially; a trigger or the
    # merge runner populates them at apply time).
    op.add_column(
        "alias_crosswalk",
        sa.Column("from_id_frozen", sa.String(36), nullable=True),
    )
    op.add_column(
        "alias_crosswalk",
        sa.Column("to_id_frozen", sa.String(36), nullable=True),
    )
    # Rewire the FKs to SET NULL — audit row survives canonical delete.
    op.drop_constraint(
        "alias_crosswalk_from_id_fkey",
        "alias_crosswalk",
        type_="foreignkey",
    )
    op.drop_constraint(
        "alias_crosswalk_to_id_fkey",
        "alias_crosswalk",
        type_="foreignkey",
    )
    op.alter_column(
        "alias_crosswalk",
        "from_id",
        existing_type=sa.String(36),
        nullable=True,
    )
    op.alter_column(
        "alias_crosswalk",
        "to_id",
        existing_type=sa.String(36),
        nullable=True,
    )
    op.create_foreign_key(
        "alias_crosswalk_from_id_fkey",
        "alias_crosswalk",
        "canonical_entities",
        ["from_id"],
        ["id"],
        ondelete="SET NULL",
    )
    op.create_foreign_key(
        "alias_crosswalk_to_id_fkey",
        "alias_crosswalk",
        "canonical_entities",
        ["to_id"],
        ["id"],
        ondelete="SET NULL",
    )


def downgrade() -> None:
    op.drop_constraint(
        "alias_crosswalk_from_id_fkey",
        "alias_crosswalk",
        type_="foreignkey",
    )
    op.drop_constraint(
        "alias_crosswalk_to_id_fkey",
        "alias_crosswalk",
        type_="foreignkey",
    )
    op.alter_column(
        "alias_crosswalk",
        "from_id",
        existing_type=sa.String(36),
        nullable=False,
    )
    op.alter_column(
        "alias_crosswalk",
        "to_id",
        existing_type=sa.String(36),
        nullable=False,
    )
    op.create_foreign_key(
        "alias_crosswalk_from_id_fkey",
        "alias_crosswalk",
        "canonical_entities",
        ["from_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.create_foreign_key(
        "alias_crosswalk_to_id_fkey",
        "alias_crosswalk",
        "canonical_entities",
        ["to_id"],
        ["id"],
        ondelete="CASCADE",
    )
    op.drop_column("alias_crosswalk", "to_id_frozen")
    op.drop_column("alias_crosswalk", "from_id_frozen")
