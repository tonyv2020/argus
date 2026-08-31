"""Vessels publish mechanism — relax the fence + audited promotion.

Revision ID: 0017_vessel_publish_mech
Revises: 0016_vessel_owner_edges
Create Date: 2026-08-31

Mirrors aircraft ``0013_aircraft_publish_mechanism`` exactly, including
the reasoning. **Surfaces nothing** — this builds the mechanism; zero
rows are promoted here or by any code path in this phase.

Vessels P1/P3 pinned the fence with equality CHECKs, which made
promotion structurally impossible on purpose. This is the reviewed
migration that opens it, and only to a **membership** check:

    surface_mode      IN ('suppress','alias','open')
    publication_state IN ('staged','published')

Still fail-closed in every way that matters:

  * **Column defaults are unchanged** — ``suppress`` / ``staged``. Any
    row written by the ingest path is still born dark; nothing
    publishes by omission.
  * A typo like ``publication_state='public'`` still fails at the
    database, exactly as before.
  * The read-gate decides visibility and defaults to excluding
    anything not published.

``vessel_promotion_audit`` records actor, reason and the before/after
of BOTH gates per promotion or demotion. Promotion is per-row and
reversible; the audit trail is what makes a mistaken promotion
diagnosable and undoable rather than merely overwritten.
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0017_vessel_publish_mech"
down_revision = "0016_vessel_owner_edges"
branch_labels = None
depends_on = None

_SURFACE = "('suppress','alias','open')"
_PUBSTATE = "('staged','published')"


def upgrade() -> None:
    """Swap equality fences for membership fences; add the audit table."""
    op.drop_constraint("ck_vessel_p1_suppress", "vessels", type_="check")
    op.drop_constraint("ck_vessel_p1_staged", "vessels", type_="check")
    op.create_check_constraint(
        "ck_vessel_surface_mode_valid", "vessels", f"surface_mode IN {_SURFACE}"
    )
    op.create_check_constraint(
        "ck_vessel_publication_state_valid", "vessels", f"publication_state IN {_PUBSTATE}"
    )

    op.drop_constraint("ck_vessel_owner_suppress", "vessel_ownership_edges", type_="check")
    op.drop_constraint("ck_vessel_owner_staged", "vessel_ownership_edges", type_="check")
    op.create_check_constraint(
        "ck_vessel_owner_surface_mode_valid",
        "vessel_ownership_edges",
        f"surface_mode IN {_SURFACE}",
    )
    op.create_check_constraint(
        "ck_vessel_owner_publication_state_valid",
        "vessel_ownership_edges",
        f"publication_state IN {_PUBSTATE}",
    )
    # ck_vessel_owner_relation and ck_vessel_owner_cited are untouched:
    # publishing relaxes visibility, never provenance.

    op.create_table(
        "vessel_promotion_audit",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("target_table", sa.String(48), nullable=False),
        sa.Column("target_id", sa.String(36), nullable=False),
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("from_surface_mode", sa.String(16), nullable=True),
        sa.Column("to_surface_mode", sa.String(16), nullable=True),
        sa.Column("from_publication_state", sa.String(16), nullable=True),
        sa.Column("to_publication_state", sa.String(16), nullable=True),
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True),
            server_default=sa.func.now(), nullable=False,
        ),
        sa.CheckConstraint(
            "action IN ('promote','demote')", name="ck_vessel_promotion_action"
        ),
        sa.CheckConstraint(
            "length(actor) > 0 and length(reason) > 0",
            name="ck_vessel_promotion_attributed",
        ),
    )
    op.create_index(
        "ix_vessel_promotion_target", "vessel_promotion_audit", ["target_table", "target_id"]
    )


def downgrade() -> None:
    """Re-pin the equality fences. FAILS if anything is published — by design."""
    op.drop_index("ix_vessel_promotion_target", table_name="vessel_promotion_audit")
    op.drop_table("vessel_promotion_audit")
    op.drop_constraint(
        "ck_vessel_owner_publication_state_valid", "vessel_ownership_edges", type_="check"
    )
    op.drop_constraint(
        "ck_vessel_owner_surface_mode_valid", "vessel_ownership_edges", type_="check"
    )
    op.create_check_constraint(
        "ck_vessel_owner_suppress", "vessel_ownership_edges", "surface_mode = 'suppress'"
    )
    op.create_check_constraint(
        "ck_vessel_owner_staged", "vessel_ownership_edges", "publication_state = 'staged'"
    )
    op.drop_constraint("ck_vessel_publication_state_valid", "vessels", type_="check")
    op.drop_constraint("ck_vessel_surface_mode_valid", "vessels", type_="check")
    op.create_check_constraint("ck_vessel_p1_suppress", "vessels", "surface_mode = 'suppress'")
    op.create_check_constraint("ck_vessel_p1_staged", "vessels", "publication_state = 'staged'")
