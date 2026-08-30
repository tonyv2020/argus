"""P3.0 — relax the aircraft fence to a validity check + audited promotion.

Revision ID: 0013_aircraft_publish_mechanism
Revises: 0012_aircraft_registration_edges
Create Date: 2026-08-30

Design: helen-k3s/docs/argus-p3-aircraft-publish-design.md (locked
2026-08-28). **P3.0 surfaces nothing** — this migration only builds the
mechanism. Zero rows are promoted here or by any code path in P3.0.

WHAT CHANGES. P1/P2 pinned the fence with equality CHECKs
(``surface_mode = 'suppress'``, ``publication_state = 'staged'``), which
made promotion structurally impossible — deliberately, so that opening it
had to be a reviewed migration. This is that migration. The equality
CHECKs become **membership** CHECKs:

    surface_mode      IN ('suppress','alias','open')
    publication_state IN ('staged','published')

Still fail-closed in every way that matters:

  * **Column defaults are unchanged** — ``suppress`` / ``staged``. A row
    written by any existing code path is still born dark. Nothing
    publishes by omission; only an explicit promotion flips a row.
  * The weaker CHECK still **rejects invalid states** — a typo like
    ``publication_state='public'`` or ``surface_mode='visible'`` fails at
    the database exactly as before.
  * The read-gate (RG2 + this phase's aircraft predicates) is what
    actually decides visibility, and it defaults to excluding anything
    not published.

WHAT IS ADDED. ``aircraft_promotion_audit`` — one row per promotion or
demotion, recording actor, reason, and the before/after of BOTH gates.
Promotion is per-row and reversible: the audit trail is what makes a
mistaken promotion diagnosable and undoable rather than merely
overwritten. **No code in P3.0 calls the promotion op.**
"""

from __future__ import annotations

import sqlalchemy as sa

from alembic import op

revision = "0013_aircraft_publish_mechanism"
down_revision = "0012_aircraft_registration_edges"
branch_labels = None
depends_on = None

# (table, old equality constraint names, new membership constraints)
_SURFACE_VALUES = "('suppress','alias','open')"
_PUBSTATE_VALUES = "('staged','published')"


def upgrade() -> None:
    """Swap equality fences for membership fences; add the audit table."""

    # ── aircraft ──
    op.drop_constraint("ck_aircraft_p1_suppress", "aircraft", type_="check")
    op.drop_constraint("ck_aircraft_p1_staged", "aircraft", type_="check")
    op.create_check_constraint(
        "ck_aircraft_surface_mode_valid", "aircraft", f"surface_mode IN {_SURFACE_VALUES}"
    )
    op.create_check_constraint(
        "ck_aircraft_publication_state_valid",
        "aircraft",
        f"publication_state IN {_PUBSTATE_VALUES}",
    )

    # ── aircraft_reference (lifecycle gate only; no privacy gate) ──
    op.drop_constraint(
        "ck_aircraft_reference_p1_staged", "aircraft_reference", type_="check"
    )
    op.create_check_constraint(
        "ck_aircraft_reference_publication_state_valid",
        "aircraft_reference",
        f"publication_state IN {_PUBSTATE_VALUES}",
    )

    # ── aircraft_registration_edges ──
    op.drop_constraint(
        "ck_aircraft_reg_edge_suppress", "aircraft_registration_edges", type_="check"
    )
    op.drop_constraint(
        "ck_aircraft_reg_edge_staged", "aircraft_registration_edges", type_="check"
    )
    op.create_check_constraint(
        "ck_aircraft_reg_edge_surface_mode_valid",
        "aircraft_registration_edges",
        f"surface_mode IN {_SURFACE_VALUES}",
    )
    op.create_check_constraint(
        "ck_aircraft_reg_edge_publication_state_valid",
        "aircraft_registration_edges",
        f"publication_state IN {_PUBSTATE_VALUES}",
    )
    # ck_aircraft_reg_edge_relation and ck_aircraft_reg_edge_cited are
    # untouched: relation stays pinned to 'registers', and an uncited
    # edge stays unrepresentable. Publishing does not relax provenance.

    # ── audit trail ──
    op.create_table(
        "aircraft_promotion_audit",
        sa.Column("id", sa.String(36), primary_key=True),
        #: 'aircraft' or 'aircraft_registration_edges'.
        sa.Column("target_table", sa.String(48), nullable=False),
        sa.Column("target_id", sa.String(36), nullable=False),
        #: 'promote' or 'demote' — demote is the reversal path.
        sa.Column("action", sa.String(16), nullable=False),
        sa.Column("from_surface_mode", sa.String(16), nullable=True),
        sa.Column("to_surface_mode", sa.String(16), nullable=True),
        sa.Column("from_publication_state", sa.String(16), nullable=True),
        sa.Column("to_publication_state", sa.String(16), nullable=True),
        #: Who asked for this. Never defaulted — an unattributed
        #: promotion is the thing this table exists to prevent.
        sa.Column("actor", sa.Text(), nullable=False),
        sa.Column("reason", sa.Text(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.CheckConstraint(
            "action IN ('promote','demote')", name="ck_aircraft_promotion_action"
        ),
        sa.CheckConstraint(
            "length(actor) > 0 and length(reason) > 0",
            name="ck_aircraft_promotion_attributed",
        ),
    )
    op.create_index(
        "ix_aircraft_promotion_target",
        "aircraft_promotion_audit",
        ["target_table", "target_id"],
    )
    op.create_index(
        "ix_aircraft_promotion_created", "aircraft_promotion_audit", ["created_at"]
    )


def downgrade() -> None:
    """Re-pin the P1/P2 equality fences.

    This will FAIL if anything has been promoted — by design. Reverting
    the mechanism while published rows exist would silently re-hide them
    with no record; the failure forces an explicit demotion first.
    """
    op.drop_index("ix_aircraft_promotion_created", table_name="aircraft_promotion_audit")
    op.drop_index("ix_aircraft_promotion_target", table_name="aircraft_promotion_audit")
    op.drop_table("aircraft_promotion_audit")

    op.drop_constraint(
        "ck_aircraft_reg_edge_publication_state_valid",
        "aircraft_registration_edges",
        type_="check",
    )
    op.drop_constraint(
        "ck_aircraft_reg_edge_surface_mode_valid",
        "aircraft_registration_edges",
        type_="check",
    )
    op.create_check_constraint(
        "ck_aircraft_reg_edge_suppress",
        "aircraft_registration_edges",
        "surface_mode = 'suppress'",
    )
    op.create_check_constraint(
        "ck_aircraft_reg_edge_staged",
        "aircraft_registration_edges",
        "publication_state = 'staged'",
    )

    op.drop_constraint(
        "ck_aircraft_reference_publication_state_valid",
        "aircraft_reference",
        type_="check",
    )
    op.create_check_constraint(
        "ck_aircraft_reference_p1_staged", "aircraft_reference", "publication_state = 'staged'"
    )

    op.drop_constraint("ck_aircraft_publication_state_valid", "aircraft", type_="check")
    op.drop_constraint("ck_aircraft_surface_mode_valid", "aircraft", type_="check")
    op.create_check_constraint("ck_aircraft_p1_suppress", "aircraft", "surface_mode = 'suppress'")
    op.create_check_constraint("ck_aircraft_p1_staged", "aircraft", "publication_state = 'staged'")
