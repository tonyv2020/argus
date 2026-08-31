"""The audited, reversible vessel promotion op.

Callers are an **explicit allowlist** — the approved publish scripts and
nothing else. A test enforces it. (Through the P4 mechanism phase the
invariant was the stronger "nobody calls this at all"; the 2026-08-31
all-135 publish is the first approved caller, so the invariant narrows
rather than disappears.)

Mirrors :mod:`app.services.aircraft_publish`, including why each
property is enforced here rather than by convention:

  * **Audited.** Every call writes a ``VesselPromotionAudit`` row in the
    same transaction as the mutation, with actor, reason and the
    before/after of BOTH gates. The DB rejects an unattributed one.
  * **Reversible.** :func:`demote` is the exact inverse and is itself
    audited, so an unwind leaves a trail rather than erasing one.
  * **Per-row.** No bulk "publish everything" verb. Callers pass ids.

THE OWNER CANONICAL IS A THIRD TARGET TABLE. Vessels P3 created the
shadow-fleet operators ``surface_mode='open', publication_state=
'staged'``, so the owner's own dossier is dark until it is published
too. Publishing the edge alone would surface nothing — the entity 404s.
Routing the owner through the SAME op is what keeps the whole publish in
one audit trail and makes :func:`demote` a complete unwind; a hand-run
``UPDATE canonical_entities`` would leave no row to reverse.

That extra reach is deliberately narrow: :func:`promote` REFUSES a
canonical that is not owner-capable, so this op cannot publish a person
canonical whatever a caller passes. :func:`demote` has no such guard —
withdrawing is always allowed.
"""

from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    CanonicalEntity,
    PublicationState,
    SurfaceMode,
    Vessel,
    VesselOwnershipEdge,
    VesselPromotionAudit,
)
from app.services.aircraft_identity import is_individual_entity, is_owner_capable

logger = logging.getLogger(__name__)

_TABLES = {
    "vessels": Vessel,
    "vessel_ownership_edges": VesselOwnershipEdge,
    "canonical_entities": CanonicalEntity,
}


class VesselPromotionError(RuntimeError):
    """A vessel promotion was refused. Message carries no owner PII."""


async def _apply(
    session: AsyncSession,
    *,
    target_table: str,
    target_id: str,
    action: str,
    to_surface_mode: str,
    to_publication_state: str,
    actor: str,
    reason: str,
) -> VesselPromotionAudit:
    """Mutate one row and write its audit entry in the same transaction."""
    if not (actor or "").strip() or not (reason or "").strip():
        raise VesselPromotionError("actor and reason are required for an audited promotion")
    model = _TABLES.get(target_table)
    if model is None:
        raise VesselPromotionError(f"unknown target_table {target_table!r}")
    row = await session.scalar(select(model).where(model.id == target_id))
    if row is None:
        raise VesselPromotionError(f"{target_table} row {target_id!r} not found")

    audit = VesselPromotionAudit(
        target_table=target_table,
        target_id=target_id,
        action=action,
        from_surface_mode=row.surface_mode,
        to_surface_mode=to_surface_mode,
        from_publication_state=row.publication_state,
        to_publication_state=to_publication_state,
        actor=actor.strip(),
        reason=reason.strip(),
    )
    row.surface_mode = to_surface_mode
    row.publication_state = to_publication_state
    session.add(row)
    session.add(audit)
    # Ids and gate values only — never owner_name_raw, never the address.
    logger.info(
        "vessel %s: %s %s -> surface_mode=%s publication_state=%s (actor=%s)",
        action, target_table, target_id, to_surface_mode, to_publication_state, actor,
    )
    return audit


async def _refuse_unless_owner_capable(session: AsyncSession, canonical_id: str) -> None:
    """Refuse to publish an owner canonical that could be a natural person.

    The last line of defence, below every cohort predicate: even a caller
    that hand-picked ids cannot make this op surface a person. Identity
    comes from the ARGUS canonical ``type``, and both helpers fail closed
    on an unknown one, so an unclassifiable owner is withheld.
    """
    ent = await session.scalar(
        select(CanonicalEntity).where(CanonicalEntity.id == canonical_id)
    )
    if ent is None:
        raise VesselPromotionError(f"canonical_entities row {canonical_id!r} not found")
    if is_individual_entity(ent.type, ent.canonical_name) or not is_owner_capable(ent.type):
        # Type only — the name is what we are refusing to disclose.
        raise VesselPromotionError(
            f"refusing to publish canonical {canonical_id!r}: type={ent.type!r} "
            "is not an owner-capable organisation"
        )


async def promote(
    session: AsyncSession,
    *,
    target_table: str,
    target_id: str,
    actor: str,
    reason: str,
    surface_mode: str = SurfaceMode.OPEN.value,
) -> VesselPromotionAudit:
    """Make one vetted row publicly readable. Audited and reversible."""
    if surface_mode == SurfaceMode.SUPPRESS.value:
        raise VesselPromotionError(
            "promote() to surface_mode='suppress' is a no-op by definition; use demote()"
        )
    if target_table == "canonical_entities":
        await _refuse_unless_owner_capable(session, target_id)
    return await _apply(
        session, target_table=target_table, target_id=target_id, action="promote",
        to_surface_mode=surface_mode,
        to_publication_state=PublicationState.PUBLISHED.value,
        actor=actor, reason=reason,
    )


async def demote(
    session: AsyncSession, *, target_table: str, target_id: str, actor: str, reason: str
) -> VesselPromotionAudit:
    """Return one row to the dark state. The exact inverse of :func:`promote`."""
    return await _apply(
        session, target_table=target_table, target_id=target_id, action="demote",
        to_surface_mode=SurfaceMode.SUPPRESS.value,
        to_publication_state=PublicationState.STAGED.value,
        actor=actor, reason=reason,
    )
