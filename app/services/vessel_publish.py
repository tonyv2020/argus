"""The audited, reversible vessel promotion op.

**Nothing calls this yet.** The mechanism exists so a Tony-approved
publish has a reviewed path; no code path, endpoint or job invokes it in
this phase, and a test asserts that.

Mirrors :mod:`app.services.aircraft_publish`, including why each
property is enforced here rather than by convention:

  * **Audited.** Every call writes a ``VesselPromotionAudit`` row in the
    same transaction as the mutation, with actor, reason and the
    before/after of BOTH gates. The DB rejects an unattributed one.
  * **Reversible.** :func:`demote` is the exact inverse and is itself
    audited, so an unwind leaves a trail rather than erasing one.
  * **Per-row.** No bulk "publish everything" verb. Callers pass ids.
"""

from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    PublicationState,
    SurfaceMode,
    Vessel,
    VesselOwnershipEdge,
    VesselPromotionAudit,
)

logger = logging.getLogger(__name__)

_TABLES = {"vessels": Vessel, "vessel_ownership_edges": VesselOwnershipEdge}


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
