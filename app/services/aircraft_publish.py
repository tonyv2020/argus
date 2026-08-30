"""P3.0 — the audited, reversible aircraft promotion op.

**Nothing in P3.0 calls this.** The mechanism exists so P3.2 has a
reviewed path to promote a vetted row; no code path, endpoint, CronJob
or sweep invokes it in this phase, and a test asserts that.

Design: helen-k3s/docs/argus-p3-aircraft-publish-design.md.

Three properties the design asks for, each enforced here rather than by
convention:

  * **Audited.** Every call writes an ``AircraftPromotionAudit`` row in
    the same transaction as the mutation, recording actor, reason, and
    the before/after of BOTH gates. Actor and reason are required and
    must be non-empty — the DB rejects an unattributed promotion.
  * **Reversible.** :func:`demote` is the exact inverse and is itself
    audited, so an unwind leaves a trail rather than erasing one.
  * **Per-row.** No bulk "publish everything" verb exists. Callers pass
    explicit ids. That is deliberate: the failure mode this whole arc
    guards against is a mass surfacing nobody reviewed.

The street address is never touched by promotion because it is never
projected or served on any read path — see
:mod:`app.services.read_gate` and the Neo4j ``Aircraft`` projection,
which select an explicit column allowlist rather than the whole row.
"""

from __future__ import annotations

import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    Aircraft,
    AircraftPromotionAudit,
    AircraftRegistrationEdge,
    PublicationState,
    SurfaceMode,
)

logger = logging.getLogger(__name__)

_TABLES = {
    "aircraft": Aircraft,
    "aircraft_registration_edges": AircraftRegistrationEdge,
}


class PromotionError(RuntimeError):
    """A promotion was refused. Message carries no registrant PII."""


def _model_for(target_table: str):
    model = _TABLES.get(target_table)
    if model is None:
        raise PromotionError(f"unknown target_table {target_table!r}")
    return model


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
) -> AircraftPromotionAudit:
    """Mutate one row and write its audit entry in the same transaction."""
    if not (actor or "").strip() or not (reason or "").strip():
        # The DB CHECK enforces this too; failing here gives a clean
        # message instead of an IntegrityError carrying bound params.
        raise PromotionError("actor and reason are required for an audited promotion")

    model = _model_for(target_table)
    row = await session.scalar(select(model).where(model.id == target_id))
    if row is None:
        raise PromotionError(f"{target_table} row {target_id!r} not found")

    audit = AircraftPromotionAudit(
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
    # Deliberately logs ids and gate values only — never registrant_name,
    # never street. See the P1 CheckViolation leak.
    logger.info(
        "aircraft %s: %s %s -> surface_mode=%s publication_state=%s (actor=%s)",
        action,
        target_table,
        target_id,
        to_surface_mode,
        to_publication_state,
        actor,
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
) -> AircraftPromotionAudit:
    """Make one vetted row publicly readable. Audited and reversible.

    ``surface_mode`` defaults to ``open``; ``alias`` is the other legal
    published mode. ``suppress`` is rejected — promoting to suppress is
    not a promotion, and silently accepting it would let a caller
    believe a row is live when the read-gate still hides it.
    """
    if surface_mode == SurfaceMode.SUPPRESS.value:
        raise PromotionError(
            "promote() to surface_mode='suppress' is a no-op by definition; use demote()"
        )
    return await _apply(
        session,
        target_table=target_table,
        target_id=target_id,
        action="promote",
        to_surface_mode=surface_mode,
        to_publication_state=PublicationState.PUBLISHED.value,
        actor=actor,
        reason=reason,
    )


async def demote(
    session: AsyncSession, *, target_table: str, target_id: str, actor: str, reason: str
) -> AircraftPromotionAudit:
    """Return one row to the dark state. The exact inverse of :func:`promote`."""
    return await _apply(
        session,
        target_table=target_table,
        target_id=target_id,
        action="demote",
        to_surface_mode=SurfaceMode.SUPPRESS.value,
        to_publication_state=PublicationState.STAGED.value,
        actor=actor,
        reason=reason,
    )
