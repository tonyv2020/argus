"""P2 — apply pending merges from ``alias_crosswalk``.

Every merge re-points the FROM canonical's edges + citations + aliases
+ anchor_registry rows to the TO canonical, then deletes the FROM row.
Idempotent (a re-applied row is a no-op because it's already gone from
``alias_crosswalk`` unappliedset).

Fail-closed on surface_mode (spec §3): the surviving canonical inherits
the MOST-protected surface_mode across the pair. Suppress > alias >
open.  A merge that would relax protection is REFUSED (logged, not
applied). helen validates post-merge that no suppressed identity
became open.

CLI:
    python -m app.services.ingest.merge_pass                 # apply all pending
    python -m app.services.ingest.merge_pass --dry-run       # log only
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import (
    AliasCrosswalk,
    AnchorRegistry,
    CanonicalEdge,
    CanonicalEntity,
    EntityAlias,
)

logger = logging.getLogger(__name__)


_SURFACE_MODE_STRICTNESS = {
    "open": 0,
    "alias": 1,
    "suppress": 2,
}


@dataclass
class MergeStats:
    """Counters for one merge sweep."""

    pending: int = 0
    applied: int = 0
    edges_repointed: int = 0
    aliases_repointed: int = 0
    anchor_rows_repointed: int = 0
    refused_privacy: int = 0
    errors: int = 0


def _most_protected(a: str, b: str) -> str:
    """Return the MORE-protected of two surface_modes (spec §3)."""
    return a if _SURFACE_MODE_STRICTNESS.get(a, 0) >= _SURFACE_MODE_STRICTNESS.get(b, 0) else b


async def _apply_one(
    session: AsyncSession, row: AliasCrosswalk, stats: MergeStats
) -> bool:
    """Apply ONE crosswalk row. Returns True on success. Fail-closed on
    surface_mode.
    """
    src = (
        await session.execute(
            select(CanonicalEntity).where(CanonicalEntity.id == row.from_id)
        )
    ).scalar_one_or_none()
    dst = (
        await session.execute(
            select(CanonicalEntity).where(CanonicalEntity.id == row.to_id)
        )
    ).scalar_one_or_none()

    if src is None or dst is None:
        logger.warning(
            "merge %s: from=%s or to=%s missing (already merged?)",
            row.id, row.from_id, row.to_id,
        )
        stats.errors += 1
        return False

    # Privacy guardrail — surviving canonical inherits MOST-protected mode.
    survivor_mode = _most_protected(src.surface_mode, dst.surface_mode)
    if survivor_mode != dst.surface_mode:
        # A merge that would need to escalate protection on the survivor
        # is refused unless the crosswalk entry explicitly opts in.
        # Escalation IS safe (open→alias, alias→suppress) but relaxation
        # via merge is what §3 forbids. Log + skip until curated.
        if _SURFACE_MODE_STRICTNESS[survivor_mode] > _SURFACE_MODE_STRICTNESS[
            dst.surface_mode
        ]:
            dst.surface_mode = survivor_mode
            logger.info(
                "merge %s: escalating survivor %s surface_mode → %s",
                row.id, dst.id, survivor_mode,
            )

    if _SURFACE_MODE_STRICTNESS[src.surface_mode] > _SURFACE_MODE_STRICTNESS[
        dst.surface_mode
    ]:
        # Src is MORE protected than dst — merging it away could surface
        # a suppressed identity via the dst's aliases. Refuse.
        logger.error(
            "merge %s: REFUSED (privacy) — src surface_mode=%s > dst=%s",
            row.id, src.surface_mode, dst.surface_mode,
        )
        stats.refused_privacy += 1
        return False

    # Re-point outbound edges.
    edges_out = (
        await session.execute(
            update(CanonicalEdge)
            .where(CanonicalEdge.source_id == src.id)
            .values(source_id=dst.id)
        )
    ).rowcount
    # Re-point inbound edges.
    edges_in = (
        await session.execute(
            update(CanonicalEdge)
            .where(CanonicalEdge.target_id == src.id)
            .values(target_id=dst.id)
        )
    ).rowcount
    stats.edges_repointed += (edges_out or 0) + (edges_in or 0)

    # Re-point aliases (the src's aliases become dst's aliases — that
    # is the whole point of the merge).
    aliases = (
        await session.execute(
            update(EntityAlias)
            .where(EntityAlias.canonical_id == src.id)
            .values(canonical_id=dst.id)
        )
    ).rowcount
    stats.aliases_repointed += aliases or 0

    # Re-point anchor_registry rows pointing at src.
    anchors = (
        await session.execute(
            update(AnchorRegistry)
            .where(AnchorRegistry.canonical_id == src.id)
            .values(canonical_id=dst.id)
        )
    ).rowcount
    stats.anchor_rows_repointed += anchors or 0

    # Freeze the audit ids BEFORE deleting src (ondelete SET NULL wipes
    # the FKs on the crosswalk row when src goes away).
    row.from_id_frozen = src.id
    row.to_id_frozen = dst.id
    row.applied_at = datetime.now(timezone.utc)
    await session.flush()

    # Delete the src canonical (ondelete CASCADE cleans citations
    # through their edges — but edges were re-pointed above so nothing
    # left).
    await session.delete(src)
    await session.flush()

    stats.applied += 1
    logger.info(
        "merge applied %s → %s (edges=%d aliases=%d anchors=%d) reason=%s",
        row.from_id, row.to_id,
        (edges_out or 0) + (edges_in or 0),
        aliases or 0,
        anchors or 0,
        row.reason,
    )
    return True


async def apply_pending(dry_run: bool = False) -> MergeStats:
    """Apply every ``alias_crosswalk`` row with NULL applied_at.

    Each merge runs in its OWN session (one commit per merge) — the
    per-merge repoint touches CanonicalEdge + EntityAlias + AnchorRegistry
    across a chain of update() calls, and reusing a session across
    merges tripped a psycopg autocommit greenlet issue on the pool
    pre-ping. Fresh session per merge = isolation + no cross-merge
    lock contention.
    """
    stats = MergeStats()
    sm = get_sessionmaker()

    async with sm() as read_session:
        pending_ids = [
            r for r in (
                await read_session.execute(
                    select(AliasCrosswalk.id).where(
                        AliasCrosswalk.applied_at.is_(None)
                    )
                )
            ).scalars().all()
        ]
    stats.pending = len(pending_ids)

    for cw_id in pending_ids:
        async with sm() as session:
            row = (
                await session.execute(
                    select(AliasCrosswalk).where(
                        AliasCrosswalk.id == cw_id
                    )
                )
            ).scalar_one_or_none()
            if row is None:
                continue
            if dry_run:
                logger.info(
                    "DRY-RUN would merge %s → %s reason=%s",
                    row.from_id, row.to_id, row.reason,
                )
                continue
            try:
                await _apply_one(session, row, stats)
                await session.commit()
            except Exception:
                await session.rollback()
                logger.exception(
                    "merge failed for %s → %s", row.from_id, row.to_id
                )
                stats.errors += 1
    return stats


def main() -> None:
    """CLI — ``python -m app.services.ingest.merge_pass [--dry-run]``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    stats = asyncio.run(apply_pending(dry_run=args.dry_run))
    logger.info("merge pass done: %s", stats)


if __name__ == "__main__":
    main()
