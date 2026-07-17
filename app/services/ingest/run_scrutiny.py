"""P1 — sweep the scrutiny agent over persons that need classification.

Priorities in this order:
1. Persons attached to the GEO Group subgraph (any hop) — the MVP anchor.
2. Persons currently `surface_mode=open` (never been through scrutiny).
3. Everyone else, oldest-first.

`scrutinize_and_log` handles the audit + updates `surface_mode` + mints
`public_alias` in one commit per person. Fail-closed defaults (Anthropic key
missing / LLM failure) route to PRIVATE + SUPPRESS, and the API layer already
suppresses those nodes + drops their edges — safe against surfacing real names.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import CanonicalEdge, CanonicalEntity, EntityType
from app.services.graph.base import normalize_name
from app.services.scrutiny import scrutinize_and_log

logger = logging.getLogger(__name__)


@dataclass
class ScrutinySweepStats:
    """Counters for one scrutiny sweep — surfaced back to callers + logs."""

    persons_examined: int = 0
    kept_open: int = 0
    aliased: int = 0
    suppressed: int = 0
    errors: int = 0


async def _find_geo_group_id(session: AsyncSession) -> str | None:
    """Look up the GEO Group organization canonical id (created in P0)."""
    row = (
        await session.execute(
            select(CanonicalEntity).where(
                CanonicalEntity.type == EntityType.ORGANIZATION.value,
                CanonicalEntity.canonical_name_normalized == normalize_name("GEO Group"),
            )
        )
    ).scalar_one_or_none()
    return row.id if row else None


async def _geo_group_neighborhood(session: AsyncSession, root_id: str, hops: int = 2) -> set[str]:
    """Return every canonical id reachable from `root_id` within `hops` — for scrutiny priority."""
    seen: set[str] = {root_id}
    frontier: set[str] = {root_id}
    for _ in range(hops):
        if not frontier:
            break
        outbound = (
            (
                await session.execute(
                    select(CanonicalEdge).where(CanonicalEdge.source_id.in_(frontier))
                )
            )
            .scalars()
            .all()
        )
        inbound = (
            (
                await session.execute(
                    select(CanonicalEdge).where(CanonicalEdge.target_id.in_(frontier))
                )
            )
            .scalars()
            .all()
        )
        next_frontier: set[str] = set()
        for e in list(outbound) + list(inbound):
            for nid in (e.source_id, e.target_id):
                if nid not in seen:
                    seen.add(nid)
                    next_frontier.add(nid)
        frontier = next_frontier
    return seen


async def run_scrutiny_sweep(
    limit: int | None = None, geo_group_only: bool = False
) -> ScrutinySweepStats:
    """Run scrutiny on persons that haven't been classified yet.

    `geo_group_only=True` restricts to persons in the GEO Group subgraph (up to
    2 hops) — the MVP anchor. `limit` caps the scan for a bounded/DEMO-key run.
    """
    stats = ScrutinySweepStats()
    sm = get_sessionmaker()

    async with sm() as session:
        geo_id = await _find_geo_group_id(session)
        neighborhood: set[str] = await _geo_group_neighborhood(session, geo_id) if geo_id else set()

    if geo_group_only and not neighborhood:
        logger.error("GEO Group canonical not found — cannot restrict to its neighborhood")
        return stats

    async with sm() as session:
        # Candidate pool = every person canonical WITHOUT a current LLM audit
        # row (helen fail-open-fix 2026-07-17). Includes suppress persons too so
        # the recalibrated prompt can promote real public figures — the sweep
        # is idempotent (audit row is written on every verdict, so a person
        # is only classified once per LLM generation).
        #
        # CRITICAL DISCIPLINE: THIS SWEEP MUST NEVER RESET surface_mode TO
        # `open` FOR ANY REASON. The fail-closed default (SUPPRESS at insert
        # time + SUPPRESS on fallback verdict) is what prevents real-name leak.
        # An operator recalibration MUST reset to SUPPRESS (not open) before
        # re-sweeping — the sweep will promote to open on affirmative PUBLIC
        # LLM verdicts.
        from app.services.scrutiny import ScrutinyDecisionLog

        candidates = (
            (
                await session.execute(
                    select(CanonicalEntity)
                    .where(CanonicalEntity.type == EntityType.PERSON.value)
                    .order_by(CanonicalEntity.created_at)
                )
            )
            .scalars()
            .all()
        )
        adjudicated_ids = set(
            (
                await session.execute(
                    select(ScrutinyDecisionLog.canonical_id).where(
                        ScrutinyDecisionLog.decided_by.like("scrutiny.llm.%")
                    )
                )
            )
            .scalars()
            .all()
        )
        candidates = [c for c in candidates if c.id not in adjudicated_ids]

    # Prioritize: GEO neighborhood first (design's MVP anchor), then everyone else.
    prioritized: list[str] = []
    seen: set[str] = set()
    for c in candidates:
        if c.id in neighborhood:
            prioritized.append(c.id)
            seen.add(c.id)
    if not geo_group_only:
        for c in candidates:
            if c.id in seen:
                continue
            prioritized.append(c.id)

    if limit is not None:
        prioritized = prioritized[:limit]

    for cid in prioritized:
        try:
            async with sm() as session:
                verdict = await scrutinize_and_log(session, cid)
                await session.commit()
            stats.persons_examined += 1
            if verdict.decision.value == "surface":
                stats.kept_open += 1
            elif verdict.decision.value == "suppress":
                stats.suppressed += 1
            else:
                stats.aliased += 1
        except Exception as exc:  # noqa: BLE001
            stats.errors += 1
            logger.exception("scrutiny row failed canonical=%s: %s", cid, exc)
        if stats.persons_examined % 50 == 0 and stats.persons_examined:
            logger.info(
                "scrutiny progress: examined=%d kept_open=%d aliased=%d suppressed=%d errors=%d",
                stats.persons_examined,
                stats.kept_open,
                stats.aliased,
                stats.suppressed,
                stats.errors,
            )

    return stats


def main() -> None:
    """CLI entrypoint — python -m app.services.ingest.run_scrutiny [--geo-only] [--limit N]."""
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--geo-only", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    stats = asyncio.run(run_scrutiny_sweep(limit=args.limit, geo_group_only=args.geo_only))
    logger.info("scrutiny sweep done: %s", stats)


if __name__ == "__main__":
    main()
