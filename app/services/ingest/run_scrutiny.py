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

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import CanonicalEdge, CanonicalEntity, EntityAlias, EntityType
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


async def run_scrutiny_batch(batch_id: str, limit: int | None = None) -> ScrutinySweepStats:
    """RG4 — decide every entity in one staged batch that has no verdict yet.

    ``POST /api/admin/batches/{batch_id}/publish`` refuses 409 while any
    entity in the batch lacks a ``scrutiny_decisions`` row, so a staged
    ingest has to run this before it can go live.

    Strictly scoped to ``batch_id``: it can only ever touch rows this
    ingest created. Pre-existing published canonicals — including any
    member already sitting on a ``suppress``/``alias`` node — are outside
    the batch and are never re-decided here, so the pass can neither
    relax nor tighten a live protection by accident.
    """
    stats = ScrutinySweepStats()
    sm = get_sessionmaker()
    async with sm() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT ce.id FROM canonical_entities ce "
                    "LEFT JOIN scrutiny_decisions sd ON sd.canonical_id = ce.id "
                    "WHERE ce.batch_id = :b AND sd.id IS NULL "
                    "ORDER BY ce.canonical_name"
                ),
                {"b": batch_id},
            )
        ).all()
    pending = [r[0] for r in rows]
    if limit is not None:
        pending = pending[:limit]
    logger.info(
        "scrutiny batch %s: %d entities awaiting a verdict", batch_id, len(pending)
    )
    for cid in pending:
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
    return stats


async def run_scrutiny_members(
    apply: bool = False, limit: int | None = None
) -> dict:
    """P1.5 — decide the CONGRESSIONAL ROSTER, and nothing else.

    Scoped by construction to canonicals carrying a ``bioguide`` alias —
    a sitting member of Congress — and, within those, only the ones that
    are missing a verdict or are not currently ``open``. Nothing else in
    the corpus is touched.

    The verdict is the deterministic hard-signal path: a bioguide (or
    ``fec.candidate``) alias classifies PUBLIC → SURFACE with no LLM
    call and no Anthropic key. ``apply=False`` (the default) computes
    every verdict and reports it WITHOUT writing an audit row or
    touching ``surface_mode`` — 46 sitting members are currently
    ``suppress`` with no recorded reason, and opening a live protection
    is an operator decision that deserves a preview first.
    """
    from app.models import SurfaceMode
    from app.services.scrutiny import ScrutinyDecisionLog, scrutinize_person

    sm = get_sessionmaker()
    out: dict = {"mode": "apply" if apply else "dry-run", "members": []}
    async with sm() as session:
        member_ids = [
            r[0]
            for r in (
                await session.execute(
                    select(EntityAlias.canonical_id)
                    .where(EntityAlias.source_system == "bioguide")
                    .distinct()
                )
            ).all()
        ]
        decided = {
            r[0]
            for r in (
                await session.execute(
                    select(ScrutinyDecisionLog.canonical_id).where(
                        ScrutinyDecisionLog.canonical_id.in_(member_ids)
                    )
                )
            ).all()
        }
        ents = (
            await session.execute(
                select(CanonicalEntity).where(CanonicalEntity.id.in_(member_ids))
            )
        ).scalars().all()
    pending = [
        e
        for e in ents
        if e.id not in decided or e.surface_mode != SurfaceMode.OPEN.value
    ]
    pending.sort(key=lambda e: e.canonical_name)
    if limit is not None:
        pending = pending[:limit]
    out["members_total"] = len(ents)
    out["members_pending"] = len(pending)

    for ent in pending:
        async with sm() as session:
            try:
                if apply:
                    verdict = await scrutinize_and_log(session, ent.id)
                    await session.commit()
                    after = (
                        await session.execute(
                            select(CanonicalEntity.surface_mode).where(
                                CanonicalEntity.id == ent.id
                            )
                        )
                    ).scalar_one()
                else:
                    verdict = await scrutinize_person(session, ent.id)
                    await session.rollback()
                    after = None
            except Exception as exc:  # noqa: BLE001
                await session.rollback()
                logger.exception("member scrutiny failed %s", ent.id)
                out["members"].append(
                    {"canonical_id": ent.id, "name": ent.canonical_name,
                     "error": f"{type(exc).__name__}: {exc}"}
                )
                continue
        out["members"].append(
            {
                "canonical_id": ent.id,
                "name": ent.canonical_name,
                "surface_mode_before": ent.surface_mode,
                "surface_mode_after": after,
                "classification": verdict.classification.value,
                "decision": verdict.decision.value,
                "decided_by": verdict.decided_by,
                "signals": verdict.signals_used,
            }
        )
    by_decision: dict[str, int] = {}
    for row in out["members"]:
        key = row.get("decision", "error")
        by_decision[key] = by_decision.get(key, 0) + 1
    out["by_decision"] = by_decision
    # Any member the deterministic path does NOT classify public is a
    # red flag, not a routine outcome — surface it separately.
    out["not_classified_public"] = [
        r for r in out["members"] if r.get("classification") != "public"
    ]
    return out


def main() -> None:
    """CLI entrypoint — python -m app.services.ingest.run_scrutiny
    [--geo-only] [--batch-id ID] [--limit N]."""
    import argparse

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--geo-only", action="store_true")
    parser.add_argument(
        "--batch-id",
        default=None,
        help="Decide only the entities in this staged batch that have no "
             "verdict yet (the publish precondition). Never touches rows "
             "outside the batch.",
    )
    parser.add_argument(
        "--members",
        action="store_true",
        help="Decide the congressional roster only (canonicals with a "
             "bioguide alias) — preview by default; --apply writes.",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="With --members: actually write the audit rows + apply the "
             "verdict to surface_mode.",
    )
    parser.add_argument("--limit", type=int, default=None)
    args = parser.parse_args()
    if args.members:
        import json

        report = asyncio.run(
            run_scrutiny_members(apply=args.apply, limit=args.limit)
        )
        print(json.dumps(report, indent=2, default=str))
        return
    if args.batch_id:
        stats = asyncio.run(run_scrutiny_batch(args.batch_id, limit=args.limit))
    else:
        stats = asyncio.run(run_scrutiny_sweep(limit=args.limit, geo_group_only=args.geo_only))
    logger.info("scrutiny sweep done: %s", stats)


if __name__ == "__main__":
    main()
