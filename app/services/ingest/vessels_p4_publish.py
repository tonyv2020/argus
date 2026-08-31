"""Vessels P4 — publish all 135 sanctioned operators. FIRST LIVE VESSEL SURFACE.

Approved by Tony 2026-08-31: publish the **entire staged corporate
cohort** — every owner canonical vessels P3 created or crosswalked, every
``vessel_ownership_edge`` it staged, and every vessel row those edges
point at. Unlike the aircraft arc there is no pilot-then-remainder split:
the cohort is 135 OFAC-designated shipping operators, each edge carries
an OFAC SDN citation by construction, and there is no name-matching tier
to be wrong about — the owner link comes from OFAC's own
``vesselOwner`` relationship, not from a fuzzy match.

This script exists so the promotion is a **tracked artifact rather than a
manual DB edit**: the cohort is a predicate you can re-evaluate, every
row goes through the audited op, and re-running is a no-op.

THREE ROWS PER VESSEL, ALL OR NOTHING. The read gate ANDs the edge and
the vessel row, and the dossier 404s unless the owner canonical is
published, so all three must move or the publish surfaces nothing. That
is the design: a half-finished run fails closed rather than half-open.

EXPLICITLY NOT PROMOTED, all staying dark:

  * **The 21 individual OFAC owners.** They were never given a canonical
    or an edge by vessels P3, so there is nothing here to publish — and
    the belt-and-braces guard below plus
    :func:`app.services.vessel_publish._refuse_unless_owner_capable`
    would refuse them anyway. Zero individuals surface.
  * **The 420 long-tail owners** below the ``>= 2 vessel`` cutoff Tony
    set on 2026-08-30. Same story: no canonical, no edge, nothing staged.
  * **Owner PII.** ``owner_name_raw`` and the five owner address columns
    are never promoted, never selected by a read path and never
    projected. Publishing a vessel row exposes exactly vessel name, IMO
    and flag — see ``_VESSEL_PUBLIC_COLUMNS`` in ``app/main.py``.

The 474 vessels with no staged edge (1,540 rows ingested, 1,066 linked)
stay dark too. They are OFAC vessels whose owner fell in one of the held
classes, and an unowned vessel node would be a fact with nothing to
attach it to.
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy import func, select

from app.db import get_sessionmaker
from app.models import (
    CanonicalEntity,
    PublicationState,
    SurfaceMode,
    Vessel,
    VesselOwnershipEdge,
)
from app.services.aircraft_identity import is_individual_entity, is_owner_capable
from app.services.vessel_publish import demote, promote

logger = logging.getLogger(__name__)

ACTOR = "helen-driver (vessels publish all-135, Tony approved 2026-08-31)"
REASON = "vessels publish all-135, Tony approved 2026-08-31"


def _is_live(row) -> bool:
    """Already at the target gate state — nothing to promote."""
    return (
        row.publication_state == PublicationState.PUBLISHED.value
        and row.surface_mode == SurfaceMode.OPEN.value
    )


async def select_cohort(session) -> list[tuple]:
    """Every staged owner→vessel edge with its vessel and owner. Read-only.

    No ``publication_state`` predicate: the cohort is defined by the
    edges vessels P3 staged, and the run is idempotent, so a re-run over
    already-published rows is a no-op rather than a different cohort.
    """
    rows = (
        await session.execute(
            select(VesselOwnershipEdge, Vessel, CanonicalEntity)
            .join(Vessel, Vessel.id == VesselOwnershipEdge.vessel_id)
            .join(CanonicalEntity, CanonicalEntity.id == VesselOwnershipEdge.canonical_id)
        )
    ).all()
    # Belt and braces, same as the aircraft publishes: no individual
    # reaches the promotion loop whatever the query above returned.
    # Identity comes from the ARGUS canonical type, and both helpers fail
    # closed on an unknown one.
    safe = [
        r
        for r in rows
        if not is_individual_entity(r[2].type, r[2].canonical_name)
        and is_owner_capable(r[2].type)
    ]
    if len(rows) != len(safe):
        logger.warning(
            "refused %d edges at the identity guard (owner not an owner-capable org)",
            len(rows) - len(safe),
        )
    return safe


async def run_reversal(owner: str, dry_run: bool = True) -> dict:
    """Withdraw ONE owner from the public surface. The unwind drill.

    ``demote`` is only meaningfully reversible if someone has run it, so
    this lives beside the publish rather than in a shell one-liner: the
    withdrawal goes through the audited op, leaves its own audit rows,
    and re-running :func:`run` restores the owner exactly.

    A vessel co-owned by another PUBLISHED owner is left alone and
    reported. Darkening it would take that owner's row off the surface
    too — the read gate ANDs the vessel with the edge — and a withdrawal
    that silently damages a neighbour is not a clean reversal.
    """
    stats: dict = {
        "dry_run": dry_run, "owner": owner, "demoted_owner": 0,
        "demoted_edges": 0, "demoted_vessels": 0, "vessels_kept_shared": 0,
    }
    sm = get_sessionmaker()
    async with sm() as session:
        rows = [r for r in await select_cohort(session) if r[2].canonical_name == owner]
        if not rows:
            raise SystemExit(f"no published cohort rows for owner {owner!r}")

        owner_ids = {r[2].id for r in rows}
        for edge, vessel, _ent in rows:
            others = (
                await session.execute(
                    select(VesselOwnershipEdge)
                    .where(VesselOwnershipEdge.vessel_id == vessel.id)
                    .where(VesselOwnershipEdge.canonical_id.notin_(list(owner_ids)))
                    .where(
                        VesselOwnershipEdge.publication_state
                        == PublicationState.PUBLISHED.value
                    )
                )
            ).scalars().all()
            if not dry_run:
                await demote(
                    session, target_table="vessel_ownership_edges", target_id=edge.id,
                    actor=ACTOR, reason=f"reversal drill: withdraw {owner}",
                )
            stats["demoted_edges"] += 1
            if others:
                stats["vessels_kept_shared"] += 1
                continue
            if not dry_run:
                await demote(
                    session, target_table="vessels", target_id=vessel.id,
                    actor=ACTOR, reason=f"reversal drill: withdraw {owner}",
                )
            stats["demoted_vessels"] += 1

        for cid in owner_ids:
            if not dry_run:
                await demote(
                    session, target_table="canonical_entities", target_id=cid,
                    actor=ACTOR, reason=f"reversal drill: withdraw {owner}",
                )
            stats["demoted_owner"] += 1
        if not dry_run:
            await session.commit()
    return stats


async def run(dry_run: bool = True) -> dict:
    """Promote the cohort. Idempotent — rows already live are skipped."""
    stats: dict = {
        "dry_run": dry_run,
        "cohort_edges": 0,
        "refused_non_org": 0,
        "owners": 0,
        "promoted_owners": 0,
        "promoted_edges": 0,
        "promoted_vessels": 0,
        "already_live_owners": 0,
        "already_live_edges": 0,
        "already_live_vessels": 0,
        "by_owner": {},
    }
    sm = get_sessionmaker()
    async with sm() as session:
        rows = await select_cohort(session)
        # Report what the guard held back, not just what it let through:
        # a cohort that silently shrank is the failure mode worth seeing.
        total_edges = await session.scalar(
            select(func.count(VesselOwnershipEdge.id))
        )
        stats["cohort_edges"] = len(rows)
        stats["refused_non_org"] = (total_edges or 0) - len(rows)

        seen_owners: set[str] = set()
        seen_vessels: set[str] = set()
        for edge, vessel, ent in rows:
            stats["by_owner"][ent.canonical_name] = (
                stats["by_owner"].get(ent.canonical_name, 0) + 1
            )

            # ── the owner canonical (once) ──
            if ent.id not in seen_owners:
                seen_owners.add(ent.id)
                if _is_live(ent):
                    stats["already_live_owners"] += 1
                elif not dry_run:
                    await promote(
                        session, target_table="canonical_entities", target_id=ent.id,
                        actor=ACTOR, reason=REASON,
                    )
                    stats["promoted_owners"] += 1
                else:
                    stats["promoted_owners"] += 1

            # ── the edge ──
            if _is_live(edge):
                stats["already_live_edges"] += 1
            elif not dry_run:
                await promote(
                    session, target_table="vessel_ownership_edges", target_id=edge.id,
                    actor=ACTOR, reason=REASON,
                )
                stats["promoted_edges"] += 1
            else:
                stats["promoted_edges"] += 1

            # ── the vessel row (once; 1,076 edges land on 1,066 vessels) ──
            if vessel.id not in seen_vessels:
                seen_vessels.add(vessel.id)
                if _is_live(vessel):
                    stats["already_live_vessels"] += 1
                elif not dry_run:
                    await promote(
                        session, target_table="vessels", target_id=vessel.id,
                        actor=ACTOR, reason=REASON,
                    )
                    stats["promoted_vessels"] += 1
                else:
                    stats["promoted_vessels"] += 1

        stats["owners"] = len(seen_owners)
        stats["vessels"] = len(seen_vessels)
        if not dry_run:
            await session.commit()
    return stats


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Vessels P4 publish (all 135 operators).")
    ap.add_argument("--apply", action="store_true", help="Commit. Default is a dry run.")
    ap.add_argument(
        "--demote-owner",
        help="Reversal drill: withdraw ONE owner by canonical_name. "
        "Re-run without it to restore.",
    )
    args = ap.parse_args()
    if args.demote_owner:
        d = asyncio.run(run_reversal(args.demote_owner, dry_run=not args.apply))
        print(f"\n=== VESSELS REVERSAL ({'APPLY' if args.apply else 'DRY RUN'}) ===")
        for k, v in d.items():
            print(f"  {k:<22} {v}")
        return
    s = asyncio.run(run(dry_run=not args.apply))
    print(f"\n=== VESSELS PUBLISH ALL-135 ({'APPLY' if args.apply else 'DRY RUN'}) ===")
    for k in (
        "cohort_edges", "refused_non_org", "owners", "vessels",
        "promoted_owners", "promoted_edges", "promoted_vessels",
        "already_live_owners", "already_live_edges", "already_live_vessels",
    ):
        print(f"  {k:<22} {s[k]:>7,}")
    print("\n  owner : vessels")
    for name, n in sorted(s["by_owner"].items(), key=lambda kv: (-kv[1], kv[0]))[:20]:
        print(f"    {name[:58]:<58} {n:>5}")


if __name__ == "__main__":  # pragma: no cover
    _main()
