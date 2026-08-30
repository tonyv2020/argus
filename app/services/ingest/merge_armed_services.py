"""P3 step 2 — dedup the armed-services canonicals. Curated, not inferred.

The FAA aircraft layer exposed that one real organisation is carried as
several canonicals: the USAF fleet was split across three, so publishing
P3.3 would have shown a reader three separate "entities" each owning
part of it.

**The merge list is hand-curated and enumerated below, never inferred
from name similarity.** The corpus contains French Navy, German Navy,
Israeli Navy, IRGC Navy, Old Navy, Tartan Army, Swiss Army knife,
Continental Army and Nigerian army — any rule loose enough to catch
"Navy"→"United States Navy" is loose enough to catch several of those.
So each merge is listed with its evidence, and anything I was not
certain about is in :data:`HELD` rather than merged.

Merges are **destructive and not reversible** — ``merge_two_canonicals``
deletes the dropped canonical. ``--dry-run`` (the default) performs the
merges inside a transaction and rolls back, so the exact stats can be
reviewed before ``--apply`` commits them.
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy import func, select

from app.db import get_sessionmaker
from app.models import AircraftRegistrationEdge, CanonicalEdge, CanonicalEntity, EntityAlias
from app.services.ingest.merge_canonicals import merge_two_canonicals

logger = logging.getLogger(__name__)

#: (survivor_name, [dropped_names], why). Names, not ids — the script
#: resolves and reports them, so the plan stays readable in review.
MERGES: list[tuple[str, list[str], str]] = [
    (
        "Department Of The Air Force",
        ["United States Air Force", "Air Force", "DEPARTMENT OF AIR FORCE"],
        "One service under four spellings. 'Air Force' carries the aliases "
        "'US Air Force'/'air force'; 'DEPARTMENT OF AIR FORCE' is the same "
        "department without the article. Survivor is the agency-typed row "
        "with 10 holds_contract edges — the richest and semantically right.",
    ),
    (
        "Department Of The Navy",
        ["United States Navy", "Navy", "Department of the Navy"],
        "'Navy' carries aliases 'U.S. Navy'/'US Navy'; 'Department of the "
        "Navy' shares the survivor's normalized name exactly (a pure "
        "case-duplicate). Foreign navies are SEPARATE canonicals and are "
        "untouched. Survivor is the agency-typed row with 12 edges.",
    ),
]

#: Same-entity candidates I did NOT merge, and why. Held rather than
#: guessed — each needs Tony's explicit go.
HELD: list[tuple[str, str]] = [
    (
        "Naval Air Systems Command",
        "NOT a duplicate. NAVAIR is a distinct command that owns its own "
        "126 aircraft. Explicitly preserved.",
    ),
    (
        "US Army + Department Of The Army",
        "Same duplicate shape as the two merges above, but the armed-services "
        "brief enumerated USAF and Navy only. Held for an explicit go.",
    ),
    (
        "U.S. Coast Guard + coast guard",
        "Same shape again (agency w/ 4 edges + a 3-alias organization row). "
        "Not enumerated in the brief. Held.",
    ),
    (
        "Air Force One (organization) + Air Force One (place)",
        "A genuine duplicate pair, but it is the aircraft/callsign, not a "
        "service — outside an armed-services dedup. Flagged separately.",
    ),
    (
        "Space Force",
        "Sits inside the Department of the Air Force but is a DISTINCT "
        "service. Not merged.",
    ),
    (
        "United States Air Force and Navy",
        "A compound news tag naming two services. Ambiguous; merging it into "
        "either would assert something the tag does not say.",
    ),
]


async def _snapshot(session, name: str) -> dict | None:
    """Everything needed to reconstruct a canonical, captured pre-merge."""
    ent = await session.scalar(
        select(CanonicalEntity).where(CanonicalEntity.canonical_name == name)
    )
    if ent is None:
        return None
    edges = await session.scalar(
        select(func.count(CanonicalEdge.id)).where(
            (CanonicalEdge.source_id == ent.id) | (CanonicalEdge.target_id == ent.id)
        )
    )
    aliases = await session.scalar(
        select(func.count(EntityAlias.id)).where(EntityAlias.canonical_id == ent.id)
    )
    ac = await session.scalar(
        select(func.count(AircraftRegistrationEdge.id)).where(
            AircraftRegistrationEdge.canonical_id == ent.id
        )
    )
    ac_pub = await session.scalar(
        select(func.count(AircraftRegistrationEdge.id)).where(
            AircraftRegistrationEdge.canonical_id == ent.id,
            AircraftRegistrationEdge.publication_state == "published",
        )
    )
    return {
        "id": ent.id,
        "name": ent.canonical_name,
        "type": ent.type,
        "surface_mode": ent.surface_mode,
        "publication_state": ent.publication_state,
        "edges": edges,
        "aliases": aliases,
        "aircraft": ac,
        "aircraft_published": ac_pub,
    }


async def run(dry_run: bool = True) -> dict:
    """Execute the curated merges. ``dry_run`` rolls back at the end."""
    report: dict = {"dry_run": dry_run, "merges": [], "before": [], "after": [], "totals": {}}
    sm = get_sessionmaker()
    async with sm() as s:
        report["totals"]["canonicals_before"] = await s.scalar(
            select(func.count(CanonicalEntity.id))
        )
        report["totals"]["canonical_edges_before"] = await s.scalar(
            select(func.count(CanonicalEdge.id))
        )
        report["totals"]["aircraft_edges_before"] = await s.scalar(
            select(func.count(AircraftRegistrationEdge.id))
        )

        for keep_name, drop_names, _why in MERGES:
            keep_snap = await _snapshot(s, keep_name)
            if keep_snap is None:
                report["merges"].append({"keep": keep_name, "error": "survivor not found"})
                continue
            report["before"].append(keep_snap)
            for drop_name in drop_names:
                drop_snap = await _snapshot(s, drop_name)
                if drop_snap is None:
                    report["merges"].append(
                        {"keep": keep_name, "drop": drop_name, "error": "not found"}
                    )
                    continue
                report["before"].append(drop_snap)
                stats = await merge_two_canonicals(s, keep_snap["id"], drop_snap["id"])
                report["merges"].append(
                    {
                        "keep": keep_name,
                        "drop": drop_name,
                        "drop_id": drop_snap["id"],
                        "refused": stats.refused,
                        "refused_reason": stats.refused_reason,
                        "edges_repointed": stats.edges_repointed,
                        "edges_collided_summed": stats.edges_collided_summed,
                        "aliases_repointed": stats.aliases_repointed,
                        "aliases_dropped_duplicate": stats.aliases_dropped_duplicate,
                        "citations_reparented": stats.citations_reparented,
                        "aircraft_edges_repointed": stats.aircraft_edges_repointed,
                        "aircraft_edges_dropped_duplicate": (
                            stats.aircraft_edges_dropped_duplicate
                        ),
                    }
                )
            report["after"].append(await _snapshot(s, keep_name))

        report["totals"]["canonicals_after"] = await s.scalar(
            select(func.count(CanonicalEntity.id))
        )
        report["totals"]["canonical_edges_after"] = await s.scalar(
            select(func.count(CanonicalEdge.id))
        )
        report["totals"]["aircraft_edges_after"] = await s.scalar(
            select(func.count(AircraftRegistrationEdge.id))
        )
        # Orphan check while still inside the transaction.
        report["totals"]["orphan_aircraft_edges"] = await s.scalar(
            select(func.count(AircraftRegistrationEdge.id)).where(
                ~AircraftRegistrationEdge.canonical_id.in_(select(CanonicalEntity.id))
            )
        )

        if dry_run:
            await s.rollback()
            logger.info("DRY RUN — rolled back")
        else:
            await s.commit()
            logger.info("merges COMMITTED")
    return report


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Armed-services canonical dedup.")
    ap.add_argument("--apply", action="store_true", help="Commit. Default is a dry run.")
    args = ap.parse_args()
    r = asyncio.run(run(dry_run=not args.apply))
    mode = "APPLY" if args.apply else "DRY RUN (rolled back)"
    print(f"\n=== ARMED-SERVICES DEDUP ({mode}) ===")
    print("\n-- BEFORE")
    for b in r["before"]:
        print(f"   {b['name'][:40]:<40} [{b['type']:<12}] edges={b['edges']:<3} "
              f"aliases={b['aliases']:<3} aircraft={b['aircraft']:<4} "
              f"pub={b['aircraft_published']}")
    print("\n-- MERGES")
    for m in r["merges"]:
        if m.get("error"):
            print(f"   !! {m.get('drop', m['keep'])}: {m['error']}")
            continue
        if m["refused"]:
            print(f"   REFUSED {m['drop']} -> {m['keep']}: {m['refused_reason']}")
            continue
        print(f"   {m['drop'][:36]:<36} -> {m['keep'][:32]:<32} "
              f"edges={m['edges_repointed']}(+{m['edges_collided_summed']} summed) "
              f"aliases={m['aliases_repointed']}(-{m['aliases_dropped_duplicate']} dup) "
              f"citations={m['citations_reparented']} "
              f"AIRCRAFT={m['aircraft_edges_repointed']}"
              f"(-{m['aircraft_edges_dropped_duplicate']} dup)")
    print("\n-- AFTER (survivors)")
    for a in r["after"]:
        print(f"   {a['name'][:40]:<40} [{a['type']:<12}] edges={a['edges']:<3} "
              f"aliases={a['aliases']:<3} aircraft={a['aircraft']:<4} "
              f"pub={a['aircraft_published']}")
    t = r["totals"]
    print("\n-- GRAPH INTEGRITY")
    print(f"   canonicals      {t['canonicals_before']} -> {t['canonicals_after']} "
          f"(delta {t['canonicals_after'] - t['canonicals_before']})")
    print(f"   canonical_edges {t['canonical_edges_before']} -> {t['canonical_edges_after']} "
          f"(delta {t['canonical_edges_after'] - t['canonical_edges_before']})")
    print(f"   aircraft_edges  {t['aircraft_edges_before']} -> {t['aircraft_edges_after']} "
          f"(delta {t['aircraft_edges_after'] - t['aircraft_edges_before']})  MUST BE 0")
    print(f"   orphan aircraft edges: {t['orphan_aircraft_edges']}  MUST BE 0")
    print("\n-- HELD (not merged)")
    for name, why in HELD:
        print(f"   {name}\n      {why}")


if __name__ == "__main__":  # pragma: no cover
    _main()
