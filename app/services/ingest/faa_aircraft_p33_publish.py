"""P3.3 — publish the remainder of the staged corporate REGISTERS edges.

Approved by Tony 2026-08-30, after the P3.2 pilot validated and the
armed-services dedup consolidated the fragmented service canonicals.

Cohort = **every remaining staged corporate edge**, which after the
merges is 730 on 0-edge canonicals plus 48 that moved onto the merged
armed-services survivors (45 Air Force, 3 Navy). Those 48 were part of
the P3.3 hold only because their canonical had no edges; the dedup gave
them one, and re-fragmenting the fleet by leaving them staged would
undo the point of step 2.

Still NOT published, and there is nothing left in these classes staged:
the 0.90 alias tier, the 15 collision suspects, and **every individual
registrant** — P3.1 measured ~90% false positives on individual name
matching, so zero individuals surface. Enforced by a hard post-filter,
not only by the SQL predicate.

``N/A`` stays excluded: a placeholder canonical from a blank disclosure
field that the registrant "N A CORP" collides with after suffix
stripping.
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy import select

from app.db import get_sessionmaker
from app.models import (
    Aircraft,
    AircraftRegistrationEdge,
    CanonicalEntity,
    PublicationState,
    SurfaceMode,
)
from app.services.aircraft_publish import promote
from app.services.ingest.faa_aircraft_p32_pilot_publish import EXCLUDED_CANONICAL_NAMES

logger = logging.getLogger(__name__)

ACTOR = "helen-driver (P3.3 remainder, Tony approved 2026-08-30)"
REASON = (
    "P3.3: remaining staged corporate exact-canonical REGISTERS edges, published "
    "after the P3.2 pilot validated and the armed-services dedup consolidated "
    "fragmented service canonicals; design decision #3, approved by Tony 2026-08-30"
)


async def select_cohort(session) -> list[tuple]:
    """Everything still staged that is eligible. Read-only."""
    rows = (
        await session.execute(
            select(AircraftRegistrationEdge, Aircraft, CanonicalEntity)
            .join(Aircraft, Aircraft.id == AircraftRegistrationEdge.aircraft_id)
            .join(CanonicalEntity, CanonicalEntity.id == AircraftRegistrationEdge.canonical_id)
            .where(AircraftRegistrationEdge.publication_state == PublicationState.STAGED.value)
            .where(AircraftRegistrationEdge.match_tier == "exact_canonical")
            .where(CanonicalEntity.surface_mode == SurfaceMode.OPEN.value)
            .where(CanonicalEntity.publication_state == PublicationState.PUBLISHED.value)
            .where(CanonicalEntity.canonical_name.notin_(list(EXCLUDED_CANONICAL_NAMES)))
        )
    ).all()
    # Belt and braces, same as the pilot: no individual reaches the
    # promotion loop whatever the predicate above did.
    safe = [r for r in rows if (r[1].type_registrant or "") not in ("1", "4")]
    if len(rows) != len(safe):
        logger.warning("refused %d individual rows at the safety filter", len(rows) - len(safe))
    return safe


async def run(dry_run: bool = True) -> dict:
    """Promote the cohort. Idempotent — already-published rows are skipped."""
    stats: dict = {
        "cohort": 0,
        "promoted_edges": 0,
        "promoted_aircraft": 0,
        "already_published": 0,
        "entities": {},
        "dry_run": dry_run,
    }
    sm = get_sessionmaker()
    async with sm() as session:
        rows = await select_cohort(session)
        stats["cohort"] = len(rows)
        for edge, aircraft, ent in rows:
            stats["entities"][ent.canonical_name] = (
                stats["entities"].get(ent.canonical_name, 0) + 1
            )
            if (
                edge.publication_state == PublicationState.PUBLISHED.value
                and aircraft.publication_state == PublicationState.PUBLISHED.value
            ):
                stats["already_published"] += 1
                continue
            if dry_run:
                continue
            await promote(
                session,
                target_table="aircraft_registration_edges",
                target_id=edge.id,
                actor=ACTOR,
                reason=REASON,
            )
            stats["promoted_edges"] += 1
            await promote(
                session,
                target_table="aircraft",
                target_id=aircraft.id,
                actor=ACTOR,
                reason=REASON,
            )
            stats["promoted_aircraft"] += 1
        if not dry_run:
            await session.commit()
    return stats


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="P3.3 remainder publish.")
    ap.add_argument("--apply", action="store_true", help="Commit. Default is a dry run.")
    args = ap.parse_args()
    s = asyncio.run(run(dry_run=not args.apply))
    print(f"\n=== P3.3 REMAINDER PUBLISH ({'APPLY' if args.apply else 'DRY RUN'}) ===")
    print(f"  cohort edges       {s['cohort']:,}")
    print(f"  entities           {len(s['entities'])}")
    print(f"  promoted edges     {s['promoted_edges']:,}")
    print(f"  promoted aircraft  {s['promoted_aircraft']:,}")
    print(f"  already published  {s['already_published']:,}")
    print(f"  excluded           {list(EXCLUDED_CANONICAL_NAMES)}")
    print("\n  entity : aircraft")
    for name, n in sorted(s["entities"].items(), key=lambda kv: (-kv[1], kv[0])):
        print(f"    {name[:52]:<52} {n:>5}")


if __name__ == "__main__":  # pragma: no cover
    _main()
