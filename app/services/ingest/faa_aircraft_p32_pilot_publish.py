"""P3.2 — the curated corporate pilot publish. FIRST LIVE SURFACING.

Design decision #3 (helen-k3s/docs/argus-p3-aircraft-publish-design.md),
approved by Tony 2026-08-30: publish the staged corporate ``REGISTERS``
edges whose resolved entity **already has ≥1 edge** in the graph — the
entities Argus substantively knows — and nothing else.

This script exists so the promotion is a **tracked artifact rather than a
manual DB edit**: the pilot is defined by a predicate you can re-evaluate,
every row goes through the P3.0 audited op, and re-running is a no-op.

EXPLICITLY NOT PROMOTED, all staying staged:
  * the 0-edge-canonical corporate pairs (Argus barely knows them)
  * the entire 0.90 ``exact_alias`` tier (collapses subsidiary into parent)
  * the 15 collision suspects
  * **every individual registrant** — P3.1 measured ~90% false positives
    on name matching, so zero individuals surface, including the two
    state-agreeing ones and ROBINSON MEYER.

Both the edge AND the aircraft row are promoted. The read-gate requires
both, so promoting one alone surfaces nothing — a half-finished run fails
closed rather than half-open.
"""

from __future__ import annotations

import argparse
import asyncio
import logging

from sqlalchemy import func, select

from app.db import get_sessionmaker
from app.models import (
    Aircraft,
    AircraftRegistrationEdge,
    CanonicalEdge,
    CanonicalEntity,
    PublicationState,
    SurfaceMode,
)
from app.services.aircraft_identity import is_individual_entity
from app.services.aircraft_publish import promote

logger = logging.getLogger(__name__)

ACTOR = "helen-driver (P3.2 pilot, Tony approved 2026-08-30)"
REASON = (
    "P3.2 curated pilot: corporate exact-canonical match to an entity with >=1 "
    "existing edge; design decision #3, approved by Tony 2026-08-30"
)

#: Canonicals excluded from the pilot despite meeting the predicate, with
#: the reason. Kept here rather than filtered silently so the exclusion is
#: reviewable.
#:
#: 'N/A' is a placeholder canonical born from a blank disclosure field
#: (one holds_asset edge, no aliases). The FAA registrant "N A CORP"
#: normalises to "n a" and collides with it. Publishing would attach two
#: aircraft to an entity rendered as "N/A" — meaningless to a reader and
#: a visible data bug.
EXCLUDED_CANONICAL_NAMES: dict[str, str] = {
    "N/A": "placeholder canonical from a blank disclosure field; 'N A CORP' collides with it",
}


async def select_pilot(session) -> list[tuple]:
    """Return (edge, aircraft, entity) for the pilot set. Read-only.

    The predicate IS the definition of the pilot — not a stored list — so
    it can be re-evaluated against the live graph at review time.
    """
    edge_count = (
        select(func.count(CanonicalEdge.id))
        .where(
            (CanonicalEdge.source_id == CanonicalEntity.id)
            | (CanonicalEdge.target_id == CanonicalEntity.id)
        )
        .correlate(CanonicalEntity)
        .scalar_subquery()
    )
    rows = (
        await session.execute(
            select(AircraftRegistrationEdge, Aircraft, CanonicalEntity)
            .join(Aircraft, Aircraft.id == AircraftRegistrationEdge.aircraft_id)
            .join(CanonicalEntity, CanonicalEntity.id == AircraftRegistrationEdge.canonical_id)
            # corporate 1.00 tier only — this is what P2 staged, asserted
            # rather than assumed so a future tier cannot slip in here
            .where(AircraftRegistrationEdge.match_tier == "exact_canonical")
            # the resolved entity must be open + published itself
            .where(CanonicalEntity.surface_mode == SurfaceMode.OPEN.value)
            .where(CanonicalEntity.publication_state == PublicationState.PUBLISHED.value)
            # ...and Argus must substantively know it
            .where(edge_count >= 1)
            .where(CanonicalEntity.canonical_name.notin_(list(EXCLUDED_CANONICAL_NAMES)))
        )
    ).all()

    # Belt and braces: no individual may reach the promotion loop, whatever
    # the predicate above did. P3.1 is the reason this is a hard filter and
    # not a comment.
    #
    # Identity comes from the ARGUS CANONICAL, never from the FAA
    # TYPE REGISTRANT code — the FAA miscodes companies as individuals
    # (UNITED AIRLINES INC, SOUTHWEST AIRLINES CO and ~20 others are
    # filed 1/4), and gating on that code wrongly withheld them. See
    # app/services/aircraft_identity.
    safe = [r for r in rows if not is_individual_entity(r[2].type, r[2].canonical_name)]
    dropped = len(rows) - len(safe)
    if dropped:
        logger.warning("refused %d person-typed rows at the identity guard", dropped)
    return safe


async def run(dry_run: bool = True) -> dict:
    """Promote the pilot set. Idempotent: already-published rows are skipped."""
    stats = {
        "pilot_edges": 0,
        "promoted_edges": 0,
        "promoted_aircraft": 0,
        "already_published": 0,
        "individuals_refused": 0,
        "entities": {},
        "dry_run": dry_run,
    }
    sm = get_sessionmaker()
    async with sm() as session:
        rows = await select_pilot(session)
        stats["pilot_edges"] = len(rows)
        for edge, aircraft, ent in rows:
            stats["entities"].setdefault(ent.canonical_name, 0)
            stats["entities"][ent.canonical_name] += 1
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
    ap = argparse.ArgumentParser(description="P3.2 corporate pilot publish.")
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Actually promote. Without this the script only reports the pilot set.",
    )
    args = ap.parse_args()
    s = asyncio.run(run(dry_run=not args.apply))
    mode = "APPLY" if args.apply else "DRY RUN"
    print(f"\n=== P3.2 CORPORATE PILOT PUBLISH ({mode}) ===")
    print(f"  pilot edges          {s['pilot_edges']:,}")
    print(f"  entities             {len(s['entities'])}")
    print(f"  promoted edges       {s['promoted_edges']:,}")
    print(f"  promoted aircraft    {s['promoted_aircraft']:,}")
    print(f"  already published    {s['already_published']:,}")
    print(f"  excluded canonicals  {list(EXCLUDED_CANONICAL_NAMES)}")
    print("\n  entity : aircraft")
    for name, n in sorted(s["entities"].items(), key=lambda kv: -kv[1]):
        print(f"    {name[:48]:<48} {n:>5}")


if __name__ == "__main__":  # pragma: no cover
    _main()
