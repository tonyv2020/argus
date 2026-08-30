"""Vessels P3 — create major operators, crosswalk PDVSA, stage the edges.

Tony approved a **>= 2 vessel** cutoff (2026-08-30): create the major
shadow-fleet operators as new canonicals, crosswalk the one accepted
same-entity variant, stage vessel→owner edges. **Surfaces nothing** —
publishing is a separate Tony-gated step.

WHAT "CREATED BUT DARK" MEANS. New canonicals are written
``surface_mode='open'`` (they are organisations, not private people)
but ``publication_state='staged'``. The RG1 read-gate excludes staged
rows from every read path, so the entities exist in PG-truth and are
invisible until an operator publishes them. That is the same mechanism
the disclosure bulk-ingest uses, so it needs no new machinery.

SUBSIDIARIES ARE THEIR OWN ENTITIES. Rosnefteflot gets a canonical;
it is NOT crosswalked to Rosneft. Gazpromneft Marine Bunker likewise.
IRISL is NOT crosswalked to "Islamic Republic of Iran" — that canonical
is the COUNTRY and is stored as an ``organization``, so the
owner-capable guard would not have caught it. The graph should say the
subsidiary owns the ships and is related to the parent, not that the
parent owns them.

Idempotent: canonicals are looked up by normalized name before
creation, aliases by OFAC id, and the edge upserts on
``(canonical_id, vessel_id)``.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import logging

from sqlalchemy import func, select

from app.db import get_sessionmaker
from app.models import (
    CanonicalEntity,
    EntityAlias,
    PublicationState,
    SurfaceMode,
    Vessel,
    VesselOwnershipEdge,
    VesselSourceSnapshot,
    _new_id,
)
from app.services.graph.base import normalize_name
from app.services.ingest.ofac_vessel_owner_dryrun import download, parse_entities
from app.services.ingest.vessels_p3_plan import CROSSWALK, CROSSWALK_HELD

logger = logging.getLogger(__name__)

CUTOFF = 2
ALIAS_SOURCE = "ofac.sdn"
ACTOR = "helen-driver (vessels P3, Tony approved 2026-08-30, cutoff >=2)"


async def _resolve_crosswalk(session, ofac_name: str) -> CanonicalEntity | None:
    """Return the existing canonical an accepted crosswalk points at."""
    entry = CROSSWALK.get(ofac_name)
    if entry is None:
        return None
    ent = await session.scalar(
        select(CanonicalEntity).where(CanonicalEntity.canonical_name == entry["canonical"])
    )
    if ent is None:
        logger.warning("crosswalk target %r not found", entry["canonical"])
    return ent


async def run(cutoff: int = CUTOFF, dry_run: bool = True, cache: str | None = None) -> dict:
    """Create canonicals + stage edges for owners at or above the cutoff."""
    entities, links = parse_entities(download(cache=cache))

    by_owner: dict[str, list] = collections.defaultdict(list)
    for vid, _vname, rtype, oid, oname in links:
        by_owner[oid].append((vid, rtype, oname))

    stats: dict = {
        "dry_run": dry_run,
        "cutoff": cutoff,
        "canonicals_created": 0,
        "canonicals_reused": 0,
        "crosswalked": 0,
        "aliases_created": 0,
        "edges_staged": 0,
        "edges_existing": 0,
        "held_individual_owners": 0,
        "held_individual_vessels": 0,
        "deferred_owners": 0,
        "deferred_vessels": 0,
        "vessels_not_in_table": 0,
        "created_names": [],
    }

    sm = get_sessionmaker()
    async with sm() as s:
        snap = await s.scalar(
            select(VesselSourceSnapshot)
            .where(VesselSourceSnapshot.source == "ofac_sdn")
            .order_by(VesselSourceSnapshot.fetched_at.desc())
            .limit(1)
        )
        if snap is None:
            raise RuntimeError("no OFAC vessel snapshot to cite — run vessels P1 first")

        # source_key -> vessel row id
        vessel_ids = {
            r[0]: r[1]
            for r in (
                await s.execute(
                    select(Vessel.source_key, Vessel.id).where(Vessel.source == "ofac_sdn")
                )
            ).all()
        }

        for oid, rows in by_owner.items():
            otype, oname = entities.get(oid, ("?", rows[0][2]))
            name = (oname or rows[0][2] or "").strip()
            distinct_vessels = {v for v, _, _ in rows}

            if otype == "Individual":
                stats["held_individual_owners"] += 1
                stats["held_individual_vessels"] += len(distinct_vessels)
                continue
            if len(distinct_vessels) < cutoff:
                stats["deferred_owners"] += 1
                stats["deferred_vessels"] += len(distinct_vessels)
                continue
            if not name:
                continue

            # ── resolve or create the owner canonical ──
            ent = await _resolve_crosswalk(s, name)
            if ent is not None:
                stats["crosswalked"] += 1
            else:
                norm = normalize_name(name)
                ent = await s.scalar(
                    select(CanonicalEntity).where(
                        CanonicalEntity.canonical_name_normalized == norm,
                        CanonicalEntity.type == "organization",
                    )
                )
                if ent is not None:
                    stats["canonicals_reused"] += 1
                elif dry_run:
                    stats["canonicals_created"] += 1
                    stats["created_names"].append(name)
                    continue  # nothing to stage against in a dry run
                else:
                    ent = CanonicalEntity(
                        id=_new_id(),
                        canonical_name=name,
                        canonical_name_normalized=norm,
                        type="organization",
                        # Open mode (an organisation, not a private
                        # person) but STAGED, so the read-gate hides it
                        # until an operator publishes.
                        surface_mode=SurfaceMode.OPEN.value,
                        publication_state=PublicationState.STAGED.value,
                        batch_id=snap.batch_id,
                    )
                    s.add(ent)
                    await s.flush()
                    stats["canonicals_created"] += 1
                    stats["created_names"].append(name)

                    existing_alias = await s.scalar(
                        select(EntityAlias).where(
                            EntityAlias.source_system == ALIAS_SOURCE,
                            EntityAlias.source_id == oid,
                        )
                    )
                    if existing_alias is None:
                        s.add(
                            EntityAlias(
                                id=_new_id(),
                                canonical_id=ent.id,
                                source_system=ALIAS_SOURCE,
                                source_id=oid,
                                surface_name=name,
                                surface_name_normalized=norm,
                                kind_hint="organization",
                                confidence=1.0,
                            )
                        )
                        stats["aliases_created"] += 1

            # ── stage the vessel→owner edges ──
            for vid, rtype, _ in rows:
                v_row_id = vessel_ids.get(vid)
                if v_row_id is None:
                    stats["vessels_not_in_table"] += 1
                    continue
                if dry_run:
                    stats["edges_staged"] += 1
                    continue
                existing = await s.scalar(
                    select(VesselOwnershipEdge).where(
                        VesselOwnershipEdge.canonical_id == ent.id,
                        VesselOwnershipEdge.vessel_id == v_row_id,
                    )
                )
                if existing is not None:
                    stats["edges_existing"] += 1
                    continue
                s.add(
                    VesselOwnershipEdge(
                        id=_new_id(),
                        canonical_id=ent.id,
                        vessel_id=v_row_id,
                        relation="owns",
                        ofac_relation=rtype,
                        owner_name_raw=name,
                        ofac_owner_id=oid,
                        snapshot_id=snap.id,
                        source_url=snap.source_url,
                        source_sha256=snap.sha256,
                        surface_mode=SurfaceMode.SUPPRESS.value,
                        publication_state=PublicationState.STAGED.value,
                        batch_id=snap.batch_id,
                    )
                )
                stats["edges_staged"] += 1

        if not dry_run:
            await s.commit()
            stats["canonical_total"] = await s.scalar(select(func.count(CanonicalEntity.id)))
    return stats


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Vessels P3 apply.")
    ap.add_argument("--apply", action="store_true", help="Commit. Default is a dry run.")
    ap.add_argument("--cutoff", type=int, default=CUTOFF)
    ap.add_argument("--cache", default="/tmp/sdn_p3a.xml")
    args = ap.parse_args()
    s = asyncio.run(run(cutoff=args.cutoff, dry_run=not args.apply, cache=args.cache))
    print(f"\n=== VESSELS P3 ({'APPLY' if args.apply else 'DRY RUN'}) cutoff >= {s['cutoff']} ===")
    for k in (
        "canonicals_created", "canonicals_reused", "crosswalked", "aliases_created",
        "edges_staged", "edges_existing", "held_individual_owners",
        "held_individual_vessels", "deferred_owners", "deferred_vessels",
        "vessels_not_in_table",
    ):
        print(f"  {k:<26} {s[k]:>7,}")
    print(f"\n  CROSSWALK accepted: {list(CROSSWALK)}")
    print(f"  CROSSWALK held:     {list(CROSSWALK_HELD)}")
    print("\n  sample created canonicals:")
    for n in s["created_names"][:12]:
        print(f"     {n[:70]}")


if __name__ == "__main__":  # pragma: no cover
    _main()
