"""P4 D — Congressional roster ingester (P1.5 folded into P4).

Populates the anchor registry with a canonical PERSON row per current
US House + Senate member from the ``@unitedstates/congress-legislators``
dataset (public domain, authoritative crosswalk of bioguide id + FEC
candidate ids + party/state/chamber + name variants).

Members of Congress were entirely absent from Argus's canonical entities
pre-P4: FEC contributions target the campaign COMMITTEE
("PETE RICKETTS FOR SENATE") with no link to the member, and members
only appeared as fragmented news-person nodes (Cruz=8, Warren=8). This
ingester creates the missing backbone.

Key design points (helen 2026-07-19):
* ``surface_mode='open'`` — public officials. NEVER default a person
  row to open without a public-official rationale; the privacy gate
  exists for private persons.
* ``fec_candidate_ids`` gets the ``fec.house`` + ``fec.senate`` id
  arrays from the dataset (a member may have IDs across multiple runs).
* ``name_variants`` gets the ``first`` + ``last`` + ``official_full``
  + any ``other_names`` — the surface strings news + FEC + roll-call
  data uses.
* ``notes`` carries chamber / state / party as a compact string so the
  P5 flow queries can filter without decoding a JSONB.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

import httpx
import yaml

from app.db import get_sessionmaker
from app.services.anchor_registry import upsert_anchor
from app.services.ingest.fec import _upsert_entity as _upsert_person_canonical
from app.models import EntityType

logger = logging.getLogger(__name__)

_ROSTER_URL = (
    "https://raw.githubusercontent.com/unitedstates/"
    "congress-legislators/main/legislators-current.yaml"
)


@dataclass
class RosterStats:
    """Counters for one roster sweep."""

    members_fetched: int = 0
    members_upserted: int = 0
    house_members: int = 0
    senate_members: int = 0
    fec_candidate_ids_attached: int = 0
    person_canonicals_created: int = 0
    bioguide_aliases_created: int = 0
    fec_candidate_aliases_created: int = 0
    errors: int = 0


async def _fetch_roster() -> list[dict[str, Any]]:
    """One HTTP GET to the legislators-current.yaml. Returns parsed list."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.get(_ROSTER_URL, follow_redirects=True)
        r.raise_for_status()
    return yaml.safe_load(r.text)


def _extract_current_term(member: dict[str, Any]) -> dict[str, Any] | None:
    """Return the most-recent-start term (the active one for a
    current-legislators row).  Sorted lexicographically on ``start``,
    which is ISO YYYY-MM-DD → stable ordering."""
    terms = member.get("terms") or []
    if not terms:
        return None
    return sorted(terms, key=lambda t: t.get("start", ""))[-1]


def _label_for(member: dict[str, Any]) -> str:
    """Human display label — ``official_full`` from name block, else
    ``first last``."""
    name = member.get("name") or {}
    return (
        name.get("official_full")
        or f"{name.get('first', '').strip()} {name.get('last', '').strip()}".strip()
    )


def _name_variants(member: dict[str, Any]) -> list[str]:
    """Every surface name we'd want to alias-search against — FEC
    candidate names use LAST, FIRST; news uses `first last`."""
    name = member.get("name") or {}
    variants: list[str] = []
    for k in ("official_full", "first", "last", "middle",
              "nickname", "suffix"):
        v = name.get(k)
        if v:
            variants.append(str(v).strip())
    # LAST, FIRST — the FEC candidate-name shape.
    if name.get("first") and name.get("last"):
        variants.append(f"{name['last'].strip()}, {name['first'].strip()}")
    # First Last — the news shape.
    if name.get("first") and name.get("last"):
        variants.append(f"{name['first'].strip()} {name['last'].strip()}")
    for other in member.get("other_names") or []:
        if other.get("last") and other.get("first"):
            variants.append(f"{other['first']} {other['last']}")
    # Dedupe preserving order.
    seen: set[str] = set()
    out: list[str] = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _fec_candidate_ids(member: dict[str, Any]) -> list[str]:
    """All fec.candidate ids in the ``id`` block (may be a list)."""
    id_block = member.get("id") or {}
    ids = id_block.get("fec") or []
    if isinstance(ids, str):
        return [ids]
    return list(ids)


async def ingest_roster() -> RosterStats:
    """Fetch legislators-current.yaml + upsert every member into the
    registry as a `person` row.  Idempotent (upsert on (label, entity_type))."""
    stats = RosterStats()
    try:
        roster = await _fetch_roster()
    except Exception:
        logger.exception("failed to fetch legislators-current.yaml")
        stats.errors = 1
        return stats

    stats.members_fetched = len(roster)
    sm = get_sessionmaker()
    async with sm() as session:
        for member in roster:
            try:
                term = _extract_current_term(member)
                if not term:
                    continue
                chamber = term.get("type", "").lower()  # 'sen' or 'rep'
                state = term.get("state", "")
                party = term.get("party", "")
                district = term.get("district")
                label = _label_for(member)
                if not label:
                    continue

                bioguide = (member.get("id") or {}).get("bioguide")
                fec_ids = _fec_candidate_ids(member)
                variants = _name_variants(member)
                if bioguide and bioguide not in variants:
                    variants.append(bioguide)

                district_str = f"-{district}" if district is not None else ""
                notes = (
                    f"chamber={chamber} state={state} party={party}"
                    f"{district_str} bioguide={bioguide}"
                )

                # Materialize a CanonicalEntity(person) + aliases so
                # roll-call votes + FEC contributions resolve back to the
                # member. anchor_registry rows alone don't touch the graph;
                # helen 2026-07-19 flagged that.
                canonical_id: str | None = None
                if bioguide:
                    canonical_id = await _upsert_person_canonical(
                        session,
                        label,
                        EntityType.PERSON.value,
                        "bioguide",
                        bioguide,
                        kind_hint="person",
                    )
                    stats.bioguide_aliases_created += 1
                # Each fec.candidate id becomes its own alias on the same
                # canonical — a member with multiple runs (S8TX00232 +
                # S6TX00298) has multiple ids all pointing at the same
                # person.
                for fec_id in fec_ids:
                    canonical_from_fec = await _upsert_person_canonical(
                        session,
                        label,
                        EntityType.PERSON.value,
                        "fec.candidate",
                        fec_id,
                        kind_hint="person",
                    )
                    if canonical_id is None:
                        canonical_id = canonical_from_fec
                    stats.fec_candidate_aliases_created += 1
                if canonical_id is not None:
                    stats.person_canonicals_created += 1

                await upsert_anchor(
                    session,
                    label=label,
                    entity_type="person",
                    priority_domain="congress",
                    fec_candidate_ids=tuple(fec_ids),
                    name_variants=tuple(variants),
                    surface_mode="open",
                    canonical_id=canonical_id,
                    notes=notes,
                )
                stats.members_upserted += 1
                if chamber == "sen":
                    stats.senate_members += 1
                else:
                    stats.house_members += 1
                if fec_ids:
                    stats.fec_candidate_ids_attached += 1
            except Exception:
                logger.exception(
                    "roster upsert failed for %s", _label_for(member)
                )
                stats.errors += 1
        await session.commit()
    return stats


def main() -> None:
    """CLI entry — ``python -m app.services.ingest.congress_roster``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    stats = asyncio.run(ingest_roster())
    logger.info("congress roster ingest done: %s", stats)


if __name__ == "__main__":
    main()
