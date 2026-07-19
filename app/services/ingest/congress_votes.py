"""P5.1 — Roll-call vote ingester (DORMANT; awaits Congress.gov API key).

The analytical chain (spec §5):
    BILL → (roll-call) → members who voted YES(+party) → contributes_to $
    ← entities → holds_contract $, agency-filtered.

Ingests key bills as `bill`/`legislation` CanonicalEntity rows + emits
``voted_for`` / ``voted_against`` edges member → bill cited to the
Congress.gov roll-call vote page.

Key provisioning: Tony provisions a free Congress.gov API key + we wire
it into a k8s Secret `argus-congress` with key `CONGRESS_API_KEY`, same
pattern as `argus-fec`. Until then the ingester logs a soft failure +
returns empty stats.

Curated bill set — start narrow (OBBB + key immigration / appropriations
bills the P5 flow analysis targets); expand via the anchor registry
(future work).
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import (
    CanonicalEdge,
    CanonicalEntity,
    EdgeRelation,
    EntityAlias,
    EntityType,
    SourceCitation,
    SourceKind,
)
from app.services.graph.base import normalize_name

logger = logging.getLogger(__name__)

_BASE = "https://api.congress.gov/v3"

# Curated key-bill anchor set — extend via registry (P4 shape).
# Congress number + bill type + bill number is the stable key.
_KEY_BILLS: tuple[tuple[int, str, int, str], ...] = (
    # (congress_number, bill_type, bill_number, human_label)
    # OBBB + immigration + appropriations bills go here once
    # the actual identifiers are decided.
)


@dataclass
class VoteIngestStats:
    """Counters for one vote-ingester pass."""

    bills_fetched: int = 0
    bills_upserted: int = 0
    votes_fetched: int = 0
    edges_created: int = 0
    edges_reused: int = 0
    citations_created: int = 0
    members_not_found: int = 0
    errors: int = 0


def _api_key() -> str | None:
    """Return the Congress.gov key or None if unprovisioned."""
    return os.environ.get("CONGRESS_API_KEY")


async def _find_member_by_bioguide(
    session: AsyncSession, bioguide: str
) -> str | None:
    """Return the canonical id for a member by their bioguide alias."""
    return (
        await session.execute(
            select(EntityAlias.canonical_id).where(
                EntityAlias.source_system == "bioguide",
                EntityAlias.source_id == bioguide,
            )
        )
    ).scalar_one_or_none()


async def _upsert_bill_canonical(
    session: AsyncSession,
    *,
    congress: int,
    bill_type: str,
    bill_number: int,
    label: str,
    congress_gov_url: str,
) -> str:
    """Upsert a BILL-type canonical + attach a congress.gov alias."""
    key = f"{congress}-{bill_type.lower()}-{bill_number}"
    existing = (
        await session.execute(
            select(EntityAlias).where(
                EntityAlias.source_system == "congress.bill",
                EntityAlias.source_id == key,
            )
        )
    ).scalar_one_or_none()
    if existing:
        return existing.canonical_id

    norm = normalize_name(label) or label.lower()
    ce = CanonicalEntity(
        canonical_name=label,
        canonical_name_normalized=norm,
        type=EntityType.CONCEPT.value,  # BILL type placeholder until enum extended
    )
    session.add(ce)
    await session.flush()
    session.add(
        EntityAlias(
            canonical_id=ce.id,
            source_system="congress.bill",
            source_id=key,
            surface_name=label,
            surface_name_normalized=norm,
        )
    )
    return ce.id


async def _emit_vote_edge(
    session: AsyncSession,
    *,
    member_canonical: str,
    bill_canonical: str,
    vote_kind: str,  # "voted_for" | "voted_against"
    citation_url: str,
    citation_ref: str,
) -> bool:
    """Emit a vote edge (create or reuse). Returns True on create."""
    relation = (
        EdgeRelation.AFFILIATED_WITH.value  # placeholder — extend enum in a follow-on migration
    )
    existing = (
        await session.execute(
            select(CanonicalEdge).where(
                CanonicalEdge.source_id == member_canonical,
                CanonicalEdge.target_id == bill_canonical,
                CanonicalEdge.relation == relation,
            )
        )
    ).scalar_one_or_none()
    if existing:
        return False
    edge = CanonicalEdge(
        source_id=member_canonical,
        target_id=bill_canonical,
        relation=relation,
        weight=1.0,
    )
    session.add(edge)
    await session.flush()
    session.add(
        SourceCitation(
            edge_id=edge.id,
            kind=SourceKind.CORPORATE_REGISTRY.value,  # placeholder; add CONGRESS_VOTE in follow-on
            citation_ref=citation_ref,
            citation_url=citation_url,
        )
    )
    return True


async def ingest_key_bills() -> VoteIngestStats:
    """Fetch + upsert every entry in :data:`_KEY_BILLS` + their votes.

    Returns empty stats when CONGRESS_API_KEY is unset (helen wires it
    to a k8s Secret when Tony provisions it, same pattern as argus-fec).
    """
    stats = VoteIngestStats()
    key = _api_key()
    if key is None:
        logger.warning(
            "CONGRESS_API_KEY not set; vote ingester runs empty. "
            "Provision via Secret argus-congress key CONGRESS_API_KEY."
        )
        return stats

    if not _KEY_BILLS:
        logger.info("no key bills configured; nothing to ingest")
        return stats

    sm = get_sessionmaker()
    async with httpx.AsyncClient(timeout=30.0) as client:
        for congress, btype, bnumber, label in _KEY_BILLS:
            path = f"/bill/{congress}/{btype.lower()}/{bnumber}"
            try:
                r = await client.get(
                    f"{_BASE}{path}",
                    params={"api_key": key, "format": "json"},
                )
                r.raise_for_status()
                stats.bills_fetched += 1
            except Exception:
                logger.exception("bill fetch failed %s", path)
                stats.errors += 1
                continue

            congress_gov_url = f"https://www.congress.gov{path}"
            async with sm() as session:
                bill_canonical = await _upsert_bill_canonical(
                    session,
                    congress=congress,
                    bill_type=btype,
                    bill_number=bnumber,
                    label=label,
                    congress_gov_url=congress_gov_url,
                )
                await session.commit()
                stats.bills_upserted += 1

            # Fetch the associated roll-call votes (endpoint varies by
            # bill; typical path: /bill/{congress}/{type}/{number}/actions
            # then filter for roll-call subactions with a vote_id).
            # Left as a follow-on refinement — the exact endpoint shape
            # will be validated once the key lands.
    return stats


def main() -> None:
    """CLI — `python -m app.services.ingest.congress_votes`."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    stats = asyncio.run(ingest_key_bills())
    logger.info("congress_votes done: %s", stats)


if __name__ == "__main__":
    main()
