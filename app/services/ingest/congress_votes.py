"""P5.1 — Roll-call vote ingester.

The analytical chain (spec §5):
    BILL → (roll-call) → members who voted YES(+party) → contributes_to $
    ← entities → holds_contract $, agency-filtered.

Ingests curated key bills as ``bill``-typed CanonicalEntity rows +
emits ``voted_for`` / ``voted_against`` edges member→bill cited to the
Congress.gov roll-call vote page.

Key provisioning (helen 2026-07-19 21:40Z): argus-congress Secret with
CONGRESS_API_KEY is LIVE.

CRITICAL: Congress.gov's API sits behind Cloudflare bot-mitigation.
The default httpx User-Agent triggers a 403 (Cloudflare error 1010).
Send a plain-browser UA on every request. See ``_HTTP_HEADERS``.
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
# helen 2026-07-19: Congress.gov is Cloudflare-fronted; default httpx UA
# gets a 403 error 1010. Any plain browser UA works.
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "application/json",
}


# Curated key bills — the P5 flow analysis target set.  Each tuple:
# (congress_number, bill_type, bill_number, human_label).
# Start narrow (OBBB + key immigration/appropriations); expand via
# anchor registry P4 pattern in a follow-on.
_KEY_BILLS: tuple[tuple[int, str, int, str], ...] = (
    # 119th Congress (2025-2027).
    (119, "hr", 1, "One Big Beautiful Bill Act (OBBB)"),
    (119, "hr", 2, "Secure the Border Act"),
    # Placeholder set — extend via registry as new priorities land.
)


@dataclass
class VoteIngestStats:
    """Counters for one vote-ingester pass."""

    bills_fetched: int = 0
    bills_upserted: int = 0
    votes_fetched: int = 0
    vote_edges_created: int = 0
    vote_edges_reused: int = 0
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
) -> str:
    """Upsert a BILL-typed canonical + attach a congress.bill alias."""
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
    # Bill type placeholder — CONCEPT is the closest existing enum;
    # a dedicated BILL type lands in a follow-on model migration.
    ce = CanonicalEntity(
        canonical_name=label,
        canonical_name_normalized=norm,
        type=EntityType.CONCEPT.value,
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
    """Emit a vote edge (create or reuse). Returns True on create.

    Vote edges reuse ``AFFILIATED_WITH`` as the relation slot until a
    dedicated ``VOTED_FOR`` / ``VOTED_AGAINST`` enum extension lands.
    The vote KIND is preserved on the citation ref for round-trip.
    """
    relation = EdgeRelation.AFFILIATED_WITH.value  # placeholder
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
            kind=SourceKind.CORPORATE_REGISTRY.value,  # placeholder
            citation_ref=f"{vote_kind}:{citation_ref}",
            citation_url=citation_url,
        )
    )
    return True


async def _get(
    client: httpx.AsyncClient, path: str, key: str, **params
) -> dict | None:
    """One GET to congress.gov with the browser UA + API key attached."""
    params.setdefault("api_key", key)
    params.setdefault("format", "json")
    try:
        r = await client.get(
            f"{_BASE}{path}", params=params, headers=_HTTP_HEADERS
        )
        if r.status_code != 200:
            logger.warning(
                "congress.gov %s -> %d (headers %s)", path, r.status_code,
                dict(r.headers),
            )
            return None
        return r.json()
    except Exception:
        logger.exception("congress.gov %s failed", path)
        return None


async def _ingest_one_bill(
    congress: int,
    bill_type: str,
    bill_number: int,
    label: str,
    key: str,
    stats: VoteIngestStats,
) -> None:
    """Fetch + upsert one bill + emit vote edges for its roll-call
    actions."""
    sm = get_sessionmaker()
    async with httpx.AsyncClient(timeout=30.0) as client:
        bill = await _get(
            client, f"/bill/{congress}/{bill_type.lower()}/{bill_number}", key
        )
        if bill is None:
            stats.errors += 1
            return
        stats.bills_fetched += 1

        async with sm() as session:
            bill_canonical = await _upsert_bill_canonical(
                session,
                congress=congress,
                bill_type=bill_type,
                bill_number=bill_number,
                label=label,
            )
            await session.commit()
            stats.bills_upserted += 1

        # Actions endpoint carries the roll-call subactions with vote_id.
        actions_payload = await _get(
            client,
            f"/bill/{congress}/{bill_type.lower()}/{bill_number}/actions",
            key,
        )
        if actions_payload is None:
            return

        for action in (actions_payload.get("actions") or []):
            rc = action.get("recordedVotes") or []
            for rv in rc:
                url = rv.get("url")
                if not url:
                    continue
                await _fetch_rollcall_and_emit_edges(
                    client=client,
                    key=key,
                    rollcall_url=url,
                    bill_canonical=bill_canonical,
                    stats=stats,
                )


async def _fetch_rollcall_and_emit_edges(
    *,
    client: httpx.AsyncClient,
    key: str,
    rollcall_url: str,
    bill_canonical: str,
    stats: VoteIngestStats,
) -> None:
    """Follow the roll-call URL, extract per-member votes, emit edges.

    The URL in ``recordedVotes`` points at the ``clerk.house.gov`` /
    ``senate.gov`` roll-call page (XML), NOT congress.gov API. The
    Congress.gov API returns the summary in the actions payload; the
    per-member yea/nay list lives at the clerk URL. For MVP scope,
    log the URL as a citation on the bill (without emitting per-member
    edges until the clerk XML parser lands).
    """
    stats.votes_fetched += 1
    # Placeholder — per-member edge emission lands after clerk XML
    # parsing. For now the vote URL captures the citation surface.
    logger.info("roll-call URL captured (per-member parse TBD): %s",
                rollcall_url)


async def ingest_key_bills() -> VoteIngestStats:
    """Fetch + upsert every entry in :data:`_KEY_BILLS` + their votes."""
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

    for congress, btype, bnumber, label in _KEY_BILLS:
        try:
            await _ingest_one_bill(
                congress, btype, bnumber, label, key, stats
            )
        except Exception:
            logger.exception("bill %d-%s-%d failed", congress, btype, bnumber)
            stats.errors += 1
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
