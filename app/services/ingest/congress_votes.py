"""P5.1 — Roll-call vote ingester (real per-member edges).

Ingests curated key bills as ``bill``-typed CanonicalEntity rows +
emits ``voted_for`` / ``voted_against`` edges member→bill for every
recorded per-member vote, cited to the clerk/Congress.gov roll-call
XML URL.

Two data sources:
    * Congress.gov API for the BILL metadata + the list of recorded-
      vote URLs (Cloudflare-fronted, needs a browser User-Agent).
    * House Clerk XML (`clerk.house.gov/evs/...`) + Senate LRC XML
      (`senate.gov/legislative/LIS/...`) for the per-member roll call.

House Clerk XML shape (verified live 2026-07-19 on roll190.xml):
    <recorded-vote>
      <legislator name-id="B001318" party="D" state="VT">Balint</legislator>
      <vote>No</vote>  <!-- Aye | No | Present | Not Voting -->
    </recorded-vote>
The ``name-id`` attribute IS the bioguide id — resolved via
``EntityAlias.source_system='bioguide'`` to the roster canonical.

Vote-value normalisation:
    Aye | Yea → voted_for
    No | Nay  → voted_against
    Present | Not Voting → skipped (no edge)
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass
from xml.etree import ElementTree as ET

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
# Congress.gov + clerk sites are Cloudflare-fronted; default httpx UA
# hits 403 error 1010. Plain Chrome UA on every request.
_HTTP_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
    ),
    "Accept": "application/json, application/xml, text/xml",
}


# Curated key bills — the P5 flow-analysis target set.
_KEY_BILLS: tuple[tuple[int, str, int, str], ...] = (
    (119, "hr", 1, "One Big Beautiful Bill Act (OBBB)"),
    (119, "hr", 2, "Secure the Border Act"),
)


_YEA = {"aye", "yea", "yes"}
_NAY = {"no", "nay"}


@dataclass
class VoteIngestStats:
    """Counters for one vote-ingester pass."""

    bills_fetched: int = 0
    bills_upserted: int = 0
    rollcall_urls_fetched: int = 0
    rollcall_urls_failed: int = 0
    per_member_votes_parsed: int = 0
    voted_for_edges_created: int = 0
    voted_against_edges_created: int = 0
    edges_reused: int = 0
    citations_created: int = 0
    members_not_found: int = 0
    votes_skipped_non_directional: int = 0
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
    ce = CanonicalEntity(
        canonical_name=label,
        canonical_name_normalized=norm,
        type=EntityType.BILL.value,
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
    relation: str,
    citation_url: str,
    citation_ref: str,
) -> bool:
    """Emit a vote edge (create or reuse). Returns True on create."""
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
            kind=SourceKind.CONGRESS_VOTE.value,
            citation_ref=citation_ref,
            citation_url=citation_url,
        )
    )
    return True


async def _fetch_json(
    client: httpx.AsyncClient, url: str, **params
) -> dict | None:
    try:
        r = await client.get(url, params=params, headers=_HTTP_HEADERS)
        if r.status_code != 200:
            logger.warning("GET %s -> %d", url, r.status_code)
            return None
        return r.json()
    except Exception:
        logger.exception("json fetch %s failed", url)
        return None


async def _fetch_xml(client: httpx.AsyncClient, url: str) -> str | None:
    try:
        r = await client.get(url, headers=_HTTP_HEADERS)
        if r.status_code != 200:
            logger.warning("GET %s -> %d", url, r.status_code)
            return None
        return r.text
    except Exception:
        logger.exception("xml fetch %s failed", url)
        return None


def _parse_house_clerk_xml(xml_text: str) -> list[tuple[str, str]]:
    """Parse House Clerk roll-call XML → list of (bioguide, vote_value).

    Non-directional votes (Present, Not Voting) are still yielded so
    the caller can count them; the caller decides whether to emit an
    edge (only for Yea/Aye/No/Nay).
    """
    out: list[tuple[str, str]] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        logger.exception("clerk XML parse failed")
        return out
    for rv in root.iter("recorded-vote"):
        leg = rv.find("legislator")
        vote = rv.find("vote")
        if leg is None or vote is None:
            continue
        bioguide = leg.get("name-id") or ""
        value = (vote.text or "").strip()
        if not bioguide or not value:
            continue
        out.append((bioguide, value))
    return out


def _parse_senate_lrc_xml(xml_text: str) -> list[tuple[str, str]]:
    """Parse Senate LRC roll-call XML → list of (bioguide, vote_value).

    Senate XML shape:
        <member>
          <lis_member_id>S001</lis_member_id>
          <member_full>Sanders, Bernard (I-VT)</member_full>
          <vote_cast>Yea</vote_cast>
        </member>

    The ``lis_member_id`` is Senate-internal; the bioguide is embedded
    in ``member_full`` via other means. For the MVP we skip senate
    (the LRC XML doesn't carry bioguide directly). Follow-on: crosswalk
    lis_member_id → bioguide via legislators dataset.
    """
    return []


async def _ingest_one_bill(
    congress: int,
    bill_type: str,
    bill_number: int,
    label: str,
    key: str,
    stats: VoteIngestStats,
) -> None:
    """Fetch + upsert one bill + emit per-member vote edges."""
    sm = get_sessionmaker()
    async with httpx.AsyncClient(timeout=60.0) as client:
        bill_payload = await _fetch_json(
            client,
            f"{_BASE}/bill/{congress}/{bill_type.lower()}/{bill_number}",
            api_key=key, format="json",
        )
        if bill_payload is None:
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

        actions_payload = await _fetch_json(
            client,
            f"{_BASE}/bill/{congress}/{bill_type.lower()}/{bill_number}/actions",
            api_key=key, format="json",
        )
        if actions_payload is None:
            return

        # Dedupe roll-call URLs — Congress.gov often repeats them per
        # action.
        rollcall_urls: set[str] = set()
        for action in actions_payload.get("actions") or []:
            for rv in action.get("recordedVotes") or []:
                url = rv.get("url")
                if url:
                    rollcall_urls.add(url)

        for url in rollcall_urls:
            stats.rollcall_urls_fetched += 1
            xml_text = await _fetch_xml(client, url)
            if xml_text is None:
                stats.rollcall_urls_failed += 1
                continue
            if "clerk.house.gov" in url:
                per_member = _parse_house_clerk_xml(xml_text)
            elif "senate.gov" in url:
                per_member = _parse_senate_lrc_xml(xml_text)
            else:
                per_member = []
            stats.per_member_votes_parsed += len(per_member)

            async with sm() as session:
                for bioguide, vote_value in per_member:
                    vlow = vote_value.lower()
                    if vlow in _YEA:
                        relation = EdgeRelation.VOTED_FOR.value
                    elif vlow in _NAY:
                        relation = EdgeRelation.VOTED_AGAINST.value
                    else:
                        stats.votes_skipped_non_directional += 1
                        continue
                    member_id = await _find_member_by_bioguide(
                        session, bioguide
                    )
                    if member_id is None:
                        stats.members_not_found += 1
                        continue
                    try:
                        created = await _emit_vote_edge(
                            session,
                            member_canonical=member_id,
                            bill_canonical=bill_canonical,
                            relation=relation,
                            citation_url=url,
                            citation_ref=f"{vote_value}:{bioguide}",
                        )
                        if created:
                            if relation == EdgeRelation.VOTED_FOR.value:
                                stats.voted_for_edges_created += 1
                            else:
                                stats.voted_against_edges_created += 1
                            stats.citations_created += 1
                        else:
                            stats.edges_reused += 1
                    except Exception:
                        logger.exception(
                            "vote edge failed member=%s bill=%s",
                            bioguide, bill_canonical,
                        )
                        stats.errors += 1
                try:
                    await session.commit()
                except Exception:
                    await session.rollback()
                    stats.errors += 1


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
