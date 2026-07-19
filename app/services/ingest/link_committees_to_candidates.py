"""P5.3 — link recipient committees to their candidate (member) canonicals.

Bridges the graph gap: FEC contributes_to targets a candidate's
principal COMMITTEE (e.g. "HANDEL FOR CONGRESS, INC."), a separate
CanonicalEntity from the member (roster canonical with
``fec.candidate`` alias). Model 1 flow queries then need to walk
committee → member, which requires an ``affiliated_with`` edge.

This module scans every ORG-typed canonical carrying a
``fec.committee`` alias, calls FEC's ``/committee/{id}/candidates/``
endpoint, and for each returned candidate id — if the roster canonical
exists (via ``fec.candidate`` alias) — emits an ``affiliated_with``
edge committee → member, cited to the FEC committee record.

Idempotent: dedupes on (source, target) so re-runs cite once.
"""

from __future__ import annotations

import asyncio
import logging
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
    SourceCitation,
    SourceKind,
)
from app.services.ingest.fec import _api_key

logger = logging.getLogger(__name__)


@dataclass
class BridgeStats:
    committees_scanned: int = 0
    committees_with_candidates: int = 0
    edges_created: int = 0
    edges_reused: int = 0
    citations_created: int = 0
    members_not_found: int = 0
    errors: int = 0


async def _member_id_for_fec_candidate(
    session: AsyncSession, fec_candidate_id: str
) -> str | None:
    """Return the roster canonical id for a fec.candidate id, or None."""
    return (
        await session.execute(
            select(EntityAlias.canonical_id).where(
                EntityAlias.source_system == "fec.candidate",
                EntityAlias.source_id == fec_candidate_id,
            )
        )
    ).scalar_one_or_none()


async def _emit_bridge_edge(
    session: AsyncSession,
    committee_canonical: str,
    member_canonical: str,
    committee_id: str,
) -> bool:
    """Emit affiliated_with edge committee → member (create or reuse).
    Returns True on create, False on reuse."""
    existing = (
        await session.execute(
            select(CanonicalEdge).where(
                CanonicalEdge.source_id == committee_canonical,
                CanonicalEdge.target_id == member_canonical,
                CanonicalEdge.relation == EdgeRelation.AFFILIATED_WITH.value,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        return False
    edge = CanonicalEdge(
        source_id=committee_canonical,
        target_id=member_canonical,
        relation=EdgeRelation.AFFILIATED_WITH.value,
        weight=1.0,
    )
    session.add(edge)
    await session.flush()
    session.add(
        SourceCitation(
            edge_id=edge.id,
            kind=SourceKind.FEC_FILING.value,
            citation_ref=committee_id,
            citation_url=f"https://www.fec.gov/data/committee/{committee_id}/",
        )
    )
    return True


async def bridge_all(max_committees: int = 5000) -> BridgeStats:
    """Scan every canonical with a fec.committee alias + emit bridge
    edges to member canonicals."""
    stats = BridgeStats()
    sm = get_sessionmaker()

    # Fetch every (committee_id, canonical_id) via aliases:
    # * fec.committee — the sponsoring PAC we ingested via PAC-mode
    # * fec.disbursement.recipient — recipient committees ingested as
    #   contribution TARGETS in ingest_pac. Their source_id is the
    #   FEC committee id when the recipient was a committee (not a
    #   candidate) — filter on the leading "C".
    async with sm() as session:
        rows_pac = (
            await session.execute(
                select(EntityAlias.source_id, EntityAlias.canonical_id)
                .where(EntityAlias.source_system == "fec.committee")
            )
        ).all()
        rows_recip = (
            await session.execute(
                select(EntityAlias.source_id, EntityAlias.canonical_id)
                .where(EntityAlias.source_system == "fec.disbursement.recipient")
                .where(EntityAlias.source_id.like("C%"))
                .limit(max_committees)
            )
        ).all()
    # Dedupe on committee_id (a canonical may carry both aliases if it
    # was ingested from multiple angles).
    seen: set[str] = set()
    rows: list[tuple[str, str]] = []
    for cid, canon in list(rows_pac) + list(rows_recip):
        if cid in seen:
            continue
        seen.add(cid)
        rows.append((cid, canon))

    async with httpx.AsyncClient(timeout=30.0) as client:
        for committee_id, committee_canonical in rows:
            stats.committees_scanned += 1
            try:
                r = await client.get(
                    f"https://api.open.fec.gov/v1/committee/{committee_id}/candidates/",
                    params={"api_key": _api_key(), "per_page": 20},
                )
                if r.status_code != 200:
                    continue
                payload = r.json()
                cands = payload.get("results") or []
                if not cands:
                    continue
                stats.committees_with_candidates += 1
            except Exception:
                logger.exception(
                    "committee %s candidate lookup failed", committee_id
                )
                stats.errors += 1
                continue

            async with sm() as session:
                for cand in cands:
                    cand_id = cand.get("candidate_id")
                    if not cand_id:
                        continue
                    member_id = await _member_id_for_fec_candidate(
                        session, cand_id
                    )
                    if member_id is None:
                        stats.members_not_found += 1
                        continue
                    try:
                        created = await _emit_bridge_edge(
                            session,
                            committee_canonical,
                            member_id,
                            committee_id,
                        )
                        if created:
                            stats.edges_created += 1
                            stats.citations_created += 1
                        else:
                            stats.edges_reused += 1
                    except Exception:
                        logger.exception(
                            "bridge edge failed %s → %s",
                            committee_canonical, member_id,
                        )
                        stats.errors += 1
                try:
                    await session.commit()
                except Exception:
                    await session.rollback()
                    stats.errors += 1
    return stats


def main() -> None:
    """CLI — `python -m app.services.ingest.link_committees_to_candidates`."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    stats = asyncio.run(bridge_all())
    logger.info("committee↔candidate bridge done: %s", stats)


if __name__ == "__main__":
    main()
