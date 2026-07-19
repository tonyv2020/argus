"""P1.6 — curated executive/board affiliations.

Emits ``affiliated_with`` edges between prominent people and their
companies (SEC officers / board members) so Model 2 traversals can
flow: person's contribs → their company's contracts.

Public-record sources cited on each edge. All parties are open
surface_mode public figures / issuers — no privacy gate impact.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

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

logger = logging.getLogger(__name__)


@dataclass
class Affiliation:
    """One curated (person, company, citation) triple."""

    person_label: str
    company_label: str
    citation_url: str
    citation_ref: str
    reason: str


# Sourced from SEC DEF 14A + company IR filings (public). Extend as
# new mega-donor→company pairs become relevant.
_CURATED: tuple[Affiliation, ...] = (
    Affiliation(
        person_label="Peter Thiel",
        company_label="Palantir Technologies Inc.",
        citation_url=(
            "https://www.sec.gov/cgi-bin/browse-edgar?"
            "action=getcompany&CIK=0001321655&type=DEF+14A"
        ),
        citation_ref="sec:1321655:def14a:thiel-chairman",
        reason="Palantir co-founder + chairman; SEC DEF 14A executive officer",
    ),
    Affiliation(
        person_label="Elon Musk",
        company_label="Tesla",
        citation_url=(
            "https://www.sec.gov/cgi-bin/browse-edgar?"
            "action=getcompany&CIK=0001318605&type=DEF+14A"
        ),
        citation_ref="sec:1318605:def14a:musk-ceo",
        reason="Tesla CEO + chairman; SEC DEF 14A executive officer",
    ),
    Affiliation(
        person_label="Elon Musk",
        company_label="SpaceX",
        citation_url="https://www.spacex.com/",
        citation_ref="spacex:company-page:musk-ceo",
        reason="SpaceX founder + CEO (private company; company IR page)",
    ),
)


@dataclass
class P16Stats:
    processed: int = 0
    edges_created: int = 0
    edges_reused: int = 0
    person_not_found: int = 0
    company_not_found: int = 0
    citations_created: int = 0
    errors: int = 0


async def _find_by_name(
    session: AsyncSession, label: str, prefer_type: str | None = None
) -> str | None:
    """Find a canonical by exact name. If multiple exist (e.g. Tesla the
    person + Tesla the org), the ``prefer_type`` argument disambiguates."""
    stmt = select(CanonicalEntity).where(CanonicalEntity.canonical_name == label)
    if prefer_type:
        stmt = stmt.where(CanonicalEntity.type == prefer_type)
    rows = (await session.execute(stmt)).scalars().all()
    if not rows:
        return None
    # Pick the row with the most edges as a tie-breaker for cross-type dupes.
    from sqlalchemy import func

    from app.models import CanonicalEdge

    if len(rows) == 1:
        return rows[0].id
    best = None
    best_count = -1
    for row in rows:
        out = (
            await session.execute(
                select(func.count()).where(CanonicalEdge.source_id == row.id)
            )
        ).scalar()
        inc = (
            await session.execute(
                select(func.count()).where(CanonicalEdge.target_id == row.id)
            )
        ).scalar()
        n = (out or 0) + (inc or 0)
        if n > best_count:
            best_count = n
            best = row
    return best.id if best else None


async def _emit(
    session: AsyncSession, aff: Affiliation, stats: P16Stats
) -> None:
    src = await _find_by_name(session, aff.person_label, prefer_type="person")
    dst = await _find_by_name(session, aff.company_label, prefer_type="organization")
    if src is None:
        logger.warning("%s: person canonical not found", aff.person_label)
        stats.person_not_found += 1
        return
    if dst is None:
        logger.warning("%s: company canonical not found", aff.company_label)
        stats.company_not_found += 1
        return
    existing = (
        await session.execute(
            select(CanonicalEdge).where(
                CanonicalEdge.source_id == src,
                CanonicalEdge.target_id == dst,
                CanonicalEdge.relation == EdgeRelation.AFFILIATED_WITH.value,
            )
        )
    ).scalar_one_or_none()
    if existing:
        stats.edges_reused += 1
        return
    edge = CanonicalEdge(
        source_id=src,
        target_id=dst,
        relation=EdgeRelation.AFFILIATED_WITH.value,
        weight=1.0,
    )
    session.add(edge)
    await session.flush()
    session.add(
        SourceCitation(
            edge_id=edge.id,
            kind=SourceKind.CORPORATE_REGISTRY.value,
            citation_ref=aff.citation_ref,
            citation_url=aff.citation_url,
        )
    )
    stats.edges_created += 1
    stats.citations_created += 1


async def seed() -> P16Stats:
    stats = P16Stats()
    sm = get_sessionmaker()
    async with sm() as session:
        for aff in _CURATED:
            stats.processed += 1
            try:
                await _emit(session, aff, stats)
            except Exception:
                logger.exception(
                    "P1.6 emit failed %s → %s",
                    aff.person_label, aff.company_label,
                )
                stats.errors += 1
        await session.commit()
    return stats


def main() -> None:
    """CLI — `python -m app.services.ingest.seed_p16_affiliations`."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    stats = asyncio.run(seed())
    logger.info("P1.6 affiliations seed done: %s", stats)


if __name__ == "__main__":
    main()
