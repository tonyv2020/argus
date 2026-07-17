"""P1 — USAspending ingestion scoped to a GEO Group anchor.

USAspending is keyless (`https://api.usaspending.gov/api/v2`). We fetch the
awards where GEO Group is the recipient, filter to ICE + BOP as awarding
agencies (design §7 slice), and emit `HOLDS_CONTRACT` edges from GEO Group
canonical → the agency canonical, with the award ID as the citation ref.

Every edge carries a `SourceCitation` to the USAspending award-detail URL.
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
    EntityType,
    SourceCitation,
    SourceKind,
)
from app.services.graph.base import normalize_name

logger = logging.getLogger(__name__)

_USA_BASE = "https://api.usaspending.gov/api/v2"
_GEO_RECIPIENT_NAMES = ("GEO GROUP INC", "THE GEO GROUP INC", "GEO GROUP, INC.")
# ICE + BOP — the detention-contract accountability beat (design §7).
# USAspending uses the Awarding SUB-AGENCY for the actual bureau (ICE / BOP);
# the top-level "Awarding Agency" is the department level (DHS / DoJ). Match on
# uppercase substrings that appear in the sub-agency label.
_TARGET_AGENCIES = (
    "IMMIGRATION AND CUSTOMS ENFORCEMENT",
    "BUREAU OF PRISONS",
    "U.S. MARSHALS SERVICE",  # USMS holds detention contracts too; keep for the ICE/BOP beat
)


@dataclass
class UsaSpendingStats:
    """Counters for one USAspending pass."""

    awards_fetched: int = 0
    agencies_matched: int = 0
    edges_created: int = 0
    edges_reused: int = 0
    citations_created: int = 0
    errors: int = 0


async def _post(client: httpx.AsyncClient, path: str, body: dict) -> dict:
    """One POST to api.usaspending.gov, JSON in JSON out."""
    r = await client.post(f"{_USA_BASE}{path}", json=body)
    r.raise_for_status()
    return r.json()


async def _find_or_create_canonical(
    session: AsyncSession,
    surface_name: str,
    entity_type: str,
    source_system: str,
    source_id: str,
) -> str:
    """Reuse alias-keyed canonical + normalized-name-keyed canonical, else create."""
    existing = (
        await session.execute(
            select(EntityAlias).where(
                EntityAlias.source_system == source_system,
                EntityAlias.source_id == source_id,
            )
        )
    ).scalar_one_or_none()
    if existing:
        return existing.canonical_id
    norm = normalize_name(surface_name)
    if norm:
        prior = (
            await session.execute(
                select(CanonicalEntity).where(
                    CanonicalEntity.type == entity_type,
                    CanonicalEntity.canonical_name_normalized == norm,
                )
            )
        ).scalar_one_or_none()
        if prior:
            session.add(
                EntityAlias(
                    canonical_id=prior.id,
                    source_system=source_system,
                    source_id=source_id,
                    surface_name=surface_name,
                    surface_name_normalized=norm,
                )
            )
            return prior.id
    ce = CanonicalEntity(
        canonical_name=surface_name,
        canonical_name_normalized=norm or surface_name.lower(),
        type=entity_type,
    )
    session.add(ce)
    await session.flush()
    session.add(
        EntityAlias(
            canonical_id=ce.id,
            source_system=source_system,
            source_id=source_id,
            surface_name=surface_name,
            surface_name_normalized=norm or surface_name.lower(),
        )
    )
    return ce.id


async def _emit_contract_edge(
    session: AsyncSession,
    src_canonical: str,
    dst_canonical: str,
    amount: float | None,
    award_id: str,
) -> tuple[str, bool]:
    """Create or reuse a HOLDS_CONTRACT edge + attach the USAspending award citation."""
    existing = (
        await session.execute(
            select(CanonicalEdge).where(
                CanonicalEdge.source_id == src_canonical,
                CanonicalEdge.target_id == dst_canonical,
                CanonicalEdge.relation == EdgeRelation.HOLDS_CONTRACT.value,
            )
        )
    ).scalar_one_or_none()
    reused = False
    if existing is None:
        edge = CanonicalEdge(
            source_id=src_canonical,
            target_id=dst_canonical,
            relation=EdgeRelation.HOLDS_CONTRACT.value,
            weight=float(amount or 0.0),
        )
        session.add(edge)
        await session.flush()
    else:
        edge = existing
        edge.weight = float((edge.weight or 0.0) + (amount or 0.0))
        reused = True
    citation_url = f"https://www.usaspending.gov/award/{award_id}"
    session.add(
        SourceCitation(
            edge_id=edge.id,
            kind=SourceKind.USASPENDING_AWARD.value,
            citation_url=citation_url,
            citation_ref=award_id,
        )
    )
    return edge.id, reused


async def _find_geo_group_canonical(session: AsyncSession) -> str | None:
    """Return the pre-existing GEO Group canonical id (from hollywood.entity_tags P0)."""
    row = (
        await session.execute(
            select(CanonicalEntity).where(
                CanonicalEntity.type == EntityType.ORGANIZATION.value,
                CanonicalEntity.canonical_name_normalized == normalize_name("GEO Group"),
            )
        )
    ).scalar_one_or_none()
    return row.id if row else None


async def ingest_geo_group_contracts(max_awards: int = 200) -> UsaSpendingStats:
    """Fetch GEO Group's awards from ICE + BOP and materialise HOLDS_CONTRACT edges."""
    stats = UsaSpendingStats()
    sm = get_sessionmaker()
    async with sm() as session:
        geo_canonical = await _find_geo_group_canonical(session)
    if geo_canonical is None:
        logger.error("GEO Group canonical not found in argus — run P0 resolver first")
        return stats

    body = {
        "filters": {
            "recipient_search_text": list(_GEO_RECIPIENT_NAMES),
            "award_type_codes": ["A", "B", "C", "D"],  # procurement contract types
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Awarding Agency",
            "Awarding Sub Agency",
            "generated_internal_id",
            "Award Amount",
        ],
        "page": 1,
        "limit": min(100, max_awards),
        "sort": "Award Amount",
        "order": "desc",
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        remaining = max_awards
        while remaining > 0:
            body["limit"] = min(100, remaining)
            payload = await _post(client, "/search/spending_by_award/", body)
            rows = payload.get("results", [])
            if not rows:
                break
            async with sm() as session:
                for r in rows:
                    stats.awards_fetched += 1
                    sub_agency = (r.get("Awarding Sub Agency") or "").upper()
                    top_agency = (r.get("Awarding Agency") or "").upper()
                    matched = next((a for a in _TARGET_AGENCIES if a in sub_agency), None)
                    if not matched:
                        continue
                    stats.agencies_matched += 1
                    award_id = r.get("generated_internal_id") or r.get("Award ID")
                    if not award_id:
                        continue
                    # Use the sub-agency as the surface_name (BOP/ICE is the interesting
                    # accountability signal). Include the top agency in the citation for
                    # traceability.
                    agency_canonical = await _find_or_create_canonical(
                        session,
                        surface_name=(r.get("Awarding Sub Agency") or matched).title(),
                        entity_type=EntityType.AGENCY.value,
                        source_system="usaspending.agency",
                        source_id=matched,
                    )
                    del top_agency  # captured for future use but unused today
                    _, reused = await _emit_contract_edge(
                        session,
                        geo_canonical,
                        agency_canonical,
                        r.get("Award Amount"),
                        str(award_id),
                    )
                    if reused:
                        stats.edges_reused += 1
                    else:
                        stats.edges_created += 1
                    stats.citations_created += 1
                try:
                    await session.commit()
                except Exception as exc:  # noqa: BLE001
                    await session.rollback()
                    stats.errors += 1
                    logger.exception("usaspending batch failed: %s", exc)
            remaining -= len(rows)
            page_meta = payload.get("page_metadata", {})
            if not page_meta.get("hasNext"):
                break
            body["page"] += 1
    return stats


def main() -> None:
    """CLI entrypoint — python -m app.services.ingest.usaspending."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = asyncio.run(ingest_geo_group_contracts())
    logger.info("usaspending ingest done: %s", stats)


if __name__ == "__main__":
    main()
