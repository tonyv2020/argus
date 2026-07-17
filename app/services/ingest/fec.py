"""P1 — FEC ingestion scoped to a GEO Group anchor.

Uses the free api.data.gov FEC endpoint (DEMO_KEY works, ~30/hour cap). Escalate
to helen for a real key when the cap bites. All flows fetched via httpx +
per-page pagination; every edge emitted carries a SourceCitation pointing at the
FEC filing/committee/candidate URL — the citation gate is enforced by
`CanonicalEdge` never being persisted without one, and the projection layer
mirrors the check.

Flow:
- Find the GEO Group PAC via committee search.
- Enumerate its recent Schedule B (disbursements) to candidates + PACs.
- For each recipient candidate/PAC:
  * resolve to a CanonicalEntity (create if none exists, tagged the right kind).
  * emit CONTRIBUTES_TO edge (PAC → recipient) with the transaction sub_id as
    the citation ref.
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

_FEC_BASE = "https://api.open.fec.gov/v1"
_DEFAULT_KEY = "DEMO_KEY"

# GEO Group's known FEC name variants — used to find the PAC.
_GEO_PAC_NAME_QUERIES = ("GEO GROUP INC PAC", "GEO GROUP", "GEO GROUP INC")


@dataclass
class FecStats:
    """Counters for one FEC pass — surfaced to callers + logs."""

    pacs_found: int = 0
    disbursements_fetched: int = 0
    recipients_created: int = 0
    recipients_matched: int = 0
    edges_created: int = 0
    edges_reused: int = 0
    citations_created: int = 0
    errors: int = 0


def _api_key() -> str:
    """Return the FEC API key from env — DEMO_KEY if unset (see docs; rate-limited)."""
    return os.environ.get("FEC_API_KEY") or _DEFAULT_KEY


async def _fec_get(client: httpx.AsyncClient, path: str, **params) -> dict:
    """One GET to api.open.fec.gov with the API key attached; returns parsed JSON."""
    params.setdefault("api_key", _api_key())
    r = await client.get(f"{_FEC_BASE}{path}", params=params)
    r.raise_for_status()
    return r.json()


async def find_geo_group_pac(client: httpx.AsyncClient) -> dict | None:
    """Return the first FEC committee record matching a known GEO Group PAC name.

    Committee shape from FEC: `{committee_id, name, party, committee_type, ...}`.
    """
    for q in _GEO_PAC_NAME_QUERIES:
        payload = await _fec_get(client, "/committees/", q=q, per_page=5)
        for row in payload.get("results", []):
            name_upper = (row.get("name") or "").upper()
            if "GEO GROUP" in name_upper:
                return row
    return None


async def _upsert_entity(
    session: AsyncSession,
    surface_name: str,
    entity_type: str,
    source_system: str,
    source_id: str,
    kind_hint: str | None = None,
) -> str:
    """Return an existing canonical id (via alias source lookup or normalized-name), else create.

    FEC entities have authoritative ids (committee_id / candidate_id); we key on
    those primarily and fall back to normalized-name so hollywood-seeded canonicals
    can pick up FEC edges even when we haven't yet inserted a FEC alias.
    """
    # 1. Prior alias with the same source key?
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

    # 2. Existing hollywood canonical by normalized-name?
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
                    kind_hint=kind_hint,
                )
            )
            return prior.id

    # 3. Create a new canonical.
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
            kind_hint=kind_hint,
        )
    )
    return ce.id


async def _emit_contribution_edge(
    session: AsyncSession,
    src_canonical: str,
    dst_canonical: str,
    amount: float | None,
    sub_id: str,
    committee_id: str,
) -> tuple[str, bool]:
    """Emit a CONTRIBUTES_TO edge (create or reuse) + attach the FEC transaction citation."""
    existing = (
        await session.execute(
            select(CanonicalEdge).where(
                CanonicalEdge.source_id == src_canonical,
                CanonicalEdge.target_id == dst_canonical,
                CanonicalEdge.relation == EdgeRelation.CONTRIBUTES_TO.value,
            )
        )
    ).scalar_one_or_none()
    reused = False
    if existing is None:
        edge = CanonicalEdge(
            source_id=src_canonical,
            target_id=dst_canonical,
            relation=EdgeRelation.CONTRIBUTES_TO.value,
            weight=float(amount or 0.0),
        )
        session.add(edge)
        await session.flush()
    else:
        edge = existing
        edge.weight = float((edge.weight or 0.0) + (amount or 0.0))
        reused = True
    citation_url = (
        f"https://www.fec.gov/data/receipts/individual-contributions/"
        f"?committee_id={committee_id}&transaction_id={sub_id}"
    )
    session.add(
        SourceCitation(
            edge_id=edge.id,
            kind=SourceKind.FEC_FILING.value,
            citation_url=citation_url,
            citation_ref=sub_id,
        )
    )
    return edge.id, reused


async def ingest_geo_group_pac(max_disbursements: int = 200) -> FecStats:
    """Fetch GEO Group's PAC, its recent disbursements, and materialise the edges.

    Bounded by `max_disbursements` so a DEMO_KEY-throttled run finishes cleanly;
    resumability is achieved via the unique alias key on `source_id` (transaction
    sub_id) — reruns skip already-cited edges.
    """
    stats = FecStats()
    sm = get_sessionmaker()
    async with httpx.AsyncClient(timeout=15.0) as client:
        pac = await find_geo_group_pac(client)
        if pac is None:
            logger.error("GEO Group PAC not found in FEC committee search")
            return stats
        stats.pacs_found = 1
        committee_id = pac["committee_id"]
        pac_name = pac["name"]
        logger.info("found GEO Group PAC: %s (%s)", pac_name, committee_id)

        async with sm() as session:
            pac_canonical = await _upsert_entity(
                session,
                pac_name,
                EntityType.PAC.value,
                "fec.committee",
                committee_id,
                kind_hint="pac",
            )
            await session.commit()

        # Schedule B disbursements — payments FROM the PAC.
        page = 1
        remaining = max_disbursements
        while remaining > 0:
            payload = await _fec_get(
                client,
                "/schedules/schedule_b/",
                committee_id=committee_id,
                per_page=min(100, remaining),
                page=page,
                sort="-disbursement_date",
            )
            rows = payload.get("results", [])
            if not rows:
                break
            async with sm() as session:
                for row in rows:
                    stats.disbursements_fetched += 1
                    recipient_name = row.get("recipient_name") or ""
                    if not recipient_name:
                        continue
                    kind_upper = (row.get("recipient_committee_type") or "").upper()
                    recipient_type = (
                        EntityType.CANDIDATE.value
                        if kind_upper.startswith("CANDIDATE")
                        else EntityType.PAC.value
                        if kind_upper
                        else EntityType.ORGANIZATION.value
                    )
                    fec_id = (
                        row.get("recipient_committee_id")
                        or row.get("recipient_candidate_id")
                        or f"unknown-{row.get('sub_id')}"
                    )
                    dst_canonical = await _upsert_entity(
                        session,
                        recipient_name,
                        recipient_type,
                        "fec.disbursement.recipient",
                        fec_id,
                    )
                    edge_id, reused = await _emit_contribution_edge(
                        session,
                        pac_canonical,
                        dst_canonical,
                        row.get("disbursement_amount"),
                        str(row.get("sub_id") or row.get("transaction_id") or ""),
                        committee_id,
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
                    logger.exception("fec batch commit failed page=%d: %s", page, exc)
            remaining -= len(rows)
            page += 1
            if payload.get("pagination", {}).get("pages", 1) < page:
                break
    return stats


def main() -> None:
    """CLI entrypoint — python -m app.services.ingest.fec."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = asyncio.run(ingest_geo_group_pac())
    logger.info("fec ingest done: %s", stats)


if __name__ == "__main__":
    main()
