"""P3a — Senate LDA (Lobbying Disclosure Act) ingestion scoped to a client anchor.

Uses the free lda.senate.gov REST API (no auth needed). Fetches lobbying
filings where a given client (default: The GEO Group, Inc.) is the client
of record. For each filing, materialises:

  * a canonical for the **client** organization (upserted).
  * a canonical for the **registrant** (the lobbying firm hired by the client).
  * a ``LOBBIES`` edge FROM the client TO the registrant weighted by count
    of filings (the semantic: "client retains firm to lobby on their behalf"),
    every emission carrying a ``senate_lda`` :class:`SourceCitation` pointing
    at the filing's public URL.

We deliberately do NOT emit lobbyist-level PERSON canonicals here — those
land as private-individual candidates that the Scrutiny Agent classifies
as OPEN / ALIAS / SUPPRESS. Lobbyist ingest is a distinct follow-up (each
LDA lobbyist is a public-role individual, but we want the scrutiny bar to
approve them explicitly rather than surface real names inline).

Same design rhythm as :mod:`app.services.ingest.fec`:

  * per-row upsert with alias source key (``senate_lda.filing`` /
    ``senate_lda.registrant`` / ``senate_lda.client``) → idempotent reruns.
  * per-page commit boundary → a network hiccup mid-run keeps prior pages.
  * bounded by ``max_filings`` so a DEMO throttle finishes cleanly.

See argus design §5.7 (external-source ingest) and P3 task d95ada3a.
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

_LDA_BASE = "https://lda.senate.gov/api/v1"

# Default client anchor — the GEO Group, matching argus's other GEO-Group-scoped
# passes (:mod:`.fec`, :mod:`.usaspending`). Override via
# ``ingest_client_filings(client_name=...)`` to sweep additional detention-industry
# clients (CoreCivic, MTC, LaSalle, …) in a later broadening pass.
_DEFAULT_CLIENT_NAME = "The GEO Group"

# The LDA public filing URL — the click-through label the UI renders.
# Filings are addressable at /filings/public/filing/<uuid>/ ; the JSON payload
# also carries a ``filing_document_url`` which points at a PDF (if disclosed).
# We citation-URL the public-record page (stable) rather than the PDF (may 404).
_FILING_URL_TEMPLATE = "https://lda.senate.gov/filings/public/filing/{uuid}/"


@dataclass
class SenateLdaStats:
    """Counters for one LDA pass — surfaced back to callers + logs."""

    filings_fetched: int = 0
    filings_skipped_off_anchor: int = 0
    clients_upserted: int = 0
    registrants_upserted: int = 0
    edges_created: int = 0
    edges_reused: int = 0
    citations_created: int = 0
    errors: int = 0


async def _lda_get(client: httpx.AsyncClient, path: str, **params) -> dict:
    """One GET to lda.senate.gov; returns parsed JSON. Raises on non-2xx."""
    r = await client.get(f"{_LDA_BASE}{path}", params=params)
    r.raise_for_status()
    return r.json()


async def _upsert_entity(
    session: AsyncSession,
    *,
    surface_name: str,
    entity_type: str,
    source_system: str,
    source_id: str,
    kind_hint: str | None = None,
) -> str:
    """Return an existing canonical id via alias-source lookup or normalized-name;
    otherwise create the canonical + attach the LDA alias.

    Mirrors the FEC ingester's shape so canonicals resolved via FEC (PACs,
    committees) and via LDA (clients, registrants) collide on
    ``canonical_name_normalized`` when the underlying org is the same — an
    LDA filing about "THE GEO GROUP, INC." attaches to the same canonical
    that FEC's committee search + hollywood's news-tagged mentions produced.
    """
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
                    kind_hint=kind_hint,
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
            kind_hint=kind_hint,
        )
    )
    return ce.id


async def _emit_lobbies_edge(
    session: AsyncSession,
    *,
    client_canonical: str,
    registrant_canonical: str,
    filing_uuid: str,
) -> tuple[str, bool]:
    """Emit a LOBBIES edge (client → registrant) + attach the filing citation.

    Edge weight = count of citations. Reruns MERGE the same edge (idempotent
    on ``(source_id, target_id, relation)``) and add a citation only when the
    same filing UUID hasn't been cited yet — so a reprocess doesn't double-count.
    """
    existing = (
        await session.execute(
            select(CanonicalEdge).where(
                CanonicalEdge.source_id == client_canonical,
                CanonicalEdge.target_id == registrant_canonical,
                CanonicalEdge.relation == EdgeRelation.LOBBIES.value,
            )
        )
    ).scalar_one_or_none()
    reused = False
    if existing is None:
        edge = CanonicalEdge(
            source_id=client_canonical,
            target_id=registrant_canonical,
            relation=EdgeRelation.LOBBIES.value,
            weight=1.0,
        )
        session.add(edge)
        await session.flush()
    else:
        edge = existing
        reused = True

    already_cited = (
        await session.execute(
            select(SourceCitation).where(
                SourceCitation.edge_id == edge.id,
                SourceCitation.citation_ref == filing_uuid,
            )
        )
    ).scalar_one_or_none()
    if already_cited is None:
        session.add(
            SourceCitation(
                edge_id=edge.id,
                kind=SourceKind.SENATE_LDA.value,
                citation_url=_FILING_URL_TEMPLATE.format(uuid=filing_uuid),
                citation_ref=filing_uuid,
            )
        )
        if reused:
            edge.weight = float((edge.weight or 0.0) + 1.0)
        return edge.id, reused
    return edge.id, True


def _client_name_matches(row: dict, anchor: str) -> bool:
    """Substring-match on the LDA row's client name against the anchor.

    LDA's ``client_name`` query param is a fuzzy contains-match at the server
    side — a query of "The GEO Group" returns "GEOTHERMAL TAX GROUP" too.
    We filter client-side against the row's ``client.name`` to guarantee the
    filings we materialize actually belong to the anchor client.
    """
    client_name = ((row.get("client") or {}).get("name") or "").upper()
    return anchor.strip().upper() in client_name


async def ingest_client_filings(
    *,
    client_name: str = _DEFAULT_CLIENT_NAME,
    max_filings: int = 200,
    page_size: int = 25,
) -> SenateLdaStats:
    """Fetch a client's LDA filings, materialise LOBBIES edges to each registrant.

    Bounded by ``max_filings`` — reruns are idempotent (alias source key on
    ``senate_lda.filing:<uuid>``), so pagination can be resumed by lifting
    the cap on the next call.

    ``page_size`` bounds the LDA server-side page (max 25 without auth per
    docs). Empty result sets stop the loop cleanly.
    """
    stats = SenateLdaStats()
    sm = get_sessionmaker()
    async with httpx.AsyncClient(timeout=20.0) as client:
        page = 1
        remaining = max_filings
        while remaining > 0:
            payload = await _lda_get(
                client,
                "/filings/",
                client_name=client_name,
                page=page,
                page_size=min(page_size, remaining),
                ordering="-dt_posted",
            )
            rows = payload.get("results", [])
            if not rows:
                break

            async with sm() as session:
                for row in rows:
                    stats.filings_fetched += 1
                    if not _client_name_matches(row, client_name):
                        stats.filings_skipped_off_anchor += 1
                        continue

                    filing_uuid = row.get("filing_uuid")
                    client_row = row.get("client") or {}
                    registrant_row = row.get("registrant") or {}
                    if not (filing_uuid and client_row and registrant_row):
                        continue

                    client_lda_id = str(client_row.get("id") or client_row.get("client_id") or "")
                    registrant_lda_id = str(
                        registrant_row.get("id")
                        or registrant_row.get("house_registrant_id")
                        or ""
                    )
                    if not (client_lda_id and registrant_lda_id):
                        continue

                    client_canonical = await _upsert_entity(
                        session,
                        surface_name=(client_row.get("name") or "").strip(),
                        entity_type=EntityType.ORGANIZATION.value,
                        source_system="senate_lda.client",
                        source_id=client_lda_id,
                    )
                    stats.clients_upserted += 1

                    # LDA's registrant.description is prose ("Law and Public Policy
                    # Firm", "Public relations, lobbying and coalitions building.")
                    # which does not fit the short-tag semantic of `kind_hint`
                    # (varchar(32); values like "pac", "candidate", "committee").
                    # Drop the description here; the alias `source_system` +
                    # `source_id` are enough to trace back to the LDA registrant
                    # record, and the surface name carries what the reader needs.
                    registrant_canonical = await _upsert_entity(
                        session,
                        surface_name=(registrant_row.get("name") or "").strip(),
                        entity_type=EntityType.ORGANIZATION.value,
                        source_system="senate_lda.registrant",
                        source_id=registrant_lda_id,
                        kind_hint=None,
                    )
                    stats.registrants_upserted += 1

                    _, reused = await _emit_lobbies_edge(
                        session,
                        client_canonical=client_canonical,
                        registrant_canonical=registrant_canonical,
                        filing_uuid=str(filing_uuid),
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
                    logger.exception("senate_lda batch commit failed page=%d: %s", page, exc)

            remaining -= len(rows)
            page += 1
            pagination_total_pages = (payload.get("pagination") or {}).get("pages")
            if pagination_total_pages is not None and page > pagination_total_pages:
                break
    return stats


def main() -> None:
    """CLI entrypoint — ``python -m app.services.ingest.senate_lda``."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = asyncio.run(ingest_client_filings())
    logger.info("senate_lda ingest done: %s", stats)


if __name__ == "__main__":
    main()
