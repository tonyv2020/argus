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
import re
from dataclasses import dataclass, field

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
    PublicationState,
    SourceCitation,
    SourceKind,
)
from app.services.graph.base import normalize_name
from app.services.ingest.domain_anchors import attach_alias

logger = logging.getLogger(__name__)

# Base URL migration (Tony/helen 2026-08-12 carceral Track A phase 2):
# lda.senate.gov returned 301 Moved Permanently → lda.gov on every
# request during the carceral sweep (17 errors), blocking new LDA
# ingestion for the fresh carceral anchors. The redirect target is
# the new https://lda.gov host — swap the base URL rather than
# follow-redirects since redirect chains double the request count +
# lose query params on some POSTs. Existing lobbies edges (Securus 9
# / Aventiv 3 / ViaPath 2) were pre-migration and remain queryable
# unchanged.
_LDA_BASE = "https://lda.gov/api/v1"

# Default client anchor — the GEO Group, matching argus's other GEO-Group-scoped
# passes (:mod:`.fec`, :mod:`.usaspending`). Override via
# ``ingest_client_filings(client_name=...)`` to sweep additional detention-industry
# clients (CoreCivic, MTC, LaSalle, …) in a later broadening pass.
_DEFAULT_CLIENT_NAME = "The GEO Group"

# The LDA public filing URL — the click-through label the UI renders.
# Filings are addressable at /filings/public/filing/<uuid>/ ; the JSON payload
# also carries a ``filing_document_url`` which points at a PDF (if disclosed).
# We citation-URL the public-record page (stable) rather than the PDF (may 404).
_FILING_URL_TEMPLATE = "https://lda.gov/filings/public/filing/{uuid}/"


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
    """One GET to the LDA public API (``_LDA_BASE``); returns parsed
    JSON. Raises on non-2xx.

    Handles 429 with exponential backoff (2s, 4s, 8s, up to 5 attempts)
    honouring a Retry-After header when present. The LDA public API
    throttles aggressively on a multi-anchor sweep — see
    ``helen-k3s/docs/argus-coverage-expansion-design.md`` rate-limits.
    """
    import asyncio

    delay = 2.0
    for attempt in range(5):
        r = await client.get(f"{_LDA_BASE}{path}", params=params)
        if r.status_code == 429:
            retry_after = r.headers.get("Retry-After")
            wait = float(retry_after) if retry_after and retry_after.isdigit() else delay
            await asyncio.sleep(wait)
            delay = min(delay * 2, 60.0)
            continue
        r.raise_for_status()
        return r.json()
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
    batch_id: str | None = None,
) -> str:
    """Return an existing canonical id via alias-source lookup or normalized-name;
    otherwise create the canonical + attach the LDA alias.

    ``batch_id`` (RG1, P1.6) stamps a NET-NEW canonical
    ``publication_state=staged``; a canonical this resolves onto keeps
    whatever state it already has. Default ``None`` = the published
    column default, so the steady-state sweeps are unchanged.

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
        publication_state=(
            PublicationState.STAGED.value if batch_id
            else PublicationState.PUBLISHED.value
        ),
        batch_id=batch_id,
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
    batch_id: str | None = None,
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
            publication_state=(
                PublicationState.STAGED.value if batch_id
                else PublicationState.PUBLISHED.value
            ),
            batch_id=batch_id,
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
            try:
                payload = await _lda_get(
                    client,
                    "/filings/",
                    client_name=client_name,
                    page=page,
                    page_size=min(page_size, remaining),
                    ordering="-dt_posted",
                )
            except httpx.HTTPStatusError as exc:
                # LDA's DRF-backed API returns 404 when paginating past the
                # last available page (rather than an empty results array).
                # Treat that as clean end-of-stream, not a real error.
                if exc.response.status_code == 404:
                    logger.info(
                        "senate_lda: end-of-pages hit for client_name=%s at page=%d",
                        client_name,
                        page,
                    )
                    break
                raise
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


# ─── P1.6 — pattern-gated, batch-aware client filings ───────────────────


@dataclass
class LdaDomainStats:
    """Counters for one P1.6 lobbying sweep."""

    anchors_processed: int = 0
    filings_fetched: int = 0
    filings_accepted: int = 0
    filings_refused_off_anchor: int = 0
    filings_incomplete: int = 0
    registrants_created: int = 0
    edges_created: int = 0
    edges_reused: int = 0
    citations_created: int = 0
    entities_staged: int = 0
    edges_staged: int = 0
    #: Client records ACCEPTED, keyed by LDA client id — recorded back
    #: into ``anchor_registry.external_ids.lda_client_ids``.
    accepted_clients: dict[str, dict] = field(default_factory=dict)
    #: Client names the patterns REFUSED, with counts. This is where
    #: "FLOCK HOMES, INC." and "AXONIUS" show up.
    refused_client_names: dict[str, int] = field(default_factory=dict)
    #: Anchors with LDA patterns but no resolved canonical.
    unanchored: list[dict] = field(default_factory=list)
    alias_conflicts: list[dict] = field(default_factory=list)
    by_registrant: list[dict] = field(default_factory=list)
    errors: int = 0


def client_name_accepted(name: str, patterns: tuple[str, ...]) -> bool:
    """Does an LDA client name belong to the anchor these patterns describe?

    Matched against the NORMALIZED name (punctuation stripped, folded to
    lowercase) so "PALANTIR TECHNOLOGIES, INC." and "Palantir
    Technologies Inc" are one string. Fail-closed: no pattern, no match.

    This replaces the substring test the pre-P1.6 pass used
    (:func:`_client_name_matches`), which accepts any name CONTAINING
    the anchor — "FLOCK HOMES, INC." for "Flock", "AXONIUS" for "Axon".
    """
    if not patterns:
        return False
    norm = normalize_name(name or "")
    if not norm:
        return False
    return any(re.search(p, norm) for p in patterns)


async def ingest_client_filings_by_pattern(
    *,
    client_canonical: str,
    queries: tuple[str, ...],
    patterns: tuple[str, ...],
    display_label: str,
    batch_id: str | None = None,
    max_filings: int = 600,
    page_size: int = 25,
    stats: LdaDomainStats | None = None,
) -> LdaDomainStats:
    """Fetch one anchor's LDA filings and emit cited ``lobbies`` edges.

    The client side of every edge is the ANCHOR's canonical — never a
    name-resolved node — so all 32 of Palantir's LDA client records land
    on one Palantir. Each accepted record's ``senate_lda.client`` id is
    attached to that canonical as an alias, which is what stops the
    older name-keyed pass from later minting a parallel node for it.
    """
    stats = stats or LdaDomainStats()
    sm = get_sessionmaker()
    per_registrant: dict[str, dict] = {}

    async with httpx.AsyncClient(timeout=30.0) as client:
        for query in queries:
            page = 1
            remaining = max_filings
            while remaining > 0:
                try:
                    payload = await _lda_get(
                        client,
                        "/filings/",
                        client_name=query,
                        page=page,
                        page_size=min(page_size, remaining),
                        ordering="-dt_posted",
                    )
                except httpx.HTTPStatusError as exc:
                    # LDA's DRF pagination 404s past the last page.
                    if exc.response.status_code == 404:
                        break
                    logger.exception(
                        "[%s] LDA fetch failed q=%r page=%d",
                        display_label, query, page,
                    )
                    stats.errors += 1
                    break
                rows = payload.get("results") or []
                if not rows:
                    break

                async with sm() as session:
                    for row in rows:
                        stats.filings_fetched += 1
                        client_row = row.get("client") or {}
                        registrant_row = row.get("registrant") or {}
                        filing_uuid = row.get("filing_uuid")
                        client_name = (client_row.get("name") or "").strip()

                        if not client_name_accepted(client_name, patterns):
                            stats.filings_refused_off_anchor += 1
                            stats.refused_client_names[client_name] = (
                                stats.refused_client_names.get(client_name, 0) + 1
                            )
                            continue
                        client_lda_id = str(
                            client_row.get("id") or client_row.get("client_id") or ""
                        )
                        registrant_lda_id = str(
                            registrant_row.get("id")
                            or registrant_row.get("house_registrant_id")
                            or ""
                        )
                        registrant_name = (registrant_row.get("name") or "").strip()
                        if not (
                            filing_uuid and client_lda_id
                            and registrant_lda_id and registrant_name
                        ):
                            stats.filings_incomplete += 1
                            continue

                        try:
                            await attach_alias(
                                session,
                                client_canonical,
                                "senate_lda.client",
                                client_lda_id,
                                client_name,
                                stats=stats,
                                label=display_label,
                            )
                            stats.accepted_clients[client_lda_id] = {
                                "client_id": int(client_lda_id),
                                "name": client_name,
                                "state": client_row.get("state"),
                            }
                            before = (
                                await session.execute(
                                    select(EntityAlias).where(
                                        EntityAlias.source_system
                                        == "senate_lda.registrant",
                                        EntityAlias.source_id == registrant_lda_id,
                                    )
                                )
                            ).scalar_one_or_none()
                            registrant_canonical = await _upsert_entity(
                                session,
                                surface_name=registrant_name,
                                entity_type=EntityType.ORGANIZATION.value,
                                source_system="senate_lda.registrant",
                                source_id=registrant_lda_id,
                                kind_hint=None,
                                batch_id=batch_id,
                            )
                            if before is None:
                                stats.registrants_created += 1
                                if batch_id:
                                    stats.entities_staged += 1
                            _, reused = await _emit_lobbies_edge(
                                session,
                                client_canonical=client_canonical,
                                registrant_canonical=registrant_canonical,
                                filing_uuid=str(filing_uuid),
                                batch_id=batch_id,
                            )
                            if reused:
                                stats.edges_reused += 1
                            else:
                                stats.edges_created += 1
                                if batch_id:
                                    stats.edges_staged += 1
                            stats.citations_created += 1
                            stats.filings_accepted += 1
                            bucket = per_registrant.setdefault(
                                registrant_lda_id,
                                {
                                    "registrant_id": registrant_lda_id,
                                    "registrant": registrant_name,
                                    "filings": 0,
                                },
                            )
                            bucket["filings"] += 1
                        except Exception:
                            logger.exception(
                                "[%s] LDA row failed filing=%s",
                                display_label, filing_uuid,
                            )
                            stats.errors += 1
                    try:
                        await session.commit()
                    except Exception:
                        await session.rollback()
                        stats.errors += 1
                        logger.exception(
                            "[%s] LDA batch commit failed page=%d",
                            display_label, page,
                        )
                remaining -= len(rows)
                page += 1
                if not payload.get("next"):
                    break

    stats.by_registrant.extend(
        sorted(per_registrant.values(), key=lambda r: -r["filings"])
    )
    return stats


async def ingest_domain_lobbying(
    priority_domains: tuple[str, ...] | None = None,
    *,
    batch_id: str | None = None,
    max_filings_per_anchor: int = 600,
) -> LdaDomainStats:
    """Sweep every registry anchor that declares LDA client patterns.

    Records each accepted ``senate_lda.client`` id back into the
    anchor's ``external_ids.lda_client_ids`` — the keyring accumulates
    the ids the patterns resolved to, so the report can show exactly
    which LDA registrations were treated as this company.
    """
    from app.models import AnchorRegistry
    from app.services.anchor_registry import anchors_for_lda_patterns

    stats = LdaDomainStats()
    sm = get_sessionmaker()
    async with sm() as session:
        anchors = await anchors_for_lda_patterns(
            session, priority_domains=priority_domains
        )
    for anchor in anchors:
        if not anchor.canonical_id:
            stats.unanchored.append({"anchor": anchor.label})
            logger.error(
                "LDA domain pass: %s has client patterns but no canonical — "
                "run domain_anchors first", anchor.label,
            )
            continue
        queries = tuple(anchor.lda_client_names) or (anchor.label,)
        before_accepted = set(stats.accepted_clients)
        await ingest_client_filings_by_pattern(
            client_canonical=anchor.canonical_id,
            queries=queries,
            patterns=tuple(anchor.lda_client_patterns),
            display_label=anchor.label,
            batch_id=batch_id,
            max_filings=max_filings_per_anchor,
            stats=stats,
        )
        stats.anchors_processed += 1
        # Record the ids these patterns resolved to, back onto the anchor.
        new_ids = sorted(
            {
                stats.accepted_clients[k]["client_id"]
                for k in set(stats.accepted_clients) - before_accepted
            }
            | set(anchor.lda_client_ids)
        )
        if new_ids != sorted(anchor.lda_client_ids):
            async with sm() as session:
                row = (
                    await session.execute(
                        select(AnchorRegistry).where(
                            AnchorRegistry.label == anchor.label,
                            AnchorRegistry.entity_type == anchor.entity_type,
                        )
                    )
                ).scalar_one_or_none()
                if row is not None:
                    keyring = dict(row.external_ids or {})
                    keyring["lda_client_ids"] = new_ids
                    row.external_ids = keyring
                    await session.commit()
    return stats


DETENTION_INDUSTRY_LDA_CLIENTS: tuple[str, ...] = (
    # P3c broadening pass (helen 2026-07-18) — detention-industry primes
    # beyond the GEO Group anchor. Each name is an LDA `client_name`
    # substring match; ``_client_name_matches`` filters LDA's fuzzy-match
    # false positives at ingest time.
    #
    # Included (verified against LDA on 2026-07-18):
    #   * The GEO Group (450 filings)
    #   * CoreCivic (452 filings)
    #   * Corrections Corporation of America (103 filings) — CoreCivic's
    #     pre-2016-rename name; the same underlying canonical when the SEC
    #     ingester's former-name aliasing catches it.
    #   * Management and Training Corporation (64 filings) — MTC, the
    #     third-largest private detention operator.
    #
    # Excluded from this pass (probed 0 filings under the obvious name variants;
    # revisit with the LDA registrant-search endpoint if we want lobbyists FOR
    # them rather than filings BY them):
    #   * LaSalle Corrections
    "The GEO Group",
    "CoreCivic",
    "Corrections Corporation of America",
    "Management and Training Corporation",
    # Prison-telecom sub-industry (helen 2026-07-19 anchor extension).
    # These are privately held (PE-owned) so no SEC — the LDA + FEC +
    # USAspending triangle is our accountability surface for them.
    "Securus Technologies",
    "Aventiv Technologies",
    "Global Tel Link",
    "ViaPath Technologies",
)


async def ingest_detention_industry(
    max_filings_per_client: int = 200, page_size: int = 25
) -> SenateLdaStats:
    """P3c — sweep every anchor in :data:`DETENTION_INDUSTRY_LDA_CLIENTS`.

    Runs :func:`ingest_client_filings` per anchor and folds the counters
    into a single :class:`SenateLdaStats`. Idempotent: reruns cite new
    filings only + never double-count.
    """
    agg = SenateLdaStats()
    for name in DETENTION_INDUSTRY_LDA_CLIENTS:
        logger.info("senate_lda ingest anchor client_name=%s", name)
        stats = await ingest_client_filings(
            client_name=name,
            max_filings=max_filings_per_client,
            page_size=page_size,
        )
        agg.filings_fetched += stats.filings_fetched
        agg.filings_skipped_off_anchor += stats.filings_skipped_off_anchor
        agg.clients_upserted += stats.clients_upserted
        agg.registrants_upserted += stats.registrants_upserted
        agg.edges_created += stats.edges_created
        agg.edges_reused += stats.edges_reused
        agg.citations_created += stats.citations_created
        agg.errors += stats.errors
    return agg


async def ingest_from_registry(
    priority_domains: tuple[str, ...] | None = None,
    max_filings_per_client: int = 200,
    page_size: int = 25,
) -> SenateLdaStats:
    """P4 registry-driven ingest — sweep every ``anchor_registry`` row
    the LDA ingester is scoped to see (via ``anchors_for_senate_lda``).

    Each anchor's ``lda_client_names`` list may hold multiple LDA
    surface names (e.g. GEO Group's "The GEO Group" + historical
    variants; CoreCivic's "CoreCivic" + pre-rename "Corrections
    Corporation of America"). Each is swept independently.
    """
    from app.db import get_sessionmaker
    from app.services.anchor_registry import anchors_for_senate_lda

    agg = SenateLdaStats()
    sm = get_sessionmaker()
    async with sm() as session:
        anchors = await anchors_for_senate_lda(
            session, priority_domains=priority_domains
        )

    for anchor in anchors:
        for client_name in anchor.lda_client_names:
            logger.info(
                "senate_lda registry anchor label=%s client_name=%s",
                anchor.label, client_name,
            )
            try:
                stats = await ingest_client_filings(
                    client_name=client_name,
                    max_filings=max_filings_per_client,
                    page_size=page_size,
                )
                agg.filings_fetched += stats.filings_fetched
                agg.filings_skipped_off_anchor += stats.filings_skipped_off_anchor
                agg.clients_upserted += stats.clients_upserted
                agg.registrants_upserted += stats.registrants_upserted
                agg.edges_created += stats.edges_created
                agg.edges_reused += stats.edges_reused
                agg.citations_created += stats.citations_created
                agg.errors += stats.errors
            except Exception:
                logger.exception(
                    "senate_lda registry ingest failed for %s (%s)",
                    anchor.label, client_name,
                )
                agg.errors += 1
    return agg


def main() -> None:
    """CLI entrypoint — ``python -m app.services.ingest.senate_lda``.

    Sweeps every client in :data:`DETENTION_INDUSTRY_LDA_CLIENTS`. Use
    :func:`ingest_client_filings` directly for a single-anchor call.
    """
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = asyncio.run(ingest_detention_industry())
    logger.info("senate_lda ingest done: %s", stats)


if __name__ == "__main__":
    main()
