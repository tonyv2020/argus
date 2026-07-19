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

# GEO Group's known FEC name variants — kept for back-compat with any
# callers still using the specialized GEO entrypoint.
_GEO_PAC_NAME_QUERIES = ("GEO GROUP INC PAC", "GEO GROUP", "GEO GROUP INC")


# P1 (2026-07-19) — detention-industry anchor set.  Extended from the
# single GEO Group anchor to CoreCivic + MTC + LaSalle so their PAC
# contributions land in the graph too.  Each entry is a tuple of
# committee-name-search queries.  Every query hits the FEC committee
# search independently; the first result whose name contains ANY of
# the ``match`` tokens is treated as the PAC for that anchor.  Keeps
# the same lookup shape ``find_geo_group_pac`` used but generalizes
# it, per design §2.P1.
DETENTION_INDUSTRY_PACS: dict[str, dict] = {
    "GEO Group": {
        "queries": ("GEO GROUP INC PAC", "GEO GROUP", "GEO GROUP INC"),
        "match": ("GEO GROUP",),
    },
    "CoreCivic": {
        # Historical CCA PAC name + current CoreCivic PAC name.
        "queries": ("CORECIVIC INC PAC", "CORECIVIC PAC", "CCA PAC",
                    "CORRECTIONS CORPORATION OF AMERICA"),
        "match": ("CORECIVIC", "CCA PAC", "CORRECTIONS CORPORATION"),
    },
    "Management & Training Corp": {
        "queries": ("MANAGEMENT AND TRAINING CORP", "MTC PAC",
                    "MANAGEMENT & TRAINING CORPORATION"),
        "match": ("MANAGEMENT AND TRAINING", "MANAGEMENT & TRAINING", "MTC"),
    },
    "LaSalle Corrections": {
        "queries": ("LASALLE CORRECTIONS", "LASALLE MANAGEMENT",
                    "LASALLE SOUTHWEST CORRECTIONS"),
        "match": ("LASALLE",),
    },
    # Prison-telecom sub-industry — SEC-skipped (privately held, PE-owned)
    # but FEC + LDA + USAspending coverage matters. Helen 2026-07-19: Securus
    # + Aventiv + STOP + GTL/ViaPath are absent (0 entities) or fragmented.
    "Securus Technologies": {
        "queries": ("SECURUS TECHNOLOGIES", "SECURUS TECH", "SECURUS PAC"),
        "match": ("SECURUS",),
    },
    "Aventiv Technologies": {
        "queries": ("AVENTIV TECHNOLOGIES", "AVENTIV TECH", "AVENTIV PAC"),
        "match": ("AVENTIV",),
    },
    "Satellite Tracking of People": {
        "queries": ("SATELLITE TRACKING OF PEOPLE", "STOP LLC", "STOP PAC"),
        "match": ("SATELLITE TRACKING", "STOP LLC"),
    },
    "GTL / ViaPath": {
        # GTL renamed to ViaPath Technologies in 2022; cover both surface names.
        "queries": ("GLOBAL TEL LINK", "GLOBAL TEL*LINK", "GTL PAC",
                    "VIAPATH TECHNOLOGIES", "VIAPATH"),
        "match": ("GLOBAL TEL", "VIAPATH", "GTL"),
    },
}


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

    Kept for back-compat.  New callers use :func:`find_pac_by_queries` with
    the appropriate entry from :data:`DETENTION_INDUSTRY_PACS`.
    """
    entry = DETENTION_INDUSTRY_PACS["GEO Group"]
    return await find_pac_by_queries(client, entry["queries"], entry["match"])


async def find_pac_by_queries(
    client: httpx.AsyncClient,
    queries: tuple[str, ...],
    match_tokens: tuple[str, ...],
) -> dict | None:
    """Generic FEC-committee finder used by the P1 detention-industry set.

    ``queries`` is the list of committee-search strings to try in order
    (FEC's ``q`` accepts a name substring).  ``match_tokens`` are the
    uppercase substrings we require in the returned committee name to
    accept a row — guards against unrelated committees whose names
    happen to appear in the search results.
    """
    for q in queries:
        payload = await _fec_get(client, "/committees/", q=q, per_page=5)
        for row in payload.get("results", []):
            name_upper = (row.get("name") or "").upper()
            if any(tok in name_upper for tok in match_tokens):
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
    """Back-compat wrapper — ingest ONLY GEO Group PAC.

    Prefer :func:`ingest_pac` with a specific anchor or
    :func:`ingest_detention_industry_pacs` for the full set.
    """
    return await ingest_pac(
        queries=DETENTION_INDUSTRY_PACS["GEO Group"]["queries"],
        match_tokens=DETENTION_INDUSTRY_PACS["GEO Group"]["match"],
        display_label="GEO Group",
        max_disbursements=max_disbursements,
    )


async def ingest_detention_industry_pacs(
    max_disbursements: int = 200,
) -> dict[str, FecStats]:
    """P1: ingest the whole detention-industry PAC set.

    Returns a per-anchor mapping of :class:`FecStats` so callers see
    which anchor lit up which counters.  Each anchor's failure is
    isolated — one missing committee doesn't sink the batch.
    """
    out: dict[str, FecStats] = {}
    for label, entry in DETENTION_INDUSTRY_PACS.items():
        try:
            out[label] = await ingest_pac(
                queries=entry["queries"],
                match_tokens=entry["match"],
                display_label=label,
                max_disbursements=max_disbursements,
            )
        except Exception:
            logger.exception("ingest failed for anchor %s", label)
            s = FecStats()
            s.errors = 1
            out[label] = s
    return out


async def ingest_from_registry(
    priority_domains: tuple[str, ...] | None = None,
    max_disbursements: int = 200,
) -> dict[str, FecStats]:
    """P4 registry-driven ingest — sweep every ``anchor_registry`` row
    the FEC ingester is scoped to see (via ``anchors_for_fec``).

    For this PR (B) we drive off ``name_variants`` — same shape the
    pre-P4 ``DETENTION_INDUSTRY_PACS`` constant used. External-ID-by-
    committee-lookup (``fec_committee_ids`` → skip fuzzy search, go
    straight to disbursements) is a follow-on refinement that needs a
    deeper refactor of :func:`ingest_pac`'s inline fetch loop and lands
    in a subsequent PR.
    """
    from app.db import get_sessionmaker
    from app.services.anchor_registry import anchors_for_fec

    out: dict[str, FecStats] = {}
    sm = get_sessionmaker()
    async with sm() as session:
        anchors = await anchors_for_fec(session, priority_domains=priority_domains)

    for anchor in anchors:
        variants = tuple(anchor.name_variants) or tuple(
            anchor.fec_committee_ids
        )
        if not variants:
            logger.warning(
                "anchor %s has no name_variants — skipping FEC pass",
                anchor.label,
            )
            continue
        try:
            out[anchor.label] = await ingest_pac(
                queries=variants,
                match_tokens=variants,
                display_label=anchor.label,
                max_disbursements=max_disbursements,
            )
        except Exception:
            logger.exception("registry ingest failed for %s", anchor.label)
            s = FecStats()
            s.errors = 1
            out[anchor.label] = s
    return out


async def ingest_pac(
    queries: tuple[str, ...],
    match_tokens: tuple[str, ...],
    display_label: str,
    max_disbursements: int = 200,
) -> FecStats:
    """Fetch ONE PAC (via ``queries``/``match_tokens``), its recent
    disbursements, and materialise the edges — parameterized version of
    the legacy GEO-only entrypoint.

    Bounded by `max_disbursements` so a DEMO_KEY-throttled run finishes
    cleanly; resumability is via the unique alias key on `source_id`
    (transaction sub_id) — reruns skip already-cited edges.
    """
    stats = FecStats()
    sm = get_sessionmaker()
    async with httpx.AsyncClient(timeout=15.0) as client:
        pac = await find_pac_by_queries(client, queries, match_tokens)
        if pac is None:
            logger.error("%s PAC not found in FEC committee search", display_label)
            return stats
        stats.pacs_found = 1
        committee_id = pac["committee_id"]
        pac_name = pac["name"]
        logger.info("found %s PAC: %s (%s)", display_label, pac_name, committee_id)

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
    """CLI entrypoint — python -m app.services.ingest.fec [anchor|--all].

    Default runs the full detention-industry anchor set (§P1).  Pass
    an anchor label (``GEO Group`` / ``CoreCivic`` / ``Management &
    Training Corp`` / ``LaSalle Corrections``) to run only that one.
    """
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    arg = " ".join(sys.argv[1:]).strip() or "--all"
    if arg in ("--all", "all", ""):
        results = asyncio.run(ingest_detention_industry_pacs())
        for label, stats in results.items():
            logger.info("[%s] fec ingest done: %s", label, stats)
    elif arg in DETENTION_INDUSTRY_PACS:
        entry = DETENTION_INDUSTRY_PACS[arg]
        stats = asyncio.run(
            ingest_pac(
                queries=entry["queries"],
                match_tokens=entry["match"],
                display_label=arg,
            )
        )
        logger.info("[%s] fec ingest done: %s", arg, stats)
    else:
        logger.error(
            "unknown anchor %r; choose from %s or --all",
            arg,
            sorted(DETENTION_INDUSTRY_PACS),
        )
        sys.exit(2)


if __name__ == "__main__":
    main()
