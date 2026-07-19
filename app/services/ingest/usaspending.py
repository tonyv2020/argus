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


# P1 (2026-07-19) — detention-industry recipient anchors.  Each entry
# names the canonical anchor and the recipient-name variants + the
# canonical hint (::canonical_name_normalized used by _find_canonical
# for the pre-existing hollywood-seeded canonical).  Extends the
# single GEO Group hardcode to CoreCivic + MTC + LaSalle, per §P1.
DETENTION_INDUSTRY_RECIPIENTS: dict[str, dict] = {
    "GEO Group": {
        "recipient_names": (
            "GEO GROUP INC", "THE GEO GROUP INC", "GEO GROUP, INC.",
        ),
        "canonical_hint": "GEO Group",
    },
    "CoreCivic": {
        "recipient_names": (
            "CORECIVIC INC",
            "CORECIVIC OF TENNESSEE LLC",
            "CORRECTIONS CORPORATION OF AMERICA",
            "CORECIVIC OF AMERICA LLC",
            "CORECIVIC OF ARIZONA LLC",
        ),
        "canonical_hint": "CoreCivic",
    },
    "Management & Training Corp": {
        "recipient_names": (
            "MANAGEMENT & TRAINING CORPORATION",
            "MANAGEMENT AND TRAINING CORPORATION",
            "MTC",
        ),
        "canonical_hint": "Management & Training Corporation",
    },
    "LaSalle Corrections": {
        "recipient_names": (
            "LASALLE CORRECTIONS LLC",
            "LASALLE SOUTHWEST CORRECTIONS",
            "LASALLE MANAGEMENT COMPANY",
        ),
        "canonical_hint": "LaSalle Corrections",
    },
    # Prison-telecom sub-industry (helen 2026-07-19 anchor extension).
    "Securus Technologies": {
        "recipient_names": (
            "SECURUS TECHNOLOGIES INC",
            "SECURUS TECHNOLOGIES LLC",
            "SECURUS TECHNOLOGIES",
        ),
        "canonical_hint": "Securus Technologies",
    },
    "Aventiv Technologies": {
        "recipient_names": (
            "AVENTIV TECHNOLOGIES LLC",
            "AVENTIV TECHNOLOGIES INC",
            "AVENTIV TECHNOLOGIES",
        ),
        "canonical_hint": "Aventiv Technologies",
    },
    "Satellite Tracking of People": {
        "recipient_names": (
            "SATELLITE TRACKING OF PEOPLE LLC",
            "SATELLITE TRACKING OF PEOPLE",
            "STOP LLC",
        ),
        "canonical_hint": "Satellite Tracking of People",
    },
    "GTL / ViaPath": {
        "recipient_names": (
            "GLOBAL TEL LINK CORPORATION",
            "GLOBAL TEL*LINK CORPORATION",
            "GLOBAL TEL LINK",
            "VIAPATH TECHNOLOGIES",
            "VIAPATH TECHNOLOGIES LLC",
        ),
        "canonical_hint": "GTL / ViaPath",
    },
}
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
    """Return the pre-existing GEO Group canonical id (from hollywood.entity_tags P0).

    Kept for back-compat; new callers use :func:`_find_recipient_canonical`.
    """
    return await _find_recipient_canonical(session, "GEO Group")


async def _find_recipient_canonical(
    session: AsyncSession, canonical_hint: str
) -> str | None:
    """Return the pre-existing canonical id for the given hint.

    Looks up an ORGANIZATION-type canonical whose normalized name matches
    ``normalize_name(canonical_hint)`` — the same lookup shape the
    hollywood.entity_tags seed produces.  Returns ``None`` when the seed
    hasn't landed yet (caller must decide to error or create).
    """
    row = (
        await session.execute(
            select(CanonicalEntity).where(
                CanonicalEntity.type == EntityType.ORGANIZATION.value,
                CanonicalEntity.canonical_name_normalized == normalize_name(canonical_hint),
            )
        )
    ).scalar_one_or_none()
    return row.id if row else None


async def ingest_geo_group_contracts(max_awards: int = 200) -> UsaSpendingStats:
    """Back-compat wrapper — ingest ONLY GEO Group awards.

    Prefer :func:`ingest_recipient_contracts` with a specific anchor or
    :func:`ingest_detention_industry_contracts` for the full set.
    """
    return await ingest_recipient_contracts(
        recipient_names=DETENTION_INDUSTRY_RECIPIENTS["GEO Group"]["recipient_names"],
        canonical_hint=DETENTION_INDUSTRY_RECIPIENTS["GEO Group"]["canonical_hint"],
        display_label="GEO Group",
        max_awards=max_awards,
    )


async def ingest_detention_industry_contracts(
    max_awards: int = 200,
) -> dict[str, UsaSpendingStats]:
    """P1: ingest the detention-industry federal contracts set."""
    out: dict[str, UsaSpendingStats] = {}
    for label, entry in DETENTION_INDUSTRY_RECIPIENTS.items():
        try:
            out[label] = await ingest_recipient_contracts(
                recipient_names=entry["recipient_names"],
                canonical_hint=entry["canonical_hint"],
                display_label=label,
                max_awards=max_awards,
            )
        except Exception:
            logger.exception("usaspending ingest failed for %s", label)
            s = UsaSpendingStats()
            s.errors = 1
            out[label] = s
    return out


async def ingest_from_registry(
    priority_domains: tuple[str, ...] | None = None,
    max_awards: int = 200,
    broaden_agency_scope: bool = False,
) -> dict[str, UsaSpendingStats]:
    """P4 registry-driven ingest — sweep every ``anchor_registry`` row
    the USAspending ingester is scoped to see (via
    ``anchors_for_usaspending``).

    The label is used as the ``canonical_hint`` so the ingester's
    existing auto-seed path (P1.4 hotfix) creates the org canonical
    for anchors that hollywood.entity_tags hasn't seeded (Securus /
    Aventiv / STOP / GTL / Palantir / Tesla / SpaceX etc.).
    """
    from app.services.anchor_registry import anchors_for_usaspending

    out: dict[str, UsaSpendingStats] = {}
    sm = get_sessionmaker()
    async with sm() as session:
        anchors = await anchors_for_usaspending(
            session, priority_domains=priority_domains
        )

    for anchor in anchors:
        try:
            out[anchor.label] = await ingest_recipient_contracts(
                recipient_names=tuple(anchor.usaspending_recipient_names),
                canonical_hint=anchor.label,
                display_label=anchor.label,
                max_awards=max_awards,
                broaden_agency_scope=broaden_agency_scope,
            )
        except Exception:
            logger.exception(
                "usaspending registry ingest failed for %s", anchor.label
            )
            s = UsaSpendingStats()
            s.errors = 1
            out[anchor.label] = s
    return out


async def ingest_recipient_contracts(
    recipient_names: tuple[str, ...],
    canonical_hint: str,
    display_label: str,
    max_awards: int = 200,
    broaden_agency_scope: bool = False,
) -> UsaSpendingStats:
    """Fetch ONE recipient's contracts + emit HOLDS_CONTRACT edges.

    Default scope: ICE / BOP / USMS (detention beat). When
    ``broaden_agency_scope=True``, accept EVERY awarding sub-agency —
    Tesla/SpaceX NASA + DoD contracts land through this path (helen
    2026-07-19 P4 validation note).
    """
    stats = UsaSpendingStats()
    sm = get_sessionmaker()
    async with sm() as session:
        geo_canonical = await _find_recipient_canonical(session, canonical_hint)
        if geo_canonical is None:
            # helen 2026-07-19: prison-telecom sub-industry is entirely absent
            # from hollywood.entity_tags (Securus/Aventiv/STOP = 0 entities).
            # Auto-seed an ORGANIZATION canonical from the anchor hint so the
            # USAspending edges can attach. Idempotent: the alias-keyed lookup
            # in _find_or_create_canonical returns the same id on rerun.
            geo_canonical = await _find_or_create_canonical(
                session,
                surface_name=canonical_hint,
                entity_type=EntityType.ORGANIZATION.value,
                source_system="usaspending.anchor",
                source_id=canonical_hint,
            )
            await session.commit()
            logger.info(
                "%s: seeded canonical %s from anchor hint %r",
                display_label,
                geo_canonical,
                canonical_hint,
            )

    body = {
        "filters": {
            "recipient_search_text": list(recipient_names),
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
                        if not broaden_agency_scope:
                            continue
                        # Broadened mode — accept any sub-agency; use the
                        # actual string as the anchor label so NASA/DoD
                        # etc. surface distinctly.
                        matched = sub_agency or top_agency or "UNKNOWN"
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


def main() -> None:  # noqa: C901  — CLI dispatcher, straight-line
    """CLI entrypoint — python -m app.services.ingest.usaspending [anchor|--all].

    Default runs the full detention-industry recipient set (§P1).  Pass
    an anchor label (``GEO Group`` / ``CoreCivic`` / ``Management &
    Training Corp`` / ``LaSalle Corrections``) to run only that one.
    """
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    arg = " ".join(sys.argv[1:]).strip() or "--all"
    if arg in ("--all", "all", ""):
        results = asyncio.run(ingest_detention_industry_contracts())
        for label, stats in results.items():
            logger.info("[%s] usaspending ingest done: %s", label, stats)
    elif arg in DETENTION_INDUSTRY_RECIPIENTS:
        entry = DETENTION_INDUSTRY_RECIPIENTS[arg]
        stats = asyncio.run(
            ingest_recipient_contracts(
                recipient_names=entry["recipient_names"],
                canonical_hint=entry["canonical_hint"],
                display_label=arg,
            )
        )
        logger.info("[%s] usaspending ingest done: %s", arg, stats)
    else:
        logger.error(
            "unknown anchor %r; choose from %s or --all",
            arg,
            sorted(DETENTION_INDUSTRY_RECIPIENTS),
        )
        sys.exit(2)
    return

def _legacy_main() -> None:
    """Kept for tests; the real entrypoint is :func:`main`."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = asyncio.run(ingest_geo_group_contracts())
    logger.info("usaspending ingest done: %s", stats)


if __name__ == "__main__":
    main()
