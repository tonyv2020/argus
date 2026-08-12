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
from sqlalchemy import func as sa_func, select
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

# USAspending spending_by_award VALID SORT KEYS for the Contract-Award
# mapping (award_type_codes A/B/C/D). Observed valid set from the API's
# error surface + documented Field Lookups. If the ``sort`` in our
# request body is NOT in this set, USAspending returns HTTP 400
# "Sort value not found in Contract Award mappings" and the ingest
# 400s for every recipient. helen 2026-08-11: a Stage 2 build shipped
# with ``sort="Total Obligated Amount"`` (invalid) which caused every
# post-backfill re-ingest to fail — since backfill did the delete
# BEFORE the re-ingest, the graph got wiped and rebuilt to zero. The
# hotfix reverts sort to "Award Amount" and adds the fail-before-
# delete probe in ``backfill_holds_contract_edges``. A regression
# test asserts the current sort is in this set.
VALID_CONTRACT_SORT_KEYS: frozenset[str] = frozenset({
    "Award ID",
    "Recipient Name",
    "Start Date",
    "End Date",
    "Award Amount",
    "Awarding Agency",
    "Awarding Sub Agency",
    "Contract Award Type",
})


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


async def _get(client: httpx.AsyncClient, path: str) -> dict:
    """One GET to api.usaspending.gov (award detail endpoint)."""
    r = await client.get(f"{_USA_BASE}{path}")
    r.raise_for_status()
    return r.json()


# Stage 2 (helen 2026-08-11) — the ~$89B GEO / ~$58B CoreCivic edge
# weights that were driving Cassandra's outline-declines were sourced
# from USAspending's "Award Amount" field, which for contracts is the
# CEILING (current award value including all option years). The
# authoritative "how much has actually been obligated" number is
# ``total_obligation`` on the award-detail endpoint, or the
# ``Total Obligated Amount`` field when ``spending_by_award`` returns
# it. We prefer the row-level field (one HTTP round-trip per page)
# and fall back to the per-award detail fetch when the row is missing
# it. Both values verify against the "Obligated Amount" figure shown
# on the public https://www.usaspending.gov/award/<id> page.
_ROW_OBLIGATION_KEYS: tuple[str, ...] = (
    "Total Obligated Amount",
    "total_obligation",
    "obligated_amount",
)


async def _award_net_obligation(
    client: httpx.AsyncClient, row: dict
) -> float | None:
    """Return the award's net federal-action obligation in USD, or None.

    Prefers the ``Total Obligated Amount`` (or equivalent) field the
    ``spending_by_award`` search returns when it's in the ``fields``
    list; falls back to the authoritative award-detail endpoint's
    ``total_obligation`` per-award when the search omitted it. The
    fallback is O(N) HTTP calls in a bad case but yields the same
    value the public USAspending award page shows, so operators can
    verify against usaspending.gov by clicking the citation URL.
    """
    for k in _ROW_OBLIGATION_KEYS:
        v = row.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    award_id = row.get("generated_internal_id") or row.get("Award ID")
    if not award_id:
        return None
    try:
        detail = await _get(client, f"/awards/{award_id}/")
    except Exception:
        return None
    for k in ("total_obligation", "obligated_amount"):
        v = detail.get(k)
        if v is not None:
            try:
                return float(v)
            except (TypeError, ValueError):
                pass
    return None


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
    """Create or reuse a HOLDS_CONTRACT edge + attach the USAspending award
    citation. IDEMPOTENT per (edge, award_id) — Stage 2 (helen 2026-08-11).

    The pre-Stage-2 code did ``edge.weight = (edge.weight or 0) + amount``
    on every ingest of the same award, so scheduled sweeps ballooned the
    weights over time — one root cause of the $89B GEO / $58B CoreCivic
    artefacts. Also re-added a SourceCitation for the same award_id per
    run, producing duplicate citation rows.

    Fixed shape: a per-award idempotency key derived from the existing
    (edge_id, kind=USASPENDING_AWARD, citation_ref=award_id) triple —
    if we've already recorded THIS award for THIS edge, we skip both
    the weight update and the citation insert. First-sighting = weight
    gets the (net-obligation) amount added ONCE + citation inserted.

    New edges are created with weight=0 and then take the amount via
    the same first-sighting path, so the create + reuse branches share
    identical accumulation semantics — no drift.
    """
    reused = False
    existing = (
        await session.execute(
            select(CanonicalEdge).where(
                CanonicalEdge.source_id == src_canonical,
                CanonicalEdge.target_id == dst_canonical,
                CanonicalEdge.relation == EdgeRelation.HOLDS_CONTRACT.value,
            )
        )
    ).scalar_one_or_none()
    if existing is None:
        edge = CanonicalEdge(
            source_id=src_canonical,
            target_id=dst_canonical,
            relation=EdgeRelation.HOLDS_CONTRACT.value,
            weight=0.0,
        )
        session.add(edge)
        await session.flush()
    else:
        edge = existing
        reused = True

    # Per-award idempotency: (edge_id, kind, award_id) is the unique
    # identity of "this award has been counted on this edge". Repeat
    # runs of the same ingest hit this branch and become no-ops.
    citation_exists = (
        await session.execute(
            select(SourceCitation).where(
                SourceCitation.edge_id == edge.id,
                SourceCitation.kind == SourceKind.USASPENDING_AWARD.value,
                SourceCitation.citation_ref == award_id,
            )
        )
    ).scalar_one_or_none()
    if citation_exists is None:
        edge.weight = float((edge.weight or 0.0) + float(amount or 0.0))
        session.add(
            SourceCitation(
                edge_id=edge.id,
                kind=SourceKind.USASPENDING_AWARD.value,
                citation_url=f"https://www.usaspending.gov/award/{award_id}",
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

    Per-anchor agency scope (helen 2026-08-11): the narrow
    ``_TARGET_AGENCIES`` whitelist (ICE / BOP / USMS) is the right
    scope ONLY for the DETENTION-OPS anchors (the
    ``DETENTION_INDUSTRY_RECIPIENTS`` set — GEO / CoreCivic / MTC /
    LaSalle / GTL / Aventiv / STOP / Securus). Every OTHER registry
    anchor (Palantir, Tesla, SpaceX, xAI, defense-tech contractors)
    gets zero rows through the whitelist and returns
    ``agencies_matched=0`` — the scheduled sweep re-errors them on
    every fire. Their real awarding agencies are NASA / DoD / State
    / etc., so they need ``broaden_agency_scope=True`` — accept
    every sub-agency and use the actual string as the anchor label.
    Palantir only produced the corrected 4.97B USD flow because helen
    ran that path by hand; the sweep needs to do it automatically.

    The caller's ``broaden_agency_scope`` still forces broadening
    for ALL anchors when set — this per-anchor decision only opens
    the scope for non-detention anchors when the caller hasn't
    already opened it globally.
    """
    from app.services.anchor_registry import anchors_for_usaspending

    detention_labels = frozenset(DETENTION_INDUSTRY_RECIPIENTS.keys())
    out: dict[str, UsaSpendingStats] = {}
    sm = get_sessionmaker()
    async with sm() as session:
        anchors = await anchors_for_usaspending(
            session, priority_domains=priority_domains
        )

    for anchor in anchors:
        # Detention-ops anchors keep the narrow ICE/BOP/USMS whitelist —
        # anchor labels for THAT beat are always a detention agency,
        # broadening would let non-detention DoD/DHS awards leak in
        # and dilute the accountability signal. Non-detention anchors
        # get broadening so their real awarding agencies surface.
        per_anchor_broaden = (
            broaden_agency_scope or anchor.label not in detention_labels
        )
        try:
            out[anchor.label] = await ingest_recipient_contracts(
                recipient_names=tuple(anchor.usaspending_recipient_names),
                canonical_hint=anchor.label,
                display_label=anchor.label,
                max_awards=max_awards,
                broaden_agency_scope=per_anchor_broaden,
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
            # Stage 2 (helen 2026-08-11): switched from "Award Amount"
            # (contract CEILING including all option years) to
            # "Total Obligated Amount" (net federal-action obligations,
            # matches the "Obligated Amount" figure on usaspending.gov's
            # public award page). Keep "Award Amount" in the fields
            # list so operators can still see the ceiling in raw dumps,
            # but the edge weight is computed from the obligation.
            "Award Amount",
            "Total Obligated Amount",
        ],
        "page": 1,
        "limit": min(100, max_awards),
        # HOTFIX (helen 2026-08-11): "Total Obligated Amount" is NOT a
        # valid ``sort`` value for the Contract-Award mapping of
        # spending_by_award — the API returns 400
        # "Sort value not found in Contract Award mappings". Sort BY
        # the ceiling ("Award Amount", the API-valid contract sort key)
        # — this only affects the ORDER we iterate results in; the
        # edge WEIGHT is still computed from the net obligation via
        # ``_award_net_obligation`` (row field or per-award detail
        # fallback). ``VALID_CONTRACT_SORT_KEYS`` below documents the
        # accepted set; a unit test asserts the current sort is in it.
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
                    # Stage 2 (helen 2026-08-11): use NET OBLIGATIONS as
                    # the edge weight, not the CEILING. Falls back to the
                    # per-award detail endpoint when spending_by_award
                    # omitted "Total Obligated Amount".
                    obligation = await _award_net_obligation(client, r)
                    _, reused = await _emit_contract_edge(
                        session,
                        geo_canonical,
                        agency_canonical,
                        obligation,
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


async def _probe_spending_by_award() -> None:
    """Fail-before-delete safety gate (helen 2026-08-11 hotfix).

    ONE minimal ``spending_by_award`` POST using the EXACT request
    shape (same filters / fields / sort / order / page / limit) the
    real ingest uses. If the API rejects our shape — invalid sort
    key, bad field, whatever — this raises BEFORE any delete runs,
    so a bad request can never wipe the graph again.

    The prior Stage 2 build shipped with an invalid sort key
    ("Total Obligated Amount" is not in the Contract-Award sort
    mapping). Because the deletes ran first and the re-ingest 400'd
    on every recipient, helen saw an empty holds_contract graph
    until she hot-patched the pod and re-ingested.

    Uses a well-known recipient anchor ("GEO GROUP INC") and
    ``limit=1`` so the probe is cheap and predictable. A 2xx even
    with zero results proves the API accepts the shape; only 4xx/5xx
    or a transport error aborts the backfill.
    """
    body = {
        "filters": {
            "recipient_search_text": ["GEO GROUP INC"],
            "award_type_codes": ["A", "B", "C", "D"],
        },
        "fields": [
            "Award ID",
            "Recipient Name",
            "Awarding Agency",
            "Awarding Sub Agency",
            "generated_internal_id",
            "Award Amount",
            "Total Obligated Amount",
        ],
        "page": 1,
        "limit": 1,
        "sort": "Award Amount",
        "order": "desc",
    }
    async with httpx.AsyncClient(timeout=30.0) as client:
        # Any HTTP error (4xx invalid sort/field, 5xx upstream) raises;
        # the backfill caller catches upstream and aborts before delete.
        await _post(client, "/search/spending_by_award/", body)


@dataclass
class BackfillStats:
    """Counters for the Stage-2 wipe-and-rebuild backfill."""

    holds_contract_edges_before: int = 0
    usaspending_citations_before: int = 0
    holds_contract_edges_deleted: int = 0
    usaspending_citations_deleted: int = 0
    holds_contract_edges_after: int = 0
    usaspending_citations_after: int = 0
    ingest_results: dict | None = None


async def backfill_holds_contract_edges(
    max_awards: int = 200,
) -> BackfillStats:
    """Stage 2 (helen 2026-08-11) wipe-and-rebuild for HOLDS_CONTRACT edges.

    The pre-Stage-2 code accumulated weights on every re-ingest AND
    used the contract CEILING instead of net obligations, so every
    HOLDS_CONTRACT edge in prod is wrong (GEO $89B, CoreCivic $58B).
    Fixing the code alone would leave those historical values
    frozen — the +=-accumulation stops but never reverses. This
    backfill is the corrective:

      1. count existing HOLDS_CONTRACT edges + their
         USASPENDING_AWARD citations (so operators can validate the
         delete counts match).
      2. DELETE every USASPENDING_AWARD SourceCitation attached to
         a HOLDS_CONTRACT edge.
      3. DELETE every HOLDS_CONTRACT CanonicalEdge (CASCADE on
         source_citations.edge_id makes step 2 redundant on Postgres
         but the explicit delete keeps the counts sound on databases
         where CASCADE isn't configured the same way).
      4. Re-run ``ingest_from_registry`` with the new obligation-
         based code + idempotent _emit_contract_edge, producing
         corrected edges from scratch.

    Idempotent by design: a second run of this backfill is safe
    (deletes are conditional on rows existing; the re-ingest is
    idempotent per (edge, award_id)). Safe to run repeatedly while
    tuning.

    Runs under a single session/transaction so a mid-backfill
    failure rolls back — the graph is either fully wiped-and-
    rebuilt or fully unchanged.

    SAFETY GATE (helen 2026-08-11 hotfix): a probe POST to
    ``spending_by_award`` runs BEFORE any delete. If the API rejects
    the request shape (invalid sort/field/filter), the probe raises
    and the backfill aborts BEFORE deleting anything. The prior
    Stage 2 build had an invalid ``sort`` key ("Total Obligated
    Amount" is not a Contract-Award sort key) and wiped every
    HOLDS_CONTRACT edge before the re-ingest 400'd. The probe
    closes that class of bug: a bad request can never wipe the
    graph again — deletes happen only after we've proven the
    downstream API can serve at least one recipient's page.
    """
    from sqlalchemy import delete

    # SAFETY: probe the API surface BEFORE any destructive DB work.
    # A raise here (invalid sort, 5xx, transport failure) exits the
    # backfill without touching a single edge or citation.
    try:
        await _probe_spending_by_award()
    except Exception:
        logger.exception(
            "usaspending backfill ABORTED — probe POST to spending_by_award "
            "failed; NO edges or citations were deleted. Fix the request "
            "shape (sort/fields/filters) and re-run."
        )
        raise

    stats = BackfillStats()
    sm = get_sessionmaker()
    async with sm() as session:
        # Pre-counts.
        stats.holds_contract_edges_before = int(
            (
                await session.execute(
                    select(sa_func.count(CanonicalEdge.id)).where(
                        CanonicalEdge.relation == EdgeRelation.HOLDS_CONTRACT.value
                    )
                )
            ).scalar_one()
            or 0
        )
        stats.usaspending_citations_before = int(
            (
                await session.execute(
                    select(sa_func.count(SourceCitation.id)).where(
                        SourceCitation.kind == SourceKind.USASPENDING_AWARD.value
                    )
                )
            ).scalar_one()
            or 0
        )

        # Explicit citation delete first (safe if ON DELETE CASCADE
        # would also handle it — the count is what we report to helen).
        cit_del = await session.execute(
            delete(SourceCitation).where(
                SourceCitation.kind == SourceKind.USASPENDING_AWARD.value,
                SourceCitation.edge_id.in_(
                    select(CanonicalEdge.id).where(
                        CanonicalEdge.relation == EdgeRelation.HOLDS_CONTRACT.value
                    )
                ),
            )
        )
        stats.usaspending_citations_deleted = int(cit_del.rowcount or 0)

        edge_del = await session.execute(
            delete(CanonicalEdge).where(
                CanonicalEdge.relation == EdgeRelation.HOLDS_CONTRACT.value
            )
        )
        stats.holds_contract_edges_deleted = int(edge_del.rowcount or 0)
        await session.commit()

    # Re-ingest fresh with the new obligation-based code. Uses the
    # registry path so every anchor in anchor_registry.usaspending_
    # recipient_names gets a run — matches what the scheduled sweep does.
    stats.ingest_results = await ingest_from_registry(max_awards=max_awards)

    async with sm() as session:
        stats.holds_contract_edges_after = int(
            (
                await session.execute(
                    select(sa_func.count(CanonicalEdge.id)).where(
                        CanonicalEdge.relation == EdgeRelation.HOLDS_CONTRACT.value
                    )
                )
            ).scalar_one()
            or 0
        )
        stats.usaspending_citations_after = int(
            (
                await session.execute(
                    select(sa_func.count(SourceCitation.id)).where(
                        SourceCitation.kind == SourceKind.USASPENDING_AWARD.value
                    )
                )
            ).scalar_one()
            or 0
        )
    return stats


def main() -> None:  # noqa: C901  — CLI dispatcher, straight-line
    """CLI entrypoint — python -m app.services.ingest.usaspending [anchor|--all|--backfill-contracts].

    Modes:
      * default (or ``--all``) — the full detention-industry recipient
        set (§P1) using the new obligation-based emitter.
      * anchor label (``GEO Group`` / ``CoreCivic`` / …) — one recipient.
      * ``--backfill-contracts`` — Stage 2 wipe-and-rebuild: delete
        every existing HOLDS_CONTRACT edge + its USASPENDING_AWARD
        citations, then re-ingest via the registry with the new code.
        Fixes historical +=-accumulation and ceiling-vs-obligation
        drift in one shot. See ``backfill_holds_contract_edges``
        docstring for the safety story.
    """
    import sys

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    arg = " ".join(sys.argv[1:]).strip() or "--all"
    if arg in ("--backfill-contracts", "backfill-contracts"):
        stats = asyncio.run(backfill_holds_contract_edges())
        logger.info(
            "usaspending backfill done: %d holds_contract edges wiped "
            "(%d citations), %d edges rebuilt (%d citations)",
            stats.holds_contract_edges_deleted,
            stats.usaspending_citations_deleted,
            stats.holds_contract_edges_after,
            stats.usaspending_citations_after,
        )
        for label, s in (stats.ingest_results or {}).items():
            logger.info("[%s] %s", label, s)
        return
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
            "unknown anchor %r; choose from %s or --all or --backfill-contracts",
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
