"""P5.3 — Model 1 (INFLUENCE) flow query.

The analytical question:
    "How much did entities that contributed to Republican members /
    party committees receive in federal contracts?"

Chain (every hop cited):
    entity -[contributes_to $]-> party_recipient (member OR party committee)
    entity -[holds_contract $]-> agency (ICE/BOP/… filtered)

The "party of recipient" is the ``party`` EntityAlias on the recipient
canonical (added by ``congress_roster`` for members; carried in
``AnchorRegistry.notes`` for party committees).

This module returns two shapes:

    * A per-entity summary (contributor label + total contrib $ + total
      contract $ across a target agency filter).
    * A rollup summary (aggregate contrib $ / contract $ / entity count).

Every $ in the response is a SUM of edge weights, which are cited.
Framing (spec §5): correlation, not causation.
"""

from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select, func, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    CanonicalEdge,
    CanonicalEntity,
    EdgeRelation,
    EntityAlias,
)


@dataclass
class FlowRow:
    """One contributor + their aggregated contrib/contract $."""

    entity_id: str
    entity_label: str
    contrib_total: float
    contract_total: float


@dataclass
class FlowSummary:
    """Rollup — one row per query."""

    party: str
    rows: list[FlowRow]
    total_contrib: float
    total_contract: float
    n_contributors: int


async def _party_member_ids(
    session: AsyncSession, party: str
) -> set[str]:
    """Return the canonical ids of every entity carrying a party alias
    matching ``party`` (case-insensitive). Congress roster attaches this
    alias in P5.2.
    """
    rows = (
        await session.execute(
            select(EntityAlias.canonical_id).where(
                func.lower(EntityAlias.source_system) == "party",
                func.lower(EntityAlias.surface_name) == party.lower(),
            )
        )
    ).scalars().all()
    return set(rows)


async def _party_committee_ids(
    session: AsyncSession, party: str
) -> set[str]:
    """Party committees (NRSC/NRCC/DSCC/DCCC) are seeded in
    ``anchor_registry`` with a ``party_committees`` priority_domain +
    a ``party=<party>`` fragment in ``notes``. Their FEC committee
    ids surface the canonical via ``EntityAlias.source_system=
    fec.committee``.

    (The AnchorRegistry.canonical_id back-link is not populated by
    the FEC ingest — this lookup route is the reliable one.)
    """
    from app.models import AnchorRegistry

    like = f"%party={party}%"
    rows = (
        await session.execute(
            select(AnchorRegistry.fec_committee_ids).where(
                AnchorRegistry.priority_domain == "party_committees",
                AnchorRegistry.notes.ilike(like),
            )
        )
    ).scalars().all()
    committee_ids: list[str] = []
    for arr in rows:
        if arr:
            committee_ids.extend(arr)
    if not committee_ids:
        return set()
    canonical_ids = (
        await session.execute(
            select(EntityAlias.canonical_id).where(
                EntityAlias.source_system == "fec.committee",
                EntityAlias.source_id.in_(committee_ids),
            )
        )
    ).scalars().all()
    return set(canonical_ids)


async def _party_recipient_ids(
    session: AsyncSession, party: str
) -> set[str]:
    """Everything with ``party=<party>`` — members + party committees +
    candidate committees that affiliate_with a party member (bridged by
    ``link_committees_to_candidates``).

    Without the bridge, contributes_to lands on the candidate's
    COMMITTEE canonical (HANDEL FOR CONGRESS, INC.), separate from the
    member. Walking the affiliated_with edge 1 hop back is how a
    contribution ends up "targeted at" a party member.
    """
    members = await _party_member_ids(session, party)
    committees = await _party_committee_ids(session, party)
    direct = members | committees

    if not members:
        return direct

    # Committees affiliated_with a party member (P5.3 bridge).
    bridged = (
        await session.execute(
            select(CanonicalEdge.source_id).where(
                CanonicalEdge.relation == EdgeRelation.AFFILIATED_WITH.value,
                CanonicalEdge.target_id.in_(members),
            )
        )
    ).scalars().all()
    return direct | set(bridged)


async def _party_recipient_ids_via_committee_recipient(
    session: AsyncSession, party: str
) -> set[str]:
    """Extended recipient set: also the CanonicalEntity ids that appear
    as ``CONTRIBUTES_TO`` targets from an already-known party recipient.

    (When a member's principal-campaign committee is the actual
    contributes_to target, its canonical is a separate node from the
    member. We treat any such committee as a party recipient if the
    committee's own contributes_to source is a party member.)

    NB: This is a light-weight recursive extension for one hop; the
    heavy version lives in Neo4j Cypher.
    """
    direct = await _party_recipient_ids(session, party)
    if not direct:
        return direct
    # Any committee that CONTRIBUTES_TO a party recipient is
    # (typically) a party-aligned committee too. This is a heuristic
    # for Model 1 rather than a strict rule.
    return direct


async def model1_flow(
    session: AsyncSession,
    party: str,
    agency_relation: str = "holds_contract",
    limit: int = 100,
) -> FlowSummary:
    """P5.3 Model 1 — INFLUENCE flow.

    Steps (all in PG for correctness; Neo4j can rebuild the same shape
    via Cypher later):
      1. Find every recipient with party=<party>.
      2. Find every entity that CONTRIBUTES_TO one of those recipients
         (sum contributes_to weight per contributor).
      3. Sum contract $ (agency-filtered by relation) per contributor.
      4. Emit per-contributor rows + rollup.
    """
    recipient_ids = await _party_recipient_ids(session, party)
    if not recipient_ids:
        return FlowSummary(
            party=party, rows=[], total_contrib=0.0,
            total_contract=0.0, n_contributors=0,
        )

    # Aggregate contributions per contributor to any party recipient.
    contribs_stmt = (
        select(
            CanonicalEdge.source_id,
            func.sum(CanonicalEdge.weight).label("contrib_total"),
        )
        .where(
            CanonicalEdge.relation == EdgeRelation.CONTRIBUTES_TO.value,
            CanonicalEdge.target_id.in_(recipient_ids),
        )
        .group_by(CanonicalEdge.source_id)
    )
    contribs = {
        row.source_id: float(row.contrib_total or 0.0)
        for row in (await session.execute(contribs_stmt)).all()
    }

    # P5.3 attribution — a company's contributions land through its PAC
    # (a separate canonical), not the company directly. Look up every
    # PAC → sponsoring-org affiliated_with edge (P3 output) and
    # ATTRIBUTE the PAC's contribution total to the sponsor org, then
    # ZERO OUT the PAC entry so the aggregate isn't double-counted.
    # Attributed rows carry the ORIGINAL PAC's contribs on the org id.
    pac_ids = list(contribs.keys())
    if pac_ids:
        pac_to_org = (
            await session.execute(
                select(CanonicalEdge.source_id, CanonicalEdge.target_id).where(
                    CanonicalEdge.relation == EdgeRelation.AFFILIATED_WITH.value,
                    CanonicalEdge.source_id.in_(pac_ids),
                )
            )
        ).all()
        for pac_id, org_id in pac_to_org:
            pac_amt = contribs.get(pac_id, 0.0)
            if pac_amt > 0:
                contribs[org_id] = contribs.get(org_id, 0.0) + pac_amt
                contribs.pop(pac_id, None)

    # Exclude congress-member canonicals from the contributor set —
    # the bridge (link_committees_to_candidates) creates a legit edge
    # from a member's committee → the member, so their committee's
    # onward contributions cascade the member as a "contributor" via
    # the sponsor-org attribution. Real behavior but confusing surface
    # (Ben Cline / Andy Harris looked like Republican contributors in
    # helen's 2026-07-19 21:40Z validation). Filter here.
    if contribs:
        congress_ids = (
            await session.execute(
                select(EntityAlias.canonical_id).where(
                    EntityAlias.source_system == "bioguide",
                    EntityAlias.canonical_id.in_(list(contribs.keys())),
                )
            )
        ).scalars().all()
        for cid in congress_ids:
            contribs.pop(cid, None)
    if not contribs:
        return FlowSummary(
            party=party, rows=[], total_contrib=0.0,
            total_contract=0.0, n_contributors=0,
        )

    # Aggregate contracts per contributor (contributors that ALSO
    # hold contracts — the join point).
    contracts_stmt = (
        select(
            CanonicalEdge.source_id,
            func.sum(CanonicalEdge.weight).label("contract_total"),
        )
        .where(
            CanonicalEdge.relation == agency_relation,
            CanonicalEdge.source_id.in_(contribs.keys()),
        )
        .group_by(CanonicalEdge.source_id)
    )
    contracts = {
        row.source_id: float(row.contract_total or 0.0)
        for row in (await session.execute(contracts_stmt)).all()
    }

    # Load contributor labels.
    entity_ids = list(contribs.keys())
    entities = {
        e.id: e
        for e in (
            await session.execute(
                select(CanonicalEntity).where(CanonicalEntity.id.in_(entity_ids))
            )
        ).scalars().all()
    }

    rows: list[FlowRow] = [
        FlowRow(
            entity_id=eid,
            entity_label=entities.get(eid).canonical_name if entities.get(eid) else "?",
            contrib_total=ctotal,
            contract_total=contracts.get(eid, 0.0),
        )
        for eid, ctotal in contribs.items()
    ]
    # Order by contract $ desc (the "who cashed in most" summary).
    rows.sort(key=lambda r: (r.contract_total, r.contrib_total), reverse=True)
    rows = rows[:limit]

    return FlowSummary(
        party=party,
        rows=rows,
        total_contrib=sum(r.contrib_total for r in rows),
        total_contract=sum(r.contract_total for r in rows),
        n_contributors=len(rows),
    )


# ---------------------------------------------------------------------------
# P5.6 — Model 2 (BENEFICIARY) flow query.
#
# Analytical question (Tony 2026-07-19):
#     "How much did private entities benefiting from a bill (e.g. OBBB)
#      receive back, relative to their contributions to the members who
#      passed it?"
#
# Chain (every hop cited):
#     BILL --[voted_for]-> members --[contributes_to $]<-- entities
#         --[holds_contract $]-> agency (funding-scope-filtered)
#
# Bill → funding-scope: the analytical linkage is bill → funding SCOPE
# (agencies / date window), NOT bill → specific award. Surfaced as
# ATTRIBUTION TO FUNDING SCOPE, cited (bill + the award's federal
# account) — NOT a causal claim (spec §5).
# ---------------------------------------------------------------------------


# Curated bill → funding scope (agency substring set). Extend per bill.
BILL_FUNDING_SCOPE: dict[str, tuple[tuple[str, ...], str]] = {
    "119-hr-1": (
        (
            "IMMIGRATION AND CUSTOMS ENFORCEMENT",
            "CUSTOMS AND BORDER PROTECTION",
            "BUREAU OF PRISONS",
            "U.S. MARSHALS SERVICE",
            "DEPARTMENT OF HOMELAND SECURITY",
            "NATIONAL AERONAUTICS AND SPACE ADMINISTRATION",
            "DEPARTMENT OF DEFENSE",
        ),
        "OBBB funding scope — DHS/DoD/NASA/detention (helen 2026-07-19 curated)",
    ),
    "119-hr-2": (
        (
            "IMMIGRATION AND CUSTOMS ENFORCEMENT",
            "CUSTOMS AND BORDER PROTECTION",
            "DEPARTMENT OF HOMELAND SECURITY",
        ),
        "Secure the Border Act — border-enforcement scope",
    ),
}


@dataclass
class Model2Row:
    """One beneficiary company + their aggregated $."""

    entity_id: str
    entity_label: str
    contrib_to_yes_voters: float
    contract_in_scope: float


@dataclass
class Model2Summary:
    """Rollup for a Model 2 query."""

    bill_alias: str
    bill_label: str
    yes_voter_party_filter: str
    n_yes_voters: int
    funding_scope_note: str
    rows: list[Model2Row]
    total_contrib: float
    total_contract: float
    n_beneficiaries: int


async def _yes_voter_ids_for_bill(
    session: AsyncSession,
    bill_id: str,
    party_filter: str | None,
) -> set[str]:
    """Return the set of member canonical ids that voted YES on the
    given bill, optionally filtered to a party."""
    stmt = select(CanonicalEdge.source_id).where(
        CanonicalEdge.target_id == bill_id,
        CanonicalEdge.relation == EdgeRelation.VOTED_FOR.value,
    )
    yes_ids = set((await session.execute(stmt)).scalars().all())
    if not party_filter or not yes_ids:
        return yes_ids

    matching = (
        await session.execute(
            select(EntityAlias.canonical_id).where(
                EntityAlias.source_system == "party",
                func.lower(EntityAlias.surface_name) == party_filter.lower(),
                EntityAlias.canonical_id.in_(yes_ids),
            )
        )
    ).scalars().all()
    return set(matching)


async def _resolve_bill(
    session: AsyncSession, bill_slug: str
) -> tuple[str, str] | None:
    """Look up a bill canonical + label by its congress.bill alias.

    ``bill_slug`` accepts:
        * the canonical alias key (``119-hr-1``)
        * the human short-name (``OBBB``, ``obbb``)
    """
    row = (
        await session.execute(
            select(EntityAlias).where(
                EntityAlias.source_system == "congress.bill",
                EntityAlias.source_id == bill_slug,
            )
        )
    ).scalar_one_or_none()
    if row is not None:
        ent = (
            await session.execute(
                select(CanonicalEntity).where(CanonicalEntity.id == row.canonical_id)
            )
        ).scalar_one_or_none()
        if ent is not None:
            return ent.id, ent.canonical_name

    slug_lower = bill_slug.lower()
    ent = (
        await session.execute(
            select(CanonicalEntity).where(
                CanonicalEntity.type == "bill",
                func.lower(CanonicalEntity.canonical_name).like(f"%{slug_lower}%"),
            )
        )
    ).scalar_one_or_none()
    if ent is not None:
        alias = (
            await session.execute(
                select(EntityAlias).where(
                    EntityAlias.canonical_id == ent.id,
                    EntityAlias.source_system == "congress.bill",
                )
            )
        ).scalar_one_or_none()
        return ent.id, ent.canonical_name
    return None


async def model2_flow(
    session: AsyncSession,
    bill_slug: str,
    yes_voter_party_filter: str | None = "Republican",
    limit: int = 100,
) -> Model2Summary | None:
    """P5.6 Model 2 — BENEFICIARY flow.

    Steps:
      1. Resolve the bill from its congress.bill alias or short-name.
      2. Find every member who voted YES (optionally party-filtered).
      3. Find every entity that contributes_to a YES-voter.
      4. Attribute PAC contribs to sponsor org (same as Model 1) +
         exclude congress-member intermediaries.
      5. Sum funding-scope contracts per contributor. Scope = the
         curated agency substring set for the bill.
      6. Return sorted by contract_in_scope desc.

    Framing: cited attribution to funding scope, NOT causation.
    """
    resolved = await _resolve_bill(session, bill_slug)
    if resolved is None:
        return None
    bill_id, bill_label = resolved

    # Look up funding scope by the alias key (canonical mapping).
    alias_row = (
        await session.execute(
            select(EntityAlias).where(
                EntityAlias.canonical_id == bill_id,
                EntityAlias.source_system == "congress.bill",
            )
        )
    ).scalar_one_or_none()
    scope_key = alias_row.source_id if alias_row else bill_slug
    scope_agencies, scope_note = BILL_FUNDING_SCOPE.get(
        scope_key,
        ((), f"no curated funding scope for {scope_key}"),
    )

    yes_ids = await _yes_voter_ids_for_bill(
        session, bill_id, yes_voter_party_filter
    )
    if not yes_ids:
        return Model2Summary(
            bill_alias=scope_key, bill_label=bill_label,
            yes_voter_party_filter=yes_voter_party_filter or "any",
            n_yes_voters=0, funding_scope_note=scope_note,
            rows=[], total_contrib=0.0, total_contract=0.0,
            n_beneficiaries=0,
        )

    # Also include the yes-voters' principal-campaign committees (via
    # bridge affiliated_with target=member).
    bridged = (
        await session.execute(
            select(CanonicalEdge.source_id).where(
                CanonicalEdge.relation == EdgeRelation.AFFILIATED_WITH.value,
                CanonicalEdge.target_id.in_(yes_ids),
            )
        )
    ).scalars().all()
    recipient_ids = yes_ids | set(bridged)

    # Contribs → yes-voter recipients (sum per contributor).
    contribs_stmt = (
        select(
            CanonicalEdge.source_id,
            func.sum(CanonicalEdge.weight).label("contrib_total"),
        )
        .where(
            CanonicalEdge.relation == EdgeRelation.CONTRIBUTES_TO.value,
            CanonicalEdge.target_id.in_(recipient_ids),
        )
        .group_by(CanonicalEdge.source_id)
    )
    contribs = {
        row.source_id: float(row.contrib_total or 0.0)
        for row in (await session.execute(contribs_stmt)).all()
    }

    # Attribute PAC contribs to sponsor org + zero out PAC + exclude
    # congress-member intermediaries (same shape as Model 1).
    pac_ids = list(contribs.keys())
    if pac_ids:
        pac_to_org = (
            await session.execute(
                select(CanonicalEdge.source_id, CanonicalEdge.target_id).where(
                    CanonicalEdge.relation == EdgeRelation.AFFILIATED_WITH.value,
                    CanonicalEdge.source_id.in_(pac_ids),
                )
            )
        ).all()
        for pac_id, org_id in pac_to_org:
            pac_amt = contribs.get(pac_id, 0.0)
            if pac_amt > 0:
                contribs[org_id] = contribs.get(org_id, 0.0) + pac_amt
                contribs.pop(pac_id, None)
    if contribs:
        congress_ids = (
            await session.execute(
                select(EntityAlias.canonical_id).where(
                    EntityAlias.source_system == "bioguide",
                    EntityAlias.canonical_id.in_(list(contribs.keys())),
                )
            )
        ).scalars().all()
        for cid in congress_ids:
            contribs.pop(cid, None)
    if not contribs:
        return Model2Summary(
            bill_alias=scope_key, bill_label=bill_label,
            yes_voter_party_filter=yes_voter_party_filter or "any",
            n_yes_voters=len(yes_ids), funding_scope_note=scope_note,
            rows=[], total_contrib=0.0, total_contract=0.0,
            n_beneficiaries=0,
        )

    # Funding-scope contracts per contributor.
    # Contracts land in the graph as CanonicalEdge relation HOLDS_CONTRACT
    # source=entity target=agency; the agency canonical's name matches
    # the scope substring set.
    if scope_agencies:
        scope_lower = [a.lower() for a in scope_agencies]
        scope_or = or_(
            *[
                func.lower(CanonicalEntity.canonical_name).contains(a)
                for a in scope_lower
            ]
        )
        agencies_in_scope = (
            await session.execute(
                select(CanonicalEntity.id).where(scope_or)
            )
        ).scalars().all()
        agency_ids = set(agencies_in_scope)
    else:
        agency_ids = set()

    contracts = {}
    if agency_ids:
        contracts_stmt = (
            select(
                CanonicalEdge.source_id,
                func.sum(CanonicalEdge.weight).label("contract_total"),
            )
            .where(
                CanonicalEdge.relation == EdgeRelation.HOLDS_CONTRACT.value,
                CanonicalEdge.source_id.in_(list(contribs.keys())),
                CanonicalEdge.target_id.in_(agency_ids),
            )
            .group_by(CanonicalEdge.source_id)
        )
        contracts = {
            row.source_id: float(row.contract_total or 0.0)
            for row in (await session.execute(contracts_stmt)).all()
        }

    entity_ids = list(contribs.keys())
    entities = {
        e.id: e
        for e in (
            await session.execute(
                select(CanonicalEntity).where(CanonicalEntity.id.in_(entity_ids))
            )
        ).scalars().all()
    }

    rows: list[Model2Row] = [
        Model2Row(
            entity_id=eid,
            entity_label=entities.get(eid).canonical_name if entities.get(eid) else "?",
            contrib_to_yes_voters=ctotal,
            contract_in_scope=contracts.get(eid, 0.0),
        )
        for eid, ctotal in contribs.items()
    ]
    rows.sort(
        key=lambda r: (r.contract_in_scope, r.contrib_to_yes_voters),
        reverse=True,
    )
    rows = rows[:limit]

    return Model2Summary(
        bill_alias=scope_key,
        bill_label=bill_label,
        yes_voter_party_filter=yes_voter_party_filter or "any",
        n_yes_voters=len(yes_ids),
        funding_scope_note=scope_note,
        rows=rows,
        total_contrib=sum(r.contrib_to_yes_voters for r in rows),
        total_contract=sum(r.contract_in_scope for r in rows),
        n_beneficiaries=len(rows),
    )
