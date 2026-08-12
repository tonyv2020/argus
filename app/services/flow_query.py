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

from dataclasses import dataclass, field

from sqlalchemy import select, func, or_
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import (
    CanonicalEdge,
    CanonicalEntity,
    EdgeRelation,
    EntityAlias,
    SourceCitation,
)
from app.services.read_gate import published_edge


@dataclass(frozen=True)
class CitationRef:
    """One primary-source citation surfaced alongside a FlowRow.

    E1 award-grade enrichment (Tony directive 2026-08-12): Model 1
    contrib/contract sums have always been backed by real SourceCitation
    rows (usaspending_award for contracts, fec_filing for contributions
    via affiliated PACs). Before E1 those citations lived only inside
    the graph and never surfaced through the flow API — so
    hollywood_gen.agents.blog.argus_research could only offer Cassandra
    entity deep-links, never the primary-source URLs her decline rule
    correctly wants. This dataclass is the shape the API surfaces so
    her SAFE CITATION MENU can carry them.
    """

    kind: str  # e.g. "usaspending_award" | "fec_filing"
    url: str
    ref: str | None = None  # award id / fec txn id / permalink slug


@dataclass
class FlowRow:
    """One contributor + their aggregated contrib/contract $ +, since E1,
    a bounded list of the top underlying primary-source citations for
    each side of the correlation (so Cassandra can cite the actual
    award / FEC filing URL, not just the Argus entity deep-link).
    """

    entity_id: str
    entity_label: str
    contrib_total: float
    contract_total: float
    # E1 award-grade citations. Populated from real SourceCitation rows
    # on the same edges whose weights sum to the totals above (contract
    # rows come from holds_contract edges; contribution rows come from
    # contributes_to edges reached via the entity's affiliated_with
    # PACs — the same PAC walk model1_flow uses to attribute contrib
    # totals, so citations + totals are consistent by construction).
    # Deduped by url; ranked by edge weight desc; capped by the
    # top_citations_per_side arg on model1_flow (default 4).
    top_contract_citations: list[CitationRef] = field(default_factory=list)
    top_contribution_citations: list[CitationRef] = field(default_factory=list)
    # BROADEN-LAND (Tony 2026-08-12): support cited-floor framing on
    # the hollywood side. ``contrib_total`` above is the PARTY-CLASSIFIED
    # attribution (recipients matching _party_recipient_ids for the
    # queried party). Party classification is bounded by FEC-quota-
    # limited enrichment, so it's typically 30-80% of the real captured
    # giving. Cassandra frames it as an HONEST FLOOR ("at least $X in
    # cited [party]-aligned contributions") — never a total — and needs
    # both numbers to say so without overclaiming.
    #
    # ``contrib_total_captured`` = sum of every contributes_to weight
    # from every source attributed to this entity (party-classified +
    # unclassified) — the "total captured" number.
    #
    # ``has_corporate_pac`` = at least one affiliated_with source of
    # this entity's contribs is a type='pac' canonical (as opposed to
    # only individual-person affiliates like Peter Thiel → Palantir).
    # Hollywood's data-quality gate rejects individual-only rows so
    # personal giving never gets published as if it were corporate PAC
    # money-flow.
    contrib_total_captured: float = 0.0
    has_corporate_pac: bool = False


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
    # RG2: exclude staged edges from public flow attribution.
    bridged = (
        await session.execute(
            select(CanonicalEdge.source_id).where(
                CanonicalEdge.relation == EdgeRelation.AFFILIATED_WITH.value,
                CanonicalEdge.target_id.in_(members),
                published_edge(),
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
    top_citations_per_side: int = 4,
) -> FlowSummary:
    """P5.3 Model 1 — INFLUENCE flow.

    Steps (all in PG for correctness; Neo4j can rebuild the same shape
    via Cypher later):
      1. Find every recipient with party=<party>.
      2. Find every entity that CONTRIBUTES_TO one of those recipients
         (sum contributes_to weight per contributor).
      3. Sum contract $ (agency-filtered by relation) per contributor.
      4. E1 (Tony directive 2026-08-12) — for the top-``limit`` rows,
         pull the top-``top_citations_per_side`` underlying
         SourceCitations for each side (usaspending_award for
         contracts; fec_filing for contributions, via the same
         affiliated_with PAC walk used to attribute contrib totals).
         Ranked by edge weight desc; deduped by URL. This is the
         hop that unlocks award-grade Cassandra columns.
      5. Emit per-contributor rows + rollup.
    """
    recipient_ids = await _party_recipient_ids(session, party)
    if not recipient_ids:
        return FlowSummary(
            party=party, rows=[], total_contrib=0.0,
            total_contract=0.0, n_contributors=0,
        )

    # Aggregate contributions per contributor to any party recipient.
    # RG2: staged contribution edges must not inflate a mid-batch flow response.
    contribs_stmt = (
        select(
            CanonicalEdge.source_id,
            func.sum(CanonicalEdge.weight).label("contrib_total"),
        )
        .where(
            CanonicalEdge.relation == EdgeRelation.CONTRIBUTES_TO.value,
            CanonicalEdge.target_id.in_(recipient_ids),
            published_edge(),
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
    # E1: build org_to_pacs mapping alongside the attribution walk so
    # the citation-gather step below can reach each org's contributes_to
    # edges via the same PAC set the totals came from (consistency).
    org_to_pacs: dict[str, set[str]] = {}
    if pac_ids:
        # RG2: PAC → sponsor org attribution must not follow staged edges.
        pac_to_org = (
            await session.execute(
                select(CanonicalEdge.source_id, CanonicalEdge.target_id).where(
                    CanonicalEdge.relation == EdgeRelation.AFFILIATED_WITH.value,
                    CanonicalEdge.source_id.in_(pac_ids),
                    published_edge(),
                )
            )
        ).all()
        for pac_id, org_id in pac_to_org:
            pac_amt = contribs.get(pac_id, 0.0)
            if pac_amt > 0:
                contribs[org_id] = contribs.get(org_id, 0.0) + pac_amt
                contribs.pop(pac_id, None)
            org_to_pacs.setdefault(org_id, set()).add(pac_id)

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
    # RG2: staged contract edges must not surface in a public flow response.
    contracts_stmt = (
        select(
            CanonicalEdge.source_id,
            func.sum(CanonicalEdge.weight).label("contract_total"),
        )
        .where(
            CanonicalEdge.relation == agency_relation,
            CanonicalEdge.source_id.in_(contribs.keys()),
            published_edge(),
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

    # E1: gather top-N primary-source citations for exactly the rows we
    # will return (bounds the extra query cost to O(top-limit rows), not
    # the whole contributor set). Each entity gets a
    # top_contract_citations list from its own holds_contract edges and
    # a top_contribution_citations list from the same PAC set the
    # contrib_total was attributed through.
    if rows and top_citations_per_side > 0:
        await _attach_top_citations(
            session,
            rows,
            org_to_pacs=org_to_pacs,
            recipient_ids=recipient_ids,
            agency_relation=agency_relation,
            top_per_side=top_citations_per_side,
        )

    # BROADEN-LAND (Tony 2026-08-12): attach contrib_total_captured
    # (all-targets contribution sum, not party-filtered) + has_corporate_pac
    # (true iff at least one affiliated source of this entity's contribs is
    # a type='pac' canonical). These support the hollywood-side cited-floor
    # framing + the $250K/real-corporate-PAC data-quality gate. Bounded to
    # the top-``limit`` rows same as citations, so the extra cost is O(top)
    # queries regardless of contributor-set size.
    if rows:
        await _attach_captured_and_pac_flag(
            session,
            rows,
            org_to_pacs=org_to_pacs,
        )

    return FlowSummary(
        party=party,
        rows=rows,
        total_contrib=sum(r.contrib_total for r in rows),
        total_contract=sum(r.contract_total for r in rows),
        n_contributors=len(rows),
    )


async def _attach_captured_and_pac_flag(
    session: AsyncSession,
    rows: list[FlowRow],
    *,
    org_to_pacs: dict[str, set[str]],
) -> None:
    """Populate ``contrib_total_captured`` + ``has_corporate_pac`` on
    each row in place.

    ``contrib_total_captured`` = sum of every contributes_to weight
    from {entity} ∪ {its affiliated PACs} — NOT party-filtered. This
    is the total giving Argus has actually captured for the entity's
    money-flow, before flow_model1's party attribution filter drops
    the unclassified portion. Cassandra frames the classified figure
    (``contrib_total``) as a FLOOR against this captured total in her
    cited-floor language ("at least $X classified of $Y total captured").

    ``has_corporate_pac`` = at least one entity in {entity + its
    affiliated PACs} has ``CanonicalEntity.type = 'pac'``. False =
    contributions come only from an individual-person affiliate (e.g.
    Peter Thiel → Palantir via the P16 executive affiliation seed);
    hollywood's data-quality gate rejects individual-only rows so
    personal giving never gets published as corporate PAC money-flow.
    """
    if not rows:
        return

    # Build per-entity source set (entity + affiliated PACs), same
    # walk _attach_top_citations uses for contribution citations.
    entity_source_ids: dict[str, set[str]] = {}
    all_sources: set[str] = set()
    for r in rows:
        srcs = {r.entity_id} | org_to_pacs.get(r.entity_id, set())
        entity_source_ids[r.entity_id] = srcs
        all_sources |= srcs

    if not all_sources:
        for r in rows:
            r.contrib_total_captured = 0.0
            r.has_corporate_pac = False
        return

    # Total captured contribs per source (no party filter).
    stmt = (
        select(
            CanonicalEdge.source_id,
            func.sum(CanonicalEdge.weight).label("captured"),
        )
        .where(
            CanonicalEdge.relation == EdgeRelation.CONTRIBUTES_TO.value,
            CanonicalEdge.source_id.in_(all_sources),
            published_edge(),
        )
        .group_by(CanonicalEdge.source_id)
    )
    captured_by_source = {
        row.source_id: float(row.captured or 0.0)
        for row in (await session.execute(stmt)).all()
    }

    # Entity types (for the corporate-PAC test) — pull once for all
    # sources.
    type_stmt = select(CanonicalEntity.id, CanonicalEntity.type).where(
        CanonicalEntity.id.in_(all_sources)
    )
    types = {
        row.id: row.type
        for row in (await session.execute(type_stmt)).all()
    }

    for r in rows:
        srcs = entity_source_ids[r.entity_id]
        r.contrib_total_captured = sum(
            captured_by_source.get(s, 0.0) for s in srcs
        )
        r.has_corporate_pac = any(
            types.get(s) == "pac" for s in srcs
        )


async def _attach_top_citations(
    session: AsyncSession,
    rows: list[FlowRow],
    *,
    org_to_pacs: dict[str, set[str]],
    recipient_ids: set[str],
    agency_relation: str,
    top_per_side: int,
) -> None:
    """Populate ``top_contract_citations`` + ``top_contribution_citations``
    on each row in place.

    Design notes:
      * Reuses the SAME edge sets model1_flow summed for the totals —
        contract citations from ``holds_contract`` edges out of the
        entity, contribution citations from ``contributes_to`` edges
        out of {entity} ∪ {its PACs} into the party recipient set.
        Any consistency divergence between totals and citations would
        undermine the whole trust story ("if the ledger sum comes from
        edges A, B, C, its citations MUST be A's, B's, C's citations
        — not the neighbour's").
      * Ranks citations by owning-edge weight desc (biggest awards +
        biggest contributions first). Ties break arbitrarily by URL
        order — good enough for a "top receipts" surface.
      * Deduplicates by citation URL: FEC individual-contribution URLs
        repeat across many edges; the reader wants one clickable page
        per unique receipt.
      * Only surfaces PUBLISHED edges (``published_edge()``), so RG2's
        staged-batch invariant continues to hold.
    """
    entity_ids = [r.entity_id for r in rows]

    # ---- contract citations: holds_contract edges out of the entity ----
    contract_edges = (
        await session.execute(
            select(
                CanonicalEdge.id,
                CanonicalEdge.source_id,
                CanonicalEdge.weight,
            ).where(
                CanonicalEdge.relation == agency_relation,
                CanonicalEdge.source_id.in_(entity_ids),
                published_edge(),
            )
        )
    ).all()
    # Group edges per entity, weight-sorted desc; keep a lookup for
    # citations by edge id.
    entity_contract_edges: dict[str, list[tuple[str, float]]] = {}
    for edge_id, src_id, weight in contract_edges:
        entity_contract_edges.setdefault(src_id, []).append((edge_id, float(weight or 0.0)))
    for eid in entity_contract_edges:
        entity_contract_edges[eid].sort(key=lambda t: t[1], reverse=True)

    contract_edge_ids = [e for _, edges in entity_contract_edges.items() for e, _ in edges]
    contract_citations_by_edge: dict[str, list[SourceCitation]] = {}
    if contract_edge_ids:
        cs = (
            await session.execute(
                select(SourceCitation).where(
                    SourceCitation.edge_id.in_(contract_edge_ids),
                )
            )
        ).scalars().all()
        for c in cs:
            contract_citations_by_edge.setdefault(c.edge_id, []).append(c)

    # ---- contribution citations: contributes_to edges out of {entity ∪ its PACs} ----
    # Build the source-set for each entity (self + affiliated PACs) so
    # the SAME edges that summed to contrib_total surface the citations.
    entity_contrib_source_ids: dict[str, set[str]] = {}
    all_contrib_sources: set[str] = set()
    for r in rows:
        srcs = {r.entity_id} | org_to_pacs.get(r.entity_id, set())
        entity_contrib_source_ids[r.entity_id] = srcs
        all_contrib_sources |= srcs

    contrib_edges = []
    if all_contrib_sources and recipient_ids:
        contrib_edges = (
            await session.execute(
                select(
                    CanonicalEdge.id,
                    CanonicalEdge.source_id,
                    CanonicalEdge.weight,
                ).where(
                    CanonicalEdge.relation == EdgeRelation.CONTRIBUTES_TO.value,
                    CanonicalEdge.source_id.in_(all_contrib_sources),
                    CanonicalEdge.target_id.in_(recipient_ids),
                    published_edge(),
                )
            )
        ).all()
    # Group per contribution-source (which is a PAC or the entity
    # itself), weight-sorted desc.
    source_contrib_edges: dict[str, list[tuple[str, float]]] = {}
    for edge_id, src_id, weight in contrib_edges:
        source_contrib_edges.setdefault(src_id, []).append((edge_id, float(weight or 0.0)))
    for src_id in source_contrib_edges:
        source_contrib_edges[src_id].sort(key=lambda t: t[1], reverse=True)

    contrib_edge_ids = [e for edges in source_contrib_edges.values() for e, _ in edges]
    contrib_citations_by_edge: dict[str, list[SourceCitation]] = {}
    if contrib_edge_ids:
        cs = (
            await session.execute(
                select(SourceCitation).where(
                    SourceCitation.edge_id.in_(contrib_edge_ids),
                )
            )
        ).scalars().all()
        for c in cs:
            contrib_citations_by_edge.setdefault(c.edge_id, []).append(c)

    def _dedupe_and_cap(cits_ordered: list[SourceCitation]) -> list[CitationRef]:
        seen: set[str] = set()
        out: list[CitationRef] = []
        for c in cits_ordered:
            key = c.citation_url
            if not key or key in seen:
                continue
            seen.add(key)
            out.append(CitationRef(kind=c.kind, url=c.citation_url, ref=c.citation_ref))
            if len(out) >= top_per_side:
                break
        return out

    for row in rows:
        # Contract side: walk this entity's edges in weight order,
        # append each edge's citations in insertion order.
        contract_ordered: list[SourceCitation] = []
        for edge_id, _ in entity_contract_edges.get(row.entity_id, []):
            contract_ordered.extend(contract_citations_by_edge.get(edge_id, []))
        row.top_contract_citations = _dedupe_and_cap(contract_ordered)

        # Contribution side: same pattern, but iterate over ALL of the
        # entity's contribution sources (self + affiliated PACs),
        # weight-sorted, and pick citations from those edges.
        contrib_ordered_all: list[tuple[float, SourceCitation]] = []
        for src_id in entity_contrib_source_ids.get(row.entity_id, set()):
            for edge_id, weight in source_contrib_edges.get(src_id, []):
                for c in contrib_citations_by_edge.get(edge_id, []):
                    contrib_ordered_all.append((weight, c))
        # Sort across all sources by owning-edge weight desc so the
        # highest-value receipts come first regardless of which PAC
        # they were on.
        contrib_ordered_all.sort(key=lambda t: t[0], reverse=True)
        row.top_contribution_citations = _dedupe_and_cap(
            [c for _, c in contrib_ordered_all]
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
    # RG2: staged voted_for edges shouldn't seed a flow response.
    stmt = select(CanonicalEdge.source_id).where(
        CanonicalEdge.target_id == bill_id,
        CanonicalEdge.relation == EdgeRelation.VOTED_FOR.value,
        published_edge(),
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
    # RG2: staged affiliation edges shouldn't expand the recipient set.
    bridged = (
        await session.execute(
            select(CanonicalEdge.source_id).where(
                CanonicalEdge.relation == EdgeRelation.AFFILIATED_WITH.value,
                CanonicalEdge.target_id.in_(yes_ids),
                published_edge(),
            )
        )
    ).scalars().all()
    recipient_ids = yes_ids | set(bridged)

    # Contribs → yes-voter recipients (sum per contributor).
    # RG2: staged contribs must not appear in public $ totals.
    contribs_stmt = (
        select(
            CanonicalEdge.source_id,
            func.sum(CanonicalEdge.weight).label("contrib_total"),
        )
        .where(
            CanonicalEdge.relation == EdgeRelation.CONTRIBUTES_TO.value,
            CanonicalEdge.target_id.in_(recipient_ids),
            published_edge(),
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
        # RG2: PAC → sponsor org attribution must not follow staged edges.
        pac_to_org = (
            await session.execute(
                select(CanonicalEdge.source_id, CanonicalEdge.target_id).where(
                    CanonicalEdge.relation == EdgeRelation.AFFILIATED_WITH.value,
                    CanonicalEdge.source_id.in_(pac_ids),
                    published_edge(),
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
        # RG2: staged contract edges must not appear in the funding-scope total.
        contracts_stmt = (
            select(
                CanonicalEdge.source_id,
                func.sum(CanonicalEdge.weight).label("contract_total"),
            )
            .where(
                CanonicalEdge.relation == EdgeRelation.HOLDS_CONTRACT.value,
                CanonicalEdge.source_id.in_(list(contribs.keys())),
                CanonicalEdge.target_id.in_(agency_ids),
                published_edge(),
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
