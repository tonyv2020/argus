"""E1 (Tony directive 2026-08-12) — model1_flow surfaces the underlying
SourceCitations for each row's contract + contribution sums.

The award-grade primary sources have always existed in the graph
(GEO Group has 200 usaspending_award citations on its holds_contract
edges + 3,800 fec_filing citations on its PAC's contributes_to edges);
the pre-E1 gap was serving — model1_flow returned only aggregate sums
+ entity ids, never the underlying SourceCitations, so
compose_argus_research could only surface argus.tonyvigna.com entity
deep-links.

These tests pin the E1 contract:

  * FlowRow exposes ``top_contract_citations`` + ``top_contribution_citations``,
    sourced from real SourceCitation rows on the SAME edges the totals
    are summed from (consistency by construction).
  * Contract citations come from ``holds_contract`` edges out of the
    entity; contribution citations come from ``contributes_to`` edges
    out of {entity ∪ its affiliated PACs} into party recipients (the
    same PAC walk that attributes contrib totals).
  * Ranked by owning-edge weight desc (biggest awards / biggest
    contributions first); deduped by URL; capped by
    ``top_citations_per_side`` (default 4).
  * Additive + back-compat: FlowRow's original fields are unchanged;
    a caller that ignores the new fields sees no behavior change.
  * The /api/flow/model1 route surfaces them under
    ``top_contract_citations`` / ``top_contribution_citations`` per row.
"""

from __future__ import annotations

import asyncio
import inspect
from dataclasses import dataclass
from typing import Any


# ─── shape: CitationRef + FlowRow carry the E1 fields ─────────────────


def test_citation_ref_dataclass_exposes_kind_url_ref() -> None:
    from app.services.flow_query import CitationRef

    c = CitationRef(kind="usaspending_award", url="https://ex.com/x", ref="AWDID")
    assert c.kind == "usaspending_award"
    assert c.url == "https://ex.com/x"
    assert c.ref == "AWDID"


def test_flow_row_defaults_are_backcompat() -> None:
    """Pre-E1 callers that construct FlowRow with the original four
    positional args must still work — top_*_citations default to empty."""
    from app.services.flow_query import FlowRow

    r = FlowRow(entity_id="e1", entity_label="GEO", contrib_total=1.0, contract_total=2.0)
    assert r.top_contract_citations == []
    assert r.top_contribution_citations == []


def test_flow_query_module_exports_e1_symbols() -> None:
    from app.services import flow_query

    assert hasattr(flow_query, "CitationRef")
    assert hasattr(flow_query, "_attach_top_citations")
    assert inspect.iscoroutinefunction(flow_query._attach_top_citations)
    # ``field(default_factory=list)`` doesn't create a class attribute
    # so use __dataclass_fields__ (the authoritative dataclass surface).
    field_names = set(flow_query.FlowRow.__dataclass_fields__.keys())
    assert "top_contract_citations" in field_names
    assert "top_contribution_citations" in field_names


# ─── model1_flow integrates citation-gather + still empties on no data ─


def test_model1_flow_empty_case_still_returns_summary_with_no_citations() -> None:
    """The empty (no-recipient) path must not touch citation-gather —
    zero-row summaries stay cheap + correct + type-safe."""
    from app.services.flow_query import FlowSummary, model1_flow

    class _E:
        def scalars(self):
            return self

        def all(self):
            return []

    class _S:
        async def execute(self, *a, **kw):
            return _E()

    summary = asyncio.run(model1_flow(_S(), party="Nope"))
    assert isinstance(summary, FlowSummary)
    assert summary.rows == []


def test_model1_flow_signature_accepts_top_citations_per_side() -> None:
    """Callers can bound the per-side cap (Cassandra uses ~4)."""
    from app.services.flow_query import model1_flow

    sig = inspect.signature(model1_flow)
    assert "top_citations_per_side" in sig.parameters


# ─── _attach_top_citations: ranking + dedupe + cap + consistency ──────


@dataclass
class _FakeEdge:
    id: str
    source_id: str
    weight: float


@dataclass
class _FakeCitation:
    edge_id: str
    kind: str
    citation_url: str
    citation_ref: str | None = None


class _StatefulScalarResult:
    def __init__(self, rows):
        self._rows = rows

    def scalars(self):
        return self

    def all(self):
        return list(self._rows)


class _StatefulRowResult:
    """Mimics a SQLAlchemy Result over row tuples (edge id, source_id, weight)."""

    def __init__(self, rows):
        self._rows = rows

    def all(self):
        return list(self._rows)

    def scalars(self):
        # scalars().all() only used for citations (SourceCitation instances)
        return _StatefulScalarResult(self._rows)


class _ScriptedSession:
    """A queue-driven fake session — each execute() pops the next
    scripted response. Lets tests control the exact response returned
    for each SELECT in the citation-gather path."""

    def __init__(self, responses):
        self._responses = list(responses)

    async def execute(self, *args, **kwargs):
        if not self._responses:
            raise AssertionError("_ScriptedSession ran out of responses")
        return self._responses.pop(0)


def test_attach_top_citations_ranks_by_edge_weight_dedupes_caps() -> None:
    """Contract-citation gather: rank by owning-edge weight desc,
    dedupe by URL, cap at top_per_side."""
    from app.services.flow_query import FlowRow, _attach_top_citations

    rows = [FlowRow(entity_id="e_geo", entity_label="GEO", contrib_total=0, contract_total=0)]

    # Contract edges out of "e_geo" — three edges, decreasing weight.
    # Edge A (weight 10) has 2 citations (one URL duplicated on edge B).
    # Edge B (weight 5) has 1 citation (dup URL — should dedupe).
    # Edge C (weight 3) has 1 citation (unique).
    contract_edges = _StatefulRowResult([
        ("edge_A", "e_geo", 10.0),
        ("edge_B", "e_geo", 5.0),
        ("edge_C", "e_geo", 3.0),
    ])
    contract_citations = _StatefulScalarResult([
        _FakeCitation("edge_A", "usaspending_award", "https://usasp/awd1"),
        _FakeCitation("edge_A", "usaspending_award", "https://usasp/awd2"),
        _FakeCitation("edge_B", "usaspending_award", "https://usasp/awd1"),  # dup
        _FakeCitation("edge_C", "usaspending_award", "https://usasp/awd3"),
    ])
    # No contribution sources → the second SELECT (contrib edges) is
    # skipped entirely (all_contrib_sources empty check). Only the
    # contract-side responses are needed.
    session = _ScriptedSession([contract_edges, contract_citations])

    asyncio.run(_attach_top_citations(
        session,
        rows,
        org_to_pacs={},  # no PACs affiliated to this entity → no contrib citations
        recipient_ids=set(),  # empty → contrib-side SELECT is skipped
        agency_relation="holds_contract",
        top_per_side=2,
    ))
    urls = [c.url for c in rows[0].top_contract_citations]
    # Ordered by edge weight desc + deduped, capped at 2:
    # awd1 (edge_A, weight 10, first) → awd2 (edge_A, weight 10, second)
    # awd1 skipped as dup; awd3 not reached (cap hit).
    assert urls == ["https://usasp/awd1", "https://usasp/awd2"]
    # Contribution citations empty (no PACs, no recipients).
    assert rows[0].top_contribution_citations == []


def test_attach_top_citations_uses_affiliated_pac_walk_for_contribs() -> None:
    """Contribution-citation gather: sources are {entity ∪ affiliated
    PACs}, target_id restricted to recipient_ids, kind matches whatever
    the graph stored (fec_filing in real life). Ranking is by owning-
    edge weight desc across ALL sources — biggest receipts first
    regardless of which PAC they were on."""
    from app.services.flow_query import FlowRow, _attach_top_citations

    rows = [FlowRow(entity_id="e_geo", entity_label="GEO", contrib_total=0, contract_total=0)]

    # First SELECT: contract edges (empty for this test).
    contract_edges = _StatefulRowResult([])
    # NOTE: with empty contract edges the code skips the second SELECT
    # for contract citations. Then contribution SELECT.
    contrib_edges = _StatefulRowResult([
        # pac_1 wrote a $500 check to a recipient → high weight, high rank
        ("edge_1", "pac_1", 500.0),
        # pac_2 wrote a $200 check → mid
        ("edge_2", "pac_2", 200.0),
        # pac_1 wrote a $100 check → low
        ("edge_3", "pac_1", 100.0),
    ])
    contrib_citations = _StatefulScalarResult([
        _FakeCitation("edge_1", "fec_filing", "https://fec/big"),
        _FakeCitation("edge_2", "fec_filing", "https://fec/mid"),
        _FakeCitation("edge_3", "fec_filing", "https://fec/small"),
    ])
    session = _ScriptedSession([contract_edges, contrib_edges, contrib_citations])

    asyncio.run(_attach_top_citations(
        session,
        rows,
        org_to_pacs={"e_geo": {"pac_1", "pac_2"}},
        recipient_ids={"r_congress_member_a"},
        agency_relation="holds_contract",
        top_per_side=3,
    ))
    urls = [c.url for c in rows[0].top_contribution_citations]
    # Sorted by edge weight desc across ALL PACs, deduped, capped:
    assert urls == ["https://fec/big", "https://fec/mid", "https://fec/small"]
    # Kinds carry through verbatim.
    kinds = {c.kind for c in rows[0].top_contribution_citations}
    assert kinds == {"fec_filing"}


def test_attach_top_citations_only_bounded_top_per_side() -> None:
    from app.services.flow_query import FlowRow, _attach_top_citations

    rows = [FlowRow(entity_id="e", entity_label="X", contrib_total=0, contract_total=0)]

    contract_edges = _StatefulRowResult([
        ("edge_1", "e", 10.0), ("edge_2", "e", 9.0),
        ("edge_3", "e", 8.0), ("edge_4", "e", 7.0),
        ("edge_5", "e", 6.0), ("edge_6", "e", 5.0),
    ])
    contract_citations = _StatefulScalarResult([
        _FakeCitation(f"edge_{i}", "usaspending_award", f"https://a/{i}")
        for i in range(1, 7)
    ])
    session = _ScriptedSession([contract_edges, contract_citations])

    asyncio.run(_attach_top_citations(
        session, rows, org_to_pacs={}, recipient_ids=set(),
        agency_relation="holds_contract", top_per_side=4,
    ))
    assert len(rows[0].top_contract_citations) == 4
    # Top-4 by weight — edges 1..4.
    assert [c.url for c in rows[0].top_contract_citations] == [
        "https://a/1", "https://a/2", "https://a/3", "https://a/4",
    ]


def test_attach_top_citations_empty_url_skipped() -> None:
    """SourceCitation.citation_url is NOT NULL in the schema but defensive
    dedupe skips empty strings without raising."""
    from app.services.flow_query import FlowRow, _attach_top_citations

    rows = [FlowRow(entity_id="e", entity_label="X", contrib_total=0, contract_total=0)]

    contract_edges = _StatefulRowResult([("edge_1", "e", 10.0), ("edge_2", "e", 5.0)])
    contract_citations = _StatefulScalarResult([
        _FakeCitation("edge_1", "usaspending_award", ""),  # empty — should skip
        _FakeCitation("edge_2", "usaspending_award", "https://a/keep"),
    ])
    session = _ScriptedSession([contract_edges, contract_citations])
    asyncio.run(_attach_top_citations(
        session, rows, org_to_pacs={}, recipient_ids=set(),
        agency_relation="holds_contract", top_per_side=3,
    ))
    urls = [c.url for c in rows[0].top_contract_citations]
    assert urls == ["https://a/keep"]


# ─── API surface: /api/flow/model1 emits the new per-row fields ───────


def test_api_flow_model1_route_emits_top_citations_shape() -> None:
    """Grep-level guard: the route builder must emit both new field
    names verbatim so hollywood's client can parse them by key."""
    from app import main

    src = inspect.getsource(main)
    # The route builder for /api/flow/model1 lives in flow_model1().
    assert "top_contract_citations" in src
    assert "top_contribution_citations" in src


# ─── back-compat: existing model1 tests still pass (unmodified) ───────


def test_pre_e1_flow_row_construction_still_works() -> None:
    """A caller that constructs FlowRow without the new fields must
    still work (positional args unchanged, new fields have defaults)."""
    from app.services.flow_query import FlowRow

    r = FlowRow(entity_id="e", entity_label="L", contrib_total=1.0, contract_total=2.0)
    # Original attrs preserved.
    assert r.entity_id == "e" and r.contrib_total == 1.0
    # New attrs default to empty.
    assert r.top_contract_citations == [] and r.top_contribution_citations == []
