"""P5.6 — Model 2 beneficiary flow query shape tests.

Hermetic — checks the module surface (rows, summary, funding-scope
config, endpoint bill→scope map). Live behavior is validated post-deploy
by hitting the endpoint.
"""

from __future__ import annotations

import inspect

from app.services import flow_query


def test_model2_flow_entrypoint_exists() -> None:
    assert hasattr(flow_query, "model2_flow")
    assert inspect.iscoroutinefunction(flow_query.model2_flow)
    assert hasattr(flow_query, "Model2Summary")
    assert hasattr(flow_query, "Model2Row")


def test_bill_funding_scope_config_present() -> None:
    """OBBB (119-hr-1) MUST have a curated funding scope so Model 2
    can attribute contract dollars into a bill-relevant window."""
    scope = flow_query.BILL_FUNDING_SCOPE
    assert "119-hr-1" in scope
    agencies, note = scope["119-hr-1"]
    joined = " ".join(agencies).upper()
    # Detention beat + Musk beat both in OBBB scope per helen's spec.
    assert "IMMIGRATION AND CUSTOMS ENFORCEMENT" in joined
    assert "NATIONAL AERONAUTICS AND SPACE" in joined or "NASA" in joined
    assert "DEFENSE" in joined


def test_model2_summary_carries_funding_scope_note() -> None:
    """The funding-scope note MUST land on the response so downstream
    audit sees which scope the contract $ was filtered on (spec §5)."""
    fields = {f.name for f in flow_query.Model2Summary.__dataclass_fields__.values()}
    assert "funding_scope_note" in fields
    assert "n_yes_voters" in fields
    assert "yes_voter_party_filter" in fields


def test_model2_flow_walks_voted_for_edges() -> None:
    """The traversal starts at BILL → members via VOTED_FOR — NOT
    VOTED_AGAINST + NOT unfiltered. Guard the source-of-truth query."""
    src = inspect.getsource(flow_query.model2_flow)
    assert "_yes_voter_ids_for_bill" in src

    yes = inspect.getsource(flow_query._yes_voter_ids_for_bill)
    assert "VOTED_FOR" in yes
    # NEVER traverse VOTED_AGAINST in Model 2 (that'd invert the beneficiary
    # semantic).
    assert "VOTED_AGAINST" not in yes


def test_model2_endpoint_accepts_bill_short_slug() -> None:
    """The endpoint takes ``bill=119-hr-1`` OR ``bill=OBBB`` so operators
    can hit it without remembering the alias key."""
    resolve = inspect.getsource(flow_query._resolve_bill)
    # Alias lookup first (deterministic), then human short-name fallback.
    assert "congress.bill" in resolve
    assert "canonical_name" in resolve


def test_model2_attributes_pac_contribs_to_sponsor_org() -> None:
    """Same shape as Model 1 — a company's contributions flow through
    its PAC; the attribution rewrites the row so contracts join
    correctly."""
    src = inspect.getsource(flow_query.model2_flow)
    assert "AFFILIATED_WITH" in src
    assert "contribs.pop" in src


def test_model2_excludes_congress_member_intermediaries() -> None:
    """Congress members surface as CONTRIBUTES_TO sources via bridge
    aliasing; they aren't real corporate beneficiaries. Filter them
    out (same rule as Model 1)."""
    src = inspect.getsource(flow_query.model2_flow)
    assert '"bioguide"' in src or "'bioguide'" in src
