"""RG5 (2026-08-07) — read-gate hardening invariants.

Covers the seven gate cases from the design doc (helen-k3s/docs/
argus-read-gate-hardening-design.md):

(a) staged entity → /api/search omits it AND /api/entities/{id} 404
(b) published node with staged edges → dossier returns only published
    edges; degree + prominence exclude staged.
(c) /api/entities/{id}/subgraph + /api/flow/model1 + /api/flow/model2
    exclude staged rows.
(d) publish flips staged → published (RG4 admin op — covered by
    ``test_admin_batches``).
(e) unpublish rolls back (same).
(f) publish REFUSED 409 when a batch entity has no scrutiny_decision
    (same).
(g) ``include_staged=1`` WITHOUT the service-token header is silently
    ignored.

Style follows the rest of the suite: structural code-shape checks +
FastAPI route inspection + TestClient short-circuits that don't
require a real DB. Full behavioural round-trip (SQL + HTTP) is
exercised by the live-proof scripts run on rollout — see the
twin-bus RG2/RG3/RG4 sign-offs.
"""

from __future__ import annotations

import inspect

from fastapi.testclient import TestClient

from app.main import app, _preview_ok
from app.models import PublicationState
from app.services import flow_query
from app.services.graph import pgvector_store
from app.services.ingest import disclosure_emit
from app.services.read_gate import (
    is_published_entity,
    maybe_published_edge,
    maybe_published_entity,
    published_edge,
    published_entity,
)


# ─── Model / enum invariants ─────────────────────────────────────────


def test_publication_state_covers_published_and_staged_only() -> None:
    """The lifecycle vocab is a closed set of exactly two values —
    fabricating a third (``draft``, ``pending``, …) would silently
    slip past the read-path filter."""
    values = {p.value for p in PublicationState}
    assert values == {"published", "staged"}


def test_column_default_is_published_not_staged() -> None:
    """The migration + ORM server_default MUST be ``published`` — the
    split-default gotcha. If someone flips this to ``staged`` the whole
    corpus goes dark on next deploy."""
    from app.models import CanonicalEdge, CanonicalEntity

    ent_col = CanonicalEntity.__table__.c.publication_state
    edge_col = CanonicalEdge.__table__.c.publication_state
    assert ent_col.server_default.arg == "published"
    assert edge_col.server_default.arg == "published"
    assert ent_col.nullable is False
    assert edge_col.nullable is False


# ─── Case (a) — staged entity: search omits + dossier 404 ────────────


def test_search_handler_filters_by_published_entity_on_every_query() -> None:
    """All THREE candidate queries in /api/search must gate on
    ``published_entity()`` (via the preview-aware wrapper). A missed
    query would leak staged entities to the public read path."""
    from app import main

    src = inspect.getsource(main.search)
    # The shared preview-aware gate is used in place of a raw predicate.
    assert "maybe_published_entity" in src
    # Applied to all three candidate blocks (open_name / open_alias / aliased).
    assert src.count(".where(pub_entity)") == 3


def test_entity_handler_404s_on_staged() -> None:
    """/api/entities/{id} must 404 when the target is staged (before
    revealing anything about its shape). The 404 comes from a call to
    ``is_published_entity`` — the greppable pivot point."""
    from app import main

    src = inspect.getsource(main.get_entity)
    assert "is_published_entity(ent)" in src
    # The 404 for staged must be behind the preview gate — preview
    # callers see it, public callers don't.
    assert "not preview and not is_published_entity(ent)" in src


# ─── Case (b) — published node's dossier excludes staged edges ───────


def test_dossier_edges_pull_filters_by_published_edge() -> None:
    """Even when the dossier target is published, staged edges (mid-
    batch bulk ingest) must NOT surface in ``connections``."""
    from app import main

    src = inspect.getsource(main.get_entity)
    assert "maybe_published_edge(preview)" in src


def test_entity_importance_filters_by_published_edge_both_queries() -> None:
    """Degree + citation counts feed search ranking. Staged edges must
    not inflate either — see spec §RG2.5."""
    from app import main

    src = inspect.getsource(main._entity_importance)
    # Two count queries (edge_count + citation_count), both filtered.
    assert src.count("published_edge()") == 2


# ─── Case (c) — subgraph + both flow models exclude staged ───────────


def test_subgraph_endpoint_404s_staged_anchor_and_hands_off() -> None:
    """The subgraph handler must return an empty graph for a staged
    anchor (same dark-until-published contract as the dossier 404) and
    then delegate to the store, which enforces the edge/node filter."""
    from app import main

    src = inspect.getsource(main.get_entity_subgraph)
    assert "is_published_entity(ent)" in src
    assert "empty_graph()" in src


def test_pgvector_store_subgraph_filters_by_published_edge_and_entity() -> None:
    """``PgVectorStore.get_entity_subgraph`` must gate the outbound +
    inbound BFS edge pulls, the final edge pull, AND the node pull —
    plus drop dangling edges whose endpoints were filtered out."""
    src = inspect.getsource(pgvector_store.PgVectorStore.get_entity_subgraph)
    # Outbound + inbound + final edge pull = 3 published_edge() gates.
    assert src.count("published_edge()") == 3
    # Node pull filtered by published_entity() too (belt-and-suspenders).
    assert src.count("published_entity()") == 1
    # Dangling-edge guard drops edges whose endpoint is missing from
    # the (filtered) node set.
    assert "node_id_set" in src
    assert "e.source_id in node_id_set and e.target_id in node_id_set" in src


def test_model1_flow_filters_every_canonical_edge_select() -> None:
    """Model 1 has FOUR CanonicalEdge reads that surface $: party-committee
    bridge + contribs + PAC→org attribution + contracts. All must gate
    on ``published_edge()``."""
    src = inspect.getsource(flow_query.model1_flow)
    # 3 in model1_flow itself (contribs, PAC→org, contracts).
    assert src.count("published_edge()") == 3
    # The 4th — the bridge — lives in the helper.
    bridge_src = inspect.getsource(flow_query._party_recipient_ids)
    assert "published_edge()" in bridge_src


def test_model2_flow_filters_every_canonical_edge_select() -> None:
    """Model 2 has FIVE CanonicalEdge reads: voted_for lookup +
    affiliated_with bridge + contribs + PAC→org + contracts."""
    src = inspect.getsource(flow_query.model2_flow)
    # 4 in model2_flow itself (bridge + contribs + pac_to_org + contracts_stmt).
    assert src.count("published_edge()") == 4
    # The 5th — voted_for — lives in the helper.
    yes_src = inspect.getsource(flow_query._yes_voter_ids_for_bill)
    assert "published_edge()" in yes_src


# ─── Case (g) — preview flag ignored without service token ───────────


def test_preview_ok_returns_false_when_server_token_unset(monkeypatch) -> None:
    """A cluster deployed WITHOUT ``ARGUS_SERVICE_TOKEN`` must never
    honour the preview flag, even if the caller sends a header — the
    fail-closed default keeps a misconfigured deploy safe."""
    from app import main

    monkeypatch.setattr(main.settings, "argus_service_token", "")
    assert _preview_ok("any-value") is False
    assert _preview_ok(None) is False


def test_preview_ok_returns_false_on_header_mismatch(monkeypatch) -> None:
    """When the server IS configured but the caller sends a wrong
    header (or none), preview stays off."""
    from app import main

    monkeypatch.setattr(main.settings, "argus_service_token", "correct-token")
    assert _preview_ok(None) is False
    assert _preview_ok("wrong-token") is False
    assert _preview_ok("correct-token") is True


def test_search_signature_carries_include_staged_and_header() -> None:
    """The endpoint must accept BOTH the flag AND the header so the
    preview downgrade path exists at the FastAPI layer."""
    from app import main

    sig = inspect.signature(main.search)
    params = sig.parameters
    assert "include_staged" in params
    assert "x_argus_service_token" in params


def test_entity_signature_carries_include_staged_and_header() -> None:
    """Same for the dossier endpoint."""
    from app import main

    sig = inspect.signature(main.get_entity)
    params = sig.parameters
    assert "include_staged" in params
    assert "x_argus_service_token" in params


def test_maybe_helpers_are_pass_through_when_preview_true() -> None:
    """The preview-aware wrappers become no-op ``true()`` predicates
    when preview is authorized — the SQL then reduces to the surface_mode
    + relation filters alone."""
    from sqlalchemy import true

    pe_true = maybe_published_entity(True)
    pe_false = maybe_published_entity(False)
    ee_true = maybe_published_edge(True)
    ee_false = maybe_published_edge(False)

    # ``true()`` is a distinct clause; ``published_entity()`` is a
    # binary comparison. Compare the compiled SQL fragments as a
    # cheap proxy.
    assert str(pe_true) == str(true())
    assert str(ee_true) == str(true())
    assert str(pe_false) != str(true())
    assert str(ee_false) != str(true())


# ─── Cross-cutting: emit stampers wire batch_id through every call ───


def test_emit_signature_requires_batch_id() -> None:
    """The RG3 discipline: caller stamps ``batch_id``; the function
    never computes one internally. The kwarg is required + validated."""
    sig = inspect.signature(disclosure_emit.emit_edges_for_doc)
    params = sig.parameters
    assert "batch_id" in params
    assert params["batch_id"].default is inspect.Parameter.empty


def test_emit_raises_valueerror_on_missing_batch_id() -> None:
    """Explicit fail-loud when a caller forgets to stamp — better than
    silently minting a new canonical without a batch grouping tag."""
    import asyncio

    async def _run() -> None:
        await disclosure_emit.emit_edges_for_doc("dead-doc-id", batch_id="")

    try:
        asyncio.run(_run())
    except ValueError as exc:
        assert "batch_id is required" in str(exc)
    else:
        raise AssertionError("emit_edges_for_doc(batch_id='') must raise ValueError")


def test_resolve_or_create_stamps_staged_on_net_new(monkeypatch) -> None:
    """A brand-new canonical MUST land staged + carrying the batch_id;
    a cache hit / DB hit MUST NOT be touched."""
    src = inspect.getsource(disclosure_emit._resolve_or_create)
    # NEW-canonical branch stamps staged + batch_id.
    assert "PublicationState.STAGED.value" in src
    assert "batch_id=batch_id" in src
    # Docstring is explicit about pre-existing rows not being touched.
    assert "PRE-EXISTING" in src or "NOT touched" in src


def test_upsert_edge_stamps_staged_on_net_new_only() -> None:
    """Same discipline for edges: new → staged + batch_id; merging
    into an existing edge leaves publication_state + batch_id alone
    (an already-published edge stays published)."""
    src = inspect.getsource(disclosure_emit._upsert_edge)
    assert "PublicationState.STAGED.value" in src
    # Merge branch must NOT touch publication_state (comment guard).
    assert "left untouched" in src or "NOT change either" in src


# ─── read_gate module surface is stable ─────────────────────────────


def test_read_gate_exports_all_five_symbols() -> None:
    """Every public read-path handler imports from this ONE module —
    the grep audit relies on the surface staying tight."""
    assert callable(published_entity)
    assert callable(published_edge)
    assert callable(is_published_entity)
    assert callable(maybe_published_entity)
    assert callable(maybe_published_edge)


def test_public_gate_uses_published_string_literal() -> None:
    """Guards against a rename that would silently invert the gate."""
    src = inspect.getsource(published_entity)
    assert '"published"' in src or "PUBLISHED" in src


# ─── RG1 × Neo4j projection (P1.5, 2026-08-21) ──────────────────────────


def test_projection_excludes_staged_rows() -> None:
    """The read gate has to hold in the PROJECTION too.

    `publication_state` gates the API read path, but the Neo4j
    projection swept every row in Postgres — so a staged batch would
    land in a Cypher-reachable graph before anyone published it, exactly
    the bypass the D2 suppress gate closes for privacy. Staged rows are
    excluded from the projectable set (and therefore pruned if an older
    sweep wrote them), and project on the sweep after publish.
    """
    from app.services.ingest.project_to_neo4j import (
        projectable_edge_ids,
        projectable_entity_ids,
    )

    class _Ent:
        def __init__(self, eid, mode="open", state="published"):
            self.id, self.surface_mode, self.publication_state = eid, mode, state

    class _Edge:
        def __init__(self, eid, state="published"):
            self.id, self.publication_state = eid, state

    entities = [
        _Ent("live"),
        _Ent("aliased", "alias"),
        _Ent("suppressed", "suppress"),
        _Ent("staged", "open", "staged"),
        _Ent("staged_and_suppressed", "suppress", "staged"),
    ]
    assert projectable_entity_ids(entities) == {"live", "aliased"}
    assert projectable_edge_ids(
        [_Edge("e_live"), _Edge("e_staged", "staged")]
    ) == {"e_live"}


def test_project_entity_refuses_a_staged_canonical() -> None:
    """Defense-in-depth: the gate lives in the projection layer too, so a
    caller that builds its own sweep still cannot write a staged row."""
    import inspect

    from app.services.graph.neo4j_projection import Neo4jProjection

    src = inspect.getsource(Neo4jProjection.project_entity)
    assert "PublicationState.STAGED.value" in src
    edge_src = inspect.getsource(Neo4jProjection.project_edge)
    assert "PublicationState.STAGED.value" in edge_src
