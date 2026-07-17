"""Smoke tests — import the app + a couple of grounding invariants."""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app
from app.models import EdgeRelation, EntityType, SourceKind
from app.services.graph.base import normalize_name


def test_app_imports_and_health_endpoint_is_registered() -> None:
    """Prove the module graph loads and /health is wired via include_router chain."""
    paths = {r.path for r in app.routes}
    assert "/health" in paths
    assert "/api/entities/{canonical_id}" in paths
    assert "/api/entities/{canonical_id}/subgraph" in paths


def test_health_endpoint_returns_ok() -> None:
    """Hit the endpoint the k8s readiness probe will hit."""
    with TestClient(app) as client:
        r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("The GEO Group, Inc.", "the geo group"),
        ("GEO", "geo"),
        ("GEO Group", "geo group"),
        ("Acme Corporation", "acme"),
        ("Acme Corp.", "acme"),
        ("Beta   LLC", "beta"),
        # non-ascii + accents collapse
        ("Éric Dupont", "eric dupont"),
    ],
)
def test_normalize_name_strips_suffixes_and_accents(raw: str, expected: str) -> None:
    """`normalize_name` must be deterministic + strip legal suffixes and diacritics."""
    assert normalize_name(raw) == expected


def test_edge_relation_covers_design_relations() -> None:
    """Design §4 lists MENTIONED_WITH / CONTRIBUTES_TO / HOLDS_CONTRACT / LOBBIES — all present."""
    values = {e.value for e in EdgeRelation}
    for expected in ("mentioned_with", "contributes_to", "holds_contract", "lobbies"):
        assert expected in values


def test_entity_type_covers_design_types() -> None:
    """Design §4 names Person / Org / PAC / Agency / Candidate / Place — all present."""
    values = {t.value for t in EntityType}
    for expected in ("person", "organization", "pac", "agency", "candidate", "place"):
        assert expected in values


def test_source_kind_covers_design_sources() -> None:
    """Design §4 sources: FEC / USAspending / LDA / article permalink — all present."""
    values = {s.value for s in SourceKind}
    for expected in (
        "article_permalink",
        "fec_filing",
        "usaspending_award",
        "senate_lda",
    ):
        assert expected in values
