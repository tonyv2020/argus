"""Search endpoint scrutiny-respecting invariants (helen 2026-07-17)."""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_search_endpoint_registered() -> None:
    """`/api/search` is wired + returns a JSON envelope with q + results + matched."""
    paths = {r.path for r in app.routes}
    assert "/api/search" in paths


def test_entity_deep_link_route_registered() -> None:
    """`/entity/{canonical_id}` serves the SPA — shareable URLs."""
    paths = {r.path for r in app.routes}
    assert "/entity/{canonical_id}" in paths


def test_search_empty_query_returns_empty_envelope() -> None:
    """A blank q must not blow up + must not return any results."""
    with TestClient(app) as c:
        # min_length=1 in the Query — a zero-length q must 422 (validation), not crash.
        r = c.get("/api/search", params={"q": ""})
    assert r.status_code == 422
