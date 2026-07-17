"""/api/resolve invariants — the-dailies chit integration."""

from __future__ import annotations

from fastapi.testclient import TestClient

from app.main import app


def test_resolve_endpoint_registered() -> None:
    """`/api/resolve` is wired."""
    paths = {r.path for r in app.routes}
    assert "/api/resolve" in paths


def test_resolve_empty_tag_returns_422() -> None:
    """A blank tag must 422 (validation) rather than resolve to anything."""
    with TestClient(app) as c:
        r = c.get("/api/resolve", params={"tag": ""})
    assert r.status_code == 422
