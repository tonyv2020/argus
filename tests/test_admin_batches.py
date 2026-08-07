"""RG5 (2026-08-07) — admin publish/unpublish + scrutiny precondition.

Covers gate cases (d), (e), (f) from the design doc and the token-gate
auth checks that also fire on cases (a)+(g). Behavioural round-trip
against a real DB is exercised on rollout by the live-proof script
(see twin-bus RG4 sign-off); this file locks the wiring + auth
short-circuits + response shapes so a rewrite can't silently regress
the contract.
"""

from __future__ import annotations

import inspect

from fastapi.testclient import TestClient

from app.main import (
    admin_publish_batch,
    admin_unpublish_batch,
    app,
    require_service_token,
)


# ─── Route registration ─────────────────────────────────────────────


def test_admin_publish_batch_route_is_registered() -> None:
    """/api/admin/batches/{batch_id}/publish must be wired as a POST."""
    routes = {(r.path, tuple(sorted(r.methods))) for r in app.routes if hasattr(r, "methods")}
    assert ("/api/admin/batches/{batch_id}/publish", ("POST",)) in routes


def test_admin_unpublish_batch_route_is_registered() -> None:
    routes = {(r.path, tuple(sorted(r.methods))) for r in app.routes if hasattr(r, "methods")}
    assert ("/api/admin/batches/{batch_id}/unpublish", ("POST",)) in routes


# ─── Token gate (401 short-circuits before any DB touch) ─────────────


def test_publish_without_header_returns_401() -> None:
    """Case (g) auth check: a call with no X-Argus-Service-Token must
    401 before any DB touch. This is what makes RG4 usable from CI
    without a real Postgres."""
    with TestClient(app) as c:
        r = c.post("/api/admin/batches/some-batch/publish")
    assert r.status_code == 401
    assert r.json() == {"detail": "unauthorized"}


def test_unpublish_without_header_returns_401() -> None:
    with TestClient(app) as c:
        r = c.post("/api/admin/batches/some-batch/unpublish")
    assert r.status_code == 401
    assert r.json() == {"detail": "unauthorized"}


def test_publish_with_wrong_header_returns_401(monkeypatch) -> None:
    """A mismatched token is treated identically to a missing one — no
    length-hint / prefix-hint leakage; identical body."""
    from app import main

    monkeypatch.setattr(main.settings, "argus_service_token", "correct-token")
    with TestClient(app) as c:
        r = c.post(
            "/api/admin/batches/foo/publish",
            headers={"X-Argus-Service-Token": "wrong-token"},
        )
    assert r.status_code == 401
    assert r.json() == {"detail": "unauthorized"}


def test_publish_with_unset_server_token_refuses_even_with_header(monkeypatch) -> None:
    """Fail-closed default: cluster deployed with no
    ``ARGUS_SERVICE_TOKEN`` refuses every admin call, even when the
    caller sends a header."""
    from app import main

    monkeypatch.setattr(main.settings, "argus_service_token", "")
    with TestClient(app) as c:
        r = c.post(
            "/api/admin/batches/foo/publish",
            headers={"X-Argus-Service-Token": "anything-goes"},
        )
    assert r.status_code == 401


# ─── require_service_token dependency shape ─────────────────────────


def test_require_service_token_is_a_dependency() -> None:
    """The auth helper is a plain async callable used with
    ``fastapi.Depends``. If someone deletes the ``Header`` default
    the endpoint stops enforcing the gate."""
    sig = inspect.signature(require_service_token)
    assert "x_argus_service_token" in sig.parameters


# ─── Publish handler shape (case d, f) ───────────────────────────────


def test_publish_handler_returns_shape_matches_spec() -> None:
    """Response contract: ``{batch_id, edges_published, entities_published}``.
    Locked by parsing the handler source for those three keys."""
    src = inspect.getsource(admin_publish_batch)
    assert '"batch_id"' in src
    assert '"edges_published"' in src
    assert '"entities_published"' in src


def test_publish_handler_checks_scrutiny_precondition() -> None:
    """Case (f): 409 body carries ``reason=scrutiny_incomplete`` + the
    count + a bounded list of missing canonical ids."""
    src = inspect.getsource(admin_publish_batch)
    assert "_batch_entities_missing_scrutiny" in src
    assert "409" in src
    assert '"reason": "scrutiny_incomplete"' in src
    assert '"missing_scrutiny_count"' in src


def test_publish_handler_does_not_leak_missing_names() -> None:
    """The 409 detail body carries IDs but NOT canonical_names — a
    suppressed person's name would defeat the whole privacy stack
    if it slipped into an error body."""
    src = inspect.getsource(admin_publish_batch)
    # `missing[0]` is the id; `missing[1]` is the name — never taken.
    # Enforce that only ``m[0]`` is used, and no ``m[1]`` slice appears.
    assert "m[0]" in src
    assert "m[1]" not in src


def test_publish_handler_updates_edges_and_entities_where_staged() -> None:
    """The UPDATEs must filter on both ``batch_id`` AND
    ``publication_state='staged'`` — idempotent re-publish flips zero
    rows (not "flips everything again")."""
    src = inspect.getsource(admin_publish_batch)
    assert "canonical_edges" in src and "canonical_entities" in src
    assert "publication_state='published'" in src
    assert "publication_state='staged'" in src


def test_publish_handler_logs_counts() -> None:
    """Spec: log both ops with counts. Structured log line contains
    batch_id + edge count + entity count."""
    src = inspect.getsource(admin_publish_batch)
    assert "logger.info(" in src
    assert '"RG4 admin_publish_batch batch_id=%s edges=%d entities=%d"' in src


# ─── Unpublish handler shape (case e) ────────────────────────────────


def test_unpublish_handler_returns_expected_shape() -> None:
    """``{batch_id, edges_unpublished, entities_unpublished}``."""
    src = inspect.getsource(admin_unpublish_batch)
    assert '"batch_id"' in src
    assert '"edges_unpublished"' in src
    assert '"entities_unpublished"' in src


def test_unpublish_handler_has_no_scrutiny_precondition() -> None:
    """Unpublish is a safety kill-switch — must always be allowed to
    hide, even before scrutiny."""
    src = inspect.getsource(admin_unpublish_batch)
    assert "_batch_entities_missing_scrutiny" not in src
    assert "scrutiny_incomplete" not in src


def test_unpublish_handler_updates_where_published() -> None:
    """Symmetric to publish: only flip currently-published rows,
    keep the op idempotent."""
    src = inspect.getsource(admin_unpublish_batch)
    assert "publication_state='staged'" in src
    assert "publication_state='published'" in src


def test_unpublish_handler_logs_counts() -> None:
    src = inspect.getsource(admin_unpublish_batch)
    assert "logger.info(" in src
    assert '"RG4 admin_unpublish_batch batch_id=%s edges=%d entities=%d"' in src
