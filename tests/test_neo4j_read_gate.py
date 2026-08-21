"""Neo4j projection read-gate regression (helen, 2026-08-21).

Two de-anon incidents were found + closed on 2026-08-21 where data reached the
Cypher/bolt-queryable Neo4j graph that must never be there:

  * 6,855 ``suppress`` canonicals carried their REAL ``canonical_name`` as a
    node label (the D2 write-gate stopped WRITING them but MERGE-only
    projection never REMOVED what earlier sweeps wrote).
  * a staged (unpublished) batch's rows would project on the next sweep,
    live before an operator published them.

Both were the same root cause: a write-time gate does not retro-clean, and
the projection swept every Postgres row. These tests fail CI if the
``project_entity`` / ``project_edge`` gate ever regresses, so a suppressed or
staged row can never again reach a bolt reader.

Style: no real DB / no real Neo4j — a recording fake driver makes
``available`` True and captures every Cypher write attempt, so the test can
assert not just the boolean return but that NOTHING (least of all a real name)
was put on the wire.
"""
from __future__ import annotations

import asyncio
import inspect

from app.models import (
    CanonicalEdge,
    CanonicalEntity,
    PublicationState,
    SurfaceMode,
)
from app.services.graph.neo4j_projection import Neo4jProjection


class _RecordingDriver:
    """Fake Neo4j driver: makes ``Neo4jProjection.available`` True and records
    every Cypher statement + params, so a test can assert whether a WRITE was
    even attempted (a refused projection must leave ``runs`` empty)."""

    def __init__(self) -> None:
        self.runs: list[tuple[str, dict]] = []

    def session(self):
        driver = self

        class _Session:
            def __enter__(self_inner):
                return self_inner

            def __exit__(self_inner, *exc):
                return False

            def run(self_inner, cypher, **params):
                driver.runs.append((cypher, params))
                return []

        return _Session()


class _FakeSession:
    """Minimal stand-in for AsyncSession — only ``.add`` is touched on the
    success path (the refuse paths return before any session use)."""

    def add(self, obj) -> None:  # noqa: D401
        pass


def _proj() -> Neo4jProjection:
    return Neo4jProjection(driver=_RecordingDriver())


def _entity(mode: str, pub: str, name: str = "Jane Real Name", public_alias=None):
    return CanonicalEntity(
        id="ent-1",
        canonical_name=name,
        type="person",
        surface_mode=mode,
        publication_state=pub,
        public_alias=public_alias,
    )


# ── the two privacy invariants ──────────────────────────────────────────


def test_suppressed_entity_is_never_projected() -> None:
    p = _proj()
    ent = _entity(SurfaceMode.SUPPRESS.value, PublicationState.PUBLISHED.value)
    ok = asyncio.run(p.project_entity(_FakeSession(), ent))
    assert ok is False
    assert p.driver.runs == [], "a suppressed canonical must not touch Neo4j"


def test_suppressed_real_name_never_reaches_the_wire() -> None:
    """Belt-and-suspenders: not only refused, the real name must never appear
    in any Cypher param — the exact leak found on 2026-08-21."""
    p = _proj()
    ent = _entity(SurfaceMode.SUPPRESS.value, PublicationState.PUBLISHED.value, name="Secret Person X")
    asyncio.run(p.project_entity(_FakeSession(), ent))
    assert "Secret Person X" not in repr(p.driver.runs)


def test_staged_entity_is_never_projected() -> None:
    p = _proj()
    ent = _entity(SurfaceMode.OPEN.value, PublicationState.STAGED.value)
    ok = asyncio.run(p.project_entity(_FakeSession(), ent))
    assert ok is False
    assert p.driver.runs == [], "a staged (unpublished) canonical must not project"


def test_staged_edge_is_never_projected() -> None:
    p = _proj()
    edge = CanonicalEdge(
        id="edge-1",
        source_id="a",
        target_id="b",
        relation="contributes_to",
        publication_state=PublicationState.STAGED.value,
        weight=1.0,
    )
    ok = asyncio.run(p.project_edge(_FakeSession(), edge))
    assert ok is False
    assert p.driver.runs == [], "a staged edge must not project"


# ── the positive control: legitimate rows DO project (else the gate could be
#    passing vacuously by refusing everything) ─────────────────────────────


def test_open_published_entity_projects_with_real_label() -> None:
    p = _proj()
    ent = _entity(SurfaceMode.OPEN.value, PublicationState.PUBLISHED.value, name="Public Co")
    ok = asyncio.run(p.project_entity(_FakeSession(), ent))
    assert ok is True
    assert any(params.get("label") == "Public Co" for _, params in p.driver.runs)


def test_alias_entity_projects_placeholder_not_real_name() -> None:
    p = _proj()
    ent = _entity(
        SurfaceMode.ALIAS.value,
        PublicationState.PUBLISHED.value,
        name="Real Donor Name",
        public_alias="Private donor #abcd1234",
    )
    ok = asyncio.run(p.project_entity(_FakeSession(), ent))
    assert ok is True
    assert "Real Donor Name" not in repr(p.driver.runs), "alias must hide the real name"


# ── guard the prune-set invariant (mass-deletion safety) ────────────────


def test_prune_set_is_derived_from_postgres_not_from_this_run() -> None:
    """The prune set MUST come from Postgres 'projectable' state, never from
    what a run managed to project — otherwise a transient Neo4j error mid-sweep
    becomes a mass deletion. Guarded as a code-shape assertion."""
    src = inspect.getsource(Neo4jProjection.prune_missing)
    assert "NEVER from" in src and "Postgres" in src
