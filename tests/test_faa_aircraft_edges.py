"""P2 — REGISTERS edge fence + structural citation.

The P1 fence tests guard the asset rows. These guard the *claims* made
about entities from them, which is the part that can defame someone if
it is wrong or leak if it surfaces.
"""

from __future__ import annotations

import inspect

from app.models import AircraftRegistrationEdge as Edge
from app.services.ingest import faa_aircraft_stage_edges as stage


def _checks() -> dict[str, str]:
    return {
        c.name: str(c.sqltext) for c in Edge.__table__.constraints if hasattr(c, "sqltext")
    }


def test_edge_fence_is_pinned_closed() -> None:
    """Same fence as the asset rows — nothing here surfaces in P2."""
    checks = _checks()
    assert "suppress" in checks["ck_aircraft_reg_edge_suppress"]
    assert "staged" in checks["ck_aircraft_reg_edge_staged"]


def test_an_uncited_edge_is_unrepresentable() -> None:
    """The 0-uncited invariant, enforced by the schema instead of by a
    sweep that finds violations only after they exist."""
    cols = Edge.__table__.columns
    for name in ("snapshot_id", "source_url", "source_sha256"):
        assert cols[name].nullable is False, f"{name} must be NOT NULL"
    # NOT NULL alone would accept '' as a citation.
    assert "ck_aircraft_reg_edge_cited" in _checks()


def test_relation_is_pinned_to_registers() -> None:
    """One relation in P2. A second kind of claim needs its own review."""
    assert "registers" in _checks()["ck_aircraft_reg_edge_relation"]


def test_edge_is_not_a_canonical_edge() -> None:
    """P2 must not quietly become a graph write — aircraft is still not
    an EntityType, and canonical_edges is not touched."""
    assert Edge.__tablename__ == "aircraft_registration_edges"
    src = inspect.getsource(stage)
    for forbidden in ("canonical_edges", "source_citations", "neo4j"):
        assert forbidden not in src, f"P2 must not write {forbidden}"


def test_pair_is_unique_so_restaging_cannot_duplicate() -> None:
    """Re-running the stager must not fan out duplicate claims."""
    uniques = {
        tuple(c.columns.keys())
        for c in Edge.__table__.constraints
        if c.__class__.__name__ == "UniqueConstraint"
    }
    assert ("canonical_id", "aircraft_id") in uniques
    assert "on conflict (canonical_id, aircraft_id) do nothing" in inspect.getsource(stage)


def test_only_the_canonical_tier_is_staged() -> None:
    """helen held the entire 0.90 alias tier — it collapses
    subsidiaries into parents. The stager must skip it explicitly."""
    src = inspect.getsource(stage.stage_edges)
    assert 'if tier != "exact_canonical"' in src
    assert "HOLD_alias_tier" in src


def test_collision_holds_cover_the_named_cases() -> None:
    """The cross-domain collisions must be held, and each hold must
    carry a reason — an unexplained exclusion is unreviewable."""
    for name in ("H&M LTD", "SKYLARK HOLDINGS LLC", "KODIAK 100 LLC", "SK GROUP LLC"):
        assert name in stage.COLLISION_HOLDS
    for name, reason in stage.COLLISION_HOLDS.items():
        assert reason and len(reason) > 10, f"{name} held without a stated reason"


def test_holds_are_keyed_case_insensitively() -> None:
    """FAA registrant strings are uppercase; a lowercase key would
    silently fail to hold and the pair would be staged."""
    src = inspect.getsource(stage.stage_edges)
    assert ".strip().upper()" in src
    for key in stage.COLLISION_HOLDS:
        assert key == key.upper()
