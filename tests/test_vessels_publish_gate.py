"""Vessel publish mechanism — exists, surfaces nothing.

Mirrors the aircraft P3.0 test set, including the two traps that set
found the hard way: a gate check that fails OPEN on a MISSING column,
and a docstring that satisfies a naive grep.
"""

from __future__ import annotations

import inspect
import pathlib

from app.models import Vessel, VesselOwnershipEdge, VesselPromotionAudit
from app.services import read_gate, vessel_publish
from app.services.graph import neo4j_projection
from app.services.ingest import project_to_neo4j

#: Owner columns that identify a person or where they live.
PII_COLUMNS = (
    "owner_name_raw", "owner_street", "owner_city",
    "owner_state", "owner_postal_code", "owner_country",
)


def _checks(model) -> dict[str, str]:
    return {
        c.name: str(c.sqltext) for c in model.__table__.constraints if hasattr(c, "sqltext")
    }


# ── fence relaxed, still validating ──────────────────────────────


def test_fence_is_now_a_validity_check() -> None:
    import re

    for model, sm_key, ps_key in (
        (Vessel, "ck_vessel_surface_mode_valid", "ck_vessel_publication_state_valid"),
        (VesselOwnershipEdge, "ck_vessel_owner_surface_mode_valid",
         "ck_vessel_owner_publication_state_valid"),
    ):
        c = _checks(model)
        assert "ck_vessel_p1_suppress" not in c and "ck_vessel_owner_suppress" not in c
        # Parse the quoted values: 'public' is a substring of 'published',
        # so a naive `not in` check passes against a broken constraint.
        assert set(re.findall(r"'([a-z_]+)'", c[sm_key])) == {"suppress", "alias", "open"}
        assert set(re.findall(r"'([a-z_]+)'", c[ps_key])) == {"staged", "published"}


def test_defaults_stay_dark() -> None:
    """Nothing publishes by omission."""
    for model in (Vessel, VesselOwnershipEdge):
        cols = model.__table__.columns
        assert cols["surface_mode"].server_default.arg == "suppress"
        assert cols["publication_state"].server_default.arg == "staged"


def test_relation_and_citation_fences_survive() -> None:
    """Publishing relaxes visibility, never provenance."""
    c = _checks(VesselOwnershipEdge)
    assert "owns" in c["ck_vessel_owner_relation"]
    assert "ck_vessel_owner_cited" in c
    assert "ck_vessel_cited" in _checks(Vessel)


# ── the audited op, invoked by nobody ────────────────────────────


def test_promotion_requires_attribution_and_is_reversible() -> None:
    a = _checks(VesselPromotionAudit)
    assert "ck_vessel_promotion_attributed" in a and "ck_vessel_promotion_action" in a
    assert "actor and reason are required" in inspect.getsource(vessel_publish._apply)
    src = inspect.getsource(vessel_publish.demote)
    assert "SurfaceMode.SUPPRESS.value" in src and "PublicationState.STAGED.value" in src
    assert "is a no-op by definition" in inspect.getsource(vessel_publish.promote)


def test_nobody_invokes_vessel_promotion_yet() -> None:
    """THE phase invariant: the mechanism exists, nothing uses it."""
    app_dir = pathlib.Path(inspect.getfile(vessel_publish)).resolve().parents[1]
    markers = (
        "from app.services.vessel_publish import",
        "from app.services import vessel_publish",
        "vessel_publish.promote(",
        "vessel_publish.demote(",
    )
    offenders = [
        str(p.relative_to(app_dir))
        for p in app_dir.rglob("*.py")
        if p.name != "vessel_publish.py"
        and any(m in p.read_text(encoding="utf-8", errors="ignore") for m in markers)
    ]
    assert offenders == [], f"nothing may promote a vessel yet: {offenders}"


# ── the read gate ────────────────────────────────────────────────


def test_vessel_gate_requires_published_AND_not_suppressed() -> None:
    for fn in (read_gate.published_vessel, read_gate.published_vessel_edge):
        sql = str(fn())
        assert "publication_state" in sql and "surface_mode" in sql and " AND " in sql.upper()


def test_materialized_vessel_check_fails_CLOSED() -> None:
    class Row:
        def __init__(self, ps, sm):
            self.publication_state, self.surface_mode = ps, sm

    assert read_gate.is_published_vessel(Row("published", "open")) is True
    assert read_gate.is_published_vessel(Row("published", "alias")) is True
    for bad in (("staged", "open"), ("published", "suppress"), (None, "open"),
                ("published", None)):
        assert read_gate.is_published_vessel(Row(*bad)) is False, bad
    assert read_gate.is_published_vessel(object()) is False

    # A row MISSING a gate column, not merely holding None. The aircraft
    # equivalent had a getattr default that let exactly this through.
    class OnlySurface:
        surface_mode = "open"

    class OnlyState:
        publication_state = "published"

    assert read_gate.is_published_vessel(OnlySurface()) is False
    assert read_gate.is_published_vessel(OnlyState()) is False


def test_dossier_vessels_gates_both_rows_and_ignores_the_preview_flag() -> None:
    from app import main

    src = inspect.getsource(main._published_vessels_for)
    assert "published_vessel_edge()" in src and "published_vessel()" in src
    assert "include_staged" not in src and "maybe_published" not in src


# ── owner PII never surfaces ─────────────────────────────────────


def test_public_column_allowlist_excludes_every_owner_pii_column() -> None:
    from app import main

    for col in PII_COLUMNS:
        assert col not in main._VESSEL_PUBLIC_COLUMNS


def test_no_read_path_selects_an_owner_pii_column() -> None:
    from app import main

    targets = {
        "web dossier": inspect.getsource(main._published_vessels_for),
        "neo4j projector": inspect.getsource(neo4j_projection.Neo4jProjection.project_vessel),
        "projection sweep": inspect.getsource(project_to_neo4j.project_all),
    }
    for where, src in targets.items():
        # Strip comments and the docstring: they NAME the PII they refuse
        # to carry, which a naive grep matches.
        body = "\n".join(ln for ln in src.splitlines() if not ln.lstrip().startswith("#"))
        body = body.split('"""')[-1]
        for col in PII_COLUMNS:
            assert col not in body, f"{where} references PII column {col}"


def test_neo4j_vessel_node_carries_only_name_imo_flag() -> None:
    src = inspect.getsource(neo4j_projection.Neo4jProjection.project_vessel)
    assert "v.vessel_name=$name" in src and "v.imo=$imo" in src and "v.flag=$flag" in src
    body = src.split('"""')[-1]
    for col in PII_COLUMNS:
        assert col not in body


# ── only published projects ──────────────────────────────────────


def test_projector_refuses_unpublished_vessels() -> None:
    src = inspect.getsource(neo4j_projection.Neo4jProjection.project_vessel)
    assert "is_published_vessel(edge)" in src and "is_published_vessel(vessel)" in src


def test_vessel_label_is_distinct_and_not_an_entity_type() -> None:
    from app.models import EntityType

    src = inspect.getsource(neo4j_projection.Neo4jProjection.project_vessel)
    assert "(v:Vessel" in src and ":OWNS" in src
    assert "vessel" not in {e.value for e in EntityType}


def test_demoted_vessels_are_pruned_from_neo4j() -> None:
    assert hasattr(neo4j_projection.Neo4jProjection, "prune_unpublished_vessels")
    assert "prune_unpublished_vessels" in inspect.getsource(project_to_neo4j.project_all)


def test_migration_revision_id_fits_the_version_column() -> None:
    root = pathlib.Path(inspect.getfile(vessel_publish)).resolve().parents[2]
    path = root / "alembic" / "versions" / "0017_vessel_publish_mech.py"
    assert path.exists(), path
    rev = next(
        ln.split("=", 1)[1].strip().strip("\"'")
        for ln in path.read_text(encoding="utf-8").splitlines()
        if ln.startswith("revision = ")
    )
    assert len(rev) <= 32, f"{rev} is {len(rev)} chars"
