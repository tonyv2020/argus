"""P3.0 — the mechanism exists and SURFACES NOTHING.

This is the de-anon surface that failed open on 2026-08-21, so every
claim in the phase's verification list gets its own test:

  1. the fence relaxed but still rejects invalid states
  2. defaults stay suppress/staged (nothing publishes by omission)
  3. the promotion op is audited, attributed and reversible
  4. NOBODY invokes promotion in P3.0
  5. the read-gate excludes staged/suppressed, and fails CLOSED
  6. street/PII never appears in any output, on any path
  7. only published aircraft project to Neo4j
"""

from __future__ import annotations

import inspect

from app.models import (
    Aircraft,
    AircraftPromotionAudit,
    AircraftRegistrationEdge,
    PublicationState,
    SurfaceMode,
)
from app.services import aircraft_publish, read_gate
from app.services.graph import neo4j_projection
from app.services.ingest import project_to_neo4j

#: Every aircraft column that identifies a person or where they live.
#: None of these may appear in any read path, ever.
PII_COLUMNS = (
    "street",
    "street2",
    "city",
    "state",
    "zip_code",
    "county",
    "registrant_name",
    "other_names",
)


def _checks(model) -> dict[str, str]:
    return {
        c.name: str(c.sqltext) for c in model.__table__.constraints if hasattr(c, "sqltext")
    }


# ── 1. fence relaxed, still rejects invalid states ───────────────


def test_fence_is_now_a_validity_check_not_an_equality_pin() -> None:
    """P1/P2 pinned equality; P3.0 allows the legal values only."""
    ac = _checks(Aircraft)
    assert "ck_aircraft_p1_suppress" not in ac
    assert "ck_aircraft_p1_staged" not in ac
    for value in ("suppress", "alias", "open"):
        assert value in ac["ck_aircraft_surface_mode_valid"]
    for value in ("staged", "published"):
        assert value in ac["ck_aircraft_publication_state_valid"]


def test_invalid_gate_values_are_still_rejected() -> None:
    """Relaxing must not mean accepting anything — a typo like
    'public' or 'visible' has to keep failing at the database."""
    import re

    ac = _checks(Aircraft)
    # Parse the quoted values rather than substring-matching: 'public'
    # is a substring of 'published', so a naive `not in` check passes
    # against a constraint that would actually accept it.
    surface = set(re.findall(r"'([a-z_]+)'", ac["ck_aircraft_surface_mode_valid"]))
    pubstate = set(re.findall(r"'([a-z_]+)'", ac["ck_aircraft_publication_state_valid"]))
    assert surface == {"suppress", "alias", "open"}
    assert pubstate == {"staged", "published"}


def test_edge_relation_and_citation_fences_survive_the_relaxation() -> None:
    """Publishing relaxes visibility, never provenance."""
    ed = _checks(AircraftRegistrationEdge)
    assert "registers" in ed["ck_aircraft_reg_edge_relation"]
    assert "ck_aircraft_reg_edge_cited" in ed


# ── 2. defaults stay dark ────────────────────────────────────────


def test_column_defaults_are_still_suppress_and_staged() -> None:
    """Nothing publishes by omission. A row written by any existing
    code path is born dark; only an explicit promotion flips it."""
    for model in (Aircraft, AircraftRegistrationEdge):
        cols = model.__table__.columns
        assert cols["surface_mode"].server_default.arg == "suppress"
        assert cols["publication_state"].server_default.arg == "staged"


# ── 3./4. the promotion op ───────────────────────────────────────


def test_promotion_requires_attribution() -> None:
    """An unattributed promotion is what the audit table exists to
    prevent — enforced in code AND by a CHECK."""
    audit = _checks(AircraftPromotionAudit)
    assert "ck_aircraft_promotion_attributed" in audit
    assert "ck_aircraft_promotion_action" in audit
    src = inspect.getsource(aircraft_publish._apply)
    assert "actor and reason are required" in src


def test_promotion_is_reversible_and_demote_is_the_exact_inverse() -> None:
    """A mistaken promotion has to be undoable, and the undo has to
    leave its own trail rather than erasing one."""
    assert hasattr(aircraft_publish, "demote")
    src = inspect.getsource(aircraft_publish.demote)
    assert "SurfaceMode.SUPPRESS.value" in src
    assert "PublicationState.STAGED.value" in src
    assert '"demote"' in src
    # and the values those symbols carry are the dark state
    assert SurfaceMode.SUPPRESS.value == "suppress"
    assert PublicationState.STAGED.value == "staged"


def test_promote_refuses_to_promote_into_suppress() -> None:
    """Promoting to suppress would leave the caller believing a row is
    live while the read-gate still hides it."""
    src = inspect.getsource(aircraft_publish.promote)
    assert "is a no-op by definition" in src


def test_promotion_has_exactly_one_caller() -> None:
    """P3.0 asserted NOBODY invoked promotion. P3.2 (Tony-approved
    2026-08-30) introduced the one legitimate caller, so the invariant
    tightens rather than disappears: the pilot script is the ONLY module
    that may promote. Anything else acquiring the ability is the thing
    this test exists to catch."""
    import pathlib

    root = pathlib.Path(aircraft_publish.__file__).resolve().parents[1]  # app/
    call_markers = (
        "from app.services.aircraft_publish import",
        "from app.services import aircraft_publish",
        "import app.services.aircraft_publish",
        "aircraft_publish.promote(",
        "aircraft_publish.demote(",
    )
    allowed = {"aircraft_publish.py", "faa_aircraft_p32_pilot_publish.py"}
    offenders = []
    for path in root.rglob("*.py"):
        if path.name in allowed:
            continue
        text = path.read_text(encoding="utf-8", errors="ignore")
        if any(m in text for m in call_markers):
            offenders.append(str(path.relative_to(root)))
    assert offenders == [], f"only the P3.2 pilot script may promote: {offenders}"


def test_pilot_can_never_promote_an_individual() -> None:
    """P3.1 measured ~90% false positives on individual name matching, so
    zero individuals surface. The pilot filters on registrant type as a
    hard post-filter, not only in the SQL predicate — a query edit must
    not be able to let one through."""
    from app.services.ingest import faa_aircraft_p32_pilot_publish as pilot

    src = inspect.getsource(pilot.select_pilot)
    assert '("1", "4")' in src
    assert "type_registrant" in src


def test_pilot_excludes_are_named_with_reasons() -> None:
    """A silent exclusion is unreviewable."""
    from app.services.ingest import faa_aircraft_p32_pilot_publish as pilot

    assert "N/A" in pilot.EXCLUDED_CANONICAL_NAMES
    for name, why in pilot.EXCLUDED_CANONICAL_NAMES.items():
        assert why and len(why) > 20, f"{name} excluded without a stated reason"


def test_pilot_promotes_both_gates_together() -> None:
    """The read-gate requires the edge AND the aircraft published, so a
    run that promoted only one would surface nothing — fail closed, not
    half-open. Both promotions are in the same loop iteration."""
    from app.services.ingest import faa_aircraft_p32_pilot_publish as pilot

    src = inspect.getsource(pilot.run)
    assert 'target_table="aircraft_registration_edges"' in src
    assert 'target_table="aircraft"' in src
    assert "ACTOR" in src and "REASON" in src


def test_pilot_requires_the_entity_to_be_known_and_open() -> None:
    """>=1 existing edge (Argus substantively knows them), open, published."""
    from app.services.ingest import faa_aircraft_p32_pilot_publish as pilot

    src = inspect.getsource(pilot.select_pilot)
    assert "edge_count >= 1" in src
    assert "SurfaceMode.OPEN.value" in src
    assert "PublicationState.PUBLISHED.value" in src
    assert 'match_tier == "exact_canonical"' in src


# ── 5. the read-gate ─────────────────────────────────────────────


def test_aircraft_gate_requires_published_AND_not_suppressed() -> None:
    """Both gates AND, same contract as canonicals."""
    for fn in (read_gate.published_aircraft, read_gate.published_registration_edge):
        sql = str(fn())
        assert "publication_state" in sql and "surface_mode" in sql
        assert " AND " in sql.upper()


def test_materialized_aircraft_check_fails_CLOSED() -> None:
    """Unlike ``is_published_entity`` (which treats NULL as published to
    keep old fixtures usable), the aircraft check must treat unknown as
    NOT published — this is the surface that leaked on 2026-08-21."""

    class Row:
        def __init__(self, ps, sm):
            self.publication_state, self.surface_mode = ps, sm

    assert read_gate.is_published_aircraft(Row("published", "open")) is True
    assert read_gate.is_published_aircraft(Row("published", "alias")) is True
    # every way of being not-published
    assert read_gate.is_published_aircraft(Row("staged", "open")) is False
    assert read_gate.is_published_aircraft(Row("published", "suppress")) is False
    assert read_gate.is_published_aircraft(Row("staged", "suppress")) is False
    assert read_gate.is_published_aircraft(Row(None, "open")) is False
    assert read_gate.is_published_aircraft(Row("published", None)) is False
    assert read_gate.is_published_aircraft(object()) is False

    # A row MISSING a gate column entirely, not merely holding None.
    # getattr(..., default) is the easy way to write this check and it
    # fails OPEN; these two cases are what force the getattr default to
    # stay None. Found by mutation testing — the None-valued cases
    # above do not exercise it, because the attribute exists.
    class OnlySurfaceMode:
        surface_mode = "open"

    class OnlyPublicationState:
        publication_state = "published"

    assert read_gate.is_published_aircraft(OnlySurfaceMode()) is False
    assert read_gate.is_published_aircraft(OnlyPublicationState()) is False


def test_dossier_aircraft_section_gates_both_rows() -> None:
    """Promoting the edge without the aircraft (or vice versa) must
    surface nothing — a half-finished promotion fails closed."""
    from app import main

    src = inspect.getsource(main._published_aircraft_for)
    assert "published_registration_edge()" in src
    assert "published_aircraft()" in src


def test_aircraft_section_ignores_the_include_staged_preview_flag() -> None:
    """``?include_staged=1`` previews canonical content for reviewers.
    It must not become a way to read unpublished aircraft."""
    from app import main

    src = inspect.getsource(main._published_aircraft_for)
    assert "include_staged" not in src
    assert "maybe_published" not in src


# ── 6. street / PII never surfaces ───────────────────────────────


def test_public_column_allowlist_excludes_every_pii_column() -> None:
    """The dossier selects an explicit allowlist, so a column added to
    the model later cannot leak by being picked up implicitly."""
    from app import main

    for col in PII_COLUMNS:
        assert col not in main._AIRCRAFT_PUBLIC_COLUMNS


def test_no_read_path_selects_a_pii_column() -> None:
    """Source-level guard across all three read paths at once."""
    from app import main

    targets = {
        "web dossier": inspect.getsource(main._published_aircraft_for),
        "neo4j projector": inspect.getsource(neo4j_projection.Neo4jProjection.project_aircraft),
        "projection sweep": inspect.getsource(project_to_neo4j.project_all),
    }
    for where, src in targets.items():
        for col in PII_COLUMNS:
            assert f"Aircraft.{col}" not in src, f"{where} selects PII column {col}"
            assert f'"{col}"' not in src, f"{where} emits PII key {col}"


def test_neo4j_aircraft_node_carries_only_n_number_and_make_model() -> None:
    """Design decision #1: zero PII on the node. Never street, never an
    individual's raw registrant name."""
    src = inspect.getsource(neo4j_projection.Neo4jProjection.project_aircraft)
    assert "a.n_number=$n" in src
    assert "a.make_model=$mm" in src
    # Check the CODE, not the docstring — the docstring names the PII
    # precisely because it is explaining what it refuses to carry.
    body = src.split('"""')[2] if src.count('"""') >= 2 else src
    for col in PII_COLUMNS:
        assert col not in body, f"projector body references PII column {col}"


# ── 7. only published projects ───────────────────────────────────


def test_projector_refuses_unpublished_aircraft() -> None:
    """Defense-in-depth: the sweep's SELECT filters, and the projector
    re-checks. This is the last place a mistake becomes Cypher-reachable."""
    src = inspect.getsource(neo4j_projection.Neo4jProjection.project_aircraft)
    assert "is_published_aircraft(edge)" in src
    assert "is_published_aircraft(aircraft)" in src
    assert "return False" in src


def test_projector_uses_a_distinct_Aircraft_label_not_an_entity_type() -> None:
    """Aircraft are first-class nodes under their own label; EntityType
    stays person/org-centric."""
    from app.models import EntityType

    src = inspect.getsource(neo4j_projection.Neo4jProjection.project_aircraft)
    assert "(a:Aircraft" in src
    assert ":REGISTERS" in src
    assert "aircraft" not in {e.value for e in EntityType}


def test_demoted_aircraft_are_pruned_from_neo4j() -> None:
    """MERGE-only projection removes nothing, so a demotion has to
    prune — otherwise the audit trail's 'reversed' is a lie about what
    Cypher can still see."""
    assert hasattr(neo4j_projection.Neo4jProjection, "prune_unpublished_aircraft")
    src = inspect.getsource(project_to_neo4j.project_all)
    assert "prune_unpublished_aircraft" in src
