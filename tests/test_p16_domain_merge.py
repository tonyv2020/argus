"""P1.6.3 — domain fragment-merge planning tests.

Hermetic: no DB. The fragments below are the REAL rows measured in the
live graph on 2026-08-21, including the LDA wrapper client records that
each hold cited ``lobbies`` edges to a different registrant.
"""

from __future__ import annotations

import pytest

from app.models import EntityType, SurfaceMode
from app.services.ingest.domain_anchors import (
    SEC_OWNER_NAMESPACE,
    USASPENDING_UEI_NAMESPACE,
)
from app.services.ingest.domain_merge import (
    NAMESPACE_PINS_TYPE,
    RETYPEABLE_FROM,
    AnchorNode,
    Node,
    build_merge_plan,
    build_retype_plan,
    mergeable_types_for,
)


def _node(nid, name, norm, **kw) -> Node:
    base = {
        "type": EntityType.ORGANIZATION.value,
        "surface_mode": SurfaceMode.OPEN.value,
        "publication_state": "published",
        "edge_count": 1,
        "alias_count": 0,
        "namespaces": frozenset(),
    }
    base.update(kw)
    return Node(id=nid, name=name, norm=norm, **base)


PALANTIR_NODE = _node("pal", "Palantir Technologies Inc.", "palantir technologies",
                      namespaces=frozenset({"sec.cik"}))
FLOCK_NODE = _node("flk", "Flock Safety", "flock safety",
                   namespaces=frozenset({USASPENDING_UEI_NAMESPACE}))
AXON_NODE = _node("axn", "Axon Enterprise", "axon enterprise",
                  namespaces=frozenset({"sec.cik"}))


def _palantir_anchor() -> AnchorNode:
    return AnchorNode(
        label="Palantir Technologies",
        node=PALANTIR_NODE,
        entity_type=EntityType.ORGANIZATION.value,
        variants=frozenset({"palantir technologies", "palantir"}),
        client_patterns=(
            r"^palantir( technologies)?$",
            r"\b(obo|for) palantir technologies$",
        ),
    )


def _flock_anchor() -> AnchorNode:
    return AnchorNode(
        label="Flock Safety",
        node=FLOCK_NODE,
        entity_type=EntityType.ORGANIZATION.value,
        variants=frozenset({"flock safety", "flock", "flock group"}),
        client_patterns=(
            r"^flock safety$",
            r"^flock group( inc)? d b a flock safety$",
            r"\bon behalf of flock safety$",
        ),
    )


def _axon_anchor() -> AnchorNode:
    return AnchorNode(
        label="Axon Enterprise",
        node=AXON_NODE,
        entity_type=EntityType.ORGANIZATION.value,
        variants=frozenset({"axon enterprise", "axon", "axon enterprises"}),
        client_patterns=(r"^axon enterprises?$",),
    )


def _plan(anchors, frags):
    nodes = {a.node.id: a.node for a in anchors}
    nodes.update({f.id: f for f in frags})
    return build_merge_plan(anchors, nodes)


# ─── the merges we want ─────────────────────────────────────────────────


def test_collapses_the_news_tag_name_fragments() -> None:
    """``AXON`` (6 edges) and ``AXON ENTERPRISES`` (8 cited lobbying
    filings) are the same company as ``Axon Enterprise``."""
    frags = [
        _node("f1", "AXON", "axon", edge_count=6),
        _node("f2", "AXON ENTERPRISES", "axon enterprises", edge_count=1),
    ]
    plan = _plan([_axon_anchor()], frags)
    assert {p.fragment_id for p in plan.pairs} == {"f1", "f2"}
    assert all(p.anchor_id == "axn" for p in plan.pairs)


def test_collapses_the_lda_wrapper_client_records() -> None:
    """The LDA ingester mints one canonical per ``client.name``, and
    filers register wrapper strings. Each of these holds real cited
    ``lobbies`` edges to a DIFFERENT registrant that never reach the
    company's profile."""
    frags = [
        _node("w1",
              "BROWNSTEIN HYATT FARBER SCHRECK LLP OBO PALANTIR TECHNOLOGIES INC.",
              "brownstein hyatt farber schreck llp obo palantir technologies",
              edge_count=1),
        _node("w2", "J.A. GREEN AND COMPANY (FOR PALANTIR TECHNOLOGIES INC.)",
              "j a green and company for palantir technologies", edge_count=1),
        _node("w3",
              "HANNEGAN LANDAU POERSCH & ROSENBAUM ADVOCACY LLC "
              "(FOR PALANTIR TECHNOLOGIES INC)",
              "hannegan landau poersch rosenbaum advocacy llc for palantir technologies",
              edge_count=1),
    ]
    plan = _plan([_palantir_anchor()], frags)
    assert {p.fragment_id for p in plan.pairs} == {"w1", "w2", "w3"}
    assert all(p.rule == "lda_client_pattern" for p in plan.pairs)


def test_absorbs_a_concept_typed_company() -> None:
    """The news-tag pipeline routinely types a company as a ``concept``
    — the same organization/concept pair P2 already merges."""
    frag = _node("c1", "Flock", "flock", type=EntityType.CONCEPT.value)
    plan = _plan([_flock_anchor()], [frag])
    assert [p.fragment_id for p in plan.pairs] == ["c1"]


# ─── the merges we refuse ───────────────────────────────────────────────


@pytest.mark.parametrize(
    "name,norm",
    [
        ("Flock Cameras", "flock cameras"),
        ("Flock database", "flock database"),
        ("Flock Surveillance Towers", "flock surveillance towers"),
        ("Flock Sock", "flock sock"),
        ("Flock Homes", "flock homes"),
    ],
)
def test_refuses_a_near_name_that_is_a_different_thing(name, norm) -> None:
    """A product, a dataset, or another company is not the company. These
    do not match a declared variant or an anchored client pattern, so
    they are never even considered."""
    plan = _plan([_flock_anchor()], [_node("n1", name, norm)])
    assert plan.pairs == []


@pytest.mark.parametrize(
    "name,norm",
    [("Joby Axon", "joby axon"), ("Axonius", "axonius"),
     ("Palantir Foundry", "palantir foundry")],
)
def test_refuses_an_unrelated_company_or_product(name, norm) -> None:
    plan = _plan(
        [_axon_anchor(), _palantir_anchor()], [_node("n1", name, norm)]
    )
    assert plan.pairs == []


@pytest.mark.parametrize(
    "mode", [SurfaceMode.SUPPRESS.value, SurfaceMode.ALIAS.value]
)
def test_never_merges_across_surface_mode(mode) -> None:
    """THE non-negotiable rule — there is no flag that overrides it."""
    frag = _node("p1", "AXON", "axon", surface_mode=mode)
    plan = _plan([_axon_anchor()], [frag])
    assert plan.pairs == []
    assert plan.skipped_by_reason == {"surface_mode_straddle": 1}


def test_never_merges_two_protected_nodes_either() -> None:
    """Consolidating two protected identities is not this pass's call,
    even when both sides agree."""
    anchor = _axon_anchor()
    protected_anchor = AnchorNode(
        label=anchor.label,
        node=Node(**{**anchor.node.__dict__,
                     "surface_mode": SurfaceMode.ALIAS.value}),
        entity_type=anchor.entity_type,
        variants=anchor.variants,
        client_patterns=anchor.client_patterns,
    )
    frag = _node("p1", "AXON", "axon", surface_mode=SurfaceMode.ALIAS.value)
    plan = _plan([protected_anchor], [frag])
    assert plan.pairs == []
    assert plan.skipped_by_reason == {"both_protected": 1}


def test_never_merges_across_publication_state() -> None:
    """A merge must not silently publish staged content, nor stage
    published content."""
    frag = _node("p1", "AXON", "axon", publication_state="staged")
    plan = _plan([_axon_anchor()], [frag])
    assert plan.pairs == []
    assert plan.skipped_by_reason == {"publication_state_straddle": 1}


def test_refuses_a_fragment_carrying_a_foreign_external_id() -> None:
    """A node with its own CIK or FEC id is a different real entity."""
    frag = _node("p1", "AXON", "axon",
                 namespaces=frozenset({"fec.committee"}))
    plan = _plan([_axon_anchor()], [frag])
    assert plan.pairs == []
    assert plan.skipped_by_reason == {"foreign_external_id": 1}


def test_refuses_a_person_fragment_for_an_org_anchor() -> None:
    frag = _node("p1", "Axon", "axon", type=EntityType.PERSON.value)
    plan = _plan([_axon_anchor()], [frag])
    assert plan.pairs == []
    assert plan.skipped_by_reason == {"type_not_mergeable": 1}


def test_a_fragment_claimed_by_two_anchors_is_refused_against_both() -> None:
    """Ambiguity is never resolved by iteration order."""
    a1 = _axon_anchor()
    a2 = AnchorNode(
        label="Other", node=_node("oth", "Other", "other"),
        entity_type=EntityType.ORGANIZATION.value,
        variants=frozenset({"axon"}), client_patterns=(),
    )
    plan = _plan([a1, a2], [_node("f1", "AXON", "axon")])
    assert plan.pairs == []
    assert plan.skipped_by_reason == {"claimed_by_multiple_anchors": 2}


def test_an_anchor_never_merges_into_another_anchor() -> None:
    plan = _plan([_axon_anchor(), _palantir_anchor()], [])
    assert plan.pairs == []


# ─── mis-types ──────────────────────────────────────────────────────────


def test_retypes_a_concept_that_carries_an_issuer_cik() -> None:
    """A canonical carrying an authoritative id whose namespace PINS a
    type but typed ``concept`` is the mis-typing P1.6.3 fixes. The id is
    the evidence — nothing is guessed."""
    nodes = {
        "x": _node("x", "Palantir Technologies", "palantir technologies",
                   type=EntityType.CONCEPT.value,
                   namespaces=frozenset({"sec.cik"}))
    }
    (plan,) = build_retype_plan(nodes, {"x": {EntityType.ORGANIZATION.value}})
    assert plan.from_type == EntityType.CONCEPT.value
    assert plan.to_type == EntityType.ORGANIZATION.value
    assert "sec.cik" in plan.evidence


def test_never_downgrades_a_real_specific_type() -> None:
    """A canonical already typed ``pac`` or ``agency`` is a conflict for
    an operator, not a repair."""
    nodes = {
        "x": _node("x", "Some PAC", "some pac", type=EntityType.PAC.value,
                   namespaces=frozenset({"sec.cik"}))
    }
    assert build_retype_plan(nodes, {"x": {EntityType.ORGANIZATION.value}}) == []


def test_conflicting_pins_are_left_alone() -> None:
    """Two ids pinning two different types is a conflict, not a repair."""
    nodes = {
        "x": _node("x", "Ambiguous", "ambiguous",
                   type=EntityType.UNKNOWN.value,
                   namespaces=frozenset({"sec.cik", SEC_OWNER_NAMESPACE}))
    }
    assert build_retype_plan(
        nodes,
        {"x": {EntityType.ORGANIZATION.value, EntityType.PERSON.value}},
    ) == []


def test_type_pins_cover_the_p16_namespaces() -> None:
    """A UEI names an organization; a reporting-owner CIK names a person.
    Conflating them would retype Peter Thiel as a company."""
    assert NAMESPACE_PINS_TYPE[USASPENDING_UEI_NAMESPACE] == (
        EntityType.ORGANIZATION.value
    )
    assert NAMESPACE_PINS_TYPE[SEC_OWNER_NAMESPACE] == EntityType.PERSON.value
    assert EntityType.CONCEPT.value in RETYPEABLE_FROM
    assert EntityType.PERSON.value not in RETYPEABLE_FROM


def test_mergeable_types_are_type_specific() -> None:
    """The concept escape hatch is organization-only — a person is never
    a concept."""
    assert EntityType.CONCEPT.value in mergeable_types_for(
        EntityType.ORGANIZATION.value
    )
    assert EntityType.CONCEPT.value not in mergeable_types_for(
        EntityType.PERSON.value
    )
