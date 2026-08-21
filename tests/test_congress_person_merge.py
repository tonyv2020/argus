"""P1.5.3 — member fragment merge tests.

Hermetic — the plan is pure once nodes are loaded, so every privacy rule
is tested without touching live data. The merge itself is P2's
``merge_two_canonicals`` and is covered by the P2 suite.
"""

from __future__ import annotations

import pytest

from app.services.ingest.congress_person_merge import (
    MERGEABLE_FRAGMENT_TYPES,
    Node,
    build_merge_plan,
    person_name_is_evidence,
)


def _n(nid: str, name: str, norm: str, **over) -> Node:
    base = dict(
        type="person",
        surface_mode="open",
        publication_state="published",
        edge_count=0,
        alias_count=0,
        namespaces=frozenset(),
    )
    base.update(over)
    return Node(id=nid, name=name, norm=norm, **base)


def _plan(member: Node, fragments: list[Node], variants: set[str] | None = None):
    by_norm: dict[str, list[Node]] = {}
    for node in [member, *fragments]:
        by_norm.setdefault(node.norm, []).append(node)
    return build_merge_plan(
        {member.id: member},
        {member.id: variants or {member.norm}},
        by_norm,
    )


_MEMBER = _n("m1", "Bernard Sanders", "bernard sanders", alias_count=40)


# ─── the happy path ─────────────────────────────────────────────────────


def test_merges_a_news_fragment_matching_a_roster_variant() -> None:
    """"Bernie Sanders" is what news prints; the roster's official_full
    is "Bernard Sanders". The nickname variant is what connects them."""
    frag = _n("f1", "Bernie Sanders", "bernie sanders", edge_count=3)
    plan = _plan(_MEMBER, [frag], {"bernard sanders", "bernie sanders"})
    assert [p.fragment_id for p in plan.pairs] == ["f1"]
    # The member always survives — it carries the authoritative ids.
    assert plan.pairs[0].member_id == "m1"
    assert plan.pairs[0].matched_on == "bernie sanders"


def test_member_is_never_merged_into_another_member() -> None:
    """Two members can share a name variant; neither is a fragment."""
    other = _n("m2", "Bernie Sanders", "bernie sanders")
    by_norm = {"bernard sanders": [_MEMBER], "bernie sanders": [other]}
    plan = build_merge_plan(
        {"m1": _MEMBER, "m2": other},
        {"m1": {"bernard sanders", "bernie sanders"}, "m2": {"bernie sanders"}},
        by_norm,
    )
    assert plan.pairs == []


# ─── HARD PRIVACY RULE ──────────────────────────────────────────────────


@pytest.mark.parametrize("mode", ["suppress", "alias"])
def test_never_merges_a_protected_fragment_into_an_open_member(mode) -> None:
    """THE non-negotiable rule — no flag overrides it. The pair is
    skipped and lands on the review list."""
    frag = _n("f1", "Bernie Sanders", "bernie sanders", surface_mode=mode)
    plan = _plan(_MEMBER, [frag], {"bernard sanders", "bernie sanders"})
    assert plan.pairs == []
    assert plan.skipped_by_reason == {"surface_mode_straddle": 1}


@pytest.mark.parametrize("mode", ["suppress", "alias"])
def test_never_merges_into_a_protected_member(mode) -> None:
    """The straddle is symmetric: an open news fragment is not merged
    into a member canonical that is currently protected either."""
    member = _n("m1", "Bernard Sanders", "bernard sanders", surface_mode=mode)
    frag = _n("f1", "Bernie Sanders", "bernie sanders")
    plan = _plan(member, [frag], {"bernard sanders", "bernie sanders"})
    assert plan.pairs == []
    assert plan.skipped_by_reason == {"surface_mode_straddle": 1}


def test_two_protected_nodes_are_left_alone() -> None:
    """Same surface_mode, but both protected — consolidating two
    protected identities is not this pass's decision."""
    member = _n("m1", "Bernard Sanders", "bernard sanders",
                surface_mode="suppress")
    frag = _n("f1", "Bernie Sanders", "bernie sanders",
              surface_mode="suppress")
    plan = _plan(member, [frag], {"bernard sanders", "bernie sanders"})
    assert plan.pairs == []
    assert plan.skipped_by_reason == {"both_protected": 1}


def test_publication_state_is_a_partition_too() -> None:
    """A staged member and a published fragment are not merged — that
    would hide live content behind an unpublished batch."""
    member = _n("m1", "Bernard Sanders", "bernard sanders",
                publication_state="staged")
    frag = _n("f1", "Bernie Sanders", "bernie sanders")
    plan = _plan(member, [frag], {"bernard sanders", "bernie sanders"})
    assert plan.pairs == []
    assert plan.skipped_by_reason == {"publication_state_straddle": 1}


# ─── identity guards ────────────────────────────────────────────────────


@pytest.mark.parametrize("ns", ["bioguide", "fec.candidate", "sec.cik"])
def test_refuses_a_fragment_carrying_a_foreign_external_id(ns) -> None:
    """An id beats a name every time — that node is a different real
    entity, however similar the name looks."""
    frag = _n("f1", "Bernie Sanders", "bernie sanders",
              namespaces=frozenset({ns}))
    plan = _plan(_MEMBER, [frag], {"bernard sanders", "bernie sanders"})
    assert plan.pairs == []
    assert plan.skipped_by_reason == {"foreign_external_id": 1}


@pytest.mark.parametrize("bad_type", ["organization", "pac", "place", "concept"])
def test_refuses_non_person_fragments(bad_type) -> None:
    """"Sanders" the organization is not the senator."""
    frag = _n("f1", "Bernie Sanders", "bernie sanders", type=bad_type)
    plan = _plan(_MEMBER, [frag], {"bernard sanders", "bernie sanders"})
    assert plan.pairs == []
    assert plan.skipped_by_reason == {"type_not_mergeable": 1}


def test_unknown_typed_fragments_are_mergeable() -> None:
    """30% of the registry is typed unknown; a member's news fragment
    frequently lands there."""
    assert "unknown" in MERGEABLE_FRAGMENT_TYPES
    frag = _n("f1", "Bernie Sanders", "bernie sanders", type="unknown")
    plan = _plan(_MEMBER, [frag], {"bernard sanders", "bernie sanders"})
    assert [p.fragment_id for p in plan.pairs] == ["f1"]


def test_refuses_a_variant_that_names_two_members() -> None:
    """"John Kennedy" is a sitting senator AND a common name — a variant
    owned by two members is evidence for neither."""
    m1 = _n("m1", "John Kennedy", "john kennedy")
    m2 = _n("m2", "John F. Kennedy", "john f kennedy")
    frag = _n("f1", "John Kennedy", "john kennedy", edge_count=9)
    by_norm = {"john kennedy": [m1, frag], "john f kennedy": [m2]}
    plan = build_merge_plan(
        {"m1": m1, "m2": m2},
        {"m1": {"john kennedy"}, "m2": {"john f kennedy", "john kennedy"}},
        by_norm,
    )
    assert plan.pairs == []
    assert plan.skipped_by_reason == {"ambiguous_variant": 2}


# ─── name evidence ──────────────────────────────────────────────────────


@pytest.mark.parametrize("norm", ["smith", "sanders", "b s", "a", ""])
def test_single_token_names_are_not_person_evidence(norm) -> None:
    """A surname alone collides with every other person of that name."""
    assert person_name_is_evidence(norm) is False


@pytest.mark.parametrize("norm", ["bernie sanders", "alexandria ocasio cortez"])
def test_two_token_names_are_evidence(norm) -> None:
    """First+last is the weakest match this pass accepts."""
    assert person_name_is_evidence(norm) is True


def test_surname_only_variant_never_produces_a_pair() -> None:
    """The roster emits bare "last" as a variant; it must never key a
    merge on its own."""
    frag = _n("f1", "Sanders", "sanders", edge_count=12)
    plan = _plan(_MEMBER, [frag], {"bernard sanders", "sanders"})
    assert plan.pairs == []
