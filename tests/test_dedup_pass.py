"""P2 dedup-pass tests — hermetic, no DB.

The candidate → plan pipeline is pure once entities are loaded, so the
privacy-critical decisions (never merge across surface_mode), the
type-compatibility matrix, survivor selection, type inference and the
before/after prediction are all testable without touching live data.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from app.services.ingest.dedup_pass import (
    DEFAULT_APPLIED_RULES,
    _HAS_DIGIT_RE,
    _PREFIX_CURATED,
    _check_postconditions,
    Candidate,
    Ent,
    _identity_key,
    _name_is_evidence,
    _name_merge_compatible,
    _resolve_type,
    _survivor_sort_key,
    build_plan,
    predict,
)

_T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _e(
    eid: str,
    name: str,
    norm: str,
    type_: str,
    surface_mode: str = "open",
    edges: int = 0,
    aliases: int = 0,
    publication_state: str = "published",
    namespaces: set[str] | None = None,
) -> Ent:
    return Ent(
        id=eid,
        name=name,
        norm=norm,
        type=type_,
        surface_mode=surface_mode,
        publication_state=publication_state,
        created_at=_T0,
        alias_count=aliases,
        edge_count=edges,
        id_namespaces=set(namespaces or ()),
    )


def _plan(ents: list[Ent], cands: list[Candidate], **kw):
    return build_plan(
        {e.id: e for e in ents}, cands, set(DEFAULT_APPLIED_RULES), **kw
    )


# ─── HARD PRIVACY RULE ──────────────────────────────────────────────────


def test_never_merges_across_surface_mode() -> None:
    """The non-negotiable rule. `Tesla`/person is suppress while
    `Tesla`/organization is open — they must NEVER be merged, and the pair
    must land on the review list instead."""
    org = _e("a", "Tesla", "tesla", "organization", "open", edges=55)
    person = _e("b", "Tesla", "tesla", "person", "suppress", edges=1)
    plan = _plan([org, person], [Candidate("a", "b", "exact_name", "norm")])

    assert plan.clusters == []
    assert [s.reason for s in plan.skipped] == ["surface_mode_straddle"]


@pytest.mark.parametrize(
    ("mode_a", "mode_b"),
    [("open", "alias"), ("open", "suppress"), ("alias", "suppress"),
     ("suppress", "open"), ("alias", "open"), ("suppress", "alias")],
)
def test_every_surface_mode_straddle_is_refused(mode_a: str, mode_b: str) -> None:
    """Fail-closed in BOTH directions — there is no 'safe' direction to
    merge a protected node in or out of."""
    ents = [
        _e("a", "X Corp", "x corp", "organization", mode_a),
        _e("b", "X Corp", "x corp", "organization", mode_b),
    ]
    plan = _plan(ents, [Candidate("a", "b", "exact_name", "norm")])
    assert plan.clusters == []
    assert plan.skipped[0].reason == "surface_mode_straddle"


def test_surface_mode_straddle_does_not_block_the_same_mode_merge() -> None:
    """Partitioning, not abandonment: the open Tesla fragments still
    collapse while the suppress-mode person node is left untouched."""
    ents = [
        _e("org", "Tesla", "tesla", "organization", "open", edges=55, aliases=517),
        _e("unk", "Tesla", "tesla", "unknown", "open", edges=1),
        _e("per", "Tesla", "tesla", "person", "suppress"),
    ]
    plan = _plan(
        ents,
        [
            Candidate("org", "unk", "exact_name", "norm"),
            Candidate("org", "per", "exact_name", "norm"),
            Candidate("unk", "per", "exact_name", "norm"),
        ],
    )
    assert len(plan.clusters) == 1
    cluster = plan.clusters[0]
    assert cluster.survivor.id == "org"
    assert [d.id for d in cluster.dropped] == ["unk"]
    assert {s.reason for s in plan.skipped} == {"surface_mode_straddle"}


def test_a_straddling_pair_cannot_sneak_in_transitively() -> None:
    """A refused pair never reaches the union-find, so a protected node
    cannot be dragged into a cluster via a third entity."""
    ents = [
        _e("open1", "Acme", "acme", "organization", "open", edges=9),
        _e("open2", "Acme", "acme", "unknown", "open"),
        _e("sup", "Acme", "acme", "organization", "suppress"),
    ]
    plan = _plan(
        ents,
        [
            Candidate("open1", "open2", "exact_name", "norm"),
            Candidate("open2", "sup", "exact_name", "norm"),
        ],
    )
    assert len(plan.clusters) == 1
    assert "sup" not in {d.id for d in plan.clusters[0].dropped}
    assert plan.clusters[0].survivor.id != "sup"


def test_publication_state_straddle_is_refused() -> None:
    """A merge must never silently publish staged content."""
    ents = [
        _e("a", "Acme", "acme", "organization", publication_state="published"),
        _e("b", "Acme", "acme", "organization", publication_state="staged"),
    ]
    plan = _plan(ents, [Candidate("a", "b", "exact_name", "norm")])
    assert plan.clusters == []
    assert plan.skipped[0].reason == "publication_state_straddle"


# ─── type compatibility ─────────────────────────────────────────────────


def test_person_name_matches_are_review_only() -> None:
    """Two people who happen to share a name are not the same person."""
    assert _name_merge_compatible("person", "person") == (False, "person_name")
    assert _name_merge_compatible("person", "unknown")[1] == "person_name"
    assert _name_merge_compatible("person", "organization")[1] == "person_name"


def test_person_unknown_can_be_opted_in_but_person_person_never_can() -> None:
    assert _name_merge_compatible("person", "unknown", True)[0] is True
    assert _name_merge_compatible("person", "person", True)[0] is False


def test_pac_cross_type_is_refused() -> None:
    """normalize_name strips the trailing 'pac' suffix, so 'AMERICA PAC'
    normalizes onto the place 'America' and 'FACEBOOK INC. PAC' onto the
    org 'Facebook'. Merging those would be badly wrong."""
    assert _name_merge_compatible("pac", "place") == (False, "pac_cross_type")
    assert _name_merge_compatible("pac", "organization")[1] == "pac_cross_type"
    assert _name_merge_compatible("pac", "unknown")[1] == "pac_cross_type"
    assert _name_merge_compatible("pac", "pac")[0] is True


def test_typed_versus_unknown_merges_and_org_concept_is_the_only_extra() -> None:
    assert _name_merge_compatible("organization", "unknown")[0] is True
    assert _name_merge_compatible("place", "unknown")[0] is True
    assert _name_merge_compatible("organization", "concept")[0] is True
    # A place is not an organization ('India', 'Berkshire', 'Kansas').
    assert _name_merge_compatible("organization", "place") == (
        False, "incompatible_type"
    )
    assert _name_merge_compatible("event", "organization")[1] == "incompatible_type"


def test_transitively_incompatible_cluster_is_refused_whole() -> None:
    """place ~ unknown ~ organization: the unknown could belong to either
    side, so refuse the cluster rather than guess."""
    ents = [
        _e("p", "India", "india", "place", edges=40),
        _e("u", "India", "india", "unknown"),
        _e("o", "India", "india", "organization", edges=3),
    ]
    plan = _plan(
        ents,
        [
            Candidate("p", "u", "exact_name", "norm"),
            Candidate("u", "o", "exact_name", "norm"),
        ],
    )
    assert plan.clusters == []
    assert [s.reason for s in plan.review] == ["type_incompatible_cluster"]


# ─── survivor + type inference ──────────────────────────────────────────


def test_survivor_is_the_richest_evidence_node() -> None:
    rich = _e("rich", "SpaceX", "spacex", "organization", edges=110, aliases=2471)
    thin = _e("thin", "SpaceX", "spacex", "unknown", edges=0, aliases=3)
    assert sorted([thin, rich], key=_survivor_sort_key)[0] is rich


def test_external_id_node_outranks_a_bigger_unanchored_node() -> None:
    """Keeping the anchored canonical as survivor keeps anchor_registry and
    the external-id aliases pointing at a stable id."""
    anchored = _e("anch", "GEO", "geo", "organization", edges=5,
                  namespaces={"sec_cik"})
    bigger = _e("big", "GEO", "geo", "organization", edges=50)
    assert sorted([bigger, anchored], key=_survivor_sort_key)[0] is anchored


def test_unknown_inherits_the_single_real_type() -> None:
    members = [
        _e("a", "SpaceX", "spacex", "organization", edges=110),
        _e("b", "SpaceX", "spacex", "unknown"),
    ]
    assert _resolve_type(members, members[0]) == ("organization", False)


def test_all_unknown_stays_unknown_without_an_external_id() -> None:
    """NEVER guess: nothing about the name may promote a type."""
    members = [
        _e("a", "Musk Thiel", "musk thiel", "unknown"),
        _e("b", "Musk/Thiel", "musk thiel", "unknown"),
    ]
    assert _resolve_type(members, members[0]) == ("unknown", False)


def test_external_id_lifts_an_all_unknown_cluster() -> None:
    members = [
        _e("a", "Acme", "acme", "unknown", namespaces={"sec_cik"}),
        _e("b", "Acme", "acme", "unknown"),
    ]
    assert _resolve_type(members, members[0]) == ("organization", False)


def test_org_concept_resolves_to_organization_and_can_be_disabled() -> None:
    """xAI / Starlink / OpenAI: the org claim is the specific one."""
    members = [
        _e("a", "xAI", "xai", "concept", edges=47),
        _e("b", "xAI", "xai", "organization", edges=5),
    ]
    assert _resolve_type(members, members[0]) == ("organization", False)
    assert _resolve_type(members, members[0], concept_to_org=False) == (
        "concept", True
    )


def test_a_genuine_type_disagreement_keeps_the_survivor_type_and_flags() -> None:
    members = [
        _e("a", "ASCO", "asco", "event", edges=11),
        _e("b", "ASCO", "asco", "agency"),
    ]
    assert _resolve_type(members, members[0]) == ("event", True)


# ─── name-evidence guards ───────────────────────────────────────────────


def test_numeric_and_tiny_names_are_not_evidence() -> None:
    """normalize_name strips punctuation, so '$85', '85' and '85%' all
    normalize to '85' and '$BE' collapses onto the ticker 'BE'."""
    assert _name_is_evidence("85") is False
    assert _name_is_evidence("be") is False
    assert _name_is_evidence("xai") is True
    assert _name_is_evidence("spacex") is True
    # An identical numeric name is still fine for exact_name — it is the
    # *squash* that conflates them, and that rule excludes digits outright.
    assert _name_is_evidence("2 5 billion") is True
    assert _HAS_DIGIT_RE.search("2 5 billion".replace(" ", "")) is not None


def test_identity_key_rejects_synthetic_fec_ids() -> None:
    """The disbursement ingester mints 'unknown-<hash>' placeholders for
    unresolvable recipients; treating one as a committee id retyped OpenAI
    as a pac."""
    assert _identity_key("fec_committee", "unknown-4062420261522640068") is None
    assert _identity_key("fec_committee", "C00142711") == "C00142711"
    assert _identity_key("sec_cik", "0000923796") == "923796"
    assert _identity_key("fec_candidate", "not-an-id") is None


# ─── prediction ─────────────────────────────────────────────────────────


def test_predict_counts_drops_collisions_and_self_loops() -> None:
    ents = [
        _e("keep", "Starlink", "starlink", "concept", edges=8),
        _e("drop", "Starlink", "starlink", "unknown", edges=1),
        _e("other", "Musk", "musk", "organization", edges=2),
    ]
    plan = _plan(ents, [Candidate("keep", "drop", "exact_name", "norm")])
    edges = [
        # keep—drop becomes a self-loop once the two collapse.
        ("keep", "drop", "mentioned_with", 3),
        # both endpoints point at `other` with the same relation → collide.
        ("keep", "other", "mentioned_with", 5),
        ("drop", "other", "mentioned_with", 2),
    ]
    p = predict({e.id: e for e in ents}, edges, plan)

    assert p["entities_before"] == 3
    assert p["entities_after"] == 2
    assert p["self_loops_created"] == 1
    assert p["citations_on_created_self_loops"] == 3
    assert p["edges_collapsed_into_existing"] == 1
    assert p["edges_after"] == 1
    # Every receipt survives except the ones on the removed self-loop.
    assert p["citations_before"] == 10
    assert p["citations_after"] == 7


def test_predict_reports_the_unknown_percentage_move() -> None:
    ents = [
        _e("keep", "SpaceX", "spacex", "organization", edges=110),
        _e("drop", "SpaceX", "spacex", "unknown"),
    ]
    plan = _plan(ents, [Candidate("keep", "drop", "exact_name", "norm")])
    p = predict({e.id: e for e in ents}, [], plan)
    assert p["unknown_before"] == 1
    assert p["unknown_after"] == 0
    assert p["unknown_pct_before"] == 50.0
    assert p["unknown_pct_after"] == 0.0


# ─── idempotency ────────────────────────────────────────────────────────


def test_replanning_after_a_merge_proposes_nothing() -> None:
    """Idempotence: once the dropped canonicals are gone, the same corpus
    yields no further merges."""
    ents = [
        _e("keep", "SpaceX", "spacex", "organization", edges=110),
        _e("drop", "SpaceX", "spacex", "unknown"),
    ]
    first = _plan(ents, [Candidate("keep", "drop", "exact_name", "norm")])
    assert len(first.clusters) == 1

    survivors = [e for e in ents if e.id == "keep"]
    second = build_plan(
        {e.id: e for e in survivors}, [], set(DEFAULT_APPLIED_RULES)
    )
    assert second.clusters == []


def test_survivor_choice_is_stable_across_runs() -> None:
    """Ties are broken deterministically so a re-run picks the same
    survivor and the merge stays convergent."""
    ents = [
        _e("bbb", "Acme", "acme", "organization"),
        _e("aaa", "Acme", "acme", "organization"),
    ]
    for _ in range(3):
        plan = _plan(ents, [Candidate("aaa", "bbb", "exact_name", "norm")])
        assert plan.clusters[0].survivor.id == "aaa"


def test_a_rule_not_in_the_applied_set_only_reaches_the_review_list() -> None:
    ents = [
        _e("a", "Palantir Technologies Inc.", "palantir technologies",
           "organization", edges=63),
        _e("b", "PALANTIR TECHNOLOGIES IN", "palantir technologies in",
           "organization", edges=1),
    ]
    plan = _plan(ents, [Candidate("a", "b", "prefix_variant", "tail 'in'")])
    assert plan.clusters == []
    assert plan.review[0].reason == "rule_not_enabled:prefix_variant"


# ─── helen 2026-08-21 review decisions ──────────────────────────────────


def test_concept_to_org_denylist_blocks_the_known_mistypings() -> None:
    """helen: keep the concept→organization retype ON (it fixes xAI /
    Starlink / OpenAI) but do not propagate the four upstream mis-typings.
    They still MERGE — only the bad type is withheld, and the cluster is
    flagged."""
    for squashed_name in ("401k", "adr", "aietf", "chipmakers"):
        members = [
            _e("a", squashed_name, squashed_name, "concept", edges=3),
            _e("b", squashed_name, squashed_name, "organization"),
        ]
        assert _resolve_type(members, members[0]) == ("concept", True)


def test_denylist_matches_the_squashed_form_not_just_the_exact_name() -> None:
    """'401(k)' normalizes to '401 k' and 'chip makers' to 'chip makers';
    both squash onto the denylist entry."""
    members = [
        _e("a", "401(k)", "401 k", "concept", edges=2),
        _e("b", "401k", "401k", "organization"),
    ]
    assert _resolve_type(members, members[0]) == ("concept", True)


def test_org_concept_retype_still_fires_for_everything_else() -> None:
    members = [
        _e("a", "Starlink", "starlink", "concept", edges=8),
        _e("b", "Starlink", "starlink", "organization", edges=1),
    ]
    assert _resolve_type(members, members[0]) == ("organization", False)


def test_curated_prefix_rule_is_applied_and_the_general_one_is_not() -> None:
    """helen: enable prefix_variant ONLY for the two Palantir OCR mangles."""
    assert "prefix_variant_curated" in DEFAULT_APPLIED_RULES
    assert "prefix_variant" not in DEFAULT_APPLIED_RULES

    ents = [
        _e("a", "Palantir Technologies Inc.", "palantir technologies",
           "organization", edges=63, aliases=304),
        _e("b", "PALANTIR TECHNOLOGIES IN", "palantir technologies in",
           "organization", edges=1),
    ]
    plan = _plan(
        ents, [Candidate("a", "b", "prefix_variant_curated", "tail 'in'")]
    )
    assert len(plan.clusters) == 1
    assert plan.clusters[0].survivor.id == "a"
    assert [d.id for d in plan.clusters[0].dropped] == ["b"]


def test_curated_prefix_list_holds_exactly_the_two_reviewed_pairs() -> None:
    """The allowlist is the whole safety argument for enabling this rule —
    it must not drift open."""
    assert _PREFIX_CURATED == frozenset({
        ("palantir technologies", "palantir technologies in"),
        ("palantir technologies", "palantir technologies inclass"),
    })


def test_postconditions_fail_when_a_protected_scrutiny_row_is_lost() -> None:
    """Losing the privacy-audit row of a suppress/alias canonical is a
    privacy incident, and the post-merge check must say so out loud."""
    before = {
        "entities": 100, "uncited_edges": 0, "orphan_citations": 0,
        "surface_mode_counts": {"open": 90, "suppress": 10},
        "scrutiny": {"total": 50, "by_surface_mode": {"open": 40, "suppress": 10}},
    }
    after = {
        "entities": 98, "uncited_edges": 0, "orphan_citations": 0,
        "surface_mode_counts": {"open": 88, "suppress": 10},
        "scrutiny": {"total": 49, "by_surface_mode": {"open": 40, "suppress": 9}},
    }
    result = _check_postconditions(before, after)
    assert result["all_passed"] is False
    assert result["checks"]["no_protected_scrutiny_rows_lost"] is False
    assert result["checks"]["scrutiny_rows_preserved"] is False
    assert result["protected_scrutiny_lost"]["suppress"] == 1


def test_postconditions_pass_on_a_clean_merge() -> None:
    before = {
        "entities": 100, "uncited_edges": 0, "orphan_citations": 0,
        "surface_mode_counts": {"open": 90, "suppress": 10},
        "scrutiny": {"total": 50, "by_surface_mode": {"open": 40, "suppress": 10}},
    }
    after = {
        "entities": 98, "uncited_edges": 0, "orphan_citations": 0,
        "surface_mode_counts": {"open": 88, "suppress": 10},
        "scrutiny": {"total": 50, "by_surface_mode": {"open": 40, "suppress": 10}},
    }
    assert _check_postconditions(before, after)["all_passed"] is True


def test_postconditions_fail_if_a_node_changed_surface_mode() -> None:
    """A merge must never move a canonical between surface_modes — the
    per-mode counts may only shrink."""
    before = {
        "entities": 100, "uncited_edges": 0, "orphan_citations": 0,
        "surface_mode_counts": {"open": 90, "suppress": 10},
        "scrutiny": {"total": 50, "by_surface_mode": {}},
    }
    after = {
        "entities": 100, "uncited_edges": 0, "orphan_citations": 0,
        "surface_mode_counts": {"open": 89, "suppress": 11},
        "scrutiny": {"total": 50, "by_surface_mode": {}},
    }
    assert _check_postconditions(before, after)["all_passed"] is False


# ─── Neo4j projection prune (post-merge staleness) ──────────────────────


def test_projection_prune_deletes_only_what_postgres_no_longer_has() -> None:
    """project_entity/project_edge are MERGE-only, so a canonical the dedup
    pass merged away would linger in Neo4j and keep serving the duplicate.
    The sweep must prune by "not in Postgres" — never by "not projected",
    which would make every suppress node (deliberately unprojected) a
    deletion target."""
    from app.services.graph.neo4j_projection import Neo4jProjection

    ran: list[tuple[str, dict]] = []

    class _FakeProjection(Neo4jProjection):
        def __init__(self):  # noqa: D107 - test double
            pass

        @property
        def available(self) -> bool:
            return True

        def _run(self, cypher, **params):
            ran.append((cypher, params))
            return [{"n": 7}]

    nodes, rels = _FakeProjection().prune_missing({"keep"}, {"e1"})
    assert (nodes, rels) == (7, 7)

    rel_cypher, rel_params = ran[0]
    node_cypher, node_params = ran[1]
    assert "NOT r.pg_id IN $ids" in rel_cypher and rel_params["ids"] == ["e1"]
    assert "NOT c.pg_id IN $ids" in node_cypher and node_params["ids"] == ["keep"]
    # Relationships first, then DETACH DELETE the nodes.
    assert "DETACH DELETE" in node_cypher


def test_projection_prune_is_a_noop_without_a_driver() -> None:
    from app.services.graph.neo4j_projection import Neo4jProjection

    class _Unavailable(Neo4jProjection):
        def __init__(self):  # noqa: D107 - test double
            pass

        @property
        def available(self) -> bool:
            return False

    assert _Unavailable().prune_missing({"a"}, {"b"}) == (0, 0)
