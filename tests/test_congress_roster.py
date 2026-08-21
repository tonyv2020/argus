"""Congress roster ingester shape tests (P4 D + P1.5).

Hermetic — no HTTP + no DB. Covers:

* Term extraction picks the most-recent-start term.
* Name variant assembly captures FEC's LAST, FIRST + news's First Last
  + the nickname shape ("Bernie Sanders").
* FEC candidate id extraction handles both list + str shapes.
* Notes carry chamber/state/party/district for P5 flow filters.
* P1.5 — MemberRecord parsing, stable variant alias keys, and the
  FAIL-CLOSED name-match guard that decides whether a member identity
  may be attached to a pre-existing node.
"""

from __future__ import annotations

import pytest

from app.models import SourceKind
from app.services.ingest.congress_roster import (
    CHAMBER_ENTITIES,
    NameMatchCandidate,
    _extract_current_term,
    _fec_candidate_ids,
    _label_for,
    _name_variants,
    name_match_allowed,
    parse_member,
    variant_alias_source_id,
)


def test_extract_current_term_picks_most_recent_start() -> None:
    """A member with multiple terms uses the most-recently-started one —
    a re-elected senator's current term is the last entry."""
    member = {
        "terms": [
            {"start": "2013-01-03", "type": "sen"},
            {"start": "2019-01-03", "type": "sen"},
            {"start": "2025-01-03", "type": "sen"},
        ]
    }
    term = _extract_current_term(member)
    assert term["start"] == "2025-01-03"


def test_extract_current_term_returns_none_on_empty() -> None:
    """A row with no terms slot returns None so the caller can skip."""
    assert _extract_current_term({}) is None
    assert _extract_current_term({"terms": []}) is None


def test_label_prefers_official_full_over_first_last() -> None:
    """The dataset carries an ``official_full`` — use it when present
    (matches ProPublica / roll-call formats)."""
    m = {"name": {"first": "Alexandria", "last": "Ocasio-Cortez",
                   "official_full": "Alexandria Ocasio-Cortez"}}
    assert _label_for(m) == "Alexandria Ocasio-Cortez"


def test_label_falls_back_to_first_last() -> None:
    """Older rows may lack official_full; construct from first + last."""
    m = {"name": {"first": "Test", "last": "Person"}}
    assert _label_for(m) == "Test Person"


def test_name_variants_include_fec_and_news_shapes() -> None:
    """FEC candidate names are ``LAST, FIRST``; news is ``First Last``.
    Both must be in name_variants so the alias-crosswalk merges the
    fragmented news-person nodes into the canonical member."""
    m = {"name": {"first": "Ted", "last": "Cruz",
                   "official_full": "Ted Cruz"}}
    variants = _name_variants(m)
    assert "Cruz, Ted" in variants
    assert "Ted Cruz" in variants


def test_name_variants_include_nickname_shape() -> None:
    """P1.5 — news prints "Bernie Sanders", the dataset's official_full
    is "Bernard Sanders". Without the nickname+last variant the news
    fragment never resolves to the member."""
    m = {
        "name": {
            "first": "Bernard",
            "last": "Sanders",
            "nickname": "Bernie",
            "official_full": "Bernard Sanders",
        }
    }
    assert "Bernie Sanders" in _name_variants(m)


def test_name_variants_dedupe_preserving_order() -> None:
    """official_full == "First Last" must not yield the variant twice —
    the alias key is derived from the variant, and a duplicate would
    collide on the (source_system, source_id) unique index."""
    m = {"name": {"first": "Jane", "last": "Doe", "official_full": "Jane Doe"}}
    variants = _name_variants(m)
    assert len(variants) == len(set(variants))


def test_fec_candidate_ids_accepts_str_and_list() -> None:
    """A member with several runs carries a LIST of fec ids; older rows
    carry a bare string."""
    assert _fec_candidate_ids({"id": {"fec": "S8TX00232"}}) == ["S8TX00232"]
    assert _fec_candidate_ids(
        {"id": {"fec": ["S8TX00232", "H2TX00001"]}}
    ) == ["S8TX00232", "H2TX00001"]
    assert _fec_candidate_ids({"id": {}}) == []


# ─── P1.5 — MemberRecord ────────────────────────────────────────────────


def _raw(**over):
    base = {
        "id": {"bioguide": "C001098", "fec": ["S2TX00312"]},
        "name": {"first": "Ted", "last": "Cruz", "official_full": "Ted Cruz"},
        "terms": [
            {"start": "2013-01-03", "end": "2019-01-03", "type": "sen",
             "state": "TX", "party": "Republican"},
            {"start": "2025-01-03", "end": "2031-01-03", "type": "sen",
             "state": "TX", "party": "Republican"},
        ],
    }
    base.update(over)
    return base


def test_parse_member_flattens_current_term() -> None:
    """The record carries the ACTIVE term's chamber/state/party, not the
    first one in the list."""
    rec = parse_member(_raw())
    assert rec is not None
    assert rec.bioguide == "C001098"
    assert rec.chamber == "sen"
    assert rec.state == "TX"
    assert rec.party == "Republican"
    assert rec.term_start == "2025-01-03"
    assert rec.fec_candidate_ids == ("S2TX00312",)


def test_parse_member_notes_carry_flow_filter_fields() -> None:
    """P5 flow queries filter on the notes string without decoding JSONB."""
    rec = parse_member(_raw())
    assert "chamber=sen" in rec.notes
    assert "state=TX" in rec.notes
    assert "bioguide=C001098" in rec.notes


def test_parse_member_house_notes_carry_district() -> None:
    """A representative's notes include the district number."""
    rec = parse_member(
        _raw(terms=[{"start": "2025-01-03", "type": "rep", "state": "NY",
                     "party": "Democrat", "district": 14}])
    )
    assert "state=NY-14" in rec.notes
    assert rec.district == 14


def test_parse_member_requires_bioguide() -> None:
    """No authoritative id → no member. The pass never invents identity
    from a name alone."""
    assert parse_member(_raw(id={"fec": ["S2TX00312"]})) is None


def test_parse_member_requires_a_term() -> None:
    """A row with no terms can't be placed in a chamber → skipped."""
    assert parse_member(_raw(terms=[])) is None


def test_bioguide_url_is_the_citation_target() -> None:
    """Every roster edge cites the member's Biographical Directory page."""
    rec = parse_member(_raw())
    assert rec.bioguide_url == (
        "https://bioguide.congress.gov/search/bio/C001098"
    )


def test_variant_alias_source_id_is_stable_and_namespaced() -> None:
    """Idempotency depends on the key being deterministic across runs;
    the bioguide prefix keeps two members sharing a variant apart."""
    a = variant_alias_source_id("C001098", "Ted Cruz")
    assert a == variant_alias_source_id("C001098", "Ted  Cruz")
    assert a != variant_alias_source_id("S000033", "Ted Cruz")
    assert len(a) <= 64


def test_variant_alias_source_id_fits_the_column() -> None:
    """source_id is varchar(64) — a long official_full must not overflow."""
    key = variant_alias_source_id("X000001", "A" * 200)
    assert len(key) == 64


def test_congress_roster_source_kind_exists() -> None:
    """Roster edges get their own citation kind so the UI can label the
    click-through as a bioguide entry."""
    assert SourceKind.CONGRESS_ROSTER.value == "congress_roster"


def test_chamber_entities_cover_both_chambers() -> None:
    """held_position targets exist for senators and representatives."""
    assert set(CHAMBER_ENTITIES) == {"sen", "rep"}


# ─── P1.5 — FAIL-CLOSED name fallback ───────────────────────────────────


def _cand(**over) -> NameMatchCandidate:
    base = dict(
        id="frag",
        name="Ted Cruz",
        type="person",
        surface_mode="open",
        publication_state="published",
        namespaces=frozenset(),
        bioguides=frozenset(),
    )
    base.update(over)
    return NameMatchCandidate(**base)


def test_name_match_allows_an_unclaimed_open_person_fragment() -> None:
    """The intended case: a news-person node with the member's exact
    name and no competing identity → the member claims it."""
    ok, reason = name_match_allowed(_cand(), "C001098")
    assert ok is True
    assert reason == "ok"


@pytest.mark.parametrize("mode", ["suppress", "alias"])
def test_name_match_refuses_protected_nodes(mode: str) -> None:
    """THE non-negotiable rule. A member identity is never attached to a
    protected node — that either mislabels a private person as a member
    or relaxes their protection on the way to publication."""
    ok, reason = name_match_allowed(_cand(surface_mode=mode), "C001098")
    assert ok is False
    assert reason == "surface_mode_not_open"


def test_name_match_refuses_non_person_types() -> None:
    """"Senate" the organization is not a member."""
    ok, reason = name_match_allowed(_cand(type="organization"), "C001098")
    assert ok is False
    assert reason == "type_not_person"


def test_name_match_refuses_a_different_members_bioguide() -> None:
    """Two members can share a surface name; the id decides. A node
    already claimed by another bioguide is a different real person."""
    ok, reason = name_match_allowed(
        _cand(namespaces=frozenset({"bioguide"}),
              bioguides=frozenset({"S000033"})),
        "C001098",
    )
    assert ok is False
    assert reason == "foreign_bioguide"


def test_name_match_allows_its_own_bioguide() -> None:
    """Re-running the pass over a node this member already owns is not a
    conflict (the bioguide branch normally short-circuits first)."""
    ok, _ = name_match_allowed(
        _cand(namespaces=frozenset({"bioguide"}),
              bioguides=frozenset({"C001098"})),
        "C001098",
    )
    assert ok is True


@pytest.mark.parametrize(
    "ns", ["fec.candidate", "fec.committee", "sec.cik", "senate_lda.registrant"]
)
def test_name_match_refuses_foreign_authoritative_ids(ns: str) -> None:
    """A node carrying someone else's authoritative id names a different
    real entity — a name collision must never override an id."""
    ok, reason = name_match_allowed(
        _cand(namespaces=frozenset({ns})), "C001098"
    )
    assert ok is False
    assert reason.startswith("foreign_external_id")


def test_name_match_ignores_non_authoritative_namespaces() -> None:
    """News aliases (hollywood.entity_tags) are not identity claims —
    a pure news fragment is exactly what we want to claim."""
    ok, _ = name_match_allowed(
        _cand(namespaces=frozenset({"hollywood.entity_tags"})), "C001098"
    )
    assert ok is True
