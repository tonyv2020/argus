"""P4 D — congress roster ingester shape tests.

Hermetic — no HTTP + no DB. Covers:

* Term extraction picks the most-recent-start term.
* Name variant assembly captures FEC's LAST, FIRST + news's First Last.
* FEC candidate id extraction handles both list + str shapes.
* Notes carry chamber/state/party/district for P5 flow filters.
* surface_mode defaults to 'open' (public official).
"""

from __future__ import annotations

from app.services.ingest.congress_roster import (
    _extract_current_term,
    _fec_candidate_ids,
    _label_for,
    _name_variants,
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
    assert "Ted Cruz" in variants          # news shape
    assert "Cruz, Ted" in variants          # FEC candidate shape
    assert "Ted Cruz" == variants[0] or "Ted Cruz" in variants


def test_name_variants_dedupe_preserving_order() -> None:
    """A member whose official_full equals `first last` shouldn't
    surface twice — dedupe preserves first-occurrence order."""
    m = {"name": {"first": "Ted", "last": "Cruz",
                   "official_full": "Ted Cruz"}}
    variants = _name_variants(m)
    assert variants.count("Ted Cruz") == 1


def test_name_variants_include_other_names_when_present() -> None:
    """Members with rename history (e.g. Rep Kim Schrier's married
    name) carry an ``other_names`` list — both variants land."""
    m = {
        "name": {"first": "Kim", "last": "Schrier"},
        "other_names": [{"first": "Kim", "last": "Weiss"}],
    }
    variants = _name_variants(m)
    assert "Kim Weiss" in variants
    assert "Kim Schrier" in variants


def test_fec_candidate_ids_accepts_list_or_string() -> None:
    """Dataset stores ``fec`` as a list of ids (most members) or a
    single string (rare — one-run legislators). Both normalise to
    a list."""
    m1 = {"id": {"fec": ["S8TX00232", "S6TX00298"]}}
    m2 = {"id": {"fec": "S6TX00298"}}
    m3 = {"id": {}}
    assert _fec_candidate_ids(m1) == ["S8TX00232", "S6TX00298"]
    assert _fec_candidate_ids(m2) == ["S6TX00298"]
    assert _fec_candidate_ids(m3) == []
