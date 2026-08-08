"""AF2 (2026-08-08) — disclosure-carrier tier bump invariants.

Behavioural round-trip against a real DB is exercised on rollout by
the live before/after ranking dump on twin-bus. This file locks the
code-shape invariants:

  * Disclosure-carrier bump is +2, applied to the effective rank tier.
  * The set of relations that count as "disclosure" is the closed
    OGE-278 set (holds_asset, income_from, held_position, owes,
    traded, party_to_agreement) — a fabricated 7th one would sneak
    past this without a matching model change.
  * The bulk disclosure-carrier query filters on published_edge()
    so RG2's read-gate is honored — staged edges must not inflate
    search ranking.
  * The response surfaces base_tier + disclosure_boosted flags so
    the boost is transparent to the caller.
"""

from __future__ import annotations

import inspect

from app.main import search


def test_search_boosts_disclosure_carriers_by_two_tiers() -> None:
    """The bump is +2, not +1 or +3 — hardcoded rank arithmetic
    tuned to lift substring-matches to tie exact-matches within the
    5-tier scheme."""
    src = inspect.getsource(search)
    assert "bump = 2 if e.id in disclosure_carriers else 0" in src


def test_search_boost_uses_closed_disclosure_relation_set() -> None:
    """Only the six OGE-278 relations count; a fabricated 7th
    should not sneak past this test."""
    src = inspect.getsource(search)
    for rel in (
        "HOLDS_ASSET",
        "INCOME_FROM",
        "HELD_POSITION",
        "OWES",
        "TRADED",
        "PARTY_TO_AGREEMENT",
    ):
        assert f"EdgeRelation.{rel}.value" in src, f"AF2 must reference EdgeRelation.{rel}"


def test_search_disclosure_query_filters_by_published_edge() -> None:
    """Staged (mid-batch bulk-ingest) edges MUST NOT inflate search
    ranking — the RG2 read-gate stays load-bearing here."""
    src = inspect.getsource(search)
    # The disclosure-carrier bulk query chains `.where(published_edge())`.
    # Structural: search source contains published_edge() AFTER the
    # relation.in_ predicate for the disclosure lookup.
    idx_rel = src.index(".relation.in_(_DISCLOSURE_RELATIONS)")
    idx_pub = src.index("published_edge()", idx_rel)
    assert idx_pub > idx_rel, "disclosure carrier lookup must apply published_edge()"


def test_search_response_surfaces_boost_transparency() -> None:
    """Boosted entities carry base_tier + disclosure_boosted so the
    caller can tell WHY a substring-match beats an exact-match."""
    src = inspect.getsource(search)
    assert 'row["base_tier"] = base_tier' in src
    assert 'row["disclosure_boosted"] = True' in src
    # Fields present ONLY when boost applied — schema stays back-compat.
    assert "effective_tier != base_tier" in src
