"""P5.2 + P5.3 + broaden-USAspending shape tests.

Live behavior is validated in a follow-on sweep + endpoint hit; these
tests guard the wiring: party alias attach path, party-committee seed
presence, broaden_agency_scope flag, Model 1 flow query surface.
"""

from __future__ import annotations

import inspect

from app.services.ingest import congress_roster, seed_anchor_registry, usaspending


def test_roster_stats_carries_party_counter() -> None:
    """RosterStats surfaces the P5.2 party-alias-created counter."""
    stats = congress_roster.RosterStats()
    assert hasattr(stats, "party_aliases_created")
    assert stats.party_aliases_created == 0


def test_roster_attaches_party_as_entityalias_not_new_canonical() -> None:
    """Party must be attached as an EntityAlias on the MEMBER canonical,
    NOT via _upsert_entity which would create a new canonical named
    "Democratic"."""
    src = inspect.getsource(congress_roster.ingest_roster)
    # EntityAlias direct attach path present.
    assert "EntityAlias(" in src
    assert 'source_system="party"' in src
    # Idempotency guard — dedupe by (canonical_id, source_system, source_id).
    assert 'source_system == "party"' in src


def test_party_committees_seeded_in_registry() -> None:
    """NRSC/NRCC/DSCC/DCCC are in the P4 seed with their FEC
    committee ids + party notes."""
    labels = {r.label for r in seed_anchor_registry._PARTY_COMMITTEES}
    assert labels == {
        "National Republican Senatorial Committee",
        "National Republican Congressional Committee",
        "Democratic Senatorial Campaign Committee",
        "Democratic Congressional Campaign Committee",
    }
    for r in seed_anchor_registry._PARTY_COMMITTEES:
        assert r.priority_domain == "party_committees"
        assert r.fec_committee_ids and r.fec_committee_ids[0].startswith("C")
        assert "party=" in r.notes


def test_all_seed_includes_party_committees() -> None:
    """Party committees must appear in _ALL_SEED so the seed script
    picks them up."""
    labels_all = {r.label for r in seed_anchor_registry._ALL_SEED}
    assert "National Republican Senatorial Committee" in labels_all
    assert "Democratic Congressional Campaign Committee" in labels_all


def test_usaspending_broaden_flag_wired_through_dispatcher() -> None:
    """`ingest_from_registry(broaden_agency_scope=True)` must thread
    the flag into `ingest_recipient_contracts` — else per-anchor
    calls don't broaden and Tesla/SpaceX NASA/DoD contracts miss."""
    src = inspect.getsource(usaspending.ingest_from_registry)
    assert "broaden_agency_scope=broaden_agency_scope" in src


def test_usaspending_broadened_mode_accepts_any_sub_agency() -> None:
    """In broadened mode, an award that misses ICE/BOP/USMS still lands
    an edge — the sub-agency string becomes the anchor label."""
    src = inspect.getsource(usaspending.ingest_recipient_contracts)
    assert "broaden_agency_scope" in src
    assert "if not broaden_agency_scope" in src, (
        "the broaden path must be an OPT-IN branch, not the default"
    )


def test_flow_query_module_exposes_model1_and_summary() -> None:
    """The P5.3 flow-query surface exists + returns a summary shape."""
    from app.services import flow_query

    assert hasattr(flow_query, "model1_flow")
    assert inspect.iscoroutinefunction(flow_query.model1_flow)
    assert hasattr(flow_query, "FlowSummary")
    assert hasattr(flow_query, "FlowRow")


def test_flow_query_uses_party_alias_lookup_not_hardcoded_labels() -> None:
    """Recipient-party detection MUST route through the party alias
    table — hardcoding member names would break as the roster changes."""
    from app.services import flow_query

    src = inspect.getsource(flow_query._party_member_ids)
    assert "EntityAlias" in src
    assert '"party"' in src or "'party'" in src


def test_model1_flow_returns_empty_summary_when_no_recipients() -> None:
    """A party with no members + no committees returns an empty summary
    with 0 totals, not a crash."""
    import asyncio

    from app.services.flow_query import FlowSummary, model1_flow

    class _FakeExec:
        def scalars(self):
            return _Empty()

        def all(self):
            return []

    class _Empty:
        def all(self):
            return []

    class _Session:
        async def execute(self, *args, **kwargs):
            return _FakeExec()

    async def run():
        return await model1_flow(_Session(), party="MadeUpParty")

    summary = asyncio.run(run())
    assert isinstance(summary, FlowSummary)
    assert summary.rows == []
    assert summary.total_contrib == 0.0
    assert summary.n_contributors == 0
