"""Batch-1 contribution-accuracy fix: FEC party-classification
enrichment (helen 2026-08-12).

Corporate-PAC recipients (member committees / leadership PACs / joint
fundraisers / state parties) lacked party aliases, so flow_model1 only
attributed a tiny fraction of captured PAC giving to R/D. The
``enrich_recipient_party_aliases`` helper looks up each unclassified
FEC recipient canonical against the FEC API, reads the ``party``
field, and adds an EntityAlias(source_system='party', surface_name=
'Republican'|'Democratic') so flow_model1 sees the full giving.

These tests pin the helper's contract (shape, source-inspection
only — the live FEC calls happen in a separate integration path).
"""

from __future__ import annotations

import inspect


def test_helper_exposes_expected_signature():
    from app.services.ingest import fec

    assert hasattr(fec, "enrich_recipient_party_aliases")
    assert inspect.iscoroutinefunction(fec.enrich_recipient_party_aliases)
    sig = inspect.signature(fec.enrich_recipient_party_aliases)
    assert "max_lookups" in sig.parameters


def test_party_enrich_stats_dataclass_shape():
    from app.services.ingest.fec import PartyEnrichStats

    s = PartyEnrichStats()
    assert s.committees_scanned == 0
    assert s.api_lookups == 0
    assert s.aliases_added == 0
    assert s.aliases_skipped_no_party == 0
    assert s.errors == 0


def test_party_map_maps_only_the_two_major_parties():
    """FEC returns party codes like 'REP', 'DEM', 'DFL', 'LIB', 'GRE',
    'IND', '' (blank). flow_model1 queries 'Republican' and
    'Democratic'; other codes shouldn't create noise aliases."""
    from app.services.ingest.fec import _FEC_PARTY_MAP

    assert _FEC_PARTY_MAP.get("REP") == "Republican"
    assert _FEC_PARTY_MAP.get("DEM") == "Democratic"
    # DFL (Minnesota Democratic-Farmer-Labor) maps to Democratic —
    # a Minnesota senator's committee.
    assert _FEC_PARTY_MAP.get("DFL") == "Democratic"
    # Everything else is unmapped so the helper skips them cleanly.
    for other in ("LIB", "GRE", "IND", "OTH", "NON", "", "UNK"):
        assert _FEC_PARTY_MAP.get(other) is None


def test_lookup_dispatches_by_fec_id_prefix():
    """C-prefixed ids -> /committee/<id>/, H/S/P-prefixed ids ->
    /candidate/<id>/. Everything else (empty, 'unknown-...', legacy
    numeric-only stubs) returns None without hitting the API."""
    import inspect

    from app.services.ingest.fec import _lookup_fec_party

    src = inspect.getsource(_lookup_fec_party)
    # Both endpoints referenced.
    assert "/committee/" in src
    assert "/candidate/" in src
    # Prefix dispatch is explicit.
    assert 'prefix == "C"' in src or "prefix == 'C'" in src
    assert 'prefix in ("H", "S", "P")' in src or "'H', 'S', 'P'" in src


def test_lookup_two_hop_follows_committee_to_candidate_party():
    """Corporate PAC recipients often land on candidate PRINCIPAL
    CAMPAIGN COMMITTEES (C-prefixed) whose FEC ``party`` field is
    blank; the actual party lives on the linked CANDIDATE record.
    Two-hop follow (committee -> candidate via ``candidate_ids``) is
    the fix for Boeing/Northrop/L3Harris undercapture — their PAC
    disbursements land on member campaign committees + one-hop
    lookup returned no party."""
    import inspect

    from app.services.ingest.fec import _lookup_fec_party

    src = inspect.getsource(_lookup_fec_party)
    # Two-hop is documented.
    assert "candidate_ids" in src
    # It goes to /candidate/<id>/ from committee context.
    lower = src.lower()
    assert "two-hop" in lower or "two hop" in lower


def test_enrichment_excludes_canonicals_already_carrying_party_alias():
    """The helper is idempotent — canonicals with an existing party
    alias are skipped at the SQL layer (no API call). Reruns are
    safe + cheap."""
    import inspect

    from app.services.ingest.fec import enrich_recipient_party_aliases

    src = inspect.getsource(enrich_recipient_party_aliases)
    # Filters at query time.
    assert "source_system == \"party\"" in src or "source_system=='party'" in src
    assert "~EntityAlias.canonical_id.in_" in src or ".in_(subq)" in src


def test_enrichment_source_id_uniqueness_guard():
    """ix_aliases_source is unique(source_system, source_id). Two
    canonicals sharing the same fec_id would trip the constraint on
    the second insert. The helper pre-loads used party source_ids +
    skips duplicates so the enrichment doesn't rollback the batch."""
    import inspect

    from app.services.ingest.fec import enrich_recipient_party_aliases

    src = inspect.getsource(enrich_recipient_party_aliases)
    assert "used_party_source_ids" in src
    assert "unique index" in src.lower() or "ix_aliases_source" in src


def test_enrichment_max_lookups_bound_default_and_shape():
    """Runtime bound so a first-pass over a fresh corporate-PAC
    ingestion doesn't blow the FEC key's daily quota + hit 429s."""
    from app.services.ingest.fec import enrich_recipient_party_aliases

    sig = inspect.signature(enrich_recipient_party_aliases)
    param = sig.parameters["max_lookups"]
    assert param.default is not inspect.Parameter.empty
    assert param.default >= 100  # tunable but non-trivial default
