"""P1 — anchor-set shape tests for FEC + USAspending.

Direct network calls stay in integration land; here we prove the
detention-industry anchor sets are well-formed and the parameterized
entrypoints exist + accept the shape their CLI dispatcher passes in.
"""

from __future__ import annotations

import inspect

from app.services.ingest import fec, usaspending


def test_fec_anchor_set_contains_expected_labels():
    labels = set(fec.DETENTION_INDUSTRY_PACS)
    assert labels >= {
        "GEO Group",
        "CoreCivic",
        "Management & Training Corp",
        "LaSalle Corrections",
    }


def test_fec_anchor_entries_shape():
    for label, entry in fec.DETENTION_INDUSTRY_PACS.items():
        assert isinstance(entry.get("queries"), tuple), label
        assert isinstance(entry.get("match"), tuple), label
        assert all(isinstance(q, str) for q in entry["queries"]), label
        assert all(isinstance(t, str) for t in entry["match"]), label
        assert entry["queries"], label
        assert entry["match"], label


def test_fec_parameterized_apis_exist():
    for name in ("find_pac_by_queries", "ingest_pac",
                 "ingest_detention_industry_pacs"):
        assert hasattr(fec, name), name
    # ingest_pac signature — display_label + queries + match_tokens are
    # what the CLI hands in.
    sig = inspect.signature(fec.ingest_pac)
    assert {"queries", "match_tokens", "display_label"} <= set(sig.parameters)


def test_usaspending_anchor_set_contains_expected_labels():
    labels = set(usaspending.DETENTION_INDUSTRY_RECIPIENTS)
    assert labels >= {
        "GEO Group",
        "CoreCivic",
        "Management & Training Corp",
        "LaSalle Corrections",
    }


def test_usaspending_anchor_entries_shape():
    for label, entry in usaspending.DETENTION_INDUSTRY_RECIPIENTS.items():
        assert isinstance(entry.get("recipient_names"), tuple), label
        assert entry["recipient_names"], label
        assert isinstance(entry.get("canonical_hint"), str), label
        assert entry["canonical_hint"], label


def test_usaspending_parameterized_apis_exist():
    for name in ("_find_recipient_canonical", "ingest_recipient_contracts",
                 "ingest_detention_industry_contracts"):
        assert hasattr(usaspending, name), name
    sig = inspect.signature(usaspending.ingest_recipient_contracts)
    assert {"recipient_names", "canonical_hint",
            "display_label"} <= set(sig.parameters)


def test_backcompat_geo_wrappers_present():
    """The pre-P1 GEO-only wrappers must still exist so existing
    callers / cron scripts keep working."""
    assert callable(fec.ingest_geo_group_pac)
    assert callable(fec.find_geo_group_pac)
    assert callable(usaspending.ingest_geo_group_contracts)
    assert callable(usaspending._find_geo_group_canonical)


def test_corecivic_has_historical_cca_alias():
    """CoreCivic PAC was CCA PAC before the 2016 rename — the queries
    must cover both."""
    cc = fec.DETENTION_INDUSTRY_PACS["CoreCivic"]
    joined = " ".join(cc["queries"]).upper()
    assert "CORECIVIC" in joined
    assert "CCA" in joined or "CORRECTIONS CORPORATION" in joined
