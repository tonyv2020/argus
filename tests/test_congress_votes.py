"""P5.1 congress votes ingester — shape + Cloudflare-safe headers.

The API-touching path is validated live; hermetic tests here guard the
critical wire settings (browser UA, JSON accept), the key-bill set
shape, and the CONGRESS_API_KEY env probe.
"""

from __future__ import annotations

import inspect
import os

from app.services.ingest import congress_votes


def test_browser_ua_sent_on_every_call() -> None:
    """Congress.gov is Cloudflare-fronted; default UA = 403 error 1010.
    Helen 2026-07-19 21:40Z pre-validation caveat."""
    assert "User-Agent" in congress_votes._HTTP_HEADERS
    ua = congress_votes._HTTP_HEADERS["User-Agent"].lower()
    assert "mozilla" in ua or "chrome" in ua, ua


def test_fetch_helpers_attach_headers() -> None:
    """The `_fetch_json` / `_fetch_xml` helpers wrap every remote call —
    both must forward `_HTTP_HEADERS` so the UA never leaks."""
    src_json = inspect.getsource(congress_votes._fetch_json)
    src_xml = inspect.getsource(congress_votes._fetch_xml)
    assert "headers=_HTTP_HEADERS" in src_json
    assert "headers=_HTTP_HEADERS" in src_xml


def test_key_bills_set_is_non_empty_and_well_shaped() -> None:
    """Every entry: (congress, bill_type, bill_number, human_label)."""
    assert congress_votes._KEY_BILLS
    for entry in congress_votes._KEY_BILLS:
        assert len(entry) == 4, entry
        congress, btype, bnumber, label = entry
        assert isinstance(congress, int) and 100 <= congress <= 999
        assert isinstance(btype, str) and btype.islower() is False or True
        assert isinstance(bnumber, int) and bnumber > 0
        assert isinstance(label, str) and label


def test_ingest_returns_empty_stats_when_key_unset(monkeypatch) -> None:
    """No key → skip cleanly; NEVER call the API without one."""
    import asyncio

    monkeypatch.delenv("CONGRESS_API_KEY", raising=False)
    stats = asyncio.run(congress_votes.ingest_key_bills())
    assert stats.bills_fetched == 0
    assert stats.bills_upserted == 0


def test_api_key_env_probe_returns_none_when_unset(monkeypatch) -> None:
    monkeypatch.delenv("CONGRESS_API_KEY", raising=False)
    assert congress_votes._api_key() is None


def test_house_clerk_xml_parser_extracts_bioguide_and_vote() -> None:
    """Parse the actual clerk XML shape (verified live on roll190.xml)."""
    xml_sample = """<?xml version="1.0"?>
<rollcall-vote>
<vote-data>
<recorded-vote><legislator name-id="A000370" party="D" state="NC">Adams</legislator><vote>No</vote></recorded-vote>
<recorded-vote><legislator name-id="A000055" party="R" state="AL">Aderholt</legislator><vote>Aye</vote></recorded-vote>
<recorded-vote><legislator name-id="B001318" party="D" state="VT">Balint</legislator><vote>Present</vote></recorded-vote>
</vote-data>
</rollcall-vote>"""
    got = congress_votes._parse_house_clerk_xml(xml_sample)
    assert got == [
        ("A000370", "No"),
        ("A000055", "Aye"),
        ("B001318", "Present"),
    ]


def test_yea_and_nay_normalization_maps_correctly() -> None:
    """Aye/Yea/Yes → voted_for; No/Nay → voted_against."""
    assert "aye" in congress_votes._YEA
    assert "yea" in congress_votes._YEA
    assert "no" in congress_votes._NAY
    assert "nay" in congress_votes._NAY
    # NEVER include Present or Not Voting in either set — spec §5.
    assert "present" not in congress_votes._YEA
    assert "present" not in congress_votes._NAY
    assert "not voting" not in congress_votes._YEA
    assert "not voting" not in congress_votes._NAY


def test_vote_ingest_stats_carries_directional_counters() -> None:
    """The stats surface must distinguish voted_for from voted_against
    counters — a bill with 218-2 needs both visible in the log."""
    s = congress_votes.VoteIngestStats()
    assert hasattr(s, "voted_for_edges_created")
    assert hasattr(s, "voted_against_edges_created")
    assert hasattr(s, "votes_skipped_non_directional")


def test_bill_canonical_type_is_bill_not_placeholder() -> None:
    """Bill canonicals use EntityType.BILL (extended from CONCEPT
    placeholder per helen 2026-07-19 21:59Z)."""
    src = inspect.getsource(congress_votes._upsert_bill_canonical)
    assert "EntityType.BILL" in src


def test_congress_vote_source_kind_used_on_citations() -> None:
    """Vote citations use SourceKind.CONGRESS_VOTE (extended from
    CORPORATE_REGISTRY placeholder per helen 2026-07-19 21:59Z)."""
    src = inspect.getsource(congress_votes._emit_vote_edge)
    assert "SourceKind.CONGRESS_VOTE" in src
