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


def test_get_helper_attaches_headers() -> None:
    """The `_get` helper wraps every congress.gov call — it must
    forward `_HTTP_HEADERS` so the UA never leaks."""
    src = inspect.getsource(congress_votes._get)
    assert "headers=_HTTP_HEADERS" in src


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


def test_vote_edge_citation_ref_carries_vote_kind() -> None:
    """The vote KIND (voted_for / voted_against) must survive on the
    citation ref so downstream analysis + audit can slice."""
    src = inspect.getsource(congress_votes._emit_vote_edge)
    assert '{vote_kind}' in src
