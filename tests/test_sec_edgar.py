"""SEC EDGAR ingester (P3b) — shape + CIK padding + filing-URL invariants."""

from __future__ import annotations

from app.services.ingest.sec_edgar import (
    DEFAULT_ANCHORS,
    INTERESTING_FORMS,
    SecAnchor,
    SecEdgarStats,
    _filing_index_url,
    _iter_recent_filings,
)


def test_cik10_zero_pads_to_ten_digits() -> None:
    """The SEC submissions API expects the CIK as a 10-digit zero-padded string."""
    a = SecAnchor(cik=923796, surface_name="GEO Group Inc")
    assert a.cik10 == "0000923796"
    assert a.cik_short == "923796"


def test_cik10_pads_short_cik() -> None:
    """A three-digit CIK still emits ten digits."""
    a = SecAnchor(cik=1, surface_name="Anything")
    assert a.cik10 == "0000000001"
    assert a.cik_short == "1"


def test_default_anchors_contain_geo_and_corecivic() -> None:
    """P3b's core anchors are the two publicly-traded detention primes."""
    ciks = {a.cik for a in DEFAULT_ANCHORS}
    assert 923796 in ciks  # GEO Group
    assert 1070985 in ciks  # CoreCivic


def test_interesting_forms_excludes_form_4() -> None:
    """Form 4 (insider transactions) fires on every executive trade —
    citing them all would swamp the corporate_registry set. The MVP scope
    keeps citations to load-bearing periodic + episodic filings."""
    assert "10-K" in INTERESTING_FORMS
    assert "10-Q" in INTERESTING_FORMS
    assert "8-K" in INTERESTING_FORMS
    assert "DEF 14A" in INTERESTING_FORMS
    assert "4" not in INTERESTING_FORMS
    assert "SC 13G" not in INTERESTING_FORMS


def test_filing_index_url_points_at_stable_index_page() -> None:
    """The citation URL is the accession's -index.htm page (stable)
    rather than any specific exhibit document (amendable, can 404)."""
    url = _filing_index_url("923796", "0001193125-26-211821")
    assert url == (
        "https://www.sec.gov/Archives/edgar/data/923796/000119312526211821/"
        "0001193125-26-211821-index.htm"
    )
    assert url.endswith("-index.htm")


def test_iter_recent_filings_zips_parallel_lists_into_rows() -> None:
    """SEC's `filings.recent` uses PARALLEL lists (form[i], accessionNumber[i],
    filingDate[i]) — the iterator MUST zip them into row-shaped dicts and
    handle uneven list lengths (submissions in flight can produce a form
    row without a matching accession)."""
    payload = {
        "filings": {
            "recent": {
                "form": ["10-K", "10-Q", "8-K"],
                "accessionNumber": ["acc-a", "acc-b", "acc-c"],
                "filingDate": ["2026-01-01", "2026-05-01", "2026-06-01"],
            }
        }
    }
    rows = list(_iter_recent_filings(payload))
    assert rows == [
        {"form": "10-K", "accession": "acc-a", "date": "2026-01-01"},
        {"form": "10-Q", "accession": "acc-b", "date": "2026-05-01"},
        {"form": "8-K", "accession": "acc-c", "date": "2026-06-01"},
    ]


def test_iter_recent_filings_tolerates_uneven_parallel_lists() -> None:
    """Truncate to the shortest of the parallel lists — a form without a
    matching accession must not be materialised as an edge with a
    NULL citation ref."""
    payload = {
        "filings": {
            "recent": {
                "form": ["10-K", "10-Q"],
                "accessionNumber": ["acc-a"],
                "filingDate": ["2026-01-01", "2026-05-01"],
            }
        }
    }
    rows = list(_iter_recent_filings(payload))
    assert rows == [{"form": "10-K", "accession": "acc-a", "date": "2026-01-01"}]


def test_iter_recent_filings_empty_when_no_recent() -> None:
    """A submissions payload without recent filings must yield nothing —
    not raise, not emit synthetic rows."""
    assert list(_iter_recent_filings({})) == []
    assert list(_iter_recent_filings({"filings": {}})) == []
    assert list(_iter_recent_filings({"filings": {"recent": {}}})) == []


def test_stats_starts_at_all_zero_counters() -> None:
    """Counter fields must be int-typed and zero-initialised so callers
    can sum results across multi-anchor runs without None-guards."""
    stats = SecEdgarStats()
    assert stats.anchors_processed == 0
    assert stats.filings_fetched == 0
    assert stats.filings_skipped_uninteresting == 0
    assert stats.issuers_upserted == 0
    assert stats.former_name_aliases_created == 0
    assert stats.edges_created == 0
    assert stats.edges_reused == 0
    assert stats.citations_created == 0
    assert stats.citations_skipped_already_cited == 0
    assert stats.errors == 0
