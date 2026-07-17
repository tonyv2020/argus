"""Senate LDA ingester (P3a) — shape + filter + citation URL invariants.

The ingester itself does async DB work + async HTTP; those paths need
a Postgres + an httpx transport mock and belong in an integration suite.
Here we hermetically test the pure-logic pieces that are the load-bearing
correctness surface:

  * ``_client_name_matches`` filters out LDA's fuzzy-match false positives
    (a query for "The GEO Group" returns "GEOTHERMAL TAX GROUP" too;
    correct behavior is to drop them before we materialize a canonical).
  * The public filing URL template resolves to the STABLE public-record
    page (not the PDF that can 404 when the disclosure is amended).
  * ``SenateLdaStats`` starts at all-zero counters so callers can rely on
    numeric fields rather than None-guards.
"""

from __future__ import annotations

from app.services.ingest.senate_lda import (
    _FILING_URL_TEMPLATE,
    SenateLdaStats,
    _client_name_matches,
)


def test_client_name_matches_accepts_the_exact_anchor() -> None:
    """A canonical GEO Group filing matches the "The GEO Group" anchor."""
    row = {"client": {"name": "THE GEO GROUP, INC."}}
    assert _client_name_matches(row, "The GEO Group")


def test_client_name_matches_rejects_lda_fuzzy_false_positive() -> None:
    """LDA fuzzy-matches "GEO+Group" against "GEOTHERMAL TAX GROUP" —
    the client-side filter MUST drop these before materializing an edge."""
    row = {"client": {"name": "GEOTHERMAL TAX GROUP"}}
    assert not _client_name_matches(row, "The GEO Group")


def test_client_name_matches_is_case_insensitive() -> None:
    """LDA case varies across filings (older filings are ALL-CAPS)."""
    row = {"client": {"name": "the geo group, inc."}}
    assert _client_name_matches(row, "The GEO Group")


def test_client_name_matches_tolerates_missing_client_block() -> None:
    """A partial/malformed row must return False, not crash."""
    assert not _client_name_matches({}, "The GEO Group")
    assert not _client_name_matches({"client": None}, "The GEO Group")
    assert not _client_name_matches({"client": {"name": None}}, "The GEO Group")


def test_filing_url_template_points_at_public_record_page() -> None:
    """The citation URL must be the stable /filings/public/filing/<uuid>/ page,
    NOT the PDF URL — PDFs can 404 when a filing is amended, the record page
    stays live."""
    uuid = "bf5d8bd9-79a9-46cf-a148-25a58c6abe8b"
    url = _FILING_URL_TEMPLATE.format(uuid=uuid)
    assert url == f"https://lda.senate.gov/filings/public/filing/{uuid}/"
    assert not url.endswith(".pdf")


def test_stats_starts_at_all_zero_counters() -> None:
    """Counters MUST be int-typed and zero-initialized so callers can sum
    without None-guards after a partial-failure run."""
    stats = SenateLdaStats()
    assert stats.filings_fetched == 0
    assert stats.filings_skipped_off_anchor == 0
    assert stats.clients_upserted == 0
    assert stats.registrants_upserted == 0
    assert stats.edges_created == 0
    assert stats.edges_reused == 0
    assert stats.citations_created == 0
    assert stats.errors == 0
