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


# ─── P3c broadening pass ──────────────────────────────────────────────


def test_detention_industry_lda_clients_covers_the_three_public_primes() -> None:
    """The P3c anchor list must cover the two publicly-traded detention primes
    (GEO Group + CoreCivic — under BOTH its current and pre-2016 legal names)
    plus MTC as the largest privately-held operator with LDA filings."""
    from app.services.ingest.senate_lda import DETENTION_INDUSTRY_LDA_CLIENTS

    lower = {c.lower() for c in DETENTION_INDUSTRY_LDA_CLIENTS}
    assert "the geo group" in lower
    assert "corecivic" in lower
    # CoreCivic renamed from Corrections Corporation of America in Oct 2016;
    # the older name still shows up on filings pre-rename, so we sweep both.
    assert "corrections corporation of america" in lower
    # MTC — the third-largest private detention operator with LDA activity.
    assert "management and training corporation" in lower


def test_detention_industry_anchors_are_deduplicated_by_name() -> None:
    """Anchor list must have unique entries — an accidental dup would double-run
    the same LDA sweep and inflate `edges_reused` counters twice."""
    from app.services.ingest.senate_lda import DETENTION_INDUSTRY_LDA_CLIENTS

    assert len(set(DETENTION_INDUSTRY_LDA_CLIENTS)) == len(DETENTION_INDUSTRY_LDA_CLIENTS)
