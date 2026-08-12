"""LDA URL migration test (Tony/helen 2026-08-12 carceral Track A
phase 2). The lda.senate.gov API returned 301 → lda.gov during the
carceral sweep (17 errors), blocking new LDA ingestion for the fresh
carceral anchors. Swap the base URL to the new host so future
ingestion runs cleanly."""

from __future__ import annotations

import inspect


def test_lda_base_url_is_lda_gov_not_lda_senate_gov():
    """The base URL must point at the new lda.gov host — the
    lda.senate.gov 301 redirect broke a whole carceral sweep."""
    from app.services.ingest import senate_lda

    assert senate_lda._LDA_BASE == "https://lda.gov/api/v1", (
        f"LDA base URL must be lda.gov post-2026-08-12 migration; got "
        f"{senate_lda._LDA_BASE!r}"
    )
    assert "lda.gov" in senate_lda._FILING_URL_TEMPLATE
    assert "lda.senate.gov" not in senate_lda._FILING_URL_TEMPLATE


def test_lda_get_uses_migrated_base_url():
    """Source-inspection guard: the _lda_get helper concatenates
    _LDA_BASE + path, so as long as _LDA_BASE is correct, the actual
    request URLs land on lda.gov. Belt-and-braces: no hardcoded
    lda.senate.gov URL in the helper body either."""
    from app.services.ingest import senate_lda

    src = inspect.getsource(senate_lda._lda_get)
    assert "_LDA_BASE" in src
    assert "lda.senate.gov" not in src
