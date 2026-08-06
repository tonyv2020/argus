"""Unit tests for :mod:`app.services.disclosure_issuer` — D2.1 issuer normalizer.

Coverage: every helen-specified transform + all fragmentation cases
observed in the D2 emit's junk-node bucket (Trump 2026 annual).
"""

from __future__ import annotations

import pytest

from app.services.disclosure_issuer import normalize_issuer


# ─── Table-driven expected outputs ──────────────────────────────────

# (input, expected_output) — mined from the actual disclosure_rows +
# canonical_entities snapshot on 2026-08-05.
CASES: list[tuple[str, str]] = [
    # helen's example: Carnival fragments all collapse to CARNIVAL CORP
    ("***CARNIVAL CORP", "CARNIVAL CORP"),
    ("CARNIVAL CORP", "CARNIVAL CORP"),
    ("CARNIVAL CORP F", "CARNIVAL CORP"),
    ("CARNIVAL CORP COMMON PAIRED STOCK", "CARNIVAL CORP"),
    ("CARNIVAL CORP PAIRED CTF", "CARNIVAL CORP"),
    ("CARNIVAL CORP NEW (PAIRED STOCK)", "CARNIVAL CORP"),
    ("CARNIVALCORPPAIREDCTF", "CARNIVAL CORP"),
    # Big-tech share/security class variants
    ("APPLE INC", "APPLE INC"),
    ("APPLE INC COM", "APPLE INC"),
    ("APPLE INC. 4.3%33 DUE 05/10/33", "APPLE INC."),
    ("MICROSOFT CORP", "MICROSOFT CORP"),
    ("MICROSOFT CORP COM", "MICROSOFT CORP"),
    ("MICROSOFTCORPORATION", "MICROSOFT CORPORATION"),
    ("NVIDIA CORP", "NVIDIA CORP"),
    ("NVIDIACORP", "NVIDIA CORP"),
    ("AMAZON.COM INC", "AMAZON.COM INC"),
    ("AMAZON COM INC", "AMAZON COM INC"),
    ("AMAZON COM INC COM", "AMAZON COM INC"),
    ("AMAZON.COMINC", "AMAZON.COM INC"),
    ("APPLECOMPUTERINC", "APPLE COMPUTER INC"),
    # Bond-descriptor cases (design §3 shape)
    (
        "NETFLIX INC REG S DUE 11/15/2029 5.375 REG INT ON 854000 BND",
        "NETFLIX INC",
    ),
    (
        "AMAZON.COM INC B/E 03.600% 041332 DTD041322 FC101322 CALL@MW+15BP",
        "AMAZON.COM INC",
    ),
    ("AIR PRODUCTS AND 4.85%34 DUE 02/08/34", "AIR PRODUCTS AND"),
    # Muni-bond cases — cut at REV/RFDG/SER/MTG/etc.
    (
        "AKRON OH INCM TAX REV VARIOUS PURP B/E 4.00 % Due Dec 1, 2025",
        "AKRON OH",
    ),
    (
        "ALASKA MUN BD BK AL 5%30REV UTX DUE 12/01/30",
        "ALASKA MUN BD BK AL",
    ),
    (
        "ALABAMA ST RFDG SER A B/E PTC 3.00 % Due Aug 1, 2026",
        "ALABAMA ST",
    ),
    (
        "ALLEN CNTY OHIO HOS 5%41SYST HLTH DUE 11/01/41XTRO",
        "ALLEN CNTY OHIO HOS",
    ),
    # Apple Hospitality REIT — distinct entity, must NOT collapse to Apple
    ("APPLE HOSPITALITY REIT I", "APPLE HOSPITALITY REIT"),
    ("APPLE HOSPITALITY REIT IREIT", "APPLE HOSPITALITY REIT"),
    # Edge cases
    ("", ""),
    ("   ", ""),
    ("***", ""),
    ("BIOGEN INC", "BIOGEN INC"),
    ("COREWEAVE INC", "COREWEAVE INC"),
]


@pytest.mark.parametrize("raw,expected", CASES)
def test_normalize_issuer(raw: str, expected: str) -> None:
    """Every listed transform matches its expected clean output."""
    assert normalize_issuer(raw) == expected, (
        f"raw={raw!r} → got={normalize_issuer(raw)!r} want={expected!r}"
    )


def test_idempotent() -> None:
    """Running the normalizer twice on any input returns the same result."""
    for raw, _expected in CASES:
        once = normalize_issuer(raw)
        twice = normalize_issuer(once)
        assert once == twice, f"non-idempotent: {raw!r} → {once!r} → {twice!r}"


def test_never_collapses_distinct_leading_tokens() -> None:
    """Guard: the normalizer must never strip a LEADING word — that would
    collapse two DISTINCT issuers to one."""
    # Even after all tail-strips, the leading token(s) survive.
    assert normalize_issuer("SHOE CARNIVAL INC").startswith("SHOE CARNIVAL")
    assert normalize_issuer("APPLE HOSPITALITY REIT I") != "APPLE"
    assert normalize_issuer("APPLE HOSPITALITY REIT I") != "APPLE INC"


def test_never_returns_empty_for_nonempty_input() -> None:
    """Any input with at least one alnum character returns a non-empty
    string — never over-strips to ``""``."""
    for raw in ("A", "AB", "CARNIVAL", "X CORP", "CORP F", "MTG"):
        got = normalize_issuer(raw)
        assert got != "", f"over-stripped {raw!r} → empty"


# D2.1 polish (helen 2026-08-06) — doubled-tail collapse, DB / year
# strip, PRTNRSHP → PARTN, curated historical aliases.
POLISH_CASES: list[tuple[str, str]] = [
    # PRTNRSHP → PARTN unify (Certificate of Partnership).
    ("ORANGE CNTY FL SCH BRD CTF PRTNRSHP", "ORANGE CNTY FL SCH BRD CTF PARTN"),
    # Trailing standalone year.
    ("CAMDENTON MO CTF PARTN 2025", "CAMDENTON MO CTF PARTN"),
    ("SOME MUNI ISSUER 2027", "SOME MUNI ISSUER"),
    # Trailing standalone DB.
    ("FORT BEND CNTY TX CTF OBLIG DB", "FORT BEND CNTY TX CTF OBLIG"),
    # Doubled trailing tail.
    ("CAMDENTON MO CTF PARTN CTF PARTN", "CAMDENTON MO CTF PARTN"),
    # Full helen example: doubled + year.
    ("CAMDENTON MO CTF PARTN CTF PARTN 2025", "CAMDENTON MO CTF PARTN"),
    # Historical alias — Apple Computer Inc → Apple Inc (2007 rename).
    ("APPLE COMPUTER INC", "APPLE INC"),
    ("APPLE COMPUTER INC.", "APPLE INC."),
    # PRTNRSHP + doubled combo.
    ("GREENE CNTY MO CTF PARTN CTF PRTNRSHP", "GREENE CNTY MO CTF PARTN"),
    # Distinct real bond variants stay distinct — helen's "muni issuers
    # themselves stay distinct real entities" rule.
    ("GREENE CNTY MO CTF PARTN CAP PJS", "GREENE CNTY MO CTF PARTN CAP PJS"),
    ("FORT BEND CNTY TX MUD 118", "FORT BEND CNTY TX MUD 118"),
]


@pytest.mark.parametrize("raw,expected", POLISH_CASES)
def test_normalize_issuer_polish(raw: str, expected: str) -> None:
    """D2.1 polish transforms match expected clean output."""
    assert normalize_issuer(raw) == expected, (
        f"raw={raw!r} → got={normalize_issuer(raw)!r} want={expected!r}"
    )
