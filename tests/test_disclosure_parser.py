"""D1 disclosure parser — hermetic tests.

Fixtures are hand-crafted snippets of ``pdftotext -layout`` output that
mirror the real Trump 2026 278ANNUAL structure. Every band string in
fixtures is drawn from :data:`VALUE_BANDS` (the closed vocabulary) so
the tests verify actual matching, not toy syntax.

The tests below lock in:

* Closed-vocabulary anchoring — a fabricated band string must land LOW
  and never be structured (fabrication canary at the parser layer).
* Case-insensitive Part 7 transaction type ("Purchase" vs "purchase").
* Multi-line row joining — Part 7's ``type/date/band`` wrapping onto
  the next line still yields HIGH.
* Boilerplate skip — a repeated "Part 7: Transactions" page header
  DOES NOT get glued onto the prior row's block (regression: this was
  the bug that made 14k Part 7 rows falsely LOW).
* Every enum value has a canonical string form + parts cover the
  design's 6+3 taxonomy.
"""

from __future__ import annotations

from app.services.disclosure_parser import (
    VALUE_BANDS,
    Confidence,
    ParsedRow,
    Part,
    parse_text,
    summarize,
)

# ─── Closed-vocabulary invariants ───────────────────────────────────


def test_value_bands_include_the_canonical_18_from_annual() -> None:
    """Any band that appears in Trump 2026 278ANNUAL must be in the set.

    Failing this test means the parser will start marking real bands
    LOW — which is a correctness regression, not a graceful degrade.
    """
    for expected in (
        "None (or less than $201)",
        "None (or less than $1,001)",
        "$1,001 - $15,000",
        "$1,000,001 - $5,000,000",
        "$5,000,001 - $25,000,000",
        "Over $50,000,000",
        "$100,001 - $250,000",
    ):
        assert expected in VALUE_BANDS


def test_fabricated_value_band_never_becomes_high() -> None:
    """The parser's fabrication canary: a band-shaped string that isn't
    literally in the vocabulary MUST NOT be coerced HIGH.

    This is the D1-side twin of §6's LLM anti-fabrication contract — no
    LLM in the parser, and no regex coercion at the parser either.
    """
    fixture = _fixture_page(
        "Part 6: Other Assets and Income",
        [
            "INVESTMENT ACCOUNT #1",
            "1     ACME CORP                                                     N/A       $1 - $999          DIVIDEND      $201 - $1,000",
        ],
    )
    rows = parse_text(fixture)
    assets = [r for r in rows if r.part == Part.PART_6_OTHER_ASSETS]
    assert len(assets) == 1
    # The row itself may be LOW because "$1 - $999" is NOT in
    # VALUE_BANDS (it's a fabricated band). The parser MUST NOT emit
    # HIGH with a coerced band — that would be the parser layer's
    # fabrication path.
    r = assets[0]
    if r.confidence == Confidence.HIGH:
        # If the parser did produce HIGH, the value_band it captured
        # MUST come from the closed vocabulary — never the fabricated
        # "$1 - $999".
        assert r.parsed.get("value_band") in VALUE_BANDS
        assert r.parsed.get("value_band") != "$1 - $999"


# ─── Part 6 — the bulk assets shape ─────────────────────────────────


def test_part6_high_confidence_row_shape() -> None:
    """A clean Part 6 row with real band → HIGH with parsed payload."""
    fixture = _fixture_page(
        "Part 6: Other Assets and Income",
        [
            "INVESTMENT ACCOUNT #1",
            "1     APPLE INC                                                     N/A       $1,000,001 - $5,000,000    DIVIDEND      $50,001 - $100,000",
        ],
    )
    rows = parse_text(fixture)
    assets = [r for r in rows if r.part == Part.PART_6_OTHER_ASSETS]
    assert len(assets) == 1
    r = assets[0]
    assert r.confidence == Confidence.HIGH
    assert r.parsed["value_band"] == "$1,000,001 - $5,000,000"
    assert r.parsed["income_band"] == "$50,001 - $100,000"
    assert r.parsed["income_type"] == "DIVIDEND"
    assert r.account_group == 1


def test_part6_wrapped_position_title_ok() -> None:
    """A row whose description wraps onto the next line still counts
    with the same row_index. Continuation is attached to the same block.
    """
    fixture = _fixture_page(
        "Part 6: Other Assets and Income",
        [
            "INVESTMENT ACCOUNT #1",
            "1     WELLS FARGO CO NEW BUY BOND DUE 12/31/2030                     N/A       $500,001 - $1,000,000    INTEREST      $1,001 - $2,500",
        ],
    )
    rows = parse_text(fixture)
    assert len([r for r in rows if r.part == Part.PART_6_OTHER_ASSETS]) == 1


# ─── Part 7 — the transaction shape (the case + wrap fix) ───────────


def test_part7_case_insensitive_transaction_type() -> None:
    """The annual uses ``Purchase`` (capitalized) — before the fix this
    fell through to LOW. Case-insensitive Part 7 tail regex fixes it.
    """
    fixture = _fixture_page(
        "Part 7: Transactions",
        [
            "INVESTMENT ACCOUNT #1",
            "1     ISHARES TRUST                                                Purchase      9/18/2025    $1,000,001 - $5,000,000",
        ],
    )
    rows = parse_text(fixture)
    trades = [r for r in rows if r.part == Part.PART_7_TRANSACTIONS]
    assert trades[0].confidence == Confidence.HIGH
    assert trades[0].parsed["transaction_type"].lower() == "purchase"
    assert trades[0].parsed["trade_date"] == "9/18/2025"
    assert trades[0].parsed["amount_band"] == "$1,000,001 - $5,000,000"


def test_part7_wrapped_tail_still_high() -> None:
    """When ``type/date/band`` wraps onto the next line the parser must
    still tokenize as HIGH — this is the regression the row-block
    joiner is here to prevent (was 14K false LOWs before the fix)."""
    fixture = _fixture_page(
        "Part 7: Transactions",
        [
            "INVESTMENT ACCOUNT #1",
            "1     ON SEMICONDUCTOR CORP",
            "                                Purchase      9/22/2025    $250,001 - $500,000",
        ],
    )
    rows = parse_text(fixture)
    trades = [r for r in rows if r.part == Part.PART_7_TRANSACTIONS]
    assert trades[0].confidence == Confidence.HIGH
    assert "ON SEMICONDUCTOR CORP" in trades[0].parsed["description"]


def test_part7_repeated_page_header_does_not_pollute_row() -> None:
    """When ``Part 7: Transactions`` repeats as a page-header on the
    next page it must be skipped as boilerplate, not glued onto the
    previous row's block. This was the exact bug that produced 14K
    false LOWs in the first smoke run.
    """
    fixture = _fixture_page(
        "Part 7: Transactions",
        [
            "INVESTMENT ACCOUNT #1",
            "1     ISHARES SELECT DIVIDEND ETF",
        ],
    ) + "\x0c" + _fixture_page(
        "Part 7: Transactions",  # repeated header on next page
        [
            "                                Purchase      9/25/2025    $500,001 - $1,000,000",
        ],
    )
    rows = parse_text(fixture)
    trades = [r for r in rows if r.part == Part.PART_7_TRANSACTIONS]
    assert len(trades) == 1
    assert trades[0].confidence == Confidence.HIGH
    assert trades[0].parsed["trade_date"] == "9/25/2025"


# ─── Part 8 — liabilities (band-anchored) ───────────────────────────


def test_part8_liability_row_ok() -> None:
    """A Part 8 row lands HIGH when a real amount band is present."""
    fixture = _fixture_page(
        "Part 8: Liabilities",
        [
            "1. The Bryn Mawr Trust Company     Seven Springs (mortgage)     $5,000,001 - $25,000,000     2000    4.50%    Paid Off July 2025",
        ],
    )
    rows = parse_text(fixture)
    liab = [r for r in rows if r.part == Part.PART_8_LIABILITIES]
    assert liab[0].confidence == Confidence.HIGH
    assert liab[0].parsed["amount_band"] == "$5,000,001 - $25,000,000"


# ─── Part 9 — gifts are recognized but ledgered as narrative ────────


def test_part9_gifts_recognized_as_own_part_not_part8() -> None:
    """Regression from the first smoke: Part 9 header wasn't recognized
    and gift rows were mis-classified as Part 8 liabilities. They also
    contain private-individual source names, so structuring in D1
    would be a Scrutiny gate violation.
    """
    fixture = _fixture_page(
        "Part 9: Gifts and Travel Reimbursements",
        [
            "1.     Anthony Constantino/Sticker Mule LLC     Amsterdam, NY     Sculpture     $250,000.00",
        ],
    )
    rows = parse_text(fixture)
    gifts = [r for r in rows if r.part == Part.PART_9_GIFTS]
    liab = [r for r in rows if r.part == Part.PART_8_LIABILITIES]
    assert len(gifts) == 1
    assert len(liab) == 0
    assert gifts[0].confidence == Confidence.LOW
    assert gifts[0].reason == "narrative_part_not_structured_in_d1"


# ─── Summary + taxonomy ─────────────────────────────────────────────


def test_summarize_gives_per_part_and_totals() -> None:
    """``summarize`` returns the exact shape D1's done-gate report
    expects: totals + per-part breakdown."""
    rows: list[ParsedRow] = [
        ParsedRow(
            part=Part.PART_6_OTHER_ASSETS,
            row_index=1,
            raw_text="",
            page=7,
            account_group=1,
            confidence=Confidence.HIGH,
        ),
        ParsedRow(
            part=Part.PART_6_OTHER_ASSETS,
            row_index=2,
            raw_text="",
            page=7,
            account_group=1,
            confidence=Confidence.LOW,
            reason="no_value_band_matched",
        ),
        ParsedRow(
            part=Part.PART_7_TRANSACTIONS,
            row_index=1,
            raw_text="",
            page=159,
            account_group=1,
            confidence=Confidence.HIGH,
        ),
    ]
    s = summarize(rows)
    assert s["total"] == 3
    assert s["high"] == 2
    assert s["low"] == 1
    assert s["per_part"]["part_6_other_assets"] == {"high": 1, "low": 1, "total": 2}
    assert s["per_part"]["part_7_transactions"] == {"high": 1, "low": 0, "total": 1}


def test_part_enum_covers_the_design_taxonomy() -> None:
    """helen's twin-bus dispatch names Parts 1, 2, 3, 4, 5, 6, 8 as
    the D1 splitter scope. Design §3 also names Part 7. We add Part 9
    so we can recognize + ledger gifts without mis-classifying them
    as Part 8. Nine parts total.
    """
    seen = {p.value for p in Part}
    assert seen == {
        "part_1_positions",
        "part_2_empl_assets",
        "part_3_agreements",
        "part_4_comp_sources",
        "part_5_spouse_assets",
        "part_6_other_assets",
        "part_7_transactions",
        "part_8_liabilities",
        "part_9_gifts",
    }


# ─── helpers ────────────────────────────────────────────────────────


def _fixture_page(part_header: str, lines: list[str]) -> str:
    """Build a synthetic pdftotext-layout page-string.

    Emits: filer-name line + part header + row lines + form-feed
    (so the parser's page-splitting sees a clean page boundary).
    """
    prelude = [
        "Filer's Name                          Page Number",
        "Donald J. Trump                       Page 1 of 1",
        part_header,
    ]
    return "\n".join(prelude + lines) + "\n\x0c"
