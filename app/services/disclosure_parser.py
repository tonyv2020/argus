"""Deterministic parser for OGE 278e financial-disclosure filings (D1).

Input: the text output of ``pdftotext -layout <annual.pdf>``.
Output: a list of :class:`ParsedRow` — one per source row in the doc,
each tagged with a ``parse_confidence`` and (when HIGH) a canonical
``parsed`` payload.

**The whole point of this file is to have no LLM in it.** Rows are
tokenized by regex anchored on:

  * The closed vocabulary of OGE value bands (18 canonical strings —
    :data:`VALUE_BANDS`). A row whose band text is not in this set is
    LOW confidence; it is never coerced.
  * The fixed part header (``Part N: ...``) and the row-number prefix
    (``NN`` at start of the description column).

Part shapes were grounded on the actual Trump 2026 278ANNUAL (file
sha256 ``84b5987e...``, 927pp, 849 INVESTMENT ACCOUNT groupings).
Every band string that appears in this file is present in
:data:`VALUE_BANDS`; anything else is LOW.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from enum import StrEnum

# ─── The closed value-band vocabulary ────────────────────────────────
#
# Every string that OGE 278e uses to express a value or income band. If a
# row's band text is not IN this set, the parser must not silently coerce
# it — it goes to LOW confidence and quarantine.  Verified by grep on
# Trump 2026 278ANNUAL.pdf (annual.txt) — 2026-08-05.
VALUE_BANDS: frozenset[str] = frozenset(
    {
        # asset value ranges (bottom-of-tier "None" markers)
        "None (or less than $201)",
        "None (or less than $1,001)",
        # small ranges
        "$0 - $200",
        "$201 - $1,000",
        "$1,001 - $2,500",
        "$1,001 - $15,000",
        "$2,501 - $5,000",
        "$5,001 - $15,000",
        "$15,001 - $50,000",
        # mid ranges
        "$50,001 - $100,000",
        "$100,001 - $250,000",
        "$100,001 - $1,000,000",
        "$250,001 - $500,000",
        "$500,001 - $1,000,000",
        "$1,000,001 - $5,000,000",
        "$5,000,001 - $25,000,000",
        "$25,000,001 - $50,000,000",
        # top-of-tier "Over" markers
        "Over $5,000,000",
        "Over $50,000,000",
    }
)


class Part(StrEnum):
    """OGE 278e Parts that D1 tokenizes."""

    #: Positions Held Outside U.S. Government.
    PART_1_POSITIONS = "part_1_positions"
    #: Filer's Employment Assets & Income and Retirement Accounts.
    PART_2_EMPL_ASSETS = "part_2_empl_assets"
    #: Employment Agreements and Arrangements.
    PART_3_AGREEMENTS = "part_3_agreements"
    #: Sources of Compensation Exceeding $5,000.
    PART_4_COMP_SOURCES = "part_4_comp_sources"
    #: Spouse's Employment Assets & Income and Retirement Accounts.
    PART_5_SPOUSE_ASSETS = "part_5_spouse_assets"
    #: Other Assets and Income (the bulk).
    PART_6_OTHER_ASSETS = "part_6_other_assets"
    #: Transactions (securities purchases/sales/exchanges).
    PART_7_TRANSACTIONS = "part_7_transactions"
    #: Liabilities.
    PART_8_LIABILITIES = "part_8_liabilities"
    #: Gifts and Travel Reimbursements. Out of D1's structured scope,
    #: but recognized so we don't mis-tokenize Part 9 rows as Part 8
    #: liabilities. Ledgered as LOW (narrative_part_not_structured_in_d1).
    PART_9_GIFTS = "part_9_gifts"


class Confidence(StrEnum):
    """Per-row parse confidence."""

    HIGH = "high"
    LOW = "low"


# Part-header regexes (docs are OCR/xlsx-origin so a bit of column drift
# is possible; we anchor on the literal Part-N marker + the recognizable
# title fragment).
_PART_HEADERS: dict[Part, re.Pattern[str]] = {
    Part.PART_1_POSITIONS: re.compile(r"^\s*Part 1: .*Positions Held", re.M),
    Part.PART_2_EMPL_ASSETS: re.compile(
        r"^\s*Part 2: .*Employment Assets", re.M
    ),
    Part.PART_3_AGREEMENTS: re.compile(
        r"^\s*Part 3: .*Employment Agreements", re.M
    ),
    Part.PART_4_COMP_SOURCES: re.compile(
        r"^\s*Part 4: .*Sources of Compensation", re.M
    ),
    Part.PART_5_SPOUSE_ASSETS: re.compile(
        r"^\s*Part 5: .*Spouse", re.M
    ),
    Part.PART_6_OTHER_ASSETS: re.compile(
        r"^\s*Part 6: .*Other Assets", re.M
    ),
    Part.PART_7_TRANSACTIONS: re.compile(r"^\s*Part 7: .*Transactions", re.M),
    Part.PART_8_LIABILITIES: re.compile(r"^\s*Part 8: .*Liabilities", re.M),
    Part.PART_9_GIFTS: re.compile(
        r"^\s*Part 9: .*Gifts", re.M
    ),
}

# INVESTMENT ACCOUNT #N — appears in Parts 2/5/6/7. Groups the following
# rows until the next INVESTMENT ACCOUNT #N (or a Part boundary).
_ACCOUNT_HEADER = re.compile(r"^\s*INVESTMENT ACCOUNT #(\d+)\s*$", re.M)

# Page boundary — pdftotext emits a form feed (0x0C) between pages when
# ``-layout`` is used. Split on that to keep per-row ``page`` accurate.
_PAGE_SPLIT = "\x0c"

# Row-number prefixes: either "N." (Parts 1, 3, 4, 8 use ``1.``) or a
# bare integer with 2+ trailing spaces (Parts 2, 5, 6, 7 use ``1  ``).
# Rest may be empty (stub rows are common in Parts 3/4).
_ROW_PREFIX = re.compile(
    r"^\s{0,15}(?P<idx>\d+)(?:\.\s*|\s{2,})(?P<rest>.*)$"
)

# For Parts 2/5/6: value_band + optional income_type + income_band.
# The band-vocabulary anchor is the whole design's anti-fabrication core:
# the regex only recognizes strings that are literally in VALUE_BANDS.
_VALUE_BAND_ALT = "|".join(
    sorted(map(re.escape, VALUE_BANDS), key=len, reverse=True)
)
_ROW_BAND_ANCHOR = re.compile(
    rf"(?P<band>{_VALUE_BAND_ALT})",
)

# For Part 7: "Type Date Amount" tail. Type is one of a small closed set
# per OGE instructions. Case-insensitive because the annual mixes cases
# ("Purchase" and "purchase" both appear).
_PART7_TYPES = ("purchase", "sale", "exchange", "S (partial)", "P (partial)")
_PART7_TYPE_ALT = "|".join(map(re.escape, _PART7_TYPES))
_PART7_TAIL = re.compile(
    rf"(?P<type>{_PART7_TYPE_ALT})\s+"
    rf"(?P<date>\d{{1,2}}/\d{{1,2}}/\d{{4}})\s+"
    rf"(?P<band>{_VALUE_BAND_ALT})\s*$",
    re.IGNORECASE,
)


# ─── Data shapes ────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class ParsedRow:
    """One tokenized row of the source doc.

    ``parsed`` is populated on HIGH confidence only; on LOW the row goes
    into quarantine with ``raw_text`` intact but no structured payload.
    """

    part: Part
    """The part the row was tokenized under."""

    row_index: int | None
    """The numeric prefix on the source line (``1``, ``2``, …), or None
    for rows that had no prefix (page-continuation of a wrapped row, or a
    stray non-row-shaped line inside a part)."""

    raw_text: str
    """The literal line(s) from the pdftotext output — the audit floor."""

    page: int
    """1-based page number in the source PDF."""

    account_group: int | None
    """The ``INVESTMENT ACCOUNT #N`` grouping the row was under (Parts
    2/5/6/7 only)."""

    confidence: Confidence
    """HIGH: all expected columns present + band literally in
    :data:`VALUE_BANDS`. LOW: anything else — never coerced."""

    parsed: dict = field(default_factory=dict)
    """Structured payload on HIGH; empty on LOW."""

    reason: str | None = None
    """When LOW, why (surfacing this in the ledger lets helen review
    each quarantine reason without opening the PDF)."""


# ─── Public API ─────────────────────────────────────────────────────


def parse_text(text: str) -> list[ParsedRow]:
    """Parse a full ``pdftotext -layout`` output into a list of rows.

    Two-pass:
      1. Walk pages/lines, tracking the current Part + INVESTMENT
         ACCOUNT # state, and group consecutive lines into logical
         *row blocks* delimited by the row-number prefix (``N.``) or a
         Part/account boundary.
      2. Tokenize each block as one string (which lets Part 7's
         wrapped ``type/date/band`` on the next line come through as
         HIGH instead of being falsely quarantined).

    The input is expected to have form-feed (``\\x0c``) page separators
    (which is what ``pdftotext -layout`` emits by default). ``page``
    on the emitted row reflects the 1-based page the ROW STARTED on
    (which is where helen's spot-check will look).
    """
    pages = text.split(_PAGE_SPLIT)
    blocks: list[_Block] = []

    current_part: Part | None = None
    current_account: int | None = None
    current_block: _Block | None = None

    def _close_block() -> None:
        nonlocal current_block
        if current_block is not None:
            blocks.append(current_block)
            current_block = None

    for page_idx, page_text in enumerate(pages, start=1):
        # Part header on this page? Update state.
        for part, pat in _PART_HEADERS.items():
            if pat.search(page_text):
                if part != current_part:
                    _close_block()
                    current_part = part
                    # Account grouping is per-part.
                    current_account = None
                break

        if current_part is None:
            continue

        for line in page_text.splitlines():
            if _is_boilerplate(line):
                # Boilerplate does NOT close a block — a wrapped row's
                # continuation can straddle a page whose header contains
                # boilerplate lines above the continuation.
                continue
            m_acct = _ACCOUNT_HEADER.match(line)
            if m_acct is not None:
                _close_block()
                current_account = int(m_acct.group(1))
                continue
            m_prefix = _ROW_PREFIX.match(line)
            if m_prefix is not None:
                _close_block()
                current_block = _Block(
                    part=current_part,
                    row_index=int(m_prefix.group("idx")),
                    raw_lines=[line.rstrip()],
                    page=page_idx,
                    account_group=(
                        current_account
                        if current_part
                        in (
                            Part.PART_2_EMPL_ASSETS,
                            Part.PART_5_SPOUSE_ASSETS,
                            Part.PART_6_OTHER_ASSETS,
                            Part.PART_7_TRANSACTIONS,
                        )
                        else None
                    ),
                )
                continue
            # Continuation line — attach to current block if there is one.
            if current_block is not None:
                current_block.raw_lines.append(line.rstrip())
    _close_block()

    return [_tokenize_block(b) for b in blocks]


@dataclass
class _Block:
    """Internal row-block used before tokenization."""

    part: Part
    row_index: int
    raw_lines: list[str]
    page: int
    account_group: int | None


def _tokenize_block(b: _Block) -> ParsedRow:
    """Tokenize a row block into a :class:`ParsedRow`."""
    # For most parts we prefer the first line as the primary shape, but
    # Part 7 wraps its ``type date band`` onto the following line — so we
    # join and search the whole block.
    first_line_rest = _ROW_PREFIX.match(b.raw_lines[0]).group("rest")  # type: ignore[union-attr]
    joined_rest = " ".join(
        [first_line_rest]
        + [ln.strip() for ln in b.raw_lines[1:] if ln.strip()]
    )
    raw_text = "\n".join(b.raw_lines)
    return _tokenize_row(
        b.part,
        row_index=b.row_index,
        raw_line=raw_text,
        rest=joined_rest,
        page=b.page,
        account_group=b.account_group,
    )


def summarize(rows: list[ParsedRow]) -> dict:
    """Return {part_slug: {'high': N, 'low': N, 'total': N}} + grand totals.

    Used by both the ledger reconciliation report and the D1 done-gate.
    """
    per_part: dict[str, dict[str, int]] = {}
    high_total = 0
    low_total = 0
    for r in rows:
        pp = per_part.setdefault(
            r.part.value, {"high": 0, "low": 0, "total": 0}
        )
        pp["total"] += 1
        if r.confidence == Confidence.HIGH:
            pp["high"] += 1
            high_total += 1
        else:
            pp["low"] += 1
            low_total += 1
    return {
        "per_part": per_part,
        "high": high_total,
        "low": low_total,
        "total": high_total + low_total,
    }


# ─── Internals ──────────────────────────────────────────────────────

# Page header/footer noise we skip inside a part. These are stable
# across the entire annual — verified by grep. Skipping matters most
# for row-block joining: a Part-N header repeats on every page, and if
# it lands as a continuation line on the previous row's block, that
# row's real tail (which arrives after) is lost.
_BOILERPLATE_PATTERNS = (
    re.compile(r"^OGE Form 278e"),
    re.compile(r"^\s*Instructions for Part \d+"),
    re.compile(r"^\s*If you need more pages"),
    re.compile(r"^\s*Note: This is a public form"),
    re.compile(r"^\s*Filer's Name\s"),
    re.compile(r"^Donald J\. Trump\s"),
    re.compile(r"^\s*Page Number"),
    re.compile(r"^\s*I?Page Number"),
    re.compile(r"^\s*I?\s*Page \d+ of \d+"),
    re.compile(r"^\s*Part \d+:"),
    re.compile(r"^\s*#\s+(?:Description|Organization Name|Employer|Source Name|Creditor Name)"),
)


def _is_boilerplate(line: str) -> bool:
    if not line.strip():
        return True
    for pat in _BOILERPLATE_PATTERNS:
        if pat.search(line):
            return True
    return False


def _find_bands(text: str) -> list[re.Match[str]]:
    """Return every closed-vocabulary band match in ``text`` (leftmost,
    non-overlapping, longest-first via alternation ordering)."""
    return list(_ROW_BAND_ANCHOR.finditer(text))


def _tokenize_row(
    part: Part,
    *,
    row_index: int,
    raw_line: str,
    rest: str,
    page: int,
    account_group: int | None,
) -> ParsedRow:
    """Dispatch to the per-part tokenizer. Every path returns a
    ``ParsedRow`` — none raise, none coerce."""
    if part in (
        Part.PART_2_EMPL_ASSETS,
        Part.PART_5_SPOUSE_ASSETS,
        Part.PART_6_OTHER_ASSETS,
    ):
        return _tokenize_assets_row(
            part, row_index, raw_line, rest, page, account_group
        )
    if part == Part.PART_7_TRANSACTIONS:
        return _tokenize_part7_row(row_index, raw_line, rest, page, account_group)
    if part == Part.PART_1_POSITIONS:
        return _tokenize_part1_row(row_index, raw_line, rest, page)
    if part == Part.PART_8_LIABILITIES:
        return _tokenize_part8_row(row_index, raw_line, rest, page)
    # Parts 3, 4, 9 have narrative fields (or Part 9 exact-dollar gifts
    # with sensitive natural-person source names) that D1 does not
    # attempt to structure — we ledger the row and hand it off to D2 /
    # vision. Part 9 in particular MUST NOT be auto-structured in D1
    # because its source names include private individuals (Scrutiny
    # Agent is D2's responsibility per design §6).
    return ParsedRow(
        part=part,
        row_index=row_index,
        raw_text=raw_line,
        page=page,
        account_group=None,
        confidence=Confidence.LOW,
        reason="narrative_part_not_structured_in_d1",
    )


def _tokenize_assets_row(
    part: Part,
    row_index: int,
    raw_line: str,
    rest: str,
    page: int,
    account_group: int | None,
) -> ParsedRow:
    """Parts 2 / 5 / 6 — assets/income table row.

    Shape: ``# | Description | EIF | Value | Income Type | Income Amount``

    HIGH requires: at least one band from :data:`VALUE_BANDS` present
    as the Value column. Income Type + Income Amount are optional.
    """
    bands = _find_bands(rest)
    if not bands:
        return ParsedRow(
            part=part,
            row_index=row_index,
            raw_text=raw_line,
            page=page,
            account_group=account_group,
            confidence=Confidence.LOW,
            reason="no_value_band_matched",
        )
    # Value = first band; Income Amount = second band if present.
    value_band = bands[0].group("band")
    income_band: str | None = bands[1].group("band") if len(bands) > 1 else None
    # Everything before the first band, minus the row number prefix and
    # trailing EIF marker, is the description. EIF is a small closed set:
    # "N/A" or (rarely) blank or "Yes". Split off the EIF token.
    lead = rest[: bands[0].start()].rstrip()
    description, eif = _split_off_eif(lead)
    # Income Type sits between value_band and income_band, if present.
    income_type: str | None = None
    if len(bands) > 1:
        between = rest[bands[0].end() : bands[1].start()].strip()
        income_type = between or None
    return ParsedRow(
        part=part,
        row_index=row_index,
        raw_text=raw_line,
        page=page,
        account_group=account_group,
        confidence=Confidence.HIGH,
        parsed={
            "description": description,
            "eif": eif,
            "value_band": value_band,
            "income_type": income_type,
            "income_band": income_band,
        },
    )


def _split_off_eif(lead: str) -> tuple[str, str | None]:
    """Split the description/EIF pair off the front of an assets row.

    OGE 278e allows EIF ∈ {"N/A", "Yes", "No"} or blank. The pdftotext
    layout keeps ~4+ spaces between columns, so we take the LAST
    whitespace-run before an ``N/A``/``Yes``/``No`` token.
    """
    m = re.search(r"\s{2,}(N/A|Yes|No)\s*$", lead)
    if m is not None:
        return lead[: m.start()].strip(), m.group(1)
    return lead.strip(), None


def _tokenize_part7_row(
    row_index: int,
    raw_line: str,
    rest: str,
    page: int,
    account_group: int | None,
) -> ParsedRow:
    """Part 7 — transactions.

    Shape: ``# | Description | Type | Date | Amount``.  Anchor is the
    tail ``Type Date Band``; description is everything before it.
    """
    m = _PART7_TAIL.search(rest)
    if m is None:
        return ParsedRow(
            part=Part.PART_7_TRANSACTIONS,
            row_index=row_index,
            raw_text=raw_line,
            page=page,
            account_group=account_group,
            confidence=Confidence.LOW,
            reason="no_tail_type_date_band",
        )
    description = rest[: m.start()].strip()
    return ParsedRow(
        part=Part.PART_7_TRANSACTIONS,
        row_index=row_index,
        raw_text=raw_line,
        page=page,
        account_group=account_group,
        confidence=Confidence.HIGH,
        parsed={
            "description": description,
            "transaction_type": m.group("type"),
            "trade_date": m.group("date"),
            "amount_band": m.group("band"),
        },
    )


def _tokenize_part1_row(
    row_index: int, raw_line: str, rest: str, page: int
) -> ParsedRow:
    """Part 1 — positions held.

    Shape: ``# | Organization Name | City/State | Org Type | Position | From | To``.

    Column geometry is fluid enough (multi-line names, wrapped positions)
    that D1 does not attempt to split into every field. HIGH requires
    only that a plausible organization-name token be present in the
    lead; D2 does the structured extraction from the ledger row.
    """
    # A completely blank row after the "N." prefix is a stub — no data.
    if not rest.strip():
        return ParsedRow(
            part=Part.PART_1_POSITIONS,
            row_index=row_index,
            raw_text=raw_line,
            page=page,
            account_group=None,
            confidence=Confidence.LOW,
            reason="empty_row_stub",
        )
    return ParsedRow(
        part=Part.PART_1_POSITIONS,
        row_index=row_index,
        raw_text=raw_line,
        page=page,
        account_group=None,
        confidence=Confidence.HIGH,
        parsed={"lead": rest.strip()},
    )


def _tokenize_part8_row(
    row_index: int, raw_line: str, rest: str, page: int
) -> ParsedRow:
    """Part 8 — liabilities.

    Shape: ``# | Creditor | Type | Amount | Year Incurred | Rate | Term``.

    HIGH requires a band from :data:`VALUE_BANDS` present as the Amount
    column. D2 splits creditor/type/year/rate/term — D1 only ledgers.
    """
    bands = _find_bands(rest)
    if not bands:
        return ParsedRow(
            part=Part.PART_8_LIABILITIES,
            row_index=row_index,
            raw_text=raw_line,
            page=page,
            account_group=None,
            confidence=Confidence.LOW,
            reason="no_value_band_matched",
        )
    return ParsedRow(
        part=Part.PART_8_LIABILITIES,
        row_index=row_index,
        raw_text=raw_line,
        page=page,
        account_group=None,
        confidence=Confidence.HIGH,
        parsed={
            "amount_band": bands[0].group("band"),
            "raw_lead": rest[: bands[0].start()].strip(),
            "raw_tail": rest[bands[0].end() :].strip(),
        },
    )
