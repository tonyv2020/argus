"""D2.1 — issuer normalization for OGE 278e descriptions.

The D1 parser preserves the raw Description column verbatim; D2 emitted
the raw description as the target canonical's name, which produced 3387
new nodes vs 784 matched, with ~316 of the new nodes carrying bond-
descriptor cruft (``PAIRED CTF``, ``REG S DUE ...``, ``5.375 REG INT
ON``) instead of resolving to the issuer.

This module normalizes each Description to its ISSUER — the same real-
world entity a pre-existing Argus canonical is likely to represent —
before the ``holds_asset`` / ``income_from`` / ``owes`` resolver runs.

Contract (helen D2.1 dispatch, 2026-08-05):

* Purely deterministic — **no LLM** touches a description here.
* ``raw_text`` on ``disclosure_rows`` stays untouched; this is a
  resolver-side transform on the ``parsed.description`` string only.
* Person-conservative: this normalizer runs on issuer/fund/bank
  descriptions (Parts 2/5/6/8), never on person names.
* The reverse — an over-strip that collapses two DISTINCT issuers to
  one — is guarded by keeping the whole issuer NP intact. We only strip
  the tail (bond-descriptor / share-class / legal suffix); we never
  drop a leading token, so ``APPLE HOSPITALITY REIT`` stays distinct
  from ``APPLE INC``.
"""

from __future__ import annotations

import re

# ─── Regex primitives ───────────────────────────────────────────────

# A "cut" pattern — everything from the first match to end-of-string is
# security/bond descriptor and gets discarded. Order matters: earlier
# cuts win because they fire on the leftmost match anyway (single anchor
# per string), but keeping simple patterns first is intentional.
_CUT_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Coupon rate — "5.375", "3.00 %", "4.85%34", "5%30"
    re.compile(r"\s*\d{1,3}(?:\.\d{1,4})?\s*%"),
    # DUE / MATURITY / MAT markers (case-insensitive whole word).
    re.compile(r"\s+\b(?:DUE|Due|MATURITY|MATURES|MAT)\b", re.IGNORECASE),
    # Bond-insurance / book-entry / trust-certificate wrappers.
    re.compile(r"\s+\b(?:B/E|B/Q|PTC|BAM|AGM|MBIA|FGIC)\b"),
    # Original-issue-discount marker (often ``OID @98.614 3.13%``).
    re.compile(r"\s+\bOID\b", re.IGNORECASE),
    # REG S / REG INT ON — regulation-S bond + regular-interest-on stubs.
    re.compile(r"\s+\bREG\s+(?:S|INT|D)\b", re.IGNORECASE),
    # DTD (dated) / FC (first coupon) / CALL@ pricing tokens.
    re.compile(r"\s+\bDTD\d", re.IGNORECASE),
    re.compile(r"\s+\bFC\d", re.IGNORECASE),
    re.compile(r"\s+\bCALL@", re.IGNORECASE),
    # Bare mid-string dates as a fallback ("12/01/30").
    re.compile(r"\s+\b\d{1,2}/\d{1,2}/\d{2,4}\b"),
)


# Trailing tokens that mean "security class / bond form" (not part of
# the issuer's name). Stripped repeatedly from the tail.
_SECURITY_TAIL_TOKENS: frozenset[str] = frozenset(
    {
        # generic share/security class markers
        "com",
        "common",
        "cmn",
        "f",
        "ctf",
        "paired",
        "stock",
        "shares",
        "units",
        "new",
        "cl",
        "class",
        "ordinary",
        "restricted",
        # bond-form markers
        "sr",
        "senior",
        "unsecured",
        "secured",
        "subordinated",
        "sub",
        "note",
        "notes",
        "nt",
        "bond",
        "bonds",
        "bnd",
        "debenture",
        "debentures",
        "conv",
        "convertible",
        "gtd",
        "guaranteed",
        # OCR/space-collapsed run-on tail like "IREIT" from "REIT I".
        "ireit",
    }
)

# Trailing single-letter class marker ("REIT I", "SER A"). 2-letter
# tokens like "AL"/"BD"/"BK" show up in muni-bond issuer NAMES ("ALASKA
# MUN BD BK AL"), so we do NOT treat those as class markers.
_CLASS_LETTER_RE = re.compile(r"^[A-Z]$")
# 1-2 letter class marker AFTER a CL/CLASS/SER/SERIES keyword.
_CLASS_LETTER_AFTER_KEYWORD_RE = re.compile(r"^[A-Z]{1,2}$")

# Two-space column split — peels off the value/income tail the parser
# occasionally left concatenated with the description.
_MULTI_SPACE_RE = re.compile(r"\s{2,}")

# Leading punctuation cleanup (OGE `***` = "new since last filing").
_LEAD_PUNCT_RE = re.compile(r"^[\s\W_]+")

# Trailing muni-bond descriptor stubs — REV VARIOUS PURP, SER A, MTG
# REV, INCM TAX REV — describe the bond series, not the issuer.
_MUNI_TAIL_STUB_RE = re.compile(
    r"\s+\b("
    r"REV(?:ENUE)?(?:\s+(?:RFDG|REFUNDING|VARIOUS|SPL|SPECIAL|IMPT|"
    r"IMPROVEMENT|PJ|PROJECT|WTS|WARRANT|TAX|GO|G\.O\.))*"
    r"|RFDG"
    r"|REFUNDING"
    r"|VARIOUS\s+PURP(?:OSE)?"
    r"|SER(?:IES)?\s+[A-Z]{1,3}\b(?:-\d+)?"
    r"|MTG(?:\s+REV)?"
    r"|MORTGAGE(?:\s+REV)?"
    r"|INCM\s+TAX\s+REV"
    r"|GROSS\s+RCPTS\s+TAX"
    r").*$",
    re.IGNORECASE,
)


# ─── Public: normalize_issuer ──────────────────────────────────────


def normalize_issuer(description: str) -> str:
    """Reduce ``description`` to its issuer name — bond/security cruft off.

    Deterministic. Idempotent (running it twice returns the same result).
    Returns an EMPTY string only if the input was empty or entirely
    punctuation.

    Examples (from the Trump 2026 annual):

      * ``"***CARNIVAL CORP"``                  → ``"CARNIVAL CORP"``
      * ``"CARNIVAL CORP F"``                   → ``"CARNIVAL CORP"``
      * ``"CARNIVAL CORP PAIRED CTF"``          → ``"CARNIVAL CORP"``
      * ``"CARNIVAL CORP NEW (PAIRED STOCK)"``  → ``"CARNIVAL CORP"``
      * ``"CARNIVALCORPPAIREDCTF"``             → ``"CARNIVAL CORP"``
      * ``"NETFLIX INC REG S DUE 11/15/2029 5.375 REG INT ON 854000 BND"``
                                                → ``"NETFLIX INC"``
      * ``"AMAZON.COM INC B/E 03.600% 041332 DTD041322 FC101322 CALL@MW+15BP"``
                                                → ``"AMAZON.COM INC"``
      * ``"APPLE INC. 4.3%33 DUE 05/10/33"``    → ``"APPLE INC."``
      * ``"APPLE INC COM"``                     → ``"APPLE INC"``
      * ``"APPLE HOSPITALITY REIT I"``          → ``"APPLE HOSPITALITY REIT"``
      * ``"APPLE HOSPITALITY REIT IREIT"``      → ``"APPLE HOSPITALITY REIT"``
      * ``"NVIDIACORP"``                        → ``"NVIDIA CORP"``
      * ``"MICROSOFT CORP COM"``                → ``"MICROSOFT CORP"``
      * ``"MICROSOFTCORPORATION"``              → ``"MICROSOFT CORPORATION"``
      * ``"AKRON OH INCM TAX REV VARIOUS PURP B/E 4.00 % Due Dec 1, 2025"``
                                                → ``"AKRON OH"``
      * ``"AIR PRODUCTS AND 4.85%34 DUE 02/08/34"``
                                                → ``"AIR PRODUCTS AND"``
    """
    s = (description or "").strip()
    if not s:
        return ""

    s = _LEAD_PUNCT_RE.sub("", s)

    parts = _MULTI_SPACE_RE.split(s)
    if parts:
        s = parts[0].strip()

    s = _repair_space_collapse(s)

    for pat in _CUT_PATTERNS:
        m = pat.search(s)
        if m:
            s = s[: m.start()].rstrip()

    m = _MUNI_TAIL_STUB_RE.search(s)
    if m:
        s = s[: m.start()].rstrip()

    while s.endswith(")"):
        depth = 0
        cut_at = None
        for i in range(len(s) - 1, -1, -1):
            ch = s[i]
            if ch == ")":
                depth += 1
            elif ch == "(":
                depth -= 1
                if depth == 0:
                    cut_at = i
                    break
        if cut_at is None:
            break
        s = s[:cut_at].rstrip()

    s = _strip_security_tail(s)

    tokens = s.split()
    while (
        len(tokens) >= 3
        and _CLASS_LETTER_RE.match(tokens[-1])
    ):
        tokens.pop()
    s = " ".join(tokens)

    # D2.1 polish (helen 2026-08-06):
    #   * PRTNRSHP → PARTN (same word, two OGE-form spellings).
    #   * Strip a trailing standalone year (2016-2099 range).
    #   * Strip a trailing standalone ``DB`` (debenture / discount-bond
    #     stub).
    #   * Collapse doubled trailing multi-word tails (``CTF PARTN CTF
    #     PARTN`` → ``CTF PARTN``).
    #   * Well-known historical-alias collapses (APPLE COMPUTER INC →
    #     APPLE INC).
    #
    # Applied as a FIXED-POINT LOOP: DB-then-year cascades matter
    # (``...TAX 2020 DB`` → strip DB → ``...TAX 2020`` → strip year →
    # ``...TAX``). A single pass ordered either way misses one direction.
    s = _POLISH_PARTN_RE.sub("PARTN", s)
    for _ in range(4):
        before = s
        s = _POLISH_TRAILING_DB_RE.sub("", s).rstrip()
        s = _POLISH_TRAILING_YEAR_RE.sub("", s).rstrip()
        s = _collapse_doubled_tail(s)
        if s == before:
            break
    s = _apply_historical_aliases(s)

    return s.strip()


# ─── Internals ──────────────────────────────────────────────────────


_LEGAL_SUFFIX_SPLIT_RE = re.compile(
    r"(?i)(?<=[A-Z])(INCORPORATED|CORPORATION|COMPUTER\s*INC|COMPANY|"
    r"HOLDINGS|CORP|INC|LLC|LTD|PLC|GMBH|N\.A\.|N\.V\.|COMINC)$"
)


def _repair_space_collapse(s: str) -> str:
    """Inject spaces where OCR / Excel-copy collapsed them.

    Handles ``NVIDIACORP``, ``MICROSOFTCORPORATION``,
    ``APPLECOMPUTERINC``, ``AMAZON.COMINC``, ``CARNIVALCORPPAIREDCTF``.
    Only fires when the WHOLE input is one uppercase run-on ending in a
    known legal suffix — never touches strings that already have spaces.
    """
    if " " in s:
        return s

    collapsed_tails = (
        "PAIREDCTF",
        "PAIRED",
        "CTF",
        "COM",
        "COMMON",
        "UNITS",
        "SHARES",
    )
    changed = True
    while changed:
        changed = False
        for tail in collapsed_tails:
            if (
                s.upper().endswith(tail)
                and len(s) > len(tail)
                and s[-len(tail) - 1] != " "
            ):
                s = s[: -len(tail)].rstrip() + " " + tail
                changed = True
                break

    m = _LEGAL_SUFFIX_SPLIT_RE.search(s)
    if m:
        s = s[: m.start()].rstrip() + " " + m.group(1)

    if " " in s:
        parts = s.split(" ", 1)
        head, rest = parts[0], parts[1]
        for token_part in (head, rest):
            token_m = _LEGAL_SUFFIX_SPLIT_RE.search(token_part)
            if token_m and token_part is head:
                head = head[: token_m.start()].rstrip() + " " + token_m.group(1)
            elif token_m and token_part is rest:
                rest = rest[: token_m.start()].rstrip() + " " + token_m.group(1)
        for word in ("COMPUTER", "SYSTEMS", "PARTNERS", "HOLDINGS", "GROUP"):
            if word in head and " " + word not in head:
                head = head.replace(word, " " + word, 1).strip()
            if word in rest and " " + word not in rest:
                rest = rest.replace(word, " " + word, 1).strip()
        s = head + " " + rest

    return s.strip()


# D2.1 polish primitives (helen 2026-08-06).

# PRTNRSHP is another OGE spelling of PARTN (both = Certificate of
# Partnership). Unify to PARTN so muni sibling nodes collapse.
_POLISH_PARTN_RE = re.compile(r"\bPRTNRSHP\b", re.IGNORECASE)

# Trailing standalone year (2016-2099) — used on some CTF PARTN muni
# rows as the issuance year suffix (``... CTF PARTN 2025``). Not part
# of the issuer identity.
_POLISH_TRAILING_YEAR_RE = re.compile(r"\s+\b(?:20[1-9][0-9])\b\s*$")

# Trailing standalone ``DB`` — muni-bond issuers occasionally carry it
# as a bond-class stub (``FORT BEND CNTY TX CTF OBLIG DB``).
_POLISH_TRAILING_DB_RE = re.compile(r"\s+\bDB\b\s*$")

# Well-known historical org-name changes. Curated + tiny by design —
# only well-established renames get an entry so we never accidentally
# collapse two genuinely distinct entities.
_HISTORICAL_ALIASES: dict[str, str] = {
    "apple computer inc": "APPLE INC",
    "apple computer inc.": "APPLE INC.",
    "apple computer": "APPLE",
}


def _collapse_doubled_tail(s: str) -> str:
    """Collapse a doubled trailing multi-word tail — ``X CTF PARTN CTF
    PARTN`` → ``X CTF PARTN``. Works for tail lengths 1-3 tokens and
    only when the same token sequence appears twice at the end.
    """
    tokens = s.split()
    if len(tokens) < 4:
        return s
    for tail_len in (3, 2, 1):
        if len(tokens) < 2 * tail_len:
            continue
        tail = tokens[-tail_len:]
        prev = tokens[-2 * tail_len : -tail_len]
        if [t.upper() for t in tail] == [t.upper() for t in prev]:
            return " ".join(tokens[:-tail_len])
    return s


def _apply_historical_aliases(s: str) -> str:
    """Replace a well-known historical org name with its modern form
    (``APPLE COMPUTER INC`` → ``APPLE INC``). Keyed on the lowercase
    exact normalized form; returns the input unchanged when no entry
    applies.
    """
    hit = _HISTORICAL_ALIASES.get(s.lower())
    return hit if hit is not None else s


def _strip_security_tail(s: str) -> str:
    """Drop trailing security-class tokens repeatedly. Preserves ≥1
    token so a non-empty input never returns ``""``."""
    tokens = s.split()
    while len(tokens) > 1:
        last = tokens[-1].lower().rstrip(".,;:")
        if last in _SECURITY_TAIL_TOKENS:
            tokens.pop()
            continue
        if _CLASS_LETTER_AFTER_KEYWORD_RE.match(tokens[-1]) and len(tokens) >= 2:
            prev = tokens[-2].lower().rstrip(".,;:")
            if prev in ("cl", "class", "ser", "series"):
                tokens.pop()
                tokens.pop()
                continue
        break
    return " ".join(tokens)
