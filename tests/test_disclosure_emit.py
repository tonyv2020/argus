"""D2 disclosure-emit tests — the fail-closed contract + primitives.

Covers:

  * Band vocabulary + bounds — closed set matches the parser's
    ``VALUE_BANDS`` exactly (a fabricated band added in the parser
    without a bounds entry fails this test).
  * ``_classify_creditor`` — the ORG / AGENCY / PERSON heuristic. The
    critical invariant: when in doubt, the classifier returns PERSON so
    the caller applies ``surface_mode=SUPPRESS``.
  * ``_split_part8_lead`` + ``_split_part8_tail`` — the D1→D2 shim that
    extracts creditor + year/rate/term from the raw Part 8 lines D1
    kept intact.
  * ``_extract_part1_org`` — the Part 1 lead-column split.
  * ``bounds_of`` never coerces an unknown band.

Every ``_upsert_*`` primitive that talks to the DB is exercised by the
live D2 run itself; here we only cover the pure helpers.
"""

from __future__ import annotations

from app.models import EntityType
from app.services.disclosure_bands import VALUE_BAND_BOUNDS, bounds_of
from app.services.disclosure_parser import VALUE_BANDS
from app.services.ingest.disclosure_emit import (
    _classify_creditor,
    _extract_part1_org,
    _split_part8_lead,
    _split_part8_tail,
)

# ─── Band vocabulary invariants ─────────────────────────────────────


def test_bounds_covers_every_parser_band_exactly() -> None:
    """The bounds map MUST cover every band the parser accepts as HIGH.

    A drift between parser and bounds would silently allow a HIGH row
    into D2 with no numeric band derivation — visible as a
    ``(band_low=0, band_high=None)`` in the graph. This test is the
    build-time canary.
    """
    assert set(VALUE_BAND_BOUNDS.keys()) == VALUE_BANDS


def test_bounds_of_returns_none_for_unknown_band() -> None:
    """A fabricated / typo'd band must return None — never a coerced
    (0, 0) or (0, band_high) that would leak a false-precision numeric
    into the graph."""
    assert bounds_of("$1 - $999") is None
    assert bounds_of("Over $1,000") is None
    assert bounds_of("") is None


def test_over_bands_have_open_upper_bound() -> None:
    """The ``Over $X`` markers must map to ``band_high=None`` so any
    aggregate that sums bands treats them as unbounded above."""
    assert VALUE_BAND_BOUNDS["Over $5,000,000"] == (5_000_001, None)
    assert VALUE_BAND_BOUNDS["Over $50,000,000"] == (50_000_001, None)


def test_none_bands_have_zero_lower_bound() -> None:
    """``None (or less than $X)`` bands must anchor at zero — never a
    fabricated point value inside the range."""
    assert VALUE_BAND_BOUNDS["None (or less than $201)"] == (0, 200)
    assert VALUE_BAND_BOUNDS["None (or less than $1,001)"] == (0, 1000)


# ─── Creditor classifier — the surface_mode=SUPPRESS gate ────────────


def test_classifier_person_is_the_default_when_in_doubt() -> None:
    """A 2-3 word capitalized name with no ORG/AGENCY hint must be
    classified as PERSON. This is the surface_mode=SUPPRESS trigger
    per design §6 (default protected)."""
    tgt, is_person = _classify_creditor("E. Jean Carroll")
    assert tgt == EntityType.PERSON.value
    assert is_person is True


def test_classifier_llc_is_organization() -> None:
    tgt, is_person = _classify_creditor("Ladder Capital Finance LLC")
    assert tgt == EntityType.ORGANIZATION.value
    assert is_person is False


def test_classifier_bank_is_organization() -> None:
    tgt, _ = _classify_creditor("Axos Bank")
    assert tgt == EntityType.ORGANIZATION.value


def test_classifier_trust_company_is_organization() -> None:
    tgt, _ = _classify_creditor("The Bryn Mawr Trust Company")
    assert tgt == EntityType.ORGANIZATION.value


def test_classifier_attorney_general_is_agency_not_person() -> None:
    """New York Attorney General is an office, not a natural person.
    Must land as AGENCY (OPEN), not PERSON (SUPPRESS)."""
    tgt, is_person = _classify_creditor("New York Attorney General")
    assert tgt == EntityType.AGENCY.value
    assert is_person is False


def test_classifier_american_express_is_organization() -> None:
    tgt, _ = _classify_creditor("American Express")
    assert tgt == EntityType.ORGANIZATION.value


# ─── Part 8 splitters ────────────────────────────────────────────────


def test_split_part8_lead_bank_row() -> None:
    """Real row from the annual: bank + descriptor."""
    creditor, ltype = _split_part8_lead(
        "The Bryn Mawr Trust Company              Seven Springs (mortgage)"
    )
    assert creditor == "The Bryn Mawr Trust Company"
    assert ltype == "Seven Springs (mortgage)"


def test_split_part8_lead_natural_person_litigation() -> None:
    """E. Jean Carroll litigation row — natural-person creditor."""
    creditor, ltype = _split_part8_lead(
        "E. Jean Carroll                          Litigation; stayed pending appeal;"
    )
    assert creditor == "E. Jean Carroll"
    assert ltype == "Litigation; stayed pending appeal;"


def test_split_part8_tail_with_year_and_rate() -> None:
    year, rate, term = _split_part8_tail("2000         4.50%          Paid Off July 2025")
    assert year == "2000"
    assert rate == "4.50%"
    assert term == "Paid Off July 2025"


def test_split_part8_tail_na_rate() -> None:
    """N/A rate + no term column — Carroll litigation shape."""
    year, rate, term = _split_part8_tail("2023         N/A            N/A")
    assert year == "2023"
    assert rate == "N/A"
    assert term == "N/A"


def test_split_part8_tail_empty_string_is_all_none() -> None:
    assert _split_part8_tail("") == (None, None, None)


# ─── Part 1 lead splitter ────────────────────────────────────────────


def test_extract_part1_org_from_wide_column_lead() -> None:
    """Real Part 1 row shape from the annual — org name is the first
    wide-column token run."""
    lead = (
        "CIC Digital LLC                     Palm Beach, FL     "
        "Limited Liability Company        Manager, President, Secretary & "
        "03/2022        01/09/2025 Treasurer"
    )
    assert _extract_part1_org(lead) == "CIC Digital LLC"


def test_extract_part1_org_from_multiword_org() -> None:
    lead = (
        "Mar-A-Lago Club, L.L.C.             Palm Beach, FL     "
        "Limited Liability Company        President                           "
        "01/24/2021        Present"
    )
    assert _extract_part1_org(lead) == "Mar-A-Lago Club, L.L.C."
