"""Closed-vocabulary value-band → (band_low, band_high) map (D2).

Every band string that D1 accepts as HIGH is mapped here to its numeric
low/high bounds. D2 stores BOTH the raw band string AND the numeric
bounds on every emitted ``holds_asset`` / ``income_from`` / ``owes``
edge — never a fabricated point value.

The map is EXHAUSTIVE against :data:`app.services.disclosure_parser.VALUE_BANDS`
(19 entries). A test verifies the two sets are equal so a new band added
in the parser cannot land without a band_low/band_high assignment here.

Convention: "None (or less than $X)" → low=0, high=X-1. "Over $X" →
low=X+1, high=None (open-ended). Ranges are inclusive-of-endpoints per
the OGE 278e instructions.
"""

from __future__ import annotations

from typing import Final

# band_low is 0 when the band is a "None (or less than $X)" marker;
# band_high is None (open-ended) when the band is "Over $X".
VALUE_BAND_BOUNDS: Final[dict[str, tuple[int, int | None]]] = {
    "None (or less than $201)": (0, 200),
    "None (or less than $1,001)": (0, 1000),
    "$0 - $200": (0, 200),
    "$201 - $1,000": (201, 1000),
    "$1,001 - $2,500": (1001, 2500),
    "$1,001 - $15,000": (1001, 15000),
    "$2,501 - $5,000": (2501, 5000),
    "$5,001 - $15,000": (5001, 15000),
    "$15,001 - $50,000": (15001, 50000),
    "$50,001 - $100,000": (50001, 100000),
    "$100,001 - $250,000": (100001, 250000),
    "$100,001 - $1,000,000": (100001, 1_000_000),
    "$250,001 - $500,000": (250001, 500000),
    "$500,001 - $1,000,000": (500001, 1_000_000),
    "$1,000,001 - $5,000,000": (1_000_001, 5_000_000),
    "$5,000,001 - $25,000,000": (5_000_001, 25_000_000),
    "$25,000,001 - $50,000,000": (25_000_001, 50_000_000),
    "Over $5,000,000": (5_000_001, None),
    "Over $50,000,000": (50_000_001, None),
}


def bounds_of(band: str) -> tuple[int, int | None] | None:
    """Return ``(band_low, band_high)`` for a known band, or None if the
    band string is not in the closed vocabulary.

    The parser has already anchored on this same vocabulary at the row
    level, so in D2's normal path ``band`` always resolves. This function
    still returns ``None`` on an unknown band so any regression at a
    later time turns into a visible reconciliation gap rather than a
    silent ``band_low=0, band_high=0``.
    """
    return VALUE_BAND_BOUNDS.get(band)
