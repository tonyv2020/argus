"""P4 seed script — shape + coverage tests.

The seed data itself is a curated fact list. Tests here guard against
regressions: every anchor a downstream ingester expects to see must be
in the seed, and every external-ID field must be well-typed.
"""

from __future__ import annotations

import pytest

from app.services.ingest.seed_anchor_registry import (
    _ALL_SEED,
    _DETENTION_OPERATORS,
    _MUSK_NETWORK,
    _PRISON_TELECOM,
    _SURVEILLANCE,
    SeedRow,
)


def test_seed_covers_five_priority_domains() -> None:
    """Five priority domains land in the seed — detention operators,
    prison telecom, surveillance, musk network (congress joins in PR D
    via the roster ingester, not this seed)."""
    domains = {row.priority_domain for row in _ALL_SEED}
    assert domains >= {
        "detention_operators",
        "prison_telecom",
        "surveillance",
        "musk_network",
    }


def test_detention_operators_include_all_p1_anchors() -> None:
    labels = {r.label for r in _DETENTION_OPERATORS}
    assert labels == {
        "GEO Group",
        "CoreCivic",
        "Management & Training Corp",
        "LaSalle Corrections",
    }


def test_prison_telecom_includes_the_four_extension_anchors() -> None:
    labels = {r.label for r in _PRISON_TELECOM}
    assert labels == {
        "Securus Technologies",
        "Aventiv Technologies",
        "Satellite Tracking of People",
        "GTL / ViaPath",
    }


def test_surveillance_includes_the_p1_6_anchors() -> None:
    labels = {r.label for r in _SURVEILLANCE}
    assert labels >= {
        "Palantir Technologies",
        "Axon Enterprise",
        "Flock Safety",
        "Clearview AI",
        "Peter Thiel",
    }


def test_musk_network_includes_the_p1_7_anchors() -> None:
    labels = {r.label for r in _MUSK_NETWORK}
    assert labels >= {
        "Elon Musk",
        "Tesla",
        "SpaceX",
        "America PAC",
    }


def test_america_pac_keyed_on_fec_committee_id_not_name() -> None:
    """The correctness argument for P4 — name-only anchors gave us
    "AMERICA PAC" resolving to the FXAIX mutual fund. America PAC MUST
    carry its FEC committee id."""
    entry = next(r for r in _MUSK_NETWORK if r.label == "America PAC")
    assert entry.fec_committee_ids, entry
    assert entry.fec_committee_ids[0].startswith("C"), entry.fec_committee_ids


def test_sec_ciks_are_integer_typed_where_present() -> None:
    """SEC's CIK zero-pad-to-10-digits path expects an int (a str would
    zero-pad wrong or fail the format string)."""
    for row in _ALL_SEED:
        if row.sec_cik is not None:
            assert isinstance(row.sec_cik, int), row


def test_persons_are_open_surface_mode() -> None:
    """Public officials + public figures — Thiel + Musk. P1.5 congress
    (roster) will also be `open`. NEVER default a `person` row to
    open without a public-official rationale — the privacy gate exists."""
    for row in _ALL_SEED:
        if row.entity_type == "person":
            assert row.surface_mode == "open", row


def test_all_rows_are_seedrow_dataclass_shape() -> None:
    """Guard against a copy-paste that omits the priority_domain."""
    for row in _ALL_SEED:
        assert isinstance(row, SeedRow), row
        assert row.priority_domain, row
        assert row.label, row
        assert row.entity_type in {
            "organization", "person", "pac", "committee", "bill",
        }, row


def test_no_duplicate_label_type_pairs() -> None:
    """Composite unique constraint on the DB is (label, entity_type);
    the seed must not violate it."""
    seen: set[tuple[str, str]] = set()
    for row in _ALL_SEED:
        key = (row.label, row.entity_type)
        assert key not in seen, key
        seen.add(key)
