"""P4 anchor registry — shape + filter semantics.

Hermetic unit tests using an in-memory `Anchor` dataclass — the DB-touching
paths (`list_anchors`, `upsert_anchor`) belong in the integration suite
against a live Postgres.

The filter helpers (``anchors_for_fec`` etc.) are covered by making
`list_anchors` return a canned list via a hand-built fake session; keeps
the correctness of the "which anchors does the FEC ingester see" wiring
provable without a DB round-trip.
"""

from __future__ import annotations

from typing import Iterable

import pytest

from app.services.anchor_registry import (
    Anchor,
    anchors_for_fec,
    anchors_for_sec_edgar,
    anchors_for_senate_lda,
    anchors_for_usaspending,
)


class _FakeSession:
    """Just enough shape to satisfy the module — the anchor helpers call
    ``list_anchors`` which SELECT's off the session; we stub that call
    site by patching the module.
    """

    pass


@pytest.fixture()
def anchors_seed() -> list[Anchor]:
    """A representative anchor mix — one row per source-of-truth signal
    so every filter helper has both a hit and a miss."""
    return [
        # detention operator — FEC + USAspending + LDA (SEC missing)
        Anchor(
            label="GEO Group",
            entity_type="organization",
            priority_domain="detention_operators",
            fec_committee_ids=["C00382916"],
            usaspending_recipient_names=["GEO GROUP INC"],
            lda_client_names=["The GEO Group"],
            name_variants=["GEO Group Inc"],
        ),
        # SEC-only anchor — Palantir (has CIK but no PAC)
        Anchor(
            label="Palantir Technologies",
            entity_type="organization",
            priority_domain="surveillance",
            sec_cik=1321655,
            usaspending_recipient_names=["PALANTIR TECHNOLOGIES INC"],
        ),
        # Person — Peter Thiel (individual-contributor mode, no FEC committee)
        Anchor(
            label="Peter Thiel",
            entity_type="person",
            priority_domain="surveillance",
            name_variants=["Peter A. Thiel"],
            surface_mode="open",
        ),
        # PAC — no company/person surface
        Anchor(
            label="America PAC",
            entity_type="pac",
            priority_domain="musk_network",
            fec_committee_ids=["C00838163"],
            name_variants=["America PAC"],
        ),
    ]


@pytest.fixture(autouse=True)
def _patch_list_anchors(monkeypatch, anchors_seed):
    """Route `list_anchors` at the anchor_registry module to the fixture."""
    from app.services import anchor_registry as ar

    async def fake_list_anchors(session, *, priority_domains=None, entity_types=None):
        rows = list(anchors_seed)
        if priority_domains:
            rows = [r for r in rows if r.priority_domain in set(priority_domains)]
        if entity_types:
            rows = [r for r in rows if r.entity_type in set(entity_types)]
        return rows

    monkeypatch.setattr(ar, "list_anchors", fake_list_anchors)


@pytest.mark.asyncio
async def test_fec_filter_excludes_persons() -> None:
    """FEC PAC-mode ingester operates on orgs + pacs — NEVER on persons.
    The full-sweep 429'd on 537 congress name-searches when the filter
    let persons through (helen 2026-07-19 20:00 UTC). Persons go through
    ``anchors_for_fec_individual`` → Schedule A only."""
    got = await anchors_for_fec(_FakeSession())
    labels = {a.label for a in got}
    assert labels == {"GEO Group", "America PAC"}
    assert "Peter Thiel" not in labels


@pytest.mark.asyncio
async def test_fec_individual_filter_isolates_persons() -> None:
    """Persons go through Schedule A only — this filter returns exactly
    the mega-donor person anchors so the sweep can dispatch them to the
    individual-contributor path."""
    from app.services.anchor_registry import anchors_for_fec_individual

    got = await anchors_for_fec_individual(_FakeSession())
    labels = {a.label for a in got}
    assert labels == {"Peter Thiel"}


@pytest.mark.asyncio
async def test_fec_individual_filter_excludes_congress_members(
    monkeypatch,
) -> None:
    """Congress members are RECIPIENTS, not mega-donors — running
    individual-contributor mode on 537 members burns FEC quota
    (helen 2026-07-19 20:08Z 429 incident)."""
    from app.services.anchor_registry import anchors_for_fec_individual, Anchor
    from app.services import anchor_registry as ar

    congress_seed = [
        Anchor(
            label="Ted Cruz",
            entity_type="person",
            priority_domain="congress",
            fec_candidate_ids=["S8TX00232"],
        ),
        Anchor(
            label="Peter Thiel",
            entity_type="person",
            priority_domain="surveillance",
        ),
    ]

    async def fake_list(session, *, priority_domains=None, entity_types=None):
        return [a for a in congress_seed if a.entity_type in (entity_types or ("person",))]

    monkeypatch.setattr(ar, "list_anchors", fake_list)

    got = await anchors_for_fec_individual(_FakeSession())
    labels = {a.label for a in got}
    assert labels == {"Peter Thiel"}
    assert "Ted Cruz" not in labels


@pytest.mark.asyncio
async def test_usaspending_filter_needs_recipient_names() -> None:
    """USAspending ingester needs a non-empty recipient-name list —
    Thiel (person) and America PAC (pac) have none → excluded."""
    got = await anchors_for_usaspending(_FakeSession())
    labels = {a.label for a in got}
    assert labels == {"GEO Group", "Palantir Technologies"}


@pytest.mark.asyncio
async def test_lda_filter_needs_client_names() -> None:
    got = await anchors_for_senate_lda(_FakeSession())
    labels = {a.label for a in got}
    assert labels == {"GEO Group"}


@pytest.mark.asyncio
async def test_sec_filter_needs_cik() -> None:
    """SEC EDGAR needs a CIK — a name alone won't do."""
    got = await anchors_for_sec_edgar(_FakeSession())
    labels = {a.label for a in got}
    assert labels == {"Palantir Technologies"}


@pytest.mark.asyncio
async def test_priority_domain_filter_narrows_all_dispatch() -> None:
    """Passing priority_domains restricts all four dispatchers — the
    curated priority-set-driven ingestion (P4 spec §2) filters at the
    registry read, not at each ingester."""
    got = await anchors_for_fec(_FakeSession(), priority_domains=["musk_network"])
    assert [a.label for a in got] == ["America PAC"]

    got = await anchors_for_usaspending(
        _FakeSession(), priority_domains=["surveillance"]
    )
    assert [a.label for a in got] == ["Palantir Technologies"]


def test_anchor_from_row_normalizes_none_lists() -> None:
    """AnchorRegistry columns are NOT NULL server-defaulted to '[]'::jsonb
    but the ORM read still can surface None on a pre-server-default row.
    `Anchor.from_row` must return list-typed fields regardless."""

    class Row:
        label = "X"
        entity_type = "organization"
        priority_domain = None
        fec_committee_ids = None
        fec_candidate_ids = None
        sec_cik = None
        usaspending_recipient_names = None
        lda_client_names = None
        name_variants = None
        surface_mode = "open"
        canonical_id = None
        notes = None

    a = Anchor.from_row(Row())
    assert a.fec_committee_ids == []
    assert a.fec_candidate_ids == []
    assert a.usaspending_recipient_names == []
    assert a.lda_client_names == []
    assert a.name_variants == []


def test_from_row_preserves_ordering_of_committee_ids() -> None:
    """A canonical entity may have multiple FEC committees (PAC + super-PAC
    + affiliated committees). Order matters for stable log output."""

    class Row:
        label = "GEO Group"
        entity_type = "organization"
        priority_domain = "detention_operators"
        fec_committee_ids = ["C00382916", "C99999999"]
        fec_candidate_ids = []
        sec_cik = 923796
        usaspending_recipient_names = ["GEO GROUP INC"]
        lda_client_names = ["The GEO Group"]
        name_variants = []
        surface_mode = "open"
        canonical_id = None
        notes = None

    a = Anchor.from_row(Row())
    assert a.fec_committee_ids == ["C00382916", "C99999999"]
    assert a.sec_cik == 923796
