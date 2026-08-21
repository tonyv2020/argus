"""P1.6.2 — FEC individual-contributor identity predicate tests.

Hermetic: no HTTP, no DB. These rows are VERBATIM shapes returned by
``/v1/schedules/schedule_a/?contributor_name=THIEL, PETER`` on
2026-08-21 — the sweep returns 464 rows of which 296 are Peter Thiel,
and the rest are other real, private people. Getting this predicate
wrong attributes a private individual's giving to a billionaire, so
every clause has a test.
"""

from __future__ import annotations

import pytest

from app.services.ingest.fec_individual import (
    DONOR_IDENTITIES,
    MEMO_CODE,
    THIEL,
    DonorIdentity,
    check_identity,
    contribution_citation_url,
    split_contributor_name,
)


def _row(**kw) -> dict:
    base = {
        "contributor_name": "THIEL, PETER",
        "contributor_city": "SAN FRANCISCO",
        "contributor_state": "CA",
        "contributor_employer": "THIEL CAPITAL LLC",
        "contributor_occupation": "PRESIDENT",
        "contribution_receipt_amount": 1000.0,
        "sub_id": "1",
    }
    base.update(kw)
    return base


# ─── accepts ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "name,employer,occupation,city",
    [
        # The four employer shapes that carry 296 real rows.
        ("THIEL, PETER", "THIEL CAPITAL LLC", "PRESIDENT", "WEST HOLLYWOOD"),
        ("THIEL, PETER", "FOUNDERS FUND", "MANAGING PARTNER", "LOS ANGELES"),
        ("THIEL, PETER", "CLARIUM CAPITAL", "EXECUTIVE", "SAN FRANCISCO"),
        ("THIEL, PETER", "FACEBOOK", "DIRECTOR", "SAN FRANCISCO"),
        # Honorific + middle initial.
        ("THIEL, PETER MR.", "THIEL CAPITAL", "PRESIDENT", "SAN FRANCISCO"),
        ("THIEL, PETER A.", "THIEL CAPITAL", "CHAIRMAN", "SAN FRANCISCO"),
        ("THIEL, PETER A MR", "CLARIUM CAPITAL PARTNERS", "PRESIDENT", "SAN FRANCISCO"),
        # Filer typed the occupation into the employer field and vice
        # versa — the joined blob still matches.
        ("THIEL, PETER", "PRESIDENT", "THIEL CAPITAL, LLC", "WEST HOLLYWOOD"),
        # Live misspellings of Clarium seen in the corpus.
        ("THIEL, PETER", "CLARIUM CAPITOL", "INVESTMENTS", "SAN FRANCISCO"),
        ("THIEL, PETER", "CALRIUM CAPITAL", "EXECUTIVE", "SAN FRANCISCO"),
    ],
)
def test_accepts_the_real_donor(name, employer, occupation, city) -> None:
    check = check_identity(
        _row(
            contributor_name=name,
            contributor_employer=employer,
            contributor_occupation=occupation,
            contributor_city=city,
        ),
        THIEL,
    )
    assert check.accepted, check.reason


# ─── refuses ────────────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "name,expected",
    [
        ("THIELEN, PETER", "surname_mismatch"),
        ("THIELKE, PETER", "surname_mismatch"),
        ("THIELE, PETER", "surname_mismatch"),
        ("THIELMAN, PETER", "surname_mismatch"),
        ("THIELEN, JOHN PETER", "surname_mismatch"),
        ("THIEL, JOHN", "given_name_mismatch"),
        ("PETER THIEL", "unparsable_name"),
    ],
)
def test_refuses_a_different_family_or_given_name(name, expected) -> None:
    """These are the near-miss surnames the live fuzzy search returns.
    An exact normalized surname match is the clause that kills them."""
    check = check_identity(_row(contributor_name=name), THIEL)
    assert not check.accepted
    assert check.reason == expected


def test_refuses_a_middle_name_that_is_a_whole_other_name() -> None:
    """A middle POSITION may hold an initial or an honorific, never a
    second given name — otherwise 'THIEL, PETER JOHN' walks in."""
    check = check_identity(_row(contributor_name="THIEL, PETER JOHN"), THIEL)
    assert not check.accepted
    assert check.reason == "unexpected_name_tokens"


@pytest.mark.parametrize(
    "city,state,employer,occupation",
    [
        # Bay City MI — a General Motors factory worker named Peter Thiel.
        ("BAY CITY", "MI", "GENERAL MOTORS CORPORATION", "FACTORY WORKER"),
        # Ottertail MN — a self-employed contractor named Peter Thiel.
        ("OTTERTAIL", "MN", "SELF EMPLOYED", "CONTRACTOR"),
        # Georgetown TX — a broker named Peter Thiel.
        ("GEORGETOWN", "TX", "TRAIL RIDGE TRADING", "BROKER"),
    ],
)
def test_refuses_a_real_private_person_with_the_exact_same_name(
    city, state, employer, occupation
) -> None:
    """The whole reason the predicate needs a corroborating affiliation:
    an exact name match is NOT identity. Attributing these rows to Peter
    Thiel is both a data error and a privacy incident."""
    check = check_identity(
        _row(
            contributor_city=city,
            contributor_state=state,
            contributor_employer=employer,
            contributor_occupation=occupation,
        ),
        THIEL,
    )
    assert not check.accepted
    assert check.reason == "no_affiliation_match"


def test_near_miss_in_state_gets_its_own_bucket() -> None:
    """A row that is probably our donor but reports an employer we have
    not declared must be REFUSED and made visible, not quietly dropped —
    it is the operator's cue to widen the declaration."""
    check = check_identity(
        _row(
            contributor_city="LOS ANGELES",
            contributor_state="CA",
            contributor_employer="SELF EMPLOYED",
            contributor_occupation="INVESTOR",
        ),
        THIEL,
    )
    assert not check.accepted
    assert check.reason == "no_affiliation_match_in_state"


def test_state_clause_refuses_an_affiliated_row_from_a_foreign_state() -> None:
    identity = DonorIdentity(
        label="T", last_name="THIEL", first_names=frozenset({"PETER"}),
        affiliation_patterns=(r"THIEL CAPITAL",), states=frozenset({"CA"}),
    )
    check = check_identity(_row(contributor_state="MN"), identity)
    assert not check.accepted
    assert check.reason == "state_mismatch"


def test_identity_with_no_declared_states_skips_the_state_clause() -> None:
    """``states`` is optional — a donor who moves often is corroborated
    by affiliation alone."""
    identity = DonorIdentity(
        label="T", last_name="THIEL", first_names=frozenset({"PETER"}),
        affiliation_patterns=(r"THIEL CAPITAL",),
    )
    assert check_identity(_row(contributor_state="ZZ"), identity).accepted


# ─── shape + generalization ─────────────────────────────────────────────


def test_split_contributor_name() -> None:
    assert split_contributor_name("THIEL, PETER A.") == ("THIEL", ["PETER", "A"])
    assert split_contributor_name("VAN DER BERG, JAN") == (
        "VAN DER BERG", ["JAN"]
    )
    assert split_contributor_name("no comma here") == ("", [])


def test_default_queries_are_derived_from_the_identity() -> None:
    """P1.7 adds Musk as data: an identity with no explicit
    ``search_queries`` still produces the LAST, FIRST shape FEC wants."""
    musk = DonorIdentity(
        label="Elon Musk", last_name="MUSK", first_names=frozenset({"ELON"}),
        affiliation_patterns=(r"TESLA", r"SPACEX"),
    )
    assert musk.queries == ("MUSK, ELON",)
    assert check_identity(
        {
            "contributor_name": "MUSK, ELON",
            "contributor_state": "TX",
            "contributor_employer": "SPACEX",
            "contributor_occupation": "CEO",
        },
        musk,
    ).accepted


def test_thiel_is_registered_and_sweeps_multiple_cycles() -> None:
    """Schedule A partitions by two-year period; a single-cycle sweep
    would miss the 2022 Senate money entirely."""
    assert DONOR_IDENTITIES["thiel"] is THIEL
    assert 2022 in THIEL.two_year_periods
    assert len(THIEL.two_year_periods) >= 4


def test_memo_code_constant_matches_fec() -> None:
    """Earmarked receipts are itemized twice — conduit and ultimate
    recipient — with the second flagged 'X'. Summing both inflates the
    donor's total."""
    assert MEMO_CODE == "X"


def test_citation_url_points_at_the_public_transaction() -> None:
    url = contribution_citation_url("C00777185", "4082220221234567890")
    assert url.startswith("https://www.fec.gov/data/receipts/")
    assert "C00777185" in url and "4082220221234567890" in url


# ─── repair of the pre-P1.6 emitter's damage ────────────────────────────


def test_repair_keeps_one_citation_per_accepted_transaction() -> None:
    """The pre-P1.6 emitter added a citation row on EVERY run and
    re-added the amount to the weight, so live edges carry the same
    ``sub_id`` up to seven times and Thiel's published Saving Arizona
    figure reads $140,000,000 against a true $20,000,000."""
    from app.services.ingest.fec_individual import EdgeRepair

    repair = EdgeRepair(
        edge_id="e1", committee="SAVING ARIZONA PAC",
        old_weight=140_000_000.0, new_weight=20_000_000.0,
        citations_before=35, duplicate_citations_removed=30,
        memo_citations_removed=0, refused_citations_removed=0,
        citations_after=5, delete_edge=False, publication_state="published",
        keep_citation_ids=("a", "b", "c", "d", "e"),
        drop_citation_ids=tuple(f"d{i}" for i in range(30)),
    )
    assert repair.citations_after == len(repair.keep_citation_ids)
    assert (
        repair.citations_before
        == len(repair.keep_citation_ids) + len(repair.drop_citation_ids)
    )
    # The report must not leak row ids into the operator's JSON.
    assert "keep_citation_ids" not in repair.to_dict()
    assert repair.to_dict()["new_weight"] == 20_000_000.0


def test_repair_row_ids_are_decided_at_plan_time() -> None:
    """The dry-run preview and the apply must not be able to diverge —
    the apply replays ids the PLAN chose, it does not re-decide."""
    import inspect

    from app.services.ingest import fec_individual

    src = inspect.getsource(fec_individual.apply_repair)
    assert "repair.drop_citation_ids" in src
    assert "check_identity" not in src, (
        "apply must not re-run the predicate; it replays the plan"
    )


def test_citation_existence_check_does_not_assume_uniqueness() -> None:
    """There is no unique index on (edge_id, citation_ref), and live
    edges genuinely carry duplicates. `scalar_one_or_none` here raised
    MultipleResultsFound on 51 of Thiel's 83 accepted rows."""
    import inspect

    from app.services.ingest import fec_individual

    src = inspect.getsource(fec_individual._citation_exists)
    body = src.split('"""')[-1]  # the docstring names the bug it fixes
    assert "scalar_one_or_none" not in body
    assert ".limit(1)" in body


def test_sweep_covers_enough_cycles_to_classify_old_citations() -> None:
    """The repair DEFERS any edge carrying a sub_id the sweep never saw,
    so the period list has to reach back past the oldest citation on the
    donor's edges rather than just the recent cycles."""
    assert min(THIEL.two_year_periods) <= 2010
