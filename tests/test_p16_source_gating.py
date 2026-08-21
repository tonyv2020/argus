"""P1.6 — external-ID gating on the USAspending and Senate LDA passes.

Both APIs take a FUZZY search parameter, so the query cannot be the
selector. These tests pin the accept gates against the exact
false-positive names each API returns for the P1.6 anchors, measured
live on 2026-08-21.
"""

from __future__ import annotations

import inspect

import pytest

from app.services.ingest import senate_lda, usaspending
from app.services.ingest.domain_anchors import SURVEILLANCE_ANCHORS
from app.services.ingest.senate_lda import client_name_accepted

PALANTIR = next(a for a in SURVEILLANCE_ANCHORS if a.label == "Palantir Technologies")
AXON = next(a for a in SURVEILLANCE_ANCHORS if a.label == "Axon Enterprise")
FLOCK = next(a for a in SURVEILLANCE_ANCHORS if a.label == "Flock Safety")
CLEARVIEW = next(a for a in SURVEILLANCE_ANCHORS if a.label == "Clearview AI")


# ─── Senate LDA client-name gate ────────────────────────────────────────


@pytest.mark.parametrize(
    "spec,name",
    [
        # All 13 spellings LDA carries for Palantir collapse to these.
        (PALANTIR, "PALANTIR TECHNOLOGIES, INC."),
        (PALANTIR, "PALANTIR TECHNOLOGIES INC"),
        (PALANTIR, "PALANTIR TECHNOLOGIES"),
        (PALANTIR, "PALANTIR"),
        # Wrapper registrations — the firm filed on Palantir's behalf.
        # These are the exact strings sitting in the graph as separate
        # organization canonicals today.
        (PALANTIR, "BROWNSTEIN HYATT FARBER SCHRECK LLP OBO PALANTIR TECHNOLOGIES INC."),
        (PALANTIR, "J.A. GREEN AND COMPANY (FOR PALANTIR TECHNOLOGIES INC.)"),
        (PALANTIR,
         "HANNEGAN LANDAU POERSCH & ROSENBAUM ADVOCACY LLC (FOR PALANTIR TECHNOLOGIES INC)"),
        (AXON, "AXON ENTERPRISE, INC."),
        (AXON, "AXON ENTERPRISE"),
        (AXON, "AXON ENTERPRISES"),
        (FLOCK, "FLOCK SAFETY"),
        (FLOCK, "FLOCK GROUP INC D/B/A FLOCK SAFETY"),
        (FLOCK, "BGR GOVERNMENT AFFAIRS, LLC ON BEHALF OF FLOCK SAFETY"),
        (CLEARVIEW, "CLEARVIEW AI"),
    ],
)
def test_lda_patterns_accept_the_anchor(spec, name) -> None:
    assert client_name_accepted(name, spec.lda_client_patterns), name


@pytest.mark.parametrize(
    "spec,name",
    [
        # Different companies the fuzzy LDA search returns alongside.
        (FLOCK, "FLOCK HOMES, INC."),
        (AXON, "AXONIUS"),
        (AXON, "AXON HOLDINGS GROUP, LLC"),
        (AXON, "AXONICS MODULATION TECHNOLOGIES, INC. (FORMERLY KNOWN AS CONTURA, INC.)"),
        (PALANTIR, "PALANTIRIII, LLC"),
        (CLEARVIEW, "CLEARVIEW CAPITAL"),
    ],
)
def test_lda_patterns_refuse_a_different_company(spec, name) -> None:
    """The pre-P1.6 gate was a SUBSTRING test, which accepts every one of
    these — 'Flock' is in 'FLOCK HOMES', 'Axon' is in 'AXONIUS'."""
    assert not client_name_accepted(name, spec.lda_client_patterns), name


def test_lda_gate_is_fail_closed_on_an_empty_pattern_set() -> None:
    """No declared pattern means nothing is accepted — never everything."""
    assert not client_name_accepted("PALANTIR TECHNOLOGIES, INC.", ())


def test_lda_substring_gate_would_have_over_matched() -> None:
    """Documents WHY the pattern gate exists, against the old helper."""
    assert senate_lda._client_name_matches(
        {"client": {"name": "FLOCK HOMES, INC."}}, "Flock"
    )
    assert not client_name_accepted(
        "FLOCK HOMES, INC.", FLOCK.lda_client_patterns
    )


# ─── USAspending UEI gate ───────────────────────────────────────────────


def test_uei_pass_requests_the_recipient_uei_field() -> None:
    """The gate is only possible if the field is in the request — without
    ``Recipient UEI`` the pass would have to trust the fuzzy
    ``recipient_search_text`` match, which returns neighbours."""
    assert "Recipient UEI" in usaspending._UEI_AWARD_FIELDS


def test_uei_pass_verifies_each_row_against_the_allowlist() -> None:
    """Querying Palantir's UEI also returns PALANTIR USG rows; querying a
    short name returns JAXON ENTERPRISES for AXON ENTERPRISE. Every row
    has to be checked against the anchor's declared UEIs."""
    src = inspect.getsource(usaspending.ingest_recipient_contracts_by_uei)
    assert "Recipient UEI" in src
    assert "awards_refused_foreign_uei" in src


def test_uei_pass_refuses_to_mint_a_recipient() -> None:
    """An anchor with a UEI but no canonical is REPORTED. Minting one
    here would reintroduce the name-keyed identity P1.6 removes."""
    src = inspect.getsource(usaspending.ingest_domain_contracts_by_uei)
    assert "unanchored" in src
    assert "canonical_id" in src


# ─── batch threading ────────────────────────────────────────────────────


@pytest.mark.parametrize(
    "fn",
    [
        usaspending._find_or_create_canonical,
        usaspending._emit_contract_edge,
        senate_lda._upsert_entity,
        senate_lda._emit_lobbies_edge,
    ],
)
def test_emitters_accept_a_batch_id(fn) -> None:
    """RG1: a P1.6 ingest must be able to stage everything it creates.
    The default stays None so the steady-state weekly sweep keeps
    writing ``published`` via the column default."""
    sig = inspect.signature(fn)
    assert "batch_id" in sig.parameters
    assert sig.parameters["batch_id"].default is None


def test_flock_and_clearview_key_on_uei_not_a_cik() -> None:
    """Both are privately held — there is no CIK to key on, so the UEI
    is the only external id and it had better be declared."""
    for spec in (FLOCK, CLEARVIEW):
        assert spec.sec_cik is None
        assert spec.usaspending_uei


# ─── robustness fixes the first live run forced ─────────────────────────


def test_agency_alias_source_id_fits_the_column() -> None:
    """`entity_aliases.source_id` is VARCHAR(64). USAspending sub-agency
    names blow through it — this one is 96 chars — and the raw insert
    raised StringDataRightTruncation, which poisoned the page's
    transaction and lost 436 awards on the first live run."""
    long_name = (
        "BUREAU OF ALCOHOL, TOBACCO, FIREARMS AND EXPLOSIVES "
        "ACQUISITION AND PROPERTY MANAGEMENT DIVISION"
    )
    assert len(long_name) > 64
    key = usaspending.agency_alias_source_id(long_name)
    assert len(key) <= 64


def test_agency_alias_source_id_is_stable_and_backward_compatible() -> None:
    """A short name keeps its exact uppercase form, so every alias
    written before this helper existed still resolves."""
    assert usaspending.agency_alias_source_id("U.S. Marshals Service") == (
        "U.S. MARSHALS SERVICE"
    )
    long_a = "OFFICE OF THE ASSISTANT SECRETARY " + "A" * 60
    long_b = "OFFICE OF THE ASSISTANT SECRETARY " + "B" * 60
    key_a = usaspending.agency_alias_source_id(long_a)
    assert key_a == usaspending.agency_alias_source_id(long_a)
    # Two sub-agencies sharing a long prefix must not collide.
    assert key_a != usaspending.agency_alias_source_id(long_b)


@pytest.mark.parametrize(
    "fn",
    [
        usaspending.ingest_recipient_contracts_by_uei,
        senate_lda.ingest_client_filings_by_pattern,
    ],
)
def test_row_loops_use_a_savepoint(fn) -> None:
    """One bad row must not poison the page's transaction. Without the
    SAVEPOINT, a single over-long agency name took down every award
    after it in the same page."""
    assert "begin_nested" in inspect.getsource(fn)


def test_citation_existence_checks_do_not_assume_uniqueness() -> None:
    """There is no unique index on (edge_id, kind, citation_ref), and
    the pre-Stage-2 emitters added a citation row per run, so historical
    edges genuinely carry the same reference more than once."""
    for fn in (usaspending._emit_contract_edge,):
        body = inspect.getsource(fn).split("citation_exists")[1]
        assert "scalar_one_or_none" not in body
