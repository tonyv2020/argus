"""P3.4 — curated individual allowlist + the identity data-guard.

Two independent things are protected here:

  * an individual surfaces ONLY via an approved allowlist entry that
    still clears every other gate, and
  * "is this a person?" is answered by the ARGUS CANONICAL, never by
    the FAA ``TYPE REGISTRANT`` code, which is wrong in both directions.
"""

from __future__ import annotations

import inspect

from app.models import AircraftIndividualAllowlist as Allow
from app.services import aircraft_identity as ident
from app.services.ingest import faa_aircraft_individual_allowlist as al


def _checks(model) -> dict[str, str]:
    return {
        c.name: str(c.sqltext) for c in model.__table__.constraints if hasattr(c, "sqltext")
    }


def test_every_migration_revision_id_fits_alembic_version_column() -> None:
    """``alembic_version.version_num`` is varchar(32). A longer revision
    id creates the table, then fails on the final version UPDATE — the
    migration rolls back and the pod CrashLoopBackOffs. Caught live on
    0014 ('0014_aircraft_individual_allowlist' is 34 chars)."""
    import pathlib

    import app

    versions = pathlib.Path(app.__file__).resolve().parents[1] / "alembic" / "versions"
    long_ids = []
    for path in versions.glob("*.py"):
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.startswith("revision = "):
                rev = line.split("=", 1)[1].strip().strip("\"'")
                if len(rev) > 32:
                    long_ids.append((path.name, rev, len(rev)))
    assert long_ids == [], f"revision ids over 32 chars: {long_ids}"


# ── PART B: the identity data-guard ──────────────────────────────


def test_company_miscoded_by_the_faa_is_NOT_an_individual() -> None:
    """UNITED AIRLINES INC and ~20 others are filed TYPE REGISTRANT 1/4.
    Gating on the FAA code wrongly withheld them from publication."""
    assert ident.is_individual_entity("organization", "UNITED AIRLINES") is False
    assert ident.is_individual_entity("organization", "Southwest Airlines") is False
    assert ident.is_individual_entity("agency", "Department Of The Air Force") is False
    assert ident.is_individual_entity("pac", "Some PAC") is False


def test_person_typed_canonical_IS_an_individual() -> None:
    assert ident.is_individual_entity("person", "Sam Graves") is True
    assert ident.is_individual_entity("candidate", "Someone") is True


def test_argus_mistyped_people_are_still_treated_as_people() -> None:
    """The five FEC disbursement recipients are real people stored as
    'organization'. Trusting the Argus type alone would newly PUBLISH
    their aircraft — the guard must override until they are retyped."""
    for name in (
        "BROOKS, WILLIAM",
        "MURPHY, MICHAEL",
        "LEWIS, DAVID S.",
        "WILSON, DAVID A",
        "ROBINSON, MICHAEL",
    ):
        assert ident.is_individual_entity("organization", name) is True, name


def test_identity_guard_fails_closed_on_unknown() -> None:
    """A row Argus cannot classify is withheld, not published."""
    assert ident.is_individual_entity(None, "Whoever") is True
    assert ident.is_individual_entity("", "Whoever") is True
    assert ident.is_individual_entity("unknown", "Whoever") is True


def test_publish_scripts_use_the_guard_not_the_faa_code() -> None:
    """Both publish paths must gate on the canonical, and neither may
    still filter on type_registrant."""
    from app.services.ingest import faa_aircraft_p32_pilot_publish as p32
    from app.services.ingest import faa_aircraft_p33_publish as p33

    for fn in (p32.select_pilot, p33.select_cohort):
        src = inspect.getsource(fn)
        assert "is_individual_entity" in src
        assert 'type_registrant or ""' not in src, "must not gate on the FAA code"


# ── PART A: the allowlist ────────────────────────────────────────


def test_allowlist_ships_empty_and_nothing_is_proposed_into_the_db() -> None:
    """This phase writes nothing. The proposal lives in code so it is
    reviewable in the PR diff, not as rows someone might approve by
    accident."""
    src = inspect.getsource(al)
    assert "PROPOSED_ENTRIES" in src
    # no INSERT of allowlist rows anywhere in the module
    assert "AircraftIndividualAllowlist(" not in src


def test_gate_reads_only_approved_entries() -> None:
    """Fail-closed: 'proposed' must never promote."""
    src = inspect.getsource(al.promote_allowlisted)
    assert 'AircraftIndividualAllowlist.status == "approved"' in src


def test_allowlist_row_must_carry_evidence_and_an_approver() -> None:
    checks = _checks(Allow)
    assert "ck_aircraft_allowlist_justified" in checks
    assert "ck_aircraft_allowlist_status" in checks
    assert "ck_aircraft_allowlist_approval_attributed" in checks
    for col in ("evidence", "source", "added_by"):
        assert Allow.__table__.columns[col].nullable is False


def test_approval_is_per_aircraft_not_per_person() -> None:
    """Approving one tail number must not sweep in another that appears
    under the same name in a later FAA snapshot."""
    uniques = {
        tuple(c.columns.keys())
        for c in Allow.__table__.constraints
        if c.__class__.__name__ == "UniqueConstraint"
    }
    assert ("n_number", "canonical_id") in uniques


def test_every_other_gate_still_applies_to_an_allowlisted_entry() -> None:
    """The allowlist is an exception to 'no individuals', not to the
    rest of the contract."""
    src = inspect.getsource(al.promote_allowlisted)
    assert 'ent.type != "person"' in src
    assert "SurfaceMode.OPEN.value" in src
    assert "PublicationState.PUBLISHED.value" in src
    assert "_hard_signal_ok" in src
    assert "await promote(" in src          # audited op, not a raw UPDATE
    assert "UPDATE" not in src.upper().replace("UPDATED", "")


def test_an_llm_scrutiny_verdict_is_not_sufficient_for_an_individual() -> None:
    """P3.1 showed LLM-verdict 'public figures' matched on bare permuted
    names with no corroboration. Only a hard public-role signal counts."""
    src = inspect.getsource(al.promote_allowlisted)
    assert "no hard public-role signal" in src
    assert set(al.PUBLIC_SOURCE_SYSTEMS) >= {"bioguide", "fec.candidate"}


def test_created_edge_is_born_dark_and_cited() -> None:
    """Individuals have no REGISTERS edge, so one is created — it must
    be born suppress/staged and carry the FAA snapshot citation."""
    src = inspect.getsource(al.promote_allowlisted)
    assert "SurfaceMode.SUPPRESS.value" in src
    assert "PublicationState.STAGED.value" in src
    assert "source_sha256=snapshot.sha256" in src
    assert "snapshot_id=snapshot.id" in src


def test_the_proposal_is_small_and_justified() -> None:
    """Tony asked for SMALL and high-confidence. Every entry must carry
    real evidence, not a bare assertion."""
    assert len(al.PROPOSED_ENTRIES) <= 3, "proposal should stay tiny"
    for e in al.PROPOSED_ENTRIES:
        assert len(e.evidence) > 120, f"{e.n_number} needs real evidence"
        assert e.source
        assert e.canonical_name and e.registrant_name
