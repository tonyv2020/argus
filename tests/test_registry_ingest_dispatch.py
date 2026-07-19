"""P4 C — verify the four ingesters' registry-dispatch wiring.

Every ingester exposes ``ingest_from_registry(priority_domains, …)``
that delegates to the registry via ``anchors_for_<name>``. This test
suite is a white-box smoke check: the entrypoints exist, the priority-
domain filter parameter is threaded through, and the FEC dispatch
prefers external IDs over name-search when both are present on an
anchor row.
"""

from __future__ import annotations

import inspect

from app.services.ingest import fec, sec_edgar, senate_lda, usaspending


def test_every_ingester_has_ingest_from_registry() -> None:
    """All 4 ingesters expose the P4 dispatch entrypoint."""
    for mod in (fec, usaspending, senate_lda, sec_edgar):
        assert hasattr(mod, "ingest_from_registry"), mod.__name__
        assert inspect.iscoroutinefunction(mod.ingest_from_registry), mod.__name__


def test_priority_domains_param_threaded_everywhere() -> None:
    """The priority-set-driven ingestion (P4 spec §2) filters at the
    registry read — every ingester must accept ``priority_domains``."""
    for mod in (fec, usaspending, senate_lda, sec_edgar):
        sig = inspect.signature(mod.ingest_from_registry)
        assert "priority_domains" in sig.parameters, mod.__name__


def test_fec_registry_prefers_external_id_over_name_search() -> None:
    """The America-PAC-vs-FXAIX correctness case — when an anchor has
    both fec_committee_ids AND name_variants, the external ID wins.

    Check the control-flow: the ``if anchor.fec_committee_ids`` branch
    is placed before the ``elif anchor.name_variants`` fallback.
    """
    src = inspect.getsource(fec.ingest_from_registry)
    idx_if = src.find("if anchor.fec_committee_ids")
    idx_elif = src.find("elif anchor.name_variants")
    assert idx_if > 0, "external-ID branch missing"
    assert idx_elif > 0, "name-variants fallback branch missing"
    assert idx_if < idx_elif, (
        "external-ID must be the primary branch — name-variants is fallback"
    )


def test_fec_ingest_pac_now_accepts_committee_id() -> None:
    """The external-ID resolution path (P4 correctness) is exposed on
    the shared ingest_pac entrypoint — a caller with a known committee
    id can skip the fuzzy search."""
    sig = inspect.signature(fec.ingest_pac)
    assert "committee_id" in sig.parameters


def test_sec_registry_dispatches_to_ingest_anchor() -> None:
    """SEC dispatches into the existing ingest_anchor per-CIK loop —
    only the anchor source is different."""
    src = inspect.getsource(sec_edgar.ingest_from_registry)
    assert "ingest_anchor(" in src
    assert "SecAnchor(" in src, (
        "must synth a SecAnchor from the registry row's sec_cik"
    )


def test_lda_registry_iterates_lda_client_names_list() -> None:
    """An anchor's ``lda_client_names`` list holds multiple LDA surface
    names (CoreCivic + Corrections Corp of America pre-rename). All
    must be swept — not just the first."""
    src = inspect.getsource(senate_lda.ingest_from_registry)
    assert "for client_name in anchor.lda_client_names" in src, (
        "LDA must sweep every client name per anchor"
    )
