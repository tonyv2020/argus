"""P4 E — FEC individual-contributor mode shape tests.

Hermetic — checks the entrypoint surface and dispatch logic without
touching the FEC API or DB. The actual disbursement fetch loop is
covered live by the P4 E roll-out (Musk + Thiel personal giving).
"""

from __future__ import annotations

import inspect

from app.services.ingest import fec


def test_individual_contributor_entrypoint_exists() -> None:
    """The FEC ingester exposes both PAC-mode (existing) + individual-
    contributor mode (new)."""
    assert hasattr(fec, "ingest_individual_contributor")
    assert inspect.iscoroutinefunction(fec.ingest_individual_contributor)
    assert hasattr(fec, "ingest_individual_contributors_from_registry")


def test_individual_contributor_sig_carries_two_year_periods() -> None:
    """Schedule A partitions by cycle — the sweep MUST support
    multiple two-year transaction periods (current + prior cycles)."""
    sig = inspect.signature(fec.ingest_individual_contributor)
    assert "two_year_periods" in sig.parameters
    default = sig.parameters["two_year_periods"].default
    assert 2024 in default, default


def test_individual_contributor_uses_schedule_a_endpoint() -> None:
    """Individual-contributor mode hits Schedule A (contributor→committee)
    NOT Schedule B (committee→disbursement). Cross-endpoint bug would
    look like Musk contributing to himself."""
    src = inspect.getsource(fec.ingest_individual_contributor)
    assert "/schedules/schedule_a/" in src
    assert "contributor_name" in src


def test_registry_dispatch_transforms_first_last_to_fec_shape() -> None:
    """A registry person row labelled "Elon Musk" needs "MUSK, ELON"
    for the FEC contributor_name query. The dispatcher must transform
    when no LAST, FIRST is in name_variants."""
    src = inspect.getsource(fec.ingest_individual_contributors_from_registry)
    # Either explicit LAST,FIRST pull from name_variants OR name split.
    assert "name_variants" in src
    assert "rsplit" in src or "split" in src, (
        "must transform 'First Last' → 'LAST, FIRST' when no explicit "
        "variant is present"
    )


def test_registry_dispatch_filters_to_person_entity_type() -> None:
    """Individual-contributor mode operates on persons only — orgs
    have their own PAC-disbursement path. Dispatch goes through
    ``anchors_for_fec_individual`` which is defined to filter
    ``entity_type='person'`` at the anchor-registry layer."""
    src = inspect.getsource(fec.ingest_individual_contributors_from_registry)
    assert "anchors_for_fec_individual" in src, (
        "dispatch must use the person-only anchor filter"
    )
