"""Scrutiny agent invariants — public/private/unknown enums + alias stability."""

from __future__ import annotations

from app.models import SurfaceMode
from app.services.scrutiny import (
    ScrutinyClass,
    ScrutinyDecision,
    compute_public_alias,
)


def test_public_alias_is_deterministic_per_canonical() -> None:
    """Same canonical_id must always produce the same public_alias — stable across runs."""
    cid = "b393bc4c-bacc-4556-9589-b6446854608e"
    a = compute_public_alias(cid)
    b = compute_public_alias(cid)
    assert a == b
    assert a.startswith("Private donor #")


def test_public_alias_distinct_per_canonical() -> None:
    """Two different canonicals must NOT collapse into the same alias — Tony's requirement."""
    a = compute_public_alias("11111111-1111-1111-1111-111111111111")
    b = compute_public_alias("22222222-2222-2222-2222-222222222222")
    assert a != b


def test_scrutiny_classes_cover_the_design_taxonomy() -> None:
    """Public / private / unknown — the tiered bar's inputs."""
    values = {c.value for c in ScrutinyClass}
    assert values == {"public", "private", "unknown"}


def test_scrutiny_decisions_cover_the_design_taxonomy() -> None:
    """Surface / aggregate / suppress — the tiered bar's outputs."""
    values = {d.value for d in ScrutinyDecision}
    assert values == {"surface", "aggregate", "suppress"}


def test_surface_mode_covers_open_alias_suppress() -> None:
    """The public API selects between open / alias / suppress on every render."""
    values = {m.value for m in SurfaceMode}
    assert values == {"open", "alias", "suppress"}


# ─── P1.5 — public-official hard signals ────────────────────────────────


def test_bioguide_is_a_public_official_hard_signal() -> None:
    """A bioguide id is a sitting member of Congress's entry in the
    Biographical Directory — the definitive public-official signal.

    Without it, a member whose FEC candidate id is absent from the
    dataset falls through to the LLM path and, with no Anthropic key
    configured, fail-closes to SUPPRESS: a public official hidden by
    accident, and their contributions unattributable.
    """
    from app.services.scrutiny import _PUBLIC_SOURCE_SYSTEMS

    assert "bioguide" in _PUBLIC_SOURCE_SYSTEMS
    assert "fec.candidate" in _PUBLIC_SOURCE_SYSTEMS


def test_scrutiny_batch_mode_is_scoped_to_the_batch() -> None:
    """RG4 publish precondition: the batch runner decides only entities
    carrying the batch_id AND lacking a verdict. A pre-existing
    canonical — including a member already sitting on a suppress node —
    is outside the batch and must never be re-decided by it."""
    import inspect

    from app.services.ingest.run_scrutiny import run_scrutiny_batch

    src = inspect.getsource(run_scrutiny_batch)
    assert "ce.batch_id = :b" in src
    assert "sd.id IS NULL" in src
