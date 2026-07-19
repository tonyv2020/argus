"""P3 — PAC → sponsoring-org affiliated_with edge shape tests.

Hermetic — inspects the fec.py code path that reads
``affiliated_committee_name`` from the FEC committee record + emits
an ``affiliated_with`` edge. Live edge counts are validated in the
follow-on sweep run.
"""

from __future__ import annotations

import inspect

from app.services.ingest import fec


def test_fec_stats_carries_affiliation_counters() -> None:
    """FecStats must expose the new affiliation counters so callers +
    logs see PAC→org edge deltas separately from contributes_to."""
    stats = fec.FecStats()
    assert hasattr(stats, "affiliation_edges_created")
    assert hasattr(stats, "affiliation_edges_reused")
    assert stats.affiliation_edges_created == 0
    assert stats.affiliation_edges_reused == 0


def test_emit_affiliation_edge_helper_exists() -> None:
    """P3 helper — emits the affiliated_with edge + FEC-committee-record
    citation. Reused edges don't increment weight (affiliation is a
    boolean fact, not summable dollars)."""
    assert hasattr(fec, "_emit_affiliation_edge")
    src = inspect.getsource(fec._emit_affiliation_edge)
    assert "AFFILIATED_WITH" in src
    # Weight is set once + not summed on reuse.
    assert "edge.weight = float(" not in src, (
        "affiliated_with is a boolean fact — reuse must NOT sum weights"
    )


def test_ingest_pac_reads_affiliated_committee_name() -> None:
    """The PAC-mode ingest must read ``affiliated_committee_name`` from
    the committee record + emit the P3 edge when it's non-NULL/NONE."""
    src = inspect.getsource(fec.ingest_pac)
    assert 'affiliated_committee_name' in src
    assert '_emit_affiliation_edge' in src
    # "NONE" is the explicit sentinel FEC uses for super-PACs +
    # unaffiliated committees; MUST be skipped or we create a bogus
    # org canonical named "NONE".
    assert '"NONE"' in src or "'NONE'" in src


def test_affiliation_edge_reuses_existing_edge() -> None:
    """A reused edge is a re-run of the same PAC ingest — must not
    create a duplicate; must return reused=True so the counter fires
    on the reused side."""
    src = inspect.getsource(fec._emit_affiliation_edge)
    assert "existing is not None" in src or "reused = existing" in src


def test_ingest_from_registry_aggregates_affiliation_counters() -> None:
    """The multi-committee-id aggregation loop must sum the affiliation
    counters across all committees of an anchor — else the per-anchor
    stats miss a whole PR's worth of new edges."""
    src = inspect.getsource(fec.ingest_from_registry)
    assert "affiliation_edges_created" in src
    assert "affiliation_edges_reused" in src
