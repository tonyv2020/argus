"""Cassandra Stage 2 (helen 2026-08-11) — usaspending obligation +
idempotent-edge + backfill wiring.

Two stacked bugs in the pre-Stage-2 code drove the $89B GEO / $58B
CoreCivic edge weights that broke Cassandra's outline:

  BUG 1 — used "Award Amount" (contract CEILING including all option
          years) instead of net obligations.
  BUG 2 — ``edge.weight = edge.weight + amount`` accumulated across
          re-ingests, so every scheduled sweep re-added the same
          awards and weights ballooned.

These tests pin the shape of the fix without spinning up Postgres:

  * ``_award_net_obligation`` picks the row's obligation field(s)
    (Total Obligated Amount / total_obligation / obligated_amount)
    and falls back to the award-detail endpoint when the row
    omitted them.
  * ``_emit_contract_edge`` is idempotent per (edge, award_id) —
    a second call with the same award adds neither weight nor a
    duplicate citation.
  * The ``spending_by_award`` request body includes the obligation
    field and sorts by it.
  * The Stage-2 backfill entry point + CLI subcommand exist.

The existing DB-integration paths are covered by helen's live
validation against the corrected GEO/CoreCivic/Palantir numbers on
the staged Argus image + a real re-ingest against USAspending.
"""

from __future__ import annotations

import inspect
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from app.services.ingest import usaspending


# --- _award_net_obligation ------------------------------------------------


@pytest.mark.asyncio
async def test_award_net_obligation_prefers_row_total_obligated_amount():
    """When ``spending_by_award`` returned the obligation field in the
    row, we use it directly — no per-award detail fetch needed."""
    client = AsyncMock()
    row = {
        "generated_internal_id": "CONT_AWD_HSCEDM12D00003",
        "Award Amount": 1_000_000_000.0,        # ceiling — the bug's fuel
        "Total Obligated Amount": 200_000_000.0,
    }
    got = await usaspending._award_net_obligation(client, row)
    assert got == 200_000_000.0
    assert client.get.call_count == 0  # never fetched the award detail


@pytest.mark.asyncio
async def test_award_net_obligation_accepts_alternate_keys():
    """Alternate spellings from older API responses / different endpoints
    also work — ``total_obligation`` + ``obligated_amount``."""
    for key in ("total_obligation", "obligated_amount"):
        client = AsyncMock()
        row = {"generated_internal_id": "X", "Award Amount": 999, key: 42.0}
        got = await usaspending._award_net_obligation(client, row)
        assert got == 42.0, f"failed on key {key}"


@pytest.mark.asyncio
async def test_award_net_obligation_falls_back_to_detail_endpoint(monkeypatch):
    """When the row omits every obligation field, the fetch goes to
    ``/awards/<generated_unique_award_id>/`` and pulls
    ``total_obligation`` from there — that's the value shown on
    the public usaspending.gov award page."""
    row = {
        "generated_internal_id": "CONT_AWD_ABC123",
        "Award Amount": 999_999_999.0,
    }
    fake_detail = {"total_obligation": 42_500_000.0, "some_other": "x"}

    async def _fake_get(client, path):
        assert path == "/awards/CONT_AWD_ABC123/"
        return fake_detail

    monkeypatch.setattr(usaspending, "_get", _fake_get)
    got = await usaspending._award_net_obligation(AsyncMock(), row)
    assert got == 42_500_000.0


@pytest.mark.asyncio
async def test_award_net_obligation_returns_none_on_missing_award_id():
    """No award id + no row-level obligation = None; caller then
    emits a $0 edge (still counted for structural presence)."""
    client = AsyncMock()
    got = await usaspending._award_net_obligation(client, {"Award Amount": 1})
    assert got is None


@pytest.mark.asyncio
async def test_award_net_obligation_returns_none_on_detail_fetch_failure(monkeypatch):
    """Fallback fetch that raises returns None — the ingest logs it and
    moves on rather than crashing the whole sweep."""
    async def _boom(client, path):
        raise RuntimeError("upstream down")

    monkeypatch.setattr(usaspending, "_get", _boom)
    got = await usaspending._award_net_obligation(
        AsyncMock(), {"generated_internal_id": "X"}
    )
    assert got is None


# --- _emit_contract_edge idempotency --------------------------------------
#
# We use a hand-rolled fake AsyncSession that:
#   - lets .execute() return a stub whose .scalar_one_or_none() returns a
#     value we control (edge-existing or citation-existing);
#   - records .add() calls;
#   - awaits .flush() as a no-op.
# The point is to verify the WEIGHT-UPDATE and CITATION-INSERT decisions
# under different (edge-exists, citation-exists) combinations, without
# depending on real Postgres.


class _FakeExecute:
    """Minimal .scalar_one_or_none() shim for async session .execute()."""

    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value


class _FakeAsyncSession:
    def __init__(self, *, existing_edge=None, existing_citation=None):
        self._results = [
            _FakeExecute(existing_edge),
            _FakeExecute(existing_citation),
        ]
        self.added = []
        self.flushed = 0

    async def execute(self, _stmt):
        return self._results.pop(0)

    def add(self, obj):
        self.added.append(obj)

    async def flush(self):
        self.flushed += 1


@pytest.mark.asyncio
async def test_emit_contract_edge_creates_new_edge_with_amount():
    """First sighting of a (src, dst) pair — new edge, weight=amount,
    citation added, reused=False."""
    from app.models import CanonicalEdge

    session = _FakeAsyncSession(existing_edge=None, existing_citation=None)
    edge_id, reused = await usaspending._emit_contract_edge(
        session, src_canonical="geo-id", dst_canonical="ice-id",
        amount=2_400_000_000.0, award_id="AWD-1",
    )
    assert reused is False
    added_edges = [o for o in session.added if isinstance(o, CanonicalEdge)]
    assert len(added_edges) == 1
    assert added_edges[0].weight == 2_400_000_000.0
    # One citation added.
    from app.models import SourceCitation
    added_citations = [o for o in session.added if isinstance(o, SourceCitation)]
    assert len(added_citations) == 1
    assert added_citations[0].citation_ref == "AWD-1"


@pytest.mark.asyncio
async def test_emit_contract_edge_existing_edge_new_award_adds_amount():
    """Edge exists but THIS award hasn't been counted yet — weight is
    updated by +amount and a new citation is inserted."""
    from app.models import CanonicalEdge

    existing = SimpleNamespace(id="edge-1", weight=100.0)
    session = _FakeAsyncSession(existing_edge=existing, existing_citation=None)
    _, reused = await usaspending._emit_contract_edge(
        session, src_canonical="geo-id", dst_canonical="ice-id",
        amount=250.0, award_id="AWD-2",
    )
    assert reused is True
    assert existing.weight == 350.0
    from app.models import SourceCitation
    added_citations = [o for o in session.added if isinstance(o, SourceCitation)]
    assert len(added_citations) == 1
    assert added_citations[0].citation_ref == "AWD-2"
    # And NO extra edge was created (only the existing one).
    added_edges = [o for o in session.added if isinstance(o, CanonicalEdge)]
    assert added_edges == []


@pytest.mark.asyncio
async def test_emit_contract_edge_second_ingest_of_same_award_is_noop():
    """The critical Stage-2 fix. Edge exists AND this award's
    citation already exists — weight MUST NOT be incremented and a
    duplicate citation MUST NOT be added. Re-ingesting the same
    scheduled sweep converges instead of ballooning."""
    from app.models import CanonicalEdge, SourceCitation

    existing_edge = SimpleNamespace(id="edge-1", weight=2_400_000_000.0)
    existing_citation = SimpleNamespace(
        id="cit-1", edge_id="edge-1", citation_ref="AWD-3"
    )
    session = _FakeAsyncSession(
        existing_edge=existing_edge, existing_citation=existing_citation,
    )
    _, reused = await usaspending._emit_contract_edge(
        session, src_canonical="geo-id", dst_canonical="ice-id",
        amount=2_400_000_000.0, award_id="AWD-3",
    )
    assert reused is True
    # WEIGHT UNCHANGED — the accumulate bug is fixed.
    assert existing_edge.weight == 2_400_000_000.0, (
        "second ingest of the same award MUST NOT increment the weight — "
        f"got {existing_edge.weight}"
    )
    # NO NEW citation.
    added_citations = [o for o in session.added if isinstance(o, SourceCitation)]
    assert added_citations == []
    added_edges = [o for o in session.added if isinstance(o, CanonicalEdge)]
    assert added_edges == []


@pytest.mark.asyncio
async def test_emit_contract_edge_multiple_awards_on_same_edge_sum():
    """Two DISTINCT awards to the same (src, dst) — weight = award1 +
    award2 across two separate _emit_contract_edge calls. This is the
    ONLY legitimate accumulation shape."""
    existing_edge = SimpleNamespace(id="edge-1", weight=100.0)

    # Award A: not yet counted.
    session_a = _FakeAsyncSession(
        existing_edge=existing_edge, existing_citation=None,
    )
    await usaspending._emit_contract_edge(
        session_a, src_canonical="geo-id", dst_canonical="ice-id",
        amount=250.0, award_id="AWD-A",
    )
    assert existing_edge.weight == 350.0

    # Award B (different id): also not counted.
    session_b = _FakeAsyncSession(
        existing_edge=existing_edge, existing_citation=None,
    )
    await usaspending._emit_contract_edge(
        session_b, src_canonical="geo-id", dst_canonical="ice-id",
        amount=1_000.0, award_id="AWD-B",
    )
    assert existing_edge.weight == 1_350.0


# --- ceiling-vs-obligation regression guards ------------------------------


def test_spending_by_award_request_includes_total_obligated_amount_field():
    """The fields list sent to spending_by_award MUST request the
    obligation field so the row-level path works — otherwise every
    ingest falls back to the N+1 award-detail endpoint."""
    src = inspect.getsource(usaspending.ingest_recipient_contracts)
    assert '"Total Obligated Amount"' in src


def test_spending_by_award_sort_is_obligation_not_ceiling():
    """Sort should be on the obligation, so top-of-list is top-actually-
    paid, not top-ceiling. Guards against a future rebase re-introducing
    the ceiling-sort."""
    src = inspect.getsource(usaspending.ingest_recipient_contracts)
    assert '"sort": "Total Obligated Amount"' in src


def test_emit_contract_edge_no_longer_uses_plus_equals_on_weight():
    """The pre-Stage-2 code did ``edge.weight = (edge.weight or 0) +
    (amount or 0)`` unconditionally on the reuse branch — regression
    guard against that literal shape returning to the tree."""
    src = inspect.getsource(usaspending._emit_contract_edge)
    # The idempotent version uses a citation-existence check first.
    assert "citation_exists" in src
    # And the accumulate path is guarded by "if citation_exists is None".
    assert "if citation_exists is None:" in src


# --- backfill wiring ------------------------------------------------------


def test_backfill_holds_contract_edges_exists_and_is_async():
    assert hasattr(usaspending, "backfill_holds_contract_edges")
    assert inspect.iscoroutinefunction(usaspending.backfill_holds_contract_edges)


def test_backfill_stats_dataclass_shape():
    stats = usaspending.BackfillStats()
    for attr in (
        "holds_contract_edges_before",
        "usaspending_citations_before",
        "holds_contract_edges_deleted",
        "usaspending_citations_deleted",
        "holds_contract_edges_after",
        "usaspending_citations_after",
    ):
        assert hasattr(stats, attr), attr
        assert getattr(stats, attr) == 0


def test_cli_dispatches_backfill_flag():
    """``--backfill-contracts`` must reach ``backfill_holds_contract_edges``
    via the main() dispatcher. Regression guard so the runbook helen
    executes lands on the right code path."""
    src = inspect.getsource(usaspending.main)
    assert "--backfill-contracts" in src
    assert "backfill_holds_contract_edges" in src
