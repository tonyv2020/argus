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
    """Minimal Result shim for async session .execute().

    Models both accessors the emitter uses: ``scalar_one_or_none`` for
    the edge lookup (unique on source/target/relation) and ``first`` for
    the citation EXISTENCE check — that one must not assert uniqueness,
    because there is no unique index on (edge_id, kind, citation_ref)
    and historical edges carry duplicates from the pre-Stage-2 emitter.
    """

    def __init__(self, value):
        self._value = value

    def scalar_one_or_none(self):
        return self._value

    def first(self):
        """A real Result yields a Row, or None when there are no rows."""
        return None if self._value is None else (self._value,)


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


def test_spending_by_award_sort_is_a_valid_contract_award_sort_key():
    """HOTFIX regression (helen 2026-08-11): a Stage 2 build shipped
    with ``sort="Total Obligated Amount"`` — NOT a valid Contract-
    Award sort mapping — so USAspending returned HTTP 400 on every
    recipient and the backfill wiped the graph before the re-ingest
    even started. This test asserts the current sort is in
    ``VALID_CONTRACT_SORT_KEYS`` so a future change re-introducing
    an invalid key fails locally, not in prod-after-wipe.
    """
    src = inspect.getsource(usaspending.ingest_recipient_contracts)
    # Extract the actual sort literal from the body dict.
    import re

    m = re.search(r'"sort":\s*"([^"]+)"', src)
    assert m is not None, "no sort key found in ingest_recipient_contracts body"
    sort_key = m.group(1)
    assert sort_key in usaspending.VALID_CONTRACT_SORT_KEYS, (
        f"sort={sort_key!r} is NOT in VALID_CONTRACT_SORT_KEYS "
        f"{sorted(usaspending.VALID_CONTRACT_SORT_KEYS)}. USAspending "
        f"would return 400 'Sort value not found in Contract Award "
        f"mappings' on every request. If USAspending adds a new sort "
        f"key, add it to VALID_CONTRACT_SORT_KEYS explicitly (do not "
        f"silence this test)."
    )


def test_valid_contract_sort_keys_excludes_the_pre_hotfix_wrong_value():
    """Documents WHY the ``Total Obligated Amount`` key isn't valid
    for the Contract-Award sort mapping — the exact failure mode
    helen caught 2026-08-11 (wiped every holds_contract edge)."""
    assert "Total Obligated Amount" not in usaspending.VALID_CONTRACT_SORT_KEYS


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


# --- HOTFIX (helen 2026-08-11): fail-before-delete safety gate ------------


def test_probe_exists_and_uses_valid_sort_key():
    """The probe MUST use the same sort key as the real ingest — else
    it validates the API surface against the wrong shape and lets a
    real-ingest failure through. Also asserts the probe uses a valid
    sort key (belt with the ingest-side assertion)."""
    assert hasattr(usaspending, "_probe_spending_by_award")
    assert inspect.iscoroutinefunction(usaspending._probe_spending_by_award)
    src = inspect.getsource(usaspending._probe_spending_by_award)
    import re

    m = re.search(r'"sort":\s*"([^"]+)"', src)
    assert m is not None, "probe request body missing a sort key"
    assert m.group(1) in usaspending.VALID_CONTRACT_SORT_KEYS


@pytest.mark.asyncio
async def test_probe_raises_on_http_error(monkeypatch):
    """When USAspending returns non-2xx, the probe raises. This is
    what makes the backfill's fail-before-delete gate work."""
    async def _fake_post(client, path, body):
        # Simulate USAspending's 400 for a bad sort key.
        request = httpx.Request("POST", "http://x")
        response = httpx.Response(400, request=request,
                                   text='{"detail":"Sort value not found in Contract Award mappings"}')
        raise httpx.HTTPStatusError(
            "Client error '400 Bad Request'", request=request, response=response,
        )

    import httpx  # local import so the fake_post closure captures it
    monkeypatch.setattr(usaspending, "_post", _fake_post)
    with pytest.raises(httpx.HTTPStatusError):
        await usaspending._probe_spending_by_award()


@pytest.mark.asyncio
async def test_backfill_aborts_before_delete_when_probe_fails(monkeypatch):
    """The critical hotfix behavior. If the probe raises, backfill
    MUST return/raise WITHOUT deleting a single edge or citation.
    The prior bug wiped the graph BEFORE the re-ingest failed;
    this test proves the new ordering closes that.

    We patch the probe to raise + patch get_sessionmaker so any
    accidental DB access shows up as a test failure (the session
    factory is called ONLY inside the section AFTER the probe).
    """
    async def _boom():
        raise RuntimeError("probe rejected — invalid sort")

    monkeypatch.setattr(usaspending, "_probe_spending_by_award", _boom)

    session_factory_calls: list[str] = []

    def _fake_sm():
        session_factory_calls.append("touched")
        raise AssertionError(
            "backfill touched get_sessionmaker AFTER a probe failure — "
            "the fail-before-delete gate is broken; deletes would run"
        )

    monkeypatch.setattr(usaspending, "get_sessionmaker", _fake_sm)

    with pytest.raises(RuntimeError, match="probe rejected"):
        await usaspending.backfill_holds_contract_edges()

    assert session_factory_calls == [], (
        "backfill must not open a session (and therefore must not run any "
        "DB DELETE) when the API probe fails"
    )


def test_backfill_docstring_documents_the_safety_gate():
    """The safety gate is only worth the paper it's printed on if
    operators know it exists. Regression against dropping the gate
    silently in a future refactor."""
    doc = usaspending.backfill_holds_contract_edges.__doc__ or ""
    assert "SAFETY" in doc.upper() or "probe" in doc.lower()
    assert "before" in doc.lower() and "delete" in doc.lower()


# --- Follow-up (helen 2026-08-11): per-anchor broaden_agency_scope --------
#
# The narrow ``_TARGET_AGENCIES`` whitelist (ICE / BOP / USMS) is the
# right scope for DETENTION-OPS anchors only. Non-detention anchors
# in the registry (Palantir, Tesla, SpaceX, xAI, defense-tech
# contractors) have zero awards in that whitelist and end up with
# ``agencies_matched=0`` — the scheduled sweep re-errors them every
# fire. Palantir was only corrected because helen ran that path by
# hand with broaden=True; the sweep now does it automatically.


@pytest.mark.asyncio
async def test_ingest_from_registry_narrow_scope_for_detention_anchor(monkeypatch):
    """A detention-industry anchor (e.g. 'GEO Group') must call
    ``ingest_recipient_contracts`` with ``broaden_agency_scope=False``
    — the narrow ICE/BOP/USMS whitelist is exactly what we want for
    the accountability beat."""
    calls: list[dict] = []

    async def _fake_anchors(session, priority_domains=None):
        return [
            SimpleNamespace(
                label="GEO Group",
                usaspending_recipient_names=["GEO GROUP INC"],
            ),
        ]

    async def _fake_ingest(**kwargs):
        calls.append(kwargs)
        return usaspending.UsaSpendingStats()

    monkeypatch.setattr(
        "app.services.anchor_registry.anchors_for_usaspending", _fake_anchors,
    )
    monkeypatch.setattr(usaspending, "ingest_recipient_contracts", _fake_ingest)

    await usaspending.ingest_from_registry()
    assert len(calls) == 1
    assert calls[0]["display_label"] == "GEO Group"
    assert calls[0]["broaden_agency_scope"] is False, (
        f"detention anchor 'GEO Group' MUST keep the narrow "
        f"whitelist; got broaden={calls[0]['broaden_agency_scope']}"
    )


@pytest.mark.asyncio
async def test_ingest_from_registry_broadens_for_non_detention_anchor(monkeypatch):
    """A non-detention anchor (e.g. 'Palantir', 'Tesla', 'SpaceX')
    must call with ``broaden_agency_scope=True`` — their real
    awarding agencies are NASA/DoD/State/etc., NOT the detention
    whitelist. This is the exact regression that made Palantir
    return agencies_matched=0 until helen ran it by hand."""
    calls: list[dict] = []

    async def _fake_anchors(session, priority_domains=None):
        return [
            SimpleNamespace(
                label="Palantir",
                usaspending_recipient_names=["PALANTIR TECHNOLOGIES INC"],
            ),
            SimpleNamespace(
                label="Tesla",
                usaspending_recipient_names=["TESLA INC"],
            ),
            SimpleNamespace(
                label="SpaceX",
                usaspending_recipient_names=["SPACE EXPLORATION TECHNOLOGIES CORP"],
            ),
            SimpleNamespace(
                label="xAI",
                usaspending_recipient_names=["X AI CORP"],
            ),
        ]

    async def _fake_ingest(**kwargs):
        calls.append(kwargs)
        return usaspending.UsaSpendingStats()

    monkeypatch.setattr(
        "app.services.anchor_registry.anchors_for_usaspending", _fake_anchors,
    )
    monkeypatch.setattr(usaspending, "ingest_recipient_contracts", _fake_ingest)

    await usaspending.ingest_from_registry()
    labels_broadened = {c["display_label"]: c["broaden_agency_scope"] for c in calls}
    assert labels_broadened == {
        "Palantir": True,
        "Tesla": True,
        "SpaceX": True,
        "xAI": True,
    }


@pytest.mark.asyncio
async def test_ingest_from_registry_mixed_anchor_set_gets_per_anchor_scope(monkeypatch):
    """A registry batch mixing detention + non-detention anchors
    gets the RIGHT scope per anchor — no one-size-fits-all
    decision."""
    calls: list[dict] = []

    async def _fake_anchors(session, priority_domains=None):
        return [
            SimpleNamespace(label="GEO Group",
                              usaspending_recipient_names=["GEO GROUP INC"]),
            SimpleNamespace(label="Palantir",
                              usaspending_recipient_names=["PALANTIR TECHNOLOGIES INC"]),
            SimpleNamespace(label="CoreCivic",
                              usaspending_recipient_names=["CORECIVIC INC"]),
            SimpleNamespace(label="Tesla",
                              usaspending_recipient_names=["TESLA INC"]),
            SimpleNamespace(label="Aventiv Technologies",
                              usaspending_recipient_names=["AVENTIV TECHNOLOGIES LLC"]),
        ]

    async def _fake_ingest(**kwargs):
        calls.append(kwargs)
        return usaspending.UsaSpendingStats()

    monkeypatch.setattr(
        "app.services.anchor_registry.anchors_for_usaspending", _fake_anchors,
    )
    monkeypatch.setattr(usaspending, "ingest_recipient_contracts", _fake_ingest)

    await usaspending.ingest_from_registry()
    by_label = {c["display_label"]: c["broaden_agency_scope"] for c in calls}
    assert by_label == {
        "GEO Group": False,             # detention → narrow
        "Palantir": True,               # non-detention → broaden
        "CoreCivic": False,             # detention → narrow
        "Tesla": True,                  # non-detention → broaden
        "Aventiv Technologies": False,  # detention (prison telecom) → narrow
    }


@pytest.mark.asyncio
async def test_caller_broaden_flag_still_force_broadens_even_detention(monkeypatch):
    """The per-anchor decision is an OR with the caller flag — if
    operators pass ``broaden_agency_scope=True`` explicitly, every
    anchor gets broadened including detention (matches the
    pre-hotfix caller-driven semantic; useful for one-off debug
    sweeps)."""
    calls: list[dict] = []

    async def _fake_anchors(session, priority_domains=None):
        return [
            SimpleNamespace(label="GEO Group",
                              usaspending_recipient_names=["GEO GROUP INC"]),
        ]

    async def _fake_ingest(**kwargs):
        calls.append(kwargs)
        return usaspending.UsaSpendingStats()

    monkeypatch.setattr(
        "app.services.anchor_registry.anchors_for_usaspending", _fake_anchors,
    )
    monkeypatch.setattr(usaspending, "ingest_recipient_contracts", _fake_ingest)

    await usaspending.ingest_from_registry(broaden_agency_scope=True)
    assert calls[0]["broaden_agency_scope"] is True
