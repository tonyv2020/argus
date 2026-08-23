"""The FEC additive-weight defect, pinned so it cannot come back.

``_emit_contribution_edge`` used to add the transaction's dollars to
``canonical_edges.weight`` and append a ``SourceCitation`` on EVERY run, with no
check that the ``sub_id`` had already been counted. A weekly CronJob therefore
inflated live published figures without bound.

Measured on live data before the fix:

    55 source entities · 3,170 edges · $2,477,161,067 overstated

24 of those sources are named real people, 22 of them sitting members of
Congress, all ``surface_mode='open'`` and ``publication_state='published'`` —
i.e. on the public read path. Elon Musk's edges read 7.00x: America PAC showed
$1,748,284,905 against a true ~$249,754,986.

The multiplier was readable straight off the citations, which is the tell that
weight and citations moved in lockstep:

    count(citations) / count(distinct citation_ref)  ==  the inflation factor

Two tests here, and they check different things on purpose:

* the SOURCE test runs everywhere and fails the build if the guard is deleted;
* the FUNCTIONAL test needs a real Postgres and proves the guard actually holds
  against the database, exercising the real existence-check SQL rather than a
  mock of it. A mock would have happily passed with the bug present.
"""

from __future__ import annotations

import inspect
import os
import uuid

import pytest


# ---------------------------------------------------------------------------
# 1. source-level: cheap, runs everywhere, catches a deletion
# ---------------------------------------------------------------------------
def test_the_guard_is_present_and_precedes_the_weight_add():
    """The ordering is the invariant, not just the presence of a check.

    ``edge.weight`` must move only AFTER the already-cited early return, so
    "weight equals the sum of the amounts this edge cites" holds by
    construction. A guard placed after the weight add would still duplicate the
    money while looking correct at a glance.
    """
    from app.services.ingest import fec

    src = inspect.getsource(fec._emit_contribution_edge)

    assert "citation_ref == sub_id" in src, (
        "the sub_id existence guard is gone; every re-run will re-add the same "
        "transaction's dollars (this cost $2.48B across 24 named people)"
    )
    assert "already_cited" in src

    guard_at = src.index("if already_cited:")
    weight_at = src.rindex("edge.weight = float(")
    assert guard_at < weight_at, (
        "edge.weight is incremented BEFORE the already-cited early return, so a "
        "repeat sub_id still double-counts the money"
    )

    # The CITATION lookup specifically must be an existence check. The EDGE
    # lookup above it legitimately uses scalar_one_or_none, because edge
    # uniqueness really is expected -- so scope the assertion to the guard's own
    # expression rather than to a window of nearby characters.
    guard_expr = src[src.index("already_cited = ("): src.index("if already_cited:")]
    assert ".first()" in guard_expr
    assert "scalar_one_or_none" not in guard_expr, (
        "the citation guard asserts uniqueness. There is no unique index on "
        "(edge_id, citation_ref) and live edges carry the same sub_id up to 7 "
        "times, so this raises MultipleResultsFound on exactly the rows the fix "
        "exists to stop growing."
    )


# ---------------------------------------------------------------------------
# 2. functional: needs a real database, proves the behaviour
# ---------------------------------------------------------------------------
pytestmark_db = pytest.mark.skipif(
    not os.environ.get("ARGUS_TEST_DATABASE_URL"),
    reason="set ARGUS_TEST_DATABASE_URL to a throwaway Postgres to run the "
    "functional idempotency proof",
)


@pytestmark_db
async def test_re_emitting_the_same_sub_id_changes_nothing():
    """Emit the SAME transaction three times. The dollars must not move.

    This is the whole defect in four lines. Before the fix the second call
    doubled ``weight`` and the third tripled it, while ``source_citations``
    grew to three rows carrying one distinct ``citation_ref``.
    """
    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.models import CanonicalEdge, CanonicalEntity, SourceCitation
    from app.services.ingest.fec import _emit_contribution_edge

    engine = create_async_engine(os.environ["ARGUS_TEST_DATABASE_URL"])
    maker = async_sessionmaker(engine, expire_on_commit=False)

    tag = uuid.uuid4().hex[:8]
    async with maker() as session:
        src = CanonicalEntity(
            canonical_name=f"TEST DONOR {tag}",
            canonical_name_normalized=f"test donor {tag}",
            type="person",
        )
        dst = CanonicalEntity(
            canonical_name=f"TEST PAC {tag}",
            canonical_name_normalized=f"test pac {tag}",
            type="pac",
        )
        session.add_all([src, dst])
        await session.flush()

        AMOUNT = 250_000.00
        SUB_ID = f"TESTSUB{tag}"

        for _ in range(3):
            await _emit_contribution_edge(
                session, src.id, dst.id, AMOUNT, SUB_ID, f"C{tag}"
            )
            await session.flush()

        edge = (
            await session.execute(
                select(CanonicalEdge).where(CanonicalEdge.source_id == src.id)
            )
        ).scalar_one()

        cites = (
            await session.execute(
                select(
                    func.count(SourceCitation.id),
                    func.count(func.distinct(SourceCitation.citation_ref)),
                ).where(SourceCitation.edge_id == edge.id)
            )
        ).one()

        assert edge.weight == pytest.approx(AMOUNT), (
            f"weight is {edge.weight}, expected {AMOUNT}. Three emissions of one "
            f"transaction moved the money {edge.weight / AMOUNT:.2f}x."
        )
        assert cites == (1, 1), f"expected exactly one citation, got {cites}"

        # The live diagnostic that measured the damage, asserted green here:
        # count(citations) / count(distinct citation_ref) is the inflation factor.
        assert cites[0] / cites[1] == 1.0

        await session.rollback()
    await engine.dispose()


@pytestmark_db
async def test_a_genuinely_new_transaction_still_adds_its_dollars():
    """The guard must not be a mute button.

    A fix that stopped double-counting by refusing to count at all would pass
    the test above and be far worse than the bug.
    """
    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from app.models import CanonicalEdge, CanonicalEntity, SourceCitation
    from app.services.ingest.fec import _emit_contribution_edge

    engine = create_async_engine(os.environ["ARGUS_TEST_DATABASE_URL"])
    maker = async_sessionmaker(engine, expire_on_commit=False)

    tag = uuid.uuid4().hex[:8]
    async with maker() as session:
        src = CanonicalEntity(
            canonical_name=f"TEST DONOR2 {tag}",
            canonical_name_normalized=f"test donor2 {tag}",
            type="person",
        )
        dst = CanonicalEntity(
            canonical_name=f"TEST PAC2 {tag}",
            canonical_name_normalized=f"test pac2 {tag}",
            type="pac",
        )
        session.add_all([src, dst])
        await session.flush()

        for i, amount in enumerate((100.0, 250.0, 25.5)):
            await _emit_contribution_edge(
                session, src.id, dst.id, amount, f"SUB{tag}-{i}", f"C{tag}"
            )
            await session.flush()

        edge = (
            await session.execute(
                select(CanonicalEdge).where(CanonicalEdge.source_id == src.id)
            )
        ).scalar_one()
        n = (
            await session.execute(
                select(func.count(SourceCitation.id)).where(
                    SourceCitation.edge_id == edge.id
                )
            )
        ).scalar_one()

        assert edge.weight == pytest.approx(375.5)
        assert n == 3

        await session.rollback()
    await engine.dispose()
