"""Batch-1 contribution-finalize (helen 2026-08-12): SPLIT-CANONICAL
merge helper.

Two shapes of split we need to close for Cassandra's money-flow pool:

  1. PARTY-COMMITTEE SPLIT — NRSC / NRCC / DSCC / DCCC each have TWO
     canonicals for the same real committee: one via
     ``source_system='fec.committee'`` (created by upsert_anchor →
     ingest_pac path; carries the ``party`` alias) and one via
     ``source_system='fec.disbursement.recipient'`` (created when
     corporate PACs — GEO / Chevron / etc. — cited a disbursement
     to that committee). Corporate PAC → recipient edges land on the
     disbursement-recipient canonical; ``_party_recipient_ids``
     matches the fec.committee canonical. flow_model1 drops the
     giving because the two canonicals don't join. Merging them
     collapses NRSC's $1.2M GEO contribs (+ everything similar) into
     the party-classified attribution — the single biggest lift
     helen's contribution-fix pass needs.

  2. CORPORATE-ORG SPLIT — Boeing / Bechtel / Centene each have a
     CONTRACT-side canonical (created by usaspending ingest) and a
     CONTRIB-side canonical (created as ``fec.affiliated_committee``
     during fec ingest — that's the PAC's parent-org shadow). The
     PAC's ``affiliated_with`` edge lands on the CONTRIB-side
     canonical; contract-side canonical has the ``holds_contract``
     edges. flow_model1 attributes contribs to the CONTRIB canonical
     but reads contracts from the CONTRACT canonical — they never
     join, so the both-sides gate rejects the row. Merging joins
     them and unlocks the pool.

MERGE IS FK-REMAP + AGGREGATE:
  * ``canonical_edges``: re-point ``source_id`` + ``target_id`` from
    the drop canonical to the keep canonical. If the resulting edge
    would collide with an existing edge on the keep canonical
    (same source, target, relation — the ``ix_edges_unique``
    invariant), SUM the weights + re-parent the drop edge's
    ``source_citations`` to the surviving edge + delete the drop
    edge.  Never lose a citation — the graph's promise is every $
    is cited.
  * ``entity_aliases``: re-point ``canonical_id`` from drop → keep.
    If an alias with the same ``(source_system, source_id)`` already
    lives on keep, drop the duplicate row (the keep alias survives;
    the drop alias would have violated ``ix_aliases_source`` on the
    naive UPDATE otherwise).
  * ``anchor_registry``: re-point ``canonical_id`` FK.
  * ``scrutiny_decisions`` + ``alias_crosswalk`` FROM/TO id
    references: repoint.
  * ``source_citations``: NOT touched directly — they follow their
    edges (either re-parented above or attached to a surviving edge).
  * DELETE the drop canonical last.

FAIL-CLOSED on surface_mode escalation. Refuses a merge that would
relax privacy protection on the survivor (spec §3, mirrors the
existing merge_pass._apply_one logic).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import (
    AliasCrosswalk,
    AnchorRegistry,
    CanonicalEdge,
    CanonicalEntity,
    EntityAlias,
    SourceCitation,
)

logger = logging.getLogger(__name__)


_SURFACE_MODE_STRICTNESS = {"open": 0, "alias": 1, "suppress": 2}


@dataclass
class MergeStats:
    """Per-merge counters for post-run audit."""

    edges_repointed: int = 0
    edges_collided_summed: int = 0
    citations_reparented: int = 0
    aliases_repointed: int = 0
    aliases_dropped_duplicate: int = 0
    anchors_repointed: int = 0
    survivor_id: str = ""
    dropped_id: str = ""
    refused: bool = False
    refused_reason: str = ""


async def merge_two_canonicals(
    session: AsyncSession,
    keep_id: str,
    drop_id: str,
) -> MergeStats:
    """Merge ``drop_id`` INTO ``keep_id`` — FK-remap all references,
    aggregate colliding edges by summing weights + re-parenting
    citations, dedupe alias unique-index violations, then delete
    the drop canonical. Idempotent-ish: a re-run against
    already-merged pair (drop_id missing) returns refused=True.

    Returns per-merge :class:`MergeStats`. Caller commits.
    """
    stats = MergeStats(survivor_id=keep_id, dropped_id=drop_id)

    if keep_id == drop_id:
        stats.refused = True
        stats.refused_reason = "keep_id == drop_id"
        return stats

    keep = (
        await session.execute(select(CanonicalEntity).where(CanonicalEntity.id == keep_id))
    ).scalar_one_or_none()
    drop = (
        await session.execute(select(CanonicalEntity).where(CanonicalEntity.id == drop_id))
    ).scalar_one_or_none()

    if keep is None or drop is None:
        stats.refused = True
        stats.refused_reason = f"keep or drop missing (keep={keep is not None}, drop={drop is not None})"
        logger.warning("merge_two_canonicals REFUSED: %s", stats.refused_reason)
        return stats

    # Privacy guardrail — the surviving canonical must inherit the
    # MOST-protected surface_mode. Refuse a merge where the drop
    # canonical is MORE protected than keep (would surface a
    # suppressed identity via keep's aliases).
    keep_prot = _SURFACE_MODE_STRICTNESS.get(keep.surface_mode, 0)
    drop_prot = _SURFACE_MODE_STRICTNESS.get(drop.surface_mode, 0)
    if drop_prot > keep_prot:
        stats.refused = True
        stats.refused_reason = (
            f"privacy: drop.surface_mode={drop.surface_mode!r} "
            f"more protected than keep={keep.surface_mode!r}"
        )
        logger.error("merge_two_canonicals REFUSED: %s", stats.refused_reason)
        return stats

    # ── 1. OUTBOUND edges from drop — re-point or aggregate ─────────
    drop_out = (
        await session.execute(
            select(CanonicalEdge).where(CanonicalEdge.source_id == drop_id)
        )
    ).scalars().all()
    for edge in drop_out:
        # Would this edge collide with an existing edge on keep?
        collision = (
            await session.execute(
                select(CanonicalEdge).where(
                    CanonicalEdge.source_id == keep_id,
                    CanonicalEdge.target_id == edge.target_id,
                    CanonicalEdge.relation == edge.relation,
                )
            )
        ).scalar_one_or_none()
        if collision is None:
            edge.source_id = keep_id
            stats.edges_repointed += 1
        else:
            # Sum weights, move citations, delete the drop edge.
            collision.weight = (collision.weight or 0.0) + (edge.weight or 0.0)
            moved = (
                await session.execute(
                    update(SourceCitation)
                    .where(SourceCitation.edge_id == edge.id)
                    .values(edge_id=collision.id)
                )
            ).rowcount or 0
            stats.citations_reparented += moved
            await session.delete(edge)
            stats.edges_collided_summed += 1
    await session.flush()

    # ── 2. INBOUND edges to drop — re-point or aggregate ────────────
    drop_in = (
        await session.execute(
            select(CanonicalEdge).where(CanonicalEdge.target_id == drop_id)
        )
    ).scalars().all()
    for edge in drop_in:
        collision = (
            await session.execute(
                select(CanonicalEdge).where(
                    CanonicalEdge.source_id == edge.source_id,
                    CanonicalEdge.target_id == keep_id,
                    CanonicalEdge.relation == edge.relation,
                )
            )
        ).scalar_one_or_none()
        if collision is None:
            edge.target_id = keep_id
            stats.edges_repointed += 1
        else:
            collision.weight = (collision.weight or 0.0) + (edge.weight or 0.0)
            moved = (
                await session.execute(
                    update(SourceCitation)
                    .where(SourceCitation.edge_id == edge.id)
                    .values(edge_id=collision.id)
                )
            ).rowcount or 0
            stats.citations_reparented += moved
            await session.delete(edge)
            stats.edges_collided_summed += 1
    await session.flush()

    # ── 3. ALIASES — repoint, drop unique collisions ────────────────
    drop_aliases = (
        await session.execute(
            select(EntityAlias).where(EntityAlias.canonical_id == drop_id)
        )
    ).scalars().all()
    for alias in drop_aliases:
        clash = (
            await session.execute(
                select(EntityAlias).where(
                    EntityAlias.canonical_id == keep_id,
                    EntityAlias.source_system == alias.source_system,
                    EntityAlias.source_id == alias.source_id,
                )
            )
        ).scalar_one_or_none()
        if clash is None:
            alias.canonical_id = keep_id
            stats.aliases_repointed += 1
        else:
            await session.delete(alias)
            stats.aliases_dropped_duplicate += 1
    await session.flush()

    # ── 4. anchor_registry FK ───────────────────────────────────────
    anchors_updated = (
        await session.execute(
            update(AnchorRegistry)
            .where(AnchorRegistry.canonical_id == drop_id)
            .values(canonical_id=keep_id)
        )
    ).rowcount or 0
    stats.anchors_repointed += anchors_updated

    # ── 5. alias_crosswalk (freeze audit + repoint live) ────────────
    _ = (
        await session.execute(
            update(AliasCrosswalk)
            .where(AliasCrosswalk.from_id == drop_id)
            .values(from_id=keep_id)
        )
    )
    _ = (
        await session.execute(
            update(AliasCrosswalk)
            .where(AliasCrosswalk.to_id == drop_id)
            .values(to_id=keep_id)
        )
    )
    # scrutiny_decisions: not always present in every schema — do a
    # best-effort raw UPDATE that no-ops if the table isn't there.
    try:
        from app.models import ScrutinyDecision
        _ = (
            await session.execute(
                update(ScrutinyDecision)
                .where(ScrutinyDecision.canonical_id == drop_id)
                .values(canonical_id=keep_id)
            )
        )
    except ImportError:
        pass

    # ── 6. Delete drop canonical (edges/citations already re-parented) ─
    await session.delete(drop)
    await session.flush()

    logger.info(
        "merge_two_canonicals: keep=%s drop=%s edges_repointed=%d "
        "collided_summed=%d citations_reparented=%d "
        "aliases_repointed=%d aliases_dropped=%d anchors=%d",
        keep_id, drop_id,
        stats.edges_repointed, stats.edges_collided_summed,
        stats.citations_reparented, stats.aliases_repointed,
        stats.aliases_dropped_duplicate, stats.anchors_repointed,
    )
    return stats


async def find_party_committee_splits(
    session: AsyncSession,
) -> list[tuple[str, str, str]]:
    """Return (label, keep_canonical, drop_canonical) for every party
    committee where the ``fec.committee`` canonical differs from the
    ``fec.disbursement.recipient`` canonical.

    ``keep`` is the ``fec.committee`` canonical (has the party alias
    via anchor_registry seeding); ``drop`` is the
    ``fec.disbursement.recipient`` canonical (holds the inbound
    corporate-PAC edges that need to attribute to keep).
    """
    party_anchors = (
        await session.execute(
            select(
                AnchorRegistry.label,
                AnchorRegistry.fec_committee_ids,
            ).where(AnchorRegistry.priority_domain == "party_committees")
        )
    ).all()

    out: list[tuple[str, str, str]] = []
    for label, cids in party_anchors:
        for cid in cids or ():
            keep = (
                await session.execute(
                    select(EntityAlias.canonical_id).where(
                        EntityAlias.source_system == "fec.committee",
                        EntityAlias.source_id == cid,
                    )
                )
            ).scalar_one_or_none()
            drop = (
                await session.execute(
                    select(EntityAlias.canonical_id).where(
                        EntityAlias.source_system == "fec.disbursement.recipient",
                        EntityAlias.source_id == cid,
                    )
                )
            ).scalar_one_or_none()
            if keep and drop and keep != drop:
                out.append((label, keep, drop))
    return out


async def merge_party_committee_splits() -> list[MergeStats]:
    """Convenience wrapper: find every party-committee split + merge
    them (drop-canonical into keep-canonical). Commits.
    """
    sm = get_sessionmaker()
    async with sm() as session:
        splits = await find_party_committee_splits(session)
    out: list[MergeStats] = []
    for label, keep, drop in splits:
        logger.info("party-committee merge: %s keep=%s drop=%s", label, keep, drop)
        async with sm() as session:
            stats = await merge_two_canonicals(session, keep, drop)
            await session.commit()
        out.append(stats)
    return out


async def merge_corporate_split(
    keep_canonical: str,
    drop_canonical: str,
) -> MergeStats:
    """Merge a corporate CONTRIB-side canonical (usually the
    ``fec.affiliated_committee`` upsert) into the CONTRACT-side
    canonical (usually the usaspending-anchored org).  This joins
    ``holds_contract`` edges + ``affiliated_with`` edges on one
    canonical so flow_model1's both-sides gate has data to compare.
    """
    sm = get_sessionmaker()
    async with sm() as session:
        stats = await merge_two_canonicals(session, keep_canonical, drop_canonical)
        await session.commit()
    return stats


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s",
    )
    stats = asyncio.run(merge_party_committee_splits())
    for s in stats:
        logger.info(
            "party-committee merge: keep=%s drop=%s edges=%d citations=%d",
            s.survivor_id, s.dropped_id,
            s.edges_repointed + s.edges_collided_summed,
            s.citations_reparented,
        )


if __name__ == "__main__":
    main()
