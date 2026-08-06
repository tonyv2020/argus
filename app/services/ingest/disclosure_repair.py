"""D2.1 — repair D2's fragmented + junk target nodes.

The initial D2 emit stored each raw OGE Description verbatim as the
target canonical's name, producing 3387 new nodes vs 784 matched. Many
of those new nodes carry bond-descriptor cruft (``PAIRED CTF``, ``REG S
DUE ...``, coupon rates) or share-class suffixes (``COM``, ``F``,
``NEW``) that should have collapsed into the issuer's existing
canonical (Carnival, Apple, Microsoft, Netflix, …).

This module walks every D2-created target canonical, re-normalizes its
name with :func:`app.services.disclosure_issuer.normalize_issuer`, and
either:

  a. Merges it INTO a pre-existing canonical whose normalized name
     matches (the desired case).
  b. Merges it into another D2-created canonical that normalized to
     the same clean name (fragment-vs-fragment consolidation).
  c. Renames it in place to its clean issuer form when no better match
     exists (still a net win — ``CARNIVALCORPPAIREDCTF`` stops
     surfacing on /api/search).

Merges are performed INLINE in a single async session — the pattern
mirrors merge_pass but bypasses the per-row session churn that trips
psycopg's async pool_pre_ping under repair-scale load (thousands of
merges in one process). An ``alias_crosswalk`` row is written per
merge for audit; it lands with ``applied_at`` set so the sweep view
matches the on-disk truth immediately.

Contract:
* Deterministic; no LLM.
* Person nodes are NEVER merged.
* raw_text on disclosure_rows stays untouched.
* Fail-closed on surface_mode — survivor inherits the MOST-protected
  mode of any fragment merged into it (same rule as merge_pass §3).
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import (
    AliasCrosswalk,
    AnchorRegistry,
    CanonicalEdge,
    CanonicalEntity,
    EdgeRelation,
    EntityAlias,
    EntityType,
    SourceCitation,
    SurfaceMode,
)
from app.services.disclosure_issuer import normalize_issuer
from app.services.graph.base import normalize_name

logger = logging.getLogger(__name__)


# Every relation the D2 emit produces on the target side.
_D2_TARGET_RELATIONS: tuple[str, ...] = (
    EdgeRelation.HOLDS_ASSET.value,
    EdgeRelation.INCOME_FROM.value,
    EdgeRelation.HELD_POSITION.value,
    EdgeRelation.OWES.value,
    EdgeRelation.PARTY_TO_AGREEMENT.value,
)


_SURFACE_MODE_STRICTNESS = {
    SurfaceMode.OPEN.value: 0,
    SurfaceMode.ALIAS.value: 1,
    SurfaceMode.SUPPRESS.value: 2,
}


@dataclass
class RepairSummary:
    """D2.1 gate metrics — reported to helen."""

    d2_target_nodes_before: int = 0
    d2_target_nodes_after: int = 0
    fragments_scanned: int = 0
    merged_to_preexisting: int = 0
    merged_to_fresh_survivor: int = 0
    renamed_in_place: int = 0
    persons_skipped: int = 0
    unchanged: int = 0
    edges_repointed: int = 0
    citations_repointed: int = 0
    fragments_deleted: int = 0
    refused_privacy: int = 0
    per_survivor_top: list = field(default_factory=list)


# ─── Public entrypoint ──────────────────────────────────────────────


async def repair_d2_fragments(
    *,
    since: date | None = None,
    reason_stamp: str = "D2.1: issuer-normalized (helen 2026-08-05)",
) -> RepairSummary:
    """Consolidate D2 fragments; return the gate metrics.

    ``since`` — only consider canonicals created on or after this date.
    Default: 2026-08-04, which brackets the initial D2 emit run.
    """
    since = since or date(2026, 8, 4)
    summary = RepairSummary()
    sm = get_sessionmaker()

    async with sm() as db:
        summary.d2_target_nodes_before = await _count_d2_target_nodes(db)
        candidates = await _load_d2_created_targets(db, since=since)
        logger.info(
            "D2.1 repair: %d D2 target nodes total, %d candidates created since %s",
            summary.d2_target_nodes_before, len(candidates), since,
        )
        summary.fragments_scanned = len(candidates)

        # Pass 1 — normalize each candidate, bucket by (clean_norm, type).
        buckets: dict[tuple[str, str], list[CanonicalEntity]] = defaultdict(list)
        for ce in candidates:
            if ce.type == EntityType.PERSON.value:
                summary.persons_skipped += 1
                continue
            clean = normalize_issuer(ce.canonical_name) or ce.canonical_name
            clean_norm = normalize_name(clean)
            if not clean_norm:
                summary.unchanged += 1
                continue
            buckets[(clean_norm, ce.type)].append(ce)

        # Pass 2 — for each bucket, resolve to a survivor, merge others.
        top_survivors: list[tuple[str, int]] = []
        for (clean_norm, ent_type), members in buckets.items():
            pre = await _find_preexisting(db, clean_norm, ent_type, since)
            if pre is not None:
                survivor = pre
                is_preexisting = True
                members_to_merge = [m for m in members if m.id != pre.id]
            else:
                is_preexisting = False
                members_sorted = sorted(members, key=lambda c: (c.created_at, c.id))
                survivor = members_sorted[0]
                clean_name = normalize_issuer(survivor.canonical_name) or survivor.canonical_name
                if survivor.canonical_name != clean_name:
                    survivor.canonical_name = clean_name
                    survivor.canonical_name_normalized = clean_norm
                    summary.renamed_in_place += 1
                members_to_merge = members_sorted[1:]

            if members_to_merge:
                top_survivors.append((survivor.canonical_name, len(members_to_merge)))

            for src in members_to_merge:
                verdict = await _merge_inline(
                    db,
                    src=src,
                    dst=survivor,
                    reason=(
                        f"{reason_stamp}: → "
                        + ("preexisting" if is_preexisting else "fresh survivor")
                    ),
                    summary=summary,
                )
                if verdict == "merged":
                    if is_preexisting:
                        summary.merged_to_preexisting += 1
                    else:
                        summary.merged_to_fresh_survivor += 1

        await db.flush()
        await db.commit()

        summary.per_survivor_top = sorted(
            top_survivors, key=lambda t: -t[1]
        )[:20]

        summary.d2_target_nodes_after = await _count_d2_target_nodes(db)

    return summary


# ─── Merge implementation (inline) ───────────────────────────────


async def _merge_inline(
    db: AsyncSession,
    *,
    src: CanonicalEntity,
    dst: CanonicalEntity,
    reason: str,
    summary: RepairSummary,
) -> str:
    """Re-point every edge/alias/anchor from ``src`` to ``dst``, then
    delete ``src``. Same shape + fail-closed guard as merge_pass but
    stays in the caller's session.
    """
    src_strict = _SURFACE_MODE_STRICTNESS.get(src.surface_mode, 0)
    dst_strict = _SURFACE_MODE_STRICTNESS.get(dst.surface_mode, 0)

    # Fail-closed: refuse if src is MORE protected than dst — a merge
    # that would surface a suppressed identity via dst's aliases.
    if src_strict > dst_strict:
        logger.error(
            "D2.1 merge REFUSED (privacy): src=%s (%s) > dst=%s (%s)",
            src.id, src.surface_mode, dst.id, dst.surface_mode,
        )
        summary.refused_privacy += 1
        return "refused_privacy"

    # Escalate dst if survivor should be more protected (unlikely in D2
    # scope — org fragments to org survivor — but keep the rule).
    survivor_mode = src.surface_mode if src_strict > dst_strict else dst.surface_mode
    if survivor_mode != dst.surface_mode:
        dst.surface_mode = survivor_mode

    # Re-point edges. Note (source_id, target_id, relation) is UNIQUE;
    # collisions mean src already had an edge with the same (peer, rel)
    # as dst — we merge metadata + drop src's edge (and its citations
    # cascade to dst's surviving edge).
    await _repoint_edges_side(db, src=src, dst=dst, side="source", summary=summary)
    await _repoint_edges_side(db, src=src, dst=dst, side="target", summary=summary)

    # Re-point aliases.
    await db.execute(
        update(EntityAlias)
        .where(EntityAlias.canonical_id == src.id)
        .values(canonical_id=dst.id)
    )
    # Re-point anchor_registry.
    await db.execute(
        update(AnchorRegistry)
        .where(AnchorRegistry.canonical_id == src.id)
        .values(canonical_id=dst.id)
    )

    # Write audit crosswalk row (already-applied).
    now = datetime.now(timezone.utc)
    db.add(
        AliasCrosswalk(
            from_id=src.id,
            to_id=dst.id,
            from_id_frozen=src.id,
            to_id_frozen=dst.id,
            reason=reason,
            applied_at=now,
        )
    )
    # Flush the crosswalk BEFORE deleting src (ondelete SET NULL wipes
    # from_id if src goes away first — the frozen columns still preserve
    # the audit trail).
    await db.flush()

    await db.delete(src)
    summary.fragments_deleted += 1
    await db.flush()
    return "merged"


async def _repoint_edges_side(
    db: AsyncSession,
    *,
    src: CanonicalEntity,
    dst: CanonicalEntity,
    side: str,
    summary: RepairSummary,
) -> None:
    """Re-point ``src``'s ``source_id`` (or ``target_id``) edges to
    ``dst``. Handles the ``(source_id, target_id, relation)`` unique
    constraint: when a collision would occur, merge src's edge metadata
    into the existing dst edge, move its citations, then delete src's
    edge.
    """
    if side == "source":
        col = CanonicalEdge.source_id
        other_col = "target_id"
    else:
        col = CanonicalEdge.target_id
        other_col = "source_id"

    src_edges = (
        (await db.execute(select(CanonicalEdge).where(col == src.id)))
        .scalars()
        .all()
    )
    for e in src_edges:
        # Compute the desired (source, target, relation) tuple after
        # re-point.
        if side == "source":
            new_source, new_target = dst.id, e.target_id
        else:
            new_source, new_target = e.source_id, dst.id
        collision = (
            await db.execute(
                select(CanonicalEdge).where(
                    CanonicalEdge.source_id == new_source,
                    CanonicalEdge.target_id == new_target,
                    CanonicalEdge.relation == e.relation,
                )
            )
        ).scalar_one_or_none()

        # Self-loops (survivor becomes both source and target) — drop.
        # SourceCitation FK on delete cascade deletes attached citations.
        if new_source == new_target:
            await db.delete(e)
            continue

        if collision is not None and collision.id != e.id:
            # Merge metadata into the survivor edge (last-write-wins).
            merged = dict(collision.edge_metadata or {})
            merged.update(e.edge_metadata or {})
            collision.edge_metadata = merged
            # Move src's citations to the survivor edge.
            await db.execute(
                update(SourceCitation)
                .where(SourceCitation.edge_id == e.id)
                .values(edge_id=collision.id)
            )
            summary.citations_repointed += 1
            # Drop src's edge (its citations already re-parented).
            await db.delete(e)
        else:
            if side == "source":
                e.source_id = dst.id
            else:
                e.target_id = dst.id
            summary.edges_repointed += 1
    await db.flush()


# ─── Query helpers ─────────────────────────────────────────────────


async def _count_d2_target_nodes(db: AsyncSession) -> int:
    """Count distinct target canonicals reachable from any D2 relation."""
    rows = (
        (
            await db.execute(
                select(CanonicalEdge.target_id)
                .where(CanonicalEdge.relation.in_(_D2_TARGET_RELATIONS))
                .distinct()
            )
        )
        .scalars()
        .all()
    )
    return len(rows)


async def _load_d2_created_targets(
    db: AsyncSession, *, since: date
) -> list[CanonicalEntity]:
    """Return every canonical that (a) was created on/after ``since`` AND
    (b) is a TARGET of one of the D2 relations. That is the fragment set."""
    target_ids = (
        (
            await db.execute(
                select(CanonicalEdge.target_id)
                .where(CanonicalEdge.relation.in_(_D2_TARGET_RELATIONS))
                .distinct()
            )
        )
        .scalars()
        .all()
    )
    if not target_ids:
        return []
    since_ts = datetime.combine(since, datetime.min.time())
    rows = (
        (
            await db.execute(
                select(CanonicalEntity).where(
                    CanonicalEntity.id.in_(target_ids),
                    CanonicalEntity.created_at >= since_ts,
                )
            )
        )
        .scalars()
        .all()
    )
    return list(rows)


async def _find_preexisting(
    db: AsyncSession, clean_norm: str, ent_type: str, since: date
) -> CanonicalEntity | None:
    """Return the pre-existing (created BEFORE ``since``) canonical whose
    normalized name matches ``clean_norm`` and whose type matches
    ``ent_type``. Prefer OPEN + oldest (stable identity)."""
    since_ts = datetime.combine(since, datetime.min.time())
    candidates = (
        (
            await db.execute(
                select(CanonicalEntity).where(
                    CanonicalEntity.canonical_name_normalized == clean_norm,
                    CanonicalEntity.type == ent_type,
                    CanonicalEntity.created_at < since_ts,
                )
            )
        )
        .scalars()
        .all()
    )
    if not candidates:
        return None
    return sorted(
        candidates,
        key=lambda c: (
            0 if c.surface_mode == SurfaceMode.OPEN.value else 1,
            c.created_at,
            c.id,
        ),
    )[0]


# ─── CLI ─────────────────────────────────────────────────────────


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", type=str, default="2026-08-04",
                    help="Only repair canonicals created on/after this ISO date")
    args = ap.parse_args()
    since = date.fromisoformat(args.since)
    summary = asyncio.run(repair_d2_fragments(since=since))
    print("D2.1 repair summary:")
    for k in (
        "d2_target_nodes_before",
        "d2_target_nodes_after",
        "fragments_scanned",
        "merged_to_preexisting",
        "merged_to_fresh_survivor",
        "renamed_in_place",
        "persons_skipped",
        "unchanged",
        "edges_repointed",
        "citations_repointed",
        "fragments_deleted",
        "refused_privacy",
    ):
        print(f"  {k:26s} = {getattr(summary, k)}")
    print("  top survivors (name → n fragments merged in):")
    for name, n in summary.per_survivor_top:
        print(f"    {n:5d}  {name}")


if __name__ == "__main__":  # pragma: no cover
    _main()
