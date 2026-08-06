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
     matches (the desired case — reconnect the disclosure edge to the
     entity Argus already tracks under FEC / USAspending / news).
  b. Merges it into another D2-created canonical that normalized to
     the same clean name (fragment-vs-fragment consolidation — e.g. 3
     Carnival variants collapse to one).
  c. Renames it in place to its clean issuer form when no better match
     exists (still a net win — the public /api/search stops surfacing
     ``CARNIVALCORPPAIREDCTF``).

Every merge goes through ``alias_crosswalk`` + :mod:`merge_pass`, which
already re-points edges + citations + aliases + anchor rows and enforces
the fail-closed surface_mode rule (survivor inherits the MOST-protected
mode). This module does not touch edges directly.

Contract:
* Deterministic; no LLM.
* Person nodes are NEVER touched — the person-conservative rule already
  guards D2's emit, and the repair leaves persons alone by construction
  (we only walk canonicals that are TARGETS of the D2 relations, and
  the D2 relations that produce persons are Part 8 liabilities where
  the person was emitted SUPPRESS'd; we leave those to Scrutiny).
* raw_text on disclosure_rows stays untouched.
* D2 emit remains idempotent: after repair, re-running emit does NOT
  produce a fresh set of fragments because the resolver's fast-path
  normalized-name match now finds the survivor.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import (
    AliasCrosswalk,
    CanonicalEdge,
    CanonicalEntity,
    EdgeRelation,
    EntityType,
    SurfaceMode,
)
from app.services.disclosure_issuer import normalize_issuer
from app.services.graph.base import normalize_name
from app.services.ingest.merge_pass import apply_pending as apply_merges

logger = logging.getLogger(__name__)


# Every relation the D2 emit produces on the target side.
_D2_TARGET_RELATIONS: tuple[str, ...] = (
    EdgeRelation.HOLDS_ASSET.value,
    EdgeRelation.INCOME_FROM.value,
    EdgeRelation.HELD_POSITION.value,
    EdgeRelation.OWES.value,
    EdgeRelation.PARTY_TO_AGREEMENT.value,
)


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
    crosswalks_written: int = 0
    merge_stats: dict = field(default_factory=dict)


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
            summary.d2_target_nodes_before,
            len(candidates),
            since,
        )
        summary.fragments_scanned = len(candidates)

        # Pass 1 — normalize each candidate, classify.
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

        # Pass 2 — for each bucket, resolve to survivor.
        for (clean_norm, ent_type), members in buckets.items():
            pre = await _find_preexisting(db, clean_norm, ent_type, since)
            if pre is not None:
                for src in members:
                    if src.id == pre.id:
                        continue
                    await _enqueue_crosswalk(
                        db, from_id=src.id, to_id=pre.id, reason=f"{reason_stamp}: → preexisting"
                    )
                    summary.crosswalks_written += 1
                    summary.merged_to_preexisting += 1
                continue

            # No pre-existing — pick a survivor within the bucket.
            members_sorted = sorted(members, key=lambda c: (c.created_at, c.id))
            survivor = members_sorted[0]
            # Rename survivor to the clean form so its canonical_name is
            # human-readable (drop `PAIRED CTF` etc.).
            clean_name = normalize_issuer(survivor.canonical_name) or survivor.canonical_name
            if survivor.canonical_name != clean_name:
                survivor.canonical_name = clean_name
                survivor.canonical_name_normalized = clean_norm
                summary.renamed_in_place += 1
            for src in members_sorted[1:]:
                await _enqueue_crosswalk(
                    db, from_id=src.id, to_id=survivor.id, reason=f"{reason_stamp}: → fresh survivor"
                )
                summary.crosswalks_written += 1
                summary.merged_to_fresh_survivor += 1

        await db.commit()

    logger.info("D2.1 repair: %d crosswalks written; applying …", summary.crosswalks_written)
    merge_stats = await apply_merges(dry_run=False)
    summary.merge_stats = {
        "pending": merge_stats.pending,
        "applied": merge_stats.applied,
        "edges_repointed": merge_stats.edges_repointed,
        "refused_privacy": merge_stats.refused_privacy,
        "errors": merge_stats.errors,
    }

    async with sm() as db:
        summary.d2_target_nodes_after = await _count_d2_target_nodes(db)

    return summary


# ─── Internals ─────────────────────────────────────────────────────


async def _count_d2_target_nodes(db: AsyncSession) -> int:
    """Count distinct target canonicals reachable from any D2 relation."""
    rows = (
        await db.execute(
            select(CanonicalEdge.target_id).where(
                CanonicalEdge.relation.in_(_D2_TARGET_RELATIONS)
            ).distinct()
        )
    ).scalars().all()
    return len(rows)


async def _load_d2_created_targets(
    db: AsyncSession, *, since: date
) -> list[CanonicalEntity]:
    """Return every canonical that (a) was created on/after ``since`` AND
    (b) is a TARGET of one of the D2 relations. This is the fragment set."""
    target_ids = (
        (
            await db.execute(
                select(CanonicalEdge.target_id).where(
                    CanonicalEdge.relation.in_(_D2_TARGET_RELATIONS)
                ).distinct()
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
    candidates_sorted = sorted(
        candidates,
        key=lambda c: (
            0 if c.surface_mode == SurfaceMode.OPEN.value else 1,
            c.created_at,
            c.id,
        ),
    )
    return candidates_sorted[0]


async def _enqueue_crosswalk(
    db: AsyncSession, *, from_id: str, to_id: str, reason: str
) -> None:
    """Write one ``alias_crosswalk`` row (unapplied). merge_pass will
    apply it in a fresh session per row."""
    existing = (
        await db.execute(
            select(AliasCrosswalk.id).where(
                AliasCrosswalk.from_id == from_id,
                AliasCrosswalk.to_id == to_id,
                AliasCrosswalk.applied_at.is_(None),
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        return
    row = AliasCrosswalk(
        from_id=from_id,
        to_id=to_id,
        reason=reason,
    )
    db.add(row)


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
        "crosswalks_written",
    ):
        print(f"  {k:26s} = {getattr(summary, k)}")
    print(f"  merge_stats                = {summary.merge_stats}")


if __name__ == "__main__":  # pragma: no cover
    _main()
