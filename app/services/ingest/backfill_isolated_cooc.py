"""T6 — MENTIONED_WITH backfill for isolated event/concept canonicals (helen 2026-07-17).

An event/concept canonical is "isolated" when it has zero incident edges. This
happens when either:
  a. It was lag-filled by /api/resolve (T4) with only ONE hollywood alias — other
     hollywood entity_tags rows for the same tag_normalized haven't been picked up.
  b. It was created by the batched resolver but its co-occurrence hasn't been
     computed yet (news_cooccurrence hasn't caught up).

This backfill:
  1. Finds every event/concept canonical with zero edges.
  2. Reads hollywood.entity_tags rows matching `tag_normalized` for that
     canonical's aliases + creates any MISSING EntityAlias rows (idempotent).
  3. For each artifact those aliases live in, finds every OTHER argus canonical
     that appears in the same artifact and emits a MENTIONED_WITH edge cited to
     the artifact permalink.

Reuses the same permalink lookup + citation shape as news_cooccurrence — the
render-side citation gate applies uniformly to both sources.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import settings
from app.db import get_sessionmaker
from app.models import (
    CanonicalEdge,
    CanonicalEntity,
    EdgeRelation,
    EntityAlias,
    EntityType,
    SourceCitation,
    SourceKind,
)
from app.services.graph.base import normalize_name

logger = logging.getLogger(__name__)


@dataclass
class IsolatedBackfillStats:
    """Counters for the isolated-node co-occurrence backfill."""

    canonicals_scanned: int = 0
    hollywood_rows_pulled: int = 0
    aliases_created: int = 0
    edges_created: int = 0
    edges_reused: int = 0
    citations_created: int = 0
    errors: int = 0


async def _hollywood_rows_for(tag_normalized: str) -> list[dict]:
    """Return every hollywood.entity_tags row matching this tag_normalized."""
    engine = create_async_engine(settings.hollywood_database_url, pool_pre_ping=True)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT id::text AS id, tag, kind_hint, artifact_kind, "
                    "       artifact_id::text AS artifact_id "
                    "FROM entity_tags WHERE tag_normalized = :n"
                ),
                {"n": tag_normalized},
            )
            return [dict(r) for r in result.mappings().all()]
    finally:
        await engine.dispose()


async def _permalinks(news_ids: list[str]) -> dict[str, str]:
    """Fetch permalink slugs for a batch of news_item ids."""
    if not news_ids:
        return {}
    engine = create_async_engine(settings.hollywood_database_url, pool_pre_ping=True)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT id::text AS id, permalink_slug "
                    "FROM news_items "
                    "WHERE id::text = ANY(:ids) AND permalink_slug IS NOT NULL"
                ),
                {"ids": news_ids},
            )
            return {r.id: r.permalink_slug for r in result.mappings().all()}
    finally:
        await engine.dispose()


def _permalink_url(kind: str, artifact_id: str, slug: str | None) -> str:
    """Assemble the public permalink URL — falls back to a kind:id ref if unslugged."""
    if slug:
        return f"https://tonyvigna.com/s/{slug}"
    return f"holo://{kind}/{artifact_id}"


async def run_isolated_backfill(max_canonicals: int | None = None) -> IsolatedBackfillStats:
    """Backfill MENTIONED_WITH for every isolated event/concept canonical."""
    stats = IsolatedBackfillStats()
    sm = get_sessionmaker()

    async with sm() as session:
        rows = (
            (
                await session.execute(
                    text(
                        "SELECT ce.id, ce.canonical_name_normalized "
                        "FROM canonical_entities ce "
                        "WHERE ce.type IN ('event','concept') "
                        "AND NOT EXISTS ("
                        "  SELECT 1 FROM canonical_edges e "
                        "  WHERE e.source_id=ce.id OR e.target_id=ce.id"
                        ")"
                    )
                )
            )
            .mappings()
            .all()
        )
    isolated = [(r["id"], r["canonical_name_normalized"]) for r in rows]
    if max_canonicals is not None:
        isolated = isolated[:max_canonicals]
    stats.canonicals_scanned = len(isolated)
    logger.info("isolated event/concept canonicals: %d", stats.canonicals_scanned)

    for canonical_id, norm in isolated:
        hollywood_rows = await _hollywood_rows_for(norm)
        stats.hollywood_rows_pulled += len(hollywood_rows)
        if not hollywood_rows:
            continue

        # a) upsert missing aliases — per-row commit + skip on unique-conflict
        #    so races with the concurrent resolver Job don't kill the batch.
        for hr in hollywood_rows:
            async with sm() as session:
                dup = (
                    await session.execute(
                        select(EntityAlias).where(
                            EntityAlias.source_system == "hollywood.entity_tags",
                            EntityAlias.source_id == hr["id"],
                        )
                    )
                ).scalar_one_or_none()
                if dup is not None:
                    continue
                session.add(
                    EntityAlias(
                        canonical_id=canonical_id,
                        source_system="hollywood.entity_tags",
                        source_id=hr["id"],
                        surface_name=hr["tag"],
                        surface_name_normalized=normalize_name(hr["tag"]),
                        kind_hint=(hr.get("kind_hint") or None),
                    )
                )
                try:
                    await session.commit()
                    stats.aliases_created += 1
                except Exception as exc:  # noqa: BLE001
                    await session.rollback()
                    stats.errors += 1
                    logger.warning(
                        "alias insert skipped canonical=%s source_id=%s: %s",
                        canonical_id,
                        hr["id"],
                        exc,
                    )

        # b) for each artifact those rows live in, find every OTHER argus canonical
        #    that appears in the same artifact and emit MENTIONED_WITH cited to permalink.
        artifact_keys = {(hr["artifact_kind"], hr["artifact_id"]) for hr in hollywood_rows}
        news_ids = [aid for (kind, aid) in artifact_keys if kind == "news"]
        permalinks = await _permalinks(news_ids)

        for kind, art_id in artifact_keys:
            # Find every other canonical attached to this artifact via a hollywood alias.
            async with sm() as session:
                # 1. fetch matching aliases in hollywood for this artifact
                engine = create_async_engine(settings.hollywood_database_url, pool_pre_ping=True)
                try:
                    async with engine.connect() as conn:
                        res = await conn.execute(
                            text(
                                "SELECT id::text AS id FROM entity_tags "
                                "WHERE artifact_kind = :k AND artifact_id::text = :a"
                            ),
                            {"k": kind, "a": art_id},
                        )
                        alias_source_ids = [r.id for r in res.mappings().all()]
                finally:
                    await engine.dispose()
                if not alias_source_ids:
                    continue

                # 2. find the argus canonicals for those alias source_ids
                canonicals_here = (
                    (
                        await session.execute(
                            select(EntityAlias.canonical_id)
                            .where(EntityAlias.source_system == "hollywood.entity_tags")
                            .where(EntityAlias.source_id.in_(alias_source_ids))
                        )
                    )
                    .scalars()
                    .all()
                )
                canonicals_here = set(canonicals_here) - {canonical_id}
                if not canonicals_here:
                    continue

                slug = permalinks.get(art_id) if kind == "news" else None
                url = _permalink_url(kind, art_id, slug)
                for other in canonicals_here:
                    a, b = sorted([canonical_id, other])
                    existing = (
                        await session.execute(
                            select(CanonicalEdge).where(
                                CanonicalEdge.source_id == a,
                                CanonicalEdge.target_id == b,
                                CanonicalEdge.relation == EdgeRelation.MENTIONED_WITH.value,
                            )
                        )
                    ).scalar_one_or_none()
                    if existing is None:
                        edge = CanonicalEdge(
                            source_id=a,
                            target_id=b,
                            relation=EdgeRelation.MENTIONED_WITH.value,
                            weight=1.0,
                        )
                        session.add(edge)
                        await session.flush()
                        stats.edges_created += 1
                    else:
                        edge = existing
                        edge.weight = float((edge.weight or 0.0) + 1.0)
                        stats.edges_reused += 1
                    session.add(
                        SourceCitation(
                            edge_id=edge.id,
                            kind=SourceKind.ARTICLE_PERMALINK.value,
                            citation_url=url,
                            citation_ref=slug or f"{kind}:{art_id}",
                        )
                    )
                    stats.citations_created += 1
                try:
                    await session.commit()
                except Exception as exc:  # noqa: BLE001
                    await session.rollback()
                    stats.errors += 1
                    logger.exception(
                        "cooc commit failed canonical=%s artifact=%s: %s",
                        canonical_id,
                        (kind, art_id),
                        exc,
                    )

    return stats


def main() -> None:
    """CLI entrypoint — python -m app.services.ingest.backfill_isolated_cooc."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = asyncio.run(run_isolated_backfill())
    logger.info("isolated backfill done: %s", stats)


if __name__ == "__main__":
    main()


# Silence linter: EntityType is imported for future maintainers who wire
# more canonical types into the backfill scope.
_ = EntityType
_ = CanonicalEntity
