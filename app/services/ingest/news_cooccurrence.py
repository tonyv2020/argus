"""P0 — derive MENTIONED_WITH edges from hollywood artifacts sharing entities.

For each hollywood artifact that has two-or-more Argus canonicals attached (via
EntityAlias.source_system='hollywood.entity_tags'), emit an undirected
MENTIONED_WITH edge for each unordered pair, weighted by co-occurrence count.
Each edge carries a SourceCitation pointing at the article's permalink so the
UI can click through.

Permalinks: hollywood exposes public permalinks under `/s/<slug>` (see
`docs/tony-times-cited-design.md`). We assemble a permalink URL by looking up
the `news_items.permalink_slug` field on the hollywood side; where a slug is
absent (some artifacts aren't news_items) the artifact_kind + id are used as a
best-effort citation ref.
"""

from __future__ import annotations

import asyncio
import logging
from collections import defaultdict
from dataclasses import dataclass

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import settings
from app.db import get_sessionmaker
from app.models import (
    CanonicalEdge,
    EdgeRelation,
    EntityAlias,
    SourceCitation,
    SourceKind,
)

logger = logging.getLogger(__name__)


@dataclass
class CooccurrenceStats:
    """Counters for the news-cooccurrence pass."""

    artifacts_scanned: int = 0
    pairs_emitted: int = 0
    edges_created: int = 0
    edges_reused: int = 0
    citations_created: int = 0
    errors: int = 0


async def _artifact_permalink_map(artifact_ids: list[str]) -> dict[str, str]:
    """Fetch permalink slugs from hollywood.news_items for a list of artifact ids."""
    if not artifact_ids:
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
                {"ids": artifact_ids},
            )
            return {row.id: row.permalink_slug for row in result.mappings().all()}
    finally:
        await engine.dispose()


def _permalink_url(kind: str, artifact_id: str, slug: str | None) -> str:
    """Assemble the public permalink URL — falls back to a kind:id ref for non-slugged artifacts."""
    if slug:
        return f"https://tonyvigna.com/s/{slug}"
    return f"holo://{kind}/{artifact_id}"


async def run_cooccurrence(min_artifacts_per_pair: int = 1) -> CooccurrenceStats:
    """Materialise MENTIONED_WITH edges from cross-canonical co-occurrence in hollywood artifacts.

    `min_artifacts_per_pair` — require at least this many shared artifacts before
    an edge surfaces. Default 1 because each edge is still individually cited to
    its supporting article; the parameter exists for later noise-tuning.
    """
    stats = CooccurrenceStats()
    sm = get_sessionmaker()
    async with sm() as session:
        # Fetch all hollywood-sourced aliases with their (artifact_kind, artifact_id).
        # entity_tags.id maps 1:1 to alias.source_id; we look up (kind, artifact_id)
        # on the hollywood side by id.
        aliases = (
            (
                await session.execute(
                    select(EntityAlias).where(EntityAlias.source_system == "hollywood.entity_tags")
                )
            )
            .scalars()
            .all()
        )
        if not aliases:
            return stats

        # Pull artifact refs from hollywood in one bulk fetch.
        alias_ids = [a.source_id for a in aliases]
        artifact_refs = await _fetch_artifact_refs(alias_ids)
        # Group canonicals by artifact so we can produce all pairs per artifact.
        by_artifact: dict[tuple[str, str], set[str]] = defaultdict(set)
        for a in aliases:
            key = artifact_refs.get(a.source_id)
            if key is None:
                continue
            by_artifact[key].add(a.canonical_id)

        stats.artifacts_scanned = len(by_artifact)
        # Emit pairs per artifact + collect for permalink batch.
        pair_artifacts: dict[tuple[str, str], list[tuple[str, str]]] = defaultdict(list)
        for (kind, art_id), canonicals in by_artifact.items():
            canonicals_sorted = sorted(canonicals)
            for i in range(len(canonicals_sorted)):
                for j in range(i + 1, len(canonicals_sorted)):
                    a, b = canonicals_sorted[i], canonicals_sorted[j]
                    pair_artifacts[(a, b)].append((kind, art_id))

        # Permalinks for the news_items artifacts.
        news_ids = list(
            {art_id for (_, art_id) in {a for pairs in pair_artifacts.values() for a in pairs}}
        )
        permalinks = await _artifact_permalink_map(news_ids)

        # Persist edges + citations.
        for (a, b), artifacts in pair_artifacts.items():
            if len(artifacts) < min_artifacts_per_pair:
                continue
            stats.pairs_emitted += 1
            edge = (
                await session.execute(
                    select(CanonicalEdge).where(
                        CanonicalEdge.source_id == a,
                        CanonicalEdge.target_id == b,
                        CanonicalEdge.relation == EdgeRelation.MENTIONED_WITH.value,
                    )
                )
            ).scalar_one_or_none()
            if edge is None:
                edge = CanonicalEdge(
                    source_id=a,
                    target_id=b,
                    relation=EdgeRelation.MENTIONED_WITH.value,
                    weight=float(len(artifacts)),
                )
                session.add(edge)
                await session.flush()
                stats.edges_created += 1
            else:
                edge.weight = float(len(artifacts))
                stats.edges_reused += 1
            for kind, art_id in artifacts:
                slug = permalinks.get(art_id)
                session.add(
                    SourceCitation(
                        edge_id=edge.id,
                        kind=SourceKind.ARTICLE_PERMALINK.value,
                        citation_url=_permalink_url(kind, art_id, slug),
                        citation_ref=slug or f"{kind}:{art_id}",
                    )
                )
                stats.citations_created += 1

        try:
            await session.commit()
        except Exception as exc:  # noqa: BLE001
            await session.rollback()
            stats.errors += 1
            logger.exception("cooccurrence commit failed: %s", exc)

    return stats


async def _fetch_artifact_refs(alias_ids: list[str]) -> dict[str, tuple[str, str]]:
    """Fetch (artifact_kind, artifact_id) per entity_tags row, keyed by tag id."""
    if not alias_ids:
        return {}
    engine = create_async_engine(settings.hollywood_database_url, pool_pre_ping=True)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT id::text AS id, artifact_kind, artifact_id::text AS artifact_id "
                    "FROM entity_tags "
                    "WHERE id::text = ANY(:ids)"
                ),
                {"ids": alias_ids},
            )
            return {row.id: (row.artifact_kind, row.artifact_id) for row in result.mappings().all()}
    finally:
        await engine.dispose()


def main() -> None:
    """CLI entrypoint — `python -m app.services.ingest.news_cooccurrence`."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = asyncio.run(run_cooccurrence())
    logger.info("cooccurrence done: %s", stats)


if __name__ == "__main__":
    main()
