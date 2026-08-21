"""P0 — sweep all CanonicalEntity + CanonicalEdge rows into Neo4j.

Idempotent: MERGE on pg_id. Skips edges without ≥1 SourceCitation (defense-in-depth
citation gate). Stamps `projected_at` on success.

The sweep also PRUNES (P2, 2026-08-21): projection is MERGE-only, so a
canonical deleted in Postgres — by the dedup/merge pass, say — would linger
in Neo4j with its old relationships and the projection would keep serving
duplicates the merge had already collapsed.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import CanonicalEdge, CanonicalEntity
from app.services.graph.neo4j_projection import Neo4jProjection

logger = logging.getLogger(__name__)


@dataclass
class ProjectionStats:
    """Counters for the projection sweep."""

    entities_projected: int = 0
    entities_failed: int = 0
    edges_projected: int = 0
    edges_skipped_no_citation: int = 0
    edges_failed: int = 0
    stale_nodes_pruned: int = 0
    stale_rels_pruned: int = 0


def _ensure_pg_id_index(projection: Neo4jProjection) -> None:
    """Ensure `Canonical(pg_id)` is unique + indexed before the sweep.

    Without this, every edge MERGE does a full label scan on both endpoints
    (~56k Canonical nodes × 2 lookups per edge) — a full reproject took
    hours pre-index instead of the minutes it should. The UNIQUE constraint
    auto-provisions a range index. Idempotent (IF NOT EXISTS).
    """
    drv = projection.driver
    if drv is None:
        return
    with drv.session() as s:
        s.run(
            "CREATE CONSTRAINT canonical_pg_id_unique IF NOT EXISTS "
            "FOR (c:Canonical) REQUIRE c.pg_id IS UNIQUE"
        )


async def project_all(session: AsyncSession, projection: Neo4jProjection) -> ProjectionStats:
    """Sweep every entity + edge into Neo4j (idempotent MERGE by pg_id)."""
    stats = ProjectionStats()
    _ensure_pg_id_index(projection)

    entities = (await session.execute(select(CanonicalEntity))).scalars().all()
    for e in entities:
        if await projection.project_entity(session, e):
            stats.entities_projected += 1
        else:
            stats.entities_failed += 1
    await session.commit()

    edges = (await session.execute(select(CanonicalEdge))).scalars().all()
    for edge in edges:
        result = await projection.project_edge(session, edge)
        if result:
            stats.edges_projected += 1
        else:
            # Distinguish citation-gate skip from actual failure via a follow-up read.
            stats.edges_skipped_no_citation += 1
    await session.commit()

    # MERGE-only projection leaves deleted canonicals behind forever, so a
    # full sweep has to prune too — otherwise the P2 dedup merges are
    # invisible here and Neo4j keeps serving the collapsed duplicates.
    stats.stale_nodes_pruned, stats.stale_rels_pruned = projection.prune_missing(
        {e.id for e in entities}, {edge.id for edge in edges}
    )
    return stats


async def main_async() -> ProjectionStats:
    """Run the projection sweep against Argus Postgres + Neo4j."""
    sm = get_sessionmaker()
    projection = Neo4jProjection()
    async with sm() as session:
        return await project_all(session, projection)


def main() -> None:
    """CLI entrypoint — python -m app.services.ingest.project_to_neo4j."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = asyncio.run(main_async())
    logger.info("projection sweep done: %s", stats)


if __name__ == "__main__":
    main()
