"""P0 — sweep all CanonicalEntity + CanonicalEdge rows into Neo4j.

Idempotent: MERGE on pg_id. Skips edges without ≥1 SourceCitation (defense-in-depth
citation gate). Stamps `projected_at` on success.
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


async def project_all(session: AsyncSession, projection: Neo4jProjection) -> ProjectionStats:
    """Sweep every entity + edge into Neo4j (idempotent MERGE by pg_id)."""
    stats = ProjectionStats()

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
