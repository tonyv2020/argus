"""Neo4jProjection — write-through projection of PG truth (design §5a/§5c).

Adapted from legal-lab's `neo4j_projection.py`. Neo4j holds NO independent
truth: every write is an idempotent MERGE keyed on the Postgres id, and
`projected_at` is stamped on the PG row on success. Missing/unavailable Neo4j
degrades to a no-op (project) or None (read) so the API still serves from PG.

Only surfaces edges that carry ≥1 SourceCitation — the citation gate is applied
here as a defense-in-depth mirror of the PG-side gate.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models import CanonicalEdge, CanonicalEntity, SourceCitation

logger = logging.getLogger(__name__)


def _now() -> datetime:
    """Timezone-aware UTC now, for `projected_at` stamps."""
    return datetime.now(UTC)


class Neo4jProjection:
    """Thin write-through projection + Cypher reads. Optional Neo4j driver."""

    def __init__(self, driver=None):
        self._driver = driver
        self._checked = driver is not None

    @property
    def driver(self):
        """Return the Neo4j driver, creating it on first call if enabled."""
        if self._driver is None and not self._checked:
            self._checked = True
            if not settings.neo4j_enabled:
                return None
            try:
                from neo4j import GraphDatabase

                self._driver = GraphDatabase.driver(
                    settings.neo4j_uri,
                    auth=(settings.neo4j_user, settings.neo4j_password),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("neo4j: driver unavailable, projection disabled: %s", exc)
                self._driver = None
        return self._driver

    @property
    def available(self) -> bool:
        """True when a live Neo4j driver is initialised and reachable."""
        return self.driver is not None

    def _run(self, cypher: str, **params):
        """Run a single Cypher statement; None-safe when driver is absent."""
        drv = self.driver
        if drv is None:
            return None
        with drv.session() as s:
            return list(s.run(cypher, **params))

    async def project_entity(self, session: AsyncSession, canonical: CanonicalEntity) -> bool:
        """MERGE a Canonical node and stamp `projected_at` on success."""
        if not self.available:
            return False
        try:
            self._run(
                "MERGE (c:Canonical {pg_id: $id}) SET c.label=$label, c.type=$type",
                id=canonical.id,
                label=canonical.canonical_name,
                type=canonical.type,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("neo4j project_entity failed for %s: %s", canonical.id, exc)
            return False
        canonical.projected_at = _now()
        session.add(canonical)
        return True

    async def project_edge(self, session: AsyncSession, edge: CanonicalEdge) -> bool:
        """MERGE a canonical edge — gated on the edge having ≥1 SourceCitation."""
        if not self.available:
            return False
        citations = (
            (await session.execute(select(SourceCitation).where(SourceCitation.edge_id == edge.id)))
            .scalars()
            .all()
        )
        if not citations:
            # No citation → not projected. The API layer must also refuse it.
            return False
        try:
            self._run(
                "MATCH (s:Canonical {pg_id: $src}), (t:Canonical {pg_id: $tgt}) "
                "MERGE (s)-[r:REL {pg_id: $id}]->(t) "
                "SET r.relation=$rel, r.weight=$w, r.citation_count=$cc",
                src=edge.source_id,
                tgt=edge.target_id,
                id=edge.id,
                rel=edge.relation,
                w=edge.weight,
                cc=len(citations),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("neo4j project_edge failed for %s: %s", edge.id, exc)
            return False
        edge.projected_at = _now()
        session.add(edge)
        return True
