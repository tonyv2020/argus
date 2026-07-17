"""PgVectorStore — cosine resolution + subgraph queries against Argus Postgres.

Adapted from `legal-lab.app.services.graph.pgvector_store`. Key differences:
- Argus is single-tenant (no ACL / user_id scoping).
- Person-conservative merge margin — a PERSON match must clear the
  threshold PLUS the safe margin (design §5.1: "never auto-merge two distinct
  real people"). Non-person types use the plain threshold.
- Async session throughout (asyncpg-style dialect).
"""

from __future__ import annotations

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models import CanonicalEdge, CanonicalEntity, EntityType, SourceCitation
from app.services.graph.base import (
    CytoscapeGraph,
    GraphStore,
    empty_graph,
    normalize_name,
)


def _vec_literal(embedding: list[float]) -> str:
    """pgvector text form '[v1,v2,...]' — cast with ::vector in SQL."""
    return "[" + ",".join(repr(float(x)) for x in embedding) + "]"


class PgVectorStore(GraphStore):
    """Postgres-backed truth store — resolution + subgraph reads."""

    async def resolve_entity(
        self,
        session: AsyncSession,
        surface_name: str,
        entity_type: str,
        embedding: list[float] | None,
    ) -> str | None:
        """Return the CanonicalEntity id this name/embedding belongs to, or None.

        Two-stage:
          1. pgvector cosine on the same `entity_type`. A PERSON match must
             clear (threshold + conservative_margin); other types use plain
             threshold. Also requires a shared distinctive normalized-name
             token to guard against embedding-only over-merge.
          2. exact normalized-name match (deterministic fallback).
        """
        base_threshold = settings.resolution_similarity_threshold
        if entity_type == EntityType.PERSON.value:
            required_similarity = base_threshold + settings.resolution_person_conservative_margin
        else:
            required_similarity = base_threshold
        max_distance = 1.0 - required_similarity

        # 1. embedding cosine
        if embedding is not None:
            rows = (
                await session.execute(
                    text(
                        "SELECT id, canonical_name, embedding <=> (:vec)::vector AS dist "
                        "FROM canonical_entities "
                        "WHERE type = :etype AND embedding IS NOT NULL "
                        "ORDER BY dist ASC LIMIT :k"
                    ),
                    {
                        "vec": _vec_literal(embedding),
                        "etype": entity_type,
                        "k": settings.resolution_top_k,
                    },
                )
            ).all()
            if rows and rows[0].dist <= max_distance:
                q_tokens = set(normalize_name(surface_name).split())
                c_tokens = set(normalize_name(rows[0].canonical_name).split())
                if q_tokens & c_tokens:
                    return rows[0].id

        # 2. normalized-name exact match
        norm = normalize_name(surface_name)
        if norm:
            candidates = (
                await session.execute(
                    select(CanonicalEntity).where(CanonicalEntity.type == entity_type)
                )
            ).scalars()
            for ce in candidates:
                if ce.canonical_name_normalized == norm:
                    return ce.id

        return None

    async def get_entity_subgraph(
        self, session: AsyncSession, canonical_id: str, hops: int = 1
    ) -> CytoscapeGraph:
        """Return the subgraph anchored at `canonical_id`, expanded up to `hops`.

        FAIL CLOSED: edges are yielded only when the edge has ≥1 SourceCitation.
        No citation → not shown (design §5.2 discipline).
        """
        # Frontier expansion — simple BFS over canonical_edges.
        seen_nodes: set[str] = {canonical_id}
        seen_edges: set[str] = set()
        frontier = {canonical_id}
        for _ in range(max(hops, 1)):
            if not frontier:
                break
            outbound = (
                (
                    await session.execute(
                        select(CanonicalEdge).where(CanonicalEdge.source_id.in_(frontier))
                    )
                )
                .scalars()
                .all()
            )
            inbound = (
                (
                    await session.execute(
                        select(CanonicalEdge).where(CanonicalEdge.target_id.in_(frontier))
                    )
                )
                .scalars()
                .all()
            )
            next_frontier: set[str] = set()
            for e in list(outbound) + list(inbound):
                # citation gate
                cite_count = (
                    (
                        await session.execute(
                            select(SourceCitation).where(SourceCitation.edge_id == e.id)
                        )
                    )
                    .scalars()
                    .all()
                )
                if not cite_count:
                    continue
                seen_edges.add(e.id)
                for nid in (e.source_id, e.target_id):
                    if nid not in seen_nodes:
                        seen_nodes.add(nid)
                        next_frontier.add(nid)
            frontier = next_frontier

        if not seen_nodes:
            return empty_graph()

        nodes = (
            (
                await session.execute(
                    select(CanonicalEntity).where(CanonicalEntity.id.in_(seen_nodes))
                )
            )
            .scalars()
            .all()
        )
        edges = (
            (await session.execute(select(CanonicalEdge).where(CanonicalEdge.id.in_(seen_edges))))
            .scalars()
            .all()
        )

        graph: CytoscapeGraph = {
            "nodes": [
                {
                    "data": {
                        "id": n.id,
                        "label": n.canonical_name,
                        "type": n.type,
                    }
                }
                for n in nodes
            ],
            "edges": [
                {
                    "data": {
                        "id": e.id,
                        "source": e.source_id,
                        "target": e.target_id,
                        "label": e.relation,
                        "weight": e.weight,
                    }
                }
                for e in edges
            ],
        }
        return graph
