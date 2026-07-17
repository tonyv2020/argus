"""FastAPI app for Argus — public, read-only navigator over the citation-cited graph."""

from __future__ import annotations

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models import CanonicalEntity
from app.services.graph.base import CytoscapeGraph, empty_graph
from app.services.graph.pgvector_store import PgVectorStore

app = FastAPI(
    title="Argus",
    description="Ontology navigator — every edge cited to a filing ID or article permalink.",
    version="0.1.0",
)


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe."""
    return {"status": "ok"}


@app.get("/api/entities/{canonical_id}")
async def get_entity(canonical_id: str, db: AsyncSession = Depends(get_db)) -> dict:
    """Return the canonical entity's summary (label + type + citation count)."""
    ent = (
        await db.execute(select(CanonicalEntity).where(CanonicalEntity.id == canonical_id))
    ).scalar_one_or_none()
    if ent is None:
        raise HTTPException(status_code=404, detail="entity not found")
    return {
        "id": ent.id,
        "label": ent.canonical_name,
        "type": ent.type,
    }


@app.get("/api/entities/{canonical_id}/subgraph")
async def get_entity_subgraph(
    canonical_id: str,
    hops: int = 1,
    db: AsyncSession = Depends(get_db),
) -> CytoscapeGraph:
    """Return the cited subgraph anchored at `canonical_id`, expanded `hops` deep.

    Edges without a SourceCitation are elided by the store (see `PgVectorStore.
    get_entity_subgraph` — design §5.2 discipline).
    """
    ent = (
        await db.execute(select(CanonicalEntity).where(CanonicalEntity.id == canonical_id))
    ).scalar_one_or_none()
    if ent is None:
        return empty_graph()
    store = PgVectorStore()
    return await store.get_entity_subgraph(db, canonical_id, hops=hops)
