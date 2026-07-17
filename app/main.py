"""FastAPI app for Argus — public, read-only navigator over the citation-cited graph."""

from __future__ import annotations

from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_db
from app.models import CanonicalEntity, EntityAlias, SurfaceMode
from app.services.graph.base import CytoscapeGraph, empty_graph
from app.services.graph.pgvector_store import PgVectorStore


def _public_label(ent: CanonicalEntity) -> str | None:
    """Return the label to show publicly for `ent`, or None if the node must be suppressed.

    Tony 2026-07-17: never leak the real name for a private person — return
    `public_alias` instead. SUPPRESS returns None so the caller elides the node.
    """
    mode = ent.surface_mode or SurfaceMode.OPEN.value
    if mode == SurfaceMode.SUPPRESS.value:
        return None
    if mode == SurfaceMode.ALIAS.value:
        return ent.public_alias or f"Private donor #{ent.id.replace('-', '')[:8]}"
    return ent.canonical_name


app = FastAPI(
    title="Argus",
    description="Ontology navigator — every edge cited to a filing ID or article permalink.",
    version="0.1.0",
)


_STATIC_DIR = Path(__file__).parent / "static"
if _STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")


@app.get("/health")
async def health() -> dict[str, str]:
    """Liveness probe."""
    return {"status": "ok"}


@app.get("/")
async def root() -> FileResponse:
    """Serve the SPA index — the profile + Cytoscape UI (P2)."""
    index = _STATIC_DIR / "index.html"
    if not index.exists():
        raise HTTPException(status_code=404, detail="ui not built")
    return FileResponse(str(index))


@app.get("/entity/{canonical_id}")
async def entity_deep_link(canonical_id: str) -> FileResponse:
    """Serve the SPA for a shareable /entity/<id> URL — client-side JS reads the id."""
    del canonical_id  # id is consumed client-side
    index = _STATIC_DIR / "index.html"
    if not index.exists():
        raise HTTPException(status_code=404, detail="ui not built")
    return FileResponse(str(index))


@app.get("/api/search")
async def search(
    q: str = Query(..., min_length=1, max_length=120),
    limit: int = Query(20, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Search canonical entities by name / alias — scrutiny-respecting.

    Rules (helen 2026-07-17):
      * suppressed entities are never returned.
      * alias-mode entities match on `public_alias` ONLY (never on `canonical_name`
        or on any of their EntityAlias.surface_name rows — those are the real
        name(s) of a real private person).
      * open entities match on canonical_name + EntityAlias.surface_name (real
        name is the label already, so aliasing on real-name is a fair search).
      * Returns `{results: [{id, label, type, surface_mode}], q, matched: N}`
        so the SPA can render "no entity found for X" gracefully.
    """
    q_norm = q.strip()
    if not q_norm:
        return {"q": q, "results": [], "matched": 0}
    like = f"%{q_norm.lower()}%"

    # 1. open entities by canonical_name (case-insensitive).
    open_by_name = (
        (
            await db.execute(
                select(CanonicalEntity)
                .where(CanonicalEntity.surface_mode == SurfaceMode.OPEN.value)
                .where(CanonicalEntity.canonical_name_normalized.ilike(like))
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )

    # 2. open entities via an alias's surface_name (real name; safe to expose).
    open_by_alias = (
        (
            await db.execute(
                select(CanonicalEntity)
                .join(EntityAlias, EntityAlias.canonical_id == CanonicalEntity.id)
                .where(CanonicalEntity.surface_mode == SurfaceMode.OPEN.value)
                .where(EntityAlias.surface_name_normalized.ilike(like))
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )

    # 3. alias-mode entities matching on `public_alias` ONLY. Real-name aliases
    #    are NEVER matched — that would leak the identity via a hit.
    aliased = (
        (
            await db.execute(
                select(CanonicalEntity)
                .where(CanonicalEntity.surface_mode == SurfaceMode.ALIAS.value)
                .where(CanonicalEntity.public_alias.ilike(like))
                .limit(limit)
            )
        )
        .scalars()
        .all()
    )

    # Dedup, cap, and render with public labels only.
    seen: set[str] = set()
    out: list[dict] = []
    for e in list(open_by_name) + list(open_by_alias) + list(aliased):
        if e.id in seen:
            continue
        seen.add(e.id)
        label = _public_label(e)
        if label is None:
            continue
        out.append(
            {
                "id": e.id,
                "label": label,
                "type": e.type,
                "surface_mode": e.surface_mode,
            }
        )
        if len(out) >= limit:
            break
    return {"q": q, "matched": len(out), "results": out}


@app.get("/api/entities/{canonical_id}")
async def get_entity(canonical_id: str, db: AsyncSession = Depends(get_db)) -> dict:
    """Return the canonical entity's summary — private-person nodes return alias, not real name."""
    ent = (
        await db.execute(select(CanonicalEntity).where(CanonicalEntity.id == canonical_id))
    ).scalar_one_or_none()
    if ent is None:
        raise HTTPException(status_code=404, detail="entity not found")
    label = _public_label(ent)
    if label is None:
        raise HTTPException(status_code=404, detail="entity not surfaceable")
    return {
        "id": ent.id,
        "label": label,
        "type": ent.type,
        "surface_mode": ent.surface_mode,
    }


@app.get("/api/entities/{canonical_id}/subgraph")
async def get_entity_subgraph(
    canonical_id: str,
    hops: int = 1,
    db: AsyncSession = Depends(get_db),
) -> CytoscapeGraph:
    """Return the cited subgraph anchored at `canonical_id`, expanded `hops` deep.

    Edges without a SourceCitation are elided by the store (design §5.2). Nodes
    with `surface_mode=alias` have their label swapped to `public_alias` (Tony
    2026-07-17). Nodes with `surface_mode=suppress` are elided from the response.
    """
    ent = (
        await db.execute(select(CanonicalEntity).where(CanonicalEntity.id == canonical_id))
    ).scalar_one_or_none()
    if ent is None or _public_label(ent) is None:
        return empty_graph()
    store = PgVectorStore()
    graph = await store.get_entity_subgraph(db, canonical_id, hops=hops)
    # Rewrite labels + drop suppressed nodes before returning.
    node_ids = [n["data"]["id"] for n in graph["nodes"]]
    ents_by_id = {
        e.id: e
        for e in (await db.execute(select(CanonicalEntity).where(CanonicalEntity.id.in_(node_ids))))
        .scalars()
        .all()
    }
    kept_nodes = []
    suppressed_ids: set[str] = set()
    for n in graph["nodes"]:
        e = ents_by_id.get(n["data"]["id"])
        if e is None:
            continue
        label = _public_label(e)
        if label is None:
            suppressed_ids.add(e.id)
            continue
        n["data"]["label"] = label
        n["data"]["surface_mode"] = e.surface_mode
        kept_nodes.append(n)
    kept_edges = [
        e
        for e in graph["edges"]
        if e["data"]["source"] not in suppressed_ids and e["data"]["target"] not in suppressed_ids
    ]
    return {"nodes": kept_nodes, "edges": kept_edges}
