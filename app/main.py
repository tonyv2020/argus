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


@app.get("/api/resolve")
async def resolve(
    tag: str = Query(..., min_length=1, max_length=120),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Resolve a hollywood entity_tag surface (`tag_normalized`) → argus canonical entity.

    Powers the-dailies article entity-chip href (helen 2026-07-17). the-dailies
    knows a tag string; it asks argus "is this a real entity, and if so where
    does it live?". Contract:

      * MATCH   → `{resolved: true, id, type, label, path: "/entity/<id>"}`
      * NO      → `{resolved: false, reason: "not an entity (topic/theme)"}`

    Scrutiny is respected — a suppressed canonical returns `{resolved: false}`
    (never leaks the real name) and an aliased canonical returns the
    `public_alias` as `label` (never the real name).

    Matching precedence (most specific first):
      1. EntityAlias.source_system='hollywood.entity_tags' with a matching
         `surface_name_normalized`. This is the highest-fidelity path — the
         tag came from the same source system.
      2. CanonicalEntity.canonical_name_normalized exact match (case-insensitive).
      3. Any EntityAlias.surface_name_normalized exact match.
    """
    from app.services.graph.base import normalize_name

    norm = normalize_name(tag)
    if not norm:
        return {"resolved": False, "reason": "not an entity (topic/theme)"}

    # 1. hollywood.entity_tags-sourced alias (highest fidelity for the caller).
    holly = (
        await db.execute(
            select(CanonicalEntity)
            .join(EntityAlias, EntityAlias.canonical_id == CanonicalEntity.id)
            .where(EntityAlias.source_system == "hollywood.entity_tags")
            .where(EntityAlias.surface_name_normalized == norm)
            .limit(1)
        )
    ).scalar_one_or_none()

    # 2. exact canonical_name_normalized match.
    match = holly
    if match is None:
        match = (
            await db.execute(
                select(CanonicalEntity)
                .where(CanonicalEntity.canonical_name_normalized == norm)
                .limit(1)
            )
        ).scalar_one_or_none()

    # 3. any surface_name_normalized match.
    if match is None:
        match = (
            await db.execute(
                select(CanonicalEntity)
                .join(EntityAlias, EntityAlias.canonical_id == CanonicalEntity.id)
                .where(EntityAlias.surface_name_normalized == norm)
                .limit(1)
            )
        ).scalar_one_or_none()

    if match is None:
        return {"resolved": False, "reason": "not an entity (topic/theme)"}

    label = _public_label(match)
    if label is None:
        # Suppressed — respect scrutiny; never leak a real name.
        return {"resolved": False, "reason": "not an entity (topic/theme)"}

    return {
        "resolved": True,
        "id": match.id,
        "type": match.type,
        "label": label,
        "surface_mode": match.surface_mode,
        "path": f"/entity/{match.id}",
    }


async def _entity_importance(db: AsyncSession, entity_id: str) -> int:
    """Importance ~= edge count + citation count summed. Cheap proxy for node significance."""
    from sqlalchemy import func

    from app.models import CanonicalEdge, SourceCitation

    edge_count = (
        await db.execute(
            select(func.count(CanonicalEdge.id)).where(
                (CanonicalEdge.source_id == entity_id) | (CanonicalEdge.target_id == entity_id)
            )
        )
    ).scalar_one() or 0
    citation_count = (
        await db.execute(
            select(func.count(SourceCitation.id))
            .join(CanonicalEdge, CanonicalEdge.id == SourceCitation.edge_id)
            .where((CanonicalEdge.source_id == entity_id) | (CanonicalEdge.target_id == entity_id))
        )
    ).scalar_one() or 0
    return int(edge_count) + int(citation_count)


@app.get("/api/search")
async def search(
    q: str = Query(..., min_length=1, max_length=120),
    limit: int = Query(20, ge=1, le=50),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Search canonical entities by name / alias — scrutiny-respecting + ranked.

    Ranking (helen 2026-07-17 fix — entity-name hits must beat mid-word substrings):
      Tier 5  exact canonical_name_normalized match.
      Tier 4  canonical_name_normalized starts with q.
      Tier 3  canonical_name_normalized substring (mid-word).
      Tier 2  EntityAlias.surface_name_normalized starts with q (open only).
      Tier 1  EntityAlias.surface_name_normalized substring (open only).
      Tier 1  public_alias contains q (alias-mode only).

    Within a tier: rank by node importance (edge_count + citation_count) desc,
    then alphabetical by label for a stable ordering.

    Scrutiny (unchanged):
      * SUPPRESS entities are never returned.
      * ALIAS entities match on `public_alias` ONLY (never on real name/aliases).
      * OPEN entities may match on canonical_name + EntityAlias.surface_name.
    """
    q_norm = q.strip().lower()
    if not q_norm:
        return {"q": q, "results": [], "matched": 0}
    like_any = f"%{q_norm}%"
    like_prefix = f"{q_norm}%"

    # Pull an over-sized candidate set so we can rank + trim. Cap each source
    # query so a hot-word query (matches thousands) doesn't scan the world.
    fetch_cap = min(limit * 6, 200)

    open_name_hits = (
        (
            await db.execute(
                select(CanonicalEntity)
                .where(CanonicalEntity.surface_mode == SurfaceMode.OPEN.value)
                .where(CanonicalEntity.canonical_name_normalized.ilike(like_any))
                .limit(fetch_cap)
            )
        )
        .scalars()
        .all()
    )

    open_alias_hits = (
        (
            await db.execute(
                select(CanonicalEntity)
                .join(EntityAlias, EntityAlias.canonical_id == CanonicalEntity.id)
                .where(CanonicalEntity.surface_mode == SurfaceMode.OPEN.value)
                .where(EntityAlias.surface_name_normalized.ilike(like_any))
                .limit(fetch_cap)
            )
        )
        .scalars()
        .all()
    )

    aliased_hits = (
        (
            await db.execute(
                select(CanonicalEntity)
                .where(CanonicalEntity.surface_mode == SurfaceMode.ALIAS.value)
                .where(CanonicalEntity.public_alias.ilike(like_any))
                .limit(fetch_cap)
            )
        )
        .scalars()
        .all()
    )

    def _tier_for(e: CanonicalEntity) -> int:
        """Compute the rank tier for an entity given the query."""
        if e.surface_mode == SurfaceMode.OPEN.value:
            name_norm = (e.canonical_name_normalized or "").lower()
            if name_norm == q_norm:
                return 5
            if name_norm.startswith(q_norm):
                return 4
            if q_norm in name_norm:
                return 3
            return 1  # not a canonical hit → must be an alias hit
        if e.surface_mode == SurfaceMode.ALIAS.value:
            return 1
        return 0

    # Fetch the alias-hit prefix/substring split cheaply (only for entities in
    # the open_alias_hits set — tier 2 vs tier 1). We only need per-entity a
    # boolean "any alias starts with q?".
    alias_hit_prefix: dict[str, bool] = {}
    if open_alias_hits:
        alias_ids = [e.id for e in open_alias_hits]
        prefix_matches = (
            (
                await db.execute(
                    select(EntityAlias.canonical_id)
                    .where(EntityAlias.canonical_id.in_(alias_ids))
                    .where(EntityAlias.surface_name_normalized.ilike(like_prefix))
                )
            )
            .scalars()
            .all()
        )
        alias_hit_prefix = {cid: True for cid in prefix_matches}

    def _final_tier(e: CanonicalEntity) -> int:
        """Combine canonical-name tier + alias-hit-tier (max wins)."""
        t = _tier_for(e)
        if e.id in {x.id for x in open_alias_hits}:
            alias_tier = 2 if alias_hit_prefix.get(e.id) else 1
            t = max(t, alias_tier)
        return t

    # Dedup by id.
    dedup: dict[str, CanonicalEntity] = {}
    for e in list(open_name_hits) + list(open_alias_hits) + list(aliased_hits):
        if _public_label(e) is None:
            continue
        dedup.setdefault(e.id, e)

    # Rank tier desc, then importance desc, then label asc.
    ranked: list[tuple[int, int, str, CanonicalEntity]] = []
    for e in dedup.values():
        tier = _final_tier(e)
        importance = await _entity_importance(db, e.id)
        label = _public_label(e) or ""
        ranked.append((tier, importance, label.lower(), e))
    ranked.sort(key=lambda t: (-t[0], -t[1], t[2]))

    out: list[dict] = []
    for tier, importance, _label, e in ranked[:limit]:
        out.append(
            {
                "id": e.id,
                "label": _public_label(e),
                "type": e.type,
                "surface_mode": e.surface_mode,
                "rank_tier": tier,
                "importance": importance,
            }
        )
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
