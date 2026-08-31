"""FastAPI app for Argus — public, read-only navigator over the citation-cited graph."""

from __future__ import annotations

import logging
from pathlib import Path

from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_db
from app.models import CanonicalEntity, EntityAlias, SurfaceMode
from app.services.graph.base import CytoscapeGraph, empty_graph
from app.services.graph.pgvector_store import PgVectorStore
from app.services.read_gate import (
    is_published_entity,
    maybe_published_edge,
    maybe_published_entity,
    published_edge,
)


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

# CORS — /api/resolve + /api/resolve/batch + /api/search are consumed by
# the-dailies (https://tonyvigna.com apex) and the SPA itself. Allow the
# `*.tonyvigna.com` family; keep credentials off (Argus is public read-only).
app.add_middleware(
    CORSMiddleware,
    allow_origins=["https://tonyvigna.com", "https://www.tonyvigna.com"],
    allow_origin_regex=r"https://[a-zA-Z0-9-]+\.tonyvigna\.com",
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["content-type"],
    allow_credentials=False,
    max_age=3600,
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


async def _resolve_one(db: AsyncSession, tag: str) -> dict:
    """Core /api/resolve logic — reused by GET and by the batch POST.

    Helen T4 2026-07-17: when an argus canonical doesn't exist for the tag but
    hollywood.entity_tags has it as `kind_hint=event|concept`, we do an
    on-demand upsert into argus rather than returning "not an entity". This
    fixes the resolver-lag edge case where "civil war" (an event) hadn't yet
    been processed and was misreported as a topic.
    """
    from app.services.graph.base import normalize_name

    norm = normalize_name(tag)
    if not norm:
        return {"resolved": False, "reason": "not an entity (topic/theme)"}

    match = (
        await db.execute(
            select(CanonicalEntity)
            .join(EntityAlias, EntityAlias.canonical_id == CanonicalEntity.id)
            .where(EntityAlias.source_system == "hollywood.entity_tags")
            .where(EntityAlias.surface_name_normalized == norm)
            .limit(1)
        )
    ).scalar_one_or_none()
    if match is None:
        match = (
            await db.execute(
                select(CanonicalEntity)
                .where(CanonicalEntity.canonical_name_normalized == norm)
                .limit(1)
            )
        ).scalar_one_or_none()
    if match is None:
        match = (
            await db.execute(
                select(CanonicalEntity)
                .join(EntityAlias, EntityAlias.canonical_id == CanonicalEntity.id)
                .where(EntityAlias.surface_name_normalized == norm)
                .limit(1)
            )
        ).scalar_one_or_none()

    # T4 lag-fill: if argus has nothing yet, look upstream at hollywood.entity_tags
    # for an event/concept-typed match and materialize a canonical on demand.
    if match is None:
        match = await _lag_fill_from_hollywood(db, norm)

    if match is None:
        return {"resolved": False, "reason": "not an entity (topic/theme)"}
    label = _public_label(match)
    if label is None:
        return {"resolved": False, "reason": "not an entity (topic/theme)"}
    return {
        "resolved": True,
        "id": match.id,
        "type": match.type,
        "label": label,
        "surface_mode": match.surface_mode,
        "path": f"/entity/{match.id}",
    }


_LAG_FILL_KINDS = ("event", "concept")


async def _lag_fill_from_hollywood(db: AsyncSession, tag_normalized: str) -> CanonicalEntity | None:
    """On-demand upsert of an event/concept canonical from hollywood.entity_tags.

    Only fills entities the resolver would eventually create anyway — respecting
    the same `kind_hint → EntityType` mapping used by the batched resolver. Never
    materialises person/org/candidate types on demand (those need embedding
    resolution to avoid false-merges) — only event + concept, which are safe
    since they're free-text and don't collide identity-wise.
    """
    from sqlalchemy import create_engine, text
    from sqlalchemy.ext.asyncio import create_async_engine

    from app.config import settings
    from app.models import EntityType

    engine = create_async_engine(settings.hollywood_database_url, pool_pre_ping=True)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT id::text AS id, tag, kind_hint "
                    "FROM entity_tags "
                    "WHERE tag_normalized = :n AND kind_hint IN ('event','concept') "
                    "LIMIT 1"
                ),
                {"n": tag_normalized},
            )
            row = result.mappings().first()
    finally:
        await engine.dispose()
        del create_engine  # imported for symmetry with the resolver; not used here.

    if not row:
        return None

    surface = row["tag"]
    kind = (row["kind_hint"] or "").strip().lower()
    new_type = EntityType.EVENT.value if kind == "event" else EntityType.CONCEPT.value
    if new_type not in {EntityType.EVENT.value, EntityType.CONCEPT.value}:
        return None

    from app.services.graph.base import normalize_name

    ce = CanonicalEntity(
        canonical_name=surface,
        canonical_name_normalized=normalize_name(surface),
        type=new_type,
    )
    db.add(ce)
    await db.flush()
    db.add(
        EntityAlias(
            canonical_id=ce.id,
            source_system="hollywood.entity_tags",
            source_id=str(row["id"]),
            surface_name=surface,
            surface_name_normalized=normalize_name(surface),
            kind_hint=kind,
        )
    )
    await db.commit()
    return ce


@app.post("/api/resolve/batch")
async def resolve_batch(payload: dict, db: AsyncSession = Depends(get_db)) -> dict:
    """Batch tag resolution — one round-trip for the-dailies chit-strip rendering.

    Body: `{"tags": ["tag_normalized_1", ...]}` (deduped by caller ideally).
    Returns: `{"results": {"tag1": <resolve>, ...}}` with the same per-tag envelope
    as `/api/resolve`.

    Caps the batch at 500 tags to keep a single call bounded.
    """
    tags = payload.get("tags") or []
    if not isinstance(tags, list):
        raise HTTPException(status_code=422, detail="tags must be a list")
    tags = [t for t in tags if isinstance(t, str) and t.strip()][:500]
    out: dict[str, dict] = {}
    for tag in tags:
        if tag in out:
            continue
        out[tag] = await _resolve_one(db, tag)
    return {"results": out}


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
         `surface_name_normalized`. Highest fidelity — the tag came from the
         same source system.
      2. CanonicalEntity.canonical_name_normalized exact match.
      3. Any EntityAlias.surface_name_normalized exact match.
    """
    return await _resolve_one(db, tag)


async def _entity_importance(db: AsyncSession, entity_id: str) -> int:
    """Importance ~= edge count + citation count summed. Cheap proxy for node significance.

    RG2: staged edges MUST NOT inflate degree / citation counts / search ranking —
    otherwise a staged batch would bump entities up the search order pre-publish.
    """
    from sqlalchemy import func

    from app.models import CanonicalEdge, SourceCitation

    edge_count = (
        await db.execute(
            select(func.count(CanonicalEdge.id))
            .where(
                (CanonicalEdge.source_id == entity_id) | (CanonicalEdge.target_id == entity_id)
            )
            .where(published_edge())
        )
    ).scalar_one() or 0
    citation_count = (
        await db.execute(
            select(func.count(SourceCitation.id))
            .join(CanonicalEdge, CanonicalEdge.id == SourceCitation.edge_id)
            .where((CanonicalEdge.source_id == entity_id) | (CanonicalEdge.target_id == entity_id))
            .where(published_edge())
        )
    ).scalar_one() or 0
    return int(edge_count) + int(citation_count)


@app.get("/api/search")
async def search(
    q: str = Query(..., min_length=1, max_length=120),
    limit: int = Query(20, ge=1, le=50),
    include_staged: bool = Query(
        False,
        description="RG4 preview flag: include staged entities. IGNORED unless "
        "the request also carries a matching X-Argus-Service-Token header — the "
        "public read path can never opt in.",
    ),
    x_argus_service_token: str | None = Header(default=None),
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

    AF2 (2026-08-08) — disclosure-filer bump: entities that are the
    SOURCE (filer) of at least one edge in the OGE disclosure-relation
    set (holds_asset / income_from / held_position / owes / traded /
    party_to_agreement) get ``+2`` on their effective tier.

    Filer-only (source, not target) is intentional: a disclosure
    edge points from the person who filed the 278e/278-T to the
    asset/counterparty they disclosed. Bumping BOTH sides would put
    every wholly-owned asset (Trump Tower LLC, held stocks, etc.)
    ahead of the filer they belong to — which was the first-cut of
    AF2's failure mode. Source-side only lifts the disclosure
    AGGREGATOR to the top, where the whole portfolio is
    discoverable via a single click.

    Rationale from the discovery incident: "Donald J. Trump"
    (8 211 disclosure edges as filer, importance ~39 000) was
    tier-3 substring while "Trump" (concept, 0 disclosure, importance
    1 990) was tier-5 exact — so a user searching *trump* couldn't
    reach the disclosure-rich node. Bumping the filer by two tiers
    ties or beats the exact-match stubs, then importance decides
    the top of the ranking. Never touches ``surface_mode``.

    Within a tier (post-bump): rank by node importance
    (edge_count + citation_count from the RG2-published-edge filter)
    desc, then alphabetical by label for a stable ordering.

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

    # RG4: preview flag is honored only when the request also carries a
    # matching service token. Public callers cannot opt in — the flag is
    # silently downgraded to False.
    preview = include_staged and _preview_ok(x_argus_service_token)
    pub_entity = maybe_published_entity(preview)

    # Pull an over-sized candidate set so we can rank + trim. Cap each source
    # query so a hot-word query (matches thousands) doesn't scan the world.
    fetch_cap = min(limit * 6, 200)

    # RG2: all three candidate queries share the same published-state gate
    # (relaxed to always-true when RG4 preview is authorized).
    open_name_hits = (
        (
            await db.execute(
                select(CanonicalEntity)
                .where(CanonicalEntity.surface_mode == SurfaceMode.OPEN.value)
                .where(pub_entity)
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
                .where(pub_entity)
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
                .where(pub_entity)
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

    # AF2 (2026-08-08): one bulk query to figure out which candidates
    # are FILERS of disclosure edges — feeds a +2 tier bump so the
    # disclosure-rich filer outranks bare stubs (see docstring for
    # rationale).
    #
    # Filer-only (source, not target): a disclosure edge points from
    # the person who filed the 278e/278-T to the asset/counterparty
    # they disclosed. If we bumped BOTH sides, every wholly-owned
    # asset entity (Trump Tower LLC, Trump Media, an S&P 500 stock)
    # would also boost and outrank the filer they belong to — which
    # is what the first-cut of AF2 did wrong. Bumping the SOURCE
    # side only puts the disclosure aggregator (the filer) at the
    # top, where the whole portfolio is discoverable.
    disclosure_carriers: set[str] = set()
    if dedup:
        from app.models import CanonicalEdge, EdgeRelation

        candidate_ids = list(dedup.keys())
        _DISCLOSURE_RELATIONS = (
            EdgeRelation.HOLDS_ASSET.value,
            EdgeRelation.INCOME_FROM.value,
            EdgeRelation.HELD_POSITION.value,
            EdgeRelation.OWES.value,
            EdgeRelation.TRADED.value,
            EdgeRelation.PARTY_TO_AGREEMENT.value,
        )
        rows = (
            await db.execute(
                select(CanonicalEdge.source_id)
                .where(CanonicalEdge.source_id.in_(candidate_ids))
                .where(CanonicalEdge.relation.in_(_DISCLOSURE_RELATIONS))
                .where(published_edge())
                .distinct()
            )
        ).all()
        for (src,) in rows:
            disclosure_carriers.add(src)

    # Rank effective_tier desc, then importance desc, then label asc.
    ranked: list[tuple[int, int, int, str, CanonicalEntity]] = []
    for e in dedup.values():
        tier = _final_tier(e)
        bump = 2 if e.id in disclosure_carriers else 0
        importance = await _entity_importance(db, e.id)
        label = _public_label(e) or ""
        ranked.append((tier + bump, tier, importance, label.lower(), e))
    ranked.sort(key=lambda t: (-t[0], -t[2], t[3]))

    out: list[dict] = []
    for effective_tier, base_tier, importance, _label, e in ranked[:limit]:
        row = {
            "id": e.id,
            "label": _public_label(e),
            "type": e.type,
            "surface_mode": e.surface_mode,
            "rank_tier": effective_tier,
            "importance": importance,
        }
        # Only surface the boost delta when it applies — keeps the
        # response schema backward-compat when nothing was boosted.
        if effective_tier != base_tier:
            row["base_tier"] = base_tier
            row["disclosure_boosted"] = True
        out.append(row)
    return {"q": q, "matched": len(out), "results": out}


@app.get("/api/entities/{canonical_id}")
async def get_entity(
    canonical_id: str,
    include_staged: bool = Query(
        False,
        description="RG4 preview flag: include staged entity + staged edges. "
        "IGNORED unless the request also carries a matching X-Argus-Service-Token "
        "header — the public read path can never opt in.",
    ),
    x_argus_service_token: str | None = Header(default=None),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """Return the canonical entity's dossier — sourced articles + connections-by-relation.

    Helen 2026-07-17 dossier: instead of the bare id/label/type/surface_mode, the
    left panel now gets a real dossier back:

      * `label`, `type`, `surface_mode`, `prominence` (degree + article count).
      * `articles`: the deduplicated Tony Times permalinks that support edges
        touching this entity (SourceCitation.kind = article_permalink), each
        with its `citation_ref` (permalink slug or kind:id fallback).
      * `connections`: dict keyed by relation → sorted list of neighbours with
        `id`, `label`, `type`, `citation_count`. Neighbours with `label = None`
        (surface_mode = suppress) are elided. Aliased neighbours surface their
        `public_alias`, never the real name.
    """
    from sqlalchemy import func

    from app.models import CanonicalEdge, SourceCitation

    # RG4: preview flag is silently downgraded to False for callers
    # without a matching service-token header. Only privileged (in-pod
    # workflow) callers can preview staged content.
    preview = include_staged and _preview_ok(x_argus_service_token)

    ent = (
        await db.execute(select(CanonicalEntity).where(CanonicalEntity.id == canonical_id))
    ).scalar_one_or_none()
    if ent is None:
        raise HTTPException(status_code=404, detail="entity not found")
    # RG2: staged entity itself is dark to the public — 404 with the same
    # shape as the surface_mode gates below (never leak that it exists).
    # RG4 preview may skip this gate.
    if not preview and not is_published_entity(ent):
        raise HTTPException(status_code=404, detail="entity not found")
    label = _public_label(ent)
    if label is None:
        raise HTTPException(status_code=404, detail="entity not surfaceable")

    # Helen 2026-07-17 privacy regression fix: for surface_mode != open we NEVER
    # return article URLs. A permalink to a source article is a deanonymization
    # channel — the article body carries the real name a private-person canonical
    # is aliased to hide. Alias-mode dossiers return only aggregate counts +
    # non-naming connection topology.
    is_open = ent.surface_mode == SurfaceMode.OPEN.value

    # RG2: even for a published node, staged edges (mid-batch bulk ingest)
    # must not surface until the batch is published (relaxed under
    # RG4 preview).
    edges = (
        (
            await db.execute(
                select(CanonicalEdge)
                .where(
                    (CanonicalEdge.source_id == canonical_id)
                    | (CanonicalEdge.target_id == canonical_id)
                )
                .where(maybe_published_edge(preview))
            )
        )
        .scalars()
        .all()
    )

    # Articles supporting any incident edge, deduped by citation_url.
    # SUPPRESS is already 404'd above; ALIAS returns article COUNTS only (no URLs).
    articles: list[dict] = []
    article_count = 0
    if edges:
        edge_ids = [e.id for e in edges]
        cites = (
            (
                await db.execute(
                    select(SourceCitation)
                    .where(SourceCitation.edge_id.in_(edge_ids))
                    .where(SourceCitation.kind == "article_permalink")
                )
            )
            .scalars()
            .all()
        )
        seen: set[str] = set()
        for c in cites:
            if c.citation_url in seen:
                continue
            seen.add(c.citation_url)
            article_count += 1
            if is_open:
                articles.append(
                    {
                        "url": c.citation_url,
                        "ref": c.citation_ref,
                    }
                )

    # Connections-by-relation. For each neighbour we compute an edge-scoped
    # citation_count so callers can rank most-sourced first.
    #
    # D4 (2026-08-06) — disclosure surfacing. When an edge carries an OGE
    # citation (kind ∈ {oge_278e, oge_278t}), inline its receipts on the
    # connection so the dossier UI + Cassandra + Ask-Argus MCP can render
    # a clickable page anchor (…PDF#page=N). For TRADED edges we further
    # expand each receipt with its parsed transaction (type/date/
    # amount_band) from the ledger row — that's what makes the trades
    # section useful vs a raw citation count. Receipts + edge_metadata
    # are ONLY surfaced when the caller entity ``ent`` is OPEN (belt-
    # and-suspenders to §P7's rule that receipts can de-anonymize).
    connections: dict[str, list[dict]] = {}
    if edges:
        # Bulk-pull neighbour entities + per-edge citation counts.
        neighbour_ids = list(
            {(e.source_id if e.target_id == canonical_id else e.target_id) for e in edges}
        )
        neighbours = {
            n.id: n
            for n in (
                await db.execute(
                    select(CanonicalEntity).where(CanonicalEntity.id.in_(neighbour_ids))
                )
            )
            .scalars()
            .all()
        }
        edge_ids_all = [e.id for e in edges]
        edge_cite_counts = dict(
            (
                await db.execute(
                    select(SourceCitation.edge_id, func.count(SourceCitation.id))
                    .where(SourceCitation.edge_id.in_(edge_ids_all))
                    .group_by(SourceCitation.edge_id)
                )
            ).all()
        )

        # Bulk-pull the OGE citations for the caller's edges (D4). One
        # round-trip per dossier — the payload is O(citations) per
        # edge; the annual has a small handful per edge (typical ~2).
        oge_receipts: dict[str, list[dict]] = {}
        if is_open:
            oge_cites = (
                (
                    await db.execute(
                        select(SourceCitation)
                        .where(SourceCitation.edge_id.in_(edge_ids_all))
                        .where(SourceCitation.kind.in_(("oge_278e", "oge_278t")))
                    )
                )
                .scalars()
                .all()
            )
            row_ids_needed = [c.disclosure_row_id for c in oge_cites if c.disclosure_row_id]
            # Pull the disclosure_rows once — traded expansion below reads
            # `parsed` (transaction_type / trade_date / amount_band).
            row_lookup: dict[str, dict] = {}
            if row_ids_needed:
                from app.models import DisclosureRow

                dr_rows = (
                    (
                        await db.execute(
                            select(DisclosureRow).where(DisclosureRow.id.in_(row_ids_needed))
                        )
                    )
                    .scalars()
                    .all()
                )
                for dr in dr_rows:
                    row_lookup[dr.id] = dr.parsed or {}
            for c in oge_cites:
                oge_receipts.setdefault(c.edge_id, []).append(
                    {
                        "kind": c.kind,
                        "url": c.citation_url,
                        "ref": c.citation_ref,
                        "page": c.page,
                        "row": row_lookup.get(c.disclosure_row_id) if c.disclosure_row_id else None,
                    }
                )

        for e in edges:
            neighbour_id = e.source_id if e.target_id == canonical_id else e.target_id
            neighbour = neighbours.get(neighbour_id)
            if neighbour is None:
                continue
            nlabel = _public_label(neighbour)
            if nlabel is None:
                # Suppressed neighbour — do not surface even the connection.
                continue
            # Receipts stay open-gated on BOTH ends: the caller ``ent``
            # must be open AND the neighbour must be open. A creditor
            # who is an aliased private person still gets a
            # non-identifying connection row, but NO OGE receipt link
            # (which would otherwise resolve the alias by page).
            neighbour_open = neighbour.surface_mode == SurfaceMode.OPEN.value
            receipts_for_edge = (
                oge_receipts.get(e.id, []) if (is_open and neighbour_open) else []
            )
            row = {
                "id": neighbour.id,
                "label": nlabel,
                "type": neighbour.type,
                "surface_mode": neighbour.surface_mode,
                "citation_count": int(edge_cite_counts.get(e.id, 0)),
                "weight": e.weight,
            }
            if is_open and neighbour_open:
                # edge_metadata carries value_band / band_low / band_high /
                # amount_band / income_type / account_group / eif / part
                # — the substance the disclosure UI renders per row.
                # Same open-on-both-ends gate as receipts.
                row["edge_metadata"] = e.edge_metadata or {}
            if receipts_for_edge:
                # Sort receipts by page ascending for a stable display.
                receipts_for_edge.sort(key=lambda r: (r.get("page") or 0, r.get("ref") or ""))
                row["receipts"] = [
                    {"kind": r["kind"], "url": r["url"], "ref": r["ref"], "page": r["page"]}
                    for r in receipts_for_edge
                ]
                if e.relation == "traded":
                    # Per-transaction expansion — type/date/amount_band
                    # from the ledger row (already stored on the citation
                    # by D3).
                    row["transactions"] = [
                        {
                            "type": (r.get("row") or {}).get("transaction_type"),
                            "date": (r.get("row") or {}).get("trade_date"),
                            "amount_band": (r.get("row") or {}).get("amount_band"),
                            "page": r["page"],
                            "url": r["url"],
                        }
                        for r in receipts_for_edge
                        if r.get("row")
                    ]
            connections.setdefault(e.relation, []).append(row)
        for rel in connections:
            connections[rel].sort(
                key=lambda x: (-x["citation_count"], -(x["weight"] or 0), x["label"].lower())
            )

    aircraft = await _published_aircraft_for(db, ent.id)
    vessels = await _published_vessels_for(db, ent.id)

    return {
        "id": ent.id,
        "label": label,
        "type": ent.type,
        "surface_mode": ent.surface_mode,
        "prominence": {
            "degree": len(edges),
            "articles": article_count,
        },
        # ALIAS-mode dossiers return an EMPTY articles list — article urls
        # would deanonymize the private person. The count still surfaces via
        # `prominence.articles`.
        "articles": articles,
        "connections": connections,
        # P3.0 — Assets → Aircraft. Published-only; empty until P3.2
        # promotes anything. Note this section does NOT honour
        # ``include_staged``: the preview flag exists for canonical
        # content review, and the aircraft surface is the de-anon area
        # that leaked on 2026-08-21. There is no code path, token or
        # query parameter that reveals an unpublished aircraft.
        "aircraft": aircraft,
        # Assets → Vessels. Published-only; empty until a Tony-approved
        # publish. Like the aircraft section it deliberately does NOT
        # honour ``include_staged`` — that flag previews canonical
        # content, and vessel owner rows carry names and addresses.
        "vessels": vessels,
    }


#: The ONLY vessel columns any public read path may emit. Owner name and
#: the owner address columns are absent by construction — the query
#: selects this allowlist rather than the ORM row, so a column added to
#: the model later cannot leak by being picked up implicitly.
_VESSEL_PUBLIC_COLUMNS = ("vessel_name", "imo_number", "flag")


async def _published_vessels_for(db: AsyncSession, canonical_id: str) -> list[dict]:
    """Published vessels owned by this entity. Zero owner PII.

    Both gates AND on BOTH the edge and the vessel row: promoting one
    without the other surfaces nothing, so a half-finished publish fails
    closed rather than half-open.
    """
    from app.models import Vessel, VesselOwnershipEdge
    from app.services.read_gate import published_vessel, published_vessel_edge

    rows = (
        await db.execute(
            select(
                Vessel.vessel_name,
                Vessel.imo_number,
                Vessel.flag,
                VesselOwnershipEdge.ofac_relation,
                VesselOwnershipEdge.source_url,
                VesselOwnershipEdge.source_sha256,
            )
            .select_from(VesselOwnershipEdge)
            .join(Vessel, Vessel.id == VesselOwnershipEdge.vessel_id)
            .where(VesselOwnershipEdge.canonical_id == canonical_id)
            .where(published_vessel_edge())
            .where(published_vessel())
            .order_by(Vessel.vessel_name)
        )
    ).all()
    return [
        {
            "vessel_name": name,
            "imo_number": imo,
            "flag": flag,
            "ofac_relation": rel,
            "citation": {
                "source": "OFAC Specially Designated Nationals list",
                "url": url,
                "sha256": sha,
                "record_key": imo or name,
            },
        }
        for name, imo, flag, rel, url, sha in rows
    ]


#: The ONLY aircraft columns any public read path may emit. Street,
#: street2, city, state, zip_code, county, registrant_name and
#: other_names are absent by construction — the query selects this
#: allowlist rather than the ORM row, so a future column cannot leak by
#: being added to the model. A test asserts the PII columns stay out.
_AIRCRAFT_PUBLIC_COLUMNS = ("n_number", "make_model", "year_mfr")


async def _published_aircraft_for(db: AsyncSession, canonical_id: str) -> list[dict]:
    """Published aircraft registered to this entity. Zero PII.

    Both gates AND on BOTH the edge and the aircraft row: an edge is
    only followed if it is published and non-suppressed, and the
    aircraft it points at must independently be published and
    non-suppressed. Promoting one without the other surfaces nothing —
    that is deliberate, so a half-finished promotion fails closed.

    Every row carries its FAA citation (dataset snapshot sha256 + the
    registry record key), which is what makes the claim checkable.
    """
    from app.models import Aircraft, AircraftReference, AircraftRegistrationEdge
    from app.services.read_gate import published_aircraft, published_registration_edge

    rows = (
        await db.execute(
            select(
                Aircraft.n_number,
                Aircraft.year_mfr,
                AircraftReference.mfr,
                AircraftReference.model,
                AircraftRegistrationEdge.source_url,
                AircraftRegistrationEdge.source_sha256,
                AircraftRegistrationEdge.match_score,
            )
            .select_from(AircraftRegistrationEdge)
            .join(Aircraft, Aircraft.id == AircraftRegistrationEdge.aircraft_id)
            .outerjoin(AircraftReference, AircraftReference.code == Aircraft.mfr_mdl_code)
            .where(AircraftRegistrationEdge.canonical_id == canonical_id)
            .where(published_registration_edge())
            .where(published_aircraft())
            .order_by(Aircraft.n_number)
        )
    ).all()

    out: list[dict] = []
    for n_number, year_mfr, mfr, model, src_url, src_sha, score in rows:
        make_model = " ".join(p for p in ((mfr or "").strip(), (model or "").strip()) if p)
        out.append(
            {
                "n_number": f"N{n_number}",
                "make_model": make_model or None,
                "year_mfr": year_mfr,
                "citation": {
                    "source": "FAA Releasable Aircraft Database",
                    "url": src_url,
                    "sha256": src_sha,
                    "record_key": f"N{n_number}",
                },
                "match_score": score,
            }
        )
    return out


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
    # RG2: subgraph of a staged entity = empty (same dark-until-published
    # contract as the dossier 404).
    if not is_published_entity(ent):
        return empty_graph()
    store = PgVectorStore()
    # RG2: the store honors the read-gate on its edge pull; staged edges +
    # staged nodes never appear in the returned graph.
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


@app.get("/api/flow/model2")
async def flow_model2(
    bill: str = Query(..., description="Bill slug — congress.bill alias (e.g. 119-hr-1) or short-name (OBBB)"),
    yes_voter_party: str | None = Query(
        "Republican",
        description="Party filter on YES-voters (None = any). Republican / Democrat / Independent.",
    ),
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """P5.6 Model 2 (BENEFICIARY) — cited $ attribution from a bill's
    YES-voters + their donors + those donors' contracts inside the
    bill's curated funding scope.

    Framing (spec §5): attribution to funding scope, NOT causation.
    """
    from app.services.flow_query import model2_flow

    summary = await model2_flow(
        db, bill_slug=bill,
        yes_voter_party_filter=yes_voter_party,
        limit=limit,
    )
    if summary is None:
        raise HTTPException(status_code=404, detail=f"bill {bill!r} not found")
    return {
        "bill_alias": summary.bill_alias,
        "bill_label": summary.bill_label,
        "yes_voter_party_filter": summary.yes_voter_party_filter,
        "n_yes_voters": summary.n_yes_voters,
        "funding_scope_note": summary.funding_scope_note,
        "n_beneficiaries": summary.n_beneficiaries,
        "total_contrib_to_yes_voters_usd": summary.total_contrib,
        "total_contract_in_scope_usd": summary.total_contract,
        "framing": "cited attribution to funding scope, not causation (spec §5)",
        "rows": [
            {
                "entity_id": r.entity_id,
                "entity_label": r.entity_label,
                "contrib_usd": r.contrib_to_yes_voters,
                "contract_usd": r.contract_in_scope,
            }
            for r in summary.rows
        ],
    }


@app.get("/api/flow/model1")
async def flow_model1(
    party: str = Query(..., description="Republican / Democratic / Independent"),
    agency_relation: str = Query(
        "holds_contract",
        description="Edge relation to sum for the 'contract $' side (default: holds_contract)",
    ),
    limit: int = Query(50, ge=1, le=500),
    db: AsyncSession = Depends(get_db),
) -> dict:
    """P5.3 Model 1 (INFLUENCE) flow query — cited $ correlation
    between contributors to a party + their federal-contract receipts.

    Framing (spec §5): correlation, NOT causation. Every $ in this
    response is a sum of edge weights, each backed by a citation.
    """
    from app.services.flow_query import model1_flow

    summary = await model1_flow(
        db, party=party, agency_relation=agency_relation, limit=limit
    )
    return {
        "party": summary.party,
        "agency_relation": agency_relation,
        "n_contributors": summary.n_contributors,
        "total_contrib_usd": summary.total_contrib,
        "total_contract_usd": summary.total_contract,
        "framing": "cited $ correlation, not causation (spec §5)",
        "rows": [
            {
                "entity_id": r.entity_id,
                "entity_label": r.entity_label,
                "contrib_usd": r.contrib_total,
                "contract_usd": r.contract_total,
                # E1 award-grade citations (Tony directive 2026-08-12):
                # per-entity top-N SourceCitation refs so downstream
                # consumers (hollywood_gen.agents.blog.argus_research)
                # can surface real usaspending/fec URLs alongside the
                # entity deep-link. Additive + back-compat: existing
                # consumers that ignore these keys see no change in
                # behavior. Empty lists for entities with no citations.
                "top_contract_citations": [
                    {"kind": c.kind, "url": c.url, "ref": c.ref}
                    for c in r.top_contract_citations
                ],
                "top_contribution_citations": [
                    {"kind": c.kind, "url": c.url, "ref": c.ref}
                    for c in r.top_contribution_citations
                ],
                # BROADEN-LAND (Tony 2026-08-12): cited-floor framing
                # support. contrib_usd (above) is PARTY-CLASSIFIED —
                # the floor Cassandra cites. contrib_usd_captured is
                # the total-captured (unclassified + classified) so
                # the block can say "at least $X classified of $Y
                # total captured". has_corporate_pac gates out
                # individual-only attribution (Palantir via Thiel).
                "contrib_usd_captured": r.contrib_total_captured,
                "has_corporate_pac": r.has_corporate_pac,
            }
            for r in summary.rows
        ],
    }


# ─── RG4 (2026-08-07) — admin control surface ─────────────────────────
#
# Publish + unpublish a bulk-disclosure batch atomically. Gated by
# ``X-Argus-Service-Token`` (env-injected). PRECONDITION on publish:
# every entity in the batch must carry a ``scrutiny_decision`` row —
# publishing before privacy has run is refused with 409. See design
# doc: ``helen-k3s/docs/argus-read-gate-hardening-design.md``.


logger = logging.getLogger(__name__)


def _preview_ok(token: str | None) -> bool:
    """RG4: does this request qualify for the ``include_staged=1`` preview?

    True ONLY when the server was configured with a token AND the caller
    presented a matching header. An unset server token (empty string)
    means: no one gets preview, regardless of what header they send —
    fail-closed so a misconfigured cluster never accidentally opens the
    preview to the public API.
    """
    if not settings.argus_service_token:
        return False
    return token == settings.argus_service_token


async def require_service_token(
    x_argus_service_token: str | None = Header(default=None),
) -> str:
    """FastAPI dependency: reject with 401 when the caller has NOT
    presented a matching ``X-Argus-Service-Token`` header (or the
    server was never configured with one)."""
    if not _preview_ok(x_argus_service_token):
        raise HTTPException(status_code=401, detail="unauthorized")
    return x_argus_service_token or ""


async def _batch_entities_missing_scrutiny(
    db: AsyncSession, batch_id: str
) -> list[tuple[str, str]]:
    """Return ``[(canonical_id, canonical_name), ...]`` for every entity
    in the given batch that has NO row in ``scrutiny_decisions``.

    The publish precondition (spec §RG4): "cannot go live before
    privacy has run." Any name-carrying entity that skipped scrutiny
    is a potential fail-open leak, so publish is refused with 409
    until every batch entity is decided.
    """
    from sqlalchemy import text

    rows = (
        await db.execute(
            text(
                "SELECT ce.id, ce.canonical_name FROM canonical_entities ce "
                "LEFT JOIN scrutiny_decisions sd ON sd.canonical_id = ce.id "
                "WHERE ce.batch_id = :b AND sd.id IS NULL "
                "ORDER BY ce.canonical_name"
            ),
            {"b": batch_id},
        )
    ).all()
    return [(r[0], r[1]) for r in rows]


@app.post("/api/admin/batches/{batch_id}/publish")
async def admin_publish_batch(
    batch_id: str,
    db: AsyncSession = Depends(get_db),
    _token: str = Depends(require_service_token),
) -> dict:
    """Publish every ``staged`` entity + edge in ``batch_id`` atomically.

    Refuses 409 when any batch entity lacks a ``scrutiny_decision`` row
    (with the list of blocking canonicals in the detail payload — the
    admin caller can look at what's outstanding). Idempotent: publishing
    an already-published batch flips nothing but still returns counts of
    zero (edges_published + entities_published) so the caller can tell
    the batch was seen but no work was needed.
    """
    from sqlalchemy import text

    missing = await _batch_entities_missing_scrutiny(db, batch_id)
    if missing:
        # Leak-safe: show only IDs + counts, not real names in the
        # error body (a suppressed person's name in a 409 body defeats
        # the whole privacy stack).
        raise HTTPException(
            status_code=409,
            detail={
                "reason": "scrutiny_incomplete",
                "batch_id": batch_id,
                "missing_scrutiny_count": len(missing),
                "missing_scrutiny_ids": [m[0] for m in missing[:20]],
                "message": (
                    f"{len(missing)} entities in batch {batch_id!r} lack a "
                    "scrutiny_decision — run scrutiny before publish."
                ),
            },
        )

    edge_result = await db.execute(
        text(
            "UPDATE canonical_edges SET publication_state='published' "
            "WHERE batch_id = :b AND publication_state='staged'"
        ),
        {"b": batch_id},
    )
    entity_result = await db.execute(
        text(
            "UPDATE canonical_entities SET publication_state='published' "
            "WHERE batch_id = :b AND publication_state='staged'"
        ),
        {"b": batch_id},
    )
    await db.commit()
    edges_published = edge_result.rowcount or 0
    entities_published = entity_result.rowcount or 0
    logger.info(
        "RG4 admin_publish_batch batch_id=%s edges=%d entities=%d",
        batch_id, edges_published, entities_published,
    )
    return {
        "batch_id": batch_id,
        "edges_published": edges_published,
        "entities_published": entities_published,
    }


@app.post("/api/admin/batches/{batch_id}/unpublish")
async def admin_unpublish_batch(
    batch_id: str,
    db: AsyncSession = Depends(get_db),
    _token: str = Depends(require_service_token),
) -> dict:
    """Instant kill-switch: flip every published row in ``batch_id`` back
    to ``staged``. No scrutiny precondition (unpublishing is a safety op —
    always allowed to hide, even without scrutiny state). Idempotent.
    """
    from sqlalchemy import text

    edge_result = await db.execute(
        text(
            "UPDATE canonical_edges SET publication_state='staged' "
            "WHERE batch_id = :b AND publication_state='published'"
        ),
        {"b": batch_id},
    )
    entity_result = await db.execute(
        text(
            "UPDATE canonical_entities SET publication_state='staged' "
            "WHERE batch_id = :b AND publication_state='published'"
        ),
        {"b": batch_id},
    )
    await db.commit()
    edges_unpublished = edge_result.rowcount or 0
    entities_unpublished = entity_result.rowcount or 0
    logger.info(
        "RG4 admin_unpublish_batch batch_id=%s edges=%d entities=%d",
        batch_id, edges_unpublished, entities_unpublished,
    )
    return {
        "batch_id": batch_id,
        "edges_unpublished": edges_unpublished,
        "entities_unpublished": entities_unpublished,
    }
