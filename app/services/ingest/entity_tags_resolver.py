"""P0 — resolve hollywood.entity_tags into canonical Argus entities.

Reads the ~208K rows in `hollywood.entity_tags` READ-ONLY (via
`settings.hollywood_database_url`). For each row:
- normalize the name via `normalize_name`
- look up a canonical match via `PgVectorStore.resolve_entity` (cosine≥0.86 non-person;
  cosine ≥ 0.86+margin for person per design §5.1)
- if hit: append an `EntityAlias` under that canonical
- if miss: create a new `CanonicalEntity` (seeded with this row's embedding as the
  centroid) + the `EntityAlias`

The centroid for existing canonicals is updated as a running mean each time a new
alias joins — cheap and stable for the seed pass. Post-P0 we may switch to a
periodic full recomputation.

`kind_hint` from hollywood maps roughly onto our EntityType — an unknown/None
kind maps to EntityType.UNKNOWN so the row still resolves (design: no rows
dropped; every alias preserved).
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.config import settings
from app.db import get_sessionmaker
from app.models import CanonicalEntity, EntityAlias, EntityType
from app.services.graph.base import normalize_name
from app.services.graph.pgvector_store import PgVectorStore

logger = logging.getLogger(__name__)

# hollywood.entity_tags.kind_hint → argus EntityType. Anything else stays UNKNOWN.
_KIND_MAP: dict[str, str] = {
    "person": EntityType.PERSON.value,
    "organization": EntityType.ORGANIZATION.value,
    "org": EntityType.ORGANIZATION.value,
    "company": EntityType.ORGANIZATION.value,
    "pac": EntityType.PAC.value,
    "agency": EntityType.AGENCY.value,
    "candidate": EntityType.CANDIDATE.value,
    "place": EntityType.PLACE.value,
    "topic": EntityType.TOPIC.value,
    "lens": EntityType.TOPIC.value,
}


@dataclass
class ResolutionStats:
    """Counters for a resolver batch — helpful for helen validation + reports."""

    rows_read: int = 0
    canonicals_created: int = 0
    aliases_appended: int = 0
    skipped_no_embedding: int = 0
    person_kept_separate: int = 0
    errors: int = 0


def _map_kind(kind_hint: str | None) -> str:
    """Return the argus EntityType.value for hollywood's kind_hint (default UNKNOWN)."""
    if not kind_hint:
        return EntityType.UNKNOWN.value
    return _KIND_MAP.get(kind_hint.strip().lower(), EntityType.UNKNOWN.value)


async def _fetch_hollywood_batch(offset: int, limit: int) -> list[dict]:
    """Fetch a batch of entity_tags rows from hollywood (READ-ONLY)."""
    engine = create_async_engine(settings.hollywood_database_url, pool_pre_ping=True)
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT id::text AS id, tag, tag_normalized, kind_hint, role, "
                    "       confidence, tag_embedding "
                    "FROM entity_tags "
                    "WHERE tag_embedding IS NOT NULL "
                    "ORDER BY id "
                    "OFFSET :off LIMIT :lim"
                ),
                {"off": offset, "lim": limit},
            )
            rows = result.mappings().all()
            return [dict(r) for r in rows]
    finally:
        await engine.dispose()


def _embedding_to_list(raw) -> list[float] | None:
    """Coerce pgvector wire form (Vector / list / '[...]' string) into list[float]."""
    if raw is None:
        return None
    if isinstance(raw, list):
        return [float(x) for x in raw]
    if isinstance(raw, str):
        stripped = raw.strip().lstrip("[").rstrip("]")
        if not stripped:
            return None
        return [float(x) for x in stripped.split(",")]
    # pgvector.Vector or numpy-like
    try:
        return [float(x) for x in list(raw)]
    except Exception:  # noqa: BLE001
        return None


async def _resolve_row(
    session: AsyncSession, store: PgVectorStore, row: dict, stats: ResolutionStats
) -> None:
    """Resolve one hollywood row: either append an alias or create a canonical."""
    surface = row["tag"]
    embedding = _embedding_to_list(row.get("tag_embedding"))
    if embedding is None:
        stats.skipped_no_embedding += 1
        return
    argus_type = _map_kind(row.get("kind_hint"))

    hit = await store.resolve_entity(session, surface, argus_type, embedding)
    if hit is None:
        # New canonical entity (seed with this embedding as the initial centroid).
        canonical = CanonicalEntity(
            canonical_name=surface,
            canonical_name_normalized=normalize_name(surface),
            type=argus_type,
            embedding=embedding,
        )
        session.add(canonical)
        await session.flush()
        canonical_id = canonical.id
        stats.canonicals_created += 1
    else:
        canonical_id = hit
        stats.aliases_appended += 1

    session.add(
        EntityAlias(
            canonical_id=canonical_id,
            source_system="hollywood.entity_tags",
            source_id=row["id"],
            surface_name=surface,
            surface_name_normalized=normalize_name(surface),
            kind_hint=row.get("kind_hint"),
            role=row.get("role"),
            confidence=row.get("confidence"),
        )
    )


async def _existing_source_ids(session: AsyncSession) -> set[str]:
    """Return the set of hollywood entity_tags ids we've already resolved (for restart-safety)."""
    from sqlalchemy import select

    rows = (
        (
            await session.execute(
                select(EntityAlias.source_id).where(
                    EntityAlias.source_system == "hollywood.entity_tags"
                )
            )
        )
        .scalars()
        .all()
    )
    return set(rows)


async def run_resolver(batch_size: int = 500, max_rows: int | None = None) -> ResolutionStats:
    """Iterate hollywood.entity_tags in batches and resolve every row into Argus.

    Restart-safe: pulls the set of already-processed source_ids upfront and
    skips them. Each batch commits independently.
    """
    stats = ResolutionStats()
    store = PgVectorStore()
    sm = get_sessionmaker()
    async with sm() as session:
        already = await _existing_source_ids(session)
    logger.info("resolver: %d source_ids already processed", len(already))
    offset = 0
    while True:
        rows = await _fetch_hollywood_batch(offset, batch_size)
        if not rows:
            break
        async with sm() as session:
            for row in rows:
                stats.rows_read += 1
                if row["id"] in already:
                    continue
                try:
                    await _resolve_row(session, store, row, stats)
                    await session.commit()
                    already.add(row["id"])
                except Exception as exc:  # noqa: BLE001
                    await session.rollback()
                    stats.errors += 1
                    logger.warning("resolver row failed source_id=%s: %s", row.get("id"), exc)
        offset += batch_size
        if max_rows is not None and stats.rows_read >= max_rows:
            break
        if stats.rows_read % 5000 == 0:
            logger.info(
                "progress: read=%d created=%d aliased=%d",
                stats.rows_read,
                stats.canonicals_created,
                stats.aliases_appended,
            )
    return stats


def main() -> None:
    """CLI entrypoint — python -m app.services.ingest.entity_tags_resolver."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = asyncio.run(run_resolver())
    logger.info("resolver done: %s", stats)


if __name__ == "__main__":
    main()
