"""AF1 (2026-08-08) — heal dead ``holo://news_item/<uuid>`` citation URLs.

news_cooccurrence stamps ``citation_url = holo://news_item/<uuid>`` when
the news_item has no ``permalink_slug`` at co-occurrence-ingest time.
When the slug lands (via a subsequent hollywood publish), the citation
row stays frozen at the ``holo://`` fallback — a public-facing dead
link.

This healer resolves those citations to their real ``/s/<slug>``
permalinks. It runs one-shot for backfill AND on every scheduled sweep,
so the same timing gap between co-occurrence-ingest and permalink-mint
self-heals as soon as the slug is available.

Design:

  * **Idempotent** — safe to run any number of times. A citation whose
    URL is already ``https://…/s/<slug>`` is not scanned; only ``holo://
    news_item/<uuid>`` rows are candidates. A candidate whose slug is
    still missing on the hollywood side is LEFT alone (stays holo://)
    so the next sweep picks it up.
  * **UUID-preserving** — the ``citation_url``'s trailing UUID is what
    we look up. ``citation_ref`` also carries the UUID (set to
    ``news_item:<uuid>`` at ingest time in ``news_cooccurrence``) as a
    belt-and-suspenders second link should the URL ever be reshuffled
    in a future change.
  * **Per-batch** — the hollywood-side lookup is bulked (one query per
    batch of ids) so a re-heal of 6k+ candidates is a couple of DB
    round-trips, not one per row.

CLI:
    python -m app.services.ingest.heal_holo_citations
"""

from __future__ import annotations

import asyncio
import logging
import re
from dataclasses import dataclass

from sqlalchemy import bindparam, text
from sqlalchemy.ext.asyncio import create_async_engine

from app.config import settings
from app.db import get_sessionmaker
from app.models import SourceCitation

logger = logging.getLogger(__name__)


_HOLO_UUID_RE = re.compile(
    r"^holo://news_item/([0-9a-fA-F-]{36})$"
)
_BATCH_SIZE = 500


@dataclass
class HealStats:
    """Per-run counters — one log line per invocation."""

    candidates: int = 0             # rows matching holo://news_item/<uuid>
    lookups: int = 0                # distinct uuids we queried hollywood for
    slugged: int = 0                # of those lookups, how many returned a slug
    updated: int = 0                # citation rows we actually rewrote
    still_dangling: int = 0         # candidates whose slug isn't in hollywood yet
    errors: int = 0


async def heal_holo_citations() -> HealStats:
    """Rewrite ``holo://news_item/<uuid>`` citations to real permalinks.

    Fail-safe: any per-batch exception is logged + counted; the pass
    continues so a partial failure heals what it can.
    """
    stats = HealStats()
    sm = get_sessionmaker()

    # 1. Collect candidates. One SELECT — the LIKE is fine at 6k+ scale.
    async with sm() as session:
        rows = (
            await session.execute(
                text(
                    "SELECT id, citation_url "
                    "FROM source_citations "
                    "WHERE citation_url LIKE 'holo://news_item/%'"
                )
            )
        ).all()

    stats.candidates = len(rows)
    if not rows:
        logger.info("heal_holo_citations: no candidates")
        return stats

    # 2. Extract UUIDs (dedup — multiple citations may point at the
    #    same artifact).
    id_to_uuid: dict[str, str] = {}
    for r in rows:
        m = _HOLO_UUID_RE.match(r.citation_url or "")
        if m:
            id_to_uuid[r.id] = m.group(1)
    uuids = sorted(set(id_to_uuid.values()))
    stats.lookups = len(uuids)

    # 3. Bulk-lookup slugs from hollywood.
    slug_map = await _slug_map(uuids)
    stats.slugged = len(slug_map)

    # 4. Rewrite in batches.
    async with sm() as session:
        buf: list[dict] = []
        for cite_id, uuid in id_to_uuid.items():
            slug = slug_map.get(uuid)
            if not slug:
                stats.still_dangling += 1
                continue
            buf.append(
                {"id": cite_id, "url": f"https://tonyvigna.com/s/{slug}"}
            )
            if len(buf) >= _BATCH_SIZE:
                await _apply_batch(session, buf, stats)
                buf.clear()
        if buf:
            await _apply_batch(session, buf, stats)
        try:
            await session.commit()
        except Exception as exc:  # noqa: BLE001
            await session.rollback()
            stats.errors += 1
            logger.exception("heal_holo_citations commit failed: %s", exc)

    logger.info(
        "heal_holo_citations done: candidates=%d lookups=%d slugged=%d "
        "updated=%d still_dangling=%d errors=%d",
        stats.candidates, stats.lookups, stats.slugged,
        stats.updated, stats.still_dangling, stats.errors,
    )
    return stats


async def _slug_map(uuids: list[str]) -> dict[str, str]:
    """UUID → permalink_slug from hollywood.news_items. UUIDs without a
    slug are omitted from the returned dict (caller treats as
    still-dangling)."""
    if not uuids:
        return {}
    engine = create_async_engine(
        settings.hollywood_database_url, pool_pre_ping=True
    )
    try:
        async with engine.connect() as conn:
            result = await conn.execute(
                text(
                    "SELECT id::text AS id, permalink_slug "
                    "FROM news_items "
                    "WHERE id::text = ANY(:ids) "
                    "AND permalink_slug IS NOT NULL"
                ),
                {"ids": uuids},
            )
            return {row.id: row.permalink_slug for row in result.mappings().all()}
    finally:
        await engine.dispose()


async def _apply_batch(session, batch: list[dict], stats: HealStats) -> None:
    """UPDATE one batch of citation rows to their new URL."""
    if not batch:
        return
    try:
        # Executemany-style bulk UPDATE — one statement, N parameter sets.
        await session.execute(
            text(
                "UPDATE source_citations SET citation_url = :url "
                "WHERE id = :id"
            ),
            batch,
        )
        stats.updated += len(batch)
    except Exception as exc:  # noqa: BLE001
        stats.errors += 1
        logger.exception("heal_holo_citations batch failed: %s", exc)


def main() -> None:
    """``python -m app.services.ingest.heal_holo_citations``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    stats = asyncio.run(heal_holo_citations())
    logger.info("heal_holo_citations final: %s", stats)


if __name__ == "__main__":  # pragma: no cover
    main()


# Silence ruff unused-import — bindparam is exported for future
# schema-hardening (e.g., ARRAY of ids) even if not used inline.
_ = bindparam, SourceCitation
