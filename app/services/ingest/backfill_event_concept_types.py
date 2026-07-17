"""One-shot backfill: retype existing UNKNOWN canonicals whose aliases say event/concept.

Before helen T2 2026-07-17 the resolver mapped hollywood.entity_tags.kind_hint=
'event'|'concept' to `unknown` — 71K aliases were silently mistyped. This script
re-classifies those canonicals in place based on the majority kind_hint across
their aliases. Idempotent — running it twice does nothing after the first pass.
"""

from __future__ import annotations

import asyncio
import logging
from collections import Counter

from sqlalchemy import select

from app.db import get_sessionmaker
from app.models import CanonicalEntity, EntityAlias, EntityType

logger = logging.getLogger(__name__)


async def run_backfill() -> dict[str, int]:
    """Retype `type=unknown` canonicals whose alias kind_hints are majority event/concept."""
    stats: Counter[str] = Counter()
    sm = get_sessionmaker()
    async with sm() as session:
        unknowns = (
            (
                await session.execute(
                    select(CanonicalEntity).where(CanonicalEntity.type == EntityType.UNKNOWN.value)
                )
            )
            .scalars()
            .all()
        )
    stats["unknown_canonicals_scanned"] = len(unknowns)

    for ce in unknowns:
        async with sm() as session:
            aliases = (
                (
                    await session.execute(
                        select(EntityAlias).where(EntityAlias.canonical_id == ce.id)
                    )
                )
                .scalars()
                .all()
            )
            counter = Counter((a.kind_hint or "").strip().lower() for a in aliases)
            best = counter.most_common(1)
            if not best:
                continue
            kind, _n = best[0]
            new_type = (
                EntityType.EVENT.value
                if kind == "event"
                else EntityType.CONCEPT.value
                if kind == "concept"
                else None
            )
            if new_type is None:
                continue
            ce_live = (
                await session.execute(select(CanonicalEntity).where(CanonicalEntity.id == ce.id))
            ).scalar_one()
            ce_live.type = new_type
            session.add(ce_live)
            await session.commit()
            stats[f"retyped_{new_type}"] += 1
    return dict(stats)


def main() -> None:
    """CLI entrypoint — python -m app.services.ingest.backfill_event_concept_types."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = asyncio.run(run_backfill())
    logger.info("backfill done: %s", stats)


if __name__ == "__main__":
    main()
