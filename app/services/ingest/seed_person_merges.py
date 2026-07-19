"""P2 H — seed person-type merges into alias_crosswalk (DRY-RUN GATED).

Congress news-person canonicals (Cruz=8, Warren=8 duplicate mentions
across news articles) fold into the roster canonicals from P4 D.
Person-type merges are the privacy-critical class: the fail-closed
surface_mode machinery gets its real workout here.

**Ship gate (helen 2026-07-19 20:53Z):** DRY-RUN ONLY until helen has
adversarially validated the fail-closed refusals. This script queues
the merges + also queues ONE synthetic adversarial row where the src
is more-protected than the dst — the dry-run report proves that row
is REFUSED (refused_privacy counter increments).

Adversarial row: a SUPPRESS-mode person canonical from
hollywood.entity_tags is picked at random + queued as
``from=<suppressed>, to=<any open person>``. The refused-privacy
counter MUST be > 0 in the dry-run output. After helen's OK, the
adversarial row is deleted + the real person merges applied.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import and_, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import (
    AliasCrosswalk,
    AnchorRegistry,
    CanonicalEntity,
    EntityAlias,
    EntityType,
)

logger = logging.getLogger(__name__)


ADVERSARIAL_REASON = (
    "P2 H ADVERSARIAL TEST: SUPPRESS→OPEN merge that MUST be refused. "
    "Delete this row after helen validates the fail-closed behavior."
)


async def _find_roster_canonical(
    session: AsyncSession, label: str
) -> CanonicalEntity | None:
    """Return the roster-created person canonical for ``label``.

    Roster canonicals carry a ``bioguide`` alias (P4 D) — that's the
    distinguishing feature vs news-person duplicates.
    """
    row = (
        await session.execute(
            select(CanonicalEntity)
            .join(EntityAlias, EntityAlias.canonical_id == CanonicalEntity.id)
            .where(
                CanonicalEntity.type == EntityType.PERSON.value,
                CanonicalEntity.canonical_name == label,
                EntityAlias.source_system == "bioguide",
            )
            .distinct()
        )
    ).scalar_one_or_none()
    return row


async def _find_news_dupes(
    session: AsyncSession, roster_canonical: CanonicalEntity
) -> list[CanonicalEntity]:
    """Return news-person canonicals with the same canonical_name as
    the roster row but a DIFFERENT id (i.e. the fragmented dupes).

    Constrains to ``surface_mode='open'`` — a suppress/alias dupe with
    the same name is a privacy signal (the person was gated for a
    reason) and requires curator review, not a bulk merge.
    """
    rows = (
        await session.execute(
            select(CanonicalEntity).where(
                CanonicalEntity.type == EntityType.PERSON.value,
                CanonicalEntity.canonical_name == roster_canonical.canonical_name,
                CanonicalEntity.id != roster_canonical.id,
                CanonicalEntity.surface_mode == "open",
            )
        )
    ).scalars().all()
    return rows


async def _pick_suppressed_person(
    session: AsyncSession,
) -> CanonicalEntity | None:
    """Random SUPPRESS-mode person for the adversarial test row."""
    row = (
        await session.execute(
            select(CanonicalEntity)
            .where(
                CanonicalEntity.type == EntityType.PERSON.value,
                CanonicalEntity.surface_mode == "suppress",
            )
            .order_by(func.random())
            .limit(1)
        )
    ).scalar_one_or_none()
    return row


async def _pick_open_roster_person(
    session: AsyncSession,
) -> CanonicalEntity | None:
    """Random OPEN-mode roster person for the adversarial row's target."""
    row = (
        await session.execute(
            select(CanonicalEntity)
            .join(EntityAlias, EntityAlias.canonical_id == CanonicalEntity.id)
            .where(
                CanonicalEntity.type == EntityType.PERSON.value,
                CanonicalEntity.surface_mode == "open",
                EntityAlias.source_system == "bioguide",
            )
            .order_by(func.random())
            .limit(1)
        )
    ).scalar_one_or_none()
    return row


async def queue_person_merges() -> dict[str, int]:
    """Discover + queue congress news-person merges + 1 adversarial
    SUPPRESS→OPEN row for the dry-run gate."""
    counts = {
        "queued_person_merges": 0,
        "adversarial_row_queued": 0,
        "skipped_no_roster": 0,
        "skipped_existing": 0,
    }
    sm = get_sessionmaker()
    async with sm() as session:
        # Iterate roster canonicals (via anchor_registry congress rows).
        roster_labels = (
            await session.execute(
                select(AnchorRegistry.label).where(
                    AnchorRegistry.priority_domain == "congress"
                )
            )
        ).scalars().all()

        for label in roster_labels:
            roster = await _find_roster_canonical(session, label)
            if roster is None:
                counts["skipped_no_roster"] += 1
                continue
            dupes = await _find_news_dupes(session, roster)
            for dupe in dupes:
                existing = (
                    await session.execute(
                        select(AliasCrosswalk).where(
                            AliasCrosswalk.from_id == dupe.id
                        )
                    )
                ).scalar_one_or_none()
                if existing:
                    counts["skipped_existing"] += 1
                    continue
                session.add(
                    AliasCrosswalk(
                        from_id=dupe.id,
                        to_id=roster.id,
                        reason=(
                            f"P2 H congress news-person dupe fold "
                            f"({dupe.canonical_name}) → roster canonical"
                        ),
                    )
                )
                counts["queued_person_merges"] += 1

        # ADVERSARIAL row: suppress→open must be REFUSED in dry-run.
        suppressed = await _pick_suppressed_person(session)
        open_target = await _pick_open_roster_person(session)
        if suppressed and open_target and suppressed.id != open_target.id:
            existing = (
                await session.execute(
                    select(AliasCrosswalk).where(
                        AliasCrosswalk.from_id == suppressed.id
                    )
                )
            ).scalar_one_or_none()
            if not existing:
                session.add(
                    AliasCrosswalk(
                        from_id=suppressed.id,
                        to_id=open_target.id,
                        reason=ADVERSARIAL_REASON,
                    )
                )
                counts["adversarial_row_queued"] = 1
                logger.info(
                    "adversarial row queued: SUPPRESS %s → OPEN %s "
                    "(MUST be refused in dry-run)",
                    suppressed.id, open_target.id,
                )

        await session.commit()
    return counts


def main() -> None:
    """CLI — `python -m app.services.ingest.seed_person_merges`."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    counts = asyncio.run(queue_person_merges())
    logger.info("person merge seed done: %s", counts)


if __name__ == "__main__":
    main()
