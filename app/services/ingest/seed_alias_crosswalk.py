"""P2 seed — populate ``alias_crosswalk`` with the curated merge queue.

Runnable as ``python -m app.services.ingest.seed_alias_crosswalk``.
Idempotent: skips a row if an unapplied entry for the same ``from_id``
already exists.

Curated on the 2026-07-19 post-P4-sweep fragmentation observed live:
* CoreCivic 4 fragments → main + KEEP lobbyist + KEEP PAC + merge
  the former-CCA-of-Tennessee variant.
* Aventiv 2 → merge the "LLC AND VARIOUS SUBSIDIARIES" legal-entity
  listing into the main.
* ViaPath 3 → merge SEC former-Global-Tel-Link variant + autoseed
  "GTL / ViaPath" into VIAPATH TECHNOLOGIES.
* Palantir 7 → merge news short-form "Palantir" into Palantir
  Technologies Inc.; KEEP 3 lobbying firms + KEEP topic mentions.
* Congress news-person duplicates: SKIPPED FROM THIS SEED — the news
  person canonicals need person-type merge review + fail-closed
  handling; a follow-on curated seed lands them.

Every row picks the survivor with the RICHEST evidence (edges + aliases)
so citations don't get orphaned. Reason string is human-readable.
"""

from __future__ import annotations

import asyncio
import logging

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import AliasCrosswalk, CanonicalEntity

logger = logging.getLogger(__name__)


# Curated merges — each entry: (from_canonical_name, to_canonical_name,
# reason). ORG-type merges only in this seed. Person-type merges land
# separately after curation review.
_CURATED: tuple[tuple[str, str, str], ...] = (
    # CoreCivic — former-name Tennessee variant.
    (
        "CORECIVIC (FORMERLY CCA OF TENNESSEE)",
        "CoreCivic",
        "SEC former-name variant; same CIK 1070985; pre-2016 CCA rename",
    ),
    # Aventiv — LLC listing.
    (
        "AVENTIV TECHNOLOGIES LLC AND VARIOUS SUBSIDIARIES",
        "AVENTIV TECHNOLOGIES",
        "USAspending legal-entity listing; same corporate identity",
    ),
    # ViaPath — merge autoseed + SEC-former-name into VIAPATH TECHNOLOGIES.
    (
        "GTL / ViaPath",
        "VIAPATH TECHNOLOGIES",
        "P4 B autoseed hint canonical; both are ViaPath Technologies",
    ),
    (
        "VIAPATH TECHNOLOGIES, FORMERLY REPORTED AS GLOBAL TEL*LINK CORPORATION",
        "VIAPATH TECHNOLOGIES",
        "SEC former-name variant; 2022 rebrand from Global Tel*Link",
    ),
    # Palantir — merge short-form news mention into SEC-issuer name.
    (
        "Palantir",
        "Palantir Technologies Inc.",
        "News short-form; both refer to the same PLTR (CIK 1321655) issuer",
    ),
    # Sponsor-org / news-org fragments (P3 + P0). Merges into the
    # richest-connected surviving canonical for each corporate identity.
    (
        "Geo Group",
        "THE GEO GROUP, INC.",
        "News title-case canonical; merged into P3 sponsor-org canonical "
        "(THE GEO GROUP, INC. — 12 edges vs Geo Group's 9)",
    ),
    # MANAGMENT & TRAINING CORPORATION (P3 typo) survives as-is until an
    # MTC canonical with more edges exists to merge into.
)


async def _lookup_canonical(
    session: AsyncSession, name: str
) -> CanonicalEntity | None:
    """Return the ORG canonical with an exact ``canonical_name`` match,
    or None."""
    row = (
        await session.execute(
            select(CanonicalEntity).where(
                CanonicalEntity.canonical_name == name,
                CanonicalEntity.type == "organization",
            )
        )
    ).scalar_one_or_none()
    return row


async def seed() -> dict[str, int]:
    """Upsert curated crosswalk rows. Returns per-status counts."""
    counts = {"queued": 0, "skipped_existing": 0, "skipped_missing": 0}
    sm = get_sessionmaker()
    async with sm() as session:
        for from_name, to_name, reason in _CURATED:
            src = await _lookup_canonical(session, from_name)
            dst = await _lookup_canonical(session, to_name)
            if src is None:
                logger.warning(
                    "seed: from %r not found in canonical_entities",
                    from_name,
                )
                counts["skipped_missing"] += 1
                continue
            if dst is None:
                logger.warning(
                    "seed: to %r not found in canonical_entities",
                    to_name,
                )
                counts["skipped_missing"] += 1
                continue
            existing = (
                await session.execute(
                    select(AliasCrosswalk).where(
                        AliasCrosswalk.from_id == src.id
                    )
                )
            ).scalar_one_or_none()
            if existing:
                counts["skipped_existing"] += 1
                continue
            session.add(
                AliasCrosswalk(
                    from_id=src.id,
                    to_id=dst.id,
                    reason=reason,
                )
            )
            counts["queued"] += 1
            logger.info(
                "queued merge %s → %s (%s)",
                from_name, to_name, reason,
            )
        await session.commit()
    return counts


def main() -> None:
    """CLI — ``python -m app.services.ingest.seed_alias_crosswalk``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    counts = asyncio.run(seed())
    logger.info("alias_crosswalk seed done: %s", counts)


if __name__ == "__main__":
    main()
