"""P4 seed — populate ``anchor_registry`` with the P1 detention-industry
+ prison-telecom anchor set (+ the surveillance/musk-network stubs P1.6/P1.7
will flesh out).

Runnable as ``python -m app.services.ingest.seed_anchor_registry``.
Idempotent (upsert keyed on ``(label, entity_type)``).

The rows here MIRROR the current per-module constants
(``DETENTION_INDUSTRY_PACS`` etc.) with external IDs added where known.
Once every ingester reads from the registry (PRs B–E), the per-module
constants get deleted — this seed is what carries the data across.

External-ID sourcing (2026-07-19 audit against FEC + SEC + USAspending):
    * FEC committee IDs verified against ``api.open.fec.gov/v1/committees``
      searches under each committee's canonical name.
    * SEC CIKs verified against ``sec.gov/cgi-bin/browse-edgar?action=getcompany``.
    * USAspending recipient names are the surface strings the current
      ingester already sweeps; kept verbatim.
"""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass

from app.db import get_sessionmaker
from app.services.anchor_registry import upsert_anchor

logger = logging.getLogger(__name__)


@dataclass
class SeedRow:
    label: str
    entity_type: str
    priority_domain: str
    fec_committee_ids: tuple[str, ...] = ()
    fec_candidate_ids: tuple[str, ...] = ()
    sec_cik: int | None = None
    usaspending_recipient_names: tuple[str, ...] = ()
    lda_client_names: tuple[str, ...] = ()
    name_variants: tuple[str, ...] = ()
    surface_mode: str = "open"
    notes: str | None = None


# Detention operators — the P1 baseline plus SEC CIKs for the two
# publicly-traded primes.
_DETENTION_OPERATORS: tuple[SeedRow, ...] = (
    SeedRow(
        label="GEO Group",
        entity_type="organization",
        priority_domain="detention_operators",
        # Verified live 2026-07-19 via /names/committees/?q=geo group —
        # THE GEO GROUP, INC. POLITICAL ACTION COMMITTEE.
        fec_committee_ids=("C00382150",),
        sec_cik=923796,
        usaspending_recipient_names=(
            "GEO GROUP INC", "THE GEO GROUP INC", "GEO GROUP, INC.",
        ),
        lda_client_names=("The GEO Group",),
        name_variants=(
            "GEO GROUP INC PAC", "GEO GROUP", "GEO GROUP INC",
        ),
    ),
    SeedRow(
        label="CoreCivic",
        entity_type="organization",
        priority_domain="detention_operators",
        # Verified live 2026-07-19 via /names/committees/?q=corecivic —
        # CORECIVIC, INC. POLITICAL ACTION COMMITTEE (CORECIVIC PAC).
        fec_committee_ids=("C00366468",),
        sec_cik=1070985,
        usaspending_recipient_names=(
            "CORECIVIC INC",
            "CORECIVIC OF TENNESSEE LLC",
            "CORRECTIONS CORPORATION OF AMERICA",
            "CORECIVIC OF AMERICA LLC",
            "CORECIVIC OF ARIZONA LLC",
        ),
        lda_client_names=("CoreCivic", "Corrections Corporation of America"),
        name_variants=(
            "CORECIVIC INC PAC", "CORECIVIC PAC", "CCA PAC",
            "CORRECTIONS CORPORATION OF AMERICA",
        ),
        notes="Rebranded from Corrections Corporation of America (CCA) in 2016.",
    ),
    SeedRow(
        label="Management & Training Corp",
        entity_type="organization",
        priority_domain="detention_operators",
        # Verified live 2026-07-19 via /names/committees/?q=management
        # and training — MANAGEMENT AND TRAINING CORPORATION POLITICAL
        # ACTION COMMITTEE.
        fec_committee_ids=("C00208322",),
        usaspending_recipient_names=(
            "MANAGEMENT & TRAINING CORPORATION",
            "MANAGEMENT AND TRAINING CORPORATION",
            "MTC",
        ),
        lda_client_names=("Management and Training Corporation",),
        name_variants=(
            "MANAGEMENT AND TRAINING CORP", "MTC PAC",
            "MANAGEMENT & TRAINING CORPORATION",
        ),
        notes="Privately held; no SEC anchor.",
    ),
    SeedRow(
        label="LaSalle Corrections",
        entity_type="organization",
        priority_domain="detention_operators",
        usaspending_recipient_names=(
            "LASALLE CORRECTIONS LLC",
            "LASALLE SOUTHWEST CORRECTIONS",
            "LASALLE MANAGEMENT COMPANY",
        ),
        name_variants=(
            "LASALLE CORRECTIONS", "LASALLE MANAGEMENT",
            "LASALLE SOUTHWEST CORRECTIONS",
        ),
        notes="Privately held; no registered FEC PAC surfaced.",
    ),
)


# Prison-telecom sub-industry — all privately held (PE-owned) so no SEC.
_PRISON_TELECOM: tuple[SeedRow, ...] = (
    SeedRow(
        label="Securus Technologies",
        entity_type="organization",
        priority_domain="prison_telecom",
        usaspending_recipient_names=(
            "SECURUS TECHNOLOGIES INC",
            "SECURUS TECHNOLOGIES LLC",
            "SECURUS TECHNOLOGIES",
        ),
        lda_client_names=("Securus Technologies",),
        name_variants=("SECURUS TECHNOLOGIES", "SECURUS TECH", "SECURUS PAC"),
        notes="Private-equity-owned; subsidiary of Aventiv Technologies.",
    ),
    SeedRow(
        label="Aventiv Technologies",
        entity_type="organization",
        priority_domain="prison_telecom",
        usaspending_recipient_names=(
            "AVENTIV TECHNOLOGIES LLC",
            "AVENTIV TECHNOLOGIES INC",
            "AVENTIV TECHNOLOGIES",
        ),
        lda_client_names=("Aventiv Technologies",),
        name_variants=("AVENTIV TECHNOLOGIES", "AVENTIV TECH", "AVENTIV PAC"),
        notes="Parent of Securus Technologies + Satellite Tracking of People.",
    ),
    SeedRow(
        label="Satellite Tracking of People",
        entity_type="organization",
        priority_domain="prison_telecom",
        usaspending_recipient_names=(
            "SATELLITE TRACKING OF PEOPLE LLC",
            "SATELLITE TRACKING OF PEOPLE",
            "STOP LLC",
        ),
        name_variants=(
            "SATELLITE TRACKING OF PEOPLE", "STOP LLC", "STOP PAC",
        ),
        notes="Aventiv subsidiary; electronic monitoring.",
    ),
    SeedRow(
        label="GTL / ViaPath",
        entity_type="organization",
        priority_domain="prison_telecom",
        usaspending_recipient_names=(
            "GLOBAL TEL LINK CORPORATION",
            "GLOBAL TEL*LINK CORPORATION",
            "GLOBAL TEL LINK",
            "VIAPATH TECHNOLOGIES",
            "VIAPATH TECHNOLOGIES LLC",
        ),
        lda_client_names=("Global Tel Link", "ViaPath Technologies"),
        name_variants=(
            "GLOBAL TEL LINK", "GLOBAL TEL*LINK", "GTL PAC",
            "VIAPATH TECHNOLOGIES", "VIAPATH",
        ),
        notes="Renamed from Global Tel Link to ViaPath Technologies in 2022.",
    ),
)


# P1.6 surveillance + tech-influence anchors (Tony 2026-07-19).
# CIKs verified against sec.gov: Palantir 1321655, Axon (formerly TASER) 1069183.
# Flock Safety = privately held (no CIK); Clearview AI = private (no CIK).
_SURVEILLANCE: tuple[SeedRow, ...] = (
    SeedRow(
        label="Palantir Technologies",
        entity_type="organization",
        priority_domain="surveillance",
        sec_cik=1321655,
        usaspending_recipient_names=(
            "PALANTIR TECHNOLOGIES INC",
            "PALANTIR USG INC",
            "PALANTIR TECHNOLOGIES",
        ),
        lda_client_names=("Palantir Technologies",),
        notes="Major ICE/DHS contractor. Thiel is chairman (P1.6 affiliation).",
    ),
    SeedRow(
        label="Axon Enterprise",
        entity_type="organization",
        priority_domain="surveillance",
        sec_cik=1069183,
        usaspending_recipient_names=(
            "AXON ENTERPRISE INC", "TASER INTERNATIONAL INC",
        ),
        lda_client_names=("Axon Enterprise",),
        notes="Body cameras + Evidence.com; renamed from TASER 2017.",
    ),
    SeedRow(
        label="Flock Safety",
        entity_type="organization",
        priority_domain="surveillance",
        usaspending_recipient_names=("FLOCK GROUP INC", "FLOCK SAFETY"),
        lda_client_names=("Flock Safety",),
        notes="Private; ALPR camera network. No SEC anchor.",
    ),
    SeedRow(
        label="Clearview AI",
        entity_type="organization",
        priority_domain="surveillance",
        usaspending_recipient_names=("CLEARVIEW AI INC",),
        lda_client_names=("Clearview AI",),
        notes="Private; facial-recognition scraping.",
    ),
    SeedRow(
        label="Peter Thiel",
        entity_type="person",
        priority_domain="surveillance",
        name_variants=("Peter A. Thiel", "Peter Andreas Thiel"),
        surface_mode="open",
        notes="Public figure. FEC individual-contributor mode (P4 PR E).",
    ),
    SeedRow(
        label="Founders Fund",
        entity_type="organization",
        priority_domain="surveillance",
        usaspending_recipient_names=("FOUNDERS FUND", "FOUNDERS FUND LLC"),
        notes="Thiel VC vehicle.",
    ),
)


# P1.7 Musk network anchors (Tony 2026-07-19).
_MUSK_NETWORK: tuple[SeedRow, ...] = (
    SeedRow(
        label="Elon Musk",
        entity_type="person",
        priority_domain="musk_network",
        name_variants=("Elon Reeve Musk", "Elon R. Musk"),
        surface_mode="open",
        notes="Public figure. FEC individual-contributor mode (P4 PR E).",
    ),
    SeedRow(
        label="Tesla",
        entity_type="organization",
        priority_domain="musk_network",
        sec_cik=1318605,
        usaspending_recipient_names=("TESLA INC", "TESLA MOTORS INC"),
        lda_client_names=("Tesla",),
    ),
    SeedRow(
        label="SpaceX",
        entity_type="organization",
        priority_domain="musk_network",
        usaspending_recipient_names=(
            "SPACE EXPLORATION TECHNOLOGIES CORP", "SPACEX",
        ),
        lda_client_names=("Space Exploration Technologies", "SpaceX"),
        notes="Private; major DoD/NASA contractor.",
    ),
    SeedRow(
        label="America PAC",
        entity_type="pac",
        priority_domain="musk_network",
        # C00879510 — verified live 2026-07-19 via
        # /names/committees/?q=america pac (id C00838163 in initial seed
        # was wrong; the real Musk super-PAC's committee_id is 879510).
        fec_committee_ids=("C00879510",),
        name_variants=("America PAC",),
        notes="Musk-funded super-PAC. External-ID keyed (name search hits FXAIX + 401(k) America PAC + others).",
    ),
    SeedRow(
        label="X Corp",
        entity_type="organization",
        priority_domain="musk_network",
        usaspending_recipient_names=("X CORP", "TWITTER INC"),
        notes="Successor of Twitter.",
    ),
    SeedRow(
        label="xAI",
        entity_type="organization",
        priority_domain="musk_network",
        usaspending_recipient_names=("XAI CORP", "X.AI CORP"),
    ),
    SeedRow(
        label="The Boring Company",
        entity_type="organization",
        priority_domain="musk_network",
        usaspending_recipient_names=("THE BORING COMPANY",),
    ),
    SeedRow(
        label="Neuralink",
        entity_type="organization",
        priority_domain="musk_network",
        usaspending_recipient_names=("NEURALINK CORP",),
    ),
)


_ALL_SEED: tuple[SeedRow, ...] = (
    _DETENTION_OPERATORS + _PRISON_TELECOM + _SURVEILLANCE + _MUSK_NETWORK
)


async def seed_all() -> dict[str, int]:
    """Upsert every seed row. Returns per-domain counts."""
    counts: dict[str, int] = {}
    sm = get_sessionmaker()
    async with sm() as session:
        for row in _ALL_SEED:
            await upsert_anchor(
                session,
                label=row.label,
                entity_type=row.entity_type,
                priority_domain=row.priority_domain,
                fec_committee_ids=row.fec_committee_ids,
                fec_candidate_ids=row.fec_candidate_ids,
                sec_cik=row.sec_cik,
                usaspending_recipient_names=row.usaspending_recipient_names,
                lda_client_names=row.lda_client_names,
                name_variants=row.name_variants,
                surface_mode=row.surface_mode,
                notes=row.notes,
            )
            counts[row.priority_domain] = counts.get(row.priority_domain, 0) + 1
        await session.commit()
    return counts


def main() -> None:
    """CLI entry — `python -m app.services.ingest.seed_anchor_registry`."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    counts = asyncio.run(seed_all())
    for domain, n in sorted(counts.items()):
        logger.info("seeded domain=%s rows=%d", domain, n)
    logger.info("seed done: %d anchors across %d domains",
                sum(counts.values()), len(counts))


if __name__ == "__main__":
    main()
