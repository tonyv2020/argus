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


# P5.2 — party committees. NRSC/NRCC = Republican, DSCC/DCCC =
# Democratic. Committee IDs verified live 2026-07-19 via
# /names/committees/. Their PAC contributions flow ENTIRELY to their
# party's candidates; capturing them is the missing "party-level giving"
# side of the influence model.
_PARTY_COMMITTEES: tuple[SeedRow, ...] = (
    SeedRow(
        label="National Republican Senatorial Committee",
        entity_type="pac",
        priority_domain="party_committees",
        fec_committee_ids=("C00027466",),   # NRSC
        name_variants=("NRSC", "National Republican Senatorial Committee"),
        notes="party=Republican chamber=Senate",
    ),
    SeedRow(
        label="National Republican Congressional Committee",
        entity_type="pac",
        priority_domain="party_committees",
        fec_committee_ids=("C00075820",),   # NRCC
        name_variants=("NRCC", "National Republican Congressional Committee"),
        notes="party=Republican chamber=House",
    ),
    SeedRow(
        label="Democratic Senatorial Campaign Committee",
        entity_type="pac",
        priority_domain="party_committees",
        fec_committee_ids=("C00042366",),   # DSCC
        name_variants=("DSCC", "Democratic Senatorial Campaign Committee"),
        notes="party=Democratic chamber=Senate",
    ),
    SeedRow(
        label="Democratic Congressional Campaign Committee",
        entity_type="pac",
        priority_domain="party_committees",
        fec_committee_ids=("C00000935",),   # DCCC
        name_variants=("DCCC", "Democratic Congressional Campaign Committee"),
        notes="party=Democratic chamber=House",
    ),
)


# ─── Batch 1 broadening (Tony 2026-08-12) ──────────────────────────────
#
# Cassandra's award-grade well was too narrow — flow_model1 surfaced only
# the 3 detention contractors on the Republican side + ZERO on the
# Democratic side. Ingestion scope was the root cause: Stage 2
# usaspending was scoped to DETENTION_INDUSTRY_RECIPIENTS + ICE/BOP/USMS.
# pick_cassandra_topic's day-of-year rotation + 14-day dedup already
# work well; the problem was pool depth, not selection.
#
# Batch 1 adds ~21 marquee federal contractors across 4 sectors so the
# well grows to ~25 with both parties populated (defense + pharma give
# bipartisan; energy tilts Republican; gov-tech mixed). The existing
# ``ingest_from_registry`` sweep path already handles non-detention
# anchors correctly via helen's per-anchor broaden_agency_scope logic
# (2026-08-11 fix), so no ingester code change is needed — only these
# seeds + the corresponding sweep call.
#
# FEC committee_ids intentionally omitted here — the fuzzy PAC search
# in ``fec.py::find_pac_by_queries`` uses ``match_tokens`` to filter
# candidates, and the name_variants below are specific enough for that
# path. If a corporate PAC ever resolves ambiguously (as happened with
# the Musk "America PAC" vs FXAIX collision), backfill the committee_id
# here — same fix pattern that made America PAC C00879510-keyed above.
#
# Priority domains:
#   defense_prime   — top-tier DoD contractors (Lockheed, RTX, Boeing…)
#   govtech_federal — federal IT/consulting primes (Booz, Leidos, SAIC…)
#   pharma_health   — federal pharma + health-insurance beneficiaries
#   energy_infra    — energy/heavy-infra contractors (Exxon, Chevron…)

_DEFENSE_PRIME: tuple[SeedRow, ...] = (
    SeedRow(
        label="Lockheed Martin",
        entity_type="organization",
        priority_domain="defense_prime",
        sec_cik=936468,
        usaspending_recipient_names=(
            "LOCKHEED MARTIN CORPORATION",
            "LOCKHEED MARTIN CORP",
            "LOCKHEED MARTIN",
        ),
        lda_client_names=("Lockheed Martin",),
        name_variants=(
            "LOCKHEED MARTIN EMPLOYEES POLITICAL ACTION COMMITTEE",
            "LOCKHEED MARTIN CORPORATION EMPLOYEES POLITICAL ACTION COMMITTEE",
            "LOCKHEED MARTIN CORPORATION PAC",
            "LOCKHEED MARTIN PAC",
        ),
        notes="Top DoD prime contractor. Batch 1 broadening 2026-08-12.",
    ),
    SeedRow(
        label="RTX (Raytheon)",
        entity_type="organization",
        priority_domain="defense_prime",
        sec_cik=101829,
        usaspending_recipient_names=(
            "RTX CORPORATION",
            "RAYTHEON COMPANY",
            "RAYTHEON TECHNOLOGIES CORPORATION",
            "RAYTHEON MISSILES & DEFENSE",
        ),
        lda_client_names=("RTX Corporation", "Raytheon Company"),
        name_variants=(
            "RAYTHEON COMPANY POLITICAL ACTION COMMITTEE",
            "RTX CORPORATION POLITICAL ACTION COMMITTEE",
            "RAYTHEON POLITICAL ACTION COMMITTEE",
            "RTX PAC",
        ),
        notes="RTX = Raytheon Technologies (2020 merger of UTC + Raytheon).",
    ),
    SeedRow(
        label="Boeing",
        entity_type="organization",
        priority_domain="defense_prime",
        sec_cik=12927,
        usaspending_recipient_names=(
            "THE BOEING COMPANY",
            "BOEING COMPANY",
            "BOEING",
        ),
        lda_client_names=("The Boeing Company",),
        name_variants=(
            "THE BOEING COMPANY POLITICAL ACTION COMMITTEE",
            "BOEING COMPANY POLITICAL ACTION COMMITTEE",
            "BOEING PAC",
        ),
    ),
    SeedRow(
        label="General Dynamics",
        entity_type="organization",
        priority_domain="defense_prime",
        sec_cik=40533,
        usaspending_recipient_names=(
            "GENERAL DYNAMICS CORPORATION",
            "GENERAL DYNAMICS INFORMATION TECHNOLOGY INC",
            "GENERAL DYNAMICS MISSION SYSTEMS INC",
            "GENERAL DYNAMICS LAND SYSTEMS INC",
        ),
        lda_client_names=("General Dynamics Corporation",),
        name_variants=(
            "GENERAL DYNAMICS CORPORATION POLITICAL ACTION COMMITTEE",
            "GENERAL DYNAMICS PAC",
        ),
    ),
    SeedRow(
        label="Northrop Grumman",
        entity_type="organization",
        priority_domain="defense_prime",
        sec_cik=1133421,
        usaspending_recipient_names=(
            "NORTHROP GRUMMAN CORPORATION",
            "NORTHROP GRUMMAN SYSTEMS CORPORATION",
            "NORTHROP GRUMMAN INNOVATION SYSTEMS",
        ),
        lda_client_names=("Northrop Grumman Corporation",),
        name_variants=(
            "NORTHROP GRUMMAN CORPORATION POLITICAL ACTION COMMITTEE",
            "NORTHROP GRUMMAN POLITICAL ACTION COMMITTEE",
            "NORTHROP GRUMMAN PAC",
        ),
    ),
    SeedRow(
        label="L3Harris",
        entity_type="organization",
        priority_domain="defense_prime",
        sec_cik=202058,
        usaspending_recipient_names=(
            "L3HARRIS TECHNOLOGIES INC",
            "L3HARRIS TECHNOLOGIES, INC.",
            "L3HARRIS TECHNOLOGIES",
        ),
        lda_client_names=("L3Harris Technologies",),
        name_variants=(
            "L3HARRIS TECHNOLOGIES POLITICAL ACTION COMMITTEE",
            "L3HARRIS PAC",
            "L3 TECHNOLOGIES POLITICAL ACTION COMMITTEE",
        ),
        notes="L3Harris formed 2019 by L3 Technologies + Harris Corp merger.",
    ),
)


_GOVTECH_FEDERAL: tuple[SeedRow, ...] = (
    SeedRow(
        label="Booz Allen Hamilton",
        entity_type="organization",
        priority_domain="govtech_federal",
        sec_cik=1443669,
        usaspending_recipient_names=(
            "BOOZ ALLEN HAMILTON INC",
            "BOOZ ALLEN HAMILTON ENGINEERING SERVICES LLC",
            "BOOZ ALLEN HAMILTON",
        ),
        lda_client_names=("Booz Allen Hamilton",),
        name_variants=(
            "BOOZ ALLEN HAMILTON INC. POLITICAL ACTION COMMITTEE",
            "BOOZ ALLEN HAMILTON POLITICAL ACTION COMMITTEE",
            "BOOZ ALLEN PAC",
        ),
    ),
    SeedRow(
        label="Leidos",
        entity_type="organization",
        priority_domain="govtech_federal",
        sec_cik=1336920,
        usaspending_recipient_names=(
            "LEIDOS INC",
            "LEIDOS INNOVATIONS CORPORATION",
            "LEIDOS",
        ),
        lda_client_names=("Leidos",),
        name_variants=(
            "LEIDOS POLITICAL ACTION COMMITTEE",
            "LEIDOS INC POLITICAL ACTION COMMITTEE",
            "LEIDOS PAC",
        ),
    ),
    SeedRow(
        label="SAIC",
        entity_type="organization",
        priority_domain="govtech_federal",
        sec_cik=1571123,
        usaspending_recipient_names=(
            "SCIENCE APPLICATIONS INTERNATIONAL CORPORATION",
            "SCIENCE APPLICATIONS INTERNATIONAL CORP",
            "SAIC",
        ),
        lda_client_names=("Science Applications International Corporation",),
        name_variants=(
            "SCIENCE APPLICATIONS INTERNATIONAL CORPORATION POLITICAL ACTION COMMITTEE",
            "SAIC POLITICAL ACTION COMMITTEE",
            "SAIC PAC",
        ),
    ),
    SeedRow(
        label="Accenture Federal Services",
        entity_type="organization",
        priority_domain="govtech_federal",
        usaspending_recipient_names=(
            "ACCENTURE FEDERAL SERVICES LLC",
            "ACCENTURE FEDERAL SERVICES",
            "ACCENTURE LLP",
        ),
        lda_client_names=("Accenture", "Accenture Federal Services"),
        name_variants=(
            "ACCENTURE LLP POLITICAL ACTION COMMITTEE",
            "ACCENTURE PAC",
        ),
        notes="Accenture Federal Services is the US-federal-focused sub.",
    ),
    SeedRow(
        label="Maximus",
        entity_type="organization",
        priority_domain="govtech_federal",
        sec_cik=1032220,
        usaspending_recipient_names=(
            "MAXIMUS INC",
            "MAXIMUS FEDERAL SERVICES INC",
            "MAXIMUS",
        ),
        lda_client_names=("Maximus",),
        name_variants=(
            "MAXIMUS INC POLITICAL ACTION COMMITTEE",
            "MAXIMUS POLITICAL ACTION COMMITTEE",
            "MAXIMUS PAC",
        ),
        notes="Major CMS/HHS contractor (call centers, benefits admin).",
    ),
)


_PHARMA_HEALTH: tuple[SeedRow, ...] = (
    SeedRow(
        label="Pfizer",
        entity_type="organization",
        priority_domain="pharma_health",
        sec_cik=78003,
        usaspending_recipient_names=(
            "PFIZER INC",
            "PFIZER, INC.",
            "PFIZER",
        ),
        lda_client_names=("Pfizer",),
        name_variants=(
            "PFIZER INC POLITICAL ACTION COMMITTEE",
            "PFIZER INC. POLITICAL ACTION COMMITTEE",
            "PFIZER PAC",
        ),
    ),
    SeedRow(
        label="McKesson",
        entity_type="organization",
        priority_domain="pharma_health",
        sec_cik=927653,
        usaspending_recipient_names=(
            "MCKESSON CORPORATION",
            "MCKESSON MEDICAL-SURGICAL INC",
            "MCKESSON",
        ),
        lda_client_names=("McKesson Corporation",),
        name_variants=(
            "MCKESSON CORPORATION EMPLOYEES POLITICAL FUND",
            "MCKESSON CORPORATION POLITICAL ACTION COMMITTEE",
            "MCKESSON PAC",
        ),
    ),
    SeedRow(
        label="Centene",
        entity_type="organization",
        priority_domain="pharma_health",
        sec_cik=1071739,
        usaspending_recipient_names=(
            "CENTENE CORPORATION",
            "CENTENE MANAGEMENT COMPANY LLC",
            "CENTENE",
        ),
        lda_client_names=("Centene Corporation",),
        name_variants=(
            "CENTENE CORPORATION POLITICAL ACTION COMMITTEE",
            "CENTENE PAC",
        ),
        notes="Largest Medicaid managed-care contractor.",
    ),
    SeedRow(
        label="Humana",
        entity_type="organization",
        priority_domain="pharma_health",
        sec_cik=49071,
        usaspending_recipient_names=(
            "HUMANA INC",
            "HUMANA GOVERNMENT BUSINESS INC",
            "HUMANA",
        ),
        lda_client_names=("Humana",),
        name_variants=(
            "HUMANA INC POLITICAL ACTION COMMITTEE",
            "HUMANA INC. POLITICAL ACTION COMMITTEE",
            "HUMANA PAC",
        ),
        notes="TRICARE East region contractor + Medicare Advantage.",
    ),
    SeedRow(
        label="UnitedHealth Group",
        entity_type="organization",
        priority_domain="pharma_health",
        sec_cik=731766,
        usaspending_recipient_names=(
            "UNITEDHEALTH GROUP INCORPORATED",
            "UNITEDHEALTH GROUP INC",
            "OPTUM RX INC",
            "OPTUMRX INC",
        ),
        lda_client_names=("UnitedHealth Group", "Optum"),
        name_variants=(
            "UNITEDHEALTH GROUP INCORPORATED POLITICAL ACTION COMMITTEE",
            "UNITED HEALTH GROUP INCORPORATED POLITICAL ACTION COMMITTEE",
            "UNITEDHEALTH PAC",
        ),
    ),
    SeedRow(
        label="Moderna",
        entity_type="organization",
        priority_domain="pharma_health",
        sec_cik=1682852,
        usaspending_recipient_names=(
            "MODERNA INC",
            "MODERNA US INC",
            "MODERNATX INC",
            "MODERNATX, INC.",
            "MODERNA",
        ),
        lda_client_names=("Moderna",),
        name_variants=(
            "MODERNA INC POLITICAL ACTION COMMITTEE",
            "MODERNA POLITICAL ACTION COMMITTEE",
            "MODERNA PAC",
        ),
        notes="Major BARDA/HHS covid-vaccine contractor.",
    ),
)


_ENERGY_INFRA: tuple[SeedRow, ...] = (
    SeedRow(
        label="ExxonMobil",
        entity_type="organization",
        priority_domain="energy_infra",
        sec_cik=34088,
        usaspending_recipient_names=(
            "EXXON MOBIL CORPORATION",
            "EXXONMOBIL",
            "EXXON MOBIL",
        ),
        lda_client_names=("ExxonMobil",),
        name_variants=(
            "EXXONMOBIL POLITICAL ACTION COMMITTEE",
            "EXXON MOBIL POLITICAL ACTION COMMITTEE",
            "EXXONMOBIL PAC",
        ),
    ),
    SeedRow(
        label="Chevron",
        entity_type="organization",
        priority_domain="energy_infra",
        sec_cik=93410,
        usaspending_recipient_names=(
            "CHEVRON U.S.A. INC",
            "CHEVRON USA INC",
            "CHEVRON CORPORATION",
            "CHEVRON",
        ),
        lda_client_names=("Chevron",),
        name_variants=(
            "CHEVRON EMPLOYEES POLITICAL ACTION COMMITTEE",
            "CHEVRON POLITICAL ACTION COMMITTEE",
            "CHEVRON PAC",
        ),
    ),
    SeedRow(
        label="Bechtel",
        entity_type="organization",
        priority_domain="energy_infra",
        usaspending_recipient_names=(
            "BECHTEL NATIONAL INC",
            "BECHTEL CORPORATION",
            "BECHTEL PLANT MACHINERY INC",
            "BECHTEL",
        ),
        lda_client_names=("Bechtel Corporation",),
        name_variants=(
            "BECHTEL GROUP INC POLITICAL ACTION COMMITTEE",
            "BECHTEL POLITICAL ACTION COMMITTEE",
            "BECHTEL PAC",
        ),
        notes="Private; major DoE weapons-complex + civil-infra prime.",
    ),
    SeedRow(
        label="Fluor",
        entity_type="organization",
        priority_domain="energy_infra",
        sec_cik=1124198,
        usaspending_recipient_names=(
            "FLUOR CORPORATION",
            "FLUOR ENTERPRISES INC",
            "FLUOR FEDERAL SERVICES INC",
            "FLUOR",
        ),
        lda_client_names=("Fluor Corporation",),
        name_variants=(
            "FLUOR CORPORATION PUBLIC AFFAIRS COMMITTEE",
            "FLUOR CORPORATION POLITICAL ACTION COMMITTEE",
            "FLUOR PAC",
        ),
    ),
)


_ALL_SEED: tuple[SeedRow, ...] = (
    _DETENTION_OPERATORS + _PRISON_TELECOM + _SURVEILLANCE + _MUSK_NETWORK
    + _PARTY_COMMITTEES
    + _DEFENSE_PRIME + _GOVTECH_FEDERAL + _PHARMA_HEALTH + _ENERGY_INFRA
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
