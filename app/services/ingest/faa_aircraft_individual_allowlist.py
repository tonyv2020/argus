"""P3.4 mechanism — promote ONLY allowlisted individual aircraft.

**Nothing is promoted by this module today.** The allowlist table ships
empty, the gate reads only ``status='approved'``, and no caller invokes
:func:`promote_allowlisted`. :data:`PROPOSED_ENTRIES` below is a
proposal for Tony to review in the PR diff — it is deliberately NOT
inserted into the database, so this phase writes nothing at all.

Why an allowlist rather than a scoring threshold: P3.1 measured ~**90%
false positives** on individual name matching. FAA stores people as
``LAST FIRST MIDDLE`` while canonicals are ``First Last``, so an exact
string match usually means the name reads the same *backwards* — a
different person. No threshold survives that; only per-aircraft human
approval does.

Four gates still apply to an allowlisted entry, all of them:

  1. the entry is ``status='approved'`` in the table,
  2. the resolved canonical is ``person``-typed, ``open`` and published,
  3. the Scrutiny people-gate says PUBLIC via a **hard signal** — an
     FEC/bioguide/LDA/corporate-registry alias, i.e. an identifier that
     IS a public role. An LLM verdict is not sufficient here,
  4. promotion goes through the audited op, with actor and reason.

Street never surfaces regardless — the read path selects an explicit
column allowlist (``_AIRCRAFT_PUBLIC_COLUMNS`` in ``app/main.py``).

NOTE ON MECHANICS: individual aircraft have **no ``REGISTERS`` edge at
all** — P2 staged only the corporate cohort. So promoting an allowlisted
individual must CREATE the edge (staged + cited from the FAA snapshot)
and then promote it, rather than flipping an existing row.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass

from sqlalchemy import select

from app.models import (
    Aircraft,
    AircraftIndividualAllowlist,
    AircraftRegistrationEdge,
    AircraftSourceSnapshot,
    CanonicalEntity,
    EntityAlias,
    PublicationState,
    SurfaceMode,
    _new_id,
)
from app.services.aircraft_publish import promote

logger = logging.getLogger(__name__)

ACTOR = "helen-driver (P3.4 individual allowlist)"

#: Alias source systems that ARE a public role — the deterministic half
#: of Scrutiny. Mirrors ``app.services.scrutiny._PUBLIC_SOURCE_SYSTEMS``.
PUBLIC_SOURCE_SYSTEMS = (
    "fec.committee",
    "fec.candidate",
    "bioguide",
    "senate.lda.registrant",
    "corporate.registry.officer",
    "corporate.registry.exec",
)


@dataclass(frozen=True)
class ProposedEntry:
    """A candidate allowlist row, for review. NOT inserted anywhere."""

    n_number: str
    registrant_name: str
    canonical_name: str
    aircraft: int
    evidence: str
    source: str


#: PROPOSAL ONLY — awaiting Tony's approval. Deliberately tiny: of the
#: 85 held individuals, exactly one clears a corroborated tail-number-to-
#: person link. Everything else is in the exclusion notes on the PR.
PROPOSED_ENTRIES: tuple[ProposedEntry, ...] = (
    ProposedEntry(
        n_number="2908G",
        registrant_name="GRAVES SAM",
        canonical_name="Sam Graves",
        aircraft=1,
        evidence=(
            "TOWN-level agreement, not merely state: the FAA registrant address is "
            "TARKIO, MO, and Rep. Sam Graves (MO-6) is a farmer from Tarkio, Missouri "
            "— his actual hometown. Argus canonical 'Sam Graves' is person-typed, open, "
            "published, 3 edges, and carries BOTH hard public-role signals (bioguide + "
            "fec.candidate); recorded Scrutiny verdict is public/surface via "
            "scrutiny.hard_signals, not an LLM guess. Public-figure basis is his office: "
            "sitting member of Congress who chairs House Transportation & Infrastructure "
            "and is a licensed pilot, so aircraft ownership is squarely within his public "
            "role rather than incidental private detail."
        ),
        source="FAA MASTER snapshot a5be617c…; Argus canonical ad92a262 (bioguide, fec.candidate)",
    ),
)


async def _hard_signal_ok(session, canonical_id: str) -> list[str]:
    """Deterministic Scrutiny: the public-role aliases this canonical has."""
    rows = (
        await session.execute(
            select(EntityAlias.source_system)
            .where(EntityAlias.canonical_id == canonical_id)
            .where(EntityAlias.source_system.in_(PUBLIC_SOURCE_SYSTEMS))
            .distinct()
        )
    ).all()
    return sorted({r[0] for r in rows})


async def promote_allowlisted(session, dry_run: bool = True) -> dict:
    """Create + promote edges for APPROVED allowlist entries only.

    Refuses anything that fails any gate. Returns a per-entry report.
    """
    stats: dict = {"approved_entries": 0, "promoted": 0, "refused": [], "dry_run": dry_run}

    entries = (
        await session.execute(
            select(AircraftIndividualAllowlist).where(
                AircraftIndividualAllowlist.status == "approved"
            )
        )
    ).scalars().all()
    stats["approved_entries"] = len(entries)
    if not entries:
        logger.info("allowlist empty (or nothing approved) — nothing to do")
        return stats

    snapshot = await session.scalar(
        select(AircraftSourceSnapshot).order_by(AircraftSourceSnapshot.fetched_at.desc()).limit(1)
    )

    for entry in entries:
        ent = await session.scalar(
            select(CanonicalEntity).where(CanonicalEntity.id == entry.canonical_id)
        )
        aircraft = await session.scalar(
            select(Aircraft).where(Aircraft.n_number == entry.n_number)
        )

        def refuse(why: str, _n: str = entry.n_number) -> None:
            # _n is bound at definition time on purpose — a closure over
            # the loop variable would report the LAST entry's tail number
            # against every refusal.
            stats["refused"].append({"n_number": _n, "reason": why})
            logger.warning("allowlist REFUSED %s: %s", _n, why)

        if ent is None or aircraft is None:
            refuse("canonical or aircraft row not found")
            continue
        if ent.type != "person":
            refuse(f"canonical is {ent.type!r}, not a person")
            continue
        if ent.surface_mode != SurfaceMode.OPEN.value:
            refuse(f"canonical surface_mode={ent.surface_mode!r}")
            continue
        if ent.publication_state != PublicationState.PUBLISHED.value:
            refuse("canonical is not published in Argus")
            continue
        signals = await _hard_signal_ok(session, ent.id)
        if not signals:
            refuse("no hard public-role signal (an LLM verdict is not sufficient here)")
            continue
        if snapshot is None:
            refuse("no FAA snapshot to cite")
            continue

        edge = await session.scalar(
            select(AircraftRegistrationEdge).where(
                AircraftRegistrationEdge.canonical_id == ent.id,
                AircraftRegistrationEdge.aircraft_id == aircraft.id,
            )
        )
        if dry_run:
            stats["promoted"] += 1
            continue
        if edge is None:
            # Individuals have no REGISTERS edge — P2 staged only the
            # corporate cohort — so the edge is created here, born
            # staged + cited, then promoted through the audited op.
            edge = AircraftRegistrationEdge(
                id=_new_id(),
                canonical_id=ent.id,
                aircraft_id=aircraft.id,
                relation="registers",
                match_tier="individual_allowlist",
                match_score=1.0,
                registrant_name_raw=entry.registrant_name,
                snapshot_id=snapshot.id,
                source_url=snapshot.source_url,
                source_sha256=snapshot.sha256,
                surface_mode=SurfaceMode.SUPPRESS.value,
                publication_state=PublicationState.STAGED.value,
                batch_id=snapshot.batch_id,
            )
            session.add(edge)
            await session.flush()
        reason = f"P3.4 allowlist entry approved by {entry.approved_by}: {entry.evidence[:160]}"
        await promote(
            session,
            target_table="aircraft_registration_edges",
            target_id=edge.id,
            actor=ACTOR,
            reason=reason,
        )
        await promote(
            session, target_table="aircraft", target_id=aircraft.id, actor=ACTOR, reason=reason
        )
        stats["promoted"] += 1

    if not dry_run:
        await session.commit()
    return stats


async def approve_proposed(session, approved_by: str) -> list[str]:
    """Insert :data:`PROPOSED_ENTRIES` as APPROVED rows. Idempotent.

    Separate from promotion on purpose: approving is the human decision
    and promoting is the mechanical consequence, so they are two
    explicit commands with the approver recorded on the row.
    """
    from datetime import UTC, datetime

    inserted: list[str] = []
    for e in PROPOSED_ENTRIES:
        ent = await session.scalar(
            select(CanonicalEntity).where(CanonicalEntity.canonical_name == e.canonical_name)
        )
        if ent is None:
            logger.warning("approve: canonical %r not found", e.canonical_name)
            continue
        existing = await session.scalar(
            select(AircraftIndividualAllowlist).where(
                AircraftIndividualAllowlist.n_number == e.n_number,
                AircraftIndividualAllowlist.canonical_id == ent.id,
            )
        )
        if existing is not None:
            continue
        session.add(
            AircraftIndividualAllowlist(
                id=_new_id(),
                n_number=e.n_number,
                registrant_name=e.registrant_name,
                canonical_id=ent.id,
                evidence=e.evidence,
                source=e.source,
                added_by=ACTOR,
                status="approved",
                approved_by=approved_by,
                approved_at=datetime.now(UTC),
            )
        )
        inserted.append(e.n_number)
    await session.commit()
    return inserted


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="P3.4 individual allowlist promotion.")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument(
        "--approve-proposed",
        metavar="APPROVER",
        help="Insert PROPOSED_ENTRIES as approved rows, attributed to APPROVER.",
    )
    args = ap.parse_args()

    from app.db import get_sessionmaker

    async def go():
        async with get_sessionmaker()() as s:
            if args.approve_proposed:
                added = await approve_proposed(s, args.approve_proposed)
                print(f"approved into the allowlist: {added or '(already present)'}")
            return await promote_allowlisted(s, dry_run=not args.apply)

    s = asyncio.run(go())
    print(f"\n=== P3.4 INDIVIDUAL ALLOWLIST ({'APPLY' if args.apply else 'DRY RUN'}) ===")
    print(f"  approved entries in table : {s['approved_entries']}")
    print(f"  promoted                  : {s['promoted']}")
    for r in s["refused"]:
        print(f"  REFUSED {r['n_number']}: {r['reason']}")
    print("\n  PROPOSED (code only, NOT in the table, awaiting Tony):")
    for e in PROPOSED_ENTRIES:
        print(f"    N{e.n_number}  {e.registrant_name} -> {e.canonical_name} "
              f"({e.aircraft} aircraft)")


if __name__ == "__main__":  # pragma: no cover
    _main()
