"""P1.5.2 — resolve FEC contributions to the MEMBER they fund.

The gap this closes
-------------------
Argus's ``contributes_to`` edges target the **recipient committee**
("GARBARINO FOR CONGRESS", "KIGGANS FOR CONGRESS") — a canonical created
from an FEC disbursement recipient, keyed on the FEC **committee id**
(``C…``). The member lives in a different canonical, keyed on **bioguide**
+ FEC **candidate id** (``H…``/``S…``) by
:mod:`app.services.ingest.congress_roster`. Nothing joins them, so money
never attributes to the person.

The join, keyed on external ids only
------------------------------------
FEC publishes the authoritative candidate↔committee linkage as a bulk
file — ``ccl<yy>.zip`` (``CAND_ID | CAND_ELECTION_YR | FEC_ELECTION_YR |
CMTE_ID | CMTE_TP | CMTE_DSGN | LINKAGE_ID``). One download per cycle
covers every committee, with no API key and no rate limit. This module:

1. Loads the linkage rows for the requested cycles.
2. Keeps only rows whose ``CAND_ID`` is an FEC candidate id carried by a
   **current member** canonical (the ``fec.candidate`` alias written by
   the roster pass).
3. Maps ``CMTE_ID`` → the committee canonical already in the graph via
   its ``fec.disbursement.recipient`` / ``fec.committee`` /
   ``fec.affiliated_committee`` alias.
4. Emits a cited ``affiliated_with`` edge **committee → member**.

Names are never used — every hop is an external id. A committee that
resolves to more than one *current* member (a joint fundraising
committee shared by two sitting members) is REFUSED rather than
attributed to a guess; it lands on the review list.

``/committee/{id}/candidates/`` (the P5.3 route in
:mod:`app.services.ingest.link_committees_to_candidates`) stays available
as an opt-in ``--api-fallback`` for committees the bulk cycles miss —
pre-2016 linkages, mostly.

Read-gate (RG1): ``--batch-id`` stamps every edge this pass creates
``publication_state=staged``. Existing (published) bridge edges from the
P5.3 run are reused untouched, never re-stamped.
"""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import logging
import zipfile
from collections import defaultdict
from dataclasses import dataclass, field

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import (
    CanonicalEdge,
    CanonicalEntity,
    EdgeRelation,
    EntityAlias,
    PublicationState,
    SourceCitation,
    SourceKind,
)
from app.services.ingest.fec import _api_key

logger = logging.getLogger(__name__)

_CCL_URL = "https://www.fec.gov/files/bulk-downloads/{cycle}/ccl{yy}.zip"
_COMMITTEE_URL = "https://www.fec.gov/data/committee/{committee_id}/"

#: Election cycles pulled by default. Contributions in the corpus reach
#: back to the 2016 cycle; each file is <100KB.
DEFAULT_CYCLES: tuple[int, ...] = (2016, 2018, 2020, 2022, 2024, 2026)

#: Alias namespaces whose ``source_id`` is an FEC **committee** id.
COMMITTEE_NAMESPACES = (
    "fec.disbursement.recipient",
    "fec.committee",
    "fec.affiliated_committee",
)

#: FEC committee designations, most-attributable first. ``P`` (principal
#: campaign committee) is the member's own committee; ``D`` is their
#: leadership PAC; ``J`` is a joint fundraiser shared with others.
_DESIGNATION_RANK = {"P": 0, "A": 1, "D": 2, "J": 3, "U": 4}

DESIGNATION_LABELS = {
    "P": "principal campaign committee",
    "A": "authorized committee",
    "D": "leadership PAC",
    "J": "joint fundraising committee",
    "U": "unauthorized committee",
    "B": "lobbyist/registrant PAC",
}


# ─── pure linkage parsing (hermetically testable) ───────────────────────


@dataclass(frozen=True)
class CclRow:
    """One row of FEC's candidate-committee linkage file."""

    candidate_id: str
    committee_id: str
    committee_type: str
    designation: str
    cycle: int


@dataclass
class CommitteeLink:
    """One committee resolved to exactly one current member."""

    committee_id: str
    candidate_id: str
    member_canonical_id: str
    designation: str
    committee_type: str
    cycles: list[int] = field(default_factory=list)

    @property
    def designation_label(self) -> str:
        """Human label for the designation code."""
        return DESIGNATION_LABELS.get(self.designation, self.designation)


def parse_ccl(text: str, cycle: int) -> list[CclRow]:
    """Parse one ``ccl.txt`` payload.

    Rows whose ``CAND_ID`` is not a candidate id (the file carries a few
    committee-to-itself rows where CAND_ID repeats the committee id) are
    dropped — a candidate id starts with H, S or P.
    """
    out: list[CclRow] = []
    for line in text.splitlines():
        parts = line.split("|")
        if len(parts) < 6:
            continue
        cand, _cand_yr, _fec_yr, cmte, cmte_tp, cmte_dsgn = parts[:6]
        if not cand or cand[0].upper() not in ("H", "S", "P"):
            continue
        if not cmte.startswith("C"):
            continue
        out.append(
            CclRow(
                candidate_id=cand.strip(),
                committee_id=cmte.strip(),
                committee_type=(cmte_tp or "").strip(),
                designation=(cmte_dsgn or "").strip().upper(),
                cycle=cycle,
            )
        )
    return out


def select_member_links(
    rows: list[CclRow], candidate_to_member: dict[str, str]
) -> tuple[dict[str, CommitteeLink], list[dict[str, object]]]:
    """Collapse linkage rows to at most ONE member per committee.

    ``candidate_to_member`` maps an FEC candidate id → the member's
    canonical id (from the roster's ``fec.candidate`` aliases).

    Returns ``(links, ambiguous)``. A committee that resolves to two
    different *member canonicals* is refused and reported — attributing
    a shared joint-fundraising committee to one of them would invent a
    fact. A committee linked to the same member under several candidate
    ids (a member who ran for House then Senate) is NOT ambiguous.
    """
    by_committee: dict[str, list[CclRow]] = defaultdict(list)
    for row in rows:
        if row.candidate_id in candidate_to_member:
            by_committee[row.committee_id].append(row)

    links: dict[str, CommitteeLink] = {}
    ambiguous: list[dict[str, object]] = []
    for committee_id, crows in by_committee.items():
        members = {candidate_to_member[r.candidate_id] for r in crows}
        if len(members) > 1:
            ambiguous.append(
                {
                    "committee_id": committee_id,
                    "member_canonical_ids": sorted(members),
                    "candidate_ids": sorted({r.candidate_id for r in crows}),
                    "reason": "committee links to multiple current members",
                }
            )
            continue
        best = min(
            crows,
            key=lambda r: (_DESIGNATION_RANK.get(r.designation, 9), -r.cycle),
        )
        links[committee_id] = CommitteeLink(
            committee_id=committee_id,
            candidate_id=best.candidate_id,
            member_canonical_id=candidate_to_member[best.candidate_id],
            designation=best.designation,
            committee_type=best.committee_type,
            cycles=sorted({r.cycle for r in crows}),
        )
    return links, ambiguous


# ─── stats ──────────────────────────────────────────────────────────────


@dataclass
class LinkStats:
    """Counters for one linkage pass."""

    cycles_loaded: list[int] = field(default_factory=list)
    ccl_rows: int = 0
    committees_linked_to_members: int = 0
    committees_ambiguous: int = 0
    committees_in_graph: int = 0
    committees_not_in_graph: int = 0
    edges_created: int = 0
    edges_reused: int = 0
    citations_created: int = 0
    edges_staged: int = 0
    #: Bridge edges whose member canonical is not surface_mode='open'.
    #: The read path hides them; counted so the report is honest.
    edges_to_non_open_members: int = 0
    api_fallback_calls: int = 0
    api_fallback_edges: int = 0
    errors: int = 0
    ambiguous_sample: list[dict[str, object]] = field(default_factory=list)


# ─── fetch ──────────────────────────────────────────────────────────────


async def fetch_ccl(cycle: int, client: httpx.AsyncClient) -> list[CclRow]:
    """Download + parse one cycle's candidate-committee linkage file."""
    url = _CCL_URL.format(cycle=cycle, yy=f"{cycle % 100:02d}")
    r = await client.get(url, follow_redirects=True, timeout=120.0)
    r.raise_for_status()
    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
        name = zf.namelist()[0]
        text = zf.read(name).decode("utf8", errors="replace")
    return parse_ccl(text, cycle)


# ─── DB helpers ─────────────────────────────────────────────────────────


async def _candidate_to_member(session: AsyncSession) -> dict[str, str]:
    """FEC candidate id → member canonical id, from the roster aliases."""
    rows = (
        await session.execute(
            select(EntityAlias.source_id, EntityAlias.canonical_id).where(
                EntityAlias.source_system == "fec.candidate"
            )
        )
    ).all()
    return {r[0]: r[1] for r in rows}


async def _committee_to_canonical(session: AsyncSession) -> dict[str, str]:
    """FEC committee id → canonical id for every committee in the graph.

    A canonical may carry the same committee id under several
    namespaces; the first one wins (they point at the same canonical by
    construction — the alias unique index is on (system, id), and the P2
    dedup pass collapsed committee splits).
    """
    rows = (
        await session.execute(
            select(EntityAlias.source_id, EntityAlias.canonical_id).where(
                EntityAlias.source_system.in_(COMMITTEE_NAMESPACES),
                EntityAlias.source_id.like("C%"),
            )
        )
    ).all()
    out: dict[str, str] = {}
    for source_id, canonical_id in rows:
        out.setdefault(source_id, canonical_id)
    return out


async def _emit_bridge_edge(
    session: AsyncSession,
    link: CommitteeLink,
    committee_canonical: str,
    *,
    batch_id: str | None,
    stats: LinkStats,
    via: str,
) -> None:
    """Cited ``affiliated_with`` committee → member edge.

    Idempotent on (source, target, relation): an existing edge — for
    instance one of the 39 published bridges from the P5.3 run — is
    reused untouched, never re-stamped into the staged batch.
    """
    if committee_canonical == link.member_canonical_id:
        # Degenerate: the committee and the member resolved to the same
        # canonical. A self-loop is not a fact about anything.
        return
    existing = (
        await session.execute(
            select(CanonicalEdge).where(
                CanonicalEdge.source_id == committee_canonical,
                CanonicalEdge.target_id == link.member_canonical_id,
                CanonicalEdge.relation == EdgeRelation.AFFILIATED_WITH.value,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        stats.edges_reused += 1
        return
    edge = CanonicalEdge(
        source_id=committee_canonical,
        target_id=link.member_canonical_id,
        relation=EdgeRelation.AFFILIATED_WITH.value,
        weight=1.0,
        edge_metadata={
            "fec_committee_id": link.committee_id,
            "fec_candidate_id": link.candidate_id,
            "designation": link.designation,
            "designation_label": link.designation_label,
            "committee_type": link.committee_type,
            "cycles": link.cycles,
            "linkage": via,
        },
        publication_state=(
            PublicationState.STAGED.value if batch_id
            else PublicationState.PUBLISHED.value
        ),
        batch_id=batch_id,
    )
    session.add(edge)
    await session.flush()
    session.add(
        SourceCitation(
            edge_id=edge.id,
            kind=SourceKind.FEC_FILING.value,
            citation_ref=(
                f"FEC candidate-committee linkage {link.candidate_id}"
                f"↔{link.committee_id} ({link.designation_label})"
            ),
            citation_url=_COMMITTEE_URL.format(committee_id=link.committee_id),
        )
    )
    stats.edges_created += 1
    stats.citations_created += 1
    if batch_id:
        stats.edges_staged += 1


# ─── the pass ───────────────────────────────────────────────────────────


async def link_contributions_to_members(
    batch_id: str | None = None,
    *,
    cycles: tuple[int, ...] = DEFAULT_CYCLES,
    api_fallback: bool = False,
    api_limit: int = 500,
    api_sleep_s: float = 0.6,
) -> LinkStats:
    """Emit committee → member ``affiliated_with`` bridges for every
    committee FEC links to a current member. Idempotent + re-runnable."""
    stats = LinkStats()
    rows: list[CclRow] = []
    async with httpx.AsyncClient(timeout=120.0) as client:
        for cycle in cycles:
            try:
                cycle_rows = await fetch_ccl(cycle, client)
            except Exception:
                logger.exception("ccl fetch failed for cycle %s", cycle)
                stats.errors += 1
                continue
            rows.extend(cycle_rows)
            stats.cycles_loaded.append(cycle)
            logger.info("ccl %s: %d linkage rows", cycle, len(cycle_rows))
    stats.ccl_rows = len(rows)

    sm = get_sessionmaker()
    async with sm() as session:
        cand_to_member = await _candidate_to_member(session)
        committee_to_canonical = await _committee_to_canonical(session)

    links, ambiguous = select_member_links(rows, cand_to_member)
    stats.committees_linked_to_members = len(links)
    stats.committees_ambiguous = len(ambiguous)
    stats.ambiguous_sample = ambiguous[:20]

    async with sm() as session:
        # surface_mode of every member we are about to bridge to — the
        # count of non-open targets goes in the report.
        member_modes = dict(
            (
                await session.execute(
                    select(CanonicalEntity.id, CanonicalEntity.surface_mode)
                )
            ).all()
        )
        for committee_id, link in links.items():
            canonical = committee_to_canonical.get(committee_id)
            if canonical is None:
                stats.committees_not_in_graph += 1
                continue
            stats.committees_in_graph += 1
            try:
                before = stats.edges_created
                await _emit_bridge_edge(
                    session, link, canonical,
                    batch_id=batch_id, stats=stats, via="fec.ccl",
                )
                if (
                    stats.edges_created > before
                    and member_modes.get(link.member_canonical_id) != "open"
                ):
                    stats.edges_to_non_open_members += 1
            except Exception:
                logger.exception(
                    "bridge failed committee=%s member=%s",
                    committee_id, link.member_canonical_id,
                )
                stats.errors += 1
        await session.commit()

    if api_fallback:
        await _api_fallback_pass(
            links, committee_to_canonical, cand_to_member,
            batch_id=batch_id, stats=stats,
            limit=api_limit, sleep_s=api_sleep_s,
        )
    return stats


async def _unlinked_contribution_committees(
    session: AsyncSession, already: set[str]
) -> list[tuple[str, str]]:
    """``(committee_id, canonical_id)`` for committees that RECEIVE a
    ``contributes_to`` edge but have no member bridge yet. Only these are
    worth an API call — an unlinked committee nobody gave to changes no
    attribution."""
    rows = (
        await session.execute(
            select(EntityAlias.source_id, EntityAlias.canonical_id)
            .join(
                CanonicalEdge,
                CanonicalEdge.target_id == EntityAlias.canonical_id,
            )
            .where(
                CanonicalEdge.relation == EdgeRelation.CONTRIBUTES_TO.value,
                EntityAlias.source_system.in_(COMMITTEE_NAMESPACES),
                EntityAlias.source_id.like("C%"),
            )
            .distinct()
        )
    ).all()
    return [(r[0], r[1]) for r in rows if r[0] not in already]


async def _api_fallback_pass(
    links: dict[str, CommitteeLink],
    committee_to_canonical: dict[str, str],
    cand_to_member: dict[str, str],
    *,
    batch_id: str | None,
    stats: LinkStats,
    limit: int,
    sleep_s: float,
) -> None:
    """Opt-in: ask ``/committee/{id}/candidates/`` about the committees
    the bulk cycles didn't cover (pre-2016 linkages, mostly).

    Fail-open per committee, retry once on a 429 — FEC's public limit is
    1000/hour on a standard key.
    """
    sm = get_sessionmaker()
    async with sm() as session:
        todo = await _unlinked_contribution_committees(session, set(links))
    todo = todo[:limit]
    logger.info("api fallback: %d committees to probe", len(todo))
    async with httpx.AsyncClient(timeout=30.0) as client:
        for committee_id, canonical in todo:
            payload = None
            for attempt in range(2):
                try:
                    r = await client.get(
                        "https://api.open.fec.gov/v1/committee/"
                        f"{committee_id}/candidates/",
                        params={"api_key": _api_key(), "per_page": 20},
                    )
                    if r.status_code == 429 and attempt == 0:
                        logger.info("FEC 429 — sleeping 60s + retrying once")
                        await asyncio.sleep(60.0)
                        continue
                    if r.status_code != 200:
                        break
                    payload = r.json()
                    break
                except Exception:  # noqa: BLE001
                    logger.debug("committee %s probe failed", committee_id)
                    break
            stats.api_fallback_calls += 1
            if sleep_s:
                await asyncio.sleep(sleep_s)
            if not payload:
                continue
            members = {
                cand_to_member[c["candidate_id"]]
                for c in (payload.get("results") or [])
                if c.get("candidate_id") in cand_to_member
            }
            if len(members) != 1:
                if len(members) > 1:
                    stats.committees_ambiguous += 1
                continue
            cand_id = next(
                c["candidate_id"]
                for c in payload["results"]
                if c.get("candidate_id") in cand_to_member
            )
            link = CommitteeLink(
                committee_id=committee_id,
                candidate_id=cand_id,
                member_canonical_id=next(iter(members)),
                designation="",
                committee_type="",
                cycles=[],
            )
            async with sm() as session:
                before = stats.edges_created
                try:
                    await _emit_bridge_edge(
                        session, link, canonical,
                        batch_id=batch_id, stats=stats,
                        via="fec.api.committee_candidates",
                    )
                    await session.commit()
                except Exception:
                    await session.rollback()
                    stats.errors += 1
                    continue
            if stats.edges_created > before:
                stats.api_fallback_edges += 1


def main() -> None:
    """CLI — ``python -m app.services.ingest.congress_money_link``."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    ap = argparse.ArgumentParser(
        description="Resolve FEC contributions to the member they fund"
    )
    ap.add_argument(
        "--batch-id",
        default=None,
        help="RG1 read-gate batch — stamps every new bridge edge staged.",
    )
    ap.add_argument(
        "--cycles",
        default=",".join(str(c) for c in DEFAULT_CYCLES),
        help="Comma-separated FEC election cycles to load.",
    )
    ap.add_argument(
        "--api-fallback",
        action="store_true",
        help="Probe /committee/{id}/candidates/ for committees the bulk "
             "cycles miss (needs FEC_API_KEY; rate-limited).",
    )
    ap.add_argument("--api-limit", type=int, default=500)
    ap.add_argument("--json-report", default=None)
    args = ap.parse_args()

    cycles = tuple(
        int(c.strip()) for c in args.cycles.split(",") if c.strip()
    )
    stats = asyncio.run(
        link_contributions_to_members(
            batch_id=args.batch_id,
            cycles=cycles,
            api_fallback=args.api_fallback,
            api_limit=args.api_limit,
        )
    )
    logger.info("congress money linkage done: %s", stats)
    print(json.dumps(stats.__dict__, indent=2, default=str))
    if args.json_report:
        with open(args.json_report, "w") as fh:
            json.dump(stats.__dict__, fh, indent=2, default=str)


if __name__ == "__main__":
    main()
