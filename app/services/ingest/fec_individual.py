"""P1.6.2 — FEC individual-contributor mode, keyed on contributor IDENTITY.

A mega-donor's personal political giving lives in FEC **Schedule A**
(itemized receipts). Unlike a committee, an individual contributor has
no FEC id: the only handles are the strings a filer typed —
``contributor_name``, ``contributor_city``, ``contributor_state``,
``contributor_employer``, ``contributor_occupation``. The endpoint's
``contributor_name`` parameter is a FULL-TEXT search, so a query is a
fuzzy net, not a selector.

Why the old path had to be replaced
-----------------------------------
``fec.ingest_individual_contributor`` (P4 E) took every row the
fuzzy search returned and attributed it to the donor. Measured against
live FEC data on 2026-08-21, a ``"THIEL, PETER"`` sweep returns 464
rows of which only 296 are Peter Thiel. The rest are **other real,
private people**: ``THIELEN, PETER`` (Herdt Consulting, FL),
``THIELKE, PETER`` (a water company, CA), ``THIEL, PETER`` of Bay City
MI (a GM factory worker), ``THIEL, PETER`` of Ottertail MN (a
self-employed contractor). Attributing a private individual's giving to
a billionaire is both a data error and a privacy incident, so the
selection rule here is a **predicate, not a search**.

The predicate — every clause fail-closed
----------------------------------------
A row is accepted only if ALL of:

1. **Surname matches exactly** (normalized). This alone rejects
   ``THIELEN`` / ``THIELKE`` / ``THIELE``.
2. **Given name is one of the declared forms.** ``THIEL, PETER`` and
   ``THIEL, PETER MR.`` pass; ``THIEL, JOHN`` does not.
3. **Every remaining name token is an accepted middle token** — a
   middle initial or an honorific. ``THIELEN, JOHN PETER`` cannot slip
   in through a middle-name position.
4. **The row is corroborated by an AFFILIATION** — ``contributor_
   employer`` or ``contributor_occupation`` matches one of the donor's
   declared affiliation patterns. This is the clause that separates the
   Bay City GM worker named Peter Thiel from the Peter Thiel who reports
   ``THIEL CAPITAL LLC``.
5. **The state is one of the declared states**, when the identity
   declares any.

Everything refused is counted by reason and sampled into the report, so
the operator sees the near-misses (a real row with an employer we have
not declared shows up as ``no_affiliation_match_in_state``) rather than
silently losing them.

Double-counting
---------------
Schedule A itemizes an earmarked contribution TWICE — once against the
conduit (WinRed / ActBlue) and once against the ultimate recipient —
with the second row flagged ``memo_code='X'``. Memo rows are cited-able
but not additive, so they are skipped and counted; summing them would
inflate the donor's total by ~3%.

What it emits
-------------
* ``contributes_to`` donor → recipient committee, one edge per
  committee, weight = summed non-memo receipts, **one citation per
  transaction** (``sub_id``) — and the citation write is idempotent, so
  re-running does not duplicate citations or double the weight.
* ``affiliated_with`` donor → employer organization, but ONLY when the
  reported employer resolves to an **already-anchored** organization.
  The employer field is free text; this pass never mints an
  organization from it. The citation is the FEC transaction on which the
  donor reported that employer.

Generalization (P1.7)
---------------------
:class:`DonorIdentity` is data. Adding Elon Musk for P1.7 is one entry
in :data:`DONOR_IDENTITIES` — surname ``MUSK``, given ``ELON``,
affiliation patterns for Tesla / SpaceX / X Corp / Boring / Neuralink —
plus a person anchor in ``domain_anchors``. No code change.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
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
    EntityType,
    PublicationState,
    SourceCitation,
    SourceKind,
    SurfaceMode,
)
from app.services.graph.base import normalize_name
from app.services.ingest.domain_anchors import attach_alias

logger = logging.getLogger(__name__)

_FEC_BASE = "https://api.open.fec.gov/v1"
_DEFAULT_KEY = "DEMO_KEY"

#: Name tokens that may appear AFTER the given name without making the
#: row a different person: a middle initial, or an honorific/suffix.
DEFAULT_MIDDLE_TOKENS: frozenset[str] = frozenset(
    {
        "MR", "MRS", "MS", "DR", "PROF",
        "JR", "SR", "II", "III", "IV",
    }
)

#: ``memo_code='X'`` marks a Schedule A row as a memo entry — the same
#: dollars itemized a second time (earmark conduit ↔ ultimate
#: recipient). Never summed.
MEMO_CODE = "X"


def _api_key() -> str:
    """FEC API key from env — DEMO_KEY if unset (heavily rate-limited)."""
    return os.environ.get("FEC_API_KEY") or _DEFAULT_KEY


# ─── the identity predicate ─────────────────────────────────────────────


@dataclass(frozen=True)
class DonorIdentity:
    """A declarative, fail-closed identity for one individual contributor.

    ``affiliation_patterns`` are regexes matched (case-insensitively)
    against ``contributor_employer`` and ``contributor_occupation``
    joined together — filers put the employer in the occupation field
    and vice versa often enough that testing them separately loses real
    rows.
    """

    label: str
    last_name: str
    first_names: frozenset[str]
    affiliation_patterns: tuple[str, ...]
    states: frozenset[str] = frozenset()
    middle_tokens: frozenset[str] = DEFAULT_MIDDLE_TOKENS
    #: FEC full-text queries used to FETCH candidate rows. Widening this
    #: cannot widen what is accepted — the predicate decides that.
    search_queries: tuple[str, ...] = ()
    #: Two-year transaction periods to sweep (FEC partitions Schedule A
    #: by cycle, so each has to be requested separately).
    two_year_periods: tuple[int, ...] = (2026, 2024, 2022, 2020, 2018, 2016)
    #: Anchor labels whose canonical an accepted employer string may map
    #: to, for the cited ``affiliated_with`` edge. Keys are regexes.
    employer_anchor_patterns: tuple[tuple[str, str], ...] = ()

    @property
    def queries(self) -> tuple[str, ...]:
        """Search queries, defaulting to ``LAST, FIRST`` per given name."""
        if self.search_queries:
            return self.search_queries
        return tuple(
            f"{self.last_name}, {first}" for first in sorted(self.first_names)
        )


@dataclass(frozen=True)
class IdentityCheck:
    """Outcome of running the predicate over one Schedule A row."""

    accepted: bool
    reason: str


def split_contributor_name(raw: str) -> tuple[str, list[str]]:
    """Split an FEC ``contributor_name`` into ``(surname, given tokens)``.

    FEC prints ``LAST, FIRST MIDDLE`` for individuals. Rows without a
    comma (a handful of malformed filings) return an empty surname,
    which the predicate then refuses — an unparsable name is not
    identity.
    """
    text = (raw or "").upper().replace(".", " ")
    if "," not in text:
        return "", []
    last, rest = text.split(",", 1)
    return " ".join(last.split()), rest.split()


def check_identity(row: dict, identity: DonorIdentity) -> IdentityCheck:
    """Does this Schedule A row belong to ``identity``? Fail-closed.

    The clause ORDER matters for the report: a row is bucketed by the
    FIRST rule that refused it, so ``surname_mismatch`` (a different
    family) never masks the more interesting
    ``no_affiliation_match_in_state`` (probably our donor, employer
    string we have not declared).
    """
    surname, given = split_contributor_name(row.get("contributor_name") or "")
    if not surname:
        return IdentityCheck(False, "unparsable_name")
    if surname != identity.last_name.upper():
        return IdentityCheck(False, "surname_mismatch")
    if not given:
        return IdentityCheck(False, "no_given_name")
    if given[0] not in {f.upper() for f in identity.first_names}:
        return IdentityCheck(False, "given_name_mismatch")
    extra = [t for t in given[1:] if t not in identity.middle_tokens and len(t) > 1]
    if extra:
        return IdentityCheck(False, "unexpected_name_tokens")

    state = (row.get("contributor_state") or "").upper()
    in_state = (not identity.states) or state in identity.states

    blob = (
        f"{row.get('contributor_employer') or ''} "
        f"{row.get('contributor_occupation') or ''}"
    ).upper()
    affiliated = any(
        re.search(p, blob, re.I) for p in identity.affiliation_patterns
    )
    if not affiliated:
        return IdentityCheck(
            False,
            "no_affiliation_match_in_state" if in_state
            else "no_affiliation_match",
        )
    if not in_state:
        return IdentityCheck(False, "state_mismatch")
    return IdentityCheck(True, "ok")


# ─── declared donors ────────────────────────────────────────────────────

THIEL = DonorIdentity(
    label="Peter Thiel",
    last_name="THIEL",
    first_names=frozenset({"PETER"}),
    # Verified against live Schedule A on 2026-08-21: Thiel's reported
    # employer across cycles is Thiel Capital, Clarium Capital (his
    # pre-2011 hedge fund — filers also typed CLARIUM CAPITOL and
    # CALRIUM CAPITAL), Founders Fund, or Facebook (he sat on that
    # board). The ``PRESIDENT``/``CHAIRMAN`` occupation appears with the
    # employer field swapped, which the joined blob still matches.
    affiliation_patterns=(
        r"THIEL CAPITAL",
        r"C[AL]{2}RIUM\s+CAPIT[AO]L",
        r"FOUNDERS FUND",
        r"PALANTIR",
        r"FACEBOOK",
    ),
    states=frozenset({"CA", "FL", "NY"}),
    search_queries=("THIEL, PETER",),
    employer_anchor_patterns=(
        (r"FOUNDERS FUND", "Founders Fund"),
        (r"PALANTIR", "Palantir Technologies"),
    ),
)

#: Every declared mega-donor. P1.7 adds Musk here.
DONOR_IDENTITIES: dict[str, DonorIdentity] = {
    "thiel": THIEL,
}


# ─── stats ──────────────────────────────────────────────────────────────


@dataclass
class IndividualContribStats:
    """Counters for one individual-contributor sweep."""

    donor: str = ""
    donor_canonical_id: str = ""
    rows_fetched: int = 0
    rows_unique: int = 0
    rows_accepted: int = 0
    rows_refused: int = 0
    memo_rows_skipped: int = 0
    refused_by_reason: dict[str, int] = field(default_factory=dict)
    #: A sample of refused rows per reason — the operator review list.
    refused_sample: dict[str, list[dict]] = field(default_factory=dict)
    #: Distinct (name, city, state, employer) shapes ACCEPTED, so an
    #: operator can eyeball that only one real person got through.
    accepted_shapes: list[dict] = field(default_factory=list)
    committees_matched: int = 0
    committees_created: int = 0
    edges_created: int = 0
    edges_reused: int = 0
    citations_created: int = 0
    citations_skipped_already_cited: int = 0
    total_contributed: float = 0.0
    entities_staged: int = 0
    edges_staged: int = 0
    affiliation_edges_created: int = 0
    affiliation_edges_reused: int = 0
    affiliation_targets_unanchored: dict[str, int] = field(default_factory=dict)
    alias_conflicts: list[dict] = field(default_factory=list)
    #: Per-committee totals — the report's headline table.
    by_committee: list[dict] = field(default_factory=list)
    errors: int = 0


# ─── FEC fetch ──────────────────────────────────────────────────────────


async def _fec_get(client: httpx.AsyncClient, path: str, **params) -> dict:
    """One GET to api.open.fec.gov with the API key attached."""
    params.setdefault("api_key", _api_key())
    r = await client.get(f"{_FEC_BASE}{path}", params=params)
    r.raise_for_status()
    return r.json()


async def fetch_schedule_a(
    identity: DonorIdentity,
    *,
    max_rows_per_period: int = 2000,
    stats: IndividualContribStats,
) -> dict[str, dict]:
    """Fetch every Schedule A row the identity's queries reach.

    Returns ``{sub_id: row}`` — deduped, because the same transaction can
    surface under two queries and under two two-year periods. Uses FEC's
    keyset (``last_index``) pagination, which is the only pagination that
    reaches past page 100 on this endpoint.
    """
    rows: dict[str, dict] = {}
    async with httpx.AsyncClient(timeout=60.0) as client:
        for query in identity.queries:
            for period in identity.two_year_periods:
                last_index: str | None = None
                last_date: str | None = None
                fetched = 0
                while fetched < max_rows_per_period:
                    params = {
                        "contributor_name": query,
                        "two_year_transaction_period": period,
                        "per_page": 100,
                        "sort": "-contribution_receipt_date",
                    }
                    if last_index:
                        params["last_index"] = last_index
                        params["last_contribution_receipt_date"] = last_date
                    try:
                        payload = await _fec_get(
                            client, "/schedules/schedule_a/", **params
                        )
                    except Exception:
                        logger.exception(
                            "[%s] schedule_a fetch failed q=%r period=%d",
                            identity.label, query, period,
                        )
                        stats.errors += 1
                        break
                    batch = payload.get("results") or []
                    if not batch:
                        break
                    for row in batch:
                        sub_id = str(row.get("sub_id") or "")
                        if sub_id:
                            rows[sub_id] = row
                    fetched += len(batch)
                    stats.rows_fetched += len(batch)
                    pagination = payload.get("pagination") or {}
                    indexes = pagination.get("last_indexes") or {}
                    last_index = indexes.get("last_index")
                    last_date = indexes.get("last_contribution_receipt_date")
                    if not last_index or fetched >= (pagination.get("count") or 0):
                        break
                    await asyncio.sleep(0.2)
    stats.rows_unique = len(rows)
    return rows


# ─── graph writes ───────────────────────────────────────────────────────


async def _donor_canonical(
    session: AsyncSession,
    identity: DonorIdentity,
    *,
    batch_id: str | None,
    stats: IndividualContribStats,
) -> str | None:
    """Canonical id for the donor — the ANCHOR's node, never a new one.

    The donor must already be anchored by ``domain_anchors`` (which keys
    a person on their SEC reporting-owner CIK). Refusing to mint one here
    is deliberate: a donor invented from a search string is exactly the
    name-keyed identity P1.6 exists to remove. Returns None when the
    anchor is missing, and the caller reports it rather than guessing.
    """
    from app.models import AnchorRegistry

    row = (
        await session.execute(
            select(AnchorRegistry).where(
                AnchorRegistry.label == identity.label,
                AnchorRegistry.entity_type == EntityType.PERSON.value,
            )
        )
    ).scalar_one_or_none()
    if row is None or not row.canonical_id:
        return None
    ent = (
        await session.execute(
            select(CanonicalEntity).where(CanonicalEntity.id == row.canonical_id)
        )
    ).scalar_one_or_none()
    if ent is None:
        return None
    del batch_id, stats  # resolution only; the anchor pass owns creation
    return ent.id


async def _recipient_canonical(
    session: AsyncSession,
    committee: dict,
    *,
    batch_id: str | None,
    stats: IndividualContribStats,
) -> str:
    """Canonical id for a recipient committee, keyed on ``committee_id``.

    ``fec.committee`` is the identity namespace the P2 dedup pass already
    treats as authoritative, so a committee that later shows up through
    the PAC-mode ingester lands on this same node.
    """
    committee_id = (committee.get("committee_id") or "").strip().upper()
    name = (committee.get("name") or "").strip()
    existing = (
        await session.execute(
            select(EntityAlias.canonical_id).where(
                EntityAlias.source_system == "fec.committee",
                EntityAlias.source_id == committee_id,
            )
        )
    ).scalar_one_or_none()
    if existing:
        stats.committees_matched += 1
        return existing
    # The disbursement-recipient namespace is the same identity space
    # (``dedup_pass._IDENTITY_NAMESPACE``) — reuse rather than fork.
    existing = (
        await session.execute(
            select(EntityAlias.canonical_id).where(
                EntityAlias.source_system == "fec.disbursement.recipient",
                EntityAlias.source_id == committee_id,
            )
        )
    ).scalar_one_or_none()
    if existing:
        stats.committees_matched += 1
        await attach_alias(
            session, existing, "fec.committee", committee_id, name,
            kind_hint="committee", stats=stats, label=name,
        )
        return existing

    kind = (
        committee.get("committee_type_full")
        or committee.get("committee_type")
        or ""
    ).upper()
    entity_type = (
        EntityType.PAC.value
        if "PAC" in kind or "SUPER" in kind or "COMMITTEE" in kind
        else EntityType.ORGANIZATION.value
    )
    norm = normalize_name(name)
    ent = CanonicalEntity(
        canonical_name=name,
        canonical_name_normalized=norm or name.lower(),
        type=entity_type,
        surface_mode=SurfaceMode.OPEN.value,
        publication_state=(
            PublicationState.STAGED.value if batch_id
            else PublicationState.PUBLISHED.value
        ),
        batch_id=batch_id,
    )
    session.add(ent)
    await session.flush()
    stats.committees_created += 1
    if batch_id:
        stats.entities_staged += 1
    await attach_alias(
        session, ent.id, "fec.committee", committee_id, name,
        kind_hint="committee", stats=stats, label=name,
    )
    return ent.id


def contribution_citation_url(committee_id: str, sub_id: str) -> str:
    """Public FEC receipts page for one itemized transaction."""
    return (
        "https://www.fec.gov/data/receipts/individual-contributions/"
        f"?committee_id={committee_id}&transaction_id={sub_id}"
    )


async def _emit_contribution(
    session: AsyncSession,
    *,
    donor_id: str,
    recipient_id: str,
    row: dict,
    committee_id: str,
    batch_id: str | None,
    stats: IndividualContribStats,
) -> None:
    """Create-or-reuse a cited ``contributes_to`` edge for one receipt.

    IDEMPOTENT on the transaction: the weight is incremented only when
    the ``sub_id`` citation is genuinely new. The pre-P1.6 emitter added
    a citation row unconditionally, so every re-run duplicated citations
    and inflated the dollar weight.
    """
    sub_id = str(row.get("sub_id") or "")
    amount = float(row.get("contribution_receipt_amount") or 0.0)

    edge = (
        await session.execute(
            select(CanonicalEdge).where(
                CanonicalEdge.source_id == donor_id,
                CanonicalEdge.target_id == recipient_id,
                CanonicalEdge.relation == EdgeRelation.CONTRIBUTES_TO.value,
            )
        )
    ).scalar_one_or_none()
    if edge is None:
        edge = CanonicalEdge(
            source_id=donor_id,
            target_id=recipient_id,
            relation=EdgeRelation.CONTRIBUTES_TO.value,
            weight=0.0,
            edge_metadata={
                "source": "fec.schedule_a",
                "committee_id": committee_id,
                "mode": "individual_contributor",
            },
            publication_state=(
                PublicationState.STAGED.value if batch_id
                else PublicationState.PUBLISHED.value
            ),
            batch_id=batch_id,
        )
        session.add(edge)
        await session.flush()
        stats.edges_created += 1
        if batch_id:
            stats.edges_staged += 1
    else:
        stats.edges_reused += 1

    already = (
        await session.execute(
            select(SourceCitation).where(
                SourceCitation.edge_id == edge.id,
                SourceCitation.citation_ref == sub_id,
            )
        )
    ).scalar_one_or_none()
    if already is not None:
        stats.citations_skipped_already_cited += 1
        return
    session.add(
        SourceCitation(
            edge_id=edge.id,
            kind=SourceKind.FEC_FILING.value,
            citation_url=contribution_citation_url(committee_id, sub_id),
            citation_ref=sub_id,
        )
    )
    edge.weight = float((edge.weight or 0.0) + amount)
    stats.citations_created += 1
    stats.total_contributed += amount


async def _emit_employer_affiliation(
    session: AsyncSession,
    *,
    donor_id: str,
    identity: DonorIdentity,
    row: dict,
    committee_id: str,
    batch_id: str | None,
    stats: IndividualContribStats,
) -> None:
    """Cited ``affiliated_with`` donor → employer, anchored orgs only.

    The employer string is free text a filer typed, so it can NEVER mint
    an organization. It is only allowed to point at a canonical that the
    anchor pass already established on an external id; anything else is
    counted in ``affiliation_targets_unanchored`` and dropped.
    """
    from app.models import AnchorRegistry

    blob = (
        f"{row.get('contributor_employer') or ''} "
        f"{row.get('contributor_occupation') or ''}"
    ).upper()
    for pattern, anchor_label in identity.employer_anchor_patterns:
        if not re.search(pattern, blob, re.I):
            continue
        anchor = (
            await session.execute(
                select(AnchorRegistry).where(AnchorRegistry.label == anchor_label)
            )
        ).scalars().first()
        if anchor is None or not anchor.canonical_id:
            stats.affiliation_targets_unanchored[anchor_label] = (
                stats.affiliation_targets_unanchored.get(anchor_label, 0) + 1
            )
            continue
        edge = (
            await session.execute(
                select(CanonicalEdge).where(
                    CanonicalEdge.source_id == donor_id,
                    CanonicalEdge.target_id == anchor.canonical_id,
                    CanonicalEdge.relation == EdgeRelation.AFFILIATED_WITH.value,
                )
            )
        ).scalar_one_or_none()
        if edge is None:
            edge = CanonicalEdge(
                source_id=donor_id,
                target_id=anchor.canonical_id,
                relation=EdgeRelation.AFFILIATED_WITH.value,
                weight=0.0,
                edge_metadata={
                    "source": "fec.schedule_a.contributor_employer",
                    "reported_employer": (
                        row.get("contributor_employer") or ""
                    ).strip(),
                    "reported_occupation": (
                        row.get("contributor_occupation") or ""
                    ).strip(),
                },
                publication_state=(
                    PublicationState.STAGED.value if batch_id
                    else PublicationState.PUBLISHED.value
                ),
                batch_id=batch_id,
            )
            session.add(edge)
            await session.flush()
            stats.affiliation_edges_created += 1
            if batch_id:
                stats.edges_staged += 1
        else:
            stats.affiliation_edges_reused += 1

        sub_id = str(row.get("sub_id") or "")
        already = (
            await session.execute(
                select(SourceCitation).where(
                    SourceCitation.edge_id == edge.id,
                    SourceCitation.citation_ref == sub_id,
                )
            )
        ).scalar_one_or_none()
        if already is None:
            session.add(
                SourceCitation(
                    edge_id=edge.id,
                    kind=SourceKind.FEC_FILING.value,
                    citation_url=contribution_citation_url(committee_id, sub_id),
                    citation_ref=sub_id,
                )
            )
            edge.weight = float((edge.weight or 0.0) + 1.0)
            stats.citations_created += 1


# ─── the pass ───────────────────────────────────────────────────────────


def _record_refusal(
    stats: IndividualContribStats, reason: str, row: dict, sample_cap: int = 10
) -> None:
    """Count a refusal and keep a bounded sample for the review list."""
    stats.rows_refused += 1
    stats.refused_by_reason[reason] = stats.refused_by_reason.get(reason, 0) + 1
    bucket = stats.refused_sample.setdefault(reason, [])
    if len(bucket) < sample_cap:
        bucket.append(
            {
                "contributor_name": row.get("contributor_name"),
                "city": row.get("contributor_city"),
                "state": row.get("contributor_state"),
                "employer": row.get("contributor_employer"),
                "occupation": row.get("contributor_occupation"),
                "amount": row.get("contribution_receipt_amount"),
                "committee": (row.get("committee") or {}).get("name"),
            }
        )


async def ingest_donor(
    donor_key: str,
    *,
    batch_id: str | None = None,
    max_rows_per_period: int = 2000,
    emit_affiliations: bool = True,
    rows: dict[str, dict] | None = None,
) -> IndividualContribStats:
    """Sweep one declared donor's Schedule A giving into the graph.

    ``rows`` is an injection point for tests + dry-runs: pass a
    pre-fetched ``{sub_id: row}`` map to skip the network entirely.
    """
    identity = DONOR_IDENTITIES.get(donor_key)
    if identity is None:
        raise ValueError(
            f"unknown donor {donor_key!r}; choose from {sorted(DONOR_IDENTITIES)}"
        )
    stats = IndividualContribStats(donor=identity.label)
    sm = get_sessionmaker()

    async with sm() as session:
        donor_id = await _donor_canonical(
            session, identity, batch_id=batch_id, stats=stats
        )
    if donor_id is None:
        logger.error(
            "fec_individual: no anchored canonical for %r — run "
            "domain_anchors first; refusing to mint a donor from a search "
            "string", identity.label,
        )
        stats.errors += 1
        return stats
    stats.donor_canonical_id = donor_id

    if rows is None:
        rows = await fetch_schedule_a(
            identity, max_rows_per_period=max_rows_per_period, stats=stats
        )
    else:
        stats.rows_fetched = len(rows)
        stats.rows_unique = len(rows)

    shapes: dict[tuple, dict] = {}
    per_committee: dict[str, dict] = {}

    async with sm() as session:
        for sub_id, row in sorted(rows.items()):
            check = check_identity(row, identity)
            if not check.accepted:
                _record_refusal(stats, check.reason, row)
                continue
            # Memo rows are the SAME dollars itemized twice.
            if (row.get("memo_code") or "").strip().upper() == MEMO_CODE:
                stats.memo_rows_skipped += 1
                continue
            committee = row.get("committee") or {}
            committee_id = (committee.get("committee_id") or "").strip().upper()
            if not committee_id or not (committee.get("name") or "").strip():
                _record_refusal(stats, "no_recipient_committee", row)
                continue

            stats.rows_accepted += 1
            key = (
                row.get("contributor_name"),
                row.get("contributor_city"),
                row.get("contributor_state"),
                row.get("contributor_employer"),
            )
            shape = shapes.setdefault(
                key,
                {
                    "contributor_name": key[0],
                    "city": key[1],
                    "state": key[2],
                    "employer": key[3],
                    "rows": 0,
                    "amount": 0.0,
                },
            )
            shape["rows"] += 1
            shape["amount"] += float(row.get("contribution_receipt_amount") or 0.0)

            try:
                recipient_id = await _recipient_canonical(
                    session, committee, batch_id=batch_id, stats=stats
                )
                await _emit_contribution(
                    session,
                    donor_id=donor_id,
                    recipient_id=recipient_id,
                    row=row,
                    committee_id=committee_id,
                    batch_id=batch_id,
                    stats=stats,
                )
                if emit_affiliations:
                    await _emit_employer_affiliation(
                        session,
                        donor_id=donor_id,
                        identity=identity,
                        row=row,
                        committee_id=committee_id,
                        batch_id=batch_id,
                        stats=stats,
                    )
                bucket = per_committee.setdefault(
                    committee_id,
                    {
                        "committee_id": committee_id,
                        "committee": committee.get("name"),
                        "rows": 0,
                        "amount": 0.0,
                    },
                )
                bucket["rows"] += 1
                bucket["amount"] += float(
                    row.get("contribution_receipt_amount") or 0.0
                )
            except Exception:
                logger.exception(
                    "fec_individual: row failed sub_id=%s", sub_id
                )
                stats.errors += 1
        try:
            await session.commit()
        except Exception:
            await session.rollback()
            stats.errors += 1
            logger.exception("fec_individual commit failed")

    stats.accepted_shapes = sorted(
        shapes.values(), key=lambda s: -s["amount"]
    )
    stats.by_committee = sorted(
        per_committee.values(), key=lambda c: -c["amount"]
    )
    return stats


def main() -> None:
    """CLI — ``python -m app.services.ingest.fec_individual``."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    ap = argparse.ArgumentParser(
        description="FEC Schedule A individual-contributor mode (identity-keyed)"
    )
    ap.add_argument(
        "--donor", default="thiel", choices=sorted(DONOR_IDENTITIES),
        help="which declared donor identity to sweep",
    )
    ap.add_argument("--batch-id", default=None)
    ap.add_argument("--max-rows-per-period", type=int, default=2000)
    ap.add_argument(
        "--no-affiliations", action="store_true",
        help="skip the cited affiliated_with donor → employer edges",
    )
    ap.add_argument("--json-report", default=None)
    args = ap.parse_args()

    stats = asyncio.run(
        ingest_donor(
            args.donor,
            batch_id=args.batch_id,
            max_rows_per_period=args.max_rows_per_period,
            emit_affiliations=not args.no_affiliations,
        )
    )
    report = dict(stats.__dict__)
    report["batch_id"] = args.batch_id
    print(json.dumps(report, indent=2, default=str))
    if args.json_report:
        with open(args.json_report, "w") as fh:
            json.dump(report, fh, indent=2, default=str)


if __name__ == "__main__":
    main()
