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
from sqlalchemy import delete, select, text
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
    two_year_periods: tuple[int, ...] = (
        2026, 2024, 2022, 2020, 2018, 2016, 2014, 2012, 2010, 2008,
    )
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
    # Verified against live Schedule A on 2026-08-21, then WIDENED after
    # the first live run surfaced 14 in-state near-misses (the
    # ``no_affiliation_match_in_state`` bucket exists exactly so these
    # are visible rather than silently lost):
    #   * ``CLARIUM`` appears bare ("CLARIUM / FINANCE") and misspelled
    #     three ways — CLARIUM, CALRIUM, CLARIAM, CLARIUM CAPITOL. The
    #     word is a coined name, so matching it alone is safe.
    #   * ``PAYPAL`` — Thiel co-founded it and filed as its CEO in the
    #     mid-2000s cycles.
    # Occupation and employer are matched against ONE joined blob because
    # filers routinely swap the two fields.
    affiliation_patterns=(
        r"THIEL CAPITAL",
        r"C[AL]{2}RI[AU]M",
        r"FOUNDERS FUND",
        r"PALANTIR",
        r"FACEBOOK",
        r"PAYPAL",
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
    #: Dollars this RUN newly cited. Not the donor's total — a re-run
    #: cites nothing new and reports 0. The donor's total is
    #: ``contributed_total`` below, summed over every accepted row.
    new_dollars_cited: float = 0.0
    contributed_total: float = 0.0
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


async def _citation_exists(
    session: AsyncSession, edge_id: str, citation_ref: str
) -> bool:
    """Has this transaction already been cited on this edge?

    An EXISTENCE check, never ``scalar_one_or_none``: there is no unique
    index on ``(edge_id, citation_ref)``, and the pre-P1.6 emitter added
    a citation row on every re-run, so live edges genuinely carry the
    same ``sub_id`` up to seven times. Asserting uniqueness here raised
    ``MultipleResultsFound`` on 51 of Peter Thiel's 83 accepted rows in
    the first live run.
    """
    return (
        await session.execute(
            select(SourceCitation.id)
            .where(
                SourceCitation.edge_id == edge_id,
                SourceCitation.citation_ref == citation_ref,
            )
            .limit(1)
        )
    ).first() is not None


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

    if await _citation_exists(session, edge.id, sub_id):
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
    stats.new_dollars_cited += amount


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
        if not await _citation_exists(session, edge.id, sub_id):
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
                # SAVEPOINT per transaction: one bad row must not poison
                # the transaction and lose every row after it.
                async with session.begin_nested():
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
    stats.contributed_total = round(
        sum(c["amount"] for c in stats.by_committee), 2
    )
    return stats


# ─── repair of the pre-P1.6 emitter's damage ────────────────────────────
#
# LIVE FINDING (2026-08-21). Peter Thiel's ``contributes_to`` edges were
# already in the graph, PUBLISHED, written by the pre-P1.6
# ``fec.ingest_individual_contributor``. That emitter added a
# SourceCitation row unconditionally on every run and added the amount
# to the edge weight every time, so the published figures are inflated
# by the number of times the ingest ran:
#
#   SAVING ARIZONA PAC                 $140,000,000  (true: $20,000,000)
#   PROTECT OHIO VALUES PAC (POV PAC)  $105,000,000  (true: $15,000,000)
#   FREE FOREVER PAC                    $14,700,000  (true:  $2,100,000)
#
# Across the 62 edges: $270,902,324 of weight backed by 1,354 citation
# rows covering only 220 distinct transactions — 6.15x average
# duplication, 7x worst case. It also used the FUZZY contributor-name
# search, so some of those transactions belong to other, private people
# with similar names.
#
# This repair is DESTRUCTIVE and touches PUBLISHED rows, so it is
# dry-run by default, scoped to one donor, and refuses to touch an edge
# it cannot fully explain.


@dataclass(frozen=True)
class EdgeRepair:
    """One donor→committee edge the repair would rewrite."""

    edge_id: str
    committee: str
    old_weight: float
    new_weight: float
    citations_before: int
    duplicate_citations_removed: int
    memo_citations_removed: int
    refused_citations_removed: int
    citations_after: int
    delete_edge: bool
    publication_state: str
    #: The exact citation rows to keep and to drop, decided at PLAN time
    #: so the dry-run preview and the apply cannot diverge.
    keep_citation_ids: tuple[str, ...] = ()
    drop_citation_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        """JSON-able row for the report — ids elided, counts kept."""
        d = dict(self.__dict__)
        d.pop("keep_citation_ids", None)
        d.pop("drop_citation_ids", None)
        return d


@dataclass
class RepairPlan:
    """What one repair pass would do to a donor's contribution edges."""

    donor: str = ""
    donor_canonical_id: str = ""
    edges_examined: int = 0
    edges_repaired: list[EdgeRepair] = field(default_factory=list)
    #: Edges carrying a citation the sweep could not classify — left
    #: EXACTLY as they are and reported. Never rewritten on a guess.
    edges_deferred: list[dict] = field(default_factory=list)
    weight_before: float = 0.0
    weight_after: float = 0.0
    citations_before: int = 0
    citations_after: int = 0

    @property
    def edges_deleted(self) -> int:
        """Edges left with no citation at all — they must not survive."""
        return sum(1 for e in self.edges_repaired if e.delete_edge)


async def plan_repair(
    donor_key: str,
    *,
    rows: dict[str, dict] | None = None,
    max_rows_per_period: int = 4000,
) -> RepairPlan:
    """Classify every citation on the donor's contribution edges.

    Each citation's ``citation_ref`` is an FEC ``sub_id``, so it can be
    checked against the SAME identity predicate the ingest uses:

    * **accepted** — the transaction is the donor's and is not a memo:
      keep exactly ONE citation, and its amount is what the weight sums.
    * **memo** — the identity is right but ``memo_code='X'`` means these
      dollars are itemized elsewhere too; the citation is dropped so the
      edge cites exactly the transactions its weight sums.
    * **refused** — the transaction belongs to somebody else. Dropping
      it is the point: it is another person's giving.
    * **unclassified** — a ``sub_id`` the sweep never saw. The edge is
      DEFERRED whole and reported: rewriting a weight from evidence we
      have not examined is a guess.
    """
    identity = DONOR_IDENTITIES.get(donor_key)
    if identity is None:
        raise ValueError(f"unknown donor {donor_key!r}")
    plan = RepairPlan(donor=identity.label)
    stats = IndividualContribStats(donor=identity.label)
    sm = get_sessionmaker()

    async with sm() as session:
        donor_id = await _donor_canonical(
            session, identity, batch_id=None, stats=stats
        )
    if donor_id is None:
        raise RuntimeError(
            f"{identity.label!r} has no anchored canonical — run domain_anchors"
        )
    plan.donor_canonical_id = donor_id

    if rows is None:
        rows = await fetch_schedule_a(
            identity, max_rows_per_period=max_rows_per_period, stats=stats
        )

    accepted: dict[str, float] = {}
    memo: set[str] = set()
    refused: set[str] = set()
    for sub_id, row in rows.items():
        if not check_identity(row, identity).accepted:
            refused.add(sub_id)
        elif (row.get("memo_code") or "").strip().upper() == MEMO_CODE:
            memo.add(sub_id)
        else:
            accepted[sub_id] = float(
                row.get("contribution_receipt_amount") or 0.0
            )

    async with sm() as session:
        edges = (
            await session.execute(
                select(CanonicalEdge).where(
                    CanonicalEdge.source_id == donor_id,
                    CanonicalEdge.relation == EdgeRelation.CONTRIBUTES_TO.value,
                )
            )
        ).scalars().all()
        for edge in edges:
            plan.edges_examined += 1
            target = (
                await session.execute(
                    select(CanonicalEntity.canonical_name).where(
                        CanonicalEntity.id == edge.target_id
                    )
                )
            ).scalar_one_or_none() or edge.target_id
            cites = (
                await session.execute(
                    select(SourceCitation.id, SourceCitation.citation_ref).where(
                        SourceCitation.edge_id == edge.id
                    )
                )
            ).all()
            plan.weight_before += float(edge.weight or 0.0)
            plan.citations_before += len(cites)

            by_ref: dict[str, list[str]] = {}
            for cite_id, ref in cites:
                by_ref.setdefault(str(ref or ""), []).append(cite_id)
            unclassified = [
                r for r in by_ref
                if r not in accepted and r not in memo and r not in refused
            ]
            if unclassified:
                plan.edges_deferred.append(
                    {
                        "edge_id": edge.id,
                        "committee": target,
                        "weight": float(edge.weight or 0.0),
                        "citations": len(cites),
                        "unclassified_refs": sorted(unclassified)[:10],
                        "unclassified_count": len(unclassified),
                    }
                )
                plan.weight_after += float(edge.weight or 0.0)
                plan.citations_after += len(cites)
                continue

            dup = sum(len(v) - 1 for r, v in by_ref.items() if r in accepted)
            memo_removed = sum(len(v) for r, v in by_ref.items() if r in memo)
            refused_removed = sum(
                len(v) for r, v in by_ref.items() if r in refused
            )
            kept = sorted(r for r in by_ref if r in accepted)
            keep_ids: list[str] = []
            drop_ids: list[str] = []
            for ref, ids in by_ref.items():
                if ref in accepted:
                    keep_ids.append(ids[0])
                    drop_ids.extend(ids[1:])
                else:
                    drop_ids.extend(ids)
            new_weight = round(sum(accepted[r] for r in kept), 2)
            repair = EdgeRepair(
                edge_id=edge.id,
                committee=target,
                old_weight=float(edge.weight or 0.0),
                new_weight=new_weight,
                citations_before=len(cites),
                duplicate_citations_removed=dup,
                memo_citations_removed=memo_removed,
                refused_citations_removed=refused_removed,
                citations_after=len(kept),
                delete_edge=not kept,
                publication_state=edge.publication_state,
                keep_citation_ids=tuple(keep_ids),
                drop_citation_ids=tuple(drop_ids),
            )
            if (
                repair.citations_before == repair.citations_after
                and repair.old_weight == repair.new_weight
            ):
                # Already correct — nothing to do, but it still counts
                # toward the after-totals.
                plan.weight_after += repair.new_weight
                plan.citations_after += repair.citations_after
                continue
            plan.edges_repaired.append(repair)
            plan.weight_after += 0.0 if repair.delete_edge else new_weight
            plan.citations_after += repair.citations_after
    return plan


async def apply_repair(plan: RepairPlan) -> dict:
    """Execute a :class:`RepairPlan`. One transaction per edge.

    Every row id was decided at plan time, so what runs here is exactly
    what the dry-run printed. Ordering inside an edge preserves the
    0-uncited invariant: an edge with nothing left to cite is DELETED
    (cascading its citations) rather than left citation-less.
    """
    sm = get_sessionmaker()
    out: dict = {
        "edges_rewritten": 0,
        "edges_deleted": 0,
        "citations_deleted": 0,
        "errors": [],
    }
    for repair in plan.edges_repaired:
        async with sm() as session:
            try:
                if repair.delete_edge:
                    await session.execute(
                        delete(CanonicalEdge).where(
                            CanonicalEdge.id == repair.edge_id
                        )
                    )
                    out["citations_deleted"] += repair.citations_before
                    out["edges_deleted"] += 1
                    await session.commit()
                    continue
                if repair.drop_citation_ids:
                    await session.execute(
                        delete(SourceCitation).where(
                            SourceCitation.id.in_(repair.drop_citation_ids)
                        )
                    )
                    out["citations_deleted"] += len(repair.drop_citation_ids)
                edge = (
                    await session.execute(
                        select(CanonicalEdge).where(
                            CanonicalEdge.id == repair.edge_id
                        )
                    )
                ).scalar_one()
                edge.weight = repair.new_weight
                out["edges_rewritten"] += 1
                await session.commit()
            except Exception as exc:  # noqa: BLE001
                await session.rollback()
                out["errors"].append(
                    {
                        "edge_id": repair.edge_id,
                        "error": f"{type(exc).__name__}: {exc}",
                    }
                )
                logger.exception("repair failed edge=%s", repair.edge_id)
    return out


async def run_repair(donor_key: str, apply: bool) -> dict:
    """Plan (always) and apply (only with ``apply=True``)."""
    plan = await plan_repair(donor_key)
    report: dict = {
        "mode": "apply" if apply else "dry-run",
        "donor": plan.donor,
        "donor_canonical_id": plan.donor_canonical_id,
        "edges_examined": plan.edges_examined,
        "edges_to_rewrite": len(plan.edges_repaired) - plan.edges_deleted,
        "edges_to_delete": plan.edges_deleted,
        "edges_deferred": len(plan.edges_deferred),
        "weight_before": round(plan.weight_before, 2),
        "weight_after": round(plan.weight_after, 2),
        "citations_before": plan.citations_before,
        "citations_after": plan.citations_after,
        "repairs": [
            r.to_dict()
            for r in sorted(plan.edges_repaired, key=lambda r: -r.old_weight)
        ],
        "deferred": plan.edges_deferred,
    }
    if apply:
        report["apply"] = await apply_repair(plan)
        sm = get_sessionmaker()
        async with sm() as session:
            report["postconditions"] = {
                "uncited_edges_zero": (
                    await session.execute(
                        text(
                            "select count(*) from canonical_edges e where not "
                            "exists (select 1 from source_citations c where "
                            "c.edge_id = e.id)"
                        )
                    )
                ).scalar_one() == 0,
                "orphan_citations_zero": (
                    await session.execute(
                        text(
                            "select count(*) from source_citations c where not "
                            "exists (select 1 from canonical_edges e where "
                            "e.id = c.edge_id)"
                        )
                    )
                ).scalar_one() == 0,
                "donor_weight_now": float(
                    (
                        await session.execute(
                            text(
                                "select coalesce(sum(weight),0) from "
                                "canonical_edges where source_id = :d and "
                                "relation = 'contributes_to'"
                            ),
                            {"d": plan.donor_canonical_id},
                        )
                    ).scalar_one()
                ),
            }
            report["postconditions"]["all_passed"] = all(
                v for k, v in report["postconditions"].items()
                if isinstance(v, bool)
            )
    return report


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
    ap.add_argument(
        "--repair", action="store_true",
        help="DESTRUCTIVE (with --apply). Rebuild this donor's existing "
             "contributes_to weights + citations from the identity "
             "predicate, undoing the pre-P1.6 emitter's duplicate "
             "citations, inflated weights and mis-attributed rows. "
             "Dry-run unless --apply is also given.",
    )
    ap.add_argument(
        "--apply", action="store_true",
        help="with --repair: actually write. Touches PUBLISHED rows.",
    )
    ap.add_argument("--json-report", default=None)
    args = ap.parse_args()

    if args.repair:
        report = asyncio.run(run_repair(args.donor, apply=args.apply))
    else:
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
