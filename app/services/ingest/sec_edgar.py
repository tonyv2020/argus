"""P3b — SEC EDGAR ingestion for detention-industry public issuers.

Uses the free https://data.sec.gov/submissions/CIK{cik10}.json endpoint
(no auth needed; SEC requires only a descriptive User-Agent header per
their fair-use policy). Fetches recent filings for each anchor CIK and
materialises:

  * a canonical for the **issuer** organization (upserted, keyed by CIK).
  * a canonical for the **SEC** as an ``AGENCY`` (the regulator; one
    shared canonical across every issuer).
  * an ``AFFILIATED_WITH`` edge (issuer → SEC) — the semantic is
    "regulated-entity to regulator", modeled with the existing
    ``AFFILIATED_WITH`` relation rather than adding a new enum value
    (design principle: reuse the smallest relation set that reads
    correctly; a downstream migration can lift this to a dedicated
    ``REGULATED_BY`` if the semantic starts to matter for graph queries).
  * a ``corporate_registry`` :class:`SourceCitation` PER filing accession
    number pointing at the SEC's public filing index page. The edge is
    weighted by count-of-citations so a company with many filings shows
    up as a heavier tie to its regulator.

We also mine ``formerNames`` off the submissions payload and register
each historical name as an additional :class:`EntityAlias` on the
issuer canonical — so future news-tagged mentions using the pre-rename
form (e.g. "Wackenhut Corrections Corporation" pre-2003 GEO Group)
still resolve to the same canonical.

Same design rhythm as :mod:`.senate_lda` / :mod:`.fec`: bounded, per-page
commit, idempotent-on-accession-number (a rerun cites new accessions
only + never double-counts).

See argus design §5.7 and Portfolio task d95ada3a (P3).
"""

from __future__ import annotations

import asyncio
import logging
import os
from dataclasses import dataclass, field
from typing import Iterable

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
    SourceCitation,
    SourceKind,
)
from app.services.graph.base import normalize_name

logger = logging.getLogger(__name__)

_SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik10}.json"

# The SEC requires a descriptive User-Agent per its fair-use policy
# (https://www.sec.gov/os/webmaster-faq#code-support). Defaults to a
# self-describing string; override in prod via env for contact-attributability.
_DEFAULT_USER_AGENT = "argus-ingest/1.0 (twin@achilles.tonyvigna.com)"

# Default detention-industry anchors (ticker → CIK). Extend for the P3c
# broadening pass; every additional anchor is one more `SecAnchor` here.
DEFAULT_ANCHORS: tuple["SecAnchor", ...] = ()  # populated below to avoid forward ref.


@dataclass(frozen=True)
class SecAnchor:
    """One (CIK, expected surface-name) pair the ingester will backfill."""

    cik: int
    surface_name: str

    @property
    def cik10(self) -> str:
        """The zero-padded 10-digit CIK the submissions API expects."""
        return str(self.cik).zfill(10)

    @property
    def cik_short(self) -> str:
        """CIK with no leading zeros — the form used in filing-archive URLs."""
        return str(self.cik)


DEFAULT_ANCHORS = (
    SecAnchor(cik=923796, surface_name="GEO Group Inc"),
    SecAnchor(cik=1070985, surface_name="CoreCivic, Inc."),
)

# Filing forms worth attaching as citations. Everything else (Form 4
# insider-transaction, 13G, small technical filings) is bounded out to
# keep the citation set focused on load-bearing corporate disclosures.
INTERESTING_FORMS: frozenset[str] = frozenset(
    {
        "10-K",
        "10-K/A",
        "10-Q",
        "10-Q/A",
        "8-K",
        "8-K/A",
        "DEF 14A",
        "S-1",
        "S-3",
        "S-4",
        "20-F",
    }
)

# Special SEC canonical — the regulator that every issuer files with. A
# single AGENCY canonical shared across all issuers; the alias source
# lookup on ("sec.regulator", "sec") keeps reruns idempotent.
_SEC_AGENCY_SURFACE = "U.S. Securities and Exchange Commission"
_SEC_AGENCY_SOURCE_SYSTEM = "sec.regulator"
_SEC_AGENCY_SOURCE_ID = "sec"


@dataclass
class SecEdgarStats:
    """Counters for one SEC pass — surfaced to callers + logs."""

    anchors_processed: int = 0
    filings_fetched: int = 0
    filings_skipped_uninteresting: int = 0
    issuers_upserted: int = 0
    former_name_aliases_created: int = 0
    edges_created: int = 0
    edges_reused: int = 0
    citations_created: int = 0
    citations_skipped_already_cited: int = 0
    errors: int = 0


def _user_agent() -> str:
    """Return the SEC User-Agent header value (env override supported)."""
    return os.environ.get("SEC_USER_AGENT") or _DEFAULT_USER_AGENT


async def _sec_get(client: httpx.AsyncClient, url: str) -> dict:
    """One GET to data.sec.gov with the required User-Agent; returns parsed JSON."""
    r = await client.get(url, headers={"User-Agent": _user_agent()})
    r.raise_for_status()
    return r.json()


def _filing_index_url(cik_short: str, accession: str) -> str:
    """The stable public filing-index page for an accession.

    Uses ``/Archives/edgar/data/<cik-no-zeros>/<accession-no-dashes>/``
    which lists every exhibit for the filing. Prefer this over any
    specific document URL — the specific docs can be amended, the index
    stays live for the accession's lifetime.
    """
    accession_stripped = accession.replace("-", "")
    return (
        f"https://www.sec.gov/Archives/edgar/data/"
        f"{cik_short}/{accession_stripped}/{accession}-index.htm"
    )


def _iter_recent_filings(submissions: dict) -> Iterable[dict]:
    """Yield each recent filing as {form, accession, date} from the payload.

    SEC's submissions endpoint groups recent filings under
    ``filings.recent`` as PARALLEL LISTS (form[i], accessionNumber[i],
    filingDate[i], ...) — not a list of objects. We zip them into
    row-shaped dicts here so the caller writes normal per-row code.
    """
    recent = (submissions.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    accns = recent.get("accessionNumber") or []
    dates = recent.get("filingDate") or []
    for i in range(min(len(forms), len(accns), len(dates))):
        yield {
            "form": forms[i],
            "accession": accns[i],
            "date": dates[i],
        }


async def _get_or_create_sec_agency(session: AsyncSession) -> str:
    """Return the canonical id for the SEC agency, creating it if absent.

    Shared across every issuer processed in a single ingest run and across
    every rerun — the alias source lookup on
    ``("sec.regulator", "sec")`` keeps this idempotent.
    """
    existing = (
        await session.execute(
            select(EntityAlias).where(
                EntityAlias.source_system == _SEC_AGENCY_SOURCE_SYSTEM,
                EntityAlias.source_id == _SEC_AGENCY_SOURCE_ID,
            )
        )
    ).scalar_one_or_none()
    if existing:
        return existing.canonical_id

    norm = normalize_name(_SEC_AGENCY_SURFACE)
    # A prior hollywood-side canonical for the SEC — if any news article ever
    # tagged "U.S. Securities and Exchange Commission" as an agency — should
    # get picked up on normalized-name before we create a duplicate.
    prior = (
        await session.execute(
            select(CanonicalEntity).where(
                CanonicalEntity.type == EntityType.AGENCY.value,
                CanonicalEntity.canonical_name_normalized == norm,
            )
        )
    ).scalar_one_or_none()
    if prior:
        session.add(
            EntityAlias(
                canonical_id=prior.id,
                source_system=_SEC_AGENCY_SOURCE_SYSTEM,
                source_id=_SEC_AGENCY_SOURCE_ID,
                surface_name=_SEC_AGENCY_SURFACE,
                surface_name_normalized=norm,
                kind_hint=None,
            )
        )
        return prior.id

    ce = CanonicalEntity(
        canonical_name=_SEC_AGENCY_SURFACE,
        canonical_name_normalized=norm,
        type=EntityType.AGENCY.value,
    )
    session.add(ce)
    await session.flush()
    session.add(
        EntityAlias(
            canonical_id=ce.id,
            source_system=_SEC_AGENCY_SOURCE_SYSTEM,
            source_id=_SEC_AGENCY_SOURCE_ID,
            surface_name=_SEC_AGENCY_SURFACE,
            surface_name_normalized=norm,
            kind_hint=None,
        )
    )
    return ce.id


async def _upsert_issuer(
    session: AsyncSession,
    *,
    anchor: SecAnchor,
    canonical_name: str,
) -> str:
    """Return the issuer canonical id keyed by CIK; create if absent."""
    existing = (
        await session.execute(
            select(EntityAlias).where(
                EntityAlias.source_system == "sec.cik",
                EntityAlias.source_id == anchor.cik10,
            )
        )
    ).scalar_one_or_none()
    if existing:
        return existing.canonical_id

    surface = (canonical_name or anchor.surface_name).strip()
    norm = normalize_name(surface)
    prior = (
        await session.execute(
            select(CanonicalEntity).where(
                CanonicalEntity.type == EntityType.ORGANIZATION.value,
                CanonicalEntity.canonical_name_normalized == norm,
            )
        )
    ).scalar_one_or_none()
    if prior:
        session.add(
            EntityAlias(
                canonical_id=prior.id,
                source_system="sec.cik",
                source_id=anchor.cik10,
                surface_name=surface,
                surface_name_normalized=norm,
                kind_hint=None,
            )
        )
        return prior.id

    ce = CanonicalEntity(
        canonical_name=surface,
        canonical_name_normalized=norm,
        type=EntityType.ORGANIZATION.value,
    )
    session.add(ce)
    await session.flush()
    session.add(
        EntityAlias(
            canonical_id=ce.id,
            source_system="sec.cik",
            source_id=anchor.cik10,
            surface_name=surface,
            surface_name_normalized=norm,
            kind_hint=None,
        )
    )
    return ce.id


async def _register_former_name_aliases(
    session: AsyncSession,
    *,
    issuer_canonical: str,
    submissions: dict,
) -> int:
    """Attach each ``formerNames`` entry to the issuer canonical as an alias.

    SEC's submissions payload carries ``formerNames`` = [{name, from, to},
    ...] — the company's prior legal names with dates. Registering them
    as aliases lets news-tagged mentions using the pre-rename form resolve
    to the same canonical (e.g. Wackenhut Corrections Corporation → GEO
    Group). Returns count of aliases newly created.
    """
    created = 0
    former_names = submissions.get("formerNames") or []
    for entry in former_names:
        name = (entry.get("name") or "").strip()
        if not name:
            continue
        norm = normalize_name(name)
        source_id = f"formerName:{name}"[:255]
        existing = (
            await session.execute(
                select(EntityAlias).where(
                    EntityAlias.source_system == "sec.former_name",
                    EntityAlias.source_id == source_id,
                )
            )
        ).scalar_one_or_none()
        if existing:
            continue
        session.add(
            EntityAlias(
                canonical_id=issuer_canonical,
                source_system="sec.former_name",
                source_id=source_id,
                surface_name=name,
                surface_name_normalized=norm,
                kind_hint=None,
            )
        )
        created += 1
    return created


async def _emit_regulator_edge_and_cite(
    session: AsyncSession,
    *,
    issuer_canonical: str,
    sec_canonical: str,
    anchor: SecAnchor,
    filing: dict,
) -> tuple[bool, bool]:
    """Emit-or-reuse issuer→SEC edge + cite this filing.

    Returns ``(edge_reused, citation_skipped_already_present)``.
    """
    existing_edge = (
        await session.execute(
            select(CanonicalEdge).where(
                CanonicalEdge.source_id == issuer_canonical,
                CanonicalEdge.target_id == sec_canonical,
                CanonicalEdge.relation == EdgeRelation.AFFILIATED_WITH.value,
            )
        )
    ).scalar_one_or_none()
    edge_reused = False
    if existing_edge is None:
        edge = CanonicalEdge(
            source_id=issuer_canonical,
            target_id=sec_canonical,
            relation=EdgeRelation.AFFILIATED_WITH.value,
            weight=1.0,
        )
        session.add(edge)
        await session.flush()
    else:
        edge = existing_edge
        edge_reused = True

    accession = filing["accession"]
    already = (
        await session.execute(
            select(SourceCitation).where(
                SourceCitation.edge_id == edge.id,
                SourceCitation.citation_ref == accession,
            )
        )
    ).scalar_one_or_none()
    if already is not None:
        return edge_reused, True

    session.add(
        SourceCitation(
            edge_id=edge.id,
            kind=SourceKind.CORPORATE_REGISTRY.value,
            citation_url=_filing_index_url(anchor.cik_short, accession),
            citation_ref=accession,
        )
    )
    if edge_reused:
        edge.weight = float((edge.weight or 0.0) + 1.0)
    return edge_reused, False


async def ingest_anchor(
    anchor: SecAnchor,
    *,
    max_filings: int = 200,
    interesting_forms: frozenset[str] = INTERESTING_FORMS,
    stats: SecEdgarStats | None = None,
) -> SecEdgarStats:
    """Ingest one anchor's SEC filings; returns the (mutated) stats."""
    stats = stats or SecEdgarStats()
    sm = get_sessionmaker()

    async with httpx.AsyncClient(timeout=20.0) as client:
        submissions = await _sec_get(
            client, _SEC_SUBMISSIONS_URL.format(cik10=anchor.cik10)
        )

    canonical_name = submissions.get("name") or anchor.surface_name

    async with sm() as session:
        sec_canonical = await _get_or_create_sec_agency(session)
        issuer_canonical = await _upsert_issuer(
            session, anchor=anchor, canonical_name=canonical_name
        )
        stats.issuers_upserted += 1
        stats.former_name_aliases_created += await _register_former_name_aliases(
            session, issuer_canonical=issuer_canonical, submissions=submissions
        )
        try:
            await session.commit()
        except Exception as exc:  # noqa: BLE001
            await session.rollback()
            stats.errors += 1
            logger.exception(
                "sec_edgar upsert commit failed cik=%s: %s", anchor.cik10, exc
            )
            return stats

    processed = 0
    async with sm() as session:
        for filing in _iter_recent_filings(submissions):
            if processed >= max_filings:
                break
            stats.filings_fetched += 1
            if filing["form"] not in interesting_forms:
                stats.filings_skipped_uninteresting += 1
                continue

            reused, skipped_cite = await _emit_regulator_edge_and_cite(
                session,
                issuer_canonical=issuer_canonical,
                sec_canonical=sec_canonical,
                anchor=anchor,
                filing=filing,
            )
            if reused:
                stats.edges_reused += 1
            else:
                stats.edges_created += 1
            if skipped_cite:
                stats.citations_skipped_already_cited += 1
            else:
                stats.citations_created += 1
            processed += 1

        try:
            await session.commit()
        except Exception as exc:  # noqa: BLE001
            await session.rollback()
            stats.errors += 1
            logger.exception(
                "sec_edgar cite commit failed cik=%s: %s", anchor.cik10, exc
            )

    return stats


async def ingest_default_anchors(max_filings_per_anchor: int = 200) -> SecEdgarStats:
    """Backfill every anchor in :data:`DEFAULT_ANCHORS`."""
    stats = SecEdgarStats()
    for anchor in DEFAULT_ANCHORS:
        logger.info("sec_edgar ingest anchor cik=%s name=%s", anchor.cik10, anchor.surface_name)
        stats = await ingest_anchor(
            anchor, max_filings=max_filings_per_anchor, stats=stats
        )
        stats.anchors_processed += 1
    return stats


def main() -> None:
    """CLI entrypoint — ``python -m app.services.ingest.sec_edgar``."""
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = asyncio.run(ingest_default_anchors())
    logger.info("sec_edgar ingest done: %s", stats)


if __name__ == "__main__":
    main()
