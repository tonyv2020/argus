"""P1.6 — SEC Section 16 ownership filings → cited ``held_position`` edges.

The brief asks for officer edges "keyed on external IDs (SEC CIK) —
never names". Form 3 / 4 / 5 is exactly that primitive: every ownership
filing is an XML document that names an **issuer CIK** and one or more
**reporting-owner CIKs**, plus the machine-readable relationship flags
``isDirector`` / ``isOfficer`` / ``isTenPercentOwner`` and the officer's
title. Both ends of the edge are federal registrar ids; the name is only
a display label.

That is what makes ``Peter Thiel → Palantir Technologies`` a filing fact
here rather than a curated claim: SEC CIK 1211060 filed Form 4s against
SEC CIK 1321655 with ``isDirector=true``. (The pre-P1.6 module
``seed_p16_affiliations`` asserted the same relationship by matching the
STRINGS "Peter Thiel" and "Palantir Technologies Inc." — this pass
replaces that with the underlying filings.)

What one pass does, per issuer CIK
----------------------------------
1. ``data.sec.gov/submissions/CIK##########.json`` → the issuer's recent
   filings; keep forms 3/4/5 (and their ``/A`` amendments).
2. Fetch each filing's ownership XML and parse ``<issuer>`` +
   every ``<reportingOwner>``.
3. **Verify the issuer CIK matches the anchor** before using anything
   from the document — a mis-filed or redirected document is skipped,
   not trusted.
4. Resolve the reporting owner on ``sec.owner_cik`` and emit one cited
   ``held_position`` owner → issuer edge, one citation per accession.

Relations we refuse to call a position
--------------------------------------
A reporting owner flagged ONLY ``isTenPercentOwner`` is an investor, not
an officeholder. Those rows are counted (``ten_percent_only_skipped``)
and dropped: ``held_position`` has to mean what it says. Likewise a
filing whose owner CIK equals the issuer CIK (a company filing against
itself) is skipped rather than becoming a self-loop.

Read-gate + privacy — FAIL-CLOSED
---------------------------------
* ``batch_id`` stamps net-new canonicals and **every edge this pass
  creates** ``publication_state=staged``.
* A net-new insider is a real person the graph has never seen, so it is
  created ``surface_mode=suppress`` — dark by construction. The pass
  also writes a ``corporate.registry.officer`` alias, which is an
  existing scrutiny HARD SIGNAL: ``run_scrutiny --batch-id`` then
  classifies them PUBLIC (an SEC-disclosed officer/director of a public
  issuer) and promotes them to ``open``. Scrutiny opens the node; this
  ingester never does.
* The pass **never rewrites an existing canonical's surface_mode**.
* An identity alias whose ``(source_system, source_id)`` is already
  owned by a different canonical is reported, never stolen.
"""

from __future__ import annotations

import argparse
import asyncio
import html
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
from app.services.ingest.domain_anchors import SEC_OWNER_NAMESPACE, attach_alias
from app.services.ingest.sec_edgar import _filing_index_url, _user_agent

logger = logging.getLogger(__name__)

_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik10}.json"

#: Section 16 ownership forms. ``/A`` amendments carry the same
#: issuer/owner/relationship block and are worth citing too.
OWNERSHIP_FORMS: frozenset[str] = frozenset({"3", "4", "5", "3/A", "4/A", "5/A"})

#: Alias namespace that is an existing scrutiny hard signal
#: (``scrutiny._PUBLIC_SOURCE_SYSTEMS``). Written only for owners the
#: filing itself flags as a director or officer.
OFFICER_SIGNAL_NAMESPACE = "corporate.registry.officer"

#: SEC's XSL-rendered document path prefix. ``filings.recent`` gives the
#: rendered document (``xslF345X05/form4-...xml``); the raw XML sits at
#: the same accession directory without the prefix.
_XSL_PREFIX_RE = re.compile(r"^xsl[^/]*/")

#: The ownership schema writes booleans as ``1``/``0`` in some schema
#: versions and ``true``/``false`` in others; both appear in a single
#: issuer's filing history.
_TRUE_VALUES = frozenset({"1", "true", "y", "yes"})


def _is_true(raw: str | None) -> bool:
    """Parse an ownership-schema boolean across both encodings."""
    return (raw or "").strip().lower() in _TRUE_VALUES


def _tag(blob: str, tag: str) -> str | None:
    """First ``<tag>…</tag>`` value in ``blob``, unescaped, or None.

    A regex rather than an XML parse on purpose: these documents are
    small, the shape is fixed by SEC's schema, and several historical
    filings carry malformed entities that make a strict parser throw on
    documents a regex reads correctly. The trade-off is that entities
    are not decoded for us, so do it here — filers really do type
    ``COO &amp; CFO`` into ``officerTitle``.
    """
    m = re.search(rf"<{tag}>(.*?)</{tag}>", blob, re.S)
    return html.unescape(m.group(1)).strip() if m else None


@dataclass(frozen=True)
class ReportingOwner:
    """One ``<reportingOwner>`` block, reduced to the decision inputs."""

    cik: str  # zero-padded to 10
    name: str
    is_director: bool
    is_officer: bool
    is_ten_percent_owner: bool
    officer_title: str = ""

    @property
    def holds_position(self) -> bool:
        """Is this a POSITION (a seat or an office), not just a stake?"""
        return self.is_director or self.is_officer

    @property
    def role_label(self) -> str:
        """Compact human label for the edge metadata."""
        parts = []
        if self.is_director:
            parts.append("director")
        if self.is_officer:
            parts.append(self.officer_title or "officer")
        if self.is_ten_percent_owner:
            parts.append("10% owner")
        return "; ".join(parts)


@dataclass(frozen=True)
class OwnershipFiling:
    """One parsed Form 3/4/5 — issuer + its reporting owners."""

    issuer_cik: str  # zero-padded to 10
    issuer_name: str
    owners: tuple[ReportingOwner, ...]


def parse_ownership_document(xml: str) -> OwnershipFiling | None:
    """Parse an ownership XML into :class:`OwnershipFiling`.

    Returns None when the document carries no issuer CIK — a redirect
    page, an error body, or an XSL-rendered HTML file served in place of
    the XML. Callers treat None as "skip", never as "empty".
    """
    issuer_cik = _tag(xml, "issuerCik")
    if not issuer_cik or not issuer_cik.strip().isdigit():
        return None
    owners: list[ReportingOwner] = []
    for m in re.finditer(r"<reportingOwner>(.*?)</reportingOwner>", xml, re.S):
        blob = m.group(1)
        cik = _tag(blob, "rptOwnerCik")
        name = _tag(blob, "rptOwnerName")
        if not cik or not cik.strip().isdigit() or not name:
            continue
        owners.append(
            ReportingOwner(
                cik=cik.strip().zfill(10),
                name=name.strip(),
                is_director=_is_true(_tag(blob, "isDirector")),
                is_officer=_is_true(_tag(blob, "isOfficer")),
                is_ten_percent_owner=_is_true(_tag(blob, "isTenPercentOwner")),
                officer_title=(_tag(blob, "officerTitle") or "").strip(),
            )
        )
    return OwnershipFiling(
        issuer_cik=issuer_cik.strip().zfill(10),
        issuer_name=(_tag(xml, "issuerName") or "").strip(),
        owners=tuple(owners),
    )


@dataclass
class SecInsiderStats:
    """Counters for one insider pass."""

    issuers_processed: int = 0
    filings_listed: int = 0
    filings_fetched: int = 0
    filings_unparsable: int = 0
    filings_issuer_mismatch: int = 0
    owners_seen: int = 0
    ten_percent_only_skipped: int = 0
    self_filed_skipped: int = 0
    persons_created: int = 0
    persons_matched: int = 0
    owner_aliases_created: int = 0
    officer_signal_aliases_created: int = 0
    edges_created: int = 0
    edges_reused: int = 0
    citations_created: int = 0
    citations_skipped_already_cited: int = 0
    #: Reuse hits refused because the edge is already PUBLISHED and
    #: this is a staged run. See the read-gate in _emit_position.
    published_edges_skipped: int = 0
    entities_staged: int = 0
    edges_staged: int = 0
    #: Positions found, for the report: one row per (owner, issuer).
    positions: list[dict] = field(default_factory=list)
    #: (source_system, source_id) pairs another canonical already owns.
    alias_conflicts: list[dict] = field(default_factory=list)
    #: Insiders resolved onto a canonical that is not ``open`` — the
    #: pass leaves them exactly as they are and reports them.
    non_open_insiders: list[dict] = field(default_factory=list)
    errors: int = 0


# ─── HTTP ───────────────────────────────────────────────────────────────


async def _get_json(client: httpx.AsyncClient, url: str) -> dict:
    """GET + parse JSON with the SEC's required descriptive User-Agent."""
    r = await client.get(url, headers={"User-Agent": _user_agent()})
    r.raise_for_status()
    return r.json()


async def _get_text(client: httpx.AsyncClient, url: str) -> str:
    """GET a filing document as text."""
    r = await client.get(url, headers={"User-Agent": _user_agent()})
    r.raise_for_status()
    return r.text


def ownership_document_url(cik: int, accession: str, primary_document: str) -> str:
    """Raw ownership-XML URL for one accession.

    ``filings.recent.primaryDocument`` points at SEC's XSL-RENDERED view
    (``xslF345X05/form4-….xml``), which is HTML. Stripping the ``xsl*/``
    path segment gives the machine-readable XML in the same directory.
    """
    doc = _XSL_PREFIX_RE.sub("", primary_document.strip())
    return (
        f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/"
        f"{accession.replace('-', '')}/{doc}"
    )


def iter_ownership_filings(submissions: dict) -> list[dict]:
    """Yield ``{form, accession, date, primary_document}`` for each
    Section 16 filing in a submissions payload.

    ``filings.recent`` stores PARALLEL LISTS, not a list of objects.
    """
    recent = (submissions.get("filings") or {}).get("recent") or {}
    forms = recent.get("form") or []
    accns = recent.get("accessionNumber") or []
    dates = recent.get("filingDate") or []
    docs = recent.get("primaryDocument") or []
    out: list[dict] = []
    for i in range(min(len(forms), len(accns), len(dates), len(docs))):
        if forms[i] not in OWNERSHIP_FORMS:
            continue
        out.append(
            {
                "form": forms[i],
                "accession": accns[i],
                "date": dates[i],
                "primary_document": docs[i],
            }
        )
    return out


# ─── resolution ─────────────────────────────────────────────────────────


async def _resolve_owner_canonical(
    session: AsyncSession,
    owner: ReportingOwner,
    *,
    batch_id: str | None,
    stats: SecInsiderStats,
) -> str:
    """Canonical id for a reporting owner, keyed on ``sec.owner_cik``.

    A net-new insider is created ``surface_mode=suppress``: it is a real
    person the corpus has not classified yet, and the fail-closed
    default is dark. ``run_scrutiny --batch-id`` promotes them off the
    ``corporate.registry.officer`` hard signal this pass writes.
    """
    existing = (
        await session.execute(
            select(EntityAlias.canonical_id).where(
                EntityAlias.source_system == SEC_OWNER_NAMESPACE,
                EntityAlias.source_id == owner.cik,
            )
        )
    ).scalar_one_or_none()
    if existing:
        stats.persons_matched += 1
        return existing

    norm = normalize_name(owner.name)
    ent = CanonicalEntity(
        canonical_name=owner.name,
        canonical_name_normalized=norm or owner.name.lower(),
        type=EntityType.PERSON.value,
        surface_mode=SurfaceMode.SUPPRESS.value,
        publication_state=(
            PublicationState.STAGED.value if batch_id
            else PublicationState.PUBLISHED.value
        ),
        batch_id=batch_id,
    )
    session.add(ent)
    await session.flush()
    stats.persons_created += 1
    if batch_id:
        stats.entities_staged += 1
    return ent.id


async def _resolve_issuer_canonical(
    session: AsyncSession, issuer_cik: str
) -> str | None:
    """Canonical id carrying ``sec.cik:<cik10>``, else None.

    Returning None is deliberate: the insider pass never MINTS an issuer.
    ``domain_anchors`` (or ``sec_edgar``) owns issuer identity, and an
    issuer that is not anchored is reported rather than invented.
    """
    return (
        await session.execute(
            select(EntityAlias.canonical_id).where(
                EntityAlias.source_system == "sec.cik",
                EntityAlias.source_id == issuer_cik,
            )
        )
    ).scalar_one_or_none()


async def _emit_position_edge(
    session: AsyncSession,
    *,
    owner_canonical: str,
    issuer_canonical: str,
    owner: ReportingOwner,
    issuer_cik: str,
    filing: dict,
    batch_id: str | None,
    stats: SecInsiderStats,
) -> None:
    """Create-or-reuse a cited ``held_position`` owner → issuer edge.

    Idempotent twice over: the edge dedupes on
    ``(source, target, relation)`` and the citation on the accession
    number, so a re-run adds only genuinely new filings. The citation is
    written in the same flush as a new edge, so the 0-uncited invariant
    never has a window in which it is false.
    """
    existing = (
        await session.execute(
            select(CanonicalEdge).where(
                CanonicalEdge.source_id == owner_canonical,
                CanonicalEdge.target_id == issuer_canonical,
                CanonicalEdge.relation == EdgeRelation.HELD_POSITION.value,
            )
        )
    ).scalar_one_or_none()

    metadata = {
        "is_director": owner.is_director,
        "is_officer": owner.is_officer,
        "is_ten_percent_owner": owner.is_ten_percent_owner,
        "officer_title": owner.officer_title,
        "role": owner.role_label,
        "issuer_cik": issuer_cik,
        "owner_cik": owner.cik,
        "latest_form": filing["form"],
        "latest_filing_date": filing["date"],
        "source": "sec.ownership",
    }

    if existing is None:
        edge = CanonicalEdge(
            source_id=owner_canonical,
            target_id=issuer_canonical,
            relation=EdgeRelation.HELD_POSITION.value,
            weight=0.0,
            edge_metadata=metadata,
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
        edge = existing
        stats.edges_reused += 1
        # READ-GATE, same rule as fec_individual: a staged run must not
        # move a live published edge. This one did not fire on P1.7 (all
        # 131 filings landed on the batch's own new edges), but the hole
        # is identical and a future domain whose insiders already have
        # published held_position edges would walk into it.
        if batch_id and edge.publication_state == PublicationState.PUBLISHED.value:
            stats.published_edges_skipped += 1
            return
        # The relationship flags are as-of the LATEST filing that names
        # this person. Refresh when we are looking at one at least as
        # recent as what is stored, so a promotion (director → director
        # + CEO) or a departure correction lands. Comparing ISO dates as
        # strings is safe and keeps the write idempotent.
        stored = edge.edge_metadata or {}
        if str(filing["date"]) >= str(stored.get("latest_filing_date") or ""):
            edge.edge_metadata = metadata

    # EXISTENCE check, never ``scalar_one_or_none`` — there is no unique
    # index on (edge_id, citation_ref).
    already = (
        await session.execute(
            select(SourceCitation.id).where(
                SourceCitation.edge_id == edge.id,
                SourceCitation.citation_ref == filing["accession"],
            ).limit(1)
        )
    ).first()
    if already is not None:
        stats.citations_skipped_already_cited += 1
        return
    session.add(
        SourceCitation(
            edge_id=edge.id,
            kind=SourceKind.CORPORATE_REGISTRY.value,
            citation_url=_filing_index_url(str(int(issuer_cik)), filing["accession"]),
            citation_ref=filing["accession"],
        )
    )
    edge.weight = float((edge.weight or 0.0) + 1.0)
    stats.citations_created += 1


# ─── the pass ───────────────────────────────────────────────────────────


async def ingest_issuer_insiders(
    cik: int,
    *,
    batch_id: str | None = None,
    max_filings: int = 120,
    stats: SecInsiderStats | None = None,
    request_delay: float = 0.12,
) -> SecInsiderStats:
    """Ingest one issuer's Section 16 insiders."""
    stats = stats or SecInsiderStats()
    cik10 = str(cik).zfill(10)
    sm = get_sessionmaker()

    async with sm() as session:
        issuer_canonical = await _resolve_issuer_canonical(session, cik10)
    if issuer_canonical is None:
        logger.error(
            "sec_insiders: issuer CIK %s has no sec.cik alias — run "
            "domain_anchors first; refusing to mint an issuer here",
            cik10,
        )
        stats.errors += 1
        return stats

    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            submissions = await _get_json(
                client, _SUBMISSIONS_URL.format(cik10=cik10)
            )
        except Exception:
            logger.exception("sec_insiders: submissions fetch failed cik=%s", cik10)
            stats.errors += 1
            return stats

        filings = iter_ownership_filings(submissions)[:max_filings]
        stats.filings_listed += len(filings)

        async with sm() as session:
            for filing in filings:
                url = ownership_document_url(
                    cik, filing["accession"], filing["primary_document"]
                )
                try:
                    xml = await _get_text(client, url)
                except Exception:
                    logger.warning(
                        "sec_insiders: document fetch failed %s", url
                    )
                    stats.filings_unparsable += 1
                    continue
                await asyncio.sleep(request_delay)
                stats.filings_fetched += 1

                parsed = parse_ownership_document(xml)
                if parsed is None:
                    stats.filings_unparsable += 1
                    continue
                # FAIL-CLOSED: only trust a document that names OUR issuer.
                if parsed.issuer_cik != cik10:
                    stats.filings_issuer_mismatch += 1
                    continue

                for owner in parsed.owners:
                    stats.owners_seen += 1
                    if owner.cik == cik10:
                        stats.self_filed_skipped += 1
                        continue
                    if not owner.holds_position:
                        stats.ten_percent_only_skipped += 1
                        continue
                    try:
                        # SAVEPOINT per owner: one bad row must not
                        # poison the transaction for the whole issuer.
                        async with session.begin_nested():
                            owner_canonical = await _resolve_owner_canonical(
                                session, owner, batch_id=batch_id, stats=stats
                            )
                            if await attach_alias(
                                session,
                                owner_canonical,
                                SEC_OWNER_NAMESPACE,
                                owner.cik,
                                owner.name,
                                kind_hint="person",
                                stats=stats,
                                label=owner.name,
                            ):
                                stats.owner_aliases_created += 1
                            # Hard scrutiny signal — a filed director/officer.
                            if await attach_alias(
                                session,
                                owner_canonical,
                                OFFICER_SIGNAL_NAMESPACE,
                                f"{cik10}:{owner.cik}",
                                owner.name,
                                kind_hint="person",
                                stats=stats,
                                label=owner.name,
                            ):
                                stats.officer_signal_aliases_created += 1
                            await _emit_position_edge(
                                session,
                                owner_canonical=owner_canonical,
                                issuer_canonical=issuer_canonical,
                                owner=owner,
                                issuer_cik=cik10,
                                filing=filing,
                                batch_id=batch_id,
                                stats=stats,
                            )
                    except Exception:
                        logger.exception(
                            "sec_insiders: owner failed cik=%s owner=%s",
                            cik10, owner.cik,
                        )
                        stats.errors += 1
            try:
                await session.commit()
            except Exception:
                await session.rollback()
                stats.errors += 1
                logger.exception("sec_insiders commit failed cik=%s", cik10)

    # Report what the pass concluded, per (owner, issuer).
    async with sm() as session:
        rows = (
            await session.execute(
                select(CanonicalEntity, CanonicalEdge)
                .join(CanonicalEdge, CanonicalEdge.source_id == CanonicalEntity.id)
                .where(
                    CanonicalEdge.target_id == issuer_canonical,
                    CanonicalEdge.relation == EdgeRelation.HELD_POSITION.value,
                )
            )
        ).all()
        for ent, edge in rows:
            if edge.edge_metadata.get("issuer_cik") != cik10:
                continue
            stats.positions.append(
                {
                    "person": ent.canonical_name,
                    "canonical_id": ent.id,
                    "surface_mode": ent.surface_mode,
                    "publication_state": ent.publication_state,
                    "owner_cik": edge.edge_metadata.get("owner_cik"),
                    "issuer_cik": cik10,
                    "role": edge.edge_metadata.get("role"),
                    "citations": int(edge.weight or 0),
                }
            )
            if ent.surface_mode != SurfaceMode.OPEN.value:
                stats.non_open_insiders.append(
                    {
                        "person": ent.canonical_name,
                        "canonical_id": ent.id,
                        "surface_mode": ent.surface_mode,
                    }
                )
    stats.issuers_processed += 1
    return stats


async def ingest_from_registry(
    priority_domains: tuple[str, ...] | None = None,
    *,
    batch_id: str | None = None,
    max_filings_per_issuer: int = 120,
) -> SecInsiderStats:
    """Sweep every anchor with at least one issuer CIK."""
    from app.services.anchor_registry import anchors_for_sec_insiders

    stats = SecInsiderStats()
    sm = get_sessionmaker()
    async with sm() as session:
        anchors = await anchors_for_sec_insiders(
            session, priority_domains=priority_domains
        )
    for anchor in anchors:
        for cik in anchor.sec_ciks:
            logger.info(
                "sec_insiders: anchor=%s cik=%s", anchor.label, str(cik).zfill(10)
            )
            await ingest_issuer_insiders(
                cik,
                batch_id=batch_id,
                max_filings=max_filings_per_issuer,
                stats=stats,
            )
    return stats


def main() -> None:
    """CLI — ``python -m app.services.ingest.sec_insiders``."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    ap = argparse.ArgumentParser(
        description="SEC Form 3/4/5 insiders → cited held_position edges"
    )
    ap.add_argument("--priority-domain", action="append", default=None)
    ap.add_argument("--cik", action="append", type=int, default=None,
                    help="ingest these issuer CIKs instead of the registry")
    ap.add_argument("--batch-id", default=None)
    ap.add_argument("--max-filings", type=int, default=120)
    ap.add_argument("--json-report", default=None)
    args = ap.parse_args()

    if not os.environ.get("SEC_USER_AGENT"):
        logger.info(
            "SEC_USER_AGENT unset — using the built-in descriptive default"
        )

    if args.cik:
        async def _run() -> SecInsiderStats:
            stats = SecInsiderStats()
            for cik in args.cik:
                await ingest_issuer_insiders(
                    cik, batch_id=args.batch_id,
                    max_filings=args.max_filings, stats=stats,
                )
            return stats
        stats = asyncio.run(_run())
    else:
        stats = asyncio.run(
            ingest_from_registry(
                tuple(args.priority_domain) if args.priority_domain else None,
                batch_id=args.batch_id,
                max_filings_per_issuer=args.max_filings,
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
