"""D2 — resolve + emit cited disclosure edges (Scrutiny fail-closed).

Reads HIGH rows from ``disclosure_rows`` for a given ``doc_id`` and, per
row, emits one :class:`CanonicalEdge` from the filer to the target
entity with a :class:`SourceCitation` that points at the archived PDF
page (``source_url#page=N``) AND back at the ``disclosure_rows`` row via
``disclosure_row_id`` FK.

Contracts (helen dispatch 2026-08-05):

* Filer (Trump) = PERSON, ``surface_mode=OPEN`` (public figure).
* Organizations / funds / issuers / banks / agencies = ``OPEN``.
* Every **named natural person** that lands as an emit target — Part 8
  creditors that resolve as natural persons, Part 3 counterparties that
  end up structured — is created with ``surface_mode=SUPPRESS`` **at
  emit time**. Promotion to OPEN / ALIAS is a SEPARATE per-person
  transaction after :func:`app.services.scrutiny.scrutinize_person`
  classifies. There must be no fail-open window even mid-build.
* Every edge cites its ``disclosure_rows.id`` + ``page``. Reconciliation
  counts by (part, relation) match the source HIGH-row count for each
  bucket — any row that didn't emit is REPORTED (not silently dropped).
* Bands stored as bands + numeric ``band_low`` / ``band_high``. Never a
  point value on top of a band.
"""

from __future__ import annotations

import logging
import re
from collections.abc import Iterable
from dataclasses import dataclass, field

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import (
    CanonicalEdge,
    CanonicalEntity,
    DisclosureDocument,
    DisclosureRow,
    EdgeRelation,
    EntityType,
    SourceCitation,
    SourceKind,
    SurfaceMode,
)
from app.services.disclosure_bands import bounds_of
from app.services.disclosure_issuer import normalize_issuer
from app.services.disclosure_parser import Part
from app.services.graph.base import normalize_name
from app.services.graph.pgvector_store import PgVectorStore

logger = logging.getLogger(__name__)


# ─── Data shapes ────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class EmitSummary:
    """Return value of :func:`emit_edges_for_doc` — the D2 gate metrics."""

    doc_id: str
    filer_canonical_id: str

    high_rows_read: int
    edges_emitted: int
    citations_emitted: int
    persons_suppressed_at_emit: int

    per_relation: dict[str, int]
    unresolved: list[str] = field(default_factory=list)
    """``disclosure_rows.id`` values that didn't produce an edge, WITH
    reason — surfaced in the reconciliation report so nothing is
    silently dropped."""


# ─── Public entrypoint ──────────────────────────────────────────────


FILER_NAME = "Donald J. Trump"
FILER_TYPE = EntityType.PERSON.value


async def emit_edges_for_doc(doc_id: str, *, commit_every: int = 500) -> EmitSummary:
    """Emit D2 edges for one archived disclosure document.

    Idempotent: re-running is safe. Edge dedup is by
    ``(source_id, target_id, relation)``; citation dedup is by
    ``(edge_id, disclosure_row_id)``. Re-running does not create
    duplicate persons.

    Progress + throughput: commits are batched every ``commit_every``
    rows so the session doesn't accumulate 27k+ pending operations,
    and a resolve cache short-circuits the O(n)-per-call name-match
    fallback in the pgvector store when the same issuer name recurs
    (typical in a Trump-annual: AAPL appears in every one of 8
    accounts × ~3-5 times).
    """
    sm = get_sessionmaker()
    async with sm() as db:
        doc = await db.get(DisclosureDocument, doc_id)
        if doc is None:
            raise ValueError(f"disclosure_documents/{doc_id} not found")

        filer_id = await _get_or_create_filer(db)
        await db.commit()  # persist filer independently of the emit batch loop

    stats: dict[str, int] = {}
    unresolved: list[str] = []
    edges_emitted = 0
    citations_emitted = 0
    persons_suppressed = 0
    high_rows_total = 0

    # (surface_name_normalized, entity_type) -> canonical_id.
    # Bounded by the count of distinct issuer/creditor names in the doc
    # (Trump 2026: a few thousand distinct issuers at most).
    resolve_cache: dict[tuple[str, str], str] = {}

    async with sm() as db:
        # Reload the doc + filer inside the emit-loop session so ORM
        # identity is fresh.
        doc = await db.get(DisclosureDocument, doc_id)

        rows = (
            (
                await db.execute(
                    select(DisclosureRow)
                    .where(DisclosureRow.doc_id == doc_id)
                    .where(DisclosureRow.parse_confidence == "high")
                    .order_by(DisclosureRow.part, DisclosureRow.page, DisclosureRow.row_index)
                )
            )
            .scalars()
            .all()
        )
        high_rows_total = len(rows)
        logger.info("D2 emit: %d HIGH rows for doc %s", high_rows_total, doc_id)

        for i, row in enumerate(rows, start=1):
            result = await _emit_row(
                db,
                doc=doc,
                filer_id=filer_id,
                row=row,
                resolve_cache=resolve_cache,
            )
            if result is None:
                unresolved.append(f"{row.id} [{row.part}]: no target derivable")
                continue
            relation, made_edge, made_citation, made_person_suppressed = result
            stats[relation] = stats.get(relation, 0) + (1 if made_edge else 0)
            if made_edge:
                edges_emitted += 1
            if made_citation:
                citations_emitted += 1
            if made_person_suppressed:
                persons_suppressed += 1
            if i % commit_every == 0:
                await db.commit()
                logger.info(
                    "D2 emit: committed %d/%d rows (edges=%d citations=%d resolves=%d)",
                    i,
                    high_rows_total,
                    edges_emitted,
                    citations_emitted,
                    len(resolve_cache),
                )

        await db.commit()
        logger.info(
            "D2 emit DONE: rows=%d edges=%d citations=%d persons_suppressed=%d unresolved=%d",
            high_rows_total,
            edges_emitted,
            citations_emitted,
            persons_suppressed,
            len(unresolved),
        )

    return EmitSummary(
        doc_id=doc_id,
        filer_canonical_id=filer_id,
        high_rows_read=high_rows_total,
        edges_emitted=edges_emitted,
        citations_emitted=citations_emitted,
        persons_suppressed_at_emit=persons_suppressed,
        per_relation=stats,
        unresolved=unresolved,
    )


# ─── Filer ──────────────────────────────────────────────────────────


async def _get_or_create_filer(db: AsyncSession) -> str:
    """Find or create the Donald J. Trump canonical entity.

    Uses the graph store's name+embedding resolver first. If unresolved,
    creates a new PERSON canonical with ``surface_mode=OPEN`` (POTUS is
    a public official — the classic OPEN case).
    """
    store = PgVectorStore()
    resolved = await store.resolve_entity(
        db,
        surface_name=FILER_NAME,
        entity_type=FILER_TYPE,
        embedding=None,  # deterministic name-match fallback
    )
    if resolved is not None:
        return resolved

    # Create fresh — POTUS is unambiguously OPEN.
    filer = CanonicalEntity(
        canonical_name=FILER_NAME,
        canonical_name_normalized=normalize_name(FILER_NAME),
        type=FILER_TYPE,
        surface_mode=SurfaceMode.OPEN.value,
    )
    db.add(filer)
    await db.flush()
    return filer.id


# ─── Per-row emit ───────────────────────────────────────────────────


async def _emit_row(
    db: AsyncSession,
    *,
    doc: DisclosureDocument,
    filer_id: str,
    row: DisclosureRow,
    resolve_cache: dict[tuple[str, str], str],
) -> tuple[str, bool, bool, bool] | None:
    """Emit one edge (+ its citation) for one HIGH ledger row.

    Returns ``(relation, made_edge, made_citation, made_person_suppressed)``
    or ``None`` if the row's shape can't be turned into an edge (which
    still counts as a reconciliation gap the report will surface).
    """
    parsed = row.parsed or {}
    part = row.part

    if part in (
        Part.PART_2_EMPL_ASSETS.value,
        Part.PART_5_SPOUSE_ASSETS.value,
        Part.PART_6_OTHER_ASSETS.value,
    ):
        return await _emit_asset(
            db,
            doc=doc,
            filer_id=filer_id,
            row=row,
            parsed=parsed,
            resolve_cache=resolve_cache,
        )

    if part == Part.PART_1_POSITIONS.value:
        return await _emit_position(
            db,
            doc=doc,
            filer_id=filer_id,
            row=row,
            parsed=parsed,
            resolve_cache=resolve_cache,
        )

    if part == Part.PART_8_LIABILITIES.value:
        return await _emit_liability(
            db,
            doc=doc,
            filer_id=filer_id,
            row=row,
            parsed=parsed,
            resolve_cache=resolve_cache,
        )

    # D3 (2026-08-06) — Part 7 transactions: Trump → issuer TRADED
    # edges with (transaction_type, trade_date, amount_band).
    if part == Part.PART_7_TRANSACTIONS.value:
        return await _emit_transaction(
            db,
            doc=doc,
            filer_id=filer_id,
            row=row,
            parsed=parsed,
            resolve_cache=resolve_cache,
        )

    # Parts 3 (agreements), 4 (comp), 9 (gifts) still not in scope.
    # Parts 3/4 have no HIGH rows (all narrative); Part 9 is
    # scrutiny-heavy (named private donors) and follows in a later
    # build.
    return None


# ─── Assets (Parts 2/5/6) → holds_asset + optional income_from ───────


async def _emit_asset(
    db: AsyncSession,
    *,
    doc: DisclosureDocument,
    filer_id: str,
    row: DisclosureRow,
    parsed: dict,
    resolve_cache: dict[tuple[str, str], str],
) -> tuple[str, bool, bool, bool] | None:
    """Emit ``holds_asset`` (always) + ``income_from`` (when income_band
    is present + non-trivial) for a Parts 2/5/6 row."""

    description = (parsed.get("description") or "").strip()
    value_band = parsed.get("value_band")
    if not description or not value_band:
        return None

    # D2.1 (2026-08-05): normalize to the ISSUER before resolution so
    # ``NETFLIX INC REG S DUE ...`` resolves to the pre-existing NETFLIX
    # canonical instead of minting a new junky-token node. Fragmentation
    # incident: 3387 new vs 784 matched targets in the initial D2 run.
    issuer_name = normalize_issuer(description) or description
    tgt_type = _guess_org_type(issuer_name)
    tgt_id = await _resolve_or_create(
        db,
        surface_name=issuer_name,
        entity_type=tgt_type,
        surface_mode=SurfaceMode.OPEN.value,
        resolve_cache=resolve_cache,
    )
    if tgt_id is None:
        return None

    v_low, v_high = _bounds(value_band)
    edge_meta = {
        "value_band": value_band,
        "band_low": v_low,
        "band_high": v_high,
        "eif": parsed.get("eif"),
        "account_group": row.account_group,
        "part": row.part,
    }
    made_edge, edge_id = await _upsert_edge(
        db,
        source_id=filer_id,
        target_id=tgt_id,
        relation=EdgeRelation.HOLDS_ASSET.value,
        edge_metadata=edge_meta,
    )
    made_citation = await _upsert_citation(
        db,
        edge_id=edge_id,
        doc=doc,
        row=row,
    )
    # income_from — only when a real income band is present.
    income_band = parsed.get("income_band")
    if income_band and income_band not in _TRIVIAL_INCOME_BANDS:
        i_low, i_high = _bounds(income_band)
        income_meta = {
            "income_type": parsed.get("income_type"),
            "income_band": income_band,
            "band_low": i_low,
            "band_high": i_high,
            "account_group": row.account_group,
            "part": row.part,
        }
        _, income_edge_id = await _upsert_edge(
            db,
            source_id=filer_id,
            target_id=tgt_id,
            relation=EdgeRelation.INCOME_FROM.value,
            edge_metadata=income_meta,
        )
        await _upsert_citation(
            db,
            edge_id=income_edge_id,
            doc=doc,
            row=row,
        )
    return (EdgeRelation.HOLDS_ASSET.value, made_edge, made_citation, False)


# ─── Transactions (Part 7 annual + 278-T) → traded ──────────────────


async def _emit_transaction(
    db: AsyncSession,
    *,
    doc: DisclosureDocument,
    filer_id: str,
    row: DisclosureRow,
    parsed: dict,
    resolve_cache: dict[tuple[str, str], str],
) -> tuple[str, bool, bool, bool] | None:
    """Emit a ``traded`` edge (Trump → issuer) for a Part 7 (annual) or
    278-T (periodic) HIGH row.

    Same resolve-to-existing-canonical path as ``_emit_asset`` — the
    ``normalize_issuer`` step is what stops the transaction cargo
    (bond descriptor, share class) from creating fragments.
    """
    description = (parsed.get("description") or "").strip()
    amount_band = parsed.get("amount_band")
    transaction_type = parsed.get("transaction_type")
    trade_date = parsed.get("trade_date")
    if not description or not amount_band or not transaction_type:
        return None

    issuer_name = normalize_issuer(description) or description
    tgt_type = _guess_org_type(issuer_name)
    tgt_id = await _resolve_or_create(
        db,
        surface_name=issuer_name,
        entity_type=tgt_type,
        surface_mode=SurfaceMode.OPEN.value,
        resolve_cache=resolve_cache,
    )
    if tgt_id is None:
        return None

    a_low, a_high = _bounds(amount_band)
    edge_meta = {
        "transaction_type": transaction_type,
        "trade_date": trade_date,
        "amount_band": amount_band,
        "band_low": a_low,
        "band_high": a_high,
        "account_group": row.account_group,
        "part": row.part,
    }
    made_edge, edge_id = await _upsert_edge(
        db,
        source_id=filer_id,
        target_id=tgt_id,
        relation=EdgeRelation.TRADED.value,
        edge_metadata=edge_meta,
    )
    made_citation = await _upsert_citation(db, edge_id=edge_id, doc=doc, row=row)
    return (EdgeRelation.TRADED.value, made_edge, made_citation, False)


# ─── Positions (Part 1) → held_position ─────────────────────────────


async def _emit_position(
    db: AsyncSession,
    *,
    doc: DisclosureDocument,
    filer_id: str,
    row: DisclosureRow,
    parsed: dict,
    resolve_cache: dict[tuple[str, str], str],
) -> tuple[str, bool, bool, bool] | None:
    """Emit ``held_position`` for a Part 1 row.

    Part 1 has 4 HIGH rows in the Trump annual (CIC Digital LLC, CIC
    Ventures LLC, Mar-A-Lago Club L.L.C., JFK Center for the Performing
    Arts). D1 kept the raw column-aligned line as ``lead``; we extract
    the organization name from the leading text column here.
    """
    lead = (parsed.get("lead") or "").strip()
    if not lead:
        return None
    org_name = _extract_part1_org(lead)
    if not org_name:
        return None
    # D2.1: normalize Position org names too (strip legal-suffix cruft
    # that would otherwise create ``CIC DIGITAL LLC`` alongside
    # ``CIC Digital LLC``).
    issuer_name = normalize_issuer(org_name) or org_name
    tgt_type = _guess_org_type(issuer_name)
    tgt_id = await _resolve_or_create(
        db,
        surface_name=issuer_name,
        entity_type=tgt_type,
        surface_mode=SurfaceMode.OPEN.value,
        resolve_cache=resolve_cache,
    )
    if tgt_id is None:
        return None
    edge_meta = {
        "raw_lead": lead,
        "part": row.part,
    }
    made_edge, edge_id = await _upsert_edge(
        db,
        source_id=filer_id,
        target_id=tgt_id,
        relation=EdgeRelation.HELD_POSITION.value,
        edge_metadata=edge_meta,
    )
    made_citation = await _upsert_citation(db, edge_id=edge_id, doc=doc, row=row)
    return (EdgeRelation.HELD_POSITION.value, made_edge, made_citation, False)


# ─── Liabilities (Part 8) → owes ─────────────────────────────────────


async def _emit_liability(
    db: AsyncSession,
    *,
    doc: DisclosureDocument,
    filer_id: str,
    row: DisclosureRow,
    parsed: dict,
    resolve_cache: dict[tuple[str, str], str],
) -> tuple[str, bool, bool, bool] | None:
    """Emit ``owes`` for a Part 8 row.

    D1 parsed only ``amount_band`` + ``raw_lead`` + ``raw_tail`` for
    Part 8 (creditor column wasn't structured). D2 splits ``raw_lead``
    into (creditor, liability_type) using the fixed OGE 278e column
    geometry and classifies the creditor as PERSON vs ORG/AGENCY —
    natural-person creditors default to ``surface_mode=SUPPRESS`` at
    emit time.
    """
    amount_band = parsed.get("amount_band")
    raw_lead = (parsed.get("raw_lead") or "").strip()
    raw_tail = (parsed.get("raw_tail") or "").strip()
    if not amount_band or not raw_lead:
        return None

    creditor, liability_type = _split_part8_lead(raw_lead)
    year_incurred, rate, term = _split_part8_tail(raw_tail)

    person_suppressed = False
    tgt_type, is_person = _classify_creditor(creditor)
    # D2.1: normalize ORG creditors (banks, LLCs, agencies). Persons run
    # unchanged — normalize_issuer targets bond/security cruft, not
    # human names, and the person-conservative default demands we never
    # transform a person name.
    creditor_for_resolve = creditor if is_person else (normalize_issuer(creditor) or creditor)
    if is_person:
        tgt_id = await _resolve_or_create(
            db,
            surface_name=creditor_for_resolve,
            entity_type=tgt_type,
            surface_mode=SurfaceMode.SUPPRESS.value,
            resolve_cache=resolve_cache,
        )
        # Was it created NOW, or did it already exist?
        if tgt_id is not None:
            # We can't cheaply tell "created vs found" without a return
            # flag from the resolver, so we conservatively count every
            # emit that named a person + emitted SUPPRESS'd — helen's
            # gate is "no fail-open window", which this satisfies
            # regardless.
            person_suppressed = True
    else:
        tgt_id = await _resolve_or_create(
            db,
            surface_name=creditor_for_resolve,
            entity_type=tgt_type,
            surface_mode=SurfaceMode.OPEN.value,
            resolve_cache=resolve_cache,
        )
    if tgt_id is None:
        return None

    a_low, a_high = _bounds(amount_band)
    edge_meta = {
        "amount_band": amount_band,
        "band_low": a_low,
        "band_high": a_high,
        "liability_type": liability_type,
        "year_incurred": year_incurred,
        "rate": rate,
        "term": term,
        "part": row.part,
    }
    made_edge, edge_id = await _upsert_edge(
        db,
        source_id=filer_id,
        target_id=tgt_id,
        relation=EdgeRelation.OWES.value,
        edge_metadata=edge_meta,
    )
    made_citation = await _upsert_citation(db, edge_id=edge_id, doc=doc, row=row)
    return (
        EdgeRelation.OWES.value,
        made_edge,
        made_citation,
        person_suppressed,
    )


# ─── Resolution + upsert primitives ─────────────────────────────────


async def _resolve_or_create(
    db: AsyncSession,
    *,
    surface_name: str,
    entity_type: str,
    surface_mode: str,
    resolve_cache: dict[tuple[str, str], str],
) -> str | None:
    """Resolve ``surface_name`` in the graph store; on miss, create a
    new canonical with the given ``surface_mode``.

    IMPORTANT — the fail-closed contract for D2: when the caller asks
    for ``surface_mode=SUPPRESS`` we set that at CREATE time inside the
    same session/transaction. There is no window where a public API
    could see the person in an OPEN state.

    Uses the caller's ``resolve_cache`` (keyed by ``(normalized_name,
    entity_type)``) to short-circuit the O(n)-per-call name-match
    fallback in :class:`PgVectorStore.resolve_entity` when the same
    issuer appears in many rows (typical in a Trump-annual: APPLE INC
    appears once per account × 8 accounts, roughly).
    """
    if not surface_name:
        return None
    norm = normalize_name(surface_name)
    cache_key = (norm, entity_type)
    hit = resolve_cache.get(cache_key)
    if hit is not None:
        return hit

    # Fast normalized-name exact match FIRST — index-backed via the
    # existing ``ix_canonical_entities_type_norm`` composite index. This
    # skips the O(n) candidate scan in :meth:`PgVectorStore.resolve_entity`
    # when embedding is None.
    if norm:
        exact = (
            await db.execute(
                select(CanonicalEntity.id).where(
                    CanonicalEntity.type == entity_type,
                    CanonicalEntity.canonical_name_normalized == norm,
                )
            )
        ).scalar_one_or_none()
        if exact is not None:
            resolve_cache[cache_key] = exact
            return exact

    # New canonical.
    ent = CanonicalEntity(
        canonical_name=surface_name,
        canonical_name_normalized=norm,
        type=entity_type,
        surface_mode=surface_mode,
    )
    db.add(ent)
    await db.flush()
    resolve_cache[cache_key] = ent.id
    return ent.id


async def _upsert_edge(
    db: AsyncSession,
    *,
    source_id: str,
    target_id: str,
    relation: str,
    edge_metadata: dict,
) -> tuple[bool, str]:
    """Return ``(created, edge_id)``. Uniqueness on
    ``(source_id, target_id, relation)`` — re-running merges metadata
    into the existing edge rather than duplicating.
    """
    existing = (
        await db.execute(
            select(CanonicalEdge).where(
                CanonicalEdge.source_id == source_id,
                CanonicalEdge.target_id == target_id,
                CanonicalEdge.relation == relation,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        # On re-run, merge new metadata (last-write-wins on scalar keys).
        merged = dict(existing.edge_metadata or {})
        merged.update(edge_metadata)
        existing.edge_metadata = merged
        return False, existing.id
    edge = CanonicalEdge(
        source_id=source_id,
        target_id=target_id,
        relation=relation,
        edge_metadata=edge_metadata,
    )
    db.add(edge)
    await db.flush()
    return True, edge.id


async def _upsert_citation(
    db: AsyncSession,
    *,
    edge_id: str,
    doc: DisclosureDocument,
    row: DisclosureRow,
) -> bool:
    """Emit one OGE_278E citation on ``edge_id`` for ``row`` if not
    already present. Dedup on ``(edge_id, disclosure_row_id)`` so
    re-running is idempotent."""
    # Tolerate multiple pre-existing citations for the same
    # (edge_id, disclosure_row_id) — no unique constraint on that pair,
    # and D2.1 fragment merges may have re-parented citations onto a
    # survivor edge that already had one for the same row.
    existing = (
        await db.execute(
            select(SourceCitation.id).where(
                SourceCitation.edge_id == edge_id,
                SourceCitation.disclosure_row_id == row.id,
            ).limit(1)
        )
    ).scalar_one_or_none()
    if existing is not None:
        return False
    # Kind is dictated by the source document's form_type — annual
    # ``oge_278e`` docs cite as OGE_278E; periodic ``oge_278t`` docs
    # cite as OGE_278T (D3). Both fingerprints remain the same shape
    # (URL + page anchor + FK back to the ledger row).
    kind = (
        SourceKind.OGE_278T.value
        if doc.form_type == "oge_278t"
        else SourceKind.OGE_278E.value
    )
    citation = SourceCitation(
        edge_id=edge_id,
        kind=kind,
        citation_url=f"{doc.oge_url}#page={row.page}",
        citation_ref=f"{doc.sha256[:16]}::{row.part}::row{row.row_index}::p{row.page}",
        disclosure_row_id=row.id,
        page=row.page,
    )
    db.add(citation)
    return True


# ─── Small helpers (heuristics + parsers) ────────────────────────────


_ORG_HINTS = (
    "LLC",
    "L.L.C.",
    "INC",
    "INC.",
    "CORP",
    "CO.",
    "COMPANY",
    "TRUST",
    "BANK",
    "SAVINGS",
    "CREDIT",
    "PAC",
    "COMMITTEE",
    "FUND",
    "N.A.",
    "N.V.",
    "S.A.",
    "GROUP",
    "SYSTEMS",
    "HOLDINGS",
    "LTD",
    "LTD.",
    "PLC",
    "CAPITAL",
    "PARTNERS",
    "ASSOCIATES",
    "REIT",
    "ETF",
    "SHARES",
    "AMERICAN EXPRESS",
    "MASTERCARD",
    "VISA",
)
_AGENCY_HINTS = (
    "ATTORNEY GENERAL",
    "DEPARTMENT",
    "BUREAU",
    "COMMISSION",
    "BOARD",
    "FEDERAL",
    "GOVERNMENT",
    "STATE OF",
    "COUNTY OF",
    "CITY OF",
    "GENERAL ASSEMBLY",
    "AUTHORITY",
)


def _guess_org_type(name: str) -> str:
    """OGE issuer/fund descriptor → entity type. Assume ``ORGANIZATION``
    unless the name matches an agency-hint. Never returns PERSON — the
    caller for asset/position/liability paths handles that split
    explicitly via :func:`_classify_creditor`."""
    upper = name.upper()
    for hint in _AGENCY_HINTS:
        if hint in upper:
            return EntityType.AGENCY.value
    return EntityType.ORGANIZATION.value


def _classify_creditor(name: str) -> tuple[str, bool]:
    """Return ``(entity_type, is_person)``.

    Heuristic: agency-hint > org-hint > default PERSON. When in doubt,
    natural person (so ``surface_mode=SUPPRESS`` kicks in). Design §6
    demands ingestion default-to-protected: a false-positive that treats
    a small bank as a private person is fine (Scrutiny promotes it);
    a false-negative that treats a real person as an org is a leak.
    """
    upper = name.upper().strip()
    for hint in _AGENCY_HINTS:
        if hint in upper:
            return EntityType.AGENCY.value, False
    for hint in _ORG_HINTS:
        # Match as whole token boundary so "INC" doesn't match "Sinclair".
        if re.search(rf"(?:^|[^A-Z0-9])({re.escape(hint)})(?:[^A-Z0-9]|$)", upper):
            return EntityType.ORGANIZATION.value, False
    return EntityType.PERSON.value, True


_TRIVIAL_INCOME_BANDS = {
    "None (or less than $201)",
    "None (or less than $1,001)",
}


def _bounds(band: str) -> tuple[int, int | None]:
    """Wrap :func:`bounds_of` to always return a tuple. On an unknown
    band (a regression signal, never expected in D2's normal path) we
    return ``(0, None)`` which serializes cleanly + surfaces the gap
    without crashing the emit loop.
    """
    b = bounds_of(band)
    if b is None:
        return (0, None)
    return b


def _extract_part1_org(lead: str) -> str | None:
    """Split off the organization name from the first column of a Part
    1 position row. Column geometry is: ``org_name  city/state  org_type
    position  from  to``. Ordinary whitespace-column split is fine for
    the four positions the annual carries.
    """
    # The first token-run before ~4 or more spaces is the org name.
    m = re.match(r"^\s*(?P<org>.+?)\s{2,}", lead)
    if m is None:
        return lead.strip() or None
    return m.group("org").strip() or None


def _split_part8_lead(raw_lead: str) -> tuple[str, str | None]:
    """Split a Part 8 raw_lead into (creditor, liability_type).

    Real shapes seen in the Trump annual:
      * "The Bryn Mawr Trust Company              Seven Springs (mortgage)"
      * "Ladder Capital Finance LLC               TIHT Commercial NY (mortgage)"
      * "E. Jean Carroll                          Litigation; stayed pending appeal;"
      * "American Express                         Credit Card"

    Split on the first 4-space run; the tail is the type/description.
    """
    m = re.match(r"^\s*(?P<creditor>.+?)\s{4,}(?P<rest>.+)$", raw_lead)
    if m is None:
        return raw_lead.strip(), None
    return m.group("creditor").strip(), m.group("rest").strip() or None


_PART8_YEAR_RE = re.compile(r"\b(19\d{2}|20\d{2})\b")
_PART8_RATE_RE = re.compile(r"(\d+(?:\.\d+)?%|N/A)")


def _split_part8_tail(raw_tail: str) -> tuple[str | None, str | None, str | None]:
    """Split a Part 8 raw_tail into (year_incurred, rate, term)."""
    if not raw_tail:
        return None, None, None
    year: str | None = None
    m_year = _PART8_YEAR_RE.search(raw_tail)
    if m_year:
        year = m_year.group(1)
    rate: str | None = None
    m_rate = _PART8_RATE_RE.search(raw_tail)
    if m_rate:
        rate = m_rate.group(1)
    # ``term`` is everything after the rate token (if any); otherwise
    # everything after the year token; else the whole tail.
    tail_pos = 0
    if m_rate:
        tail_pos = m_rate.end()
    elif m_year:
        tail_pos = m_year.end()
    term = raw_tail[tail_pos:].strip() or None
    return year, rate, term


# ─── CLI entrypoint ─────────────────────────────────────────────────


async def _run_all_for_latest_doc() -> None:  # pragma: no cover — CLI shim
    """Emit edges for the most recently ingested disclosure_documents row."""
    import logging as _logging

    _logging.basicConfig(level=_logging.INFO)
    sm = get_sessionmaker()
    async with sm() as db:
        doc_id = (
            await db.execute(
                select(DisclosureDocument.id)
                .order_by(DisclosureDocument.created_at.desc())
                .limit(1)
            )
        ).scalar_one_or_none()
        if doc_id is None:
            print("no disclosure_documents rows found — run D1 ingester first")
            return
    result = await emit_edges_for_doc(doc_id)
    _print_summary(result)


async def _run_all_docs() -> None:  # pragma: no cover — D3 CLI shim
    """Emit edges for EVERY disclosure_documents row (annual + all 278-T)."""
    import logging as _logging

    _logging.basicConfig(level=_logging.INFO)
    sm = get_sessionmaker()
    async with sm() as db:
        doc_rows = (
            (
                await db.execute(
                    select(DisclosureDocument.id, DisclosureDocument.form_type,
                           DisclosureDocument.filed_date)
                    .order_by(DisclosureDocument.filed_date.asc())
                )
            )
            .all()
        )
    print(f"emitting for {len(doc_rows)} disclosure_documents:")
    for row in doc_rows:
        print(f"  {row.filed_date} {row.form_type} id={row.id}")
    for row in doc_rows:
        print(f"\n=== doc {row.id} ({row.form_type} filed {row.filed_date}) ===")
        result = await emit_edges_for_doc(row.id)
        _print_summary(result)


def _print_summary(result: EmitSummary) -> None:
    """Pretty-print an ``EmitSummary`` block."""
    print(f"doc_id                    : {result.doc_id}")
    print(f"filer_canonical_id        : {result.filer_canonical_id}")
    print(f"high_rows_read            : {result.high_rows_read}")
    print(f"edges_emitted             : {result.edges_emitted}")
    print(f"citations_emitted         : {result.citations_emitted}")
    print(f"persons_suppressed_at_emit: {result.persons_suppressed_at_emit}")
    print("per_relation:")
    for rel, n in sorted(result.per_relation.items()):
        print(f"  {rel:22s} = {n}")
    if result.unresolved:
        print(f"unresolved                : {len(result.unresolved)}")
        for line in result.unresolved[:5]:
            print(f"  {line}")


def _entrypoint() -> None:  # pragma: no cover — CLI shim
    """``python -m app.services.ingest.disclosure_emit [--all]``.

    Default: emit for the most recently ingested doc. ``--all``: emit
    for every disclosure_documents row (D3 uses this to cover annual
    Part 7 + every 278-T periodic report in one pass).
    """
    import argparse
    import asyncio

    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true",
                    help="Emit for every disclosure_documents row.")
    args = ap.parse_args()
    if args.all:
        asyncio.run(_run_all_docs())
    else:
        asyncio.run(_run_all_for_latest_doc())


if __name__ == "__main__":  # pragma: no cover
    _entrypoint()


# ``ruff`` unused-import guard for the module-level ``field`` import; it
# is used by the ``@dataclass(field=...)`` above.
_ = Iterable, field
