"""P4 anchor registry — the single source of truth for what to ingest.

Ingesters (fec/usaspending/senate_lda/sec_edgar) resolve their target set
from ``anchor_registry`` rows filtered to the source they care about, rather
than reading a per-module hardcoded constant.  The prior per-module constants
(``DETENTION_INDUSTRY_PACS``, ``DETENTION_INDUSTRY_RECIPIENTS``,
``DETENTION_INDUSTRY_LDA_CLIENTS``, ``sec_edgar.DEFAULT_ANCHORS``) get seeded
into the registry once + then removed.

``AnchorRegistry`` shape (see ``app.models``):

* ``label`` + ``entity_type`` — uniquely name the anchor. Used for logging.
* ``priority_domain`` — free-text tag (``detention_operators``,
  ``prison_telecom``, ``congress``, ``surveillance``, ``musk_network``); the
  priority-set-driven ingestion filters on it.
* ``fec_committee_ids`` / ``fec_candidate_ids`` — external IDs; **preferred
  over name matching** (helen 2026-07-19: names give "AMERICA PAC"=FXAIX-fund).
* ``sec_cik`` — SEC's zero-padded-to-10 CIK as a bigint.
* ``usaspending_recipient_names`` / ``lda_client_names`` — external name
  fallbacks when there's no external-ID crosswalk.
* ``name_variants`` — free-form alternates used for name-search fallbacks
  (e.g. FEC committee-name-search queries).
* ``surface_mode`` — open / alias / suppress; fail-closed on merges (see
  design §3 privacy guardrail).

The lookups here are thin — no I/O to external systems, only Postgres. The
service is intentionally decoupled from the ingesters (`app.services.ingest.*`)
so tests can swap the registry read for a fixture without wiring in the whole
ingest stack.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Iterable, Sequence

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import AnchorRegistry


def _as_ints(values) -> list[int]:
    """Coerce a keyring list to ints, dropping anything non-numeric.

    The keyring is JSONB written by seeds and by operators, so a value
    can arrive as ``"12345"``, ``12345`` or a typo. A malformed id is
    DROPPED rather than coerced — an ingest filter built from a bad id
    would silently select the wrong entity.
    """
    out: list[int] = []
    for v in values or []:
        if v is None:
            continue
        try:
            out.append(int(str(v).strip()))
        except (TypeError, ValueError):
            continue
    return out


@dataclass(frozen=True)
class Anchor:
    """A resolved anchor row — a plain read-model over :class:`AnchorRegistry`.

    Ingesters take this shape rather than the ORM row so they can be tested
    with hand-built fixtures. The list fields default-empty so a caller
    can always ``for x in a.fec_committee_ids`` without a None-guard.
    """

    label: str
    entity_type: str
    priority_domain: str | None = None
    fec_committee_ids: list[str] = field(default_factory=list)
    fec_candidate_ids: list[str] = field(default_factory=list)
    sec_cik: int | None = None
    usaspending_recipient_names: list[str] = field(default_factory=list)
    lda_client_names: list[str] = field(default_factory=list)
    name_variants: list[str] = field(default_factory=list)
    #: P1.6 external-ID keyring (see ``AnchorRegistry.external_ids``).
    #: ``usaspending_uei`` / ``lda_client_ids`` / ``lda_registrant_ids`` /
    #: ``sec_ciks`` / ``sec_owner_cik``.
    external_ids: dict = field(default_factory=dict)
    surface_mode: str = "open"
    canonical_id: str | None = None
    notes: str | None = None

    # ── typed accessors over the keyring ────────────────────────────
    #
    # Every accessor NORMALIZES and DROPS malformed values rather than
    # passing them through: a bad id in the keyring must not become a
    # silently-wrong ingest filter.

    @property
    def usaspending_uei(self) -> list[str]:
        """Recipient UEIs — uppercased, 12-char alphanumeric only."""
        out = []
        for v in self.external_ids.get("usaspending_uei") or []:
            u = str(v).strip().upper()
            if len(u) == 12 and u.isalnum():
                out.append(u)
        return out

    @property
    def lda_client_ids(self) -> list[int]:
        """Senate LDA client ids (numeric)."""
        return _as_ints(self.external_ids.get("lda_client_ids"))

    @property
    def lda_registrant_ids(self) -> list[int]:
        """Senate LDA registrant ids (numeric)."""
        return _as_ints(self.external_ids.get("lda_registrant_ids"))

    @property
    def sec_ciks(self) -> list[int]:
        """Every issuer CIK for this anchor — the typed ``sec_cik``
        column first, then any secondaries in the keyring, deduped."""
        out: list[int] = []
        if self.sec_cik is not None:
            out.append(int(self.sec_cik))
        for cik in _as_ints(self.external_ids.get("sec_ciks")):
            if cik not in out:
                out.append(cik)
        return out

    @property
    def sec_owner_cik(self) -> int | None:
        """Form 3/4/5 reporting-owner CIK — PERSON anchors only."""
        vals = _as_ints([self.external_ids.get("sec_owner_cik")])
        return vals[0] if vals else None

    @classmethod
    def from_row(cls, row: AnchorRegistry) -> "Anchor":
        return cls(
            label=row.label,
            entity_type=row.entity_type,
            priority_domain=row.priority_domain,
            fec_committee_ids=list(row.fec_committee_ids or []),
            fec_candidate_ids=list(row.fec_candidate_ids or []),
            sec_cik=row.sec_cik,
            usaspending_recipient_names=list(
                row.usaspending_recipient_names or []
            ),
            lda_client_names=list(row.lda_client_names or []),
            name_variants=list(row.name_variants or []),
            # ``getattr`` for the same reason the lists use ``or []``:
            # this read model is also built from hand-rolled row
            # objects in tests and from pre-migration rows.
            external_ids=dict(getattr(row, "external_ids", None) or {}),
            surface_mode=row.surface_mode,
            canonical_id=row.canonical_id,
            notes=row.notes,
        )


async def list_anchors(
    session: AsyncSession,
    *,
    priority_domains: Sequence[str] | None = None,
    entity_types: Sequence[str] | None = None,
) -> list[Anchor]:
    """Return every anchor matching the given filters (both AND'd).

    ``priority_domains`` / ``entity_types`` are optional include-filters
    (empty means "all rows"). Ordered by ``label`` for stable log output.
    """
    stmt = select(AnchorRegistry).order_by(AnchorRegistry.label)
    if priority_domains:
        stmt = stmt.where(
            AnchorRegistry.priority_domain.in_(list(priority_domains))
        )
    if entity_types:
        stmt = stmt.where(AnchorRegistry.entity_type.in_(list(entity_types)))
    rows = (await session.execute(stmt)).scalars().all()
    return [Anchor.from_row(r) for r in rows]


async def anchors_for_fec(
    session: AsyncSession,
    *,
    priority_domains: Sequence[str] | None = None,
) -> list[Anchor]:
    """Anchors the FEC ingester (PAC-mode) should sweep — anything with
    a committee ID or the name-search fallback.

    EXCLUDES ``entity_type='person'``: persons go through the
    individual-contributor path (Schedule A) via
    :func:`anchors_for_fec_individual`. Without this exclusion the
    full-sweep would fire 537 congress-member name-searches at the
    FEC ``/committees/`` endpoint and get 429'd within seconds.
    """
    anchors = await list_anchors(session, priority_domains=priority_domains)
    return [
        a for a in anchors
        if a.entity_type != "person"
        and (a.fec_committee_ids or a.name_variants)
    ]


async def anchors_for_fec_individual(
    session: AsyncSession,
    *,
    priority_domains: Sequence[str] | None = None,
) -> list[Anchor]:
    """Anchors the FEC ingester (individual-contributor mode, Schedule
    A) should sweep — mega-donor persons only.

    Explicitly EXCLUDES the ``congress`` priority domain: congress
    members RECEIVE contributions (they're aliased so incoming FEC
    contributions resolve to them via ``fec.candidate``); they aren't
    mega-donors. Running individual-contributor mode on 537 members
    burns ~1600 Schedule A requests and 429's the FEC key (helen
    2026-07-19 20:08Z).

    Persons carry LAST,FIRST in ``name_variants`` for the contributor
    query shape.
    """
    anchors = await list_anchors(
        session,
        priority_domains=priority_domains,
        entity_types=("person",),
    )
    return [a for a in anchors if a.priority_domain != "congress"]


async def anchors_for_usaspending(
    session: AsyncSession,
    *,
    priority_domains: Sequence[str] | None = None,
) -> list[Anchor]:
    """Anchors the USAspending ingester should sweep — anything with a
    non-empty ``usaspending_recipient_names``."""
    anchors = await list_anchors(session, priority_domains=priority_domains)
    return [a for a in anchors if a.usaspending_recipient_names]


async def anchors_for_senate_lda(
    session: AsyncSession,
    *,
    priority_domains: Sequence[str] | None = None,
) -> list[Anchor]:
    """Anchors the LDA ingester should sweep — anything with a non-empty
    ``lda_client_names``."""
    anchors = await list_anchors(session, priority_domains=priority_domains)
    return [a for a in anchors if a.lda_client_names]


async def anchors_for_usaspending_uei(
    session: AsyncSession,
    *,
    priority_domains: Sequence[str] | None = None,
) -> list[Anchor]:
    """P1.6 — anchors the UEI-keyed USAspending pass should sweep.

    Distinct from :func:`anchors_for_usaspending`, which sweeps on
    ``usaspending_recipient_names`` (a fuzzy search string). This one
    requires a real recipient UEI, so the pass can verify every award
    row it accepts against an external id.
    """
    anchors = await list_anchors(session, priority_domains=priority_domains)
    return [a for a in anchors if a.usaspending_uei]


async def anchors_for_lda_ids(
    session: AsyncSession,
    *,
    priority_domains: Sequence[str] | None = None,
) -> list[Anchor]:
    """P1.6 — anchors the id-keyed Senate LDA pass should sweep.

    Requires ``external_ids.lda_client_ids``; the name-keyed pass
    (:func:`anchors_for_senate_lda`) stays for the older domains.
    """
    anchors = await list_anchors(session, priority_domains=priority_domains)
    return [a for a in anchors if a.lda_client_ids]


async def anchors_for_sec_insiders(
    session: AsyncSession,
    *,
    priority_domains: Sequence[str] | None = None,
) -> list[Anchor]:
    """P1.6 — anchors whose SEC Form 3/4/5 insiders should be ingested.

    Any anchor with at least one issuer CIK qualifies; the pass reads
    the issuer's ownership filings and emits officer/director edges
    keyed on (issuer CIK, reporting-owner CIK).
    """
    anchors = await list_anchors(session, priority_domains=priority_domains)
    return [a for a in anchors if a.sec_ciks]


async def anchors_for_sec_edgar(
    session: AsyncSession,
    *,
    priority_domains: Sequence[str] | None = None,
) -> list[Anchor]:
    """Anchors the SEC EDGAR ingester should sweep — anything with a
    non-null ``sec_cik``."""
    anchors = await list_anchors(session, priority_domains=priority_domains)
    return [a for a in anchors if a.sec_cik is not None]


async def upsert_anchor(
    session: AsyncSession,
    *,
    label: str,
    entity_type: str,
    priority_domain: str | None = None,
    fec_committee_ids: Iterable[str] = (),
    fec_candidate_ids: Iterable[str] = (),
    sec_cik: int | None = None,
    usaspending_recipient_names: Iterable[str] = (),
    lda_client_names: Iterable[str] = (),
    name_variants: Iterable[str] = (),
    external_ids: dict | None = None,
    surface_mode: str = "open",
    canonical_id: str | None = None,
    notes: str | None = None,
) -> AnchorRegistry:
    """Idempotent upsert keyed on ``(label, entity_type)``.

    Overwrites the JSONB list fields — the intended semantics for seeds is
    "declare the full authoritative set at this call". Callers that want
    to APPEND should pre-read the existing row + merge.
    """
    existing = (
        await session.execute(
            select(AnchorRegistry).where(
                AnchorRegistry.label == label,
                AnchorRegistry.entity_type == entity_type,
            )
        )
    ).scalar_one_or_none()

    if existing is None:
        row = AnchorRegistry(
            label=label,
            entity_type=entity_type,
            priority_domain=priority_domain,
            fec_committee_ids=list(fec_committee_ids),
            fec_candidate_ids=list(fec_candidate_ids),
            sec_cik=sec_cik,
            usaspending_recipient_names=list(usaspending_recipient_names),
            lda_client_names=list(lda_client_names),
            name_variants=list(name_variants),
            external_ids=dict(external_ids or {}),
            surface_mode=surface_mode,
            canonical_id=canonical_id,
            notes=notes,
        )
        session.add(row)
        await session.flush()
        return row

    existing.priority_domain = priority_domain
    existing.fec_committee_ids = list(fec_committee_ids)
    existing.fec_candidate_ids = list(fec_candidate_ids)
    existing.sec_cik = sec_cik
    existing.usaspending_recipient_names = list(usaspending_recipient_names)
    existing.lda_client_names = list(lda_client_names)
    existing.name_variants = list(name_variants)
    existing.external_ids = dict(external_ids or {})
    existing.surface_mode = surface_mode
    existing.canonical_id = canonical_id
    existing.notes = notes
    await session.flush()
    return existing
