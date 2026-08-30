"""ORM models for Argus — canonical entity registry + relationship edges + citations.

Every edge carries a **SourceCitation** — a relationship is not shown without one.
"""

from __future__ import annotations

from datetime import date, datetime
from enum import StrEnum
from uuid import uuid4

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


def _new_id() -> str:
    """Fresh UUID4 string — DB-side default ids for portability across dialects."""
    return str(uuid4())


class EntityType(StrEnum):
    """Canonical entity kind (refined from hollywood.entity_tags.kind_hint per design §4)."""

    PERSON = "person"
    ORGANIZATION = "organization"
    PAC = "pac"
    AGENCY = "agency"
    CANDIDATE = "candidate"
    CONTRACT = "contract"
    LOBBYING_REG = "lobbying_reg"
    PLACE = "place"
    TOPIC = "topic"
    # helen T2 2026-07-17 — hollywood.entity_tags carries 15K events + 56K
    # concepts. Both are real resolvable entities (a specific war, a named
    # policy programme, an initiative). Distinct from TOPIC/theme (a heading).
    EVENT = "event"
    CONCEPT = "concept"
    # P5.1 — roll-call vote ingester lands bills as dedicated canonicals.
    BILL = "bill"
    UNKNOWN = "unknown"


class SurfaceMode(StrEnum):
    """How the public API renders this canonical (Tony 2026-07-17 refinement).

    Private people get a REAL unique node with real edges (the graph is still
    correct + supports real analysis) but the public API returns the stable
    non-identifying `public_alias` instead of `canonical_name`. Two distinct
    private people are TWO distinct canonicals — never collapsed to one generic
    "private donor" placeholder.
    """

    OPEN = "open"  # organizations, agencies, public people — real name shown
    ALIAS = "alias"  # private people — return public_alias, hide real name
    SUPPRESS = "suppress"  # not surfaced at all


class PublicationState(StrEnum):
    """RG1 (2026-08-07) — content-lifecycle gate orthogonal to surface_mode.

    ``surface_mode`` gates PRIVACY; ``publication_state`` gates whether the
    row is live on the public read path. Both gates AND — a published +
    suppress row is still 404 to the public. A published node's staged
    edges stay dark until the batch is published.

    Migration default is ``PUBLISHED`` (server_default) so the existing
    corpus stays live. Only bulk-disclosure ingests (RG3) stamp
    ``STAGED``; steady-state emitters keep the column default.
    """

    PUBLISHED = "published"
    STAGED = "staged"


class EdgeRelation(StrEnum):
    """Relationship type on a canonical edge (design §4 table)."""

    CONTRIBUTES_TO = "contributes_to"
    HOLDS_CONTRACT = "holds_contract"
    LOBBIES = "lobbies"
    SUBSIDIARY_OF = "subsidiary_of"
    EXEC_OF = "exec_of"
    AFFILIATED_WITH = "affiliated_with"
    MENTIONED_WITH = "mentioned_with"
    TAGGED_AS = "tagged_as"
    # P5.1 — roll-call votes: member → bill edges.
    VOTED_FOR = "voted_for"
    VOTED_AGAINST = "voted_against"
    # D2 (2026-08-05) — OGE 278e financial-disclosure edges.
    HOLDS_ASSET = "holds_asset"
    INCOME_FROM = "income_from"
    HELD_POSITION = "held_position"
    OWES = "owes"
    PARTY_TO_AGREEMENT = "party_to_agreement"
    # D3 (2026-08-06) — OGE Part 7 annual + 278-T periodic transactions.
    TRADED = "traded"


class SourceKind(StrEnum):
    """Where a SourceCitation points — used by the UI to render the click-through label."""

    ARTICLE_PERMALINK = "article_permalink"
    FEC_FILING = "fec_filing"
    USASPENDING_AWARD = "usaspending_award"
    SENATE_LDA = "senate_lda"
    # P5.1 — roll-call vote citations pointing at clerk/Congress.gov URLs.
    CONGRESS_VOTE = "congress_vote"
    # P1.5 (2026-08-21) — roster edges cited to the member's Biographical
    # Directory entry, sourced from unitedstates/congress-legislators.
    CONGRESS_ROSTER = "congress_roster"
    CORPORATE_REGISTRY = "corporate_registry"
    # D2 (2026-08-05) — financial-disclosure citations point at the archived
    # OGE PDF at a specific page (source_url includes ``#page=<n>``); the
    # ``disclosure_row_id`` column FKs back to the disclosure_rows ledger.
    OGE_278E = "oge_278e"
    OGE_278T = "oge_278t"


class CanonicalEntity(Base):
    """One canonical entity — the cluster of hollywood.entity_tags rows that resolved together.

    ``canonical_name`` is the representative surface form; the actual name variants
    used across artifacts live on ``EntityAlias``. ``embedding`` is the centroid over the
    cluster (used for downstream resolution of new mentions).
    """

    __tablename__ = "canonical_entities"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    canonical_name: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_name_normalized: Mapped[str] = mapped_column(Text, nullable=False)
    type: Mapped[str] = mapped_column(String(32), nullable=False)
    # 1024-dim to match hollywood.entity_tags.tag_embedding.
    embedding: Mapped[list[float] | None] = mapped_column(Vector(1024), nullable=True)
    # Tony 2026-07-17: private-person handling. `surface_mode` = OPEN by default
    # (organizations, public people). Scrutiny may set it to ALIAS (private
    # person — public API returns `public_alias`, never the real name) or
    # SUPPRESS (never surfaced). `public_alias` is a stable non-identifying
    # label like "Private donor #a1b2c3d4" — computed from the canonical id so
    # it's distinct + stable per real person.
    surface_mode: Mapped[str] = mapped_column(String(16), nullable=False, server_default="open")
    public_alias: Mapped[str | None] = mapped_column(Text, nullable=True)
    # RG1 (2026-08-07): content-lifecycle gate. Default 'published' so the
    # existing corpus stays live post-migration; only bulk disclosure
    # emitters stamp 'staged'. `batch_id` groups a bulk ingest for atomic
    # publish/unpublish.
    publication_state: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="published"
    )
    batch_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    # Neo4j idempotency stamp — populated after successful MERGE; NULL means the
    # projection sweep has not yet mirrored this row.
    projected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    aliases: Mapped[list[EntityAlias]] = relationship(back_populates="canonical")
    outgoing_edges: Mapped[list[CanonicalEdge]] = relationship(
        back_populates="source", foreign_keys="CanonicalEdge.source_id"
    )
    incoming_edges: Mapped[list[CanonicalEdge]] = relationship(
        back_populates="target", foreign_keys="CanonicalEdge.target_id"
    )

    __table_args__ = (
        Index("ix_canonical_entities_type_norm", "type", "canonical_name_normalized"),
        # Cosine HNSW to match hollywood; keeps resolve_entity O(log n).
        Index(
            "ix_canonical_entities_embedding_hnsw",
            "embedding",
            postgresql_using="hnsw",
            postgresql_with={"m": 16, "ef_construction": 64},
            postgresql_ops={"embedding": "vector_cosine_ops"},
        ),
    )


class EntityAlias(Base):
    """One (source-system, surface-name) alias that resolved to a CanonicalEntity."""

    __tablename__ = "entity_aliases"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    canonical_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("canonical_entities.id", ondelete="CASCADE"), nullable=False
    )
    # Original hollywood.entity_tags.id (or FEC/USAspending id for later phases).
    source_system: Mapped[str] = mapped_column(String(32), nullable=False)
    source_id: Mapped[str] = mapped_column(String(64), nullable=False)
    surface_name: Mapped[str] = mapped_column(Text, nullable=False)
    surface_name_normalized: Mapped[str] = mapped_column(Text, nullable=False)
    kind_hint: Mapped[str | None] = mapped_column(String(32), nullable=True)
    role: Mapped[str | None] = mapped_column(String(32), nullable=True)
    confidence: Mapped[float | None] = mapped_column(Float, nullable=True)

    canonical: Mapped[CanonicalEntity] = relationship(back_populates="aliases")

    __table_args__ = (
        Index("ix_aliases_norm", "surface_name_normalized"),
        Index("ix_aliases_source", "source_system", "source_id", unique=True),
    )


class CanonicalEdge(Base):
    """One relationship between two canonical entities. Always accompanied by 1+ citations."""

    __tablename__ = "canonical_edges"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    source_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("canonical_entities.id", ondelete="CASCADE"), nullable=False
    )
    target_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("canonical_entities.id", ondelete="CASCADE"), nullable=False
    )
    relation: Mapped[str] = mapped_column(String(32), nullable=False)
    weight: Mapped[float | None] = mapped_column(Float, nullable=True)
    # D2 (2026-08-05): per-edge structured attributes (value_band + numeric
    # band_low/band_high, income_type, income_band, account_group, eif, and
    # per-Part-N fields like position/org_type/year_incurred/rate/term).
    # Bands stored as bands + numeric derivations; never a false-precision
    # point value on top of a band.
    edge_metadata: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    # RG1 (2026-08-07): mirrors CanonicalEntity — default 'published' keeps
    # steady-state emitters' edges live; disclosure_emit (RG3) stamps
    # 'staged' + a batch_id.
    publication_state: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="published"
    )
    batch_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    projected_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    source: Mapped[CanonicalEntity] = relationship(
        back_populates="outgoing_edges", foreign_keys=[source_id]
    )
    target: Mapped[CanonicalEntity] = relationship(
        back_populates="incoming_edges", foreign_keys=[target_id]
    )
    citations: Mapped[list[SourceCitation]] = relationship(back_populates="edge")

    __table_args__ = (
        Index("ix_edges_source_relation", "source_id", "relation"),
        Index("ix_edges_target_relation", "target_id", "relation"),
        Index("ix_edges_unique", "source_id", "target_id", "relation", unique=True),
    )


class SourceCitation(Base):
    """A citation (URL/filing-id/permalink) supporting exactly one CanonicalEdge.

    An edge with zero citations must never surface in a public response — the check
    is enforced at the projection + API layers, not just at write time.
    """

    __tablename__ = "source_citations"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    edge_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("canonical_edges.id", ondelete="CASCADE"), nullable=False
    )
    kind: Mapped[str] = mapped_column(String(32), nullable=False)
    citation_url: Mapped[str] = mapped_column(Text, nullable=False)
    # E.g. FEC transaction ID, USAspending award ID, article permalink slug.
    citation_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    # D2 (2026-08-05) — OGE-278e specific: FK back to the ledger row that
    # produced this citation + the 1-based PDF page. Both nullable so
    # pre-existing FEC/USAspending/news citations don't need backfill.
    disclosure_row_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("disclosure_rows.id", ondelete="SET NULL"),
        nullable=True,
    )
    page: Mapped[int | None] = mapped_column(Integer, nullable=True)
    seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    edge: Mapped[CanonicalEdge] = relationship(back_populates="citations")

    __table_args__ = (
        Index("ix_citations_edge", "edge_id"),
        Index("ix_source_citations_disclosure_row_id", "disclosure_row_id"),
    )


class LlmUsage(Base):
    """Per-call LLM usage log — one row per Anthropic call (Atlas spend Part 1b).

    Written by every Anthropic caller in this app (scrutiny classifier +
    any future LLM-driven ingestor). Atlas's MCP + dashboard reads this
    table to compute per-app + per-feature spend, latency, and error rate.

    Cost is NOT stored here — it's computed downstream in the Atlas adapter
    against a per-model pricing config that lives on the Atlas side.
    Cache-read + cache-write tokens live on their own columns since they
    price at ~0.1x + ~1.25x the base input rate respectively (helen
    2026-07-18 Part 1a refinement).

    See migration 0003_llm_usage for the column shape + indexes.
    """

    __tablename__ = "llm_usage"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    app: Mapped[str] = mapped_column(String(64), nullable=False)
    feature: Mapped[str] = mapped_column(String(128), nullable=False)
    model: Mapped[str] = mapped_column(String(128), nullable=False)
    prompt_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    completion_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cache_read_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cache_write_tokens: Mapped[int | None] = mapped_column(Integer, nullable=True)
    call_ms: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ok: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    ts: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class AliasCrosswalk(Base):
    """P2 — one row per curated merge decision.

    ``from_id`` will be merged INTO ``to_id`` when the merge pass runs.
    ``applied_at`` is NULL for pending rows; set after re-pointing
    completes.

    Fail-closed on surface_mode (spec §3): the surviving canonical
    inherits the MOST-protected surface_mode across the pair. See
    migration 0005_alias_crosswalk for the column shape.
    """

    __tablename__ = "alias_crosswalk"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    from_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("canonical_entities.id", ondelete="SET NULL"),
        nullable=True,
    )
    to_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("canonical_entities.id", ondelete="SET NULL"),
        nullable=True,
    )
    # Frozen copies survive canonical delete — SET NULL removes the FKs
    # but the audit trail keeps its original references.
    from_id_frozen: Mapped[str | None] = mapped_column(
        String(36), nullable=True
    )
    to_id_frozen: Mapped[str | None] = mapped_column(
        String(36), nullable=True
    )
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    applied_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class AnchorRegistry(Base):
    """P4 — the shared anchor registry.

    Single source of truth for what the FEC / USAspending / LDA / SEC
    ingesters target. Replaces the 4 per-module hardcoded constants
    (``DETENTION_INDUSTRY_PACS`` etc.) with one row per curated anchor.

    Adding a domain (private-prison → prison-telecom → congress →
    Thiel/surveillance → Musk network) becomes a data edit + a re-run,
    not a code change. External-ID-first keying (fec_committee_ids,
    sec_cik, fec_candidate_ids) is the correctness argument — name
    matching gave us "AMERICA PAC"=FXAIX-fund + "Anduril"=concept in
    prior sweeps.

    See migration 0004_anchor_registry for the column shape + indexes.
    """

    __tablename__ = "anchor_registry"

    id: Mapped[str] = mapped_column(
        UUID(as_uuid=False),
        primary_key=True,
        server_default=func.gen_random_uuid(),
    )
    label: Mapped[str] = mapped_column(Text, nullable=False)
    entity_type: Mapped[str] = mapped_column(Text, nullable=False)
    priority_domain: Mapped[str | None] = mapped_column(Text, nullable=True)
    fec_committee_ids: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=func.jsonb_build_array()
    )
    fec_candidate_ids: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=func.jsonb_build_array()
    )
    sec_cik: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    usaspending_recipient_names: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=func.jsonb_build_array()
    )
    lda_client_names: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=func.jsonb_build_array()
    )
    name_variants: Mapped[list] = mapped_column(
        JSONB, nullable=False, server_default=func.jsonb_build_array()
    )
    # P1.6 (2026-08-21) — the external-ID keyring. One JSONB map rather
    # than a typed column per source, so anchoring a new domain against a
    # new authority is a data edit. Known keys:
    #   usaspending_uei      list[str]  — recipient UEI (the real key;
    #                                     ``usaspending_recipient_names``
    #                                     is only a fuzzy search string)
    #   lda_client_ids       list[int]  — Senate LDA client ids
    #   lda_registrant_ids   list[int]  — Senate LDA registrant ids
    #   sec_ciks             list[int]  — secondary issuer CIKs beyond
    #                                     ``sec_cik`` (e.g. subsidiaries)
    #   sec_owner_cik        int        — Form 3/4/5 reporting-owner CIK
    #                                     for a PERSON anchor
    # See migration 0010_anchor_external_ids.
    external_ids: Mapped[dict] = mapped_column(
        JSONB, nullable=False, server_default="{}"
    )
    surface_mode: Mapped[str] = mapped_column(
        Text, nullable=False, server_default="open"
    )
    canonical_id: Mapped[str | None] = mapped_column(
        String(36),
        ForeignKey("canonical_entities.id", ondelete="SET NULL"),
        nullable=True,
    )
    notes: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("label", "entity_type",
                         name="uq_anchor_registry_label_type"),
        Index("ix_anchor_registry_priority_domain", "priority_domain"),
        Index("ix_anchor_registry_entity_type", "entity_type"),
        Index("ix_anchor_registry_sec_cik", "sec_cik"),
    )


# ─── D1 financial-disclosure ingestion (see 0007 migration) ─────────


class DisclosureDocument(Base):
    """One archived OGE 278e / 278-T financial-disclosure PDF.

    Owns the audit lineage for its rows. The PDF bytes live at
    ``storage_path`` on the container volume; ``sha256`` is the
    citation contract's fingerprint (same value is quoted in the
    per-edge citation once D2 lands).
    """

    __tablename__ = "disclosure_documents"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    form_type: Mapped[str] = mapped_column(String(16), nullable=False)
    filer_name: Mapped[str] = mapped_column(Text, nullable=False)
    oge_url: Mapped[str] = mapped_column(Text, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    filed_date: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    period_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    period_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    page_count: Mapped[int] = mapped_column(Integer, nullable=False)
    storage_path: Mapped[str] = mapped_column(Text, nullable=False)
    bytes_len: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


class DisclosureRow(Base):
    """One parsed source line from a disclosure PDF — the audit ledger
    between the page and any downstream graph edge D2+ emits.

    HIGH rows carry a structured ``parsed`` payload; LOW rows carry
    only the raw text and a machine-readable ``reason``. Never
    coerced. The design's fail-closed contract is enforced by the
    parser side (see :mod:`app.services.disclosure_parser`); this
    table just stores what the parser said.
    """

    __tablename__ = "disclosure_rows"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    doc_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("disclosure_documents.id", ondelete="CASCADE"),
        nullable=False,
    )
    part: Mapped[str] = mapped_column(String(32), nullable=False)
    row_index: Mapped[int | None] = mapped_column(Integer, nullable=True)
    account_group: Mapped[int | None] = mapped_column(Integer, nullable=True)
    page: Mapped[int] = mapped_column(Integer, nullable=False)
    raw_text: Mapped[str] = mapped_column(Text, nullable=False)
    parsed: Mapped[dict] = mapped_column(JSONB, nullable=False, server_default="{}")
    parse_confidence: Mapped[str] = mapped_column(String(8), nullable=False)
    parse_method: Mapped[str] = mapped_column(String(16), nullable=False)
    reason: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )


# ─── P1 aircraft asset layer (2026-08-29) ────────────────────────
#
# Standalone PG-truth tables. Deliberately NOT wired to the graph:
# no ``EntityType`` member, no ``canonical_entities`` row, no Neo4j
# projection, no read-gate participation, no entity resolution. The
# FAA registrant name on an ``Aircraft`` row is a raw source string,
# NOT a resolved person — connecting it to a canonical is P2 and is
# Tony's call (helen decision doc, 2026-08-29).
#
# THE FENCE. ``MASTER.txt`` carries the home street address of every
# individual registrant — ~316k rows of live PII. The fence is
# therefore structural, not conventional: ``surface_mode`` and
# ``publication_state`` are pinned by CHECK constraints, so an
# UPDATE that tries to surface a row FAILS at the database. Opening
# the fence requires dropping a named constraint in a migration —
# a reviewable schema change, not a query someone can run by hand.


class AircraftSourceSnapshot(Base):
    """One download of the FAA Releasable Aircraft Database.

    The provenance anchor for every row this ingest writes — the
    aircraft analogue of :class:`DisclosureDocument`. P1 emits no
    edges and therefore no ``SourceCitation``; when P2 emits them,
    the citation quotes this snapshot's ``sha256``.

    The zip is NOT archived to disk (194 MB uncompressed of mostly
    PII we have no reason to keep a second copy of). The sha256 of
    the fetched bytes is what makes the claim checkable.
    """

    __tablename__ = "aircraft_source_snapshots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    bytes_len: Mapped[int] = mapped_column(BigInteger, nullable=False)
    #: ``Last-Modified`` as served by the FAA, when present — the
    #: registry's own statement of vintage, distinct from when we fetched it.
    source_last_modified: Mapped[str | None] = mapped_column(Text, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    #: Batch id stamped onto every row written from this snapshot.
    batch_id: Mapped[str] = mapped_column(String(64), nullable=False)
    master_rows: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    acftref_rows: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    __table_args__ = (
        UniqueConstraint("batch_id", name="uq_aircraft_snapshot_batch"),
        Index("ix_aircraft_snapshot_sha256", "sha256"),
    )


class AircraftReference(Base):
    """One ``ACFTREF.txt`` row — manufacturer/model reference data.

    Pure reference data keyed by the FAA ``CODE`` that
    ``Aircraft.mfr_mdl_code`` points at. Carries no personal data,
    so it takes the lifecycle gate (``publication_state``) but NOT
    ``surface_mode`` — a privacy mode on "Cessna / 172S" would be
    meaningless, and a column that is always the same value invites
    someone to read it as a privacy claim it is not making.
    """

    __tablename__ = "aircraft_reference"

    #: FAA ``CODE`` — the join key from ``Aircraft.mfr_mdl_code``.
    code: Mapped[str] = mapped_column(String(7), primary_key=True)
    mfr: Mapped[str | None] = mapped_column(Text, nullable=True)
    model: Mapped[str | None] = mapped_column(Text, nullable=True)
    type_acft: Mapped[str | None] = mapped_column(String(8), nullable=True)
    type_eng: Mapped[str | None] = mapped_column(String(8), nullable=True)
    ac_cat: Mapped[str | None] = mapped_column(String(8), nullable=True)
    build_cert_ind: Mapped[str | None] = mapped_column(String(8), nullable=True)
    no_eng: Mapped[int | None] = mapped_column(Integer, nullable=True)
    no_seats: Mapped[int | None] = mapped_column(Integer, nullable=True)
    ac_weight: Mapped[str | None] = mapped_column(String(16), nullable=True)
    speed: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tc_data_sheet: Mapped[str | None] = mapped_column(Text, nullable=True)
    tc_data_holder: Mapped[str | None] = mapped_column(Text, nullable=True)

    publication_state: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="staged"
    )
    snapshot_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("aircraft_source_snapshots.id", ondelete="SET NULL"), nullable=True
    )
    batch_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        CheckConstraint(
            "publication_state IN ('staged','published')",
            name="ck_aircraft_reference_publication_state_valid",
        ),
        Index("ix_aircraft_reference_mfr", "mfr"),
    )


class Aircraft(Base):
    """One ``MASTER.txt`` row — a single FAA aircraft registration.

    PII-BEARING. ``registrant_name`` / ``street`` / ``street2`` /
    ``city`` / ``state`` / ``zip_code`` are, for a ``type_registrant``
    of ``1`` (Individual), a private person's name and home address.
    Nothing in P1 reads this table on any public path; both gates are
    pinned closed by CHECK constraint (see the module comment above).

    Idempotency key is ``unique_id`` — the FAA's own stable per-
    registration id. ``n_number`` is unique in a given snapshot but
    is reassigned across deregistration, so it is indexed, not keyed.
    """

    __tablename__ = "aircraft"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)

    #: FAA ``UNIQUE ID`` — stable natural key, the upsert conflict target.
    unique_id: Mapped[str] = mapped_column(String(16), nullable=False)
    #: Registration mark WITHOUT the leading "N" (the file stores it bare).
    n_number: Mapped[str] = mapped_column(String(10), nullable=False)
    serial_number: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: Join key into :class:`AircraftReference`. No FK — MASTER rows
    #: reference codes absent from ACFTREF, and a dangling code is a
    #: fact about the source, not a reason to reject the row.
    mfr_mdl_code: Mapped[str | None] = mapped_column(String(7), nullable=True)
    eng_mfr_mdl: Mapped[str | None] = mapped_column(String(7), nullable=True)
    year_mfr: Mapped[int | None] = mapped_column(Integer, nullable=True)

    # ── registrant (PII) ──
    #: FAA code: 1=Individual 2=Partnership 3=Corporation 4=Co-Owned
    #: 5=Government 7=LLC 8=Non-Citizen Corp 9=Non-Citizen Co-Owned.
    type_registrant: Mapped[str | None] = mapped_column(String(2), nullable=True)
    registrant_name: Mapped[str | None] = mapped_column(Text, nullable=True)
    street: Mapped[str | None] = mapped_column(Text, nullable=True)
    street2: Mapped[str | None] = mapped_column(Text, nullable=True)
    city: Mapped[str | None] = mapped_column(Text, nullable=True)
    state: Mapped[str | None] = mapped_column(String(4), nullable=True)
    zip_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    region: Mapped[str | None] = mapped_column(String(4), nullable=True)
    county: Mapped[str | None] = mapped_column(String(8), nullable=True)
    country: Mapped[str | None] = mapped_column(String(4), nullable=True)
    #: ``OTHER NAMES(1..5)`` collapsed to an ordered list of the
    #: non-blank entries. Order is the source's, and is meaningful.
    other_names: Mapped[list] = mapped_column(JSONB, nullable=False, server_default="[]")

    # ── registration facts ──
    last_action_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    cert_issue_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    certification: Mapped[str | None] = mapped_column(Text, nullable=True)
    type_aircraft: Mapped[str | None] = mapped_column(String(4), nullable=True)
    type_engine: Mapped[str | None] = mapped_column(String(4), nullable=True)
    status_code: Mapped[str | None] = mapped_column(String(4), nullable=True)
    mode_s_code: Mapped[str | None] = mapped_column(String(16), nullable=True)
    mode_s_code_hex: Mapped[str | None] = mapped_column(String(16), nullable=True)
    #: ``FRACT OWNER`` — "Y" means fractional ownership; blank otherwise.
    fract_owner: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    air_worth_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    expiration_date: Mapped[date | None] = mapped_column(Date, nullable=True)
    kit_mfr: Mapped[str | None] = mapped_column(Text, nullable=True)
    kit_model: Mapped[str | None] = mapped_column(Text, nullable=True)

    # ── the fence ──
    surface_mode: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="suppress"
    )
    publication_state: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="staged"
    )

    # ── provenance ──
    snapshot_id: Mapped[str | None] = mapped_column(
        String(36), ForeignKey("aircraft_source_snapshots.id", ondelete="SET NULL"), nullable=True
    )
    batch_id: Mapped[str | None] = mapped_column(String(64), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("unique_id", name="uq_aircraft_unique_id"),
        # P3.0 (migration 0013): the P1 equality fence became a
        # VALIDITY fence. Promotion is now possible, but only through
        # the audited op — the column DEFAULTS are still suppress/staged,
        # so a row written by any existing path is born dark, and the
        # read-gate excludes anything not published.
        CheckConstraint(
            "surface_mode IN ('suppress','alias','open')",
            name="ck_aircraft_surface_mode_valid",
        ),
        CheckConstraint(
            "publication_state IN ('staged','published')",
            name="ck_aircraft_publication_state_valid",
        ),
        Index("ix_aircraft_n_number", "n_number"),
        Index("ix_aircraft_mfr_mdl_code", "mfr_mdl_code"),
        Index("ix_aircraft_batch_id", "batch_id"),
        # P2 entity resolution will scan by registrant name; cheap now.
        Index("ix_aircraft_registrant_name", "registrant_name"),
    )


class AircraftRegistrationEdge(Base):
    """P2 — a canonical entity REGISTERS an aircraft. Fenced + cited.

    NOT a ``CanonicalEdge``. A canonical edge needs both endpoints to
    be canonical entities, and P1/P2 deliberately do not make aircraft
    an ``EntityType`` — so a graph edge is not available without a
    schema change nobody has approved. This is the join-table form
    (design doc option (a)): the analytic value of "which canonicals
    own aircraft" without adding a node kind to the read gate, the
    Neo4j projection and the de-anon surface all at once.

    **Citation is structural.** ``snapshot_id`` / ``source_url`` /
    ``source_sha256`` are all NOT NULL, so an uncited edge cannot be
    written — the table-level analogue of the 0-uncited-edges
    invariant, enforced by the schema rather than by a sweep that has
    to find violations after the fact.

    Both gates are pinned closed exactly as on :class:`Aircraft`.
    Nothing here surfaces; publishing is P3 and is Tony's call.
    """

    __tablename__ = "aircraft_registration_edges"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    canonical_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("canonical_entities.id", ondelete="CASCADE"), nullable=False
    )
    aircraft_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("aircraft.id", ondelete="CASCADE"), nullable=False
    )
    relation: Mapped[str] = mapped_column(String(16), nullable=False, server_default="registers")

    # ── how this match was made (auditable, not just a score) ──
    match_tier: Mapped[str] = mapped_column(String(24), nullable=False)
    match_score: Mapped[float] = mapped_column(Float, nullable=False)
    #: The alias text that matched, when the tier is alias-based. NULL
    #: for a canonical-name match. P2 stages only the canonical tier,
    #: so this is NULL today — it exists so a later tier cannot be
    #: staged without recording which string produced it.
    matched_via: Mapped[str | None] = mapped_column(Text, nullable=True)
    #: The raw FAA registrant string this edge was derived from.
    registrant_name_raw: Mapped[str] = mapped_column(Text, nullable=False)

    # ── citation (all NOT NULL — an uncited edge is unrepresentable) ──
    snapshot_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("aircraft_source_snapshots.id", ondelete="RESTRICT"),
        nullable=False,
    )
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_sha256: Mapped[str] = mapped_column(String(64), nullable=False)

    # ── the fence ──
    surface_mode: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="suppress"
    )
    publication_state: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="staged"
    )

    batch_id: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("canonical_id", "aircraft_id", name="uq_aircraft_reg_edge_pair"),
        CheckConstraint("relation = 'registers'", name="ck_aircraft_reg_edge_relation"),
        CheckConstraint(
            "surface_mode IN ('suppress','alias','open')",
            name="ck_aircraft_reg_edge_surface_mode_valid",
        ),
        CheckConstraint(
            "publication_state IN ('staged','published')",
            name="ck_aircraft_reg_edge_publication_state_valid",
        ),
        # Belt to the NOT NULLs' braces: an empty-string citation would
        # satisfy NOT NULL while citing nothing.
        CheckConstraint(
            "length(source_url) > 0 and length(source_sha256) = 64",
            name="ck_aircraft_reg_edge_cited",
        ),
        Index("ix_aircraft_reg_edge_canonical", "canonical_id"),
        Index("ix_aircraft_reg_edge_aircraft", "aircraft_id"),
        Index("ix_aircraft_reg_edge_batch", "batch_id"),
    )


class AircraftPromotionAudit(Base):
    """P3.0 — one row per aircraft promotion or demotion.

    The mechanism that replaces P1/P2's equality fence. Promotion is
    per-row, attributed and reversible; this table is what makes a
    mistaken promotion diagnosable and undoable rather than merely
    overwritten.

    ``actor`` and ``reason`` are NOT NULL and CHECK-ed non-empty — an
    unattributed promotion is exactly what this exists to prevent.

    **Nothing in P3.0 writes to this table.** The op exists; no caller
    invokes it. P3.2 is the first phase that promotes anything, and only
    after Tony approves the surfacing list.
    """

    __tablename__ = "aircraft_promotion_audit"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    target_table: Mapped[str] = mapped_column(String(48), nullable=False)
    target_id: Mapped[str] = mapped_column(String(36), nullable=False)
    action: Mapped[str] = mapped_column(String(16), nullable=False)
    from_surface_mode: Mapped[str | None] = mapped_column(String(16), nullable=True)
    to_surface_mode: Mapped[str | None] = mapped_column(String(16), nullable=True)
    from_publication_state: Mapped[str | None] = mapped_column(String(16), nullable=True)
    to_publication_state: Mapped[str | None] = mapped_column(String(16), nullable=True)
    actor: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        CheckConstraint("action IN ('promote','demote')", name="ck_aircraft_promotion_action"),
        CheckConstraint(
            "length(actor) > 0 and length(reason) > 0",
            name="ck_aircraft_promotion_attributed",
        ),
        Index("ix_aircraft_promotion_target", "target_table", "target_id"),
        Index("ix_aircraft_promotion_created", "created_at"),
    )


class AircraftIndividualAllowlist(Base):
    """P3.4 — the curated exception to "no individual surfaces".

    P3.1 measured ~90% false positives on individual name matching, so
    individuals are blanket-held. This table is the narrow way one gets
    out: an explicit per-aircraft approval carrying its evidence.

    Keyed per ``(n_number, canonical_id)`` — approval is per AIRCRAFT,
    not per person, so approving one tail number cannot silently sweep
    in another that appears under the same name in a later FAA snapshot.

    ``evidence``/``source``/``added_by`` are NOT NULL and CHECK-ed
    non-empty, and an ``approved`` row must name its approver. The gate
    reads only ``status='approved'``; the table ships empty.
    """

    __tablename__ = "aircraft_individual_allowlist"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    n_number: Mapped[str] = mapped_column(String(10), nullable=False)
    registrant_name: Mapped[str] = mapped_column(Text, nullable=False)
    canonical_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("canonical_entities.id", ondelete="RESTRICT"), nullable=False
    )
    evidence: Mapped[str] = mapped_column(Text, nullable=False)
    source: Mapped[str] = mapped_column(Text, nullable=False)
    added_by: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(String(16), nullable=False, server_default="proposed")
    approved_by: Mapped[str | None] = mapped_column(Text, nullable=True)
    approved_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("n_number", "canonical_id", name="uq_aircraft_allowlist_pair"),
        CheckConstraint(
            "status IN ('proposed','approved','rejected')",
            name="ck_aircraft_allowlist_status",
        ),
        CheckConstraint(
            "length(evidence) > 0 and length(source) > 0 and length(added_by) > 0",
            name="ck_aircraft_allowlist_justified",
        ),
        CheckConstraint(
            "status <> 'approved' or (approved_by is not null and approved_at is not null)",
            name="ck_aircraft_allowlist_approval_attributed",
        ),
        Index("ix_aircraft_allowlist_status", "status"),
    )


# ─── Vessels P1 asset layer (2026-08-30) ─────────────────────────
#
# Second physical-asset class, mirroring the aircraft layer's isolation:
# standalone PG-truth tables, no EntityType member, no canonical row, no
# Neo4j projection, no read-path participation, no entity resolution.
# Owner strings are raw source text, not resolved people.


class VesselSourceSnapshot(Base):
    """One download of a vessel source (OFAC SDN or USCG documentation).

    The provenance anchor every vessel row cites. ``source`` is
    constrained so a third source cannot appear without a migration.
    """

    __tablename__ = "vessel_source_snapshots"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    bytes_len: Mapped[int] = mapped_column(BigInteger, nullable=False)
    source_last_modified: Mapped[str | None] = mapped_column(Text, nullable=True)
    fetched_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    batch_id: Mapped[str] = mapped_column(String(64), nullable=False)
    rows_ingested: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")

    __table_args__ = (
        UniqueConstraint("batch_id", name="uq_vessel_snapshot_batch"),
        CheckConstraint("source IN ('ofac_sdn','uscg_nvdc')", name="ck_vessel_snapshot_source"),
        Index("ix_vessel_snapshot_sha256", "sha256"),
    )


class Vessel(Base):
    """One vessel from a registry or sanctions list. Fenced + cited.

    PII-BEARING: ``owner_name_raw`` and the owner address columns are, for
    an individually-owned vessel, a private person's name and address.
    They are stored for future matching and are never surfaced — P1 has
    no read path at all, which is the strongest form of that guarantee.

    Natural key is ``(source, source_key)``, so USCG documentation slots
    in beside OFAC without a schema change.
    """

    __tablename__ = "vessels"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=_new_id)
    source: Mapped[str] = mapped_column(String(32), nullable=False)
    source_key: Mapped[str] = mapped_column(String(64), nullable=False)

    vessel_name: Mapped[str] = mapped_column(Text, nullable=False)
    imo_number: Mapped[str | None] = mapped_column(String(16), nullable=True)
    call_sign: Mapped[str | None] = mapped_column(String(32), nullable=True)
    flag: Mapped[str | None] = mapped_column(Text, nullable=True)
    vessel_type: Mapped[str | None] = mapped_column(Text, nullable=True)
    tonnage: Mapped[str | None] = mapped_column(String(32), nullable=True)
    gross_tonnage: Mapped[str | None] = mapped_column(String(32), nullable=True)
    hull_number: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # ── owner (PII) ──
    owner_name_raw: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner_street: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner_city: Mapped[str | None] = mapped_column(Text, nullable=True)
    owner_state: Mapped[str | None] = mapped_column(String(64), nullable=True)
    owner_postal_code: Mapped[str | None] = mapped_column(String(32), nullable=True)
    owner_country: Mapped[str | None] = mapped_column(String(64), nullable=True)

    # ── sanctions ──
    sanctions_program: Mapped[str | None] = mapped_column(Text, nullable=True)
    sanctions_remarks: Mapped[str | None] = mapped_column(Text, nullable=True)
    is_sanctioned: Mapped[bool] = mapped_column(Boolean, nullable=False, server_default="false")

    # ── the fence ──
    surface_mode: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="suppress"
    )
    publication_state: Mapped[str] = mapped_column(
        String(16), nullable=False, server_default="staged"
    )

    # ── structural citation ──
    snapshot_id: Mapped[str] = mapped_column(
        String(36), ForeignKey("vessel_source_snapshots.id", ondelete="RESTRICT"),
        nullable=False,
    )
    source_url: Mapped[str] = mapped_column(Text, nullable=False)
    source_sha256: Mapped[str] = mapped_column(String(64), nullable=False)
    batch_id: Mapped[str] = mapped_column(String(64), nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    __table_args__ = (
        UniqueConstraint("source", "source_key", name="uq_vessel_source_key"),
        CheckConstraint("source IN ('ofac_sdn','uscg_nvdc')", name="ck_vessel_source"),
        CheckConstraint("surface_mode = 'suppress'", name="ck_vessel_p1_suppress"),
        CheckConstraint("publication_state = 'staged'", name="ck_vessel_p1_staged"),
        CheckConstraint(
            "length(source_url) > 0 and length(source_sha256) = 64", name="ck_vessel_cited"
        ),
        CheckConstraint("length(vessel_name) > 0", name="ck_vessel_named"),
        Index("ix_vessel_name", "vessel_name"),
        Index("ix_vessel_imo", "imo_number"),
        Index("ix_vessel_owner_name", "owner_name_raw"),
        Index("ix_vessel_batch", "batch_id"),
        Index("ix_vessel_sanctioned", "is_sanctioned"),
    )
