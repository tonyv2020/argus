"""ORM models for Argus — canonical entity registry + relationship edges + citations.

Every edge carries a **SourceCitation** — a relationship is not shown without one.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import uuid4

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger,
    Boolean,
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
