"""ORM models for Argus — canonical entity registry + relationship edges + citations.

Every edge carries a **SourceCitation** — a relationship is not shown without one.
"""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from uuid import uuid4

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    func,
)
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


class SourceKind(StrEnum):
    """Where a SourceCitation points — used by the UI to render the click-through label."""

    ARTICLE_PERMALINK = "article_permalink"
    FEC_FILING = "fec_filing"
    USASPENDING_AWARD = "usaspending_award"
    SENATE_LDA = "senate_lda"
    CORPORATE_REGISTRY = "corporate_registry"


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
    seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    edge: Mapped[CanonicalEdge] = relationship(back_populates="citations")

    __table_args__ = (Index("ix_citations_edge", "edge_id"),)


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
