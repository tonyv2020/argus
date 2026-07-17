"""P1 — Scrutiny Agent (design §5.4).

Before any real-person node or edge surfaces publicly, the Scrutiny Agent
classifies the person as PUBLIC or PRIVATE and applies a tiered bar (private =
VERY HIGH → suppress or aggregate; public = MEDIUM-HIGH → sourced connections
about their public role are fair game). Every decision is logged with a reason
into `scrutiny_decisions` — the record is auditable, mirrored on the Torres
citation-verifier discipline.

Design principle: "public figures are accountable; private individuals get
strong protection even when the data is public."

Hard signals classify public deterministically (no LLM cost/latency):
- named as an FEC committee principal or candidate;
- named as an LDA registrant;
- has an EntityAlias in an org-officer/corporate-registry source_system.

For borderline cases we use Anthropic Sonnet (model-floor per design §5.4) —
the LLM is asked to output a strict JSON verdict + reason. The classifier defaults
to `private` when a call fails, when the LLM returns an unparseable response, or
when no evidence is available (fail-closed = strongest protection).
"""

from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from uuid import uuid4

from sqlalchemy import DateTime, ForeignKey, Index, String, Text, func, select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from app.config import settings
from app.db import Base
from app.models import CanonicalEntity, EntityAlias, EntityType, SurfaceMode

logger = logging.getLogger(__name__)


def compute_public_alias(canonical_id: str) -> str:
    """Return a stable non-identifying label for a private-person canonical.

    Tony 2026-07-17: private people keep a REAL unique node with real edges
    (the graph must stay correct) but the public API returns this label rather
    than the actual name. Derived from the canonical id so it's DISTINCT per
    real person AND STABLE across runs.
    """
    short = canonical_id.replace("-", "")[:8]
    return f"Private donor #{short}"


class ScrutinyDecision(StrEnum):
    """Final surfacing decision — surfaced/aggregated/suppressed."""

    SURFACE = "surface"
    AGGREGATE = "aggregate"
    SUPPRESS = "suppress"


class ScrutinyClass(StrEnum):
    """Public-vs-private classification for the tiered bar."""

    PUBLIC = "public"
    PRIVATE = "private"
    UNKNOWN = "unknown"


class ScrutinyDecisionLog(Base):
    """Auditable record of every scrutiny decision — canonical + class + decision + reason.

    One row per (canonical_id, decision_at) — reruns append a new row rather than
    overwrite so a bar tightening later can be audited against past surfacings.
    """

    __tablename__ = "scrutiny_decisions"

    id: Mapped[str] = mapped_column(String(36), primary_key=True, default=lambda: str(uuid4()))
    canonical_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("canonical_entities.id", ondelete="CASCADE"),
        nullable=False,
    )
    classification: Mapped[str] = mapped_column(String(16), nullable=False)
    decision: Mapped[str] = mapped_column(String(16), nullable=False)
    signals_used: Mapped[str] = mapped_column(Text, nullable=False)
    reason: Mapped[str] = mapped_column(Text, nullable=False)
    decided_by: Mapped[str] = mapped_column(String(64), nullable=False)
    decided_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    __table_args__ = (Index("ix_scrutiny_canonical", "canonical_id"),)


@dataclass
class ScrutinyVerdict:
    """Output of one scrutiny classification — feeds ScrutinyDecisionLog + the API gate."""

    classification: ScrutinyClass
    decision: ScrutinyDecision
    signals_used: list[str]
    reason: str
    decided_by: str


_PUBLIC_SOURCE_SYSTEMS = {
    "fec.committee",
    "fec.candidate",
    "senate.lda.registrant",
    "corporate.registry.officer",
    "corporate.registry.exec",
}


async def _hard_signals(session: AsyncSession, canonical_id: str) -> list[str]:
    """Return the list of hard signals that classify a person as public (empty = borderline)."""
    aliases = (
        (await session.execute(select(EntityAlias).where(EntityAlias.canonical_id == canonical_id)))
        .scalars()
        .all()
    )
    signals = []
    for a in aliases:
        if a.source_system in _PUBLIC_SOURCE_SYSTEMS:
            signals.append(a.source_system)
    return signals


def _classify_from_llm(canonical_name: str, aliases: list[EntityAlias]) -> ScrutinyVerdict:
    """Call Anthropic Sonnet to classify a borderline case; fail-closed to PRIVATE + SUPPRESS.

    Only invoked when hard signals are absent. The LLM output MUST be a strict
    JSON object; any parse failure or network error falls back to the safe
    default (PRIVATE / SUPPRESS) — matches the design's "strong protection when
    uncertain" line.
    """
    api_key = settings.anthropic_api_key or os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        return ScrutinyVerdict(
            ScrutinyClass.PRIVATE,
            ScrutinyDecision.SUPPRESS,
            ["no_hard_signals", "no_anthropic_key"],
            "no Anthropic key configured; defaulting to strong protection",
            "scrutiny.fallback",
        )
    try:
        import anthropic

        client = anthropic.Anthropic(api_key=api_key)
        prompt = (
            "Classify this person as PUBLIC or PRIVATE for a cited-fact accountability "
            "graph. PUBLIC = elected official, candidate, registered lobbyist, corporate "
            "officer/exec, or otherwise notable public role. PRIVATE = everyone else, "
            "including small political donors and private citizens.\n\n"
            f"Name: {canonical_name}\n"
            f"Aliases: {', '.join(a.surface_name for a in aliases[:8])}\n\n"
            'Reply with STRICT JSON only: {"class": "public|private", "decision": '
            '"surface|aggregate|suppress", "reason": "<one-line>"}.'
        )
        msg = client.messages.create(
            model=settings.anthropic_model,
            max_tokens=200,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = msg.content[0].text if msg.content else ""
        # Sonnet sometimes wraps JSON in ```json ... ``` fences; strip them.
        stripped = raw.strip()
        if stripped.startswith("```"):
            # Drop first + last fence lines
            lines = stripped.splitlines()
            if len(lines) >= 2:
                stripped = "\n".join(lines[1:-1]).strip()
        # Also handle "prefix text\n{json}" — take from first "{" to last "}".
        if "{" in stripped and "}" in stripped:
            stripped = stripped[stripped.index("{") : stripped.rindex("}") + 1]
        data = json.loads(stripped)
        classification = ScrutinyClass(data["class"])
        decision = ScrutinyDecision(data["decision"])
        return ScrutinyVerdict(
            classification,
            decision,
            ["llm_borderline"],
            data.get("reason", ""),
            f"scrutiny.llm.{settings.anthropic_model}",
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning("scrutiny LLM call failed: %s", exc)
        return ScrutinyVerdict(
            ScrutinyClass.PRIVATE,
            ScrutinyDecision.SUPPRESS,
            ["no_hard_signals", "llm_failed"],
            f"LLM classification failed ({type(exc).__name__}); defaulting to strong protection",
            "scrutiny.fallback",
        )


async def scrutinize_person(session: AsyncSession, canonical_id: str) -> ScrutinyVerdict:
    """Classify + decide surfacing for a person canonical. Deterministic when possible.

    Only applies to `type=person` canonicals. Callers should short-circuit for
    non-person types (the gate exists to protect private individuals; other
    entity types are surfaced normally, subject to the citation gate).
    """
    ent = (
        await session.execute(select(CanonicalEntity).where(CanonicalEntity.id == canonical_id))
    ).scalar_one_or_none()
    if ent is None:
        raise ValueError(f"canonical entity {canonical_id} not found")
    if ent.type != EntityType.PERSON.value:
        return ScrutinyVerdict(
            ScrutinyClass.UNKNOWN,
            ScrutinyDecision.SURFACE,
            ["not_a_person"],
            "non-person canonical — the scrutiny gate applies only to real people",
            "scrutiny.bypass",
        )
    signals = await _hard_signals(session, canonical_id)
    if signals:
        return ScrutinyVerdict(
            ScrutinyClass.PUBLIC,
            ScrutinyDecision.SURFACE,
            signals,
            f"hard signals classify public: {', '.join(sorted(set(signals)))}",
            "scrutiny.hard_signals",
        )
    aliases = (
        (await session.execute(select(EntityAlias).where(EntityAlias.canonical_id == canonical_id)))
        .scalars()
        .all()
    )
    return _classify_from_llm(ent.canonical_name, aliases)


async def scrutinize_and_log(session: AsyncSession, canonical_id: str) -> ScrutinyVerdict:
    """Run the scrutiny verdict + persist an audit row + update the canonical's surface fields.

    Tony 2026-07-17: private people keep their unique canonical (real edges); we
    ONLY toggle `surface_mode` + set a stable `public_alias`. The graph stays
    correct; the public API renders the alias in place of the real name.
    """
    verdict = await scrutinize_person(session, canonical_id)
    session.add(
        ScrutinyDecisionLog(
            canonical_id=canonical_id,
            classification=verdict.classification.value,
            decision=verdict.decision.value,
            signals_used=json.dumps(verdict.signals_used),
            reason=verdict.reason,
            decided_by=verdict.decided_by,
        )
    )
    # Update the canonical's surface fields to reflect the verdict — the row
    # itself (id, edges, embedding, real-name FK) is unchanged.
    ent = (
        await session.execute(select(CanonicalEntity).where(CanonicalEntity.id == canonical_id))
    ).scalar_one_or_none()
    if ent is not None and ent.type == EntityType.PERSON.value:
        # Helen 2026-07-17 canonical mapping:
        #   public + surface  → OPEN (real name shown)  = public_named
        #   private + surface → ALIAS (public_alias)    = private_anonymized+alias
        #   private + aggregate → ALIAS                 = private_anonymized+alias
        #   any + suppress    → SUPPRESS                = elided
        if verdict.decision == ScrutinyDecision.SUPPRESS:
            ent.surface_mode = SurfaceMode.SUPPRESS.value
            ent.public_alias = None
        elif verdict.classification == ScrutinyClass.PUBLIC:
            ent.surface_mode = SurfaceMode.OPEN.value
            ent.public_alias = None
        else:
            ent.surface_mode = SurfaceMode.ALIAS.value
            ent.public_alias = compute_public_alias(canonical_id)
        session.add(ent)
    return verdict


def _now() -> datetime:
    """Timezone-aware UTC now — used only for the log row's Python-side default."""
    return datetime.now(UTC)
