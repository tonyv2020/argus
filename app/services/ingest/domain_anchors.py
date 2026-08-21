"""P1.6 — anchor a priority domain on EXTERNAL IDS, never on names.

The P4 seed put the surveillance / tech-influence stubs in
``anchor_registry`` (Palantir, Axon, Flock Safety, Clearview AI,
Founders Fund, Peter Thiel) but every one of them carried only a label
plus a handful of fuzzy search strings, and none resolved to a
canonical. Meanwhile the graph held the domain incidentally and badly
fragmented — ``AXON``/``Axon Enterprise``/``AXON ENTERPRISES``,
``Flock``/``Flock Safety``/``FLOCK GROUP INC D/B/A FLOCK SAFETY``,
``Anduril`` typed ``concept``.

This pass makes the domain first-class. For each declared anchor it:

1. Resolves ONE canonical, keyed in priority order on the anchor's
   **authoritative external ids** — SEC issuer CIK, SEC Form 3/4/5
   reporting-owner CIK, USAspending recipient UEI, Senate LDA client id,
   FEC committee / candidate id — falling back to a **fail-closed** exact
   name match and, only then, to a fresh canonical.
2. Attaches every external id as an :class:`~app.models.EntityAlias`, so
   the downstream source ingesters (``usaspending``, ``senate_lda``,
   ``sec_insiders``, ``fec_individual``) resolve their rows onto the same
   node instead of minting a parallel one.
3. Registers the anchor's name variants in the ``domain.anchor``
   namespace, which is what P1.6.3's fragment collapse keys on.
4. Syncs the ``anchor_registry`` row — including the new
   ``external_ids`` keyring (migration 0010) — and points
   ``canonical_id`` at the resolved node.

It emits **no edges**: an anchor is an identity, and every P1.6 edge is
cited to the source that produced it (USAspending award, LDA filing, SEC
ownership filing, FEC transaction). Nothing here can create an uncited
edge because it creates no edge at all.

Read-gate (RG1)
---------------
``batch_id`` stamps every **net-new** canonical ``publication_state=
staged`` + ``batch_id``. Pre-existing published canonicals the pass
resolves ONTO are never restamped — re-staging a live node would pull
it off the public read path.

Privacy — FAIL-CLOSED
---------------------
* A net-new anchor canonical is created with the anchor's declared
  ``surface_mode`` (``open`` for issuers, agencies and public figures).
* The pass **never rewrites an existing canonical's surface_mode**, in
  either direction. An anchor that lands on a ``suppress``/``alias``
  node is reported in ``non_open_anchors`` for an operator decision.
* The exact-name fallback refuses any node that is protected, is the
  wrong type, or already carries a *foreign* authoritative id — the
  three ways a name match attaches an anchor identity to the wrong real
  entity.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Protocol

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import (
    CanonicalEntity,
    EntityAlias,
    EntityType,
    PublicationState,
    SurfaceMode,
)
from app.services.anchor_registry import upsert_anchor
from app.services.graph.base import normalize_name

logger = logging.getLogger(__name__)

#: Alias namespace for an anchor's declared name variants. Deliberately
#: distinct from the identity namespaces below so a variant string can
#: never be mistaken for an authoritative id.
VARIANT_NAMESPACE = "domain.anchor"

#: SEC Form 3/4/5 **reporting-owner** CIK. Distinct from ``sec.cik``
#: (the ISSUER namespace, which pins ``organization``): a reporting
#: owner is usually a natural person, and conflating the two would
#: retype Peter Thiel as an organization in the dedup pass.
SEC_OWNER_NAMESPACE = "sec.owner_cik"

#: USAspending recipient UEI — the real key behind an award row.
USASPENDING_UEI_NAMESPACE = "usaspending.uei"

#: Authoritative id namespaces. A node already carrying one of these for
#: a DIFFERENT entity must never absorb an anchor identity on a name
#: match. Mirrors ``congress_roster._AUTHORITATIVE_NAMESPACES`` and adds
#: the two P1.6 namespaces.
AUTHORITATIVE_NAMESPACES = frozenset(
    {
        "bioguide",
        "fec.candidate",
        "fec.committee",
        "fec.affiliated_committee",
        "sec.cik",
        SEC_OWNER_NAMESPACE,
        USASPENDING_UEI_NAMESPACE,
        "senate_lda.registrant",
        "senate_lda.client",
    }
)


# ─── declaration ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class AnchorSpec:
    """One declared anchor — label, type, and its external-ID keyring.

    Everything here is DATA. Adding an anchor (or a whole new domain) is
    an edit to :data:`DOMAIN_SPECS`, not a code change, which is the
    same contract ``anchor_registry`` was built for.
    """

    label: str
    entity_type: str
    priority_domain: str
    #: SEC issuer CIK (the primary one; secondaries go in ``sec_ciks``).
    sec_cik: int | None = None
    sec_ciks: tuple[int, ...] = ()
    #: SEC Form 3/4/5 reporting-owner CIK — PERSON anchors.
    sec_owner_cik: int | None = None
    usaspending_uei: tuple[str, ...] = ()
    lda_client_ids: tuple[int, ...] = ()
    lda_registrant_ids: tuple[int, ...] = ()
    fec_committee_ids: tuple[str, ...] = ()
    fec_candidate_ids: tuple[str, ...] = ()
    #: Fuzzy search strings the older name-keyed ingesters still use.
    #: For LDA these are the SERVER-SIDE QUERY strings; what is actually
    #: accepted is decided by ``lda_client_patterns`` below.
    usaspending_recipient_names: tuple[str, ...] = ()
    lda_client_names: tuple[str, ...] = ()
    #: Regexes matched against the NORMALIZED LDA client name. LDA mints
    #: a new client id per REGISTRATION, so a fixed id allowlist goes
    #: stale the moment the company hires another firm — see
    #: ``Anchor.lda_client_patterns``. Anchored (``^…$``) wherever a
    #: bare prefix would over-match: "flock" alone claims FLOCK HOMES,
    #: "axon" alone claims AXONIUS and AXONICS.
    lda_client_patterns: tuple[str, ...] = ()
    #: Surface forms news + filings actually print. Used for the
    #: fail-closed name fallback and by the P1.6.3 fragment collapse.
    name_variants: tuple[str, ...] = ()
    surface_mode: str = SurfaceMode.OPEN.value
    notes: str | None = None

    @property
    def external_ids(self) -> dict:
        """The keyring as it is stored in ``anchor_registry.external_ids``."""
        out: dict = {}
        if self.usaspending_uei:
            out["usaspending_uei"] = list(self.usaspending_uei)
        if self.lda_client_ids:
            out["lda_client_ids"] = list(self.lda_client_ids)
        if self.lda_registrant_ids:
            out["lda_registrant_ids"] = list(self.lda_registrant_ids)
        if self.sec_ciks:
            out["sec_ciks"] = list(self.sec_ciks)
        if self.sec_owner_cik is not None:
            out["sec_owner_cik"] = int(self.sec_owner_cik)
        if self.lda_client_patterns:
            out["lda_client_patterns"] = list(self.lda_client_patterns)
        return out

    @property
    def identity_keys(self) -> list[tuple[str, str]]:
        """``(source_system, source_id)`` pairs, most-authoritative first.

        Resolution walks this list in order: the first key already in the
        graph wins. SEC CIKs come first because they are assigned by a
        federal registrar and never recycled; UEI next (SAM.gov
        registration); LDA client ids last, because LDA lets one real
        company register several client records.
        """
        keys: list[tuple[str, str]] = []
        if self.sec_owner_cik is not None:
            keys.append((SEC_OWNER_NAMESPACE, str(self.sec_owner_cik).zfill(10)))
        for cik in ((self.sec_cik,) if self.sec_cik is not None else ()) + self.sec_ciks:
            keys.append(("sec.cik", str(cik).zfill(10)))
        for uei in self.usaspending_uei:
            keys.append((USASPENDING_UEI_NAMESPACE, uei.strip().upper()))
        for cid in self.fec_committee_ids:
            keys.append(("fec.committee", cid.strip().upper()))
        for cid in self.fec_candidate_ids:
            keys.append(("fec.candidate", cid.strip().upper()))
        for lid in self.lda_client_ids:
            keys.append(("senate_lda.client", str(lid)))
        # Dedupe, preserving order.
        seen: set[tuple[str, str]] = set()
        out: list[tuple[str, str]] = []
        for k in keys:
            if k not in seen:
                seen.add(k)
                out.append(k)
        return out


# ─── the P1.6 domain ────────────────────────────────────────────────────
#
# Every id below was verified against the issuing authority on
# 2026-08-21 (SEC EDGAR submissions API; USAspending /api/v2/recipient/;
# FEC /v1/committees/). The ``notes`` field records what the id IS, so a
# later operator can re-verify without re-deriving it.

SURVEILLANCE_ANCHORS: tuple[AnchorSpec, ...] = (
    AnchorSpec(
        label="Peter Thiel",
        entity_type=EntityType.PERSON.value,
        priority_domain="surveillance",
        # SEC Form 3/4/5 reporting-owner CIK — "THIEL PETER", the filer
        # behind his Palantir director filings. This is the external id
        # that makes the Thiel→Palantir edge a filing fact, not a claim.
        sec_owner_cik=1211060,
        name_variants=(
            "Peter Thiel",
            "Peter A. Thiel",
            "Peter Andreas Thiel",
            "THIEL PETER",
            "THIEL, PETER",
        ),
        surface_mode=SurfaceMode.OPEN.value,
        notes=(
            "Public figure. SEC reporting-owner CIK 1211060. Personal "
            "political giving lands via the FEC individual-contributor "
            "mode (fec_individual)."
        ),
    ),
    AnchorSpec(
        label="Palantir Technologies",
        entity_type=EntityType.ORGANIZATION.value,
        priority_domain="surveillance",
        sec_cik=1321655,
        # Both UEIs are returned by USAspending's own recipient search
        # for FSY4LVSBGWB7 — Palantir USG Inc is the federal-contracting
        # arm and its awards belong on the Palantir node.
        usaspending_uei=("FSY4LVSBGWB7", "HNN4F9JZWDY8"),
        usaspending_recipient_names=(
            "PALANTIR TECHNOLOGIES INC",
            "PALANTIR USG INC",
            "PALANTIR TECHNOLOGIES",
        ),
        lda_client_names=("Palantir",),
        lda_client_patterns=(
            r"^palantir( technologies)?$",
            # Filers also register a wrapper string naming the firm that
            # acts for Palantir ("… OBO PALANTIR TECHNOLOGIES INC.").
            r"\b(obo|for) palantir technologies$",
        ),
        name_variants=(
            "Palantir Technologies Inc.",
            "Palantir Technologies",
            "Palantir",
            "Palantir Technologies Inc",
            "PALANTIR USG INC",
        ),
        notes="SEC CIK 1321655 (PLTR). USAspending UEI FSY4LVSBGWB7 + HNN4F9JZWDY8.",
    ),
    AnchorSpec(
        label="Axon Enterprise",
        entity_type=EntityType.ORGANIZATION.value,
        priority_domain="surveillance",
        sec_cik=1069183,
        usaspending_uei=("TBW7MGPYURM7",),
        usaspending_recipient_names=("AXON ENTERPRISE INC", "TASER INTERNATIONAL INC"),
        lda_client_names=("Axon",),
        # Anchored: a bare "axon" prefix claims AXONIUS, AXONICS
        # MODULATION TECHNOLOGIES and AXON HOLDINGS GROUP, all of which
        # are different companies with their own LDA registrations.
        lda_client_patterns=(r"^axon enterprises?$",),
        name_variants=(
            "Axon Enterprise, Inc.",
            "Axon Enterprise",
            "Axon",
            "AXON",
            "AXON ENTERPRISES",
            "TASER International",
        ),
        notes="SEC CIK 1069183 (AXON, formerly TASER International). UEI TBW7MGPYURM7.",
    ),
    AnchorSpec(
        label="Flock Safety",
        entity_type=EntityType.ORGANIZATION.value,
        priority_domain="surveillance",
        # Privately held — no CIK. USAspending UEI is the authoritative key.
        usaspending_uei=("QDLLBKCGL851",),
        usaspending_recipient_names=("FLOCK GROUP INC", "FLOCK SAFETY"),
        lda_client_names=("Flock",),
        # Anchored: "flock" alone claims FLOCK HOMES, INC.
        lda_client_patterns=(
            r"^flock safety$",
            r"^flock group( inc)? d b a flock safety$",
            r"\bon behalf of flock safety$",
        ),
        name_variants=(
            "Flock Safety",
            "Flock Group Inc",
            "FLOCK GROUP INC D/B/A FLOCK SAFETY",
            "Flock",
        ),
        notes="Private (no CIK). USAspending UEI QDLLBKCGL851 = FLOCK GROUP INC.",
    ),
    AnchorSpec(
        label="Clearview AI",
        entity_type=EntityType.ORGANIZATION.value,
        priority_domain="surveillance",
        usaspending_uei=("E5BEYXDQYJS6",),
        usaspending_recipient_names=("CLEARVIEW AI INC",),
        lda_client_names=("Clearview",),
        lda_client_patterns=(r"^clearview ai$",),
        name_variants=("Clearview AI", "Clearview AI, Inc.", "Clearview"),
        notes="Private (no CIK). USAspending UEI E5BEYXDQYJS6 = CLEARVIEW AI, INC.",
    ),
    AnchorSpec(
        label="Founders Fund",
        entity_type=EntityType.ORGANIZATION.value,
        priority_domain="surveillance",
        # Founders Fund is a fund FAMILY: EDGAR carries 40+ separate CIKs,
        # one per fund LP + its management LLC. The management companies
        # are the durable entity; the vintage funds come and go. Only the
        # management CIKs are declared, so the anchor keys on the entity
        # that persists across vintages.
        sec_cik=1695329,  # Founders Fund II Management, LLC
        sec_ciks=(
            1852758,  # Founders Fund III Management, LLC
            1616081,  # Founders Fund IV Management, LLC
            1697733,  # Founders Fund V Management, LLC
            1853431,  # Founders Fund Growth Management, LLC
            2106825,  # Founders Fund Growth II Management, LP
        ),
        usaspending_recipient_names=(),
        lda_client_names=("Founders Fund",),
        lda_client_patterns=(r"^founders fund$",),
        name_variants=("Founders Fund", "Founders Fund LLC", "FOUNDERS FUND"),
        notes=(
            "Thiel VC vehicle. No single CIK — EDGAR registers one per fund "
            "vintage; the management-company CIKs are declared here."
        ),
    ),
)

#: Every domain this pass can materialise. P1.7 adds ``musk_network``
#: here and reuses the whole pass unchanged.
DOMAIN_SPECS: dict[str, tuple[AnchorSpec, ...]] = {
    "surveillance": SURVEILLANCE_ANCHORS,
}


# ─── fail-closed name fallback ──────────────────────────────────────────


@dataclass(frozen=True)
class NameMatchCandidate:
    """An existing canonical the exact-name fallback is considering."""

    id: str
    name: str
    type: str
    surface_mode: str
    publication_state: str
    #: Authoritative id namespaces already on the node.
    namespaces: frozenset[str] = frozenset()


def anchor_name_match_allowed(
    cand: NameMatchCandidate, spec: AnchorSpec
) -> tuple[bool, str]:
    """May this anchor identity be attached to ``cand`` on a name match?

    FAIL-CLOSED — a name match is the weakest evidence we accept:

    * ``surface_mode`` must be ``open`` AND must equal the anchor's own
      declared mode. Attaching an anchor identity to a protected node
      either mislabels a protected entity or relaxes its protection on
      the way to publication.
    * The type must be the anchor's type, or ``unknown``: 32% of the
      registry is typed ``unknown`` and those are exactly the fragments
      worth claiming. Anything else names a different kind of thing.
      ``concept`` is accepted for an ORGANIZATION anchor because the
      news-tag pipeline routinely types a company as a concept — the
      documented ``organization``/``concept`` mis-typing the P2 dedup
      pass already merges.
    * The node must carry NO authoritative external id. Unlike the
      roster's fallback (which tolerates the member's own bioguide), an
      anchor's own ids were already tried in the id-keyed step above, so
      any id left on a name-matched node is by definition foreign.
    """
    if cand.surface_mode != SurfaceMode.OPEN.value:
        return False, "surface_mode_not_open"
    if cand.surface_mode != spec.surface_mode:
        return False, "surface_mode_straddle"
    allowed_types = {spec.entity_type, EntityType.UNKNOWN.value}
    if spec.entity_type == EntityType.ORGANIZATION.value:
        allowed_types.add(EntityType.CONCEPT.value)
    if cand.type not in allowed_types:
        return False, f"type_mismatch:{cand.type}"
    if cand.namespaces:
        return False, f"foreign_external_id:{sorted(cand.namespaces)[0]}"
    return True, "ok"


# ─── stats ──────────────────────────────────────────────────────────────


class AliasConflictSink(Protocol):
    """Anything that can record an alias-ownership conflict.

    :func:`attach_alias` is shared by every P1.6 pass and each keeps its
    own stats dataclass; all they must agree on is this one list.
    """

    alias_conflicts: list[dict]


@dataclass
class AnchorPassStats:
    """Counters for one domain-anchor pass."""

    anchors_declared: int = 0
    anchors_upserted: int = 0
    resolved_by_external_id: int = 0
    resolved_by_name: int = 0
    created_fresh: int = 0
    entities_staged: int = 0
    identity_aliases_created: int = 0
    variant_aliases_created: int = 0
    #: Which external id resolved each anchor — the report's provenance.
    resolution: list[dict] = field(default_factory=list)
    #: Name-match candidates the fail-closed guard refused, by reason.
    name_match_refused: dict[str, int] = field(default_factory=dict)
    #: Anchors sitting on a non-``open`` canonical — operator review.
    non_open_anchors: list[dict] = field(default_factory=list)
    #: Identity aliases that could NOT be attached because another
    #: canonical already owns that (source_system, source_id).
    alias_conflicts: list[dict] = field(default_factory=list)
    errors: int = 0


# ─── DB helpers ─────────────────────────────────────────────────────────


async def _canonical_for_alias(
    session: AsyncSession, source_system: str, source_id: str
) -> str | None:
    """Canonical id carrying ``(source_system, source_id)``, else None."""
    return (
        await session.execute(
            select(EntityAlias.canonical_id).where(
                EntityAlias.source_system == source_system,
                EntityAlias.source_id == source_id,
            )
        )
    ).scalar_one_or_none()


async def _name_candidates(
    session: AsyncSession, norm: str
) -> list[NameMatchCandidate]:
    """Every canonical whose normalized name equals ``norm``, decorated
    with the authoritative id namespaces it already carries."""
    rows = (
        await session.execute(
            select(CanonicalEntity).where(
                CanonicalEntity.canonical_name_normalized == norm
            )
        )
    ).scalars().all()
    out: list[NameMatchCandidate] = []
    for ent in rows:
        namespaces = (
            await session.execute(
                select(EntityAlias.source_system).where(
                    EntityAlias.canonical_id == ent.id,
                    EntityAlias.source_system.in_(sorted(AUTHORITATIVE_NAMESPACES)),
                )
            )
        ).scalars().all()
        out.append(
            NameMatchCandidate(
                id=ent.id,
                name=ent.canonical_name,
                type=ent.type,
                surface_mode=ent.surface_mode,
                publication_state=ent.publication_state,
                namespaces=frozenset(namespaces),
            )
        )
    return out


async def attach_alias(
    session: AsyncSession,
    canonical_id: str,
    source_system: str,
    source_id: str,
    surface_name: str,
    *,
    kind_hint: str | None = None,
    stats: AliasConflictSink | None = None,
    label: str = "",
) -> bool:
    """Idempotent alias attach. True when a row was created.

    ``(source_system, source_id)`` is UNIQUE across the whole registry.
    If another canonical already owns the pair, this does NOT steal it —
    it records the conflict for the report. Silently re-pointing an
    external id is how two real entities become one.
    """
    existing = (
        await session.execute(
            select(EntityAlias).where(
                EntityAlias.source_system == source_system,
                EntityAlias.source_id == source_id,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        if existing.canonical_id != canonical_id and stats is not None:
            stats.alias_conflicts.append(
                {
                    "anchor": label,
                    "source_system": source_system,
                    "source_id": source_id,
                    "owned_by": existing.canonical_id,
                    "wanted_by": canonical_id,
                }
            )
        return False
    norm = normalize_name(surface_name)
    session.add(
        EntityAlias(
            canonical_id=canonical_id,
            source_system=source_system,
            source_id=source_id,
            surface_name=surface_name,
            surface_name_normalized=norm or surface_name.lower(),
            kind_hint=kind_hint,
        )
    )
    return True


def variant_alias_source_id(spec: AnchorSpec, variant: str) -> str:
    """Stable ``entity_aliases.source_id`` for one anchor name variant.

    Must be deterministic across re-runs (idempotency) and namespaced by
    the anchor label, since two anchors may share a variant. Truncated to
    the column's 64 chars.
    """
    slug = normalize_name(variant).replace(" ", "-")
    return f"{normalize_name(spec.label).replace(' ', '-')}:{slug}"[:64]


# ─── resolution ─────────────────────────────────────────────────────────


async def resolve_anchor_canonical(
    session: AsyncSession,
    spec: AnchorSpec,
    *,
    batch_id: str | None,
    stats: AnchorPassStats,
) -> tuple[str, str, str]:
    """Return ``(canonical_id, how, detail)`` for one anchor.

    Priority: any authoritative external id already in the graph →
    fail-closed exact-name fallback over the anchor's declared variants →
    create fresh (staged).
    """
    # 1. Authoritative external ids, most-authoritative first.
    for source_system, source_id in spec.identity_keys:
        canonical_id = await _canonical_for_alias(session, source_system, source_id)
        if canonical_id:
            stats.resolved_by_external_id += 1
            return canonical_id, "external_id", f"{source_system}:{source_id}"

    # 2. Fail-closed exact-name fallback. The label is tried first, then
    #    the declared variants, so the most canonical form wins ties.
    for surface in (spec.label, *spec.name_variants):
        norm = normalize_name(surface)
        if not norm:
            continue
        for cand in await _name_candidates(session, norm):
            ok, reason = anchor_name_match_allowed(cand, spec)
            if ok:
                stats.resolved_by_name += 1
                return cand.id, "name", norm
            stats.name_match_refused[reason] = (
                stats.name_match_refused.get(reason, 0) + 1
            )

    # 3. Fresh canonical.
    norm = normalize_name(spec.label)
    ent = CanonicalEntity(
        canonical_name=spec.label,
        canonical_name_normalized=norm or spec.label.lower(),
        type=spec.entity_type,
        surface_mode=spec.surface_mode,
        publication_state=(
            PublicationState.STAGED.value if batch_id
            else PublicationState.PUBLISHED.value
        ),
        batch_id=batch_id,
    )
    session.add(ent)
    await session.flush()
    stats.created_fresh += 1
    if batch_id:
        stats.entities_staged += 1
    return ent.id, "created", ""


# ─── the pass ───────────────────────────────────────────────────────────


async def materialise_domain(
    domain: str,
    *,
    batch_id: str | None = None,
    specs: tuple[AnchorSpec, ...] | None = None,
) -> AnchorPassStats:
    """Resolve + register every anchor in ``domain``.

    Idempotent + re-runnable: identity is keyed on external ids and every
    alias write dedupes first, so a second run resolves the same nodes
    and creates nothing.
    """
    if specs is None:
        specs = DOMAIN_SPECS.get(domain, ())
        if not specs:
            raise ValueError(
                f"unknown domain {domain!r}; choose from {sorted(DOMAIN_SPECS)}"
            )
    stats = AnchorPassStats(anchors_declared=len(specs))
    sm = get_sessionmaker()
    async with sm() as session:
        for spec in specs:
            try:
                canonical_id, how, detail = await resolve_anchor_canonical(
                    session, spec, batch_id=batch_id, stats=stats
                )

                # ── identity aliases (the whole point of the pass) ───
                for source_system, source_id in spec.identity_keys:
                    if await attach_alias(
                        session,
                        canonical_id,
                        source_system,
                        source_id,
                        spec.label,
                        kind_hint=spec.entity_type,
                        stats=stats,
                        label=spec.label,
                    ):
                        stats.identity_aliases_created += 1

                # ── name variants ────────────────────────────────────
                for variant in spec.name_variants:
                    if await attach_alias(
                        session,
                        canonical_id,
                        VARIANT_NAMESPACE,
                        variant_alias_source_id(spec, variant),
                        variant,
                        kind_hint=spec.entity_type,
                        stats=stats,
                        label=spec.label,
                    ):
                        stats.variant_aliases_created += 1

                ent = (
                    await session.execute(
                        select(CanonicalEntity).where(
                            CanonicalEntity.id == canonical_id
                        )
                    )
                ).scalar_one()

                # ── privacy report (never rewritten here) ────────────
                if ent.surface_mode != SurfaceMode.OPEN.value:
                    stats.non_open_anchors.append(
                        {
                            "anchor": spec.label,
                            "canonical_id": ent.id,
                            "name": ent.canonical_name,
                            "surface_mode": ent.surface_mode,
                        }
                    )

                await upsert_anchor(
                    session,
                    label=spec.label,
                    entity_type=spec.entity_type,
                    priority_domain=spec.priority_domain,
                    fec_committee_ids=spec.fec_committee_ids,
                    fec_candidate_ids=spec.fec_candidate_ids,
                    sec_cik=spec.sec_cik,
                    usaspending_recipient_names=spec.usaspending_recipient_names,
                    lda_client_names=spec.lda_client_names,
                    name_variants=spec.name_variants,
                    external_ids=spec.external_ids,
                    surface_mode=spec.surface_mode,
                    canonical_id=canonical_id,
                    notes=spec.notes,
                )
                stats.anchors_upserted += 1
                stats.resolution.append(
                    {
                        "anchor": spec.label,
                        "canonical_id": canonical_id,
                        "canonical_name": ent.canonical_name,
                        "type": ent.type,
                        "surface_mode": ent.surface_mode,
                        "publication_state": ent.publication_state,
                        "resolved_by": how,
                        "detail": detail,
                    }
                )
            except Exception:
                logger.exception("anchor upsert failed for %s", spec.label)
                stats.errors += 1
        await session.commit()
    return stats


def main() -> None:
    """CLI — ``python -m app.services.ingest.domain_anchors``."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    ap = argparse.ArgumentParser(
        description="Materialise a priority domain's anchors on external ids"
    )
    ap.add_argument(
        "--domain",
        default="surveillance",
        choices=sorted(DOMAIN_SPECS),
        help="which declared domain to materialise",
    )
    ap.add_argument(
        "--batch-id",
        default=None,
        help="RG1 read-gate batch. Net-new entities are stamped "
             "publication_state=staged with this tag (dark until an "
             "operator publishes the batch).",
    )
    ap.add_argument("--json-report", default=None)
    args = ap.parse_args()

    stats = asyncio.run(
        materialise_domain(args.domain, batch_id=args.batch_id)
    )
    report = dict(stats.__dict__)
    report["domain"] = args.domain
    report["batch_id"] = args.batch_id
    print(json.dumps(report, indent=2, default=str))
    if args.json_report:
        with open(args.json_report, "w") as fh:
            json.dump(report, fh, indent=2, default=str)


if __name__ == "__main__":
    main()
