"""Congressional roster ingester — one canonical PERSON per current member.

P4 D built the first version (registry rows + bioguide/fec.candidate
aliases). **P1.5 (helen 2026-08-21)** promotes it to the foundational
roster pass: members become first-class canonical people so FEC money
and roll-call votes attribute to the *person*, not to a campaign
committee string.

Source of truth: ``@unitedstates/congress-legislators``
(``legislators-current.yaml``, public domain) — the authoritative
crosswalk of **bioguide id** + **FEC candidate ids** + party / state /
chamber / district / terms / name variants.

What one pass does
------------------
1. Resolves each current member to ONE canonical, keyed in priority
   order on EXTERNAL IDS: ``bioguide`` → ``fec.candidate`` → a
   fail-closed exact-name fallback (:func:`name_match_allowed`).
2. Attaches the identity aliases (``bioguide:<id>``,
   ``fec.candidate:<id>``) plus the common **name variants**
   (``congress.legislators`` namespace) that news + FEC + roll-call
   data actually surface, so later ingests resolve to the member.
3. Emits a cited ``held_position`` edge member → chamber
   (``United States Senate`` / ``United States House of
   Representatives``), cited to the member's bioguide page + the
   dataset — the roster's own receipt, keeping the 0-uncited
   invariant.
4. Keeps the anchor-registry row in sync (P4 behaviour, unchanged).

Read-gate (RG1)
---------------
``batch_id`` threads through the whole pass. When it is set, every
**net-new** canonical + **every edge this pass creates** is stamped
``publication_state=staged`` + ``batch_id`` — dark on the public read
path until an operator publishes the batch. ``batch_id=None`` (the
default, and what the weekly ``scheduled_sweep`` uses) keeps the
column default ``published``, so steady-state behaviour is unchanged.

Privacy — FAIL-CLOSED
---------------------
* Members are public officials → a **net-new** member canonical is
  created ``surface_mode='open'``.
* The pass **never rewrites an existing canonical's surface_mode**. A
  member whose canonical is already ``suppress``/``alias`` is left
  exactly as it is and reported in ``non_open_members`` for an
  operator to review — flipping a live protection open is not an
  ingester's decision.
* The exact-name fallback refuses to attach a member identity to a
  non-``open`` node (that is how a private person acquires a
  member's identity). It also refuses any node already carrying a
  *foreign* authoritative id.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from dataclasses import dataclass, field
from typing import Any

import httpx
import yaml
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
from app.services.anchor_registry import upsert_anchor
from app.services.graph.base import normalize_name

logger = logging.getLogger(__name__)

_ROSTER_URL = (
    "https://raw.githubusercontent.com/unitedstates/"
    "congress-legislators/main/legislators-current.yaml"
)
#: Human-facing landing page for the dataset — the citation_url on every
#: roster edge points at the member's bioguide page, this is the
#: citation_ref provenance tag.
DATASET_URL = "https://github.com/unitedstates/congress-legislators"
_BIOGUIDE_URL = "https://bioguide.congress.gov/search/bio/{bioguide}"

#: Alias namespace for the member's common name variants (First Last,
#: nickname, LAST, FIRST …). Distinct from the identity namespaces so a
#: name variant can never be mistaken for an authoritative id.
VARIANT_NAMESPACE = "congress.legislators"

#: Authoritative id namespaces. A node already carrying one of these for
#: a DIFFERENT entity must never absorb a member identity via a name
#: match — that is a different real person/organisation.
_AUTHORITATIVE_NAMESPACES = frozenset(
    {
        "bioguide",
        "fec.candidate",
        "fec.committee",
        "fec.affiliated_committee",
        "sec.cik",
        "senate_lda.registrant",
    }
)

#: Chamber canonical names — matched on normalized name + organization
#: type, created only when absent.
CHAMBER_ENTITIES = {
    "sen": "United States Senate",
    "rep": "United States House of Representatives",
}


# ─── pure parsing (hermetically testable) ───────────────────────────────


@dataclass(frozen=True)
class MemberRecord:
    """One current member, flattened out of legislators-current.yaml."""

    bioguide: str
    label: str
    chamber: str  # 'sen' | 'rep'
    state: str
    party: str
    district: int | None
    term_start: str
    term_end: str
    fec_candidate_ids: tuple[str, ...]
    name_variants: tuple[str, ...]

    @property
    def bioguide_url(self) -> str:
        """Citation URL — the member's Biographical Directory entry."""
        return _BIOGUIDE_URL.format(bioguide=self.bioguide)

    @property
    def notes(self) -> str:
        """Compact chamber/state/party string for the P5 flow filters."""
        # District belongs to the STATE token ("state=NY-14"). P4 D appended
        # it to `party` ("party=Democrat-14"), which reads as a party name.
        district_str = f"-{self.district}" if self.district is not None else ""
        return (
            f"chamber={self.chamber} state={self.state}{district_str} "
            f"party={self.party} bioguide={self.bioguide}"
        )


@dataclass
class RosterStats:
    """Counters for one roster sweep."""

    members_fetched: int = 0
    members_upserted: int = 0
    house_members: int = 0
    senate_members: int = 0
    fec_candidate_ids_attached: int = 0
    person_canonicals_created: int = 0
    bioguide_aliases_created: int = 0
    fec_candidate_aliases_created: int = 0
    # P5.2 — party attribution: one alias per member with
    # source_system='party' + surface_name='Democratic'/'Republican'/…
    party_aliases_created: int = 0
    # ── P1.5 ────────────────────────────────────────────────────────
    #: Members resolved by an authoritative id already in the graph.
    resolved_by_bioguide: int = 0
    resolved_by_fec_candidate: int = 0
    #: Members resolved onto a pre-existing (news) fragment by name.
    resolved_by_name_fragment: int = 0
    #: Members with no prior node at all → fresh canonical.
    created_fresh: int = 0
    #: Name-match candidates the fail-closed guard refused, by reason.
    name_match_refused: dict[str, int] = field(default_factory=dict)
    name_variant_aliases_created: int = 0
    chamber_edges_created: int = 0
    chamber_edges_reused: int = 0
    citations_created: int = 0
    entities_staged: int = 0
    edges_staged: int = 0
    #: Members whose canonical is NOT surface_mode='open' — left
    #: untouched, reported for operator review.
    non_open_members: list[dict[str, str]] = field(default_factory=list)
    errors: int = 0


async def _fetch_roster() -> list[dict[str, Any]]:
    """One HTTP GET to the legislators-current.yaml. Returns parsed list."""
    async with httpx.AsyncClient(timeout=60.0) as client:
        r = await client.get(_ROSTER_URL, follow_redirects=True)
        r.raise_for_status()
    return yaml.safe_load(r.text)


def _extract_current_term(member: dict[str, Any]) -> dict[str, Any] | None:
    """Return the most-recent-start term (the active one for a
    current-legislators row).  Sorted lexicographically on ``start``,
    which is ISO YYYY-MM-DD → stable ordering."""
    terms = member.get("terms") or []
    if not terms:
        return None
    return sorted(terms, key=lambda t: t.get("start", ""))[-1]


def _label_for(member: dict[str, Any]) -> str:
    """Human display label — ``official_full`` from name block, else
    ``first last``."""
    name = member.get("name") or {}
    return (
        name.get("official_full")
        or f"{name.get('first', '').strip()} {name.get('last', '').strip()}".strip()
    )


def _name_variants(member: dict[str, Any]) -> list[str]:
    """Every surface name we'd want to alias-search against — FEC
    candidate names use LAST, FIRST; news uses `first last`."""
    name = member.get("name") or {}
    variants: list[str] = []
    for k in ("official_full", "first", "last", "middle",
              "nickname", "suffix"):
        v = name.get(k)
        if v:
            variants.append(str(v).strip())
    # LAST, FIRST — the FEC candidate-name shape.
    if name.get("first") and name.get("last"):
        variants.append(f"{name['last'].strip()}, {name['first'].strip()}")
    # First Last — the news shape.
    if name.get("first") and name.get("last"):
        variants.append(f"{name['first'].strip()} {name['last'].strip()}")
    # Nickname Last — "Bernie Sanders" for Bernard Sanders, the shape
    # news actually prints. This is the single highest-yield variant for
    # collapsing news fragments onto the member.
    if name.get("nickname") and name.get("last"):
        variants.append(f"{name['nickname'].strip()} {name['last'].strip()}")
    if name.get("first") and name.get("middle") and name.get("last"):
        variants.append(
            f"{name['first'].strip()} {name['middle'].strip()} {name['last'].strip()}"
        )
    for other in member.get("other_names") or []:
        if other.get("last"):
            first = (other.get("first") or name.get("first") or "").strip()
            variants.append(f"{first} {other['last'].strip()}".strip())
    # Dedupe preserving order.
    seen: set[str] = set()
    out: list[str] = []
    for v in variants:
        if v and v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _fec_candidate_ids(member: dict[str, Any]) -> list[str]:
    """All fec.candidate ids in the ``id`` block (may be a list)."""
    id_block = member.get("id") or {}
    ids = id_block.get("fec") or []
    if isinstance(ids, str):
        return [ids]
    return list(ids)


def parse_member(raw: dict[str, Any]) -> MemberRecord | None:
    """Flatten one legislators-current.yaml row into a
    :class:`MemberRecord`. Returns None when the row can't be keyed
    (no active term, no bioguide id, or no display label) — a member
    with no authoritative id is never invented."""
    term = _extract_current_term(raw)
    if not term:
        return None
    bioguide = ((raw.get("id") or {}).get("bioguide") or "").strip()
    label = _label_for(raw)
    if not bioguide or not label:
        return None
    district = term.get("district")
    return MemberRecord(
        bioguide=bioguide,
        label=label,
        chamber=(term.get("type") or "").lower(),
        state=term.get("state") or "",
        party=term.get("party") or "",
        district=int(district) if district is not None else None,
        term_start=term.get("start") or "",
        term_end=term.get("end") or "",
        fec_candidate_ids=tuple(_fec_candidate_ids(raw)),
        name_variants=tuple(_name_variants(raw)),
    )


def variant_alias_source_id(bioguide: str, variant: str) -> str:
    """Stable ``entity_aliases.source_id`` for one name variant.

    ``(source_system, source_id)`` is UNIQUE, so the key must be
    deterministic across re-runs (idempotency) AND namespaced by
    bioguide (two members may share a variant like "Smith"). Truncated
    to the column's 64 chars.
    """
    slug = normalize_name(variant).replace(" ", "-")
    return f"{bioguide}:{slug}"[:64]


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
    #: The bioguide ids already on the node (empty for a news fragment).
    bioguides: frozenset[str] = frozenset()


def name_match_allowed(
    cand: NameMatchCandidate, member_bioguide: str
) -> tuple[bool, str]:
    """May this member identity be attached to ``cand`` on a name match?

    FAIL-CLOSED. A name match is the weakest evidence we accept, so it
    only passes for an unclaimed, already-public person node:

    * ``type`` must be ``person`` — a member is not an organization.
    * ``surface_mode`` must be ``open``. Attaching a member's bioguide
      to a ``suppress``/``alias`` node either mislabels a protected
      private person as a member, or silently relaxes protection on the
      way to publication. Same non-negotiable rule as the P2 dedup
      pass; the pair is skipped and logged instead.
    * The node must carry no *foreign* authoritative id — a different
      bioguide, an FEC candidate/committee id, a SEC CIK. Those name a
      different real entity.
    """
    if cand.type != EntityType.PERSON.value:
        return False, "type_not_person"
    if cand.surface_mode != SurfaceMode.OPEN.value:
        return False, "surface_mode_not_open"
    if cand.bioguides and cand.bioguides != frozenset({member_bioguide}):
        return False, "foreign_bioguide"
    foreign = cand.namespaces & (_AUTHORITATIVE_NAMESPACES - {"bioguide"})
    if foreign:
        return False, f"foreign_external_id:{sorted(foreign)[0]}"
    return True, "ok"


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
        aliases = (
            await session.execute(
                select(EntityAlias.source_system, EntityAlias.source_id).where(
                    EntityAlias.canonical_id == ent.id,
                    EntityAlias.source_system.in_(sorted(_AUTHORITATIVE_NAMESPACES)),
                )
            )
        ).all()
        out.append(
            NameMatchCandidate(
                id=ent.id,
                name=ent.canonical_name,
                type=ent.type,
                surface_mode=ent.surface_mode,
                publication_state=ent.publication_state,
                namespaces=frozenset(a[0] for a in aliases),
                bioguides=frozenset(
                    a[1] for a in aliases if a[0] == "bioguide"
                ),
            )
        )
    return out


async def _attach_alias(
    session: AsyncSession,
    canonical_id: str,
    source_system: str,
    source_id: str,
    surface_name: str,
    kind_hint: str | None = None,
) -> bool:
    """Idempotent alias attach. True when a row was created."""
    existing = (
        await session.execute(
            select(EntityAlias).where(
                EntityAlias.source_system == source_system,
                EntityAlias.source_id == source_id,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
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


async def _resolve_member_canonical(
    session: AsyncSession,
    rec: MemberRecord,
    *,
    batch_id: str | None,
    stats: RosterStats,
) -> tuple[str, str]:
    """Return ``(canonical_id, how)`` for one member.

    Priority: ``bioguide`` alias → any ``fec.candidate`` alias →
    fail-closed exact-name fallback → create fresh. ``how`` is one of
    ``bioguide`` / ``fec.candidate`` / ``name_fragment`` / ``created``.
    """
    # 1. Authoritative: bioguide.
    canonical_id = await _canonical_for_alias(session, "bioguide", rec.bioguide)
    if canonical_id:
        stats.resolved_by_bioguide += 1
        return canonical_id, "bioguide"

    # 2. Authoritative: any FEC candidate id already in the graph.
    for fec_id in rec.fec_candidate_ids:
        canonical_id = await _canonical_for_alias(session, "fec.candidate", fec_id)
        if canonical_id:
            stats.resolved_by_fec_candidate += 1
            return canonical_id, "fec.candidate"

    # 3. Fail-closed exact-name fallback onto an existing news fragment.
    norm = normalize_name(rec.label)
    if norm:
        for cand in await _name_candidates(session, norm):
            ok, reason = name_match_allowed(cand, rec.bioguide)
            if ok:
                stats.resolved_by_name_fragment += 1
                return cand.id, "name_fragment"
            stats.name_match_refused[reason] = (
                stats.name_match_refused.get(reason, 0) + 1
            )

    # 4. Fresh canonical — public official, so surface_mode=open.
    ent = CanonicalEntity(
        canonical_name=rec.label,
        canonical_name_normalized=norm or rec.label.lower(),
        type=EntityType.PERSON.value,
        surface_mode=SurfaceMode.OPEN.value,
        publication_state=(
            PublicationState.STAGED.value if batch_id
            else PublicationState.PUBLISHED.value
        ),
        batch_id=batch_id,
    )
    session.add(ent)
    await session.flush()
    stats.created_fresh += 1
    stats.person_canonicals_created += 1
    if batch_id:
        stats.entities_staged += 1
    return ent.id, "created"


async def _chamber_canonical(
    session: AsyncSession,
    chamber: str,
    *,
    batch_id: str | None,
    stats: RosterStats,
) -> str | None:
    """Canonical id for the member's chamber — reuse the existing
    organization node, create it (staged) only when absent."""
    label = CHAMBER_ENTITIES.get(chamber)
    if not label:
        return None
    norm = normalize_name(label)
    existing = (
        await session.execute(
            select(CanonicalEntity).where(
                CanonicalEntity.canonical_name_normalized == norm,
                CanonicalEntity.type == EntityType.ORGANIZATION.value,
            )
        )
    ).scalars().first()
    if existing is not None:
        return existing.id
    ent = CanonicalEntity(
        canonical_name=label,
        canonical_name_normalized=norm,
        type=EntityType.ORGANIZATION.value,
        surface_mode=SurfaceMode.OPEN.value,
        publication_state=(
            PublicationState.STAGED.value if batch_id
            else PublicationState.PUBLISHED.value
        ),
        batch_id=batch_id,
    )
    session.add(ent)
    await session.flush()
    if batch_id:
        stats.entities_staged += 1
    return ent.id


async def _emit_held_position(
    session: AsyncSession,
    member_id: str,
    chamber_id: str,
    rec: MemberRecord,
    *,
    batch_id: str | None,
    stats: RosterStats,
) -> None:
    """Cited ``held_position`` edge member → chamber. Idempotent on
    (source, target, relation); the citation is created with the edge so
    the 0-uncited invariant never has a window to break."""
    existing = (
        await session.execute(
            select(CanonicalEdge).where(
                CanonicalEdge.source_id == member_id,
                CanonicalEdge.target_id == chamber_id,
                CanonicalEdge.relation == EdgeRelation.HELD_POSITION.value,
            )
        )
    ).scalar_one_or_none()
    if existing is not None:
        stats.chamber_edges_reused += 1
        return
    edge = CanonicalEdge(
        source_id=member_id,
        target_id=chamber_id,
        relation=EdgeRelation.HELD_POSITION.value,
        weight=1.0,
        edge_metadata={
            "chamber": rec.chamber,
            "state": rec.state,
            "party": rec.party,
            "district": rec.district,
            "term_start": rec.term_start,
            "term_end": rec.term_end,
            "bioguide": rec.bioguide,
            "source": "unitedstates/congress-legislators",
        },
        publication_state=(
            PublicationState.STAGED.value if batch_id
            else PublicationState.PUBLISHED.value
        ),
        batch_id=batch_id,
    )
    session.add(edge)
    await session.flush()
    session.add(
        SourceCitation(
            edge_id=edge.id,
            kind=SourceKind.CONGRESS_ROSTER.value,
            citation_ref=f"legislators-current.yaml bioguide={rec.bioguide}",
            citation_url=rec.bioguide_url,
        )
    )
    stats.chamber_edges_created += 1
    stats.citations_created += 1
    if batch_id:
        stats.edges_staged += 1


# ─── the pass ───────────────────────────────────────────────────────────


async def ingest_roster(
    batch_id: str | None = None,
    *,
    emit_chamber_edges: bool = True,
    roster: list[dict[str, Any]] | None = None,
) -> RosterStats:
    """Fetch legislators-current.yaml + materialise every current member.

    Idempotent + re-runnable: identity is keyed on external ids, every
    alias/edge write dedupes first. ``batch_id`` (RG1) stamps net-new
    entities + every edge this pass creates as ``staged``; the default
    ``None`` keeps the published column default for the weekly sweep.
    """
    stats = RosterStats()
    if roster is None:
        try:
            roster = await _fetch_roster()
        except Exception:
            logger.exception("failed to fetch legislators-current.yaml")
            stats.errors = 1
            return stats

    stats.members_fetched = len(roster)
    sm = get_sessionmaker()
    async with sm() as session:
        for raw in roster:
            rec = parse_member(raw)
            if rec is None:
                continue
            try:
                canonical_id, how = await _resolve_member_canonical(
                    session, rec, batch_id=batch_id, stats=stats
                )

                # ── identity aliases ────────────────────────────────
                if await _attach_alias(
                    session, canonical_id, "bioguide", rec.bioguide,
                    rec.label, kind_hint="person",
                ):
                    stats.bioguide_aliases_created += 1
                for fec_id in rec.fec_candidate_ids:
                    if await _attach_alias(
                        session, canonical_id, "fec.candidate", fec_id,
                        rec.label, kind_hint="person",
                    ):
                        stats.fec_candidate_aliases_created += 1
                if rec.fec_candidate_ids:
                    stats.fec_candidate_ids_attached += 1

                # ── name variants ───────────────────────────────────
                for variant in rec.name_variants:
                    if await _attach_alias(
                        session,
                        canonical_id,
                        VARIANT_NAMESPACE,
                        variant_alias_source_id(rec.bioguide, variant),
                        variant,
                        kind_hint="person",
                    ):
                        stats.name_variant_aliases_created += 1

                # ── party (P5.2) ────────────────────────────────────
                if rec.party:
                    existing = (
                        await session.execute(
                            select(EntityAlias).where(
                                EntityAlias.canonical_id == canonical_id,
                                EntityAlias.source_system == "party",
                                EntityAlias.source_id == rec.bioguide,
                            )
                        )
                    ).scalar_one_or_none()
                    if existing is None:
                        session.add(
                            EntityAlias(
                                canonical_id=canonical_id,
                                source_system="party",
                                source_id=rec.bioguide,
                                surface_name=rec.party,
                                surface_name_normalized=(
                                    normalize_name(rec.party) or rec.party.lower()
                                ),
                            )
                        )
                        stats.party_aliases_created += 1

                # ── chamber edge ────────────────────────────────────
                if emit_chamber_edges:
                    chamber_id = await _chamber_canonical(
                        session, rec.chamber, batch_id=batch_id, stats=stats
                    )
                    if chamber_id:
                        await _emit_held_position(
                            session, canonical_id, chamber_id, rec,
                            batch_id=batch_id, stats=stats,
                        )

                # ── privacy report (never rewritten here) ───────────
                ent = (
                    await session.execute(
                        select(CanonicalEntity).where(
                            CanonicalEntity.id == canonical_id
                        )
                    )
                ).scalar_one()
                if ent.surface_mode != SurfaceMode.OPEN.value:
                    stats.non_open_members.append(
                        {
                            "canonical_id": ent.id,
                            "name": ent.canonical_name,
                            "surface_mode": ent.surface_mode,
                            "bioguide": rec.bioguide,
                            "chamber": rec.chamber,
                            "state": rec.state,
                        }
                    )

                await upsert_anchor(
                    session,
                    label=rec.label,
                    entity_type="person",
                    priority_domain="congress",
                    fec_candidate_ids=rec.fec_candidate_ids,
                    name_variants=rec.name_variants,
                    surface_mode="open",
                    canonical_id=canonical_id,
                    notes=rec.notes,
                )
                stats.members_upserted += 1
                if rec.chamber == "sen":
                    stats.senate_members += 1
                else:
                    stats.house_members += 1
            except Exception:
                logger.exception("roster upsert failed for %s", rec.label)
                stats.errors += 1
        await session.commit()
    return stats


def main() -> None:
    """CLI entry — ``python -m app.services.ingest.congress_roster``."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    ap = argparse.ArgumentParser(description="Argus Congressional roster ingester")
    ap.add_argument(
        "--batch-id",
        default=None,
        help="RG1 read-gate batch. Net-new entities + every edge this pass "
             "creates are stamped publication_state=staged with this tag "
             "(dark until an operator publishes the batch).",
    )
    ap.add_argument(
        "--no-chamber-edges",
        action="store_true",
        help="Skip the cited held_position member → chamber edges.",
    )
    ap.add_argument("--json-report", default=None)
    args = ap.parse_args()

    stats = asyncio.run(
        ingest_roster(
            batch_id=args.batch_id,
            emit_chamber_edges=not args.no_chamber_edges,
        )
    )
    logger.info("congress roster ingest done: %s", stats)
    report = {
        k: v for k, v in stats.__dict__.items() if k != "non_open_members"
    }
    report["non_open_member_count"] = len(stats.non_open_members)
    report["non_open_members"] = stats.non_open_members
    print(json.dumps(report, indent=2, default=str))
    if args.json_report:
        with open(args.json_report, "w") as fh:
            json.dump(report, fh, indent=2, default=str)


if __name__ == "__main__":
    main()
