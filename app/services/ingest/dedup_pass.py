"""P2 — idempotent fragmentation dedup/merge pass (P2.1 crosswalk + P2.2 merge).

The corpus fragments the same real-world entity across several canonicals:
``Tesla`` exists as ``person`` AND ``unknown`` AND ``organization``;
``SpaceX`` as ``unknown`` + ``organization``; ``xAI`` as ``organization`` +
``concept``; ``Starlink`` as ``unknown`` + ``concept`` + ``organization``.
32% of the registry is typed ``unknown``. This pass resolves fragments to ONE
canonical per real-world entity, re-points every edge + citation + alias +
anchor + scrutiny decision onto the survivor, and deletes the emptied node.

RUN IT READ-ONLY FIRST.  ``--dry-run`` is the default; it sets
``default_transaction_read_only`` on the session so an accidental write
raises instead of touching live public data, and emits a full JSON + text
report.  ``--apply`` is the only mode that writes.

CANDIDATE RULES (P2.1 crosswalk), in priority order
---------------------------------------------------
Applied by default:

``ext_id``
    Two canonicals share an AUTHORITATIVE external identity: SEC CIK
    (``sec.cik``), FEC committee (``fec.committee`` /
    ``fec.disbursement.recipient``), FEC candidate (``fec.candidate``),
    bioguide (``bioguide``), or the same ``anchor_registry`` row.
    ``fec.affiliated_committee`` is DELIBERATELY EXCLUDED — that alias is a
    *sponsorship pointer*, not an identity: committee ``C00142711`` is
    carried both by the org ``Boeing`` and by ``THE BOEING COMPANY PAC``,
    and ``C00027466`` links ``YOUNG VICTORY COMMITTEE`` to ``NRSC``.
    Treating it as identity would collapse orgs into their PACs and destroy
    the contribution attribution flow_model1 depends on.

``sec_former_name``
    A standalone canonical whose normalized name equals a SEC *former name*
    of a CIK-owning canonical (``WACKENHUT CORRECTIONS CORP`` → GEO Group,
    ``TESLA MOTORS, INC.`` → Tesla, ``CORRECTIONS CORP OF AMERICA`` →
    CoreCivic).

``exact_name``
    Identical ``canonical_name_normalized`` and a COMPATIBLE type pair
    (see ``_name_merge_compatible``).

``squashed_name``
    Identical whitespace-stripped normalized name — catches ``Space X`` vs
    ``SpaceX`` and ``PALANTIRTECHNOLOGIES INC`` vs
    ``Palantir Technologies Inc.``.

``prefix_variant_curated``
    The hand-reviewed subset of ``prefix_variant`` pinned in
    ``_PREFIX_CURATED`` — currently the two Palantir OCR mangles
    (``PALANTIR TECHNOLOGIES IN``, ``… INCLASS``), approved by helen
    2026-08-21. It can never match anything not in that list.

Review-only (reported, NEVER applied without an explicit flag):

``person_name``      person↔person / person↔unknown name matches. A false
                     MERGE of two real people is far worse than a false
                     split (cf. ``resolution_person_conservative_margin``).
                     ``--rules …,person_unknown`` opts the person↔unknown
                     half in; person↔person can never be opted in.
``pac_cross_type``   ``pac`` vs non-``pac`` name matches. ``normalize_name``
                     strips the trailing ``pac`` suffix, so ``AMERICA PAC``
                     normalizes onto the *place* ``America`` and
                     ``FACEBOOK INC. PAC`` onto the org ``Facebook``.
``incompatible_type``other type pairs we refuse to guess on
                     (``organization``↔``place``, ``organization``↔``person``…).
``prefix_variant``   OCR/truncation variants at large. Only the curated
                     subset above is ever applied.
``vector``           pgvector cosine ≥ ``--vector-threshold`` plus a shared
                     name token. ``--enable-vector`` opts in. helen
                     2026-08-21: OFF — it finds real duplicates but also
                     ``Series A``/``Series A-1`` and ``NHL playoffs``/
                     ``Stanley Cup playoffs``.

HARD PRIVACY RULE — fail-closed, non-negotiable
-----------------------------------------------
NEVER merge across ``surface_mode``.  A candidate pair whose members differ
in ``surface_mode`` is SKIPPED and logged to the review list — no exception,
no flag to override.  (``Tesla``/person is ``suppress`` while
``Tesla``/organization is ``open``: the org↔unknown merge proceeds, the
person node is left alone.)  The same fail-closed partition is applied to
``publication_state`` so a merge can never silently publish staged content.

TYPE INFERENCE
--------------
A survivor's type is upgraded ONLY on a reliable signal, never from the
name (see ``_resolve_type``): an authoritative external id on a cluster
member (SEC CIK → ``organization``, FEC committee → ``pac``, bioguide/FEC
candidate → ``person``); or exactly ONE real type among the members, which
the survivor inherits — that is the ``unknown`` → typed upgrade; or the
documented ``{organization, concept}`` case (a company the news-tag
pipeline filed as a concept: xAI, Starlink, OpenAI), which
``--no-concept-to-org`` disables. Any OTHER disagreement is a real
conflict: the survivor keeps its own type and the cluster is flagged.

CLI::

    python -m app.services.ingest.dedup_pass --dry-run
    python -m app.services.ingest.dedup_pass --dry-run --json-report /tmp/r.json
    python -m app.services.ingest.dedup_pass --apply      # destructive
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db import get_sessionmaker
from app.models import AliasCrosswalk
from app.services.graph.base import normalize_name
from app.services.ingest.merge_canonicals import merge_two_canonicals

logger = logging.getLogger(__name__)


# ─── identity namespaces ────────────────────────────────────────────────
#
# Maps an ``entity_aliases.source_system`` to the identity namespace its
# ``source_id`` lives in. Two canonicals carrying the same (namespace, id)
# ARE the same real-world entity.
#
# NOT listed (deliberately): ``fec.affiliated_committee`` (sponsorship
# pointer, not identity — see module docstring), ``fec.contributor``,
# ``party``, ``senate_lda.*``, ``usaspending.*``, ``hollywood.entity_tags``
# (all name-derived, not authoritative ids).
_IDENTITY_NAMESPACE: dict[str, str] = {
    "sec.cik": "sec_cik",
    "fec.committee": "fec_committee",
    "fec.disbursement.recipient": "fec_committee",
    "fec.candidate": "fec_candidate",
    "bioguide": "bioguide",
    "congress.bill": "congress_bill",
}

# An authoritative id in this namespace pins the entity's type. Used for
# the "NEVER guess" type-inference rule.
_NAMESPACE_TYPE: dict[str, str] = {
    "sec_cik": "organization",
    "fec_committee": "pac",
    "fec_candidate": "person",
    "bioguide": "person",
    "congress_bill": "bill",
}

# Higher wins when resolving a merged cluster's surviving type. ``unknown``
# is 0 so it can never beat a real type.
_TYPE_AUTHORITY: dict[str, int] = {
    "agency": 70,
    "pac": 60,
    "bill": 55,
    "person": 50,
    "candidate": 45,
    "organization": 40,
    "contract": 35,
    "lobbying_reg": 34,
    "event": 30,
    "place": 25,
    "concept": 20,
    "topic": 15,
    "unknown": 0,
}

_SURFACE_MODE_STRICTNESS = {"open": 0, "alias": 1, "suppress": 2}

# Not every ``fec.*`` alias carries a real FEC id: the disbursement ingester
# mints ``unknown-<hash>`` placeholders for recipients it could not resolve
# to a committee (``Open AI`` carries two of them). Those are NOT identity
# and must not pin a type — a placeholder would have retyped ``OpenAI`` as a
# ``pac``. Only well-formed FEC ids count.
_FEC_COMMITTEE_RE = re.compile(r"^C\d{8}$")
_FEC_CANDIDATE_RE = re.compile(r"^[HSP][0-9A-Z]{8}$")
_SEC_CIK_RE = re.compile(r"^\d{1,10}$")

_NAMESPACE_ID_RE: dict[str, re.Pattern[str]] = {
    "fec_committee": _FEC_COMMITTEE_RE,
    "fec_candidate": _FEC_CANDIDATE_RE,
    "sec_cik": _SEC_CIK_RE,
}


def _identity_key(namespace: str, source_id: str) -> str | None:
    """Normalized identity key, or None when ``source_id`` is not a
    well-formed id in that namespace."""
    key = (source_id or "").strip()
    if namespace == "sec_cik":
        key = key.lstrip("0")
    pattern = _NAMESPACE_ID_RE.get(namespace)
    if pattern is not None and not pattern.match(key):
        return None
    return key or None

# Type pairs we accept for a NAME-derived merge beyond same-type and
# X↔unknown. ``organization``/``concept`` is required by the live
# fragmentation helen measured (xAI, Starlink) — a company tagged as a
# concept by the news-tag pipeline.
_EXTRA_NAME_MERGE_PAIRS: frozenset[frozenset[str]] = frozenset(
    {frozenset({"organization", "concept"})}
)

# Minimum squashed-name length for the ``squashed_name`` rule — short
# squashes ("us a" → "usa") collide too easily to be evidence.
_MIN_SQUASH_LEN = 6
# A name has to be at least this long, and contain a letter, before an
# identical-name match counts as evidence. ``normalize_name`` strips
# punctuation, so without this "$85" / "85" / "85%" all normalize to "85"
# and "$BE" collapses onto the ticker "BE".
_MIN_NAME_LEN = 3
_HAS_ALPHA_RE = re.compile(r"[a-z]")
_HAS_DIGIT_RE = re.compile(r"\d")

# helen 2026-08-21: the organization/concept retype is on, but these four
# are known upstream mis-typings — something in the corpus claims
# ``organization`` for what is plainly a concept. Merge them, but do NOT
# propagate the bad type; the cluster is flagged for review instead.
# Keyed on the whitespace-stripped normalized name so "401(k)" / "401k" and
# "AI ETF" / "Chipmakers" / "chip makers" all match one entry each.
_CONCEPT_TO_ORG_DENYLIST: frozenset[str] = frozenset(
    {"401k", "adr", "aietf", "chipmakers"}
)

# helen 2026-08-21: ``prefix_variant`` stays review-only in general — a
# token-boundary tail matches far too much to trust. These two specific
# pairs are approved: OCR mangles of the Palantir SEC issuer name, each
# with 1 edge and 0 aliases. Keyed as (short_norm, long_norm) so the rule
# can never widen beyond what was reviewed.
_PREFIX_CURATED: frozenset[tuple[str, str]] = frozenset(
    {
        ("palantir technologies", "palantir technologies in"),
        ("palantir technologies", "palantir technologies inclass"),
    }
)


def _name_is_evidence(norm: str) -> bool:
    """Is this normalized name distinctive enough to key a merge on?"""
    return len(norm) >= _MIN_NAME_LEN and bool(_HAS_ALPHA_RE.search(norm))
# Minimum normalized-name length for the review-only ``prefix_variant``
# rule; short prefixes match far too much to even be worth reviewing.
_MIN_PREFIX_LEN = 12


# ─── data ───────────────────────────────────────────────────────────────


@dataclass
class Ent:
    """One canonical entity, plus the counters survivor-selection needs."""

    id: str
    name: str
    norm: str
    type: str
    surface_mode: str
    publication_state: str
    created_at: datetime | None
    alias_count: int = 0
    edge_count: int = 0
    # Identity namespaces this canonical carries an authoritative id in.
    id_namespaces: set[str] = field(default_factory=set)
    anchored: bool = False

    @property
    def squashed(self) -> str:
        return self.norm.replace(" ", "")

    @property
    def has_ext_id(self) -> bool:
        return bool(self.id_namespaces) or self.anchored


@dataclass
class Candidate:
    """One proposed (a, b) same-entity pair from one rule."""

    a: str
    b: str
    rule: str
    evidence: str


@dataclass
class Cluster:
    """A resolved merge cluster: one survivor + the canonicals folded in."""

    survivor: Ent
    dropped: list[Ent]
    rules: set[str]
    survivor_type: str
    evidence: list[str]
    type_conflict: bool = False

    def to_dict(self) -> dict:
        return {
            "type_conflict": self.type_conflict,
            "survivor": {
                "id": self.survivor.id,
                "name": self.survivor.name,
                "type": self.survivor.type,
                "new_type": self.survivor_type,
                "surface_mode": self.survivor.surface_mode,
                "edges": self.survivor.edge_count,
                "aliases": self.survivor.alias_count,
            },
            "dropped": [
                {
                    "id": d.id,
                    "name": d.name,
                    "type": d.type,
                    "surface_mode": d.surface_mode,
                    "edges": d.edge_count,
                    "aliases": d.alias_count,
                }
                for d in self.dropped
            ],
            "rules": sorted(self.rules),
            "evidence": self.evidence[:6],
        }


@dataclass
class Skipped:
    """A candidate pair we refused, with the machine-readable reason."""

    a: str
    b: str
    a_name: str
    b_name: str
    a_detail: str
    b_detail: str
    rule: str
    reason: str

    def to_dict(self) -> dict:
        return {
            "reason": self.reason,
            "rule": self.rule,
            "a": {"id": self.a, "name": self.a_name, "detail": self.a_detail},
            "b": {"id": self.b, "name": self.b_name, "detail": self.b_detail},
        }


# ─── compatibility + survivor policy ────────────────────────────────────


def _name_merge_compatible(
    t1: str, t2: str, allow_person_unknown: bool = False
) -> tuple[bool, str]:
    """Is a NAME-derived merge of these two types allowed?

    Returns ``(ok, reason)``; ``reason`` names the review bucket when not.
    """
    if "person" in (t1, t2):
        # person↔person and person↔unknown alike: a false merge of two real
        # people is the worst failure this pass can produce. The
        # person↔unknown half can be opted in explicitly by helen via
        # ``--rules …,person_unknown``; person↔person never can.
        if allow_person_unknown and "unknown" in (t1, t2) and t1 != t2:
            return True, ""
        return False, "person_name"
    if t1 == t2:
        return True, ""
    if "pac" in (t1, t2):
        # normalize_name() strips the trailing "pac" legal suffix, so
        # "AMERICA PAC" collides with the place "America". Never guess.
        return False, "pac_cross_type"
    if "unknown" in (t1, t2):
        # An `unknown` carries no type claim — inheriting the typed side is
        # exactly the brief's "merged into a typed survivor → inherit".
        return True, ""
    if frozenset({t1, t2}) in _EXTRA_NAME_MERGE_PAIRS:
        return True, ""
    return False, "incompatible_type"


def _ext_id_merge_compatible(t1: str, t2: str) -> tuple[bool, str]:
    """Type gate for an EXTERNAL-ID merge.

    A shared authoritative id is strong evidence, so this is looser than the
    name gate — but a person↔non-person pair still means one of the two ids
    is wrong, so refuse and send it to review.
    """
    if t1 == t2 or "unknown" in (t1, t2):
        return True, ""
    person_side = {"person", "candidate"}
    if bool({t1} & person_side) != bool({t2} & person_side):
        return False, "ext_id_person_mismatch"
    return True, ""


def _survivor_sort_key(e: Ent) -> tuple:
    """Richest-evidence-first ordering. Ties broken deterministically so a
    re-run picks the same survivor."""
    return (
        0 if e.has_ext_id else 1,
        -e.edge_count,
        -e.alias_count,
        e.created_at.isoformat() if e.created_at else "9999",
        e.id,
    )


def _resolve_type(
    members: list[Ent], survivor: Ent, concept_to_org: bool = True
) -> tuple[str, bool]:
    """Surviving type for a merge cluster. Returns ``(type, conflicted)``.

    Reliable signals only, in order:

    1. Every member is ``unknown`` → an authoritative external id may lift
       it (SEC CIK → organization, FEC committee → pac, bioguide → person);
       otherwise it stays ``unknown``.  Nothing is inferred from the name.
    2. Exactly one real type among the members → the survivor inherits it.
       This is the ``unknown`` → typed upgrade the brief asks for.
    3. ``{organization, concept}`` → ``organization``.  A company tagged as
       a "concept" by the news-tag pipeline is a documented failure mode
       (xAI, Starlink, OpenAI) — the org claim is the specific one.
    4. Any other disagreement is a genuine conflict: keep the SURVIVOR's own
       type and flag the cluster so a human decides. Never rank-and-guess.
    """
    typed = {m.type for m in members if m.type != "unknown"}
    if not typed:
        namespaces: set[str] = set()
        for m in members:
            namespaces |= m.id_namespaces
        for ns, t in _NAMESPACE_TYPE.items():
            if ns in namespaces:
                return t, False
        return "unknown", False
    if len(typed) == 1:
        return typed.pop(), False
    if typed == {"organization", "concept"}:
        # Note the cost of this rule: it also propagates upstream
        # mis-typings ("401(k)", "ADR", "AI ETF" are all tagged
        # ``organization`` somewhere in the corpus). ``concept_to_org=False``
        # turns it off — the merge still happens, the survivor keeps its own
        # type, and the cluster is flagged for review instead.
        denied = any(m.squashed in _CONCEPT_TO_ORG_DENYLIST for m in members)
        if concept_to_org and not denied:
            return "organization", False
        return (survivor.type if survivor.type != "unknown" else "concept"), True
    if survivor.type != "unknown":
        return survivor.type, True
    return max(typed, key=lambda t: _TYPE_AUTHORITY.get(t, 0)), True


# ─── loading ────────────────────────────────────────────────────────────


async def _load(session: AsyncSession) -> tuple[dict[str, Ent], list[tuple]]:
    """Load every canonical + its counters, and every edge triple."""
    rows = (
        await session.execute(
            text(
                """
                select e.id, e.canonical_name, e.canonical_name_normalized,
                       e.type, e.surface_mode, e.publication_state, e.created_at,
                       coalesce(a.n, 0) as alias_count,
                       coalesce(o.n, 0) + coalesce(i.n, 0) as edge_count
                from canonical_entities e
                left join (select canonical_id, count(*) n
                             from entity_aliases group by 1) a
                       on a.canonical_id = e.id
                left join (select source_id id, count(*) n
                             from canonical_edges group by 1) o on o.id = e.id
                left join (select target_id id, count(*) n
                             from canonical_edges group by 1) i on i.id = e.id
                """
            )
        )
    ).all()
    ents: dict[str, Ent] = {}
    for r in rows:
        ents[r[0]] = Ent(
            id=r[0],
            name=r[1],
            norm=r[2] or "",
            type=r[3],
            surface_mode=r[4],
            publication_state=r[5],
            created_at=r[6],
            alias_count=int(r[7]),
            edge_count=int(r[8]),
        )

    for cid, ss, sid in (
        await session.execute(
            text(
                "select canonical_id, source_system, source_id from entity_aliases "
                "where source_system = any(:ns)"
            ),
            {"ns": list(_IDENTITY_NAMESPACE)},
        )
    ).all():
        e = ents.get(cid)
        ns = _IDENTITY_NAMESPACE[ss]
        if e is not None and _identity_key(ns, sid) is not None:
            e.id_namespaces.add(ns)

    for (cid,) in (
        await session.execute(
            text("select canonical_id from anchor_registry where canonical_id is not null")
        )
    ).all():
        e = ents.get(cid)
        if e is not None:
            e.anchored = True

    edges = (
        await session.execute(
            text(
                """
                select e.source_id, e.target_id, e.relation,
                       coalesce(c.n, 0)
                from canonical_edges e
                left join (select edge_id, count(*) n
                             from source_citations group by 1) c
                       on c.edge_id = e.id
                """
            )
        )
    ).all()
    return ents, [tuple(r) for r in edges]


# ─── candidate generation ───────────────────────────────────────────────


async def _cand_ext_id(
    session: AsyncSession, ents: dict[str, Ent]
) -> list[Candidate]:
    """Rule ``ext_id`` — canonicals sharing an authoritative external id."""
    buckets: dict[tuple[str, str], set[str]] = defaultdict(set)

    for cid, ss, sid in (
        await session.execute(
            text(
                "select canonical_id, source_system, source_id from entity_aliases "
                "where source_system = any(:ns)"
            ),
            {"ns": list(_IDENTITY_NAMESPACE)},
        )
    ).all():
        ns = _IDENTITY_NAMESPACE[ss]
        key = _identity_key(ns, sid)
        if key is not None:
            buckets[(ns, key)].add(cid)

    # anchor_registry rows carry the same ids and a canonical_id FK — an
    # anchor is an identity assertion a human curated.
    for cid, cik, cmtes, cands in (
        await session.execute(
            text(
                "select canonical_id, sec_cik, fec_committee_ids, fec_candidate_ids "
                "from anchor_registry where canonical_id is not null"
            )
        )
    ).all():
        for ns, values in (
            ("sec_cik", [str(cik)] if cik else []),
            ("fec_committee", list(cmtes or ())),
            ("fec_candidate", list(cands or ())),
        ):
            for v in values:
                key = _identity_key(ns, str(v))
                if key is not None:
                    buckets[(ns, key)].add(cid)

    out: list[Candidate] = []
    for (ns, key), ids in buckets.items():
        live = sorted(i for i in ids if i in ents)
        if len(live) < 2:
            continue
        for other in live[1:]:
            out.append(Candidate(live[0], other, "ext_id", f"{ns}={key}"))
    return out


async def _cand_sec_former_name(
    session: AsyncSession, ents: dict[str, Ent], by_norm: dict[str, list[Ent]]
) -> list[Candidate]:
    """Rule ``sec_former_name`` — a canonical named like a CIK owner's SEC
    former name folds into the CIK owner."""
    out: list[Candidate] = []
    for cid, surface in (
        await session.execute(
            text(
                "select canonical_id, surface_name from entity_aliases "
                "where source_system = 'sec.former_name'"
            )
        )
    ).all():
        owner = ents.get(cid)
        if owner is None:
            continue
        norm = normalize_name(surface)
        if not norm:
            continue
        for other in by_norm.get(norm, ()):
            if other.id == owner.id:
                continue
            out.append(
                Candidate(owner.id, other.id, "sec_former_name",
                          f"SEC former name {surface!r}")
            )
    return out


def _cand_names(
    ents: dict[str, Ent], by_norm: dict[str, list[Ent]]
) -> list[Candidate]:
    """Rules ``exact_name`` + ``squashed_name``."""
    out: list[Candidate] = []
    for norm, group in by_norm.items():
        if len(group) < 2 or not _name_is_evidence(norm):
            continue
        anchor = group[0]
        for other in group[1:]:
            out.append(Candidate(anchor.id, other.id, "exact_name", f"norm={norm!r}"))

    by_squash: dict[str, list[Ent]] = defaultdict(list)
    for e in ents.values():
        sq = e.squashed
        # Digits are excluded outright: normalize_name turns "." into a
        # space, so "$2.5 billion" and "$25 billion" both squash to
        # "25billion" — and "$11.5 million" onto "$115 Million". Numeric
        # names can never be squash evidence.
        if len(sq) >= _MIN_SQUASH_LEN and not _HAS_DIGIT_RE.search(sq):
            by_squash[sq].append(e)
    for sq, group in by_squash.items():
        if len(group) < 2 or len({e.norm for e in group}) < 2:
            continue  # single normalized form → already covered by exact_name
        anchor = group[0]
        for other in group[1:]:
            if other.norm == anchor.norm:
                continue
            out.append(
                Candidate(anchor.id, other.id, "squashed_name", f"squash={sq!r}")
            )
    return out


def _cand_prefix(by_norm: dict[str, list[Ent]]) -> list[Candidate]:
    """Review-only rule ``prefix_variant`` — OCR/truncation tails such as
    ``PALANTIR TECHNOLOGIES IN`` against ``Palantir Technologies Inc.``."""
    norms = sorted(
        n for n in by_norm if len(n) >= _MIN_PREFIX_LEN and _name_is_evidence(n)
    )
    out: list[Candidate] = []
    for i, short in enumerate(norms):
        for long in norms[i + 1:]:
            if not long.startswith(short):
                break
            if long == short:
                continue
            tail = long[len(short):]
            # Only a *token-boundary* tail, and only a short one: a genuine
            # OCR tail ("in", "inclass"), not a different entity
            # ("bank of america plaza").
            if not tail.startswith(" ") or len(tail) > 10:
                continue
            rule = (
                "prefix_variant_curated"
                if (short, long) in _PREFIX_CURATED
                else "prefix_variant"
            )
            for a in by_norm[short]:
                for b in by_norm[long]:
                    out.append(
                        Candidate(a.id, b.id, rule,
                                  f"{short!r} + tail {tail.strip()!r}")
                    )
    return out


async def _cand_vector(
    session: AsyncSession, ents: dict[str, Ent], threshold: float
) -> list[Candidate]:
    """Rule ``vector`` — pgvector cosine ≥ ``threshold`` plus ≥1 shared name
    token. Server-side lateral top-K so the whole scan is one round trip."""
    rows = (
        await session.execute(
            text(
                """
                select a.id, n.id, 1 - (a.embedding <=> n.embedding) as sim
                from canonical_entities a
                cross join lateral (
                    select e.id, e.embedding
                    from canonical_entities e
                    where e.embedding is not null
                      and e.id <> a.id
                      and e.surface_mode = a.surface_mode
                    order by e.embedding <=> a.embedding
                    limit :k
                ) n
                where a.embedding is not null
                  and 1 - (a.embedding <=> n.embedding) >= :thr
                """
            ),
            {"k": settings.resolution_top_k, "thr": threshold},
        )
    ).all()

    out: list[Candidate] = []
    seen: set[tuple[str, str]] = set()
    for aid, bid, sim in rows:
        pair = (aid, bid) if aid < bid else (bid, aid)
        if pair in seen:
            continue
        seen.add(pair)
        a, b = ents.get(pair[0]), ents.get(pair[1])
        if a is None or b is None or a.norm == b.norm:
            continue  # exact_name already owns identical names
        if not (set(a.norm.split()) & set(b.norm.split())):
            continue  # shared-token gate — cosine alone over-merges
        out.append(Candidate(a.id, b.id, "vector", f"cosine={float(sim):.4f}"))
    return out


# ─── clustering ─────────────────────────────────────────────────────────


class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        self.parent.setdefault(x, x)
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]
            x = self.parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


@dataclass
class Plan:
    """The full outcome of a planning run — what merges, what is refused."""

    clusters: list[Cluster] = field(default_factory=list)
    skipped: list[Skipped] = field(default_factory=list)
    review: list[Skipped] = field(default_factory=list)
    rule_pairs: dict[str, int] = field(default_factory=lambda: defaultdict(int))
    rule_accepted: dict[str, int] = field(default_factory=lambda: defaultdict(int))

    def skip_counts(self) -> dict[str, int]:
        c: dict[str, int] = defaultdict(int)
        for s in self.skipped:
            c[s.reason] += 1
        for s in self.review:
            c[s.reason] += 1
        return dict(sorted(c.items(), key=lambda kv: -kv[1]))


def _skip(cands_a: Ent, cands_b: Ent, rule: str, reason: str) -> Skipped:
    return Skipped(
        a=cands_a.id,
        b=cands_b.id,
        a_name=cands_a.name,
        b_name=cands_b.name,
        a_detail=f"{cands_a.type}/{cands_a.surface_mode} "
                 f"edges={cands_a.edge_count} aliases={cands_a.alias_count}",
        b_detail=f"{cands_b.type}/{cands_b.surface_mode} "
                 f"edges={cands_b.edge_count} aliases={cands_b.alias_count}",
        rule=rule,
        reason=reason,
    )


def build_plan(
    ents: dict[str, Ent],
    candidates: list[Candidate],
    applied_rules: set[str],
    allow_person_unknown: bool = False,
    concept_to_org: bool = True,
) -> Plan:
    """Turn raw candidate pairs into merge clusters + a review list.

    Fail-closed order per pair: surface_mode → publication_state → type.
    A pair that trips any gate never reaches the union-find, so it can not
    be pulled in transitively either.
    """
    plan = Plan()
    uf = _UnionFind()
    accepted: list[Candidate] = []

    for c in candidates:
        a, b = ents.get(c.a), ents.get(c.b)
        if a is None or b is None or a.id == b.id:
            continue
        plan.rule_pairs[c.rule] += 1

        # ── HARD PRIVACY GATE — never merge across surface_mode. ────────
        if a.surface_mode != b.surface_mode:
            plan.skipped.append(_skip(a, b, c.rule, "surface_mode_straddle"))
            continue
        # Lifecycle gate — a merge must never silently publish staged data.
        if a.publication_state != b.publication_state:
            plan.skipped.append(_skip(a, b, c.rule, "publication_state_straddle"))
            continue

        if c.rule in ("ext_id", "sec_former_name"):
            ok, reason = _ext_id_merge_compatible(a.type, b.type)
        else:
            ok, reason = _name_merge_compatible(
                a.type, b.type, allow_person_unknown
            )
        if not ok:
            plan.review.append(_skip(a, b, c.rule, reason))
            continue

        if c.rule not in applied_rules:
            plan.review.append(_skip(a, b, c.rule, f"rule_not_enabled:{c.rule}"))
            continue

        accepted.append(c)
        uf.union(a.id, b.id)

    groups: dict[str, list[str]] = defaultdict(list)
    for node in list(uf.parent):
        groups[uf.find(node)].append(node)

    ev_by_root: dict[str, list[str]] = defaultdict(list)
    rules_by_root: dict[str, set[str]] = defaultdict(set)
    for c in accepted:
        root = uf.find(c.a)
        rules_by_root[root].add(c.rule)
        ev_by_root[root].append(f"{c.rule}: {c.evidence}")

    for root, ids in groups.items():
        members = [ents[i] for i in ids if i in ents]
        if len(members) < 2:
            continue

        # Transitive closure can pull an incompatible type into a cluster
        # (place ~ unknown ~ organization). Refuse the whole cluster rather
        # than guess which member is the odd one out.
        bad = _first_incompatible(
            members, rules_by_root[root], allow_person_unknown
        )
        if bad is not None:
            plan.review.append(
                _skip(bad[0], bad[1], "|".join(sorted(rules_by_root[root])),
                      "type_incompatible_cluster")
            )
            continue

        members.sort(key=_survivor_sort_key)
        survivor, dropped = members[0], members[1:]
        for d in dropped:
            plan.rule_accepted["_entities_dropped"] += 1
        for r in rules_by_root[root]:
            plan.rule_accepted[r] += 1
        stype, conflicted = _resolve_type(members, survivor, concept_to_org)
        plan.clusters.append(
            Cluster(
                survivor=survivor,
                dropped=dropped,
                rules=set(rules_by_root[root]),
                survivor_type=stype,
                type_conflict=conflicted,
                evidence=sorted(set(ev_by_root[root])),
            )
        )

    plan.clusters.sort(key=lambda c: (-len(c.dropped), c.survivor.name))
    return plan


def _first_incompatible(
    members: list[Ent], rules: set[str], allow_person_unknown: bool = False
) -> tuple[Ent, Ent] | None:
    """First type-incompatible pair inside a cluster, or None.

    The looser external-id gate applies only when EVERY rule that built the
    cluster is id-backed — one name-derived link is enough to demand the
    strict name gate across the whole cluster.
    """
    id_backed = bool(rules) and rules <= {"ext_id", "sec_former_name"}
    for i, a in enumerate(members):
        for b in members[i + 1:]:
            if id_backed:
                ok, _ = _ext_id_merge_compatible(a.type, b.type)
            else:
                ok, _ = _name_merge_compatible(
                    a.type, b.type, allow_person_unknown
                )
            if not ok:
                return a, b
    return None


# ─── prediction ─────────────────────────────────────────────────────────


def predict(
    ents: dict[str, Ent], edges: list[tuple], plan: Plan
) -> dict:
    """Predict the post-merge shape WITHOUT touching the DB."""
    remap: dict[str, str] = {}
    new_type: dict[str, str] = {}
    for cl in plan.clusters:
        new_type[cl.survivor.id] = cl.survivor_type
        for d in cl.dropped:
            remap[d.id] = cl.survivor.id

    before_types: dict[str, int] = defaultdict(int)
    after_types: dict[str, int] = defaultdict(int)
    for e in ents.values():
        before_types[e.type] += 1
        if e.id in remap:
            continue
        after_types[new_type.get(e.id, e.type)] += 1

    seen: set[tuple[str, str, str]] = set()
    self_loops = 0
    self_loop_citations = 0
    collisions = 0
    for src, tgt, rel, ncit in edges:
        s = remap.get(src, src)
        t = remap.get(tgt, tgt)
        if s == t and src != tgt:
            self_loops += 1
            self_loop_citations += int(ncit)
            continue
        if s == t:
            continue  # pre-existing self-loop, untouched by this pass
        key = (s, t, rel)
        if key in seen:
            collisions += 1
        seen.add(key)

    pre_existing_self = sum(1 for s, t, _r, _n in edges if s == t)
    before_entities = len(ents)
    after_entities = before_entities - len(remap)
    before_edges = len(edges)
    after_edges = before_edges - self_loops - collisions
    before_citations = sum(int(n) for *_x, n in edges)

    type_upgrades: dict[str, int] = defaultdict(int)
    conflicts = 0
    for cl in plan.clusters:
        if cl.survivor_type != cl.survivor.type:
            type_upgrades[f"{cl.survivor.type}→{cl.survivor_type}"] += 1
        if cl.type_conflict:
            conflicts += 1

    return {
        "entities_before": before_entities,
        "entities_after": after_entities,
        "entities_dropped": len(remap),
        "unknown_before": before_types.get("unknown", 0),
        "unknown_after": after_types.get("unknown", 0),
        "unknown_pct_before": round(
            100.0 * before_types.get("unknown", 0) / max(before_entities, 1), 2
        ),
        "unknown_pct_after": round(
            100.0 * after_types.get("unknown", 0) / max(after_entities, 1), 2
        ),
        "types_before": dict(sorted(before_types.items(), key=lambda kv: -kv[1])),
        "types_after": dict(sorted(after_types.items(), key=lambda kv: -kv[1])),
        "edges_before": before_edges,
        "edges_after": after_edges,
        "edges_collapsed_into_existing": collisions,
        "self_loops_created": self_loops,
        "citations_on_created_self_loops": self_loop_citations,
        "pre_existing_self_loops": pre_existing_self,
        "citations_before": before_citations,
        "citations_after": before_citations - self_loop_citations,
        "type_upgrades": dict(sorted(type_upgrades.items(), key=lambda kv: -kv[1])),
        "clusters_with_type_conflict": conflicts,
    }


# ─── planning entrypoint ────────────────────────────────────────────────

DEFAULT_APPLIED_RULES = frozenset(
    {
        "ext_id",
        "sec_former_name",
        "exact_name",
        "squashed_name",
        # Safe to default-on: it can only ever match the two hand-reviewed
        # pairs pinned in _PREFIX_CURATED.
        "prefix_variant_curated",
    }
)
ALL_RULES = DEFAULT_APPLIED_RULES | {"prefix_variant", "vector", "person_unknown"}


async def plan_dedup(
    session: AsyncSession,
    applied_rules: set[str] | None = None,
    enable_vector: bool = False,
    vector_threshold: float = 0.93,
    with_prefix_preview: bool = True,
    allow_person_unknown: bool = False,
    concept_to_org: bool = True,
) -> tuple[dict[str, Ent], list[tuple], Plan]:
    """Generate candidates + build the merge plan. Pure read."""
    ents, edges = await _load(session)

    by_norm: dict[str, list[Ent]] = defaultdict(list)
    for e in ents.values():
        by_norm[e.norm].append(e)
    for group in by_norm.values():
        group.sort(key=_survivor_sort_key)

    candidates: list[Candidate] = []
    candidates += await _cand_ext_id(session, ents)
    candidates += await _cand_sec_former_name(session, ents, by_norm)
    candidates += _cand_names(ents, by_norm)
    if with_prefix_preview:
        candidates += _cand_prefix(by_norm)
    if enable_vector:
        candidates += await _cand_vector(session, ents, vector_threshold)

    rules = set(applied_rules if applied_rules is not None else DEFAULT_APPLIED_RULES)
    return ents, edges, build_plan(
        ents,
        candidates,
        rules,
        allow_person_unknown=allow_person_unknown,
        concept_to_org=concept_to_org,
    )


# ─── apply ──────────────────────────────────────────────────────────────


@dataclass
class ApplyStats:
    clusters_applied: int = 0
    entities_dropped: int = 0
    edges_repointed: int = 0
    edges_collided_summed: int = 0
    citations_reparented: int = 0
    aliases_repointed: int = 0
    scrutiny_repointed: int = 0
    self_loops_deleted: int = 0
    self_loop_citations_deleted: int = 0
    types_upgraded: int = 0
    refused: int = 0


async def _existing_self_loops(session: AsyncSession, keep: str) -> list[str]:
    """Self-loop edge ids already on the survivor BEFORE the merge — 32 such
    edges predate this pass and are none of its business."""
    return [
        r[0]
        for r in (
            await session.execute(
                text(
                    "select id from canonical_edges "
                    "where source_id = :id and target_id = :id"
                ),
                {"id": keep},
            )
        ).all()
    ]


async def _drop_self_loops(
    session: AsyncSession, keep: str, preexisting: list[str]
) -> tuple[int, int]:
    """Delete the self-loops the merge itself created (an A—B edge whose two
    endpoints just became the same node). Pre-existing loops are left alone.
    Returns (edges, citations) removed."""
    params = {"id": keep, "pre": preexisting or [""]}
    ncit = (
        await session.execute(
            text(
                "select count(*) from source_citations c "
                "join canonical_edges e on e.id = c.edge_id "
                "where e.source_id = :id and e.target_id = :id "
                "and e.id <> all(:pre)"
            ),
            params,
        )
    ).scalar_one()
    res = await session.execute(
        text(
            "delete from canonical_edges where source_id = :id "
            "and target_id = :id and id <> all(:pre)"
        ),
        params,
    )
    return res.rowcount or 0, int(ncit)


async def apply_plan(
    plan: Plan, limit: int | None = None, keep_self_loops: bool = False
) -> ApplyStats:
    """DESTRUCTIVE. Execute the plan — one transaction per cluster.

    Idempotent: a re-run re-plans from the post-merge state, where the
    dropped canonicals no longer exist, so the same clusters are not
    proposed twice.
    """
    stats = ApplyStats()
    sm = get_sessionmaker()

    for cl in plan.clusters[: limit if limit is not None else len(plan.clusters)]:
        async with sm() as session:
            try:
                preexisting = await _existing_self_loops(session, cl.survivor.id)
                for d in cl.dropped:
                    ms = await merge_two_canonicals(session, cl.survivor.id, d.id)
                    if ms.refused:
                        logger.error(
                            "REFUSED %s → %s: %s", d.id, cl.survivor.id,
                            ms.refused_reason,
                        )
                        stats.refused += 1
                        continue
                    stats.edges_repointed += ms.edges_repointed
                    stats.edges_collided_summed += ms.edges_collided_summed
                    stats.citations_reparented += ms.citations_reparented
                    stats.aliases_repointed += ms.aliases_repointed
                    stats.scrutiny_repointed += ms.scrutiny_repointed
                    stats.entities_dropped += 1
                    session.add(
                        AliasCrosswalk(
                            from_id=None,
                            to_id=cl.survivor.id,
                            from_id_frozen=d.id,
                            to_id_frozen=cl.survivor.id,
                            reason=f"P2 dedup [{','.join(sorted(cl.rules))}] "
                                   f"{d.name!r} → {cl.survivor.name!r}; "
                                   f"{'; '.join(cl.evidence[:3])}",
                            applied_at=datetime.now(timezone.utc),
                        )
                    )
                if not keep_self_loops:
                    loops, loop_cits = await _drop_self_loops(
                        session, cl.survivor.id, preexisting
                    )
                    stats.self_loops_deleted += loops
                    stats.self_loop_citations_deleted += loop_cits

                if cl.survivor_type != cl.survivor.type:
                    await session.execute(
                        text("update canonical_entities set type = :t where id = :id"),
                        {"t": cl.survivor_type, "id": cl.survivor.id},
                    )
                    stats.types_upgraded += 1
                await session.commit()
                stats.clusters_applied += 1
            except Exception:
                await session.rollback()
                logger.exception("cluster merge failed for survivor %s", cl.survivor.id)
                stats.refused += 1
    return stats


def _check_postconditions(before: dict, after: dict) -> dict:
    """Machine-checked pass/fail on the invariants that matter after a live
    merge. Reported alongside the numbers so nobody has to eyeball them."""
    b_scr, a_scr = before["scrutiny"], after["scrutiny"]
    protected_lost = {
        mode: b_scr["by_surface_mode"].get(mode, 0)
        - a_scr["by_surface_mode"].get(mode, 0)
        for mode in ("suppress", "alias")
    }
    checks = {
        "uncited_edges_still_zero": after["uncited_edges"] == 0,
        "no_orphan_citations": after["orphan_citations"] == 0,
        "scrutiny_rows_preserved": a_scr["total"] == b_scr["total"],
        "no_protected_scrutiny_rows_lost": all(
            v <= 0 for v in protected_lost.values()
        ),
        "no_entity_changed_surface_mode": all(
            after["surface_mode_counts"].get(m, 0)
            <= before["surface_mode_counts"].get(m, 0)
            for m in set(before["surface_mode_counts"]) | set(
                after["surface_mode_counts"]
            )
        ),
        "entities_only_decreased": after["entities"] <= before["entities"],
    }
    return {
        "all_passed": all(checks.values()),
        "checks": checks,
        "scrutiny_total_delta": a_scr["total"] - b_scr["total"],
        "protected_scrutiny_lost": protected_lost,
    }


async def scrutiny_on_dropped(session: AsyncSession, plan: Plan) -> dict:
    """Pre-flight: how many ``scrutiny_decisions`` rows sit on canonicals the
    plan would DROP, split by surface_mode.

    Every one of these has to be repointed onto the survivor before the
    delete, or the ON DELETE CASCADE eats it. This is the number the
    post-run census must account for.
    """
    dropped = [d.id for cl in plan.clusters for d in cl.dropped]
    if not dropped:
        return {"total": 0, "by_surface_mode": {}, "dropped_canonicals": 0}
    rows = (
        await session.execute(
            text(
                "select e.surface_mode, count(*) from scrutiny_decisions s "
                "join canonical_entities e on e.id = s.canonical_id "
                "where s.canonical_id = any(:ids) group by 1"
            ),
            {"ids": dropped},
        )
    ).all()
    by_mode = dict(rows)
    return {
        "total": sum(by_mode.values()),
        "by_surface_mode": by_mode,
        "dropped_canonicals": len(dropped),
    }


async def scrutiny_census(session: AsyncSession) -> dict:
    """Privacy-audit census: how many ``scrutiny_decisions`` rows exist, and
    how they split by the ``surface_mode`` of the canonical they document.

    Compared before and after a live pass. ``suppress``/``alias`` must not
    lose a single row — that is the privacy audit trail for a protected
    identity, and its FK is ON DELETE CASCADE.
    """
    total = (
        await session.execute(text("select count(*) from scrutiny_decisions"))
    ).scalar_one()
    by_mode = dict(
        (
            await session.execute(
                text(
                    "select e.surface_mode, count(*) from scrutiny_decisions s "
                    "join canonical_entities e on e.id = s.canonical_id "
                    "group by 1"
                )
            )
        ).all()
    )
    return {"total": total, "by_surface_mode": by_mode}


# ─── invariants ─────────────────────────────────────────────────────────


async def check_invariants(session: AsyncSession) -> dict:
    """The invariants helen validates. Run before AND after a live pass."""
    q = lambda s: session.execute(text(s))  # noqa: E731
    return {
        "entities": (await q("select count(*) from canonical_entities")).scalar_one(),
        "edges": (await q("select count(*) from canonical_edges")).scalar_one(),
        "citations": (await q("select count(*) from source_citations")).scalar_one(),
        "uncited_edges": (
            await q(
                "select count(*) from canonical_edges e where not exists "
                "(select 1 from source_citations c where c.edge_id = e.id)"
            )
        ).scalar_one(),
        "orphan_citations": (
            await q(
                "select count(*) from source_citations c where not exists "
                "(select 1 from canonical_edges e where e.id = c.edge_id)"
            )
        ).scalar_one(),
        "self_loops": (
            await q("select count(*) from canonical_edges where source_id = target_id")
        ).scalar_one(),
        "unknown_entities": (
            await q("select count(*) from canonical_entities where type = 'unknown'")
        ).scalar_one(),
        "surface_mode_counts": dict(
            (await q(
                "select surface_mode, count(*) from canonical_entities group by 1"
            )).all()
        ),
        "scrutiny": await scrutiny_census(session),
        "type_counts": dict(
            (await q(
                "select type, count(*) from canonical_entities group by 1"
            )).all()
        ),
    }


# ─── reporting ──────────────────────────────────────────────────────────

# Fragments helen named in the brief — the report must show what happens to
# each one, whether it merges or is refused.
NAMED_FRAGMENTS = ("tesla", "spacex", "space x", "xai", "starlink", "palantir")


def _sample_by_reason(items: list[Skipped], per_reason: int = 12) -> dict:
    """Up to ``per_reason`` worked examples for every refusal reason —
    a flat top-40 list is swamped by whichever bucket happens to be biggest."""
    out: dict[str, list[dict]] = defaultdict(list)
    for s in items:
        if len(out[s.reason]) < per_reason:
            out[s.reason].append(s.to_dict())
    return dict(sorted(out.items()))


def _touches_named(text_blob: str) -> bool:
    low = text_blob.lower()
    return any(n in low for n in NAMED_FRAGMENTS)


def build_report(
    ents: dict[str, Ent],
    edges: list[tuple],
    plan: Plan,
    invariants: dict,
    mode: str,
) -> dict:
    prediction = predict(ents, edges, plan)
    named_clusters = [
        c.to_dict() for c in plan.clusters
        if _touches_named(c.survivor.norm)
        or any(_touches_named(d.norm) for d in c.dropped)
    ]
    named_skips = [
        s.to_dict() for s in plan.skipped + plan.review
        if _touches_named(s.a_name) or _touches_named(s.b_name)
    ]
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": mode,
        "invariants_before": invariants,
        "prediction": prediction,
        "rules": {
            "candidate_pairs": dict(sorted(plan.rule_pairs.items())),
            "clusters_per_rule": dict(sorted(plan.rule_accepted.items())),
        },
        "clusters_total": len(plan.clusters),
        "skips": plan.skip_counts(),
        "surface_mode_straddling_pairs_skipped": sum(
            1 for s in plan.skipped if s.reason == "surface_mode_straddle"
        ),
        "surface_mode_straddling_examples": [
            s.to_dict() for s in plan.skipped
            if s.reason == "surface_mode_straddle"
        ][:25],
        "named_fragment_clusters": named_clusters,
        "named_fragment_skips": named_skips,
        "largest_clusters": [c.to_dict() for c in plan.clusters[:25]],
        "review_by_reason": _sample_by_reason(plan.review + plan.skipped),
        # The concept→organization retype is the judgement call in this
        # pass: it fixes xAI/Starlink/OpenAI but also propagates upstream
        # mis-typings. Sampled so it can be accepted or rejected on sight.
        "concept_to_org_sample": [
            c.to_dict() for c in plan.clusters
            if c.survivor.type == "concept" and c.survivor_type == "organization"
        ][:30],
        "type_conflict_clusters": [
            c.to_dict() for c in plan.clusters if c.type_conflict
        ][:30],
    }


def render_text(report: dict) -> str:
    p = report["prediction"]
    inv = report["invariants_before"]
    lines = [
        f"ARGUS P2 DEDUP — {report['mode'].upper()}  ({report['generated_at']})",
        "",
        "── counts ─────────────────────────────────────────────",
        f"entities   {p['entities_before']:>7} → {p['entities_after']:>7} "
        f"(-{p['entities_dropped']})",
        f"unknown    {p['unknown_before']:>7} → {p['unknown_after']:>7}   "
        f"{p['unknown_pct_before']}% → {p['unknown_pct_after']}%",
        f"edges      {p['edges_before']:>7} → {p['edges_after']:>7}  "
        f"(collapsed into existing: {p['edges_collapsed_into_existing']}, "
        f"self-loops removed: {p['self_loops_created']})",
        f"citations  {p['citations_before']:>7} → {p['citations_after']:>7}  "
        f"(only the {p['citations_on_created_self_loops']} sitting on removed "
        f"self-loops; every other receipt is re-parented)",
        f"uncited edges before: {inv['uncited_edges']}   "
        f"predicted after: 0 (a merge re-points citations, never drops them)",
        "",
        "── merge clusters ─────────────────────────────────────",
        f"clusters: {report['clusters_total']}   "
        f"type conflicts flagged: {p['clusters_with_type_conflict']}",
    ]
    for rule, n in report["rules"]["candidate_pairs"].items():
        acc = report["rules"]["clusters_per_rule"].get(rule, 0)
        lines.append(f"  {rule:<18} candidate pairs {n:>6}   clusters {acc:>6}")
    risk = report.get("scrutiny_rows_on_dropped_canonicals", {})
    lines += [
        "",
        "── privacy audit (scrutiny_decisions) ─────────────────",
        f"rows now: {inv['scrutiny']['total']}  by surface_mode: "
        f"{inv['scrutiny']['by_surface_mode']}",
        f"rows sitting on the {risk.get('dropped_canonicals', 0)} canonicals "
        f"this plan drops: {risk.get('total', 0)} {risk.get('by_surface_mode', {})}"
        "  (all repointed onto the survivor before any delete; the merge "
        "REFUSES if any is still attached)",
    ]
    after = report.get("postconditions")
    if after:
        lines.append(
            f"POSTCONDITIONS: {'ALL PASSED' if after['all_passed'] else 'FAILED'} "
            f"{after['checks']}"
        )
    lines += ["", "── type upgrades on survivors ─────────────────────────"]
    for change, n in p["type_upgrades"].items():
        lines.append(f"  {change:<28} {n:>6}")
    lines += [
        "",
        "── refused / review ───────────────────────────────────",
        f"surface_mode-straddling pairs SKIPPED: "
        f"{report['surface_mode_straddling_pairs_skipped']}",
    ]
    for reason, n in report["skips"].items():
        lines.append(f"  {reason:<30} {n:>6}")
    lines += ["", "── named fragments ────────────────────────────────────"]
    for c in report["named_fragment_clusters"]:
        s = c["survivor"]
        lines.append(
            f"  MERGE  {s['name']!r} [{s['type']}→{s['new_type']}/"
            f"{s['surface_mode']}] survives; drops: "
            + ", ".join(
                f"{d['name']!r}[{d['type']}/{d['surface_mode']}]"
                for d in c["dropped"]
            )
        )
    for s in report["named_fragment_skips"]:
        lines.append(
            f"  SKIP   {s['reason']}: {s['a']['name']!r} ({s['a']['detail']}) "
            f"×  {s['b']['name']!r} ({s['b']['detail']})"
        )
    return "\n".join(lines)


# ─── CLI ────────────────────────────────────────────────────────────────


async def run(
    apply: bool,
    applied_rules: set[str],
    enable_vector: bool,
    vector_threshold: float,
    json_report: str | None,
    limit: int | None,
    keep_self_loops: bool = False,
    concept_to_org: bool = True,
) -> dict:
    sm = get_sessionmaker()
    read_only_proof = None
    async with sm() as session:
        if not apply:
            # Belt-and-braces: in dry-run the transaction is physically
            # incapable of writing to live public data — any stray INSERT /
            # UPDATE / DELETE raises read_only_sql_transaction (25006)
            # instead of touching the published corpus.
            await session.execute(text("set transaction read only"))
            await session.execute(text("set default_transaction_read_only = on"))
            read_only_proof = (
                await session.execute(text("show transaction_read_only"))
            ).scalar_one()
        invariants = await check_invariants(session)
        ents, edges, plan = await plan_dedup(
            session,
            applied_rules=applied_rules,
            enable_vector=enable_vector,
            vector_threshold=vector_threshold,
            allow_person_unknown="person_unknown" in applied_rules,
            concept_to_org=concept_to_org,
        )
        scrutiny_at_risk = await scrutiny_on_dropped(session, plan)

    report = build_report(
        ents, edges, plan, invariants, "apply" if apply else "dry-run"
    )
    report["applied_rules"] = sorted(applied_rules)
    report["scrutiny_rows_on_dropped_canonicals"] = scrutiny_at_risk
    if read_only_proof is not None:
        report["dry_run_transaction_read_only"] = read_only_proof

    if apply:
        stats = await apply_plan(
            plan, limit=limit, keep_self_loops=keep_self_loops
        )
        report["apply_stats"] = stats.__dict__
        async with sm() as session:
            after = await check_invariants(session)
        report["invariants_after"] = after
        report["postconditions"] = _check_postconditions(invariants, after)

    print(render_text(report))
    if json_report:
        with open(json_report, "w") as fh:
            json.dump(report, fh, indent=2, default=str)
        logger.info("json report → %s", json_report)
    else:
        print("\n── JSON ──")
        print(json.dumps(report, indent=2, default=str))
    return report


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    ap = argparse.ArgumentParser(description="Argus P2 dedup/merge pass")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True)
    g.add_argument("--apply", action="store_true",
                   help="DESTRUCTIVE — execute the merges")
    ap.add_argument("--rules", default=",".join(sorted(DEFAULT_APPLIED_RULES)),
                    help=f"comma-separated subset of {sorted(ALL_RULES)}")
    ap.add_argument("--enable-vector", action="store_true",
                    help="generate pgvector candidates (preview unless "
                         "'vector' is also in --rules)")
    ap.add_argument("--vector-threshold", type=float, default=0.93)
    ap.add_argument("--json-report", default=None)
    ap.add_argument("--limit", type=int, default=None,
                    help="apply at most N clusters (staged rollout)")
    ap.add_argument("--no-concept-to-org", action="store_true",
                    help="do NOT retype an organization/concept cluster to "
                         "'organization'; keep the survivor's type and flag "
                         "the cluster for review")
    ap.add_argument("--keep-self-loops", action="store_true",
                    help="keep the A→A edges a merge creates (lossless but "
                         "leaves 'X mentioned_with X' artifacts in the graph)")
    args = ap.parse_args()

    rules = {r.strip() for r in args.rules.split(",") if r.strip()}
    unknown = rules - ALL_RULES
    if unknown:
        ap.error(f"unknown rules: {sorted(unknown)}")

    asyncio.run(
        run(
            apply=bool(args.apply),
            applied_rules=rules,
            enable_vector=args.enable_vector or "vector" in rules,
            vector_threshold=args.vector_threshold,
            json_report=args.json_report,
            limit=args.limit,
            keep_self_loops=args.keep_self_loops,
            concept_to_org=not args.no_concept_to_org,
        )
    )


if __name__ == "__main__":
    main()
