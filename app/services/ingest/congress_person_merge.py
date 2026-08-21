"""P1.5.3 — collapse fragmented news-person nodes onto the member.

The same sitting member is often spread across several canonicals: the
roster node (``Bernard Sanders`` — bioguide + FEC candidate ids) and one
or more news-tag fragments (``Bernie Sanders``, ``Chuck Schumer`` for
``Charles E. Schumer``). Each fragment holds real cited edges that never
reach the member's profile.

This pass reuses the P2 dedup machinery — the merge itself is
:func:`app.services.ingest.merge_canonicals.merge_two_canonicals`, which
re-points every edge, citation, alias, anchor and scrutiny row onto the
survivor and refuses (rather than degrades) when the privacy-audit rows
can't be moved. What is new here is the *candidate rule*: the member
canonical is always the survivor, and a fragment only qualifies on
evidence keyed to the roster.

Rules — person↔person is the most dangerous merge there is
----------------------------------------------------------
A false merge of two real people is worse than leaving them split, so:

* **Identity comes from the roster.** A fragment qualifies only by
  matching one of the member's roster name variants exactly (normalized).
* **Never against an external id.** A fragment already carrying an
  authoritative id (another bioguide, an FEC candidate/committee id, a
  SEC CIK) is a different real entity — refused.
* **Two tokens minimum.** A surname-only node ("Smith") is not identity.
* **Ambiguous names are refused.** A variant that matches two members,
  or a fragment that matches two members, is skipped and reported.
* **FAIL-CLOSED on surface_mode.** A pair whose members differ in
  ``surface_mode`` is NEVER merged — no flag overrides it. A pair where
  both sides are protected is also left alone: consolidating two
  protected identities is not this pass's call. The same partition
  applies to ``publication_state``.

Dry-run is the default and it is read-only at the transaction level.
``--apply`` is the only mode that writes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
from collections import defaultdict
from dataclasses import dataclass, field

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import CanonicalEntity, EntityAlias, EntityType, SurfaceMode
from app.services.graph.base import normalize_name
from app.services.ingest.congress_roster import (
    _AUTHORITATIVE_NAMESPACES,
    VARIANT_NAMESPACE,
)
from app.services.ingest.dedup_pass import _name_is_evidence
from app.services.ingest.merge_canonicals import (
    ScrutinyRepointFailed,
    merge_two_canonicals,
)

logger = logging.getLogger(__name__)

#: Types a member fragment may carry. `unknown` is included because 30%
#: of the registry is typed unknown; anything else (organization, place,
#: concept, pac) names a different kind of thing.
MERGEABLE_FRAGMENT_TYPES = frozenset(
    {EntityType.PERSON.value, EntityType.UNKNOWN.value}
)


# ─── data ───────────────────────────────────────────────────────────────


@dataclass(frozen=True)
class Node:
    """One canonical, reduced to what the merge decision needs."""

    id: str
    name: str
    norm: str
    type: str
    surface_mode: str
    publication_state: str
    edge_count: int = 0
    alias_count: int = 0
    namespaces: frozenset[str] = frozenset()


@dataclass(frozen=True)
class MergePair:
    """A planned merge — the member always survives."""

    member_id: str
    member_name: str
    fragment_id: str
    fragment_name: str
    matched_on: str
    fragment_edges: int
    fragment_aliases: int


@dataclass(frozen=True)
class SkippedPair:
    """A refused candidate + the rule that refused it."""

    member_id: str
    member_name: str
    fragment_id: str
    fragment_name: str
    reason: str
    detail: str = ""

    def to_dict(self) -> dict[str, str | int]:
        """JSON-able row for the review list."""
        return {
            "member_id": self.member_id,
            "member": self.member_name,
            "fragment_id": self.fragment_id,
            "fragment": self.fragment_name,
            "reason": self.reason,
            "detail": self.detail,
        }


@dataclass
class MergePlan:
    """What one planning pass decided."""

    pairs: list[MergePair] = field(default_factory=list)
    skipped: list[SkippedPair] = field(default_factory=list)

    @property
    def skipped_by_reason(self) -> dict[str, int]:
        """Refusal histogram — the report's headline safety number."""
        out: dict[str, int] = {}
        for s in self.skipped:
            out[s.reason] = out.get(s.reason, 0) + 1
        return out


def person_name_is_evidence(norm: str) -> bool:
    """A person name must have at least two real tokens.

    ``normalize_name`` strips punctuation, so a surname-only fragment
    ("Smith") or an initial ("A B") is not identity — it collides with
    every other person of that name in the corpus.
    """
    if not _name_is_evidence(norm):
        return False
    tokens = [t for t in norm.split(" ") if len(t) >= 2]
    return len(tokens) >= 2


# ─── planning (pure) ────────────────────────────────────────────────────


def build_merge_plan(
    members: dict[str, Node],
    variants: dict[str, set[str]],
    by_norm: dict[str, list[Node]],
) -> MergePlan:
    """Decide which fragments merge onto which member.

    ``members`` maps member canonical id → :class:`Node`.
    ``variants`` maps member canonical id → its normalized roster name
    variants. ``by_norm`` maps a normalized name → every canonical
    carrying it.
    """
    plan = MergePlan()

    # A variant that names two different members is not evidence for
    # either of them.
    owners: dict[str, set[str]] = defaultdict(set)
    for member_id, norms in variants.items():
        for norm in norms:
            owners[norm].add(member_id)

    # A fragment reachable from two different members is refused once,
    # against the first member that reaches it.
    claimed: dict[str, str] = {}
    contested: set[str] = set()
    for member_id, norms in variants.items():
        for norm in norms:
            if len(owners[norm]) > 1:
                continue
            for node in by_norm.get(norm, ()):
                if node.id in members:
                    continue
                if node.id in claimed and claimed[node.id] != member_id:
                    contested.add(node.id)
                claimed.setdefault(node.id, member_id)

    seen: set[tuple[str, str]] = set()
    for member_id, norms in sorted(variants.items()):
        member = members[member_id]
        for norm in sorted(norms):
            if len(owners[norm]) > 1:
                if not person_name_is_evidence(norm):
                    # A single-token variant ("Robert", "M") is refused by
                    # the evidence rule anyway; logging every node that
                    # happens to carry it buries the real ambiguities.
                    continue
                for node in by_norm.get(norm, ()):
                    if node.id in members:
                        continue
                    key = (member_id, node.id)
                    if key in seen:
                        continue
                    seen.add(key)
                    plan.skipped.append(
                        SkippedPair(
                            member_id, member.name, node.id, node.name,
                            "ambiguous_variant",
                            f"{norm!r} names {len(owners[norm])} members",
                        )
                    )
                continue
            if not person_name_is_evidence(norm):
                continue
            for node in by_norm.get(norm, ()):
                if node.id in members or node.id == member_id:
                    continue
                key = (member_id, node.id)
                if key in seen:
                    continue
                seen.add(key)

                if node.id in contested:
                    plan.skipped.append(
                        SkippedPair(
                            member_id, member.name, node.id, node.name,
                            "fragment_claimed_by_multiple_members", norm,
                        )
                    )
                    continue
                foreign = node.namespaces & _AUTHORITATIVE_NAMESPACES
                if foreign:
                    plan.skipped.append(
                        SkippedPair(
                            member_id, member.name, node.id, node.name,
                            "foreign_external_id", sorted(foreign)[0],
                        )
                    )
                    continue
                if node.type not in MERGEABLE_FRAGMENT_TYPES:
                    plan.skipped.append(
                        SkippedPair(
                            member_id, member.name, node.id, node.name,
                            "type_not_mergeable", node.type,
                        )
                    )
                    continue
                # ── THE NON-NEGOTIABLE RULE ─────────────────────────
                if node.surface_mode != member.surface_mode:
                    plan.skipped.append(
                        SkippedPair(
                            member_id, member.name, node.id, node.name,
                            "surface_mode_straddle",
                            f"member={member.surface_mode} "
                            f"fragment={node.surface_mode}",
                        )
                    )
                    continue
                if node.surface_mode != SurfaceMode.OPEN.value:
                    plan.skipped.append(
                        SkippedPair(
                            member_id, member.name, node.id, node.name,
                            "both_protected", node.surface_mode,
                        )
                    )
                    continue
                if node.publication_state != member.publication_state:
                    plan.skipped.append(
                        SkippedPair(
                            member_id, member.name, node.id, node.name,
                            "publication_state_straddle",
                            f"member={member.publication_state} "
                            f"fragment={node.publication_state}",
                        )
                    )
                    continue
                plan.pairs.append(
                    MergePair(
                        member_id=member_id,
                        member_name=member.name,
                        fragment_id=node.id,
                        fragment_name=node.name,
                        matched_on=norm,
                        fragment_edges=node.edge_count,
                        fragment_aliases=node.alias_count,
                    )
                )
    return plan


# ─── loading ────────────────────────────────────────────────────────────


async def _load(
    session: AsyncSession,
) -> tuple[dict[str, Node], dict[str, set[str]], dict[str, list[Node]]]:
    """Load members, their roster name variants, and the normalized-name
    index over every canonical."""
    ns_rows = (
        await session.execute(
            select(EntityAlias.canonical_id, EntityAlias.source_system).where(
                EntityAlias.source_system.in_(sorted(_AUTHORITATIVE_NAMESPACES))
            )
        )
    ).all()
    namespaces: dict[str, set[str]] = defaultdict(set)
    for canonical_id, source_system in ns_rows:
        namespaces[canonical_id].add(source_system)

    counts = dict(
        (
            await session.execute(
                text(
                    "select c.id, "
                    "  (select count(*) from canonical_edges e "
                    "     where e.source_id=c.id or e.target_id=c.id) "
                    "from canonical_entities c"
                )
            )
        ).all()
    )
    alias_counts = dict(
        (
            await session.execute(
                text(
                    "select canonical_id, count(*) from entity_aliases "
                    "group by 1"
                )
            )
        ).all()
    )

    nodes: dict[str, Node] = {}
    by_norm: dict[str, list[Node]] = defaultdict(list)
    for ent in (
        await session.execute(select(CanonicalEntity))
    ).scalars():
        node = Node(
            id=ent.id,
            name=ent.canonical_name,
            norm=ent.canonical_name_normalized,
            type=ent.type,
            surface_mode=ent.surface_mode,
            publication_state=ent.publication_state,
            edge_count=counts.get(ent.id, 0),
            alias_count=alias_counts.get(ent.id, 0),
            namespaces=frozenset(namespaces.get(ent.id, ())),
        )
        nodes[node.id] = node
        by_norm[node.norm].append(node)

    member_ids = {
        r[0]
        for r in (
            await session.execute(
                select(EntityAlias.canonical_id).where(
                    EntityAlias.source_system == "bioguide"
                )
            )
        ).all()
    }
    members = {mid: nodes[mid] for mid in member_ids if mid in nodes}

    # Roster name variants, written by the roster pass. The member's own
    # canonical name is always included so the pass still works before
    # the variant aliases exist.
    variants: dict[str, set[str]] = {
        mid: {members[mid].norm} for mid in members
    }
    for canonical_id, surface_norm in (
        await session.execute(
            select(
                EntityAlias.canonical_id, EntityAlias.surface_name_normalized
            ).where(EntityAlias.source_system == VARIANT_NAMESPACE)
        )
    ).all():
        if canonical_id in variants and surface_norm:
            variants[canonical_id].add(normalize_name(surface_norm))
    return members, variants, dict(by_norm)


# ─── apply ──────────────────────────────────────────────────────────────


@dataclass
class ApplyStats:
    """Counters for the destructive half."""

    merged: int = 0
    refused: int = 0
    edges_repointed: int = 0
    edges_collided_summed: int = 0
    citations_reparented: int = 0
    aliases_repointed: int = 0
    scrutiny_repointed: int = 0
    refusals: list[dict[str, str]] = field(default_factory=list)


async def apply_plan(plan: MergePlan) -> ApplyStats:
    """Execute the planned merges, one transaction per pair."""
    stats = ApplyStats()
    sm = get_sessionmaker()
    for pair in plan.pairs:
        async with sm() as session:
            try:
                result = await merge_two_canonicals(
                    session, keep_id=pair.member_id, drop_id=pair.fragment_id
                )
                if result.refused:
                    await session.rollback()
                    stats.refused += 1
                    stats.refusals.append(
                        {
                            "member": pair.member_name,
                            "fragment": pair.fragment_name,
                            "reason": result.refused_reason,
                        }
                    )
                    continue
                await session.commit()
                stats.merged += 1
                stats.edges_repointed += result.edges_repointed
                stats.edges_collided_summed += result.edges_collided_summed
                stats.citations_reparented += result.citations_reparented
                stats.aliases_repointed += result.aliases_repointed
                stats.scrutiny_repointed += result.scrutiny_repointed
            except ScrutinyRepointFailed as exc:
                await session.rollback()
                stats.refused += 1
                stats.refusals.append(
                    {
                        "member": pair.member_name,
                        "fragment": pair.fragment_name,
                        "reason": f"scrutiny_repoint_failed: {exc}",
                    }
                )
            except Exception as exc:  # noqa: BLE001
                await session.rollback()
                stats.refused += 1
                stats.refusals.append(
                    {
                        "member": pair.member_name,
                        "fragment": pair.fragment_name,
                        "reason": f"{type(exc).__name__}: {exc}",
                    }
                )
                logger.exception(
                    "merge failed %s ← %s", pair.member_name, pair.fragment_name
                )
    return stats


async def check_invariants(session: AsyncSession) -> dict:
    """The invariants helen validates — run before AND after."""
    q = lambda s: session.execute(text(s))  # noqa: E731
    return {
        "entities": (
            await q("select count(*) from canonical_entities")
        ).scalar_one(),
        "edges": (await q("select count(*) from canonical_edges")).scalar_one(),
        "citations": (
            await q("select count(*) from source_citations")
        ).scalar_one(),
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
        "surface_mode_counts": dict(
            (
                await q(
                    "select surface_mode, count(*) from canonical_entities "
                    "group by 1"
                )
            ).all()
        ),
        "scrutiny_rows": (
            await q("select count(*) from scrutiny_decisions")
        ).scalar_one(),
        "members": (
            await q(
                "select count(distinct canonical_id) from entity_aliases "
                "where source_system='bioguide'"
            )
        ).scalar_one(),
    }


async def run(apply: bool) -> dict:
    """Plan (always) and apply (only with ``apply=True``)."""
    sm = get_sessionmaker()
    report: dict = {"mode": "apply" if apply else "dry-run"}
    async with sm() as session:
        if not apply:
            # Physically incapable of writing: a stray INSERT/UPDATE/
            # DELETE raises 25006 instead of touching live public data.
            await session.execute(text("set transaction read only"))
            report["dry_run_transaction_read_only"] = (
                await session.execute(text("show transaction_read_only"))
            ).scalar_one()
        report["invariants_before"] = await check_invariants(session)
        members, variants, by_norm = await _load(session)
        plan = build_merge_plan(members, variants, by_norm)

    report["members_loaded"] = len(members)
    report["planned_merges"] = len(plan.pairs)
    report["skipped_by_reason"] = plan.skipped_by_reason
    report["surface_mode_straddles_skipped"] = plan.skipped_by_reason.get(
        "surface_mode_straddle", 0
    )
    report["pairs"] = [p.__dict__ for p in plan.pairs]
    report["skipped_sample"] = [s.to_dict() for s in plan.skipped[:60]]

    if apply:
        stats = await apply_plan(plan)
        report["apply_stats"] = stats.__dict__
        async with sm() as session:
            after = await check_invariants(session)
        report["invariants_after"] = after
        before = report["invariants_before"]
        report["postconditions"] = {
            "uncited_edges_still_zero": after["uncited_edges"] == 0,
            "no_orphan_citations": after["orphan_citations"] == 0,
            "members_preserved": after["members"] == before["members"],
            "scrutiny_rows_preserved": (
                after["scrutiny_rows"] >= before["scrutiny_rows"]
            ),
            "no_protected_node_lost": all(
                after["surface_mode_counts"].get(mode, 0)
                == before["surface_mode_counts"].get(mode, 0)
                for mode in ("suppress", "alias")
            ),
            "entities_only_decreased": (
                after["entities"] <= before["entities"]
            ),
        }
        report["postconditions"]["all_passed"] = all(
            report["postconditions"].values()
        )
    return report


def main() -> None:
    """CLI — ``python -m app.services.ingest.congress_person_merge``."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    ap = argparse.ArgumentParser(
        description="Merge fragmented news-person nodes onto the member"
    )
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True)
    g.add_argument(
        "--apply", action="store_true", help="DESTRUCTIVE — execute the merges"
    )
    ap.add_argument("--json-report", default=None)
    args = ap.parse_args()

    report = asyncio.run(run(apply=args.apply))
    print(json.dumps(report, indent=2, default=str))
    if args.json_report:
        with open(args.json_report, "w") as fh:
            json.dump(report, fh, indent=2, default=str)


if __name__ == "__main__":
    main()
