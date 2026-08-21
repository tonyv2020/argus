"""P1.6.3 — collapse a domain's fragments onto its anchors + fix mis-types.

The surveillance domain arrived in the graph incidentally, from three
pipelines that never agreed on a name:

* **news tags** — ``AXON`` (6 edges) and ``Axon Enterprise`` (26) are two
  organizations; ``Flock`` and ``Flock Safety`` are two more; ``Anduril``
  is typed ``concept``.
* **Senate LDA** — the ingester mints one canonical per ``client.name``
  string, and filers register wrapper strings: ``BROWNSTEIN HYATT FARBER
  SCHRECK LLP OBO PALANTIR TECHNOLOGIES INC.``, ``J.A. GREEN AND COMPANY
  (FOR PALANTIR TECHNOLOGIES INC.)``, ``BGR GOVERNMENT AFFAIRS, LLC ON
  BEHALF OF FLOCK SAFETY``. Each is a real client record for the anchor
  and each carries real cited ``lobbies`` edges to a DIFFERENT registrant
  — 20 cited filings that never reach Palantir's or Flock's profile.
* **SEC / FEC** — OCR mangles of the issuer name, already collapsed by
  the P2 dedup pass.

This pass reuses the P2 merge machinery —
:func:`app.services.ingest.merge_canonicals.merge_two_canonicals`
re-points every edge, citation, alias, anchor and scrutiny row onto the
survivor and REFUSES rather than degrades when the privacy-audit rows
cannot move. What is new is the candidate rule.

Candidate rule — declared, never inferred
-----------------------------------------
The anchor is always the survivor. A fragment qualifies on exactly two
kinds of evidence, both DECLARED in ``domain_anchors``:

1. its normalized name equals one of the anchor's ``domain.anchor``
   name variants, or
2. its normalized name matches one of the anchor's
   ``lda_client_patterns`` — the same anchored regexes that gate the LDA
   ingest, so a node the LDA ingester created from this company's client
   record collapses onto the company.

Nothing else qualifies. A near-name ("Flock Cameras", "Palantir
Foundry", "Joby Axon") is a different thing and is left alone.

Fail-closed refusals, in order
------------------------------
* **surface_mode straddle** — a pair whose members differ is NEVER
  merged, and there is no flag to override it. A pair where the fragment
  is protected at all is refused even if the anchor is too: consolidating
  a protected identity is not this pass's call.
* **publication_state straddle** — same partition, so a merge can never
  silently publish staged content or stage published content.
* **foreign external id** — a fragment carrying an authoritative id
  (a CIK, a UEI, an FEC id, a bioguide) that the anchor did not declare
  names a different real entity.
* **type not mergeable** — ``organization``/``unknown``/``concept`` for
  an organization anchor (the documented news-tag mis-typing); nothing
  else.
* **claimed by two anchors** — ambiguity is refused, not guessed.

Mis-types
---------
Separately from merging, a canonical carrying an authoritative external
id whose namespace PINS a type (``sec.cik`` and ``usaspending.uei`` →
organization, ``sec.owner_cik`` → person, ``fec.committee`` → pac) but
typed something weaker (``unknown``, ``concept``, ``event``, ``topic``)
is retyped. This is the P2 "never guess — only a reliable signal" rule
applied as a repair: the id is the evidence, and the pass refuses to
DOWNGRADE a type or to touch a canonical with no such id.

Dry-run is the default and it is read-only at the transaction level:
``SET TRANSACTION READ ONLY`` makes a stray write raise 25006 rather
than touch live public data. ``--apply`` is the only mode that writes.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import re
from collections import defaultdict
from dataclasses import dataclass, field

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import (
    AnchorRegistry,
    CanonicalEntity,
    EntityAlias,
    EntityType,
    SurfaceMode,
)
from app.services.graph.base import normalize_name
from app.services.ingest.dedup_pass import _name_is_evidence
from app.services.ingest.domain_anchors import (
    AUTHORITATIVE_NAMESPACES,
    SEC_OWNER_NAMESPACE,
    USASPENDING_UEI_NAMESPACE,
    VARIANT_NAMESPACE,
)
from app.services.ingest.merge_canonicals import (
    ScrutinyRepointFailed,
    merge_two_canonicals,
)

logger = logging.getLogger(__name__)

#: Fragment types an ORGANIZATION anchor may absorb. ``unknown`` because
#: 32% of the registry is typed that way; ``concept`` because the
#: news-tag pipeline routinely types a company as one — the same
#: organization/concept pair the P2 dedup pass already merges.
_ORG_MERGEABLE_TYPES = frozenset(
    {
        EntityType.ORGANIZATION.value,
        EntityType.UNKNOWN.value,
        EntityType.CONCEPT.value,
    }
)
_PERSON_MERGEABLE_TYPES = frozenset(
    {EntityType.PERSON.value, EntityType.UNKNOWN.value}
)


def mergeable_types_for(anchor_type: str) -> frozenset[str]:
    """Fragment types an anchor of this type may absorb."""
    if anchor_type == EntityType.ORGANIZATION.value:
        return _ORG_MERGEABLE_TYPES
    if anchor_type == EntityType.PERSON.value:
        return _PERSON_MERGEABLE_TYPES
    return frozenset({anchor_type, EntityType.UNKNOWN.value})


#: An authoritative id in these namespaces PINS the canonical's type.
#: Mirrors ``dedup_pass._NAMESPACE_TYPE`` and adds the two P1.6
#: namespaces.
NAMESPACE_PINS_TYPE: dict[str, str] = {
    "sec.cik": EntityType.ORGANIZATION.value,
    USASPENDING_UEI_NAMESPACE: EntityType.ORGANIZATION.value,
    SEC_OWNER_NAMESPACE: EntityType.PERSON.value,
    "fec.committee": EntityType.PAC.value,
    "fec.candidate": EntityType.PERSON.value,
    "bioguide": EntityType.PERSON.value,
}

#: Types weak enough that a pinning external id may overwrite them. A
#: real, specific type (``person``, ``pac``, ``agency``) is NEVER
#: overwritten here — that is a conflict for an operator, not a repair.
RETYPEABLE_FROM: frozenset[str] = frozenset(
    {
        EntityType.UNKNOWN.value,
        EntityType.CONCEPT.value,
        EntityType.EVENT.value,
        EntityType.TOPIC.value,
    }
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
class AnchorNode:
    """One resolved anchor + the evidence it may claim fragments with."""

    label: str
    node: Node
    entity_type: str
    variants: frozenset[str]
    client_patterns: tuple[str, ...]


@dataclass(frozen=True)
class MergePair:
    """A planned merge — the anchor always survives."""

    anchor: str
    anchor_id: str
    fragment_id: str
    fragment_name: str
    fragment_type: str
    matched_on: str
    rule: str
    fragment_edges: int
    fragment_aliases: int


@dataclass(frozen=True)
class SkippedPair:
    """A refused candidate + the rule that refused it."""

    anchor: str
    anchor_id: str
    fragment_id: str
    fragment_name: str
    reason: str
    detail: str = ""

    def to_dict(self) -> dict:
        """JSON-able row for the operator review list."""
        return {
            "anchor": self.anchor,
            "anchor_id": self.anchor_id,
            "fragment_id": self.fragment_id,
            "fragment": self.fragment_name,
            "reason": self.reason,
            "detail": self.detail,
        }


@dataclass(frozen=True)
class RetypePlan:
    """One canonical whose authoritative id disagrees with its type."""

    canonical_id: str
    name: str
    from_type: str
    to_type: str
    evidence: str

    def to_dict(self) -> dict:
        """JSON-able row for the report."""
        return {
            "canonical_id": self.canonical_id,
            "name": self.name,
            "from_type": self.from_type,
            "to_type": self.to_type,
            "evidence": self.evidence,
        }


@dataclass
class MergePlan:
    """What one planning pass decided."""

    pairs: list[MergePair] = field(default_factory=list)
    skipped: list[SkippedPair] = field(default_factory=list)
    retypes: list[RetypePlan] = field(default_factory=list)

    @property
    def skipped_by_reason(self) -> dict[str, int]:
        """Refusal histogram — the report's headline safety number."""
        out: dict[str, int] = {}
        for s in self.skipped:
            out[s.reason] = out.get(s.reason, 0) + 1
        return out


# ─── planning (pure) ────────────────────────────────────────────────────


def _fragment_matches(
    node: Node, anchor: AnchorNode
) -> tuple[bool, str, str]:
    """Does ``node`` match ``anchor``'s declared evidence?

    Returns ``(matched, rule, detail)``. Name variants are compared
    exactly on the normalized form; client patterns are the SAME
    anchored regexes the LDA ingest gate uses, so what merges is
    precisely what would have been ingested onto the anchor.
    """
    if node.norm and node.norm in anchor.variants:
        return True, "name_variant", node.norm
    for pattern in anchor.client_patterns:
        if node.norm and re.search(pattern, node.norm):
            return True, "lda_client_pattern", pattern
    return False, "", ""


def build_merge_plan(
    anchors: list[AnchorNode], nodes: dict[str, Node]
) -> MergePlan:
    """Decide which fragments collapse onto which anchor.

    A fragment reachable from two different anchors is refused against
    both — ambiguity is never resolved by iteration order.
    """
    plan = MergePlan()
    anchor_ids = {a.node.id for a in anchors}

    # Pass 1 — who claims what.
    claims: dict[str, list[tuple[AnchorNode, str, str]]] = defaultdict(list)
    for anchor in anchors:
        for node in nodes.values():
            if node.id in anchor_ids:
                continue
            if not _name_is_evidence(node.norm):
                continue
            matched, rule, detail = _fragment_matches(node, anchor)
            if matched:
                claims[node.id].append((anchor, rule, detail))

    # Pass 2 — apply the fail-closed gates, in a fixed order so the
    # report buckets a pair by the FIRST rule that refused it.
    for fragment_id, claimants in sorted(claims.items()):
        node = nodes[fragment_id]
        if len(claimants) > 1:
            for anchor, _rule, _detail in claimants:
                plan.skipped.append(
                    SkippedPair(
                        anchor.label, anchor.node.id, node.id, node.name,
                        "claimed_by_multiple_anchors",
                        ", ".join(sorted(a.label for a, _, _ in claimants)),
                    )
                )
            continue
        anchor, rule, detail = claimants[0]

        # ── THE NON-NEGOTIABLE RULE ─────────────────────────────────
        if node.surface_mode != anchor.node.surface_mode:
            plan.skipped.append(
                SkippedPair(
                    anchor.label, anchor.node.id, node.id, node.name,
                    "surface_mode_straddle",
                    f"anchor={anchor.node.surface_mode} "
                    f"fragment={node.surface_mode}",
                )
            )
            continue
        if node.surface_mode != SurfaceMode.OPEN.value:
            plan.skipped.append(
                SkippedPair(
                    anchor.label, anchor.node.id, node.id, node.name,
                    "both_protected", node.surface_mode,
                )
            )
            continue
        if node.publication_state != anchor.node.publication_state:
            plan.skipped.append(
                SkippedPair(
                    anchor.label, anchor.node.id, node.id, node.name,
                    "publication_state_straddle",
                    f"anchor={anchor.node.publication_state} "
                    f"fragment={node.publication_state}",
                )
            )
            continue
        foreign = node.namespaces & AUTHORITATIVE_NAMESPACES
        if foreign:
            plan.skipped.append(
                SkippedPair(
                    anchor.label, anchor.node.id, node.id, node.name,
                    "foreign_external_id", sorted(foreign)[0],
                )
            )
            continue
        if node.type not in mergeable_types_for(anchor.entity_type):
            plan.skipped.append(
                SkippedPair(
                    anchor.label, anchor.node.id, node.id, node.name,
                    "type_not_mergeable",
                    f"{node.type} → {anchor.entity_type}",
                )
            )
            continue

        plan.pairs.append(
            MergePair(
                anchor=anchor.label,
                anchor_id=anchor.node.id,
                fragment_id=node.id,
                fragment_name=node.name,
                fragment_type=node.type,
                matched_on=detail,
                rule=rule,
                fragment_edges=node.edge_count,
                fragment_aliases=node.alias_count,
            )
        )
    return plan


def build_retype_plan(
    nodes: dict[str, Node], pins: dict[str, set[str]]
) -> list[RetypePlan]:
    """Canonicals whose authoritative id disagrees with their type.

    ``pins`` maps canonical id → the set of TYPES its authoritative ids
    pin. A canonical whose ids pin two different types is a conflict, not
    a repair, and is left for an operator. A canonical already carrying a
    real, specific type is never downgraded.
    """
    out: list[RetypePlan] = []
    for canonical_id, pinned in sorted(pins.items()):
        node = nodes.get(canonical_id)
        if node is None or len(pinned) != 1:
            continue
        target = next(iter(pinned))
        if node.type == target:
            continue
        if node.type not in RETYPEABLE_FROM:
            continue
        out.append(
            RetypePlan(
                canonical_id=node.id,
                name=node.name,
                from_type=node.type,
                to_type=target,
                evidence=",".join(
                    sorted(node.namespaces & set(NAMESPACE_PINS_TYPE))
                ),
            )
        )
    return out


# ─── loading ────────────────────────────────────────────────────────────


async def _load(
    session: AsyncSession, domain: str
) -> tuple[list[AnchorNode], dict[str, Node], dict[str, set[str]]]:
    """Load the domain's anchors, every canonical, and the type pins."""
    ns_rows = (
        await session.execute(
            select(EntityAlias.canonical_id, EntityAlias.source_system).where(
                EntityAlias.source_system.in_(sorted(AUTHORITATIVE_NAMESPACES))
            )
        )
    ).all()
    namespaces: dict[str, set[str]] = defaultdict(set)
    for canonical_id, source_system in ns_rows:
        namespaces[canonical_id].add(source_system)

    edge_counts = dict(
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
                text("select canonical_id, count(*) from entity_aliases group by 1")
            )
        ).all()
    )

    nodes: dict[str, Node] = {}
    for ent in (await session.execute(select(CanonicalEntity))).scalars():
        nodes[ent.id] = Node(
            id=ent.id,
            name=ent.canonical_name,
            norm=ent.canonical_name_normalized,
            type=ent.type,
            surface_mode=ent.surface_mode,
            publication_state=ent.publication_state,
            edge_count=edge_counts.get(ent.id, 0),
            alias_count=alias_counts.get(ent.id, 0),
            namespaces=frozenset(namespaces.get(ent.id, ())),
        )

    # Anchors for this domain, with their declared evidence.
    anchor_rows = (
        await session.execute(
            select(AnchorRegistry).where(
                AnchorRegistry.priority_domain == domain
            )
        )
    ).scalars().all()
    variant_rows = (
        await session.execute(
            select(
                EntityAlias.canonical_id, EntityAlias.surface_name_normalized
            ).where(EntityAlias.source_system == VARIANT_NAMESPACE)
        )
    ).all()
    variants_by_canonical: dict[str, set[str]] = defaultdict(set)
    for canonical_id, surface_norm in variant_rows:
        if surface_norm:
            variants_by_canonical[canonical_id].add(normalize_name(surface_norm))

    anchors: list[AnchorNode] = []
    for row in anchor_rows:
        if not row.canonical_id or row.canonical_id not in nodes:
            continue
        node = nodes[row.canonical_id]
        variants = set(variants_by_canonical.get(row.canonical_id, set()))
        variants.add(node.norm)
        for v in row.name_variants or []:
            n = normalize_name(str(v))
            if n:
                variants.add(n)
        patterns = tuple(
            str(p) for p in ((row.external_ids or {}).get("lda_client_patterns") or [])
        )
        anchors.append(
            AnchorNode(
                label=row.label,
                node=node,
                entity_type=row.entity_type,
                variants=frozenset(v for v in variants if _name_is_evidence(v)),
                client_patterns=patterns,
            )
        )

    # Type pins — only for canonicals in this domain's blast radius:
    # the anchors themselves plus anything they could claim. Retyping
    # the whole corpus is not this pass's job.
    pins: dict[str, set[str]] = defaultdict(set)
    for canonical_id, systems in namespaces.items():
        for system in systems:
            pinned = NAMESPACE_PINS_TYPE.get(system)
            if pinned:
                pins[canonical_id].add(pinned)
    return anchors, nodes, dict(pins)


def _domain_scope(
    anchors: list[AnchorNode], nodes: dict[str, Node], plan: MergePlan
) -> set[str]:
    """Canonical ids this pass is allowed to retype: the anchors and the
    fragments it is merging onto them. Nothing else in the corpus."""
    scope = {a.node.id for a in anchors}
    scope |= {p.fragment_id for p in plan.pairs}
    del nodes
    return scope


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
    retyped: int = 0
    refusals: list[dict] = field(default_factory=list)


async def apply_plan(plan: MergePlan) -> ApplyStats:
    """Execute the planned merges + retypes, one transaction per item."""
    stats = ApplyStats()
    sm = get_sessionmaker()
    for pair in plan.pairs:
        async with sm() as session:
            try:
                result = await merge_two_canonicals(
                    session, keep_id=pair.anchor_id, drop_id=pair.fragment_id
                )
                if result.refused:
                    await session.rollback()
                    stats.refused += 1
                    stats.refusals.append(
                        {
                            "anchor": pair.anchor,
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
                        "anchor": pair.anchor,
                        "fragment": pair.fragment_name,
                        "reason": f"scrutiny_repoint_failed: {exc}",
                    }
                )
            except Exception as exc:  # noqa: BLE001
                await session.rollback()
                stats.refused += 1
                stats.refusals.append(
                    {
                        "anchor": pair.anchor,
                        "fragment": pair.fragment_name,
                        "reason": f"{type(exc).__name__}: {exc}",
                    }
                )
                logger.exception(
                    "merge failed %s ← %s", pair.anchor, pair.fragment_name
                )

    for retype in plan.retypes:
        async with sm() as session:
            try:
                ent = (
                    await session.execute(
                        select(CanonicalEntity).where(
                            CanonicalEntity.id == retype.canonical_id
                        )
                    )
                ).scalar_one_or_none()
                # Re-check under the write transaction: a merge above may
                # have deleted or already retyped this canonical.
                if ent is None or ent.type not in RETYPEABLE_FROM:
                    continue
                ent.type = retype.to_type
                await session.commit()
                stats.retyped += 1
            except Exception as exc:  # noqa: BLE001
                await session.rollback()
                stats.refusals.append(
                    {
                        "retype": retype.name,
                        "reason": f"{type(exc).__name__}: {exc}",
                    }
                )
    return stats


# ─── invariants ─────────────────────────────────────────────────────────


async def check_invariants(session: AsyncSession) -> dict:
    """The invariants helen validates — run before AND after."""
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
        "surface_mode_counts": dict(
            (
                await q(
                    "select surface_mode, count(*) from canonical_entities group by 1"
                )
            ).all()
        ),
        "scrutiny_rows": (
            await q("select count(*) from scrutiny_decisions")
        ).scalar_one(),
        "anchored_canonicals": (
            await q(
                "select count(*) from anchor_registry "
                "where canonical_id is not null"
            )
        ).scalar_one(),
    }


def _check_postconditions(before: dict, after: dict) -> dict:
    """Machine-checked postconditions — what helen reads first."""
    out = {
        "uncited_edges_still_zero": after["uncited_edges"] == 0,
        "no_orphan_citations": after["orphan_citations"] == 0,
        "citations_preserved": after["citations"] >= before["citations"],
        "no_protected_node_lost": all(
            after["surface_mode_counts"].get(mode, 0)
            == before["surface_mode_counts"].get(mode, 0)
            for mode in ("suppress", "alias")
        ),
        "scrutiny_rows_preserved": after["scrutiny_rows"] >= before["scrutiny_rows"],
        "entities_only_decreased": after["entities"] <= before["entities"],
        "anchors_still_resolved": (
            after["anchored_canonicals"] >= before["anchored_canonicals"]
        ),
    }
    out["all_passed"] = all(out.values())
    return out


# ─── CLI ────────────────────────────────────────────────────────────────


async def run(domain: str, apply: bool) -> dict:
    """Plan (always) and apply (only with ``apply=True``)."""
    sm = get_sessionmaker()
    report: dict = {"domain": domain, "mode": "apply" if apply else "dry-run"}
    async with sm() as session:
        if not apply:
            # Physically incapable of writing: a stray INSERT/UPDATE/
            # DELETE raises 25006 instead of touching live public data.
            await session.execute(text("set transaction read only"))
            report["dry_run_transaction_read_only"] = (
                await session.execute(text("show transaction_read_only"))
            ).scalar_one()
        report["invariants_before"] = await check_invariants(session)
        anchors, nodes, pins = await _load(session, domain)
        plan = build_merge_plan(anchors, nodes)
        scope = _domain_scope(anchors, nodes, plan)
        plan.retypes = [
            r
            for r in build_retype_plan(nodes, pins)
            if r.canonical_id in scope
        ]

    report["anchors_loaded"] = [
        {
            "anchor": a.label,
            "canonical_id": a.node.id,
            "canonical_name": a.node.name,
            "type": a.node.type,
            "surface_mode": a.node.surface_mode,
            "publication_state": a.node.publication_state,
            "variants": sorted(a.variants),
            "client_patterns": list(a.client_patterns),
        }
        for a in anchors
    ]
    report["planned_merges"] = len(plan.pairs)
    report["pairs"] = [p.__dict__ for p in plan.pairs]
    report["planned_retypes"] = len(plan.retypes)
    report["retypes"] = [r.to_dict() for r in plan.retypes]
    report["skipped_by_reason"] = plan.skipped_by_reason
    report["surface_mode_straddles_skipped"] = plan.skipped_by_reason.get(
        "surface_mode_straddle", 0
    )
    report["skipped"] = [s.to_dict() for s in plan.skipped[:80]]

    if apply:
        stats = await apply_plan(plan)
        report["apply_stats"] = stats.__dict__
        async with sm() as session:
            after = await check_invariants(session)
        report["invariants_after"] = after
        report["postconditions"] = _check_postconditions(
            report["invariants_before"], after
        )
    return report


def main() -> None:
    """CLI — ``python -m app.services.ingest.domain_merge``."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    ap = argparse.ArgumentParser(
        description="Collapse a domain's fragments onto its anchors"
    )
    ap.add_argument("--domain", default="surveillance")
    g = ap.add_mutually_exclusive_group()
    g.add_argument("--dry-run", action="store_true", default=True)
    g.add_argument(
        "--apply", action="store_true", help="DESTRUCTIVE — execute the merges"
    )
    ap.add_argument("--json-report", default=None)
    args = ap.parse_args()

    report = asyncio.run(run(args.domain, apply=args.apply))
    print(json.dumps(report, indent=2, default=str))
    if args.json_report:
        with open(args.json_report, "w") as fh:
            json.dump(report, fh, indent=2, default=str)


if __name__ == "__main__":
    main()
