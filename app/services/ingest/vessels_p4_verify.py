"""Vessels P4 — the fail-closed proof for the all-135 publish.

Asserting "nothing leaked" is not evidence. This is the re-runnable
four-part proof the read-gate standard requires, and it is deliberately
NOT a unit test: it runs against **live Postgres, the live HTTP surface
and live Neo4j**, because those are the three places a reader can reach.

  1. **What surfaced** — the cohort is published in PG, and each owner's
     LIVE dossier actually returns its vessels WITH an OFAC citation.
  2. **What must not** — every owner-PII value in the vessels table and
     every held individual owner's name is searched for, in two scopes:
     the ``vessels[]`` payload each dossier returns (zero occurrences,
     no exemptions — that array IS the panel), and whole response bodies
     (one exempt class, a sanctioned owner's own published label, and
     every exemption is printed).
  3. **What stayed dark** — the individual and long-tail owners OFAC
     itself names are re-derived from the source XML and checked against
     PG **by OFAC id**, not by name: an unrelated canonical that merely
     shares a name is a collision, and is reported with the evidence
     (no OFAC alias, no vessel) rather than counted either way.
  4. **What was untouched** — the aircraft layer's published counts.

Plus the Neo4j surface: ``Vessel`` node property keys are compared
against the allowlist, so a projector that started carrying a new
column fails here rather than in a Cypher session.

Exit code is non-zero if any check fails, so a Job going Complete is
itself part of the proof.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import json
import logging
import os
import sys

import httpx
from sqlalchemy import func, select

from app.db import get_sessionmaker
from app.models import (
    Aircraft,
    AircraftRegistrationEdge,
    CanonicalEntity,
    EntityAlias,
    PublicationState,
    SurfaceMode,
    Vessel,
    VesselOwnershipEdge,
    VesselPromotionAudit,
)
from app.services.graph.base import normalize_name

logger = logging.getLogger(__name__)

API = os.environ.get("ARGUS_API_URL", "http://api.argus.svc.cluster.local")

#: Vessel columns that identify a person or where they live. Every value
#: any vessel row holds in these is searched for in every live body.
PII_COLUMNS = (
    "owner_name_raw", "owner_street", "owner_city",
    "owner_state", "owner_postal_code", "owner_country",
)

#: The ONLY property keys a ``Vessel`` node may carry. ``pg_id`` is the
#: MERGE key; the other three are the published facts.
VESSEL_NODE_KEYS = {"pg_id", "vessel_name", "imo", "flag"}

#: Owner counts the publish was approved for. Checked, not assumed.
EXPECTED = {"owners": 135, "edges": 1076, "vessels": 1066}
SPOT_CHECKS = {
    "ISLAMIC REPUBLIC OF IRAN SHIPPING LINES": 121,
    "JOINT STOCK COMPANY SOVCOMFLOT": 81,
    "PDVSA": 40,
}


class Result:
    """Accumulates pass/fail lines so one run reports every failure."""

    def __init__(self) -> None:
        self.lines: list[tuple[bool, str]] = []

    def check(self, ok: bool, label: str, detail: str = "") -> bool:
        self.lines.append((bool(ok), f"{label}{(' — ' + detail) if detail else ''}"))
        return bool(ok)

    @property
    def failed(self) -> list[str]:
        return [t for ok, t in self.lines if not ok]

    def report(self) -> int:
        print("\n=== VESSELS P4 PUBLISH — LIVE VERIFICATION ===")
        for ok, text in self.lines:
            print(f"  [{'PASS' if ok else 'FAIL'}] {text}")
        print(f"\n  {len(self.lines) - len(self.failed)}/{len(self.lines)} checks passed")
        return 1 if self.failed else 0


async def _pg_state(session) -> dict:
    """Everything the DB says about the two asset layers."""
    edges = (
        await session.execute(
            select(VesselOwnershipEdge, Vessel, CanonicalEntity)
            .join(Vessel, Vessel.id == VesselOwnershipEdge.vessel_id)
            .join(CanonicalEntity, CanonicalEntity.id == VesselOwnershipEdge.canonical_id)
        )
    ).all()
    pub = [
        (e, v, c)
        for e, v, c in edges
        if e.publication_state == PublicationState.PUBLISHED.value
        and e.surface_mode != SurfaceMode.SUPPRESS.value
        and v.publication_state == PublicationState.PUBLISHED.value
        and v.surface_mode != SurfaceMode.SUPPRESS.value
        and c.publication_state == PublicationState.PUBLISHED.value
        and c.surface_mode != SurfaceMode.SUPPRESS.value
    ]
    by_owner: dict[str, int] = collections.Counter()
    for _e, _v, c in pub:
        by_owner[c.canonical_name] += 1

    pii_values: set[str] = set()
    for row in (await session.execute(select(Vessel))).scalars():
        for col in PII_COLUMNS:
            val = (getattr(row, col, None) or "").strip()
            if val:
                pii_values.add(val)

    return {
        "edges_total": len(edges),
        "published_edges": len(pub),
        "published_owners": len({c.id for _e, _v, c in pub}),
        "published_vessels": len({v.id for _e, v, _c in pub}),
        "by_owner": dict(by_owner),
        "owner_ids": {c.canonical_name: c.id for _e, _v, c in pub},
        "pii_values": pii_values,
        "vessels_published_total": await session.scalar(
            select(func.count(Vessel.id)).where(
                Vessel.publication_state == PublicationState.PUBLISHED.value
            )
        ),
        "aircraft_published": await session.scalar(
            select(func.count(Aircraft.id)).where(
                Aircraft.publication_state == PublicationState.PUBLISHED.value,
                Aircraft.surface_mode != SurfaceMode.SUPPRESS.value,
            )
        ),
        "aircraft_edges_published": await session.scalar(
            select(func.count(AircraftRegistrationEdge.id)).where(
                AircraftRegistrationEdge.publication_state
                == PublicationState.PUBLISHED.value,
                AircraftRegistrationEdge.surface_mode != SurfaceMode.SUPPRESS.value,
            )
        ),
        "aircraft_person_entities": await session.scalar(
            select(func.count(func.distinct(CanonicalEntity.id)))
            .select_from(AircraftRegistrationEdge)
            .join(CanonicalEntity, CanonicalEntity.id == AircraftRegistrationEdge.canonical_id)
            .where(
                AircraftRegistrationEdge.publication_state
                == PublicationState.PUBLISHED.value,
                CanonicalEntity.type == "person",
            )
        ),
        "audit_promote": await session.scalar(
            select(func.count(VesselPromotionAudit.id)).where(
                VesselPromotionAudit.action == "promote"
            )
        ),
        "audit_unattributed": await session.scalar(
            select(func.count(VesselPromotionAudit.id)).where(
                (func.length(func.trim(VesselPromotionAudit.actor)) == 0)
                | (func.length(func.trim(VesselPromotionAudit.reason)) == 0)
            )
        ),
    }


async def _held_owners_are_dark(session, res: Result, cache: str | None) -> dict:
    """Re-derive the held classes from OFAC and prove PG never published them.

    Source of truth is the SDN XML, not our own notes about it: an owner
    we forgot to hold would still show up here.
    """
    try:
        from app.services.ingest.ofac_vessel_owner_dryrun import download, parse_entities
        from app.services.ingest.vessels_p3_apply import CUTOFF

        entities, links = parse_entities(download(cache=cache))
    except Exception as exc:  # noqa: BLE001
        res.check(False, "held-class re-derivation from OFAC", f"could not parse source: {exc}")
        return {}

    by_owner: dict[str, set[str]] = collections.defaultdict(set)
    for vid, _vname, _rtype, oid, _oname in links:
        by_owner[oid].add(vid)

    individuals: dict[str, str] = {}
    longtail: dict[str, str] = {}
    for oid, vessels in by_owner.items():
        otype, oname = entities.get(oid, ("?", None))
        name = (oname or "").strip()
        if not name:
            continue
        if otype == "Individual":
            individuals[oid] = name
        elif len(vessels) < CUTOFF:
            longtail[oid] = name

    res.check(
        len(individuals) == 21,
        "OFAC still names 21 individual vessel owners",
        f"found {len(individuals)}",
    )
    res.check(
        len(longtail) == 420,
        f"OFAC still names 420 sub-cutoff (<{CUTOFF} vessel) owners",
        f"found {len(longtail)}",
    )

    # Held owners must have no OFAC identity in Argus and no vessel edge.
    #
    # IDENTITY, NOT NAME. The link between an OFAC owner and an Argus
    # canonical is the ``ofac.sdn`` alias carrying the OFAC id — that is
    # what vessels P3 wrote, and it is the only thing that means "this
    # canonical IS that owner". Matching on the normalized name instead
    # produces false positives: an unrelated published canonical named
    # "Patriot" (type ``unknown``, from hollywood.entity_tags, no OFAC
    # alias, no vessel edge) collides with a held long-tail owner of the
    # same name and looks like a leak that isn't one.
    held = {**individuals, **longtail}
    alias_rows = (
        await session.execute(
            select(EntityAlias.source_id, CanonicalEntity)
            .join(CanonicalEntity, CanonicalEntity.id == EntityAlias.canonical_id)
            .where(EntityAlias.source_system == "ofac.sdn")
            .where(EntityAlias.source_id.in_(list(held)))
        )
    ).all()
    res.check(
        not alias_rows,
        "no held individual or long-tail owner is identified by an OFAC alias in Argus",
        f"{len(alias_rows)} linked",
    )

    norms = {normalize_name(n): n for n in held.values() if n}
    rows = (
        await session.execute(
            select(CanonicalEntity, func.count(VesselOwnershipEdge.id))
            .outerjoin(
                VesselOwnershipEdge,
                VesselOwnershipEdge.canonical_id == CanonicalEntity.id,
            )
            .where(CanonicalEntity.canonical_name_normalized.in_(list(norms)))
            .group_by(CanonicalEntity.id)
        )
    ).all()
    with_edges = [c.canonical_name for c, n in rows if n]
    res.check(
        not with_edges,
        "no canonical name-matching a held owner has a vessel edge",
        f"{len(with_edges)} with edges: {with_edges[:5]}",
    )
    # Same-name canonicals that ARE published are reported, never assumed
    # innocent: each one is asserted to carry no OFAC identity and no
    # vessel, which is what makes it a collision rather than a publish.
    collisions = [
        c.canonical_name
        for c, n in rows
        if c.publication_state == PublicationState.PUBLISHED.value
        and c.surface_mode != SurfaceMode.SUPPRESS.value
        and not n
    ]
    if collisions:
        print(f"  note: {len(collisions)} published name-collision(s), no OFAC id, "
              f"no vessel: {collisions}")

    return individuals


async def _live_surface(state: dict, individuals: dict, res: Result) -> None:
    """Fetch every published owner's LIVE dossier + web page and scan them.

    The web panel is client-rendered from ``entity.vessels`` in the same
    JSON, so the JSON body IS the panel's content; the HTML page is
    fetched too, to prove the route serves and to widen the PII scan.
    """
    bodies: list[tuple[str, str]] = []
    #: The vessel surface in isolation — the `vessels` array of every
    #: dossier, i.e. exactly the bytes the Assets—Vessels panel renders.
    #: Nothing in the PII corpus may appear HERE, no exemptions.
    vessel_payloads: list[tuple[str, str]] = []
    with_vessels = 0
    total_listed = 0
    uncited = 0
    spot: dict[str, int] = {}

    async with httpx.AsyncClient(base_url=API, timeout=30.0) as client:
        for name, cid in sorted(state["owner_ids"].items()):
            try:
                r = await client.get(f"/api/entities/{cid}")
            except Exception as exc:  # noqa: BLE001
                res.check(False, f"live dossier for {name[:40]}", str(exc))
                continue
            if r.status_code != 200:
                res.check(False, f"live dossier for {name[:40]}", f"HTTP {r.status_code}")
                continue
            bodies.append((f"api/entities/{name[:40]}", r.text))
            data = r.json()
            vessels = data.get("vessels") or []
            vessel_payloads.append((f"vessels[] of {name[:40]}", json.dumps(vessels)))
            if vessels:
                with_vessels += 1
            total_listed += len(vessels)
            if name in SPOT_CHECKS:
                spot[name] = len(vessels)
            for v in vessels:
                cit = v.get("citation") or {}
                if not (cit.get("url") and len(cit.get("sha256") or "") == 64):
                    uncited += 1

        # The HTML route, on the three spot-check entities.
        for name in SPOT_CHECKS:
            cid = state["owner_ids"].get(name)
            if not cid:
                continue
            h = await client.get(f"/entity/{cid}")
            res.check(
                h.status_code == 200 and "Assets — Vessels" in h.text,
                f"web dossier page serves the Assets—Vessels panel: {name[:36]}",
                f"HTTP {h.status_code}",
            )
            bodies.append((f"entity/{name[:36]}", h.text))

        # Search must not surface a held individual owner by name. Scan
        # the RESULTS only: /api/search echoes `q` back in its body, so
        # a whole-body scan reports our own query as a hit.
        leaked_search = []
        for oid, person in individuals.items():
            s = await client.get("/api/search", params={"q": person, "limit": 10})
            if s.status_code != 200:
                res.check(False, f"live search for a held owner ({oid})", f"HTTP {s.status_code}")
                continue
            hits = (s.json() or {}).get("results") or []
            bodies.append((f"search-results/{oid}", json.dumps(hits)))
            surname = person.split(",")[0].strip()
            for h in hits:
                label = h.get("label") or ""
                # Stricter than equality: any label CONTAINING the held
                # name — or their surname — is a hit worth failing on.
                if normalize_name(person) in normalize_name(label) or (
                    len(surname) > 3 and normalize_name(surname) in normalize_name(label)
                ):
                    leaked_search.append(f"{person!r} -> {label!r}")
        res.check(
            not leaked_search,
            f"live /api/search surfaces none of the {len(individuals)} held individual "
            "owners by name",
            f"{len(leaked_search)} leaked: {leaked_search[:5]}",
        )

    res.check(
        with_vessels == EXPECTED["owners"],
        f"all {EXPECTED['owners']} owners return a non-empty Vessels section LIVE",
        f"{with_vessels} did",
    )
    res.check(
        total_listed == EXPECTED["edges"],
        f"live dossiers list {EXPECTED['edges']} vessels in total",
        f"listed {total_listed}",
    )
    res.check(uncited == 0, "every live vessel row carries an OFAC url + sha256",
              f"{uncited} uncited")
    for name, want in SPOT_CHECKS.items():
        res.check(spot.get(name) == want, f"spot-check LIVE: {name[:44]} = {want}",
                  f"got {spot.get(name)}")

    # ── the PII scan ──
    #
    # Two scopes, because "does this string appear on the page" and "did
    # the vessel surface disclose it" are different questions.
    corpus = state["pii_values"] | {n for n in individuals.values() if len(n) > 3}

    def _scan(targets: list[tuple[str, str]], needles: set[str]) -> list[str]:
        found = []
        for needle in needles:
            low = needle.lower()
            for where, body in targets:
                if low in body.lower():
                    found.append(f"{needle!r} in {where}")
                    break
        return found

    # SCOPE 1 — the vessel surface itself. No exemptions: not one value
    # from any owner-PII column, and not one held individual's name, may
    # appear in the bytes the Vessels panel renders.
    res.check(
        not _scan(vessel_payloads, corpus),
        f"owner PII absent from all {len(vessel_payloads)} live vessels[] payloads "
        f"({len(corpus)} strings searched, zero exemptions)",
        f"hits: {_scan(vessel_payloads, corpus)[:5]}",
    )

    # SCOPE 2 — whole response bodies. Here ONE class is exempt and is
    # printed rather than hidden: a value that is itself the published
    # canonical_name of a sanctioned owner. `vessels.owner_name_raw` for
    # an OFAC vessel is the designated COMPANY, and that company's own
    # dossier is published on purpose — its name appearing on its own
    # page is the publish working, not owner PII leaking. Every such
    # exemption is listed below so the claim can be audited.
    published_labels = {n.lower() for n in state["by_owner"]}
    exempt = {
        v for v in corpus
        if any(v.lower() in lbl or lbl in v.lower() for lbl in published_labels)
    }
    if exempt:
        print(f"  note: {len(exempt)} PII-column value(s) exempt from the whole-body "
              f"scan as published owner labels: {sorted(exempt)}")
    body_hits = _scan(bodies, corpus - exempt)
    res.check(
        not body_hits,
        f"owner PII absent from all {len(bodies)} whole live response bodies "
        f"({len(corpus - exempt)} strings searched, {len(exempt)} exempt and listed)",
        f"{len(body_hits)} hits: {body_hits[:5]}",
    )


def _neo4j(state: dict, res: Result) -> None:
    """The Cypher surface: node count, property allowlist, PII scan."""
    from app.services.graph.neo4j_projection import Neo4jProjection

    proj = Neo4jProjection()
    if not proj.available:
        res.check(False, "neo4j reachable")
        return
    drv = proj.driver
    with drv.session() as s:
        n_nodes = s.run("MATCH (v:Vessel) RETURN count(v) AS n").single()["n"]
        n_rels = s.run("MATCH (:Canonical)-[r:OWNS]->(:Vessel) RETURN count(r) AS n").single()["n"]
        keys = set(
            s.run("MATCH (v:Vessel) UNWIND keys(v) AS k RETURN collect(DISTINCT k) AS ks")
            .single()["ks"]
        )
        props = [
            str(v)
            for rec in s.run("MATCH (v:Vessel) RETURN properties(v) AS p")
            for v in (rec["p"] or {}).values()
            if v is not None
        ]

    res.check(n_nodes == EXPECTED["vessels"], f"neo4j has {EXPECTED['vessels']} Vessel nodes",
              f"found {n_nodes}")
    res.check(n_rels == EXPECTED["edges"], f"neo4j has {EXPECTED['edges']} OWNS rels",
              f"found {n_rels}")
    res.check(keys <= VESSEL_NODE_KEYS,
              f"Vessel node keys within allowlist {sorted(VESSEL_NODE_KEYS)}",
              f"extra: {sorted(keys - VESSEL_NODE_KEYS)}")
    blob = "\n".join(props).lower()
    leaked = [p for p in state["pii_values"] if p.lower() in blob]
    res.check(not leaked, "no owner-PII value appears in any Vessel node property",
              f"{leaked[:5]}")


async def run(cache: str | None = None) -> int:
    """Run every check. Returns a process exit code."""
    res = Result()
    sm = get_sessionmaker()
    async with sm() as session:
        state = await _pg_state(session)

        res.check(state["published_owners"] == EXPECTED["owners"],
                  f"PG: {EXPECTED['owners']} owner canonicals published",
                  f"got {state['published_owners']}")
        res.check(state["published_edges"] == EXPECTED["edges"],
                  f"PG: {EXPECTED['edges']} vessel edges published (all three gates)",
                  f"got {state['published_edges']}")
        res.check(state["published_vessels"] == EXPECTED["vessels"],
                  f"PG: {EXPECTED['vessels']} vessel rows published",
                  f"got {state['published_vessels']}")
        res.check(state["vessels_published_total"] == EXPECTED["vessels"],
                  "PG: no vessel published beyond the owned cohort",
                  f"{state['vessels_published_total']} published overall")
        for name, want in SPOT_CHECKS.items():
            res.check(state["by_owner"].get(name) == want,
                      f"PG spot-check: {name[:44]} = {want}",
                      f"got {state['by_owner'].get(name)}")

        res.check(state["audit_promote"] >= EXPECTED["owners"] + EXPECTED["edges"],
                  "PG: an audit row per promotion",
                  f"{state['audit_promote']} promote rows")
        res.check(state["audit_unattributed"] == 0,
                  "PG: no unattributed promotion in the audit trail",
                  f"{state['audit_unattributed']} unattributed")

        res.check(state["aircraft_published"] == 5989,
                  "aircraft layer untouched: 5,989 published aircraft",
                  f"got {state['aircraft_published']}")
        res.check(state["aircraft_edges_published"] == 5989,
                  "aircraft layer untouched: 5,989 published REGISTERS edges",
                  f"got {state['aircraft_edges_published']}")
        res.check(state["aircraft_person_entities"] == 1,
                  "aircraft layer untouched: exactly 1 allowlisted individual",
                  f"got {state['aircraft_person_entities']}")

        individuals = await _held_owners_are_dark(session, res, cache) or {}

    await _live_surface(state, individuals, res)
    _neo4j(state, res)

    print(json.dumps({k: v for k, v in state.items()
                      if k not in ("by_owner", "owner_ids", "pii_values")}, indent=2))
    return res.report()


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Vessels P4 live verification.")
    ap.add_argument("--cache", default="/tmp/sdn_verify.xml")
    args = ap.parse_args()
    sys.exit(asyncio.run(run(cache=args.cache)))


if __name__ == "__main__":  # pragma: no cover
    _main()
