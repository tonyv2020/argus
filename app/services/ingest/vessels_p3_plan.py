"""Vessels P3 — hybrid plan. DRY RUN ONLY: creates nothing, stages nothing.

Tony's decision (2026-08-30): create the major shadow-fleet operators as
NEW canonicals, crosswalk only true same-entity name variants to
existing canonicals, defer the long tail. Publishing stays separately
gated.

This module computes the plan and prints it for review. It opens a
``READ ONLY`` transaction, so the database refuses a write.

THE CROSSWALK IS THE DANGEROUS PART. Vessels P2 found real overlap that
exact matching misses — ``PETROLEOS DE VENEZUELA, S.A.`` is the same
company as Argus's ``PDVSA``. But the neighbouring cases are NOT the
same entity:

    JOINT STOCK COMPANY ROSNEFTEFLOT   is a SUBSIDIARY of Rosneft
    GAZPROMNEFT MARINE BUNKER LLC      is a SUBSIDIARY of Gazprom Neft

Crosswalking those would repeat the aircraft carrier-vs-parent mistake,
where FedEx Freight collapsed into FedEx and Rolls-Royce Corp into the
plc. So the crosswalk is a **hand-curated allowlist with per-entry
evidence** (:data:`CROSSWALK`), never a scoring tier, and this module's
automatic search only produces CANDIDATES for a human to judge.

Subsidiaries get their own new canonical. That is the point: the graph
should say Rosnefteflot owns these ships and is related to Rosneft, not
that Rosneft owns them.
"""

from __future__ import annotations

import argparse
import collections
import logging
import os

import psycopg

from app.services.aircraft_identity import is_owner_capable
from app.services.graph.base import normalize_name
from app.services.ingest.ofac_vessel_owner_dryrun import download, parse_entities

logger = logging.getLogger(__name__)

#: Owners with at least this many vessels become new canonicals.
DEFAULT_CUTOFF = 5

#: HAND-CURATED same-entity crosswalks: OFAC legal name -> existing
#: Argus canonical. Each entry states the evidence. Nothing is added
#: here by a script; the automatic search only proposes candidates.
#:
#: The bar is "these are the same legal entity under two names",
#: NOT "these are related companies".
CROSSWALK: dict[str, dict] = {
    "PETROLEOS DE VENEZUELA, S.A.": {
        "canonical": "PDVSA",
        "evidence": (
            "Acronym of the full legal name: P(etroleos) D(e) V(enezuela) S.A. "
            "-> PDVSA. Same state oil company, not a parent/subsidiary pair — "
            "PDVSA IS Petroleos de Venezuela S.A., which is why the acronym "
            "reconstructs the name exactly."
        ),
    },
}

#: Same-entity-looking pairs deliberately NOT crosswalked, with why.
CROSSWALK_HELD: dict[str, str] = {
    "JOINT STOCK COMPANY ROSNEFTEFLOT": (
        "Rosnefteflot is the SHIPPING SUBSIDIARY of Rosneft, not Rosneft. "
        "Crosswalking would assert the parent owns these vessels — the "
        "aircraft FedEx Freight -> FedEx mistake. Gets its own canonical."
    ),
    "GAZPROMNEFT MARINE BUNKER LIMITED LIABILITY COMPANY": (
        "Marine bunkering subsidiary of Gazprom Neft, not Gazprom Neft. "
        "Own canonical."
    ),
    "ISLAMIC REPUBLIC OF IRAN SHIPPING LINES": (
        "The substring search proposes 'Islamic Republic of Iran' — the "
        "COUNTRY. IRISL is a state shipping company, not the state. This is "
        "the most tempting wrong crosswalk in the set (121 vessels) and "
        "would attribute a national fleet to a sovereign nation entity. "
        "Own canonical."
    ),
}


def acronym(name: str) -> str:
    """First letter of each normalized token — 'petroleos de venezuela s a' -> 'pdvsa'."""
    toks = normalize_name(name).split()
    return "".join(t[0] for t in toks if t)


def find_crosswalk_candidates(owner: str, by_norm: dict, by_acronym: dict) -> list[tuple]:
    """Propose existing canonicals that MIGHT be the same entity.

    Candidates only — every one is judged by a human before it reaches
    :data:`CROSSWALK`. Each carries the strategy that found it, because
    'substring' is the strategy that finds subsidiaries and needs the
    most suspicion.
    """
    out: list[tuple] = []
    norm = normalize_name(owner)
    if not norm:
        return out
    for rec in by_norm.get(norm, []):
        out.append(("exact", rec))
    # Minimum 4 letters. At 3 the strategy produced a false positive on
    # live data: DALIAN OCEAN FISHING COMPANY and DEFENSE OF FREEDOM PAC
    # both reduce to "dof" once legal suffixes are stripped.
    acr = acronym(owner)
    if len(acr) >= 4:
        for rec in by_acronym.get(acr, []):
            out.append(("acronym", rec))
    # Substring: an existing canonical name appearing inside the OFAC
    # name. This is where subsidiaries surface (Rosneft inside
    # Rosnefteflot), so it is reported and never auto-accepted.
    # Multi-token only: single generic words ("company", "management",
    # "marine") match half the corpus and drown the real candidates.
    for cand_norm, recs in by_norm.items():
        if (
            len(cand_norm) >= 6
            and " " in cand_norm
            and cand_norm in norm
            and cand_norm != norm
        ):
            for rec in recs:
                out.append(("substring", rec))
    return out


def run(cutoff: int = DEFAULT_CUTOFF, cache: str | None = None) -> dict:
    """Compute the P3 plan. Writes nothing."""
    url = os.environ["DATABASE_URL_SYNC"].replace("+psycopg", "")
    entities, links = parse_entities(download(cache=cache))

    by_owner: dict[str, list] = collections.defaultdict(list)
    for vid, _vname, _rtype, oid, oname in links:
        by_owner[oid].append((vid, oname))

    owners = []
    for oid, rows in by_owner.items():
        otype, oname = entities.get(oid, ("?", rows[0][1]))
        owners.append(
            {
                "ofac_id": oid,
                "name": oname or rows[0][1] or "",
                "ofac_type": otype,
                "vessels": len({v for v, _ in rows}),
            }
        )
    owners.sort(key=lambda o: -o["vessels"])
    total_owned = len({v for v, *_ in links})

    rep: dict = {
        "total_owners": len(owners),
        "total_owned_vessels": total_owned,
        "cutoff": cutoff,
        "cutoff_options": [],
        "majors": [],
        "deferred": 0,
        "deferred_vessels": 0,
        "individuals_held": 0,
        "individual_vessels": 0,
        "crosswalk_candidates": [],
    }

    # Coverage at several cutoffs, so the choice is informed.
    org_owners = [o for o in owners if o["ofac_type"] != "Individual"]
    # DISTINCT vessels, not the sum of per-owner counts: a vessel with
    # two owners would otherwise be counted twice and inflate coverage.
    owner_vessels = {oid: {v for v, _ in rows} for oid, rows in by_owner.items()}
    for c in (1, 2, 3, 5, 10, 20):
        sel = [o for o in org_owners if o["vessels"] >= c]
        cov = len(set().union(*(owner_vessels[o["ofac_id"]] for o in sel)) if sel else set())
        rep["cutoff_options"].append(
            {
                "cutoff": c,
                "owners": len(sel),
                "vessels": cov,
                "pct": round(100.0 * cov / max(1, total_owned), 1),
            }
        )

    with psycopg.connect(url) as conn:
        conn.read_only = True
        cur = conn.cursor()
        cur.execute(
            "select id, canonical_name, canonical_name_normalized, type, surface_mode, "
            "publication_state from canonical_entities"
        )
        by_norm: dict[str, list] = collections.defaultdict(list)
        by_acr: dict[str, list] = collections.defaultdict(list)
        for cid, cname, cnorm, ctype, smode, pstate in cur.fetchall():
            key = cnorm or normalize_name(cname)
            rec = (cid, cname, ctype, smode, pstate)
            by_norm[key].append(rec)
            a = "".join(t[0] for t in key.split() if t)
            if len(a) >= 4:
                by_acr[a].append(rec)

    for o in owners:
        if o["ofac_type"] == "Individual":
            rep["individuals_held"] += 1
            rep["individual_vessels"] += o["vessels"]
            continue
        if o["vessels"] < cutoff:
            rep["deferred"] += 1
            rep["deferred_vessels"] += o["vessels"]
            continue
        cands = [
            (strategy, rec)
            for strategy, rec in find_crosswalk_candidates(o["name"], by_norm, by_acr)
            if is_owner_capable(rec[2])
        ]
        entry = dict(o)
        entry["crosswalk"] = CROSSWALK.get(o["name"])
        entry["held_reason"] = CROSSWALK_HELD.get(o["name"])
        entry["candidates"] = [
            {"strategy": s, "canonical": r[1], "type": r[2]} for s, r in cands
        ]
        if cands:
            rep["crosswalk_candidates"].append(entry)
        rep["majors"].append(entry)

    rep["majors_vessels"] = sum(m["vessels"] for m in rep["majors"])
    return rep


def _print(r: dict) -> None:  # pragma: no cover
    print("\n=== VESSELS P3 PLAN — DRY RUN, NOTHING CREATED, NOTHING STAGED ===")
    print(f"  owners with vessels        {r['total_owners']}")
    print(f"  vessels with an owner      {r['total_owned_vessels']}")
    print("\n-- CUTOFF OPTIONS (organisation owners only)")
    print(f"     {'>= vessels':<12}{'owners':>8}{'vessels':>10}{'coverage':>11}")
    for c in r["cutoff_options"]:
        print(f"     {c['cutoff']:<12}{c['owners']:>8}{c['vessels']:>10}{c['pct']:>10}%")
    print(f"\n  CHOSEN CUTOFF: >= {r['cutoff']} vessels")
    print(f"     majors to create   {len(r['majors'])}  covering {r['majors_vessels']} vessels")
    print(f"     deferred long tail {r['deferred']}  ({r['deferred_vessels']} vessels)")
    print(f"     individuals HELD   {r['individuals_held']}  ({r['individual_vessels']} vessels)")

    print(f"\n-- MAJOR OPERATORS TO CREATE ({len(r['majors'])})")
    print(f"     {'ships':>6}  {'OFAC entity':<56} note")
    for m in r["majors"]:
        note = ""
        if m["crosswalk"]:
            note = f"CROSSWALK -> {m['crosswalk']['canonical']}"
        elif m["held_reason"]:
            note = "HELD from crosswalk (subsidiary) — own canonical"
        elif m["candidates"]:
            note = "candidate(s): " + ", ".join(
                f"{c['canonical']}[{c['strategy']}]" for c in m["candidates"][:2]
            )
        print(f"     {m['vessels']:>6}  {m['name'][:56]:<56} {note}")

    n = len(r["crosswalk_candidates"])
    print(f"\n-- AUTOMATIC CROSSWALK CANDIDATES FOR HUMAN JUDGMENT ({n})")
    for m in r["crosswalk_candidates"]:
        print(f"     {m['name'][:58]}  ({m['vessels']} ships)")
        for c in m["candidates"]:
            print(f"        [{c['strategy']:<9}] -> {c['canonical']} [{c['type']}]")
        if m["crosswalk"]:
            print(f"        ACCEPTED -> {m['crosswalk']['canonical']}: "
                  f"{m['crosswalk']['evidence'][:140]}")
        if m["held_reason"]:
            print(f"        HELD: {m['held_reason'][:150]}")

    print("\n-- CURATED CROSSWALK (accepted)")
    for k, v in CROSSWALK.items():
        print(f"     {k}  ->  {v['canonical']}")
    print("-- CROSSWALK HELD (same-entity-looking but NOT the same entity)")
    for k, v in CROSSWALK_HELD.items():
        print(f"     {k}\n        {v}")
    print("\nNOTHING WAS CREATED OR STAGED.")


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Vessels P3 hybrid plan (dry run).")
    ap.add_argument("--cutoff", type=int, default=DEFAULT_CUTOFF)
    ap.add_argument("--cache", default="/tmp/sdn_p3.xml")
    args = ap.parse_args()
    _print(run(cutoff=args.cutoff, cache=args.cache))


if __name__ == "__main__":  # pragma: no cover
    _main()
