"""Vessels P2 — vessel→owner relationships from OFAC ENHANCED XML. DRY RUN.

**Writes nothing. Stages nothing. Surfaces nothing.** Opens a
``READ ONLY`` transaction so the database refuses a write, and prints a
report for helen. No edge is created here; that is a later phase and
needs approval.

WHY THIS SOURCE. Vessels P1 ingested ``SDN.CSV``, whose flat
``Vessel_Owner`` column was populated for only **6 of 1,540** rows —
the sanctions→owner linkage does not live there. ``SDN_ENHANCED.XML``
(109 MB) carries it as explicit relationships:

    <entity id="15036">                     <- id IS the SDN ent_num,
      <generalInfo><entityType>Vessel</...>     so it joins to
      <relationships>                          vessels.source_key
        <relationship>
          <type>Owned or Controlled By</type>
          <relatedEntity entityId="15117">NATIONAL IRANIAN TANKER COMPANY</...>

Census of the live file: 19,321 entities (9,922 Entity, 7,517
Individual, 1,540 Vessel, 342 Aircraft) and 9,016 relationships, the
commonest being "Owned or Controlled By" (3,943).

GUARDS — every aircraft lesson applies, and OFAC adds one of its own:

  * **Corporate-first.** An owner whose OFAC ``entityType`` is
    ``Individual`` is HELD for the curated allowlist, never auto-linked.
    The aircraft LAST-FIRST name trap is a name-ORDER problem; OFAC
    names are better formed, but the underlying risk — asserting that a
    named person owns a specific asset on a name match — is identical,
    so the same default-drop applies.
  * **Owner-capable only.** A resolved canonical must be an
    organization, agency or PAC. Never a concept or a place — the
    aircraft re-stage matched "MILITARY TECH INC" to the *concept*
    "military tech" once that rule was missing.
  * **Ambiguity at the best score** is reported, never silently resolved.
  * **Single-token** names are dropped outright.
  * OFAC-listed org owners are inherently public (that is what a
    sanctions listing is), so they are candidates — but candidacy is
    still not publication.
"""

from __future__ import annotations

import argparse
import collections
import logging
import os
import tempfile
import urllib.request
import xml.etree.ElementTree as ET

import psycopg

from app.services.aircraft_identity import is_individual_entity, is_owner_capable
from app.services.graph.base import normalize_name
from app.services.ingest.faa_aircraft_match_dryrun import (
    SCORE_EXACT_ALIAS,
    SCORE_EXACT_CANONICAL,
    SCORE_TOKEN_SET,
    _load_canonical_index,
)

logger = logging.getLogger(__name__)

OFAC_ENHANCED_URL = (
    "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN_ENHANCED.XML"
)
_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

#: Relationship types that assert OWNERSHIP or CONTROL of the vessel.
#: Deliberately narrow: "Providing support to" and "Associate Of" are
#: real relationships but they are not ownership, and treating them as
#: such would put a support entity's name on an asset it does not own.
OWNERSHIP_RELATIONS = frozenset(
    {
        "Owned or Controlled By",
        "Owns, controls, or operates",
        "Property in the interest of",
    }
)


def _strip(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def download(url: str = OFAC_ENHANCED_URL, cache: str | None = None) -> str:
    """Stream the enhanced XML to a temp file; return the path."""
    if cache and os.path.exists(cache):
        logger.info("using cached %s (%d bytes)", cache, os.path.getsize(cache))
        return cache
    path = cache or tempfile.mkstemp(prefix="sdn-enhanced-", suffix=".xml")[1]
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    total = 0
    with urllib.request.urlopen(req, timeout=900) as resp, open(path, "wb") as out:
        while chunk := resp.read(1 << 20):
            out.write(chunk)
            total += len(chunk)
    logger.info("OFAC enhanced XML: %d bytes -> %s", total, path)
    return path


def parse_entities(path: str) -> tuple[dict, list]:
    """Return ``(entities, ownership_links)`` from the enhanced XML.

    ``entities`` maps OFAC entity id -> ``(entityType, primary name)``.
    ``ownership_links`` is one tuple per vessel→owner ownership edge.
    """
    entities: dict[str, tuple[str, str | None]] = {}
    links: list[tuple[str, str, str, str, str | None]] = []
    for _ev, el in ET.iterparse(path, events=("end",)):
        if _strip(el.tag) != "entity":
            continue
        eid = el.get("id")
        gi = el.find("./{*}generalInfo/{*}entityType")
        etype = gi.text if gi is not None else "?"
        nm = el.find(".//{*}formattedFullName")
        name = nm.text if nm is not None else None
        entities[eid] = (etype, name)
        if etype == "Vessel":
            for rel in el.findall("./{*}relationships/{*}relationship"):
                t = rel.find("./{*}type")
                rtype = t.text if t is not None else None
                rel_ent = rel.find("./{*}relatedEntity")
                if rel_ent is None or rtype not in OWNERSHIP_RELATIONS:
                    continue
                links.append((eid, name, rtype, rel_ent.get("entityId"), rel_ent.text))
        el.clear()
    return entities, links


def _confidence(tier: str, n_best: int, n_tokens: int) -> str:
    if tier != "exact_canonical":
        return "medium" if tier == "exact_alias" else "low"
    if n_best > 1 or n_tokens < 2:
        return "low"
    return "high"


def run(cache: str | None = None) -> dict:
    """Resolve OFAC vessel owners against Argus. Writes nothing."""
    url = os.environ["DATABASE_URL_SYNC"].replace("+psycopg", "")
    path = download(cache=cache)
    entities, links = parse_entities(path)

    rep: dict = {
        "entities_total": len(entities),
        "entity_type_census": collections.Counter(t for t, _ in entities.values()),
        "ownership_links": len(links),
        "vessels_with_owner": len({v for v, *_ in links}),
        "distinct_owners": len({o for *_, o, _ in links}),
        "owner_type_census": collections.Counter(),
        "rel_type_census": collections.Counter(r for _, _, r, _, _ in links),
        "vessels_in_argus": 0,
        "resolved": [],
        "unresolved": [],
        "held": collections.Counter(),
        "tiers": collections.Counter(),
        "cohorts": collections.Counter(),
    }

    with psycopg.connect(url) as conn:
        conn.read_only = True
        cur = conn.cursor()
        exact, tokenset = _load_canonical_index(cur)
        cur.execute("select source_key, vessel_name from vessels where source='ofac_sdn'")
        argus_vessels = {r[0]: r[1] for r in cur.fetchall()}
        rep["argus_vessel_rows"] = len(argus_vessels)

    # owner id -> the vessels it owns (dedupe the owner-side work)
    by_owner: dict[str, list] = collections.defaultdict(list)
    for vid, vname, rtype, oid, oname in links:
        if vid in argus_vessels:
            rep["vessels_in_argus"] += 1
        by_owner[oid].append((vid, vname, rtype, oname))

    for oid, rows in by_owner.items():
        otype, oname = entities.get(oid, ("?", rows[0][3]))
        rep["owner_type_census"][otype] += 1
        n_vessels = len(rows)
        display = oname or rows[0][3] or ""

        # OFAC says this owner is a natural person -> HELD, never linked.
        if otype == "Individual":
            rep["cohorts"]["HELD_individual_owner"] += 1
            rep["held"]["individual_owner"] += n_vessels
            continue

        norm = normalize_name(display)
        toks = frozenset(norm.split()) if norm else frozenset()
        if not norm:
            rep["cohorts"]["DROP_unnamed"] += 1
            continue
        if len(toks) == 1:
            rep["cohorts"]["DROP_single_token"] += 1
            continue

        hits = [
            (SCORE_EXACT_CANONICAL if rec[4] == "canonical" else SCORE_EXACT_ALIAS, rec)
            for rec in exact.get(norm, [])
        ]
        if not hits and len(toks) >= 2:
            hits = [(SCORE_TOKEN_SET, rec) for rec in tokenset.get(toks, [])]
        if not hits:
            rep["cohorts"]["NEW_candidate_not_in_argus"] += 1
            rep["unresolved"].append(
                {"owner": display, "ofac_type": otype, "vessels": n_vessels}
            )
            continue

        best = round(max(h[0] for h in hits), 2)
        tier = {
            SCORE_EXACT_CANONICAL: "exact_canonical",
            SCORE_EXACT_ALIAS: "exact_alias",
            SCORE_TOKEN_SET: "token_set",
        }[best]
        best_ids = {rec[0] for s, rec in hits if round(s, 2) == best}
        cid, ctype, smode, cname, _via, _mv, edges = max(hits, key=lambda h: h[0])[1]

        if any(rec[2] in ("suppress", "alias") for _s, rec in hits):
            rep["cohorts"]["HELD_privacy"] += 1
            continue
        if is_individual_entity(ctype, cname):
            rep["cohorts"]["HELD_individual_canonical"] += 1
            continue
        if not is_owner_capable(ctype):
            rep["cohorts"]["DROP_not_owner_capable"] += 1
            continue
        if len(best_ids) > 1:
            rep["cohorts"]["DROP_ambiguous"] += 1
            continue

        conf = _confidence(tier, len(best_ids), len(toks))
        rep["tiers"][f"{tier}/{conf}"] += 1
        rep["cohorts"]["RESOLVED_to_argus"] += 1
        rep["resolved"].append(
            {
                "owner": display,
                "ofac_type": otype,
                "canonical": cname,
                "canonical_type": ctype,
                "edges": edges,
                "tier": tier,
                "score": best,
                "confidence": conf,
                "vessels": n_vessels,
            }
        )

    return rep


def _print(r: dict) -> None:  # pragma: no cover
    print("\n=== VESSELS P2 — OFAC OWNER RESOLUTION (DRY RUN, NOTHING WRITTEN) ===")
    print(f"  OFAC entities            {r['entities_total']:,}   {dict(r['entity_type_census'])}")
    print(f"  ownership relationships  {r['ownership_links']:,}   {dict(r['rel_type_census'])}")
    print(f"  vessels with an owner    {r['vessels_with_owner']:,}")
    print(f"  of those, in Argus       {r['vessels_in_argus']:,} / {r['argus_vessel_rows']:,} rows")
    print(f"  distinct owners          {r['distinct_owners']:,}   {dict(r['owner_type_census'])}")

    print("\n-- COHORTS (owner-side)")
    for k, v in sorted(r["cohorts"].items()):
        print(f"     {k:<32} {v:>6}")
    print("\n-- CONFIDENCE TIERS (resolved owners)")
    for k, v in sorted(r["tiers"].items()):
        print(f"     {k:<32} {v:>6}")

    res = sorted(r["resolved"], key=lambda x: -x["vessels"])
    print(f"\n-- RESOLVED TO EXISTING ARGUS ENTITIES ({len(res)}) — top 25 by fleet")
    print(f"     {'owner (OFAC)':<40} {'-> canonical':<34} "
          f"{'tier':<16} {'conf':<7} {'edg':>4} {'ships':>6}")
    for x in res[:25]:
        print(f"     {x['owner'][:40]:<40} {x['canonical'][:34]:<34} "
              f"{x['tier']:<16} {x['confidence']:<7} {x['edges']:>4} {x['vessels']:>6}")

    new = sorted(r["unresolved"], key=lambda x: -x["vessels"])
    print(f"\n-- NEW CANDIDATES, not yet in Argus ({len(new)}) — top 25 by fleet")
    for x in new[:25]:
        print(f"     {x['owner'][:52]:<52} [{x['ofac_type']}]  ships={x['vessels']}")

    print("\n  HELD (never auto-linked):", dict(r["held"]))
    print("\nNOTHING WAS WRITTEN. No edge, no staged row, no vessel promoted.")


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Vessels P2 owner-resolution dry run.")
    ap.add_argument("--cache", default="/tmp/sdn_enh.xml")
    args = ap.parse_args()
    _print(run(cache=args.cache))


if __name__ == "__main__":  # pragma: no cover
    _main()
