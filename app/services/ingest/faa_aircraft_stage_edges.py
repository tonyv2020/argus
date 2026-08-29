"""P2 — stage REGISTERS edges for the approved cohort only.

helen's decision (2026-08-29), applied literally:

  * **STAGE** the 1.00 ``exact_canonical`` tier, minus the collision
    holds below.
  * **HOLD** the entire 0.90 ``exact_alias`` tier. It systematically
    collapses subsidiaries into parents and managers into what they
    manage (Rolls-Royce Corp into the plc, FedEx Freight into FedEx,
    Bell Asset Management into Bell Global Equities Fund).
  * **HOLD** every individual registrant, every ``suppress``/``alias``
    candidate, and every collision hold — for Tony, unstaged.
  * Everything written is ``suppress`` + ``staged`` + cited. Publishing
    is P3 and is Tony's call.

Re-runnable: the edge upserts on ``(canonical_id, aircraft_id)``.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import logging
import os

import psycopg

from app.models import _new_id
from app.services.graph.base import normalize_name
from app.services.ingest.faa_aircraft_match_dryrun import (
    SCORE_EXACT_CANONICAL,
    _load_canonical_index,
    classify,
)

logger = logging.getLogger(__name__)

#: Pairs held as probable name collisions rather than staged, with the
#: reason. These are the ones from the 86 zero-edge 1.00 pairs where
#: the registrant and the canonical look like different organisations
#: that happen to share a name — plus the ones I simply am not
#: confident about, which helen's instruction says to hold too.
#:
#: Keyed by the raw FAA registrant string.
COLLISION_HOLDS: dict[str, str] = {
    "H&M LTD": "clothing retailer H&M — cross-domain collision",
    "H & M LTD": "clothing retailer H&M — cross-domain collision",
    "SKYLARK HOLDINGS LLC": "Skylark Holdings Co., Ltd. is the Japanese restaurant group",
    "SK GROUP LLC": "SK Group is the Korean chaebol — a US LLC is not it",
    "RUSSELL GROUP INC": "Russell Group is the UK association of research universities",
    "SIRIUS REAL ESTATE LLC": "Sirius Real Estate Limited is UK/German-listed",
    "BEYOND AIR LLP": "Beyond Air, Inc. is a NASDAQ pharma company; LLP is a different form",
    "KODIAK 100 LLC": "'Kodiak 100' is an AIRCRAFT MODEL, not an organisation",
    "BOARD OF REGENTS": "canonical is the generic lowercase 'board of regents' — unresolvable",
    "BARON PARTNERS INC": "Baron Partners is a fund; the registrant is not obviously it",
    "BARON PARTNERS LLC": "Baron Partners is a fund; the registrant is not obviously it",
    "AMERICAN RESOURCES INC": "name too generic to assert identity",
    "LIBERTY CAPITAL LLC": "'Liberty Capital' is a very common name; LLC vs Corporation",
    "MOUNTAIN EXPRESS LLC": "name too generic to assert identity",
    "ROCKET ONE LLC": "name too generic to assert identity",
}


async def stage_edges(dry_run: bool = False) -> dict:
    """Stage REGISTERS edges for the approved cohort. Idempotent."""
    url = os.environ["DATABASE_URL_SYNC"].replace("+psycopg", "")
    stats: dict = {
        "staged": 0,
        "distinct_pairs": 0,
        "held": collections.Counter(),
        "held_collision_pairs": {},
        "skipped_existing": 0,
    }

    with psycopg.connect(url) as conn:
        cur = conn.cursor()
        exact, tokenset = _load_canonical_index(cur)
        logger.info("indexed %d exact keys", len(exact))

        cur.execute(
            "select id, sha256, source_url, batch_id from aircraft_source_snapshots "
            "order by fetched_at desc limit 1"
        )
        snap_id, sha, src_url, batch_id = cur.fetchone()
        logger.info("citing snapshot %s (%s)", batch_id, sha[:12])

        cur.execute(
            "select id, n_number, registrant_name, type_registrant "
            "from aircraft where registrant_name is not null"
        )
        rows = cur.fetchall()

        pending: list[tuple] = []
        pairs: set[tuple] = set()
        for aid, _nnum, rname, rtype in rows:
            norm = normalize_name(rname)
            if not norm:
                continue
            toks = frozenset(norm.split())
            hits = [
                (SCORE_EXACT_CANONICAL if rec[4] == "canonical" else 0.90, rec)
                for rec in exact.get(norm, [])
            ]
            if not hits:
                continue
            best = round(max(h[0] for h in hits), 2)
            best_ids = {rec[0] for s, rec in hits if round(s, 2) == best}
            distinct_ids = {rec[0] for _s, rec in hits}
            tier = "exact_canonical" if best == SCORE_EXACT_CANONICAL else "exact_alias"

            cohort = classify(rtype, toks, hits, best, best_ids, distinct_ids)
            if cohort != "ELIGIBLE":
                stats["held"][cohort] += 1
                continue

            # helen: hold the whole 0.90 tier.
            if tier != "exact_canonical":
                stats["held"]["HOLD_alias_tier"] += 1
                continue

            reason = COLLISION_HOLDS.get((rname or "").strip().upper())
            if reason:
                stats["held"]["HOLD_collision"] += 1
                cid = next(iter(best_ids))
                stats["held_collision_pairs"].setdefault(
                    (rname, cid), {"registrant": rname, "reason": reason, "aircraft": 0}
                )
                stats["held_collision_pairs"][(rname, cid)]["aircraft"] += 1
                continue

            cid, _ctype, _sm, cname, _via, _mv, _ed = max(hits, key=lambda h: h[0])[1]
            pairs.add((rname, cname))
            pending.append((cid, aid, tier, best, rname))

        stats["distinct_pairs"] = len(pairs)
        for k, v in stats["held_collision_pairs"].items():
            v["canonical_id"] = k[1]

        if dry_run:
            logger.info("DRY RUN — would stage %d edges", len(pending))
            stats["staged"] = 0
            stats["would_stage"] = len(pending)
            return stats

        for i in range(0, len(pending), 1000):
            chunk = pending[i : i + 1000]
            cur.executemany(
                """insert into aircraft_registration_edges
                     (id, canonical_id, aircraft_id, relation, match_tier, match_score,
                      matched_via, registrant_name_raw, snapshot_id, source_url,
                      source_sha256, surface_mode, publication_state, batch_id)
                   values (%s,%s,%s,'registers',%s,%s,NULL,%s,%s,%s,%s,
                           'suppress','staged',%s)
                   on conflict (canonical_id, aircraft_id) do nothing""",
                [
                    (_new_id(), cid, aid, tier, score, rname, snap_id, src_url, sha, batch_id)
                    for cid, aid, tier, score, rname in chunk
                ],
            )
            stats["staged"] += len(chunk)
        conn.commit()
        cur.execute("select count(*) from aircraft_registration_edges")
        stats["rows_in_table"] = cur.fetchone()[0]

    return stats


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    s = asyncio.run(stage_edges(dry_run=args.dry_run))
    print("\n=== P2 STAGE REGISTERS EDGES ===")
    print(f"  edges staged        {s['staged']:,}")
    print(f"  distinct pairs      {s['distinct_pairs']:,}")
    print(f"  rows in table       {s.get('rows_in_table', 0):,}")
    print("\n  HELD (not staged):")
    for k, v in sorted(s["held"].items()):
        print(f"    {k:<22} {v:>7,}")
    print(f"\n  COLLISION HOLDS ({len(s['held_collision_pairs'])} pairs):")
    for v in sorted(s["held_collision_pairs"].values(), key=lambda x: x["registrant"]):
        print(f"    {v['registrant'][:40]:<40} ac={v['aircraft']:<4} {v['reason']}")


if __name__ == "__main__":  # pragma: no cover
    _main()
