"""P3.1 — individual public-figure gate. DRY RUN ONLY. Writes nothing.

Design decision #2 (helen-k3s/docs/argus-p3-aircraft-publish-design.md):

    An individual registrant's aircraft surface **only if** the registrant
    resolves (high-confidence) to an entity that is *already published in
    Argus* **AND** passes the Scrutiny people-gate as an in-scope public
    figure. Default **DROP**. Street never surfaces regardless.

This module evaluates that gate against the 85 held individual matches and
prints a report. It opens a ``READ ONLY`` transaction, so the database
refuses a write rather than the code merely abstaining. Nothing is
promoted, staged or resolved.

**Three fail-closed choices worth stating, because each one drops rows a
looser reading would surface:**

1. **The Scrutiny gate is evaluated deterministically only.**
   ``scrutinize_person`` falls back to an LLM when a person has no hard
   signals — and with no key configured that path fail-closes to SUPPRESS
   anyway. This dry run never calls it: a person is treated as an in-scope
   public figure only via a **hard signal** (an FEC/bioguide/LDA/corporate-
   registry alias — an identifier that *is* a public role) or a **recorded
   SURFACE verdict** in ``scrutiny_decisions``. "We would have to ask a
   model" is not evidence of public-figure status, so it drops.

2. **"Published" means published AND open.** An ``alias``-mode entity is a
   private person Argus deliberately shows under a pseudonym; attaching a
   tail number to it would re-identify exactly what the alias hides.

3. **High confidence is narrow.** Only an exact canonical-name match, on a
   multi-token name, with no tie at the best score. FAA stores individuals
   as ``LAST FIRST MIDDLE`` while canonicals are ``First Last``, so a
   token-set match is a name *permutation* — good enough to suggest a
   person, nowhere near good enough to publish an asset claim about them.
"""

from __future__ import annotations

import argparse
import collections
import logging
import os

import psycopg

from app.services.graph.base import normalize_name
from app.services.ingest.faa_aircraft_match_dryrun import (
    SCORE_EXACT_ALIAS,
    SCORE_EXACT_CANONICAL,
    SCORE_TOKEN_SET,
    _load_canonical_index,
)

logger = logging.getLogger(__name__)

#: FAA TYPE REGISTRANT codes denoting a natural person.
INDIVIDUAL_TYPES = {"1", "4"}

#: Alias source systems that ARE a public role. Mirrors
#: ``app.services.scrutiny._PUBLIC_SOURCE_SYSTEMS`` — kept as an explicit
#: copy so a change there is a visible diff here rather than a silent
#: widening of who counts as a public figure.
PUBLIC_SOURCE_SYSTEMS = (
    "fec.committee",
    "fec.candidate",
    "bioguide",
    "senate.lda.registrant",
    "corporate.registry.officer",
    "corporate.registry.exec",
)

# Reason codes for the DROP list.
DROP_NO_MATCH = "no-match"
DROP_LOW_CONF = "low-conf"
DROP_UNPUBLISHED = "unpublished"
DROP_FAILS_SCRUTINY = "fails-scrutiny"
DROP_NOT_A_PERSON = "not-a-person-entity"


def _confidence(tier: str, n_best_ids: int, n_tokens: int) -> str:
    """High only when the match is exact, unique and multi-token."""
    if tier != "exact_canonical":
        return "low"
    if n_best_ids > 1:
        return "low"
    if n_tokens < 2:
        return "low"
    return "high"


def run(limit: int | None = None) -> dict:
    """Evaluate the gate. Returns the report payload. Writes nothing."""
    url = os.environ["DATABASE_URL_SYNC"].replace("+psycopg", "")
    report: dict = {
        "cohort_rows": 0,
        "cohort_pairs": 0,
        "would_surface": [],
        "needs_judgment": [],
        "drops": [],
        "drop_counts": collections.Counter(),
        "aircraft_in_cohort": 0,
    }

    with psycopg.connect(url) as conn:
        conn.read_only = True
        cur = conn.cursor()
        exact, tokenset = _load_canonical_index(cur)
        logger.info("indexed %d exact keys", len(exact))

        # Entity facts we need for the gate, fetched once.
        cur.execute(
            "select id, canonical_name, type, surface_mode, publication_state "
            "from canonical_entities"
        )
        ents = {
            r[0]: {
                "name": r[1],
                "type": r[2],
                "surface_mode": r[3],
                "publication_state": r[4],
            }
            for r in cur.fetchall()
        }

        # Hard public-role signals: an alias from a source system that IS
        # a public role. This is the deterministic half of scrutiny.
        cur.execute(
            "select distinct canonical_id, source_system from entity_aliases "
            "where source_system = any(%s)",
            (list(PUBLIC_SOURCE_SYSTEMS),),
        )
        hard_signals: dict[str, set[str]] = collections.defaultdict(set)
        for cid, src in cur.fetchall():
            hard_signals[cid].add(src)

        # Recorded scrutiny verdicts — most recent per canonical.
        cur.execute(
            "select distinct on (canonical_id) canonical_id, classification, decision, decided_by "
            "from scrutiny_decisions order by canonical_id, decided_at desc"
        )
        verdicts = {r[0]: {"class": r[1], "decision": r[2], "by": r[3]} for r in cur.fetchall()}

        # The cohort: individual registrants with >=1 candidate.
        sql = (
            "select id, n_number, registrant_name, type_registrant from aircraft "
            "where registrant_name is not null and type_registrant = any(%s)"
        )
        cur.execute(sql, (list(INDIVIDUAL_TYPES),))

        pairs: dict[tuple, dict] = {}
        for _aid, nnum, rname, _rtype in cur:
            norm = normalize_name(rname)
            if not norm:
                continue
            toks = frozenset(norm.split())
            hits = [
                (SCORE_EXACT_CANONICAL if rec[4] == "canonical" else SCORE_EXACT_ALIAS, rec)
                for rec in exact.get(norm, [])
            ]
            if not hits and len(toks) >= 2:
                hits = [(SCORE_TOKEN_SET, rec) for rec in tokenset.get(toks, [])]
            if not hits:
                continue

            best = round(max(h[0] for h in hits), 2)
            tier = {
                SCORE_EXACT_CANONICAL: "exact_canonical",
                SCORE_EXACT_ALIAS: "exact_alias",
                SCORE_TOKEN_SET: "token_set",
            }[best]
            best_ids = {rec[0] for s, rec in hits if round(s, 2) == best}

            # P2 classify() order puts privacy first, so a suppress/alias
            # candidate is already in HOLD_privacy, not here.
            if any(rec[2] in ("suppress", "alias") for _s, rec in hits):
                continue

            cid = sorted(best_ids)[0]
            key = (rname, cid)
            rec = pairs.get(key)
            if rec is None:
                pairs[key] = {
                    "registrant": rname,
                    "canonical_id": cid,
                    "tier": tier,
                    "score": best,
                    "n_best_ids": len(best_ids),
                    "n_tokens": len(toks),
                    "aircraft": 1,
                    "example_n": f"N{nnum}",
                }
            else:
                rec["aircraft"] += 1
            report["cohort_rows"] += 1

        report["cohort_pairs"] = len(pairs)
        report["aircraft_in_cohort"] = sum(p["aircraft"] for p in pairs.values())

        # ── apply the gate, pair by pair ──
        for pair in sorted(pairs.values(), key=lambda p: p["registrant"]):
            cid = pair["canonical_id"]
            ent = ents.get(cid)
            conf = _confidence(pair["tier"], pair["n_best_ids"], pair["n_tokens"])
            pair["confidence"] = conf

            if ent is None:
                pair["reason"] = DROP_NO_MATCH
                report["drops"].append(pair)
                report["drop_counts"][DROP_NO_MATCH] += 1
                continue

            pair["entity"] = ent["name"]
            pair["entity_type"] = ent["type"]
            pair["published"] = (
                ent["publication_state"] == "published" and ent["surface_mode"] == "open"
            )
            sig = sorted(hard_signals.get(cid, ()))
            rec_v = verdicts.get(cid)
            pair["hard_signals"] = sig
            pair["recorded_verdict"] = (
                f"{rec_v['class']}/{rec_v['decision']} ({rec_v['by']})" if rec_v else None
            )
            # Deterministic scrutiny only — see the module docstring.
            if sig:
                pair["scrutiny"] = f"PUBLIC (hard signals: {', '.join(sig)})"
                scrutiny_ok = True
            elif rec_v and rec_v["decision"] == "surface" and rec_v["class"] == "public":
                pair["scrutiny"] = f"PUBLIC (recorded: {rec_v['by']})"
                scrutiny_ok = True
            else:
                pair["scrutiny"] = (
                    f"NOT ESTABLISHED (recorded: {pair['recorded_verdict']})"
                    if rec_v
                    else "NOT ESTABLISHED (no hard signal, no recorded verdict)"
                )
                scrutiny_ok = False
            pair["scrutiny_ok"] = scrutiny_ok

            # Order matters: report the STRONGEST reason to drop.
            if ent["type"] != "person":
                pair["reason"] = DROP_NOT_A_PERSON
            elif not pair["published"]:
                pair["reason"] = DROP_UNPUBLISHED
            elif not scrutiny_ok:
                pair["reason"] = DROP_FAILS_SCRUTINY
            elif conf != "high":
                pair["reason"] = DROP_LOW_CONF
            else:
                pair["reason"] = None

            if pair["reason"] is None:
                report["would_surface"].append(pair)
            else:
                report["drops"].append(pair)
                report["drop_counts"][pair["reason"]] += 1
                # Plausible-but-not-confident: clears every gate EXCEPT
                # confidence. Defaults to DROP; Tony decides.
                if (
                    pair["reason"] == DROP_LOW_CONF
                    and pair["published"]
                    and scrutiny_ok
                    and ent["type"] == "person"
                ):
                    report["needs_judgment"].append(pair)

    return report


def _fmt(p: dict) -> str:
    return (
        f"    {p['registrant'][:34]:<34} -> {str(p.get('entity'))[:30]:<30} "
        f"{p['tier']:<16} {p['score']:<5.2f} conf={p['confidence']:<4} "
        f"pub={'Y' if p.get('published') else 'N'}  ac={p['aircraft']:<3} "
        f"{p.get('scrutiny','-')}"
    )


def _print(r: dict) -> None:  # pragma: no cover
    print("\n=== P3.1 INDIVIDUAL PUBLIC-FIGURE GATE — DRY RUN, NOTHING WRITTEN ===")
    print(f"cohort: {r['cohort_pairs']} distinct registrant->entity pairs "
          f"over {r['aircraft_in_cohort']} aircraft ({r['cohort_rows']} matched rows)")

    print(f"\n-- WOULD SURFACE ({len(r['would_surface'])})")
    if not r["would_surface"]:
        print("    (none — every individual fails at least one gate)")
    for p in r["would_surface"]:
        print(_fmt(p))

    print(f"\n-- NEEDS TONY JUDGMENT ({len(r['needs_judgment'])}) — plausible, not confident; DEFAULTED TO DROP")
    if not r["needs_judgment"]:
        print("    (none)")
    for p in r["needs_judgment"]:
        print(_fmt(p))

    print(f"\n-- DROPPED ({len(r['drops'])}) by reason")
    for code, n in sorted(r["drop_counts"].items()):
        print(f"    {code:<22} {n:>4}")

    print("\n-- DROP DETAIL (grouped by reason)")
    by_reason: dict[str, list] = collections.defaultdict(list)
    for p in r["drops"]:
        by_reason[p["reason"]].append(p)
    for code in sorted(by_reason):
        print(f"\n  [{code}] ({len(by_reason[code])})")
        for p in by_reason[code]:
            print(_fmt(p))

    print("\nNOTHING WAS WRITTEN. No promotion, no staging, no resolution persisted.")


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="P3.1 individual gate dry run (read-only).")
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()
    _print(run(limit=args.limit))


if __name__ == "__main__":  # pragma: no cover
    _main()
