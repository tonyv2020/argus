"""P2 dry run — FAA registrant → canonical match CANDIDATES. Read-only.

**This module writes nothing.** No edge, no canonical, no staged row,
no column update. It opens a ``READ ONLY`` transaction so the database
itself refuses a write, scores candidate matches, and prints a
distribution for review. Creating a ``REGISTERS`` edge is a separate
step that does not exist yet and must not until these numbers are
reviewed.

Why the caution: resolving 316k registrant names against 63k
canonicals is the same shape as the FEC fuzzy-name misattribution
that produced the false $140M Thiel claim. The failure is not a crash
— it is a confident wrong edge attached to a named person. So the dry
run is built to surface the ways it would be wrong rather than to
maximise matches:

  * **Ambiguity** — a normalized name resolving to >1 canonical is
    reported, never silently resolved to the first.
  * **Cross-type** — an Individual registrant matching an
    organization (or vice versa) is counted separately; it usually
    means a shared surname, not a real link.
  * **Single-token** — "SMITH" matching a canonical is the classic
    false positive and is scored/reported apart from everything else.
  * **Privacy** — candidates landing on ``suppress``/``alias``
    canonicals are counted separately. Surfacing those is Tony's
    call, not a scoring threshold.

Tiers are deliberately conservative — exact and token-set only. No
edit distance, no embedding, no nickname table. A tighter net that
reports its own misses is worth more here than a wider one whose
false positives have to be found later on live published data.
"""

from __future__ import annotations

import argparse
import collections
import logging
import os

import psycopg

from app.services.graph.base import normalize_name

logger = logging.getLogger(__name__)

_DUMP_ALL = bool(os.environ.get("FAA_DUMP_ALL_PAIRS"))

#: FAA TYPE REGISTRANT -> the canonical entity types a match may
#: plausibly land on. Anything else is counted as cross-type.
_EXPECTED_TYPES: dict[str, set[str]] = {
    "1": {"person", "candidate"},           # Individual
    "4": {"person", "candidate"},           # Co-Owned
    "2": {"organization", "pac"},           # Partnership
    "3": {"organization", "pac"},           # Corporation
    "5": {"agency", "organization"},        # Government
    "7": {"organization", "pac"},           # LLC
    "8": {"organization", "pac"},           # Non-Citizen Corporation
    "9": {"organization", "pac"},           # Non-Citizen Co-Owned
}

#: Confidence per tier. These are candidate scores for REVIEW, not
#: an auto-accept threshold — nothing in P2 auto-accepts.
SCORE_EXACT_CANONICAL = 1.00
SCORE_EXACT_ALIAS = 0.90
SCORE_TOKEN_SET = 0.75


def _load_canonical_index(cur) -> tuple[dict, dict]:
    """Build exact + token-set indexes over canonicals and their aliases.

    Reuses ``normalize_name`` — the same function that produced
    ``canonical_name_normalized`` — so the dry run's keys mean the
    same thing the stored ones do.
    """
    exact: dict[str, list[tuple]] = collections.defaultdict(list)
    tokenset: dict[frozenset, list[tuple]] = collections.defaultdict(list)

    # Edge count is a review signal, not a filter: a canonical with 0
    # edges is a thin news-tag entity Argus barely knows, and attaching
    # a fleet to one is a weaker claim than the score alone suggests.
    cur.execute(
        "select e.id, e.type, e.surface_mode, e.canonical_name, e.canonical_name_normalized, "
        "  (select count(*) from canonical_edges g "
        "   where g.source_id=e.id or g.target_id=e.id) "
        "from canonical_entities e"
    )
    for cid, ctype, smode, name, norm, edges in cur.fetchall():
        key = norm or normalize_name(name)
        rec = (cid, ctype, smode, name, "canonical", name, edges)
        exact[key].append(rec)
        toks = frozenset(key.split())
        if len(toks) >= 2:
            tokenset[toks].append(rec)

    # NOTE: display the CANONICAL entity's name, not the alias text.
    # Showing the alias makes a review sample lie — "U.S. Department of
    # Energy" is an alias string, and the reviewer needs to see which
    # entity it actually resolves to.
    cur.execute(
        "select a.canonical_id, e.type, e.surface_mode, e.canonical_name, "
        "       a.surface_name_normalized, a.surface_name, "
        "  (select count(*) from canonical_edges g "
        "   where g.source_id=e.id or g.target_id=e.id) "
        "from entity_aliases a join canonical_entities e on e.id = a.canonical_id"
    )
    for cid, ctype, smode, cname, norm, alias_name, edges in cur.fetchall():
        key = norm or normalize_name(alias_name)
        rec = (cid, ctype, smode, cname, "alias", alias_name, edges)
        exact[key].append(rec)
        toks = frozenset(key.split())
        if len(toks) >= 2:
            tokenset[toks].append(rec)

    return exact, tokenset


def run_dryrun(limit: int | None = None, examples: int = 12) -> dict:
    """Score match candidates and return the distribution. Writes nothing."""
    url = os.environ["DATABASE_URL_SYNC"].replace("+psycopg", "")
    stats: dict = {
        "aircraft_rows": 0,
        "with_registrant_name": 0,
        "distinct_registrant_names": 0,
        "candidates": 0,
        "index_hits": 0,
        "rows_with_candidate": 0,
        "by_tier": collections.Counter(),
        "score_hist": collections.Counter(),
        "ambiguous_rows": 0,
        "resolved_by_tier_rows": 0,
        "cross_type_rows": 0,
        "single_token_rows": 0,
        "privacy_sensitive_rows": collections.Counter(),
        "by_registrant_type": collections.Counter(),
        "org_examples": [],
        "cohorts": collections.Counter(),
        "eligible_by_tier": collections.Counter(),
        "eligible_pairs": {},
    }

    with psycopg.connect(url) as conn:
        # Belt and braces: the database refuses a write in this session,
        # so a stray INSERT in future edits fails loudly rather than
        # quietly creating an edge nobody reviewed.
        conn.read_only = True
        cur = conn.cursor()

        logger.info("loading canonical index…")
        exact, tokenset = _load_canonical_index(cur)
        logger.info("indexed %d exact keys, %d token-set keys", len(exact), len(tokenset))

        sql = (
            "select unique_id, n_number, registrant_name, type_registrant "
            "from aircraft where registrant_name is not null"
        )
        if limit:
            sql += f" limit {int(limit)}"
        cur.execute("select count(*) from aircraft")
        stats["aircraft_rows"] = cur.fetchone()[0]
        cur.execute("select count(distinct registrant_name) from aircraft")
        stats["distinct_registrant_names"] = cur.fetchone()[0]

        cur.execute(sql)
        for _uid, nnum, rname, rtype in cur:
            stats["with_registrant_name"] += 1
            norm = normalize_name(rname)
            if not norm:
                continue
            toks = frozenset(norm.split())

            hits: list[tuple] = []
            for rec in exact.get(norm, []):
                score = (
                    SCORE_EXACT_CANONICAL if rec[4] == "canonical" else SCORE_EXACT_ALIAS
                )
                hits.append((score, rec))
            if not hits and len(toks) >= 2:
                for rec in tokenset.get(toks, []):
                    hits.append((SCORE_TOKEN_SET, rec))

            if not hits:
                continue

            # Tier follows the BEST score, not whichever hit happened to
            # come last — a row matching a canonical AND its alias is an
            # exact-canonical match, not an alias one.
            best = round(max(h[0] for h in hits), 2)
            tier = {
                SCORE_EXACT_CANONICAL: "exact_canonical",
                SCORE_EXACT_ALIAS: "exact_alias",
                SCORE_TOKEN_SET: "token_set",
            }[best]

            # distinct canonicals, not distinct index entries — an entity
            # matching by both its name and its alias is one candidate.
            distinct_ids = {rec[0] for _s, rec in hits}
            # Ambiguity that MATTERS is a tie at the best score. Hits at
            # a lower tier are already resolved by the score: the live
            # case is "AMERICAN AIRLINES INC", which matches the carrier
            # canonically (1.00) and its parent "American Airlines Group"
            # only by alias (0.90). Those are two real entities, not a
            # duplicate — the carrier is the right answer and the score
            # already says so. Counting them as ambiguous overstates the
            # problem and would argue for a merge that must not happen.
            best_ids = {rec[0] for s, rec in hits if round(s, 2) == best}

            stats["rows_with_candidate"] += 1
            # Count distinct canonicals; raw index hits overstate by the
            # number of aliases a big organisation happens to carry.
            stats["candidates"] += len(distinct_ids)
            stats["index_hits"] += len(hits)
            stats["by_tier"][tier] += 1
            stats["score_hist"][best] += 1
            stats["by_registrant_type"][rtype or "(null)"] += 1
            if len(best_ids) > 1:
                stats["ambiguous_rows"] += 1
            elif len(distinct_ids) > 1:
                stats["resolved_by_tier_rows"] += 1
            if len(toks) == 1:
                stats["single_token_rows"] += 1

            expected = _EXPECTED_TYPES.get(rtype or "", set())
            if expected and not any(rec[1] in expected for _s, rec in hits):
                stats["cross_type_rows"] += 1

            for _s, rec in hits:
                if rec[2] in ("suppress", "alias"):
                    stats["privacy_sensitive_rows"][rec[2]] += 1
                    break

            cohort = classify(rtype, toks, hits, best, best_ids, distinct_ids)
            stats["cohorts"][cohort] += 1
            if cohort == "ELIGIBLE":
                stats["eligible_by_tier"][tier] += 1
            if cohort == "ELIGIBLE":
                cid, ctype, _sm, cname, via, matched_via, edges = max(
                    hits, key=lambda h: h[0]
                )[1]
                key = (rname, cid)
                rec = stats["eligible_pairs"].get(key)
                if rec is None:
                    stats["eligible_pairs"][key] = {
                        "registrant": rname, "canonical": cname,
                        "canonical_type": ctype, "tier": tier, "score": best,
                        "rtype": rtype, "n_number": nnum, "aircraft": 1,
                        "matched_via": matched_via, "edges": edges,
                    }
                else:
                    rec["aircraft"] += 1

            # Examples are ORGANISATION-typed registrants only. An
            # individual's name plus tail number is exactly the linkage
            # the fence exists to withhold, so it never goes in a report.
            if (
                len(stats["org_examples"]) < examples
                and rtype in ("2", "3", "7", "8")
                and not any(rec[2] in ("suppress", "alias") for _s, rec in hits)
            ):
                cid, ctype, smode, cname, via, _mv, _ed = hits[0][1]
                stats["org_examples"].append(
                    {
                        "n_number": nnum,
                        "registrant": rname,
                        "canonical": cname,
                        "canonical_type": ctype,
                        "score": hits[0][0],
                        "via": via,
                        "n_candidates": len(best_ids),
                    }
                )

    return stats


# ─── cohort selection (the gate helen approves) ──────────────────

#: FAA registrant types that denote a natural person. Every match on
#: one of these is HELD for Tony regardless of score — attaching a
#: tail number to a named individual is the privacy decision itself.
_INDIVIDUAL_TYPES = {"1", "4"}


def classify(rtype, toks, hits, best, best_ids, distinct_ids):
    """Return the cohort this matched row belongs to.

    Order matters: a row can be disqualified several ways and the
    STRONGEST reason wins, so a suppressed individual is reported as a
    hold rather than quietly dropped as cross-type.
    """
    if any(rec[2] in ("suppress", "alias") for _s, rec in hits):
        return "HOLD_privacy"
    if rtype in _INDIVIDUAL_TYPES:
        return "HOLD_individual"
    if len(toks) == 1:
        return "DROP_single_token"
    expected = _EXPECTED_TYPES.get(rtype or "", set())
    if expected and not any(rec[1] in expected for _s, rec in hits):
        return "DROP_cross_type"
    if len(best_ids) > 1:
        return "DROP_true_tie"
    if best < SCORE_EXACT_ALIAS:
        return "DROP_below_tier"
    return "ELIGIBLE"


def sample_eligible(pairs: dict, want: int = 40) -> list[dict]:
    """Deterministic sample of distinct registrant->entity pairs.

    Deduped by pair, because 12 rows of the same airline tell a
    reviewer nothing that one row does not. Deliberately over-weights
    the weaker 0.90 tier relative to its share — that is the tier whose
    quality is actually in question.
    """
    by_tier = {"exact_canonical": [], "exact_alias": []}
    for key in sorted(pairs):
        rec = pairs[key]
        if rec["tier"] in by_tier:
            by_tier[rec["tier"]].append(rec)
    want_alias = min(len(by_tier["exact_alias"]), max(1, want // 3))
    want_canon = want - want_alias
    out = []
    for tier, n in (("exact_canonical", want_canon), ("exact_alias", want_alias)):
        rows = by_tier[tier]
        if not rows:
            continue
        stride = max(1, len(rows) // max(1, n))
        out.extend(rows[::stride][:n])
    return out


def _print(stats: dict) -> None:  # pragma: no cover
    n = stats["rows_with_candidate"]
    print("\n=== P2 MATCH DRY RUN — READ ONLY, NOTHING WRITTEN ===")
    print(f"aircraft rows                {stats['aircraft_rows']:,}")
    print(f"  with registrant_name       {stats['with_registrant_name']:,}")
    print(f"  distinct registrant names  {stats['distinct_registrant_names']:,}")
    print(f"rows with >=1 candidate      {n:,}"
          f"  ({100.0*n/max(1,stats['with_registrant_name']):.1f}% of named rows)")
    print(f"distinct candidate pairs     {stats['candidates']:,}"
          f"   (raw index hits {stats['index_hits']:,} — inflated by alias rows)")

    print("\n-- score distribution (best score per row)")
    for score in sorted(stats["score_hist"], reverse=True):
        c = stats["score_hist"][score]
        print(f"   {score:.2f}  {c:>8,}  {'#' * max(1, int(40 * c / max(1, n)))}")

    print("\n-- by tier")
    for tier, c in stats["by_tier"].most_common():
        print(f"   {tier:<18} {c:>8,}")

    print("\n-- REVIEW FLAGS (why a match may be wrong)")
    print(f"   ambiguous AT BEST SCORE    {stats['ambiguous_rows']:>8,}   <- the real problem")
    print(f"   multi-candidate, resolved  {stats['resolved_by_tier_rows']:>8,}   "
          f"(lower-tier hits the score already settles, e.g. carrier vs parent)")
    print(f"   cross-type registrant      {stats['cross_type_rows']:>8,}")
    print(f"   single-token name          {stats['single_token_rows']:>8,}")
    for mode, c in stats["privacy_sensitive_rows"].items():
        print(f"   candidate is {mode:<13} {c:>8,}")

    print("\n-- by FAA type_registrant")
    for t, c in sorted(stats["by_registrant_type"].items()):
        print(f"   {t:<4} {c:>8,}")

    print("\n-- COHORTS (the gate)")
    for name in ("ELIGIBLE", "HOLD_privacy", "HOLD_individual", "DROP_single_token",
                 "DROP_cross_type", "DROP_true_tie", "DROP_below_tier"):
        print(f"   {name:<20} {stats['cohorts'].get(name, 0):>8,}")
    pairs = stats["eligible_pairs"]
    print("   ELIGIBLE split by tier (rows / distinct pairs):")
    for t in ("exact_canonical", "exact_alias"):
        npairs = sum(1 for r in pairs.values() if r["tier"] == t)
        thin = sum(1 for r in pairs.values() if r["tier"] == t and r["edges"] == 0)
        print(f"      {t:<17} {stats['eligible_by_tier'].get(t,0):>7,} rows / "
              f"{npairs:>4} pairs  ({thin} of those pairs have a 0-edge canonical)")
    print(f"   distinct eligible registrant->entity pairs: {len(pairs):,}")

    print("\n-- SAMPLE FOR REVIEW (distinct pairs; organisations only)")
    print(f"   {'registrant_name':<38} {'canonical entity':<30} {'score':<6} "
          f"{'edges':>5} {'#ac':>5}  matched_via (if alias)")
    for r in sample_eligible(pairs, want=40):
        via = "" if r["tier"] == "exact_canonical" else r["matched_via"][:26]
        flag = "  <-- 0 edges" if r["edges"] == 0 else ""
        print(f"   {r['registrant'][:38]:<38} {r['canonical'][:30]:<30} "
              f"{r['score']:<6.2f} {r['edges']:>5} {r['aircraft']:>5}  {via}{flag}")

    if _DUMP_ALL:
        print("\n-- ALL ELIGIBLE 1.00 PAIRS WITH A 0-EDGE CANONICAL (collision judgment)")
        rows = [r for r in pairs.values()
                if r["tier"] == "exact_canonical" and r["edges"] == 0]
        for r in sorted(rows, key=lambda x: x["registrant"]):
            print(f"   {r['registrant'][:44]:<44} | {r['canonical'][:40]:<40} | "
                  f"ac={r['aircraft']}")
        print(f"   ({len(rows)} pairs)")

    print("\n-- examples (ORGANISATION registrants only; individuals withheld)")
    for ex in stats["org_examples"]:
        print(f"   N{ex['n_number']:<7} {ex['score']:.2f} {ex['via']:<9} "
              f"cand={ex['n_candidates']}  {ex['registrant'][:38]:<38} -> "
              f"{ex['canonical'][:34]} [{ex['canonical_type']}]")
    print("\nNOTHING WAS WRITTEN. No edge, no canonical, no staged row.")


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="P2 match dry run (read-only).")
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--examples", type=int, default=12)
    args = ap.parse_args()
    _print(run_dryrun(limit=args.limit, examples=args.examples))


if __name__ == "__main__":  # pragma: no cover
    _main()
