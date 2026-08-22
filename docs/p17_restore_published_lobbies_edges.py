"""Undo P1.7's writes to LIVE PUBLISHED `lobbies` edges.

senate_lda's reuse branch was ungated, so the STAGED Musk lobbying sweep
added citations (and +1.0 weight each) to published Tesla edges. Each
such citation added exactly 1.0, so the restore subtracts the count and
deletes the citations. Restores the PRE-P1.7 state only.
"""
import asyncio, os, sys
from sqlalchemy import text
from app.db import get_sessionmaker

WINDOW = os.environ.get("WINDOW_MIN", "90")
APPLY = "--apply" in sys.argv
EXPECT = {  # edge -> pre-P1.7 weight, measured before the run
    "Tesla|Tesla": 38.0,
    "Tesla|FOUNDERS POLICY GROUP": 2.0,
    "Tesla|CHAMBERS, CONLON & HARTWELL, LLC": 3.0,
    "Tesla|WEST FRONT STRATEGIES LLC": 15.0,
    "Tesla|BURTON STRATEGY GROUP": 4.0,
    "Tesla|RUBIN AND RUDMAN LLP": 3.0,
}

async def main():
    sm = get_sessionmaker()
    async with sm() as s:
        if not APPLY:
            await s.execute(text("SET TRANSACTION READ ONLY"))
        rows = (await s.execute(text(f"""
            select e.id, s.canonical_name src, t.canonical_name tgt, e.weight,
                   count(sc.id) new_cites
            from canonical_edges e
            join canonical_entities s on s.id=e.source_id
            join canonical_entities t on t.id=e.target_id
            join source_citations sc on sc.edge_id=e.id
            where e.relation='lobbies' and e.publication_state='published'
              and sc.seen_at > now() - interval '{WINDOW} minutes'
            group by e.id, s.canonical_name, t.canonical_name, e.weight
            order by 5 desc"""))).mappings().all()
    print(f"published lobbies edges touched: {len(rows)}")
    total = 0
    ok = True
    for r in rows:
        key = f"{r['src']}|{r['tgt']}"
        proj = float(r["weight"]) - r["new_cites"]
        want = EXPECT.get(key)
        match = want is not None and abs(proj - want) < 1e-9
        ok = ok and match
        total += r["new_cites"]
        print(f"  {key[:52]:52s} {r['weight']:>6.0f} -> {proj:>6.0f}  (want {want})  cites -{r['new_cites']}  {'OK' if match else 'MISMATCH'}")
    print(f"\ntotal citations to delete: {total}")
    if not APPLY:
        print("DRY RUN (read-only). Re-run with --apply."); return
    if not ok or len(rows) != len(EXPECT):
        print("REFUSING to apply: projection does not land on the recorded pre-P1.7 weights"); return
    async with sm() as s:
        for r in rows:
            await s.execute(text("update canonical_edges set weight = weight - :d where id=:e"),
                            {"d": float(r["new_cites"]), "e": r["id"]})
        await s.execute(text(f"""
            delete from source_citations sc using canonical_edges e
            where sc.edge_id=e.id and e.relation='lobbies'
              and e.publication_state='published'
              and sc.seen_at > now() - interval '{WINDOW} minutes'"""))
        await s.commit()
        after = (await s.execute(text("""
            select round(sum(weight)::numeric,2) w, count(*) n from canonical_edges
            where relation='lobbies' and publication_state='published'"""))).first()
        print(f"\nAFTER: published lobbies total weight={after[0]} across {after[1]} edges")
asyncio.run(main())
