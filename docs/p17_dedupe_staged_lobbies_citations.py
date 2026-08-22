"""Dedupe (edge_id, citation_ref) on STAGED lobbies edges in this batch.

The fragment merge re-points a fragment edge's citations onto the
anchor's edge without deduping by ref, so a filing both edges cited
lands on the survivor twice -- and the weight, which is a COUNT of
filings, counts it twice.

Scoped to publication_state='staged' and this batch only. The identical
artefact on PUBLISHED edges (43 rows, from the P1.6.3 merge) is NOT
touched: that is live data and helen's call.
"""
import asyncio, os, sys
from sqlalchemy import text
from app.db import get_sessionmaker

BATCH = os.environ["BATCH_ID"]
APPLY = "--apply" in sys.argv

async def main():
    sm = get_sessionmaker()
    async with sm() as s:
        if not APPLY:
            await s.execute(text("SET TRANSACTION READ ONLY"))
        rows = (await s.execute(text("""
            select e.id edge_id, s.canonical_name src, t.canonical_name tgt,
                   e.weight, sc.citation_ref, count(*)-1 as extra
            from source_citations sc
            join canonical_edges e on e.id=sc.edge_id
            join canonical_entities s on s.id=e.source_id
            join canonical_entities t on t.id=e.target_id
            where e.relation='lobbies' and e.publication_state='staged'
              and e.batch_id=:b
            group by 1,2,3,4,5 having count(*)>1"""), {"b": BATCH})).mappings().all()
        per_edge = {}
        for r in rows:
            per_edge.setdefault((r["edge_id"], r["src"], r["tgt"], float(r["weight"])), 0)
            per_edge[(r["edge_id"], r["src"], r["tgt"], float(r["weight"]))] += r["extra"]
        print(f"staged lobbies edges with duplicate refs: {len(per_edge)}")
        total = 0
        for (eid, src, tgt, w), extra in sorted(per_edge.items(), key=lambda kv: -kv[1]):
            total += extra
            print(f"  {src} -> {tgt[:44]:44s} weight {w:>5.0f} -> {w-extra:>5.0f}  drop {extra} dup rows")
        print(f"\ntotal duplicate citation rows to delete: {total}")
        if not APPLY:
            print("DRY RUN (read-only). Re-run with --apply."); return
    async with sm() as s:
        # Keep the lowest id per (edge_id, citation_ref); drop the rest.
        res = await s.execute(text("""
            delete from source_citations sc
            using canonical_edges e
            where sc.edge_id = e.id
              and e.relation='lobbies' and e.publication_state='staged'
              and e.batch_id=:b
              and sc.id <> (
                select min(sc2.id) from source_citations sc2
                where sc2.edge_id=sc.edge_id and sc2.citation_ref=sc.citation_ref)"""),
            {"b": BATCH})
        print("deleted rows:", res.rowcount)
        for (eid, src, tgt, w), extra in per_edge.items():
            await s.execute(text("update canonical_edges set weight = weight - :d where id=:e"),
                            {"d": float(extra), "e": eid})
        await s.commit()
        after = (await s.execute(text("""
            select count(*) n, round(sum(weight)::numeric,2) w from canonical_edges
            where relation='lobbies' and publication_state='staged' and batch_id=:b"""),
            {"b": BATCH})).first()
        print(f"AFTER: staged lobbies {after[0]} edges, weight {after[1]}")
        chk = (await s.execute(text("""
            select count(*) from (
              select sc.edge_id, sc.citation_ref from source_citations sc
              join canonical_edges e on e.id=sc.edge_id
              where e.relation='lobbies' and e.publication_state='staged' and e.batch_id=:b
              group by 1,2 having count(*)>1) x"""), {"b": BATCH})).scalar()
        print("remaining duplicate (edge,ref) pairs in batch:", chk)
asyncio.run(main())
