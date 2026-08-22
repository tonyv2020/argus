"""Undo P1.7's accidental writes to LIVE PUBLISHED Musk edges.

This restores the pre-P1.7 state exactly. It is NOT the repair of the
pre-existing 7x inflation -- that stays untouched and is helen's call.

Keyed on the citations this run created (seen_at inside the run window)
on edges that are PUBLISHED. Each such citation added its FEC amount
(contributes_to) or 1.0 (affiliated_with) to the edge weight, so the
restore subtracts exactly that and deletes the citation.
"""
import asyncio, os, sys, json
from sqlalchemy import text
from app.db import get_sessionmaker

DONOR = "508182fd-3c6a-472e-b6c8-8a5ba609b72c"
WINDOW = os.environ.get("WINDOW_MIN", "90")
APPLY = "--apply" in sys.argv
TARGET_CONTRIB = 2049497166.36   # published contributes_to total, pre-P1.7

async def main():
    sm = get_sessionmaker()
    async with sm() as s:
        if not APPLY:
            await s.execute(text("SET TRANSACTION READ ONLY"))
        rows = (await s.execute(text(f"""
            select sc.id cid, sc.edge_id, sc.citation_ref, e.relation,
                   e.weight, e.publication_state
            from source_citations sc join canonical_edges e on e.id=sc.edge_id
            where e.source_id=:d and e.publication_state='published'
              and e.relation in ('contributes_to','affiliated_with')
              and sc.seen_at > now() - interval '{WINDOW} minutes'
        """), {"d": DONOR})).mappings().all()
        print(f"citations to undo: {len(rows)}")
        by_rel = {}
        for r in rows: by_rel[r["relation"]] = by_rel.get(r["relation"],0)+1
        print("  by relation:", by_rel)

        # amounts: contributes_to needs the FEC amount per sub_id
        amounts = {}
        if any(r["relation"]=="contributes_to" for r in rows):
            import urllib.request, urllib.parse, time
            KEY=os.environ["FEC_API_KEY"]
            need={r["citation_ref"] for r in rows if r["relation"]=="contributes_to"}
            for per in [2026,2024,2022,2020,2018,2016,2014,2012,2010,2008]:
                idx=last=None
                while True:
                    q=dict(api_key=KEY, contributor_name="MUSK, ELON",
                           two_year_transaction_period=per, per_page=100,
                           sort="-contribution_receipt_date")
                    if idx: q["last_index"]=idx; q["last_contribution_receipt_date"]=last
                    u="https://api.open.fec.gov/v1/schedules/schedule_a/?"+urllib.parse.urlencode(q)
                    with urllib.request.urlopen(u, timeout=120) as fh: d=json.load(fh)
                    res=d.get("results",[])
                    for r in res:
                        amounts[str(r.get("sub_id"))]=float(r.get("contribution_receipt_amount") or 0)
                    pg=(d.get("pagination") or {}).get("last_indexes") or {}
                    idx=pg.get("last_index"); last=pg.get("last_contribution_receipt_date")
                    if not res or not idx: break
                    time.sleep(0.25)
            missing=need-set(amounts)
            if missing:
                print("REFUSING: no FEC amount for", len(missing), "sub_ids"); return

        delta={}
        for r in rows:
            amt = 1.0 if r["relation"]=="affiliated_with" else amounts[r["citation_ref"]]
            delta[r["edge_id"]] = delta.get(r["edge_id"],0.0)+amt
        print(f"\nedges affected: {len(delta)}")
        print(f"total weight to subtract: ${sum(delta.values()):,.2f}")
        cur = (await s.execute(text("""
            select relation, round(sum(weight)::numeric,2) w, count(*) n from canonical_edges
            where source_id=:d and publication_state='published'
              and relation in ('contributes_to','affiliated_with') group by 1"""),
            {"d": DONOR})).all()
        print("\nBEFORE:", [(r[0], float(r[1]), r[2]) for r in cur])
        proj = {}
        for rel,w,n in cur: proj[rel]=float(w)
        for eid,d_ in delta.items():
            rel=[r["relation"] for r in rows if r["edge_id"]==eid][0]
            proj[rel]-=d_
        print("PROJECTED:", {k: round(v,2) for k,v in proj.items()})
        print(f"target contributes_to: ${TARGET_CONTRIB:,.2f}  -> match:",
              abs(proj.get("contributes_to",0)-TARGET_CONTRIB) < 0.005)

        if not APPLY:
            print("\nDRY RUN (read-only). Re-run with --apply."); return
        if abs(proj.get("contributes_to",0)-TARGET_CONTRIB) >= 0.005:
            print("REFUSING to apply: projection does not land on the pre-P1.7 total"); return
        for eid,d_ in delta.items():
            await s.execute(text("update canonical_edges set weight = weight - :d where id=:e"),
                            {"d": d_, "e": eid})
        await s.execute(text(f"""
            delete from source_citations sc using canonical_edges e
            where sc.edge_id=e.id and e.source_id=:d and e.publication_state='published'
              and e.relation in ('contributes_to','affiliated_with')
              and sc.seen_at > now() - interval '{WINDOW} minutes'"""), {"d": DONOR})
        await s.commit()
        after = (await s.execute(text("""
            select relation, round(sum(weight)::numeric,2) w, count(*) n from canonical_edges
            where source_id=:d and publication_state='published'
              and relation in ('contributes_to','affiliated_with') group by 1"""),
            {"d": DONOR})).all()
        print("\nAFTER:", [(r[0], float(r[1]), r[2]) for r in after])
asyncio.run(main())
