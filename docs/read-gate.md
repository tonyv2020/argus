# Read-gate hardening — operator guide (RG, 2026-08-07)

**Design doc (helen):** `helen-k3s/docs/argus-read-gate-hardening-design.md`.

This is the operator-facing companion to the design doc — what changed on
disk, where the gate points are, and the exact command sequence for a
bulk-ingest publish/unpublish.

## Why

Argus public reads went straight to Postgres with no content-lifecycle
gate — the instant `disclosure_emit` wrote an edge it was public. The
Trump-annual ingest was HELD only by convention. The read-gate makes the
HOLD a hard filter:

- Bulk ingests stage rows into the DB with `publication_state='staged'`
  + a caller-supplied `batch_id`.
- Public reads (search, dossier, subgraph, flow, ranking) fail-closed on
  `publication_state='published'` — staged rows are invisible.
- Publish is one atomic admin op, gated by an operator token AND the
  scrutiny precondition. Unpublish rolls back instantly.

Steady-state emitters (news cooccurrence, FEC, LDA, USAspending) are
unaffected — they keep writing `published` via the column default.

## What lives where

| Piece | File |
| --- | --- |
| Migration (`publication_state` + `batch_id`) | `alembic/versions/0009_read_gate_publication_state.py` |
| `PublicationState` enum + mapped columns | `app/models.py` |
| Shared read-path helper | `app/services/read_gate.py` |
| Wiring — search / dossier / subgraph / model1 / model2 / _entity_importance | `app/main.py`, `app/services/graph/pgvector_store.py`, `app/services/flow_query.py` |
| Emitter staging + `batch_id` | `app/services/ingest/disclosure_emit.py` |
| Admin control surface | `app/main.py` (`admin_publish_batch` + `admin_unpublish_batch`) |
| Token config | `app/config.py` (`argus_service_token`, env `ARGUS_SERVICE_TOKEN`) |
| k8s secret wiring | `k8s/base/api.yaml` (optional secretRef `argus-service-token`) |
| Tests | `tests/test_read_gate.py`, `tests/test_admin_batches.py` |

## Split-default (the one gotcha)

- **Column** default = `published` — migrating leaves the whole corpus
  live. If default were `staged`, migration would dark the whole graph
  = outage.
- **Emitter** default = `staged` — only bulk disclosure ingests stage.
  Steady-state emitters keep writing `published`.

## Read-path gate audit

Every public read path applies the gate via a single greppable helper:

```
git grep -E 'published_(entity|edge)|is_published_entity' app/
```

Six call sites (plus the helper module) — search (×3 queries),
dossier (404-on-staged + edge pull), subgraph (delegates to
`PgVectorStore.get_entity_subgraph` — outbound + inbound + node
pull + edge pull + dangling-edge guard), model1 (×4 CanonicalEdge
selects), model2 (×5), `_entity_importance` (×2).

## Bulk ingest workflow

**1. Run the emitter with a batch id.** Auto-generate one if you
don't need to reuse it:

```bash
python -m app.services.ingest.disclosure_emit --all --batch-id "$BATCH_ID"
```

`--all` shares one `batch_id` across every disclosure document so
publish lands atomically. Every net-new entity + net-new edge gets
`publication_state=staged` + `batch_id`. Pre-existing rows are not
touched.

**2. Sanity check the batch.**

```sql
SELECT COUNT(*) FROM canonical_entities WHERE batch_id = :b;
SELECT COUNT(*) FROM canonical_edges    WHERE batch_id = :b;
SELECT COUNT(*)
FROM canonical_entities ce
LEFT JOIN scrutiny_decisions sd ON sd.canonical_id = ce.id
WHERE ce.batch_id = :b AND sd.id IS NULL;   -- must reach 0 before publish
```

**3. Run scrutiny** on every batch entity (see `app/services/scrutiny.py`
+ the `run_scrutiny` ingester CLI). Every entity in the batch MUST
have a `scrutiny_decisions` row or `publish` will refuse 409.

**4. Publish the batch:**

```bash
curl -sS -X POST \
  -H "X-Argus-Service-Token: $ARGUS_SERVICE_TOKEN" \
  "https://argus.tonyvigna.com/api/admin/batches/$BATCH_ID/publish"
# → {"batch_id": "...", "edges_published": N, "entities_published": M}
```

If scrutiny is incomplete you get:

```
409 {"detail": {"reason": "scrutiny_incomplete",
                "batch_id": "...",
                "missing_scrutiny_count": K,
                "missing_scrutiny_ids": [<first 20 canonical ids>],
                "message": "..."}}
```

Rerun scrutiny for the missing ids, then re-issue publish. Re-publishing
an already-published batch returns zeros (no work; op is idempotent).

**5. Kill-switch (unpublish) — no scrutiny precondition:**

```bash
curl -sS -X POST \
  -H "X-Argus-Service-Token: $ARGUS_SERVICE_TOKEN" \
  "https://argus.tonyvigna.com/api/admin/batches/$BATCH_ID/unpublish"
# → {"batch_id": "...", "edges_unpublished": N, "entities_unpublished": M}
```

Unpublish is safe to run at any time (a safety op is always allowed to
hide, even without scrutiny state).

## Preview flag (privileged)

`?include_staged=1` on `/api/entities/{id}` or `/api/search` is silently
downgraded to `False` unless the caller also presents a matching
`X-Argus-Service-Token` header — the public path can never opt in. Use
it for pre-publish QA of a staged batch.

```bash
curl -sS -H "X-Argus-Service-Token: $ARGUS_SERVICE_TOKEN" \
  "https://argus.tonyvigna.com/api/entities/<id>?include_staged=1"
```

Under preview, dossier + search behave as if every row were
`published` for that one request. `/api/entities/{id}/subgraph`,
`/api/flow/*`, and `_entity_importance` do NOT expose a preview flag
(explicit spec §RG4 decision — analytical endpoints are always public-only).

## Privacy discipline (unchanged)

`publication_state` is orthogonal to `surface_mode`. Both gates AND at
read time:

- A `published` + `suppress` entity is still 404.
- A `published` + `alias` entity still returns `public_alias`.
- Receipts + `edge_metadata` require BOTH endpoints to be `surface_mode=open`
  (D4 rule preserved).

The 409 body from publish carries `missing_scrutiny_ids` — canonical
ids only, never `canonical_name`. A suppressed person's name in an
error body would defeat the whole privacy stack.

## Config knobs

| Env var | Effect when set | Effect when empty (default) |
| --- | --- | --- |
| `ARGUS_SERVICE_TOKEN` | Admin ops accept a matching header (401 otherwise); `include_staged=1` honored under matching header | Admin ops refuse every call; `include_staged=1` silently ignored (fail-closed) |

## Rollback

The RG1 migration is additive-only. To revert:

```bash
alembic downgrade -1
```

drops both new columns + all three partial indexes. Then roll the api
image back to the pre-RG23 tag (`d4-1f0d91b`).

## Related design docs

- `helen-k3s/docs/argus-read-gate-hardening-design.md` — RG spec.
- `helen-k3s/docs/argus-disclosure-ingestion-design.md` — D1–D4 context.
- `helen-k3s/docs/argus-ontology-navigator-design.md` — the P0–P4 base.
