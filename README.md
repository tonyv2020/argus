# Argus — Ontology Navigator

Argus is a decoupled ontology + relationship graph over the Tony Times content plus public
accountability data (FEC, USAspending, Senate LDA), surfaced as a *"who/what is connected to X"*
navigator with an expandable, cited graph.

**Every edge is cited to a filing ID or an article permalink** — a relationship with no source is
never shown as fact. Read-only, fully public.

Design: `helen-k3s/docs/argus-ontology-navigator-design.md`.

## What lives here

- **FastAPI** backend with per-entity profiles + subgraph endpoints.
- **Postgres** = canonical entity registry + raw source records + pgvector for cosine resolution.
- **Neo4j** = the navigable projection.
- **Frontend** (Cytoscape) = expandable relationship graph (P2).

## Reuse, not import

Argus reuses proven patterns from `legal-lab` (Neo4j projection + pgvector≥0.86 canonical resolution +
Cytoscape viz). It shares zero code — own repo, own DB, own deploy.

## Phases

- **P0** — scaffold + resolve `hollywood.entity_tags` (208K rows) → canonical entities + derive
  news-cooccurrence `MENTIONED_WITH` edges cited to permalinks + project to Neo4j.
- **P1** — FEC + USAspending (scoped to GEO Group MVP) + the Scrutiny Agent (design §5.4;
  public-vs-private tiered bar; gates every real-person surfacing).
- **P2** — profile + expandable Cytoscape UI; GEO Group profile end-to-end; live on achilles k3s
  (namespace `argus`, LAN-registry images).
- **P3** — Senate LDA + corporate registry + broaden past GEO Group.
- **P3b/P4 (coverage expansion, 2026-07-19)** — parameterized FEC + USAspending
  ingesters over the detention-industry anchor set (GEO Group + CoreCivic +
  Management & Training Corp + LaSalle Corrections). Back-compat GEO-only
  wrappers retained. Design: `helen-k3s/docs/argus-coverage-expansion-design.md`.

## The Scrutiny Agent (P1)

Before any real-person node or edge surfaces, an automated agent classifies public vs private
figure, applies a tiered bar (very-high for private, medium-high for public), and logs its reason
(auditable). Sonnet-floor LLM. Design §5.4.

## Read-gate + publication lifecycle (RG, 2026-08-07)

Every canonical entity + edge carries a `publication_state` (`published` | `staged`) plus an
optional `batch_id`. `published` rows are live on the public read path; `staged` rows are
invisible to `/api/search`, `/api/entities/{id}` (404), `/api/entities/{id}/subgraph`,
`/api/flow/model1`, `/api/flow/model2`, and the internal ranking-signal `_entity_importance`.

The column default is `published` so the whole existing corpus stays live; only bulk-disclosure
ingests (see `app/services/ingest/disclosure_emit.py`) stamp `staged` on net-new rows and pass a
caller-supplied `batch_id` grouping tag. Steady-state emitters (news cooccurrence, FEC, LDA,
USAspending) keep writing `published` via the column default.

Admin operators publish a bulk batch atomically once scrutiny has run:

```
POST /api/admin/batches/{batch_id}/publish     (X-Argus-Service-Token required)
POST /api/admin/batches/{batch_id}/unpublish   (kill-switch — same token)
```

`publish` refuses `409 {reason: "scrutiny_incomplete", missing_scrutiny_count, ...}` when any
batch entity lacks a `scrutiny_decisions` row. `unpublish` has no precondition (a safety op is
always allowed to hide). Both return `{batch_id, edges_(un)published, entities_(un)published}`
and are idempotent.

The privileged preview flag `?include_staged=1` on `/api/entities/{id}` and `/api/search` is
silently ignored unless the caller also presents a matching `X-Argus-Service-Token` header —
the public path can never opt into staged content.

`publication_state` is orthogonal to `surface_mode` — both gates AND at read time. A
`published` + `suppress` entity is still 404. The receipts / edge_metadata open-on-both-ends
rule (D4) is untouched.

Full operator guide: [`docs/read-gate.md`](docs/read-gate.md).

## P2 dedup/merge pass (2026-08-21)

`app/services/ingest/dedup_pass.py` is a re-runnable, idempotent fragmentation
cleanup: the same real-world entity is spread across several canonicals
(`Tesla` as person + unknown + organization, `xAI` as organization + concept),
and 32% of the registry is typed `unknown`. The pass resolves fragments to one
canonical, re-points every edge, citation, alias, anchor and scrutiny decision
onto the survivor, and deletes the emptied node.

Everything in the corpus is `publication_state=published`, so **run it
read-only first**. `--dry-run` (the default) opens its transaction with
`SET TRANSACTION READ ONLY` and emits a before/after report; `--apply` is the
only mode that writes.

```
python -m app.services.ingest.dedup_pass --dry-run --enable-vector
python -m app.services.ingest.dedup_pass --apply          # destructive
kubectl -n argus apply -f k8s/base/dedup-job.yaml         # dry-run job
```

**Fail-closed on privacy:** a candidate pair whose members differ in
`surface_mode` is never merged — it is skipped and logged to the review list,
with no flag to override. The same partition applies to `publication_state`.
Type is upgraded only on a reliable signal (authoritative external id, or a
single real type among the cluster members); anything else keeps the
survivor's type and is flagged for review. Rules and their refusal buckets are
documented in the module docstring.
