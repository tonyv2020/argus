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

## P1.5 Congressional roster (2026-08-21)

`unitedstates/congress-legislators` is the authoritative crosswalk
(bioguide id + FEC candidate ids + party/state/chamber + name variants)
that makes all 537 current members first-class canonical people, so FEC
money and roll-call votes attribute to the **person** rather than to a
campaign-committee string.

```
python -m app.services.ingest.congress_roster      --batch-id p15-...
python -m app.services.ingest.congress_money_link  --batch-id p15-...
python -m app.services.ingest.run_scrutiny         --batch-id p15-...
python -m app.services.ingest.congress_person_merge --dry-run   # --apply writes
kubectl -n argus apply -f k8s/base/p15-roster-job.yaml           # see header
```

- **`congress_roster`** — external-ID-first resolution (bioguide →
  fec.candidate → a fail-closed exact-name fallback), name-variant
  aliases in the `congress.legislators` namespace, and a cited
  `held_position` member → chamber edge.
- **`congress_money_link`** — joins `contributes_to` to the member via
  FEC's own candidate-committee linkage bulk file (`ccl<yy>.zip`):
  committee id → candidate id → member canonical. Every hop is an
  external id; names never enter the join. A committee shared by two
  sitting members is refused, not guessed.
- **`congress_person_merge`** — collapses fragmented news-person nodes
  onto the member, reusing the P2 merge machinery. Dry-run by default.

**Read-gate:** `--batch-id` stamps every net-new entity and every edge
these passes create `publication_state=staged`. Nothing is public until
an operator runs `POST /api/admin/batches/{batch_id}/publish`, which
still refuses while any batch entity lacks a scrutiny verdict.

**Fail-closed on privacy:** a member identity is never attached to a
`suppress`/`alias` node, no fragment merge ever straddles `surface_mode`
(or `publication_state`), and the roster pass **never rewrites an
existing canonical's `surface_mode`** — members already sitting on a
protected node are reported for an operator decision, not silently
opened.

## P1.6 Surveillance / tech-influence domain (2026-08-21)

Thiel / Palantir / Founders Fund / Flock Safety / Clearview AI / Axon,
anchored on **external ids, never names**. The domain was already in the
graph incidentally and badly fragmented — `AXON` and `Axon Enterprise`
were two organizations, `Anduril` was typed `concept`, and four Senate
LDA *client records* (`BROWNSTEIN HYATT … OBO PALANTIR TECHNOLOGIES
INC.`, `J.A. GREEN AND COMPANY (FOR PALANTIR …)`, `HANNEGAN … (FOR
PALANTIR …)`, `BGR GOVERNMENT AFFAIRS … ON BEHALF OF FLOCK SAFETY`) were
separate canonicals each holding cited lobbying edges that never reached
the company's profile.

```
python -m app.services.ingest.domain_anchors  --domain surveillance --batch-id p16-...
python -m app.services.ingest.sec_insiders    --priority-domain surveillance --batch-id p16-...
python -m app.services.ingest.domain_sources  --domain surveillance --batch-id p16-...
python -m app.services.ingest.fec_individual  --donor thiel --batch-id p16-...
python -m app.services.ingest.domain_merge    --domain surveillance --dry-run   # --apply writes
python -m app.services.ingest.fec_individual  --donor thiel --repair            # --apply writes
kubectl -n argus apply -f k8s/base/p16-domain-job.yaml                          # see header
```

- **`domain_anchors`** — one declarative `AnchorSpec` per anchor, keyed
  on SEC issuer CIK, SEC Form 3/4/5 reporting-owner CIK, USAspending
  recipient UEI, Senate LDA client id, or FEC committee/candidate id.
  Resolution is external id → **fail-closed** exact-name fallback →
  fresh staged canonical. It emits no edges: an anchor is an identity.
- **`sec_insiders`** — Form 3/4/5 → cited `held_position` owner → issuer
  edges keyed on *(issuer CIK, reporting-owner CIK)*. This is what makes
  Thiel → Palantir a filing fact rather than a name match. A
  10 %-owner-only filer is not a position and is skipped.
- **`domain_sources`** — USAspending awards, with every row verified
  against the anchor's declared **`Recipient UEI`** (`recipient_search_
  text` is fuzzy and returns neighbours), plus Senate LDA filings gated
  on anchored client-name patterns.
- **`fec_individual`** — Schedule A individual-contributor mode keyed on
  a fail-closed identity **predicate** (see below).
- **`domain_merge`** — collapses the fragments onto the anchors, reusing
  the P2 merge machinery, and repairs mis-types from an authoritative id.

### The external-ID keyring

`anchor_registry.external_ids` (migration 0010) is one JSONB map —
`usaspending_uei`, `lda_client_ids`, `lda_registrant_ids`, `sec_ciks`,
`sec_owner_cik`, `lda_client_patterns` — so anchoring a new domain
against a new authority is a data edit, not a new column. LDA needs
patterns rather than an id allowlist because it mints a new `client.id`
per **registration**, not per company: "Palantir" has 32 client records
across 13 spellings, and a fixed allowlist goes stale the moment the
company hires another firm. Names select; ids key, and every id a
pattern resolves to is recorded back into `lda_client_ids`.

### Why individual-contributor mode needed a predicate (P1.6.2)

An individual contributor has no FEC id — the only handles are the
strings a filer typed — and `contributor_name` is a **full-text search**,
so a query is a fuzzy net, not a selector. Measured live on 2026-08-21, a
`"THIEL, PETER"` sweep returns **464 rows of which 296 are Peter Thiel**.
The rest are other real, private people: `THIELEN, PETER` (Herdt
Consulting, FL), `THIELKE, PETER` (a water company, CA), and two exact
`THIEL, PETER` matches who are a General Motors factory worker in Bay
City MI and a self-employed contractor in Ottertail MN. Attributing a
private individual's giving to a billionaire is both a data error and a
privacy incident.

A row is accepted only if **all** of: the surname matches exactly; the
given name is one of the declared forms; every remaining name token is a
middle initial or an honorific; the reported employer *or* occupation
matches one of the donor's declared affiliation patterns; and the state
is one of the declared states. Everything refused is counted by reason
and sampled into the report, so a near-miss (a real row with an employer
we have not declared) surfaces as `no_affiliation_match_in_state` rather
than being silently lost.

Two further correctness rules: memo rows (`memo_code='X'`) are the same
dollars itemized twice — once against the earmark conduit, once against
the ultimate recipient — and are never summed; and citations dedupe on
the transaction `sub_id`, so a re-run no longer duplicates citations or
inflates the edge weight the way the pre-P1.6 emitter did.

`DonorIdentity` is data — P1.7 adds Musk as one entry plus a person
anchor, with no code change.

### `--repair`: undoing the pre-P1.6 emitter

The predecessor (`fec.ingest_individual_contributor`) added a
`SourceCitation` unconditionally on every run **and** re-added the amount
to the edge weight, so the published figures were inflated by the number
of times the ingest had run — Peter Thiel's Saving Arizona PAC edge read
**$140,000,000** against a true $20,000,000, and across his 62 edges
$270,902,324 of weight was backed by 1,354 citation rows covering only
220 distinct transactions. It also used the fuzzy name search, so some
of those transactions belonged to other, private people.

`--repair` rebuilds a donor's contribution edges from the identity
predicate: keep one citation per accepted transaction, drop duplicates,
drop memo rows, drop mis-attributed rows, recompute the weight from what
remains, and **delete** an edge left with nothing to cite so the
0-uncited invariant holds. Every row id is chosen at plan time, so the
dry-run preview and the apply cannot diverge, and an edge carrying a
`sub_id` the sweep never saw is **deferred whole** rather than rewritten
on a guess. Dry-run is the default and opens `SET TRANSACTION READ ONLY`;
it touches published rows, so `--apply` is an operator decision.

### Read-gate + fail-closed

`--batch-id` stamps every net-new entity and every edge these passes
create `publication_state=staged`; nothing is public until an operator
runs `POST /api/admin/batches/{batch_id}/publish`, which still refuses
while any batch entity lacks a scrutiny verdict. A canonical a pass
resolves *onto* is never restamped — that would pull a live node off the
public read path.

A **net-new SEC insider is created `surface_mode=suppress`**: it is a
real person the corpus has not classified. The pass writes a
`corporate.registry.officer` alias, which is an existing scrutiny hard
signal, and `run_scrutiny --batch-id` is what promotes them. The
ingester never opens a node. No merge ever straddles `surface_mode` or
`publication_state`, and no pass rewrites an existing canonical's
`surface_mode` in either direction — an anchor that lands on a protected
node is reported for an operator decision.

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
