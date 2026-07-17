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

## The Scrutiny Agent (P1)

Before any real-person node or edge surfaces, an automated agent classifies public vs private
figure, applies a tiered bar (very-high for private, medium-high for public), and logs its reason
(auditable). Sonnet-floor LLM. Design §5.4.
