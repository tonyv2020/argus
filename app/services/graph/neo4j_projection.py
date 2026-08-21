"""Neo4jProjection — write-through projection of PG truth (design §5a/§5c).

Adapted from legal-lab's `neo4j_projection.py`. Neo4j holds NO independent
truth: every write is an idempotent MERGE keyed on the Postgres id, and
`projected_at` is stamped on the PG row on success. Missing/unavailable Neo4j
degrades to a no-op (project) or None (read) so the API still serves from PG.

Only surfaces edges that carry ≥1 SourceCitation — the citation gate is applied
here as a defense-in-depth mirror of the PG-side gate.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.models import (
    CanonicalEdge,
    CanonicalEntity,
    PublicationState,
    SourceCitation,
    SurfaceMode,
)

logger = logging.getLogger(__name__)


def _now() -> datetime:
    """Timezone-aware UTC now, for `projected_at` stamps."""
    return datetime.now(UTC)


class Neo4jProjection:
    """Thin write-through projection + Cypher reads. Optional Neo4j driver."""

    def __init__(self, driver=None):
        self._driver = driver
        self._checked = driver is not None

    @property
    def driver(self):
        """Return the Neo4j driver, creating it on first call if enabled."""
        if self._driver is None and not self._checked:
            self._checked = True
            if not settings.neo4j_enabled:
                return None
            try:
                from neo4j import GraphDatabase

                self._driver = GraphDatabase.driver(
                    settings.neo4j_uri,
                    auth=(settings.neo4j_user, settings.neo4j_password),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning("neo4j: driver unavailable, projection disabled: %s", exc)
                self._driver = None
        return self._driver

    @property
    def available(self) -> bool:
        """True when a live Neo4j driver is initialised and reachable."""
        return self.driver is not None

    def _run(self, cypher: str, **params):
        """Run a single Cypher statement; None-safe when driver is absent."""
        drv = self.driver
        if drv is None:
            return None
        with drv.session() as s:
            return list(s.run(cypher, **params))

    async def project_entity(self, session: AsyncSession, canonical: CanonicalEntity) -> bool:
        """MERGE a Canonical node and stamp `projected_at` on success.

        Privacy gate (D2, 2026-08-05): the label written to Neo4j MUST
        respect ``surface_mode``:

          * ``OPEN``     → label = ``canonical_name``.
          * ``ALIAS``    → label = ``public_alias`` (real name is never
                          projected).
          * ``SUPPRESS`` → the entity is NOT projected at all. Neo4j has
                          no node for a suppressed canonical, so
                          Cypher-level readers cannot leak a real name
                          through the graph either.

        Read-gate (RG1): a ``publication_state=staged`` canonical is not
        projected either, whatever its surface_mode.

        The public API's own render layer already enforces the same
        contract; this is defense-in-depth so a Cypher query that
        bypasses the API layer cannot see a suppressed real name.
        """
        if not self.available:
            return False
        if canonical.publication_state == PublicationState.STAGED.value:
            # RG1 read-gate (P1.5, 2026-08-21): a staged canonical is not
            # live content. Projecting it would put an unpublished row in
            # a Cypher-reachable graph — the same class of bypass the D2
            # suppress gate closes. It projects on the sweep after an
            # operator publishes the batch.
            return False
        mode = canonical.surface_mode or SurfaceMode.OPEN.value
        if mode == SurfaceMode.SUPPRESS.value:
            # Do NOT project — no node in Neo4j for a suppressed canonical.
            # ``projected_at`` remains NULL; a subsequent Scrutiny
            # promotion to OPEN/ALIAS will cause the next sweep to
            # project properly.
            return False
        if mode == SurfaceMode.ALIAS.value:
            label = canonical.public_alias or (
                f"Private donor #{canonical.id.replace('-', '')[:8]}"
            )
        else:
            label = canonical.canonical_name
        try:
            self._run(
                "MERGE (c:Canonical {pg_id: $id}) "
                "SET c.label=$label, c.type=$type, c.surface_mode=$mode",
                id=canonical.id,
                label=label,
                type=canonical.type,
                mode=mode,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("neo4j project_entity failed for %s: %s", canonical.id, exc)
            return False
        canonical.projected_at = _now()
        session.add(canonical)
        return True

    def prune_missing(
        self, projectable_entity_ids: set[str], live_edge_ids: set[str]
    ) -> tuple[int, int]:
        """Delete every projected node/relationship that Postgres says should
        NOT be there. Returns ``(nodes_deleted, rels_deleted)``.

        ``projectable_entity_ids`` is the set of canonicals Postgres says are
        projectable RIGHT NOW — live, and ``surface_mode != 'suppress'``.
        Anything else with a ``Canonical`` node is deleted. That covers two
        distinct failures, both caused by projection being MERGE-only:

        * **Deleted canonicals.** One the P2 dedup pass merged away lingers
          forever with its old relationships, so the projection keeps
          serving duplicates the merge already collapsed.
        * **Suppressed canonicals — PRIVACY.** ``project_entity`` refuses to
          project a ``suppress`` node (D2, 2026-08-05), but refusing to
          WRITE never removes what an earlier sweep already wrote. Every
          suppress canonical projected before that gate landed still carried
          its real ``canonical_name`` as ``c.label``, reachable from Cypher
          — 6,855 of them, found 2026-08-21. Deleting is the only thing that
          closes it.

        Derive the set from Postgres state, NEVER from "what this run
        managed to project" — a transient Neo4j error mid-sweep would
        otherwise turn into a mass deletion.
        """
        if not self.available:
            return 0, 0
        rels = self._run(
            "MATCH ()-[r:REL]->() WHERE NOT r.pg_id IN $ids "
            "DELETE r RETURN count(r) AS n",
            ids=list(live_edge_ids),
        )
        nodes = self._run(
            "MATCH (c:Canonical) WHERE NOT c.pg_id IN $ids "
            "DETACH DELETE c RETURN count(c) AS n",
            ids=list(projectable_entity_ids),
        )
        n_rels = rels[0]["n"] if rels else 0
        n_nodes = nodes[0]["n"] if nodes else 0
        logger.info(
            "neo4j prune: deleted %d stale relationships + %d stale nodes",
            n_rels, n_nodes,
        )
        return n_nodes, n_rels

    async def project_edge(self, session: AsyncSession, edge: CanonicalEdge) -> bool:
        """MERGE a canonical edge — gated on ≥1 SourceCitation AND on the
        edge being published.

        Read-gate (RG1, P1.5 2026-08-21): a ``staged`` edge belongs to an
        unpublished batch. It projects on the sweep after an operator
        publishes the batch, never before.
        """
        if not self.available:
            return False
        if edge.publication_state == PublicationState.STAGED.value:
            return False
        citations = (
            (await session.execute(select(SourceCitation).where(SourceCitation.edge_id == edge.id)))
            .scalars()
            .all()
        )
        if not citations:
            # No citation → not projected. The API layer must also refuse it.
            return False
        try:
            self._run(
                "MATCH (s:Canonical {pg_id: $src}), (t:Canonical {pg_id: $tgt}) "
                "MERGE (s)-[r:REL {pg_id: $id}]->(t) "
                "SET r.relation=$rel, r.weight=$w, r.citation_count=$cc",
                src=edge.source_id,
                tgt=edge.target_id,
                id=edge.id,
                rel=edge.relation,
                w=edge.weight,
                cc=len(citations),
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("neo4j project_edge failed for %s: %s", edge.id, exc)
            return False
        edge.projected_at = _now()
        session.add(edge)
        return True
