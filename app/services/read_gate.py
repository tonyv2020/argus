"""RG2 (2026-08-07) — shared publication-state filter for every public read path.

Import ``published_edge`` / ``published_entity`` from this module in every
public read handler. The one-source rule keeps the five gate points
(``/api/search``, ``/api/entities/{id}``, ``/api/entities/{id}/subgraph``,
``/api/flow/model1``, ``/api/flow/model2``, plus the internal
``_entity_importance``) in lockstep — a bug fix here fires in every
call site.

Grep audit: ``git grep -E 'published_(entity|edge)|is_published_entity'``
should return exactly the six call sites + this module.

Design doc: helen-k3s/docs/argus-read-gate-hardening-design.md.
"""

from __future__ import annotations

from app.models import CanonicalEdge, CanonicalEntity, PublicationState

_PUBLISHED = PublicationState.PUBLISHED.value


def published_entity():
    """SQLAlchemy predicate: entity row is live on the public read path."""
    return CanonicalEntity.publication_state == _PUBLISHED


def published_edge():
    """SQLAlchemy predicate: edge row is live on the public read path."""
    return CanonicalEdge.publication_state == _PUBLISHED


def is_published_entity(ent: CanonicalEntity) -> bool:
    """Materialized-row check for the dossier 404 gate.

    An entity with no ``publication_state`` set (which cannot happen once
    the migration has run — NOT NULL) is treated as published to keep
    unit tests + fixtures usable without stamping every row.
    """
    return (ent.publication_state or _PUBLISHED) == _PUBLISHED


# RG4 (2026-08-07) — preview-aware variants for the two endpoints that
# accept ``?include_staged=1`` (search + dossier). Every other read path
# uses the plain ``published_*`` gate — subgraph, flow, and importance
# do NOT expose a preview flag (spec §RG4).


def maybe_published_entity(include_staged: bool):
    """SQLAlchemy predicate that becomes a no-op when the caller has
    presented the service token AND asked for the preview.
    """
    if include_staged:
        from sqlalchemy import true

        return true()
    return published_entity()


def maybe_published_edge(include_staged: bool):
    """Edge counterpart to :func:`maybe_published_entity`."""
    if include_staged:
        from sqlalchemy import true

        return true()
    return published_edge()
