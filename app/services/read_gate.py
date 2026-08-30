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

from app.models import CanonicalEdge, CanonicalEntity, PublicationState, SurfaceMode

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


# ─── P3.0 aircraft gate (2026-08-30) ─────────────────────────────
#
# Aircraft carry BOTH gates and they AND together, same as canonicals:
# a row is readable only when publication_state='published' AND
# surface_mode<>'suppress'.
#
# These predicates differ from the canonical ones above in one way that
# matters. ``is_published_entity`` treats a NULL publication_state as
# published, to keep old fixtures usable — a fail-OPEN default that is
# tolerable there because those rows predate the column. Aircraft have
# no such history: every row was written after 0011 with NOT NULL
# columns, so a missing/unknown value can only mean a bug or a hand-
# built object. The aircraft checks therefore fail CLOSED — unknown is
# not published. This is the de-anon surface that leaked on 2026-08-21;
# the default has to be "hide".

from app.models import Aircraft, AircraftRegistrationEdge  # noqa: E402

_SUPPRESS = SurfaceMode.SUPPRESS.value


def published_aircraft():
    """SQLAlchemy predicate: aircraft row is live on the public read path."""
    return (Aircraft.publication_state == _PUBLISHED) & (
        Aircraft.surface_mode != _SUPPRESS
    )


def published_registration_edge():
    """SQLAlchemy predicate: REGISTERS edge is live on the public read path."""
    return (AircraftRegistrationEdge.publication_state == _PUBLISHED) & (
        AircraftRegistrationEdge.surface_mode != _SUPPRESS
    )


def is_published_aircraft(row) -> bool:
    """Materialized-row check. Fail-CLOSED: unknown is not published.

    Accepts an ``Aircraft`` or an ``AircraftRegistrationEdge`` — both
    carry the same two gate columns, and callers on the render path
    should not have to care which they hold.
    """
    return (
        getattr(row, "publication_state", None) == _PUBLISHED
        and getattr(row, "surface_mode", None) != _SUPPRESS
        and getattr(row, "surface_mode", None) is not None
    )
