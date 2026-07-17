"""GraphStore boundary + Cytoscape wire types + name normalization.

Adapted from `legal-lab.app.services.graph.base` — same shape (Cytoscape JSON,
resolve_entity, subgraph reads) but scoped to Argus's public/read-only model:
no user_id ACL, no per-doc subgraph.
"""

from __future__ import annotations

import re
import unicodedata
from typing import Protocol, TypedDict, runtime_checkable

from sqlalchemy.ext.asyncio import AsyncSession


class CytoscapeNodeData(TypedDict, total=False):
    """One Cytoscape node — canonical entity."""

    id: str
    label: str
    type: str
    source_count: int


class CytoscapeEdgeData(TypedDict, total=False):
    """One Cytoscape edge — canonical relationship + citation count."""

    id: str
    source: str
    target: str
    label: str
    weight: float | None
    citation_count: int


class CytoscapeElement(TypedDict):
    """Cytoscape element wrapper `{"data": {...}}`."""

    data: dict


class CytoscapeGraph(TypedDict):
    """The wire shape — Cytoscape JSON."""

    nodes: list[CytoscapeElement]
    edges: list[CytoscapeElement]


def empty_graph() -> CytoscapeGraph:
    """Empty Cytoscape graph — nodes + edges both empty lists."""
    return {"nodes": [], "edges": []}


_WS_RE = re.compile(r"\s+")
_SUFFIXES = (
    "incorporated",
    "corporation",
    "company",
    "limited",
    "inc",
    "corp",
    "co",
    "llc",
    "llp",
    "lp",
    "ltd",
    "plc",
    "gmbh",
    "sa",
    "nv",
    "pac",
)


def normalize_name(name: str) -> str:
    """Lowercase, strip accents/punctuation/legal-suffixes, collapse whitespace.

    Deterministic key for the name-match fast path + the over-merge guard on
    embedding matches.
    """
    s = unicodedata.normalize("NFKD", name or "")
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = _WS_RE.sub(" ", s).strip()
    tokens = [t for t in s.split(" ") if t]
    while tokens and tokens[-1] in _SUFFIXES:
        tokens.pop()
    return " ".join(tokens) if tokens else s


@runtime_checkable
class GraphStore(Protocol):
    """Argus GraphStore boundary — every backend speaks this."""

    async def resolve_entity(
        self,
        session: AsyncSession,
        surface_name: str,
        entity_type: str,
        embedding: list[float] | None,
    ) -> str | None:
        """Return canonical id if a match clears the threshold, else None (new)."""
        ...

    async def get_entity_subgraph(
        self, session: AsyncSession, canonical_id: str, hops: int = 1
    ) -> CytoscapeGraph:
        """Cytoscape subgraph anchored at `canonical_id`, expanded `hops` deep."""
        ...
