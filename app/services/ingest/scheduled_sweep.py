"""P4 F — scheduled priority-domain sweep, one entry per k8s CronJob run.

Sequences (in order):
    1. Congress roster (idempotent upsert of 537 members)
    2. FEC ingest from registry (external-ID + name-search fallback)
    3. FEC individual-contributor ingest from registry (Musk + Thiel etc.)
    4. USAspending ingest from registry
    5. LDA ingest from registry
    6. SEC EDGAR ingest from registry
    7. Neo4j reproject

Each step is wrapped in a try/except so one failure doesn't stall the
rest. Per-step counters roll into a single log line so operator log
review + Atlas dashboards can attribute impact to source.

Optional ``ARGUS_SWEEP_DOMAINS`` env var — CSV of priority-domain
labels — restricts the sweep to a subset. Default: all.
"""

from __future__ import annotations

import asyncio
import logging
import os

from app.services.ingest import (
    congress_roster,
    fec,
    project_to_neo4j,
    sec_edgar,
    senate_lda,
    usaspending,
)

logger = logging.getLogger(__name__)


def _priority_domains() -> tuple[str, ...] | None:
    """Read the priority-domain filter from env (CSV) — None means all."""
    raw = os.environ.get("ARGUS_SWEEP_DOMAINS")
    if not raw:
        return None
    return tuple(d.strip() for d in raw.split(",") if d.strip())


async def run_sweep() -> dict[str, object]:
    """One end-to-end pass. Returns a dict keyed per phase for easy log
    reduction."""
    domains = _priority_domains()
    domain_note = ",".join(domains) if domains else "ALL"
    logger.info("argus scheduled sweep begin domains=%s", domain_note)

    out: dict[str, object] = {}

    # 1. Congress roster (independent of priority_domains — the roster
    #    itself defines the congress domain).
    try:
        out["congress_roster"] = await congress_roster.ingest_roster()
        logger.info("[congress_roster] %s", out["congress_roster"])
    except Exception:
        logger.exception("congress_roster failed")
        out["congress_roster"] = "error"

    # 2. FEC PAC-mode ingest.
    try:
        out["fec_pacs"] = await fec.ingest_from_registry(
            priority_domains=domains
        )
        logger.info("[fec_pacs] %d anchors", len(out["fec_pacs"]))
    except Exception:
        logger.exception("fec_pacs failed")
        out["fec_pacs"] = "error"

    # 3. FEC individual-contributor mode (person anchors).
    try:
        out["fec_individuals"] = (
            await fec.ingest_individual_contributors_from_registry(
                priority_domains=domains
            )
        )
        logger.info(
            "[fec_individuals] %d anchors", len(out["fec_individuals"])
        )
    except Exception:
        logger.exception("fec_individuals failed")
        out["fec_individuals"] = "error"

    # 4. USAspending.
    try:
        out["usaspending"] = await usaspending.ingest_from_registry(
            priority_domains=domains
        )
        logger.info(
            "[usaspending] %d anchors", len(out["usaspending"])
        )
    except Exception:
        logger.exception("usaspending failed")
        out["usaspending"] = "error"

    # 5. Senate LDA.
    try:
        out["senate_lda"] = await senate_lda.ingest_from_registry(
            priority_domains=domains
        )
        logger.info("[senate_lda] %s", out["senate_lda"])
    except Exception:
        logger.exception("senate_lda failed")
        out["senate_lda"] = "error"

    # 6. SEC EDGAR.
    try:
        out["sec_edgar"] = await sec_edgar.ingest_from_registry(
            priority_domains=domains
        )
        logger.info("[sec_edgar] %s", out["sec_edgar"])
    except Exception:
        logger.exception("sec_edgar failed")
        out["sec_edgar"] = "error"

    # 7. Reproject the newly-landed edges to Neo4j.
    try:
        out["reproject"] = await project_to_neo4j.main_async()
        logger.info("[reproject] %s", out["reproject"])
    except Exception:
        logger.exception("reproject failed")
        out["reproject"] = "error"

    logger.info("argus scheduled sweep done domains=%s", domain_note)
    return out


def main() -> None:
    """CLI + CronJob entry — `python -m app.services.ingest.scheduled_sweep`."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    asyncio.run(run_sweep())


if __name__ == "__main__":
    main()
