"""P1.6 — one CLI over the domain's external-ID-gated source passes.

``usaspending`` and ``senate_lda`` both already have CLIs, but they are
positional-argument dispatchers over the older detention-industry anchor
constants. Rather than bend those into taking ``--batch-id`` and
``--priority-domain``, this module is the P1.6 entrypoint over the two
new registry-driven, externally-gated passes:

* :func:`app.services.ingest.usaspending.ingest_domain_contracts_by_uei`
  — cited ``holds_contract`` edges, every award row verified against the
  anchor's declared recipient UEI.
* :func:`app.services.ingest.senate_lda.ingest_domain_lobbying`
  — cited ``lobbies`` edges, every filing verified against the anchor's
  declared client-name patterns.

Both stage everything they create behind ``--batch-id``. Order does not
matter between them; both require ``domain_anchors`` to have run first,
because each resolves its subject from ``anchor_registry.canonical_id``
and REFUSES to mint one from a name.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging

logger = logging.getLogger(__name__)


async def run_domain_sources(
    domain: str,
    *,
    batch_id: str | None,
    contracts: bool = True,
    lobbying: bool = True,
    max_awards_per_anchor: int = 500,
    max_filings_per_anchor: int = 600,
) -> dict:
    """Run the selected source passes for one priority domain."""
    from app.services.ingest.senate_lda import ingest_domain_lobbying
    from app.services.ingest.usaspending import ingest_domain_contracts_by_uei

    report: dict = {"domain": domain, "batch_id": batch_id}
    if contracts:
        stats = await ingest_domain_contracts_by_uei(
            (domain,),
            batch_id=batch_id,
            max_awards_per_anchor=max_awards_per_anchor,
        )
        report["contracts"] = dict(stats.__dict__)
        logger.info(
            "usaspending UEI pass: %d anchors, %d awards accepted, "
            "%d refused on a foreign UEI, $%.2f obligated",
            stats.anchors_processed, stats.awards_accepted,
            stats.awards_refused_foreign_uei, stats.obligation_total,
        )
    if lobbying:
        stats = await ingest_domain_lobbying(
            (domain,),
            batch_id=batch_id,
            max_filings_per_anchor=max_filings_per_anchor,
        )
        report["lobbying"] = dict(stats.__dict__)
        logger.info(
            "LDA pattern pass: %d anchors, %d filings accepted, "
            "%d refused off-anchor",
            stats.anchors_processed, stats.filings_accepted,
            stats.filings_refused_off_anchor,
        )
    return report


def main() -> None:
    """CLI — ``python -m app.services.ingest.domain_sources``."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s"
    )
    ap = argparse.ArgumentParser(
        description="P1.6 external-ID-gated contract + lobbying ingest"
    )
    ap.add_argument("--domain", default="surveillance")
    ap.add_argument("--batch-id", default=None)
    ap.add_argument(
        "--only", choices=("contracts", "lobbying", "all"), default="all"
    )
    ap.add_argument("--max-awards", type=int, default=500)
    ap.add_argument("--max-filings", type=int, default=600)
    ap.add_argument("--json-report", default=None)
    args = ap.parse_args()

    report = asyncio.run(
        run_domain_sources(
            args.domain,
            batch_id=args.batch_id,
            contracts=args.only in ("contracts", "all"),
            lobbying=args.only in ("lobbying", "all"),
            max_awards_per_anchor=args.max_awards,
            max_filings_per_anchor=args.max_filings,
        )
    )
    print(json.dumps(report, indent=2, default=str))
    if args.json_report:
        with open(args.json_report, "w") as fh:
            json.dump(report, fh, indent=2, default=str)


if __name__ == "__main__":
    main()
