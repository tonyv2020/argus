"""D3 — 278-T periodic transaction reports (batch ingest).

The Trump 278-T set is a handful of separate OGE PDFs, each covering
some window of securities purchases/sales/exchanges >$1,000. Each PDF
follows the SAME layout as the annual (Part 7 transactions section) —
the D1 parser + ledger flow reuses without change, only the
``form_type`` on the ``disclosure_documents`` row differs (``oge_278t``).

This module wires up a curated list of confirmed Trump 278-T URLs
(publicly hosted by OGE) and calls ``ingest_annual(form_type="oge_278t")``
for each one. Each doc is archived idempotently by ``sha256`` (the D1
short-circuit).

Follow-on 278-T filings arrive periodically; append their URL to
:data:`TRUMP_2026_278T_URLS` when they publish.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from dataclasses import dataclass
from datetime import date

from app.services.ingest.disclosure import ingest_annual

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class Trump278T:
    """One curated Trump 278-T report."""

    filed_date: date
    url: str


# Curated list — Trump 278-T periodic transaction reports posted to the
# OGE PAS index. Filed dates from the OGE filing metadata; add new
# entries as they publish.
TRUMP_2026_278T_URLS: tuple[Trump278T, ...] = (
    Trump278T(
        filed_date=date(2025, 9, 3),
        url=(
            "https://extapps2.oge.gov/201/Presiden.nsf/PAS+Index/"
            "322B8A28DB21CC9285258CFD002C0D0B/$FILE/"
            "Donald%20J.%20Trump%209.3.25%20278-T.pdf"
        ),
    ),
    Trump278T(
        filed_date=date(2025, 10, 17),
        url=(
            "https://extapps2.oge.gov/201/Presiden.nsf/PAS+Index/"
            "AA799A2729B4D1BE85258D430031A320/$FILE/"
            "Donald%20J.%20Trump%2010.17.2025%20278-T.pdf"
        ),
    ),
    Trump278T(
        filed_date=date(2025, 10, 20),
        url=(
            "https://extapps2.oge.gov/201/Presiden.nsf/PAS+Index/"
            "18353894FE440B3685258D430031A337/$FILE/"
            "Donald%20J.%20Trump%2010.20.2025%20278-T%20(2).pdf"
        ),
    ),
    Trump278T(
        filed_date=date(2026, 2, 26),
        url=(
            "https://extapps2.oge.gov/201/Presiden.nsf/PAS+Index/"
            "174165F6E1E120B185258DB000347F54/$FILE/"
            "Donald%20J.%20Trump%202.26.2026%20278-T%20(1).pdf"
        ),
    ),
    Trump278T(
        filed_date=date(2026, 4, 20),
        url=(
            "https://extapps2.oge.gov/201/Presiden.nsf/PAS+Index/"
            "CD75555856A7D2E485258DE4002DD4A0/$FILE/"
            "Donald-J-Trump-4.20.2026-278T.pdf"
        ),
    ),
    Trump278T(
        filed_date=date(2026, 5, 8),
        url=(
            "https://extapps2.oge.gov/201/Presiden.nsf/PAS+Index/"
            "5326D3AF5BE7C25385258DF7002DD1B7/$FILE/"
            "Trump,%20Donald%20J.-05.08.2026-278T.pdf"
        ),
    ),
    Trump278T(
        filed_date=date(2026, 5, 8),
        url=(
            "https://extapps2.oge.gov/201/Presiden.nsf/PAS+Index/"
            "405E4EC4E27BE8D185258DF7002DD1C0/$FILE/"
            "Trump,%20Donald%20J.-05.08.2026-278T(2).pdf"
        ),
    ),
    Trump278T(
        filed_date=date(2026, 6, 25),
        url=(
            "https://extapps2.oge.gov/201/Presiden.nsf/PAS+Index/"
            "F9CA13B970439E8F85258E27002DDF15/$FILE/"
            "Donald-J-Trump-06.25.2026-278T%20(2).pdf"
        ),
    ),
)


async def ingest_all_trump_278t() -> list[dict]:
    """Ingest every curated Trump 278-T report. Idempotent per ``sha256``.

    Returns a list of one summary dict per doc; failures for a single doc
    are logged + captured in-place (with ``error`` key) and do not
    abort the batch.
    """
    results: list[dict] = []
    for item in TRUMP_2026_278T_URLS:
        try:
            summary = await ingest_annual(
                item.url,
                filer_name="Donald J. Trump",
                filed_date=item.filed_date,
                form_type="oge_278t",
            )
            results.append(
                {
                    "filed_date": item.filed_date.isoformat(),
                    "url": item.url,
                    "doc_id": summary.doc_id,
                    "sha256": summary.sha256,
                    "page_count": summary.page_count,
                    "bytes_len": summary.bytes_len,
                    "total_rows": summary.total_rows,
                    "high": summary.high,
                    "low": summary.low,
                    "per_part": summary.per_part,
                }
            )
            logger.info(
                "278-T %s: doc=%s pages=%d rows=%d (high=%d low=%d)",
                item.filed_date,
                summary.doc_id,
                summary.page_count,
                summary.total_rows,
                summary.high,
                summary.low,
            )
        except Exception as exc:  # pragma: no cover — network / parse
            logger.exception("278-T %s: ingest failed", item.filed_date)
            results.append(
                {
                    "filed_date": item.filed_date.isoformat(),
                    "url": item.url,
                    "error": str(exc),
                }
            )
    return results


# ─── CLI ─────────────────────────────────────────────────────────


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="Print the URL manifest only; do not fetch.")
    args = ap.parse_args()
    if args.dry_run:
        for item in TRUMP_2026_278T_URLS:
            print(f"{item.filed_date}  {item.url}")
        return
    results = asyncio.run(ingest_all_trump_278t())
    print(f"\n278-T batch ingest complete: {len(results)} docs")
    for r in results:
        if "error" in r:
            print(f"  ERROR  filed={r['filed_date']}  {r['error']}")
        else:
            per_part_summary = ", ".join(
                f"{p}:{c.get('high',0)}h/{c.get('low',0)}l"
                for p, c in r["per_part"].items()
            )
            print(
                f"  OK     filed={r['filed_date']}  doc={r['doc_id']}  "
                f"pages={r['page_count']}  rows={r['total_rows']}  "
                f"[{per_part_summary}]"
            )


if __name__ == "__main__":  # pragma: no cover
    _main()
