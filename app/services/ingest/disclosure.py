"""D1 — financial-disclosure ingester (archive + parse + ledger).

Flow (see design ``argus-disclosure-ingestion-design.md`` §5):

  1. Archive the source PDF: download bytes, verify sha256, page_count
     via ``pdftotext -l 1`` (cheap), write to ``storage_path``, insert
     one ``disclosure_documents`` row.
  2. Run ``pdftotext -layout`` on the stored PDF → text with form-feed
     page separators.
  3. Hand the text to :func:`app.services.disclosure_parser.parse_text`
     — pure, deterministic, closed-vocabulary band anchor.
  4. Batch-insert the returned rows into ``disclosure_rows``.

No graph writes.  No LLM.  No public surface.  Idempotent by
``sha256``: re-ingesting the same PDF is a no-op (unique constraint).
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
import subprocess
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import httpx
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.db import get_sessionmaker
from app.models import DisclosureDocument, DisclosureRow
from app.services.disclosure_parser import (
    ParsedRow,
    parse_text,
    summarize,
)

logger = logging.getLogger(__name__)


DEFAULT_STORAGE_ROOT = Path(os.environ.get("ARGUS_DISCLOSURE_STORAGE_ROOT", "/data/disclosures"))
DEFAULT_BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) "
    "Argus/1.0 (financial-disclosure-ingester)"
)


@dataclass(frozen=True, slots=True)
class IngestSummary:
    """Return value of :func:`ingest_annual` — the D1 gate metrics."""

    doc_id: str
    sha256: str
    page_count: int
    bytes_len: int
    total_rows: int
    high: int
    low: int
    per_part: dict


# ─── Public ─────────────────────────────────────────────────────────


async def ingest_annual(
    oge_url: str,
    *,
    filer_name: str,
    filed_date: date | None = None,
    period_start: date | None = None,
    period_end: date | None = None,
    storage_root: Path | None = None,
) -> IngestSummary:
    """Archive + parse + ledger a 278e annual filing.

    Idempotent: if the same ``sha256`` is already in
    ``disclosure_documents`` we return its existing summary and skip
    re-parsing.
    """
    root = storage_root or DEFAULT_STORAGE_ROOT
    root.mkdir(parents=True, exist_ok=True)

    pdf_bytes = await _download(oge_url)
    sha256 = hashlib.sha256(pdf_bytes).hexdigest()
    logger.info(
        "disclosure archive: url=%s sha256=%s bytes=%d",
        oge_url,
        sha256,
        len(pdf_bytes),
    )

    sm = get_sessionmaker()
    async with sm() as db:
        existing = (
            (
                await db.execute(
                    select(DisclosureDocument).where(
                        DisclosureDocument.sha256 == sha256
                    )
                )
            )
            .scalar_one_or_none()
        )
        if existing is not None:
            logger.info(
                "disclosure archive: sha256=%s already ingested (doc_id=%s) — no-op",
                sha256,
                existing.id,
            )
            rows_count = await _row_summary(db, existing.id)
            return IngestSummary(
                doc_id=existing.id,
                sha256=sha256,
                page_count=existing.page_count,
                bytes_len=existing.bytes_len,
                **rows_count,
            )

    # Persist bytes at content-addressed path.
    storage_path = root / f"{sha256}.pdf"
    storage_path.write_bytes(pdf_bytes)

    # Run pdftotext to (a) count pages and (b) produce -layout text.
    page_count = _page_count(storage_path)
    layout_text = _pdftotext_layout(storage_path)

    # Insert the document row.
    sm = get_sessionmaker()
    async with sm() as db:
        doc = DisclosureDocument(
            form_type="oge_278e",
            filer_name=filer_name,
            oge_url=oge_url,
            sha256=sha256,
            filed_date=filed_date,
            period_start=period_start,
            period_end=period_end,
            page_count=page_count,
            storage_path=str(storage_path),
            bytes_len=len(pdf_bytes),
        )
        db.add(doc)
        await db.commit()
        await db.refresh(doc)
        doc_id = doc.id

    # Parse + ledger.
    parsed_rows = parse_text(layout_text)
    logger.info(
        "disclosure parse: doc_id=%s rows=%d",
        doc_id,
        len(parsed_rows),
    )
    await _insert_rows(doc_id, parsed_rows)

    s = summarize(parsed_rows)
    return IngestSummary(
        doc_id=doc_id,
        sha256=sha256,
        page_count=page_count,
        bytes_len=len(pdf_bytes),
        total_rows=s["total"],
        high=s["high"],
        low=s["low"],
        per_part=s["per_part"],
    )


# ─── Internals ──────────────────────────────────────────────────────


async def _download(url: str) -> bytes:
    """Fetch a public OGE PDF. Browser UA + follow redirects — the OGE
    host returns 403 without a browser-looking UA."""
    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(60.0, connect=10.0),
        headers={"User-Agent": DEFAULT_BROWSER_UA},
    ) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        return resp.content


def _pdftotext_layout(pdf_path: Path) -> str:
    """Run ``pdftotext -layout`` and return stdout. Sync so we can be
    called from within the async ingester without a thread pool."""
    result = subprocess.run(  # noqa: S603 — trusted args
        ["pdftotext", "-layout", str(pdf_path), "-"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _page_count(pdf_path: Path) -> int:
    """Read the page count from ``pdfinfo`` output."""
    result = subprocess.run(  # noqa: S603 — trusted args
        ["pdfinfo", str(pdf_path)],
        check=True,
        capture_output=True,
        text=True,
    )
    for line in result.stdout.splitlines():
        if line.startswith("Pages:"):
            return int(line.split(":", 1)[1].strip())
    raise ValueError("pdfinfo did not report Pages:")


async def _insert_rows(doc_id: str, parsed_rows: list[ParsedRow]) -> None:
    """Batch-insert parsed rows into the ledger."""
    sm = get_sessionmaker()
    async with sm() as db:
        batch = 1000
        for i in range(0, len(parsed_rows), batch):
            chunk = parsed_rows[i : i + batch]
            db.add_all(
                [
                    DisclosureRow(
                        doc_id=doc_id,
                        part=r.part.value,
                        row_index=r.row_index,
                        account_group=r.account_group,
                        page=r.page,
                        raw_text=r.raw_text,
                        parsed=r.parsed,
                        parse_confidence=r.confidence.value,
                        parse_method="layout",
                        reason=r.reason,
                    )
                    for r in chunk
                ]
            )
            await db.commit()


async def _row_summary(db: AsyncSession, doc_id: str) -> dict:
    """Return summary counts for an already-ingested doc — used when the
    idempotency check finds a pre-existing sha256 and we still want to
    return an :class:`IngestSummary`.
    """
    from sqlalchemy import func as sqlfunc

    q = (
        select(
            DisclosureRow.part,
            DisclosureRow.parse_confidence,
            sqlfunc.count().label("n"),
        )
        .where(DisclosureRow.doc_id == doc_id)
        .group_by(DisclosureRow.part, DisclosureRow.parse_confidence)
    )
    rows = (await db.execute(q)).all()
    per_part: dict[str, dict[str, int]] = {}
    high = 0
    low = 0
    for part, conf, n in rows:
        pp = per_part.setdefault(
            part, {"high": 0, "low": 0, "total": 0}
        )
        pp[conf] = n
        pp["total"] = pp["total"] + n
        if conf == "high":
            high += n
        else:
            low += n
    return {
        "total_rows": high + low,
        "high": high,
        "low": low,
        "per_part": per_part,
    }


# ─── CLI entrypoint ─────────────────────────────────────────────────

TRUMP_2026_ANNUAL_URL = (
    "https://extapps2.oge.gov/201/Presiden.nsf/PAS+Index/"
    "69AEAA9D7455ACD585258E27002DDEE1/$FILE/"
    "Donald-J-Trump-2026-278ANNUAL.pdf"
)


async def main() -> None:  # pragma: no cover — thin CLI shim
    """``python -m app.services.ingest.disclosure`` runs the D1
    Trump-annual archive + parse against the configured DB."""
    logging.basicConfig(level=logging.INFO)
    result = await ingest_annual(
        TRUMP_2026_ANNUAL_URL,
        filer_name="Donald J. Trump",
        filed_date=date(2026, 6, 30),
    )
    print(f"doc_id       : {result.doc_id}")
    print(f"sha256       : {result.sha256}")
    print(f"pages        : {result.page_count}")
    print(f"total_rows   : {result.total_rows}")
    print(f"high         : {result.high}")
    print(f"low          : {result.low}")
    high_rate = 100 * result.high / max(result.total_rows, 1)
    print(f"high_rate    : {high_rate:.1f}%")
    print("per_part:")
    for part, c in result.per_part.items():
        print(
            f"  {part:24s}  total={c['total']:6d}  high={c['high']:6d}  low={c['low']:6d}"
        )


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
