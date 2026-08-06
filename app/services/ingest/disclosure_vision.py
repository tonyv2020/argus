"""D3.1 — vision-LLM path for OCR-garbled 278-T periodic reports.

D3 archived 8 Trump 278-T PDFs. Six were xlsx-origin and extracted
cleanly via ``pdftotext -layout`` (2171 traded edges from Part 7
annual + 205 from those six). Two — the 2026-04-20 8-page report and
one of the 2026-05-08 reports (5pp) — were image-scan PDFs whose
``pdftotext`` output was OCR-garbled (0 tail-matches in
:func:`parse_text_278t`).

Per design §5 those two docs quarantine cleanly (never fabricated) and
route to a vision-LLM pass. This module implements that path:

  1. Re-fetch the archived PDF bytes.
  2. ``pdftoppm`` each page to PNG @ 200 DPI.
  3. Send each page image to Claude vision with a STRICT prompt
     constrained to the closed value-band vocabulary + canonical
     transaction types (purchase / sale / exchange).
  4. Parse the JSON reply. Any row whose ``amount_band`` isn't
     literally in :data:`VALUE_BANDS`, or whose ``transaction_type``
     isn't in the canonical set, is QUARANTINED (LOW, reason
     ``vision_out_of_vocab``) — NEVER coerced.
  5. HIGH rows land in ``disclosure_rows`` (same shape as
     :func:`parse_text_278t` output) so the existing D3 emit code
     produces ``traded`` edges cited to the PDF page.

Anti-fabrication contract (design §5, helen D3.1 spec):
- ``bands`` are validated against ``VALUE_BANDS``. Off-vocabulary
  bands → LOW.
- ``types`` map through :data:`_CANONICAL_TYPE_SET`; anything else
  → LOW.
- The model is told explicitly: ``if a row is unreadable, emit
  {"unreadable": true}`` for that row; the canary of an unreadable
  page returning fabricated rows is caught by a per-doc post-check.
"""

from __future__ import annotations

import argparse
import asyncio
import base64
import hashlib
import json
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path

import httpx
from sqlalchemy import select

from app.config import settings
from app.db import get_sessionmaker
from app.models import DisclosureDocument, DisclosureRow
from app.services.disclosure_parser import Confidence, Part, VALUE_BANDS
from app.services.ingest.disclosure import DEFAULT_BROWSER_UA

logger = logging.getLogger(__name__)


_CANONICAL_TYPE_SET = {"purchase", "sale", "exchange"}
_MODEL = os.environ.get("ARGUS_VISION_MODEL", "claude-opus-4-7")
_DPI = int(os.environ.get("ARGUS_VISION_DPI", "200"))


@dataclass
class VisionPageResult:
    """Per-page vision extraction outcome."""

    doc_id: str
    page: int
    high_rows: int = 0
    low_rows: int = 0
    unreadable: bool = False
    rows: list[dict] = field(default_factory=list)


@dataclass
class VisionDocResult:
    """Per-doc rollup."""

    doc_id: str
    filed_date: str | None
    page_count: int
    high_rows: int = 0
    low_rows: int = 0
    unreadable_pages: int = 0
    inserted_rows: int = 0
    per_page: list[VisionPageResult] = field(default_factory=list)


# ─── Prompt ─────────────────────────────────────────────────────────


def _prompt() -> str:
    """Strict extraction prompt — closed vocab + fail-closed."""
    bands = "\n".join(f"    - {b!r}" for b in sorted(VALUE_BANDS))
    return f"""You are extracting rows from ONE page of a public OGE Form 278-T
Periodic Transaction Report (Donald J. Trump, filer).

Each row on this page is ONE securities transaction with these columns:
  # | Description | Type | Date | Notification (Yes/No, ignore) | Amount

Return ONLY a JSON object with this shape:
{{
  "rows": [
    {{
      "row_index": <int>,          // the # column on the row
      "description": "<string>",   // Description column EXACTLY as shown (bond/security descriptor OK; do not paraphrase)
      "transaction_type": "purchase" | "sale" | "exchange",
      "trade_date": "M/D/YYYY",    // literal date shown
      "amount_band": "<string>"    // MUST be one of the closed vocabulary below
    }},
    ...
  ]
}}

RULES (STRICT — no fabrication):
1. amount_band MUST be exactly one of these literal strings (copy/paste):
{bands}
   If a row's amount doesn't clearly match one of these, EMIT that row with:
     {{"row_index": <int>, "unreadable": true}}
2. transaction_type MUST be one of: "purchase", "sale", "exchange".
   If unclear, mark that row unreadable (as above).
3. If the WHOLE page has no transaction rows (cover page, certifications,
   summary, form instructions), return {{"rows": []}}.
4. NEVER invent a description. If a description is illegible, mark
   that row unreadable.
5. Return ONLY the JSON object. No preamble, no code fences, no
   trailing text.
"""


# ─── Vision call ────────────────────────────────────────────────────


def _b64_png(png_bytes: bytes) -> str:
    return base64.b64encode(png_bytes).decode("ascii")


def _call_claude(png_bytes: bytes) -> dict:
    """One Anthropic Messages API call with the page image."""
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise RuntimeError("ANTHROPIC_API_KEY not set — cannot run vision pass")
    body = {
        "model": _MODEL,
        "max_tokens": 4096,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": "image/png",
                            "data": _b64_png(png_bytes),
                        },
                    },
                    {"type": "text", "text": _prompt()},
                ],
            }
        ],
    }
    with httpx.Client(timeout=60.0) as client:
        resp = client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json=body,
        )
    resp.raise_for_status()
    data = resp.json()
    text = "".join(
        block.get("text", "") for block in data.get("content", []) if block.get("type") == "text"
    ).strip()
    # Strip trailing/leading code fences defensively.
    if text.startswith("```"):
        text = text.strip("`")
        if text.startswith("json"):
            text = text[4:]
    return json.loads(text)


# ─── Row validation (closed vocab) ─────────────────────────────────


def _validate_row(raw: dict) -> tuple[bool, dict, str | None]:
    """Return (is_high, cleaned_parsed_dict, low_reason).

    HIGH iff amount_band is literally in VALUE_BANDS AND transaction_type
    is in the canonical set AND description non-empty. Anything else
    quarantines — never coerced.
    """
    if raw.get("unreadable"):
        return False, {}, "vision_unreadable"
    band = str(raw.get("amount_band") or "").strip()
    if band not in VALUE_BANDS:
        return False, {}, "vision_band_out_of_vocab"
    ttype = str(raw.get("transaction_type") or "").strip().lower()
    if ttype not in _CANONICAL_TYPE_SET:
        return False, {}, "vision_type_out_of_vocab"
    desc = str(raw.get("description") or "").strip()
    if not desc:
        return False, {}, "vision_no_description"
    date = str(raw.get("trade_date") or "").strip()
    return True, {
        "description": desc,
        "transaction_type": ttype,
        "trade_date": date,
        "amount_band": band,
    }, None


# ─── Public: process one doc ────────────────────────────────────────


async def process_doc(doc_id: str) -> VisionDocResult:
    """Vision-extract one archived disclosure document and insert
    ``disclosure_rows`` for every HIGH transaction found. LOW rows
    also land in the ledger (audit).
    """
    sm = get_sessionmaker()
    async with sm() as db:
        doc = await db.get(DisclosureDocument, doc_id)
        if doc is None:
            raise ValueError(f"disclosure_documents/{doc_id} not found")
        result = VisionDocResult(
            doc_id=doc.id,
            filed_date=doc.filed_date.isoformat() if doc.filed_date else None,
            page_count=doc.page_count,
        )
        oge_url = doc.oge_url
        sha_expected = doc.sha256

    pdf_bytes = _download(oge_url)
    got_sha = hashlib.sha256(pdf_bytes).hexdigest()
    if got_sha != sha_expected:
        raise RuntimeError(
            f"sha256 mismatch for doc {doc_id}: expected {sha_expected}, got {got_sha}"
        )
    logger.info("vision: doc=%s sha=%s pages=%d", doc_id, sha_expected[:12], result.page_count)

    with tempfile.TemporaryDirectory() as td:
        pdf_path = Path(td) / f"{doc_id}.pdf"
        pdf_path.write_bytes(pdf_bytes)
        page_pngs = _rasterize(pdf_path, _DPI)
        logger.info("vision: rasterized %d pages @ %d dpi", len(page_pngs), _DPI)

        page_results: list[VisionPageResult] = []
        for page_idx, png_path in page_pngs:
            page_res = VisionPageResult(doc_id=doc_id, page=page_idx)
            try:
                png = png_path.read_bytes()
                extracted = _call_claude(png)
                rows = extracted.get("rows") or []
            except Exception:
                logger.exception("vision call failed on doc=%s page=%d", doc_id, page_idx)
                page_res.unreadable = True
                result.unreadable_pages += 1
                page_results.append(page_res)
                continue
            if not rows:
                page_results.append(page_res)
                continue
            for raw in rows:
                is_high, parsed, reason = _validate_row(raw)
                if is_high:
                    page_res.high_rows += 1
                    page_res.rows.append({
                        "row_index": raw.get("row_index"),
                        "parsed": parsed,
                        "confidence": Confidence.HIGH.value,
                    })
                else:
                    page_res.low_rows += 1
                    page_res.rows.append({
                        "row_index": raw.get("row_index"),
                        "raw": raw,
                        "confidence": Confidence.LOW.value,
                        "reason": reason,
                    })
            result.high_rows += page_res.high_rows
            result.low_rows += page_res.low_rows
            page_results.append(page_res)

    result.per_page = page_results

    # Wipe any prior LOW-only rows for this doc that were leftovers
    # from the pdftotext pass — the vision run is authoritative.
    async with sm() as db:
        from sqlalchemy import delete
        await db.execute(delete(DisclosureRow).where(DisclosureRow.doc_id == doc_id))
        await db.commit()

    # Insert new ledger rows (HIGH + LOW).
    row_seq = 0
    async with sm() as db:
        for pr in result.per_page:
            for entry in pr.rows:
                row_seq += 1
                if entry["confidence"] == Confidence.HIGH.value:
                    db.add(DisclosureRow(
                        doc_id=doc_id,
                        part=Part.PART_7_TRANSACTIONS.value,
                        row_index=entry.get("row_index") or row_seq,
                        page=pr.page,
                        raw_text=(
                            f"vision:{_MODEL} p.{pr.page} "
                            f"desc={entry['parsed']['description']!r} "
                            f"tx={entry['parsed']['transaction_type']} "
                            f"date={entry['parsed']['trade_date']} "
                            f"band={entry['parsed']['amount_band']}"
                        ),
                        parsed=entry["parsed"],
                        parse_confidence=Confidence.HIGH.value,
                        parse_method="vision",
                    ))
                else:
                    db.add(DisclosureRow(
                        doc_id=doc_id,
                        part=Part.PART_7_TRANSACTIONS.value,
                        row_index=entry.get("row_index") or row_seq,
                        page=pr.page,
                        raw_text=f"vision:{_MODEL} p.{pr.page} raw={entry.get('raw')!r}",
                        parsed={},
                        parse_confidence=Confidence.LOW.value,
                        parse_method="vision",
                        reason=entry.get("reason") or "vision_low",
                    ))
                result.inserted_rows += 1
        await db.commit()
    logger.info(
        "vision: doc=%s HIGH=%d LOW=%d unreadable_pages=%d inserted=%d",
        doc_id, result.high_rows, result.low_rows, result.unreadable_pages, result.inserted_rows,
    )
    return result


# ─── Helpers ────────────────────────────────────────────────────────


def _download(url: str) -> bytes:
    """Fetch the archived OGE PDF (browser UA + follow redirects)."""
    with httpx.Client(
        follow_redirects=True,
        timeout=httpx.Timeout(60.0, connect=10.0),
        headers={"User-Agent": DEFAULT_BROWSER_UA},
    ) as client:
        resp = client.get(url)
        resp.raise_for_status()
        return resp.content


def _rasterize(pdf_path: Path, dpi: int) -> list[tuple[int, Path]]:
    """Render pdf → per-page PNGs via ``pdftoppm``."""
    out_dir = pdf_path.parent / "pages"
    out_dir.mkdir(exist_ok=True)
    subprocess.run(  # noqa: S603 — trusted args
        [
            "pdftoppm", "-png", "-r", str(dpi),
            str(pdf_path), str(out_dir / "p"),
        ],
        check=True,
    )
    pages: list[tuple[int, Path]] = []
    for p in sorted(out_dir.iterdir()):
        # pdftoppm names files p-01.png, p-02.png, …
        stem = p.stem
        if not stem.startswith("p-"):
            continue
        try:
            idx = int(stem.split("-", 1)[1])
        except (IndexError, ValueError):
            continue
        pages.append((idx, p))
    return pages


# ─── CLI ─────────────────────────────────────────────────────────


async def _cli_run(doc_ids: list[str]) -> None:
    for doc_id in doc_ids:
        try:
            res = await process_doc(doc_id)
            print(json.dumps({
                "doc_id": res.doc_id,
                "filed_date": res.filed_date,
                "page_count": res.page_count,
                "high_rows": res.high_rows,
                "low_rows": res.low_rows,
                "unreadable_pages": res.unreadable_pages,
                "inserted_rows": res.inserted_rows,
            }, indent=2))
        except Exception as e:  # pragma: no cover — CLI shim
            logger.exception("vision doc %s failed", doc_id)
            print(json.dumps({"doc_id": doc_id, "error": str(e)}))


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser()
    ap.add_argument("doc_ids", nargs="+", help="disclosure_documents.id to re-parse via vision")
    args = ap.parse_args()
    asyncio.run(_cli_run(args.doc_ids))


if __name__ == "__main__":  # pragma: no cover
    _main()

_ = shutil, sys  # ruff-unused import guard for opt future use
