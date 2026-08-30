"""Vessels P1 — OFAC SDN sanctioned-vessel ingest.

Lands the vessel rows of the OFAC Specially Designated Nationals list as
a standalone asset layer in PG-truth. Per the vessels design doc, P1 is
**ingest + fence only**: no read path, no Neo4j, no entity resolution.
``owner_name_raw`` is raw source text, not a resolved entity.

SOURCE FORMAT (verified live 2026-08-30). ``SDN.CSV`` is 12 unlabelled
columns and OFAC writes the literal string ``-0-`` for "no value":

    0 ent_num · 1 name · 2 sdn_type · 3 program · 4 title · 5 call_sign
    6 vessel_type · 7 tonnage · 8 gross_tonnage · 9 flag
    10 vessel_owner · 11 remarks

Rows with ``sdn_type == 'vessel'`` are the vessel list — 1,540 of 19,322
rows at time of writing. The IMO number is not a column; when present it
is embedded in ``remarks`` as "Vessel Registration Identification IMO
1234567", so it is extracted by pattern and left NULL when absent rather
than guessed.

AIRCRAFT LESSONS BAKED IN FROM THE START, not retrofitted:

  * **The upsert excludes the gate columns** (:data:`_GATE_COLUMNS`). A
    re-ingest is a data refresh, not a publishing decision. On aircraft
    this was found only after a pilot had been promoted, one weekly run
    away from silently un-publishing 5,210 rows with no audit trail.
  * **Constraint errors are redacted.** Postgres echoes the entire
    failing row in ``DETAIL`` and SQLAlchemy adds the bound parameters —
    both would print the owner's name and address. Neither reaches a log.
  * **Structural citation**: every row carries the snapshot id, url and
    sha256, NOT NULL and CHECK-ed, so an uncited vessel cannot exist.
  * Chunk size derives from the column count so a multi-row INSERT cannot
    breach Postgres' 65535 bind-parameter cap mid-load.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import io
import logging
import re
import urllib.request
from dataclasses import dataclass, field

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import get_sessionmaker
from app.models import Vessel, VesselSourceSnapshot, _new_id

logger = logging.getLogger(__name__)

SOURCE = "ofac_sdn"
OFAC_SDN_URL = "https://sanctionslistservice.ofac.treas.gov/api/PublicationPreview/exports/SDN.CSV"

#: OFAC serves the list to a browser User-Agent.
_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

#: OFAC's sentinel for "no value" in every column.
_NULL_TOKEN = "-0-"

#: See the module docstring — a re-ingest must never change what is
#: published. Only an audited promote/demote may touch these.
_GATE_COLUMNS = frozenset({"surface_mode", "publication_state"})

_PARAM_BUDGET = 50_000

#: "Vessel Registration Identification IMO 9223368" and variants.
_IMO_RE = re.compile(r"\bIMO\s*#?\s*(\d{7})\b", re.I)


@dataclass
class VesselIngestSummary:
    """What one OFAC ingest run did."""

    batch_id: str = ""
    sha256: str = ""
    bytes_len: int = 0
    source_last_modified: str | None = None
    snapshot_id: str = ""
    total_rows: int = 0
    vessel_rows: int = 0
    with_imo: int = 0
    with_owner: int = 0
    malformed: int = 0
    malformed_sample: list[str] = field(default_factory=list)
    dry_run: bool = False


def _clean(value: str | None) -> str | None:
    """Strip padding; OFAC's ``-0-`` sentinel and empty both become NULL."""
    if value is None:
        return None
    out = value.strip()
    if not out or out == _NULL_TOKEN:
        return None
    return out


def extract_imo(remarks: str | None) -> str | None:
    """Pull the 7-digit IMO out of the free-text remarks, or None.

    Not guessed: absent means NULL, because a wrong IMO would silently
    merge two different ships under the durable cross-source key.
    """
    if not remarks:
        return None
    m = _IMO_RE.search(remarks)
    return m.group(1) if m else None


def parse_sdn_vessels(raw: str, summary: VesselIngestSummary):
    """Yield vessel rows from SDN.CSV as ``vessels`` column dicts."""
    reader = csv.reader(io.StringIO(raw))
    for row in reader:
        summary.total_rows += 1
        if len(row) < 12:
            summary.malformed += 1
            if len(summary.malformed_sample) < 5:
                summary.malformed_sample.append(",".join(row)[:160])
            continue
        if (row[2] or "").strip().lower() != "vessel":
            continue
        ent_num = _clean(row[0])
        name = _clean(row[1])
        if not ent_num or not name:
            summary.malformed += 1
            continue
        remarks = _clean(row[11])
        imo = extract_imo(remarks)
        owner = _clean(row[10])
        summary.vessel_rows += 1
        if imo:
            summary.with_imo += 1
        if owner:
            summary.with_owner += 1
        yield {
            "source": SOURCE,
            "source_key": ent_num,
            "vessel_name": name,
            "imo_number": imo,
            "call_sign": _clean(row[5]),
            "vessel_type": _clean(row[6]),
            "tonnage": _clean(row[7]),
            "gross_tonnage": _clean(row[8]),
            "flag": _clean(row[9]),
            "owner_name_raw": owner,
            "sanctions_program": _clean(row[3]),
            "sanctions_remarks": remarks,
            "is_sanctioned": True,
            "surface_mode": "suppress",
            "publication_state": "staged",
        }


def download(url: str = OFAC_SDN_URL) -> tuple[str, str, int, str | None]:
    """Fetch SDN.CSV. Returns ``(text, sha256, bytes_len, last_modified)``."""
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    with urllib.request.urlopen(req, timeout=300) as resp:
        data = resp.read()
        last_modified = resp.headers.get("Last-Modified")
    digest = hashlib.sha256(data).hexdigest()
    logger.info("OFAC SDN: %d bytes sha256=%s", len(data), digest)
    # OFAC ships latin-1; a handful of names carry accented bytes.
    return data.decode("latin-1"), digest, len(data), last_modified


class VesselWriteError(RuntimeError):
    """A vessel write failed. Message is REDACTED of owner PII."""


def _redacted(exc: Exception, rows: list[dict]) -> VesselWriteError:
    """Rebuild a write failure without the owner row.

    Postgres' constraint ``DETAIL`` echoes the entire failing row and
    SQLAlchemy's message embeds the bound parameters — both carry
    ``owner_name_raw`` and the owner address. Only the constraint name
    and the vessel source keys survive.
    """
    constraint = None
    diag = getattr(getattr(exc, "orig", None), "diag", None)
    if diag is not None:
        constraint = getattr(diag, "constraint_name", None)
    keys = [str(r.get("source_key", "?")) for r in rows[:8]]
    more = f" (+{len(rows) - 8} more)" if len(rows) > 8 else ""
    return VesselWriteError(
        f"{type(exc).__name__} writing vessels"
        f"{f' (constraint {constraint})' if constraint else ''}"
        f" [DETAIL REDACTED — contains owner PII]"
        f"; affected source_key: {', '.join(keys)}{more}"
    )


def _chunk_size() -> int:
    return max(1, _PARAM_BUDGET // max(1, len(Vessel.__table__.columns)))


async def _upsert(session, rows: list[dict]) -> None:
    """Upsert a chunk on (source, source_key), preserving the gates."""
    if not rows:
        return
    stmt = pg_insert(Vessel).values(rows)
    update_cols = {
        c.name: stmt.excluded[c.name]
        for c in Vessel.__table__.columns
        if c.name
        not in {"id", "source", "source_key", "created_at", "updated_at"} | _GATE_COLUMNS
    }
    update_cols["updated_at"] = func.now()
    try:
        await session.execute(
            stmt.on_conflict_do_update(
                index_elements=["source", "source_key"], set_=update_cols
            )
        )
    except Exception as exc:
        raise _redacted(exc, rows) from None


async def ingest_ofac_vessels(
    url: str = OFAC_SDN_URL, *, dry_run: bool = False
) -> VesselIngestSummary:
    """Download + parse + upsert the OFAC vessel list. Idempotent."""
    summary = VesselIngestSummary(dry_run=dry_run)
    raw, sha, size, last_modified = download(url)
    summary.sha256, summary.bytes_len = sha, size
    summary.source_last_modified = last_modified
    summary.batch_id = f"ofac-sdn-{sha[:12]}"

    if dry_run:
        for _ in parse_sdn_vessels(raw, summary):
            pass
        return summary

    sm = get_sessionmaker()
    async with sm() as session:
        snap = await session.scalar(
            select(VesselSourceSnapshot).where(
                VesselSourceSnapshot.batch_id == summary.batch_id
            )
        )
        if snap is None:
            snap = VesselSourceSnapshot(
                source=SOURCE,
                source_url=url,
                sha256=sha,
                bytes_len=size,
                source_last_modified=last_modified,
                batch_id=summary.batch_id,
            )
            session.add(snap)
            await session.flush()
        summary.snapshot_id = snap.id

        size_ = _chunk_size()
        buf: list[dict] = []
        for row in parse_sdn_vessels(raw, summary):
            row["id"] = _new_id()
            row["snapshot_id"] = snap.id
            row["source_url"] = url
            row["source_sha256"] = sha
            row["batch_id"] = summary.batch_id
            buf.append(row)
            if len(buf) >= size_:
                await _upsert(session, buf)
                buf = []
        if buf:
            await _upsert(session, buf)
        snap.rows_ingested = summary.vessel_rows
        await session.commit()
    return summary


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="OFAC SDN sanctioned-vessel ingest.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--url", default=OFAC_SDN_URL)
    args = ap.parse_args()
    s = asyncio.run(ingest_ofac_vessels(args.url, dry_run=args.dry_run))
    print(f"\nOFAC SDN vessel ingest {'(DRY RUN) ' if s.dry_run else ''}complete")
    print(f"  batch_id       {s.batch_id}")
    print(f"  sha256         {s.sha256}")
    print(f"  bytes          {s.bytes_len:,}")
    print(f"  last-modified  {s.source_last_modified}")
    print(f"  total SDN rows {s.total_rows:,}")
    print(f"  vessel rows    {s.vessel_rows:,}  (malformed {s.malformed})")
    print(f"  with IMO       {s.with_imo:,}")
    print(f"  with owner     {s.with_owner:,}")
    print("  fence          surface_mode=suppress publication_state=staged")
    for line in s.malformed_sample:
        print(f"  MALFORMED  {line}")


if __name__ == "__main__":  # pragma: no cover
    _main()
