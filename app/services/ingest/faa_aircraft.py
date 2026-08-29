"""P1 — FAA Releasable Aircraft Database ingest (MASTER + ACFTREF).

Lands the FAA civil aircraft registry as a standalone asset layer in
PG-truth. Per helen's decision doc (2026-08-29) P1 is **ingest + fence
only**:

  * no ``EntityType`` member, no ``canonical_entities`` row, no edge
  * no Neo4j projection, no participation in the public read path
  * **no entity resolution** — ``registrant_name`` is a raw source
    string, not a resolved person. Connecting "TRUMP DONALD J" on a
    registration to the canonical Donald Trump is P2 and is Tony's
    call, because that is the step that makes a claim about a person.

THE FENCE. ``MASTER.txt`` carries the home street address of every
individual registrant — roughly 316k rows of live PII. Every row this
module writes is stamped ``surface_mode='suppress'`` and
``publication_state='staged'``, and the schema pins both with CHECK
constraints (migration 0011) so a mistake here fails at the database
rather than leaking. This module never writes any other value.

Source: https://registry.faa.gov/database/ReleasableAircraft.zip
(~73 MB zip; MASTER.txt 194 MB, ACFTREF.txt 15 MB uncompressed).
The FAA serves the file to a browser User-Agent but answers 503 to
urllib's default and to HEAD — hence :data:`_UA` and GET-only.

Idempotent. ``aircraft`` upserts on the FAA ``UNIQUE ID``,
``aircraft_reference`` on the FAA ``CODE``; the batch id is derived
from the sha256 of the downloaded bytes, so re-running the same
snapshot updates in place instead of duplicating.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import hashlib
import io
import logging
import os
import tempfile
import urllib.request
import zipfile
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import date

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from app.db import get_sessionmaker
from app.models import Aircraft, AircraftReference, AircraftSourceSnapshot, _new_id

logger = logging.getLogger(__name__)

FAA_RELEASABLE_URL = "https://registry.faa.gov/database/ReleasableAircraft.zip"

#: The FAA edge answers 503 to urllib's default User-Agent and to any
#: HEAD request. A browser UA + GET is what actually works; this is a
#: fetch quirk of the source, not an attempt to disguise the client.
_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
)

#: Bind-parameter budget per INSERT. Postgres hard-caps a statement at
#: 65535 parameters, and a multi-row VALUES insert spends
#: rows × columns of them — 2000 aircraft rows would blow the cap and
#: fail mid-load. Chunk size is therefore derived from the table's
#: column count (see :func:`_chunk_size`), not fixed.
_PARAM_BUDGET = 50_000

# The fence. Module-level constants so there is exactly one place
# these values are written, and the tests can assert against them.
FENCE_SURFACE_MODE = "suppress"
FENCE_PUBLICATION_STATE = "staged"


@dataclass
class IngestSummary:
    """What one ingest run did — the report payload."""

    batch_id: str = ""
    sha256: str = ""
    bytes_len: int = 0
    source_last_modified: str | None = None
    snapshot_id: str = ""
    acftref_rows: int = 0
    master_rows: int = 0
    #: Source lines whose field count did not match the header. Kept as
    #: a count + a small sample rather than dropped silently — a nonzero
    #: value means the source layout moved and the mapping needs a look.
    master_malformed: int = 0
    acftref_malformed: int = 0
    malformed_sample: list[str] = field(default_factory=list)
    dry_run: bool = False


# ─── field coercion ──────────────────────────────────────────────


def _clean(value: str | None) -> str | None:
    """Strip the source's space padding; empty becomes NULL, not ''."""
    if value is None:
        return None
    out = value.strip()
    return out or None


def _as_date(value: str | None) -> date | None:
    """FAA dates are ``YYYYMMDD``; blank/garbage becomes NULL.

    Returns None rather than raising — a single unparseable date is a
    fact about one source row, not a reason to abort a 316k-row load.
    """
    raw = _clean(value)
    if not raw or len(raw) != 8 or not raw.isdigit():
        return None
    try:
        return date(int(raw[0:4]), int(raw[4:6]), int(raw[6:8]))
    except ValueError:
        return None


def _as_int(value: str | None) -> int | None:
    """Numeric-ish FAA field to int; blank or non-numeric becomes NULL.

    The registry zero-pads (``NO-SEATS`` is ``015``), so plain ``int``
    is right — but ``YEAR MFR`` and friends are sometimes blank or
    contain stray text, which is a NULL rather than an error.
    """
    raw = _clean(value)
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _norm_header(name: str) -> str:
    """Normalise a header cell — strips the UTF-8 BOM and padding.

    The registry files are latin-1, so the UTF-8 BOM that opens each
    header decodes to the three characters ``ï»¿`` rather than to
    U+FEFF. Both forms are stripped: matching only U+FEFF silently
    breaks the FIRST column of each file (``N-NUMBER`` / ``CODE``),
    which are exactly the two natural keys — so every row fails its
    key check and the whole load drops as "malformed".
    """
    return name.replace("﻿", "").replace("ï»¿", "").strip().upper()


def _require_columns(idx: dict[str, int], required: set[str], filename: str) -> None:
    """Fail loudly when the header does not carry the columns we map.

    A header we cannot map is a source-layout change, not a data
    problem — without this it presents as every row being individually
    malformed, which reads like bad data and buries the real cause.
    """
    missing = sorted(required - idx.keys())
    if missing:
        raise ValueError(
            f"{filename}: header is missing required column(s) {missing}. "
            f"Header parsed as: {sorted(idx)}"
        )


# ─── parsing ─────────────────────────────────────────────────────


def _rows(raw: bytes | io.BufferedIOBase) -> Iterator[list[str]]:
    """Yield CSV rows from a latin-1 FAA text member.

    The registry files are latin-1, not UTF-8 — a handful of
    registrant names carry accented bytes that UTF-8 rejects.
    """
    stream = io.TextIOWrapper(raw, encoding="latin-1", newline="")
    yield from csv.reader(stream)


def parse_master(rows: Iterator[list[str]], summary: IngestSummary) -> Iterator[dict]:
    """Map ``MASTER.txt`` rows to ``aircraft`` column dicts.

    Column positions are resolved from the file's own header rather
    than hardcoded, so a column added upstream shifts nothing.
    """
    header = [_norm_header(c) for c in next(rows)]
    idx = {name: i for i, name in enumerate(header)}
    _require_columns(idx, {"N-NUMBER", "UNIQUE ID", "NAME", "MFR MDL CODE"}, "MASTER.txt")

    def g(row: list[str], col: str) -> str | None:
        i = idx.get(col)
        return row[i] if i is not None and i < len(row) else None

    for row in rows:
        # The trailing comma on every line yields one extra empty
        # field; anything else means an embedded comma broke the row.
        if len(row) != len(header):
            summary.master_malformed += 1
            if len(summary.malformed_sample) < 5:
                summary.malformed_sample.append(",".join(row)[:200])
            continue

        unique_id = _clean(g(row, "UNIQUE ID"))
        n_number = _clean(g(row, "N-NUMBER"))
        if not unique_id or not n_number:
            summary.master_malformed += 1
            if len(summary.malformed_sample) < 5:
                summary.malformed_sample.append(",".join(row)[:200])
            continue

        other_names = [
            v
            for v in (_clean(g(row, f"OTHER NAMES({n})")) for n in range(1, 6))
            if v
        ]

        yield {
            "unique_id": unique_id,
            "n_number": n_number,
            "serial_number": _clean(g(row, "SERIAL NUMBER")),
            "mfr_mdl_code": _clean(g(row, "MFR MDL CODE")),
            "eng_mfr_mdl": _clean(g(row, "ENG MFR MDL")),
            "year_mfr": _as_int(g(row, "YEAR MFR")),
            "type_registrant": _clean(g(row, "TYPE REGISTRANT")),
            "registrant_name": _clean(g(row, "NAME")),
            "street": _clean(g(row, "STREET")),
            "street2": _clean(g(row, "STREET2")),
            "city": _clean(g(row, "CITY")),
            "state": _clean(g(row, "STATE")),
            "zip_code": _clean(g(row, "ZIP CODE")),
            "region": _clean(g(row, "REGION")),
            "county": _clean(g(row, "COUNTY")),
            "country": _clean(g(row, "COUNTRY")),
            "other_names": other_names,
            "last_action_date": _as_date(g(row, "LAST ACTION DATE")),
            "cert_issue_date": _as_date(g(row, "CERT ISSUE DATE")),
            "certification": _clean(g(row, "CERTIFICATION")),
            "type_aircraft": _clean(g(row, "TYPE AIRCRAFT")),
            "type_engine": _clean(g(row, "TYPE ENGINE")),
            "status_code": _clean(g(row, "STATUS CODE")),
            "mode_s_code": _clean(g(row, "MODE S CODE")),
            "mode_s_code_hex": _clean(g(row, "MODE S CODE HEX")),
            # "Y" means fractional ownership; blank means it is not
            # claimed either way, so NULL rather than False.
            "fract_owner": (
                True if (_clean(g(row, "FRACT OWNER")) or "").upper() == "Y" else None
            ),
            "air_worth_date": _as_date(g(row, "AIR WORTH DATE")),
            "expiration_date": _as_date(g(row, "EXPIRATION DATE")),
            "kit_mfr": _clean(g(row, "KIT MFR")),
            "kit_model": _clean(g(row, "KIT MODEL")),
            "surface_mode": FENCE_SURFACE_MODE,
            "publication_state": FENCE_PUBLICATION_STATE,
        }


def parse_acftref(rows: Iterator[list[str]], summary: IngestSummary) -> Iterator[dict]:
    """Map ``ACFTREF.txt`` rows to ``aircraft_reference`` column dicts."""
    header = [_norm_header(c) for c in next(rows)]
    idx = {name: i for i, name in enumerate(header)}
    _require_columns(idx, {"CODE", "MFR", "MODEL"}, "ACFTREF.txt")

    def g(row: list[str], col: str) -> str | None:
        i = idx.get(col)
        return row[i] if i is not None and i < len(row) else None

    for row in rows:
        if len(row) != len(header):
            summary.acftref_malformed += 1
            continue
        code = _clean(g(row, "CODE"))
        if not code:
            summary.acftref_malformed += 1
            continue
        yield {
            "code": code,
            "mfr": _clean(g(row, "MFR")),
            "model": _clean(g(row, "MODEL")),
            "type_acft": _clean(g(row, "TYPE-ACFT")),
            "type_eng": _clean(g(row, "TYPE-ENG")),
            "ac_cat": _clean(g(row, "AC-CAT")),
            "build_cert_ind": _clean(g(row, "BUILD-CERT-IND")),
            "no_eng": _as_int(g(row, "NO-ENG")),
            "no_seats": _as_int(g(row, "NO-SEATS")),
            "ac_weight": _clean(g(row, "AC-WEIGHT")),
            "speed": _as_int(g(row, "SPEED")),
            "tc_data_sheet": _clean(g(row, "TC-DATA-SHEET")),
            "tc_data_holder": _clean(g(row, "TC-DATA-HOLDER")),
            "publication_state": FENCE_PUBLICATION_STATE,
        }


# ─── fetch ───────────────────────────────────────────────────────


def download(url: str = FAA_RELEASABLE_URL) -> tuple[str, str, int, str | None]:
    """Stream the releasable-database zip to a temp file.

    Returns ``(path, sha256, bytes_len, last_modified)``. The caller
    owns the temp file and must unlink it — the zip is 73 MB of mostly
    PII and there is no reason to leave a copy on disk.
    """
    req = urllib.request.Request(url, headers={"User-Agent": _UA})
    digest = hashlib.sha256()
    total = 0
    fd, path = tempfile.mkstemp(prefix="faa-releasable-", suffix=".zip")
    with os.fdopen(fd, "wb") as out:
        with urllib.request.urlopen(req, timeout=600) as resp:
            last_modified = resp.headers.get("Last-Modified")
            while chunk := resp.read(1 << 20):
                out.write(chunk)
                digest.update(chunk)
                total += len(chunk)
    logger.info("FAA zip: %d bytes sha256=%s", total, digest.hexdigest())
    return path, digest.hexdigest(), total, last_modified


# ─── load ────────────────────────────────────────────────────────


def _chunk_size(table) -> int:
    """Rows per INSERT that keep the statement under Postgres' 65535
    bind-parameter cap for this table's column count."""
    return max(1, _PARAM_BUDGET // max(1, len(table.__table__.columns)))


class FencedWriteError(RuntimeError):
    """A write to a fenced aircraft table failed. Message is REDACTED.

    Raised in place of the driver's own exception, never alongside it.
    """


def _row_ids(rows: list[dict]) -> str:
    """Non-identifying handle for a failed chunk: tail numbers / FAA codes.

    An ``n_number`` is painted on the outside of the aircraft. A
    registrant's name and street address are not, and neither belongs
    in an error message.
    """
    ids = [str(r.get("n_number") or r.get("code") or "?") for r in rows[:8]]
    more = f" (+{len(rows) - 8} more)" if len(rows) > 8 else ""
    return ", ".join(ids) + more


def _redacted(exc: Exception, table, rows: list[dict]) -> FencedWriteError:
    """Rebuild a write failure with the PII stripped out.

    Postgres' ``CheckViolation`` DETAIL echoes the ENTIRE failing row —
    registrant name, street, city, zip — and SQLAlchemy's own message
    additionally embeds the bound parameters, i.e. the whole chunk. So
    neither the driver message nor the SQLAlchemy wrapper may reach a
    log, a response or a traceback. Only the constraint name (which is
    the actionable part) and tail numbers survive.
    """
    constraint = None
    diag = getattr(getattr(exc, "orig", None), "diag", None)
    if diag is not None:
        constraint = getattr(diag, "constraint_name", None)
    return FencedWriteError(
        f"{type(exc).__name__} writing {table.__tablename__}"
        f"{f' (constraint {constraint})' if constraint else ''}"
        f" [DETAIL REDACTED — contains registrant PII]"
        f"; affected n_number/code: {_row_ids(rows)}"
    )


async def _upsert_chunk(session, table, rows: list[dict], conflict: str) -> None:
    """Upsert one chunk, refreshing every non-key column on conflict.

    Any failure is re-raised REDACTED and with the original suppressed
    (``from None``) — a chained exception would print the very DETAIL
    this exists to withhold.
    """
    if not rows:
        return
    stmt = pg_insert(table).values(rows)
    update_cols = {
        c.name: stmt.excluded[c.name]
        for c in table.__table__.columns
        if c.name not in {conflict, "id", "created_at", "updated_at"}
    }
    update_cols["updated_at"] = func.now()
    try:
        await session.execute(
            stmt.on_conflict_do_update(index_elements=[conflict], set_=update_cols)
        )
    except Exception as exc:
        raise _redacted(exc, table, rows) from None


async def _safe_commit(session, table, rows: list[dict]) -> None:
    """Commit, redacting any deferred constraint error the same way."""
    try:
        await session.commit()
    except Exception as exc:
        raise _redacted(exc, table, rows) from None


async def _load_stream(
    session,
    table,
    parsed: Iterator[dict],
    *,
    conflict: str,
    snapshot_id: str,
    batch_id: str,
    needs_id: bool,
    limit: int | None = None,
    commit_every: int = 50_000,
) -> int:
    """Consume parsed rows and upsert them in cap-safe chunks.

    Returns the number of rows written. Stamps provenance on every
    row; mints a UUID pk for tables whose key is not the natural one.
    """
    size = _chunk_size(table)
    buf: list[dict] = []
    written = 0

    async def flush() -> None:
        nonlocal buf, written
        if not buf:
            return
        await _upsert_chunk(session, table, buf, conflict)
        written += len(buf)
        last, buf = buf, []
        if written % commit_every < size:
            await _safe_commit(session, table, last)
            logger.info("%s: %d rows…", table.__tablename__, written)

    for row in parsed:
        row["snapshot_id"] = snapshot_id
        row["batch_id"] = batch_id
        if needs_id:
            row["id"] = _new_id()
        buf.append(row)
        if len(buf) >= size:
            await flush()
        if limit is not None and written >= limit:
            break
    await flush()
    return written


async def ingest_faa_aircraft(
    url: str = FAA_RELEASABLE_URL,
    *,
    limit: int | None = None,
    dry_run: bool = False,
) -> IngestSummary:
    """Download, parse and upsert the FAA registry. Idempotent.

    ``dry_run`` fetches and parses but writes nothing — used to check
    the source layout still maps before touching the database.
    """
    summary = IngestSummary(dry_run=dry_run)
    path, sha, total, last_modified = download(url)
    summary.sha256, summary.bytes_len = sha, total
    summary.source_last_modified = last_modified
    # Batch id is derived from content, so re-running an unchanged
    # snapshot lands on the same batch instead of minting a new one.
    summary.batch_id = f"faa-aircraft-{sha[:12]}"

    try:
        zf = zipfile.ZipFile(path)

        if dry_run:
            with zf.open("ACFTREF.txt") as fh:
                summary.acftref_rows = sum(1 for _ in parse_acftref(_rows(fh), summary))
            with zf.open("MASTER.txt") as fh:
                rows = parse_master(_rows(fh), summary)
                summary.master_rows = sum(
                    1 for i, _ in enumerate(rows) if limit is None or i < limit
                )
            return summary

        sm = get_sessionmaker()
        async with sm() as session:
            # ── snapshot row (provenance anchor) ──
            existing = await session.scalar(
                select(AircraftSourceSnapshot).where(
                    AircraftSourceSnapshot.batch_id == summary.batch_id
                )
            )
            if existing is None:
                snap = AircraftSourceSnapshot(
                    source_url=url,
                    sha256=sha,
                    bytes_len=total,
                    source_last_modified=last_modified,
                    batch_id=summary.batch_id,
                )
                session.add(snap)
                await session.flush()
            else:
                snap = existing
            summary.snapshot_id = snap.id

            # ── ACFTREF first: MASTER's mfr_mdl_code points at it ──
            with zf.open("ACFTREF.txt") as fh:
                summary.acftref_rows = await _load_stream(
                    session,
                    AircraftReference,
                    parse_acftref(_rows(fh), summary),
                    conflict="code",
                    snapshot_id=snap.id,
                    batch_id=summary.batch_id,
                    needs_id=False,
                )
            await session.commit()
            logger.info("ACFTREF: %d rows upserted", summary.acftref_rows)

            # ── MASTER ──
            with zf.open("MASTER.txt") as fh:
                summary.master_rows = await _load_stream(
                    session,
                    Aircraft,
                    parse_master(_rows(fh), summary),
                    conflict="unique_id",
                    snapshot_id=snap.id,
                    batch_id=summary.batch_id,
                    needs_id=True,
                    limit=limit,
                )

            snap.master_rows = summary.master_rows
            snap.acftref_rows = summary.acftref_rows
            await _safe_commit(session, Aircraft, [])
            logger.info("MASTER: %d rows upserted", summary.master_rows)
    finally:
        os.unlink(path)

    return summary


# ─── CLI ─────────────────────────────────────────────────────────


def _main() -> None:  # pragma: no cover
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ap = argparse.ArgumentParser(description="Ingest the FAA Releasable Aircraft Database.")
    ap.add_argument(
        "--dry-run",
        action="store_true",
        help="Fetch + parse and report counts; write nothing to the database.",
    )
    ap.add_argument("--limit", type=int, default=None, help="Cap MASTER rows (smoke runs).")
    ap.add_argument("--url", default=FAA_RELEASABLE_URL)
    args = ap.parse_args()

    s = asyncio.run(ingest_faa_aircraft(args.url, limit=args.limit, dry_run=args.dry_run))
    print(f"\nFAA aircraft ingest {'(DRY RUN) ' if s.dry_run else ''}complete")
    print(f"  batch_id       {s.batch_id}")
    print(f"  sha256         {s.sha256}")
    print(f"  bytes          {s.bytes_len:,}")
    print(f"  last-modified  {s.source_last_modified}")
    print(f"  acftref_rows   {s.acftref_rows:,}  (malformed {s.acftref_malformed})")
    print(f"  master_rows    {s.master_rows:,}  (malformed {s.master_malformed})")
    print(f"  fence          surface_mode={FENCE_SURFACE_MODE} "
          f"publication_state={FENCE_PUBLICATION_STATE}")
    for line in s.malformed_sample:
        print(f"  MALFORMED  {line}")


if __name__ == "__main__":  # pragma: no cover
    _main()
