"""P1 aircraft asset layer — parser, fence, and no-graph-coupling tests.

The fence tests are the point of this file. ``MASTER.txt`` is ~316k
rows of live PII including home addresses, and P1's whole contract is
that none of it surfaces. Three independent things have to hold:

  1. the ingester stamps ``suppress``/``staged`` on every row,
  2. the ORM pins both with CHECK constraints, so a bad UPDATE fails
     at the database rather than leaking, and
  3. nothing in the module touches the graph tables.

Each is asserted separately — any one of them silently regressing is
a privacy incident, so none of them is allowed to be the only guard.
"""

from __future__ import annotations

import inspect

from app.models import Aircraft, AircraftReference, EntityType
from app.services.ingest import faa_aircraft as faa

# ─── fixtures mirroring the real FAA layout ──────────────────────

def as_source_text(header: str) -> str:
    """Return a header string exactly as the reader sees it.

    The registry files carry a UTF-8 BOM but are decoded latin-1, so
    the BOM arrives as the three characters ``ï»¿`` — NOT U+FEFF. A
    fixture that uses U+FEFF passes against a parser that is broken
    on the real file, which is precisely what happened on the first
    live dry run: the BOM stuck to ``N-NUMBER``/``CODE``, both natural
    keys went unmapped, and all 410,331 rows dropped as malformed.
    """
    return ("﻿" + header).encode("utf-8").decode("latin-1")


# The exact MASTER.txt header as served (leading BOM, a leading space
# on " KIT MODEL", and a trailing comma that yields a 35th field).
MASTER_HEADER = as_source_text(
    "N-NUMBER,SERIAL NUMBER,MFR MDL CODE,ENG MFR MDL,YEAR MFR,TYPE REGISTRANT,"
    "NAME,STREET,STREET2,CITY,STATE,ZIP CODE,REGION,COUNTY,COUNTRY,LAST ACTION DATE,"
    "CERT ISSUE DATE,CERTIFICATION,TYPE AIRCRAFT,TYPE ENGINE,STATUS CODE,MODE S CODE,"
    "FRACT OWNER,AIR WORTH DATE,OTHER NAMES(1),OTHER NAMES(2),OTHER NAMES(3),"
    "OTHER NAMES(4),OTHER NAMES(5),EXPIRATION DATE,UNIQUE ID,KIT MFR, KIT MODEL,"
    "MODE S CODE HEX,"
)

_MASTER_COLS = [
    "N-NUMBER", "SERIAL NUMBER", "MFR MDL CODE", "ENG MFR MDL", "YEAR MFR",
    "TYPE REGISTRANT", "NAME", "STREET", "STREET2", "CITY", "STATE", "ZIP CODE",
    "REGION", "COUNTY", "COUNTRY", "LAST ACTION DATE", "CERT ISSUE DATE",
    "CERTIFICATION", "TYPE AIRCRAFT", "TYPE ENGINE", "STATUS CODE", "MODE S CODE",
    "FRACT OWNER", "AIR WORTH DATE", "OTHER NAMES(1)", "OTHER NAMES(2)",
    "OTHER NAMES(3)", "OTHER NAMES(4)", "OTHER NAMES(5)", "EXPIRATION DATE",
    "UNIQUE ID", "KIT MFR", "KIT MODEL", "MODE S CODE HEX",
]

ACFTREF_HEADER = as_source_text(
    "CODE,MFR,MODEL,TYPE-ACFT,TYPE-ENG,AC-CAT,BUILD-CERT-IND,NO-ENG,NO-SEATS,"
    "AC-WEIGHT,SPEED,TC-DATA-SHEET,TC-DATA-HOLDER,"
)


def master_row(**over: str) -> str:
    """Build one MASTER line, space-padded like the source, plus the
    trailing comma that makes the 35th field."""
    vals = {c: "" for c in _MASTER_COLS}
    vals.update(over)
    return ",".join(vals[c] for c in _MASTER_COLS) + ","


def parse_master_lines(*lines: str) -> tuple[list[dict], faa.IngestSummary]:
    """Run the real parser over a synthetic MASTER file."""
    import csv

    summary = faa.IngestSummary()
    rows = iter(csv.reader([MASTER_HEADER, *lines]))
    return list(faa.parse_master(rows, summary)), summary


# ─── the fence ───────────────────────────────────────────────────


def test_fence_constants_are_suppress_and_staged() -> None:
    """P1's fence values, per helen's decision doc — NOT a 4th surface_mode."""
    assert faa.FENCE_SURFACE_MODE == "suppress"
    assert faa.FENCE_PUBLICATION_STATE == "staged"


def test_every_parsed_row_carries_the_fence() -> None:
    """The ingester stamps the fence explicitly rather than leaning on
    the column default — a default is one ``server_default`` edit away
    from silently becoming 'open'."""
    parsed, _ = parse_master_lines(
        master_row(**{"N-NUMBER": "1234A", "UNIQUE ID": "00001", "NAME": "SMITH JOHN"}),
        master_row(**{"N-NUMBER": "5678B", "UNIQUE ID": "00002", "NAME": "9AT LLC"}),
    )
    assert len(parsed) == 2
    for row in parsed:
        assert row["surface_mode"] == "suppress"
        assert row["publication_state"] == "staged"


def test_aircraft_model_pins_both_gates_with_check_constraints() -> None:
    """The database is the backstop: a row can only ever be
    suppress+staged. Opening the fence must be a migration that drops
    a named constraint, not an UPDATE."""
    checks = {
        c.name: str(c.sqltext)
        for c in Aircraft.__table__.constraints
        if hasattr(c, "sqltext")
    }
    assert "ck_aircraft_p1_suppress" in checks
    assert "ck_aircraft_p1_staged" in checks
    assert "suppress" in checks["ck_aircraft_p1_suppress"]
    assert "staged" in checks["ck_aircraft_p1_staged"]


def test_aircraft_reference_is_staged_but_claims_no_privacy_mode() -> None:
    """Manufacturer/model reference data has no personal information;
    it takes the lifecycle gate but must not carry a surface_mode that
    would read as a privacy claim it is not making."""
    names = {
        c.name for c in AircraftReference.__table__.constraints if hasattr(c, "sqltext")
    }
    assert "ck_aircraft_reference_p1_staged" in names
    assert "surface_mode" not in AircraftReference.__table__.columns


# ─── no graph coupling (P1 is standalone) ────────────────────────


def test_entity_type_gained_no_aircraft_member() -> None:
    """P1 explicitly does not make aircraft a graph entity kind."""
    values = {e.value for e in EntityType}
    assert "aircraft" not in values
    assert "asset" not in values


def test_ingester_touches_no_graph_table() -> None:
    """No canonical entity, edge, citation or Neo4j projection in P1 —
    resolution and surfacing are P2 and are Tony's call."""
    src = inspect.getsource(faa)
    for forbidden in (
        "CanonicalEntity",
        "CanonicalEdge",
        "SourceCitation",
        "neo4j",
        "project_to_neo4j",
    ):
        assert forbidden not in src, f"P1 must not reference {forbidden}"


# ─── parsing against the real layout ─────────────────────────────


def test_master_row_maps_to_the_right_columns() -> None:
    """Positions come from the file's own header; the BOM and the
    padded values must not leak into the parsed record."""
    parsed, summary = parse_master_lines(
        master_row(
            **{
                "N-NUMBER": "100  ",
                "SERIAL NUMBER": "5334          ",
                "MFR MDL CODE": "7100510",
                "YEAR MFR": "1940",
                "TYPE REGISTRANT": "1",
                "NAME": "BENE MARY D       ",
                "STREET": "PO BOX 329    ",
                "CITY": "KETCHUM   ",
                "STATE": "OK",
                "ZIP CODE": "743490329 ",
                "LAST ACTION DATE": "20230122",
                "CERT ISSUE DATE": "20050506",
                "AIR WORTH DATE": "19540430",
                "UNIQUE ID": "50002263",
                "MODE S CODE HEX": "A00722",
            }
        )
    )
    assert summary.master_malformed == 0
    (row,) = parsed
    assert row["n_number"] == "100"
    assert row["serial_number"] == "5334"
    assert row["registrant_name"] == "BENE MARY D"
    assert row["type_registrant"] == "1"
    assert row["street"] == "PO BOX 329"
    assert row["city"] == "KETCHUM"
    assert row["state"] == "OK"
    assert row["year_mfr"] == 1940
    assert row["unique_id"] == "50002263"
    assert row["mode_s_code_hex"] == "A00722"
    assert row["last_action_date"].isoformat() == "2023-01-22"
    assert row["cert_issue_date"].isoformat() == "2005-05-06"
    assert row["air_worth_date"].isoformat() == "1954-04-30"
    # Blank source fields become NULL, never the empty string.
    assert row["street2"] is None
    assert row["expiration_date"] is None
    assert row["other_names"] == []


def test_other_names_collapse_in_source_order() -> None:
    """OTHER NAMES(1..5) become an ordered list of the non-blank ones —
    the order is the source's and is meaningful."""
    parsed, _ = parse_master_lines(
        master_row(
            **{
                "N-NUMBER": "1A",
                "UNIQUE ID": "9",
                "OTHER NAMES(1)": "ALPHA HOLDINGS  ",
                "OTHER NAMES(2)": "   ",
                "OTHER NAMES(3)": "BRAVO TRUST",
            }
        )
    )
    assert parsed[0]["other_names"] == ["ALPHA HOLDINGS", "BRAVO TRUST"]


def test_fract_owner_blank_is_null_not_false() -> None:
    """A blank FRACT OWNER means the source made no claim — recording
    it as False would assert something the registry did not say."""
    yes, _ = parse_master_lines(
        master_row(**{"N-NUMBER": "1A", "UNIQUE ID": "1", "FRACT OWNER": "Y"})
    )
    blank, _ = parse_master_lines(
        master_row(**{"N-NUMBER": "2B", "UNIQUE ID": "2", "FRACT OWNER": " "})
    )
    assert yes[0]["fract_owner"] is True
    assert blank[0]["fract_owner"] is None


def test_malformed_rows_are_counted_not_silently_dropped() -> None:
    """A row whose field count does not match the header means the
    layout moved. It is skipped, but the count + sample surface it."""
    parsed, summary = parse_master_lines(
        master_row(**{"N-NUMBER": "1A", "UNIQUE ID": "1"}),
        "TOO,FEW,FIELDS",
    )
    assert len(parsed) == 1
    assert summary.master_malformed == 1
    assert summary.malformed_sample


def test_row_missing_its_natural_key_is_rejected() -> None:
    """unique_id is the upsert conflict target — a row without one
    cannot be made idempotent, so it is not loaded."""
    parsed, summary = parse_master_lines(master_row(**{"N-NUMBER": "1A", "UNIQUE ID": "  "}))
    assert parsed == []
    assert summary.master_malformed == 1


def test_acftref_maps_and_coerces_zero_padded_ints() -> None:
    """NO-ENG/NO-SEATS are zero-padded in the source ('02', '015')."""
    import csv

    summary = faa.IngestSummary()
    line = (
        "0020901,AAR AIRLIFT GROUP INC   ,UH-60A   ,6,3 ,1,0,02,015,CLASS 3,0000,"
        "               ,          ,"
    )
    rows = list(faa.parse_acftref(iter(csv.reader([ACFTREF_HEADER, line])), summary))
    assert summary.acftref_malformed == 0
    (row,) = rows
    assert row["code"] == "0020901"
    assert row["mfr"] == "AAR AIRLIFT GROUP INC"
    assert row["model"] == "UH-60A"
    assert row["no_eng"] == 2
    assert row["no_seats"] == 15
    assert row["speed"] == 0
    assert row["ac_weight"] == "CLASS 3"
    assert row["publication_state"] == "staged"


def test_bad_dates_and_ints_become_null_not_errors() -> None:
    """One unparseable field is a fact about one source row, not a
    reason to abort a 316k-row load."""
    assert faa._as_date("20240823").isoformat() == "2024-08-23"
    assert faa._as_date("        ") is None
    assert faa._as_date("00000000") is None
    assert faa._as_date("2024") is None
    assert faa._as_int("  ") is None
    assert faa._as_int("ABC") is None
    assert faa._as_int("015") == 15


# ─── load-path safety ────────────────────────────────────────────


def test_chunk_size_stays_under_the_postgres_parameter_cap() -> None:
    """A multi-row VALUES insert spends rows x columns bind params and
    Postgres hard-caps a statement at 65535. Blowing it fails the load
    mid-run, so the chunk size is derived from the column count."""
    for table in (Aircraft, AircraftReference):
        size = faa._chunk_size(table)
        assert size >= 1
        assert size * len(table.__table__.columns) < 65535


def test_latin1_bom_does_not_break_the_first_column() -> None:
    """Regression, 2026-08-29 live dry run. The BOM decodes to 'ï»¿'
    under latin-1, so it lands on N-NUMBER and CODE — the two natural
    keys. Stripping only U+FEFF drops 100% of both files."""
    assert MASTER_HEADER.startswith("ï»¿N-NUMBER")
    assert ACFTREF_HEADER.startswith("ï»¿CODE")
    assert faa._norm_header("ï»¿N-NUMBER") == "N-NUMBER"
    assert faa._norm_header("﻿CODE") == "CODE"

    parsed, summary = parse_master_lines(
        master_row(**{"N-NUMBER": "1234A", "UNIQUE ID": "77", "NAME": "SMITH JOHN"})
    )
    assert summary.master_malformed == 0
    assert parsed[0]["n_number"] == "1234A"


def test_unmappable_header_raises_instead_of_dropping_every_row() -> None:
    """A header we cannot map is a source-layout change, not bad data.
    Silently counting 316k individually-malformed rows buries the
    cause; failing the run surfaces it."""
    import csv

    import pytest

    summary = faa.IngestSummary()
    bad = "WING-COUNT,PAINT-COLOUR,"
    with pytest.raises(ValueError, match="missing required column"):
        list(faa.parse_master(iter(csv.reader([bad, "1,2,"])), summary))
    with pytest.raises(ValueError, match="missing required column"):
        list(faa.parse_acftref(iter(csv.reader([bad, "1,2,"])), summary))


def test_batch_id_is_derived_from_content() -> None:
    """Re-running an unchanged snapshot must land on the same batch so
    the upsert is a no-op rather than a duplicate load."""
    src = inspect.getsource(faa.ingest_faa_aircraft)
    assert 'f"faa-aircraft-{sha[:12]}"' in src


# ─── PII redaction on write failure (P2 hard requirement) ────────

# A realistic driver message. Postgres' CheckViolation DETAIL echoes
# the ENTIRE failing row, and SQLAlchemy additionally embeds the bound
# parameters — so the raw text carries the registrant's name and home
# address. Neither may reach a log, a response or a traceback.
_LEAKY = (
    '(psycopg.errors.CheckViolation) new row for relation "aircraft" '
    'violates check constraint "ck_aircraft_p1_suppress"\n'
    'DETAIL:  Failing row contains (af0d80ac, 00600060, 100, 5334, 7100510, '
    '17003, 1940, 1, BENE MARY D, PO BOX 329, null, KETCHUM, OK, 743490329, '
    '2, 097, US, [], open, staged).\n'
    "[parameters: {'registrant_name': 'BENE MARY D', 'street': 'PO BOX 329', "
    "'city': 'KETCHUM', 'zip_code': '743490329'}]"
)

_PII_TOKENS = ("BENE MARY D", "PO BOX 329", "KETCHUM", "743490329", "DETAIL:  Failing row")


class _FakeDiag:
    constraint_name = "ck_aircraft_p1_suppress"


class _FakeOrig(Exception):
    diag = _FakeDiag()


class _FakeIntegrityError(Exception):
    """Stands in for sqlalchemy.exc.IntegrityError, message and all."""

    orig = _FakeOrig()

    def __str__(self) -> str:
        return _LEAKY


def test_write_failure_message_carries_no_registrant_pii() -> None:
    """The redacted error must not contain the name, street, city, zip
    or the raw DETAIL — that is the whole point of the fence."""
    err = faa._redacted(
        _FakeIntegrityError(),
        Aircraft,
        [{"n_number": "100", "registrant_name": "BENE MARY D", "street": "PO BOX 329"}],
    )
    msg = str(err)
    for token in _PII_TOKENS:
        assert token not in msg, f"redacted message leaked {token!r}: {msg}"


def test_write_failure_message_keeps_what_is_actionable() -> None:
    """Redaction must not make the error useless: the constraint name
    and the tail number(s) are what an operator needs."""
    err = faa._redacted(
        _FakeIntegrityError(), Aircraft, [{"n_number": "100"}, {"n_number": "444WN"}]
    )
    msg = str(err)
    assert "ck_aircraft_p1_suppress" in msg
    assert "100" in msg and "444WN" in msg
    assert "aircraft" in msg
    assert "REDACTED" in msg
    assert isinstance(err, faa.FencedWriteError)


def test_row_ids_caps_the_list_and_never_emits_names() -> None:
    """A failed chunk is up to ~1.3k rows; the handle stays bounded and
    is drawn only from tail numbers, which are painted on the outside
    of the aircraft. Names and addresses are not."""
    rows = [{"n_number": f"N{i}", "registrant_name": "SMITH JOHN"} for i in range(20)]
    out = faa._row_ids(rows)
    assert "SMITH JOHN" not in out
    assert "+12 more" in out
    # 8 ids listed => 7 separators.
    assert out.count(",") == 7
    assert out.startswith("N0, N1,")


def test_upsert_reraises_redacted_with_original_suppressed() -> None:
    """``from None`` matters: a chained exception prints the original
    in the traceback, which would re-leak the DETAIL this suppresses."""
    import asyncio

    class _Session:
        async def execute(self, *_a, **_k):
            raise _FakeIntegrityError()

    async def go():
        await faa._upsert_chunk(
            _Session(), Aircraft, [{"n_number": "100", "unique_id": "1"}], "unique_id"
        )

    try:
        asyncio.run(go())
    except faa.FencedWriteError as err:
        assert err.__cause__ is None and err.__suppress_context__ is True
        for token in _PII_TOKENS:
            assert token not in str(err)
    else:
        raise AssertionError("expected FencedWriteError")


def test_ingester_never_logs_a_raw_exception_on_the_write_path() -> None:
    """Guards the other leak route: logging the caught exception (or
    using logger.exception, which prints the traceback) would emit the
    DETAIL even though the raised error is clean."""
    src = inspect.getsource(faa)
    for bad in ("logger.exception", "logger.error(exc", "print(exc", "str(exc)"):
        assert bad not in src, f"write path must not emit the raw exception: {bad}"
