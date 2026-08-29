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

# The exact MASTER.txt header as served (leading BOM, a leading space
# on " KIT MODEL", and a trailing comma that yields a 35th field).
MASTER_HEADER = (
    "﻿N-NUMBER,SERIAL NUMBER,MFR MDL CODE,ENG MFR MDL,YEAR MFR,TYPE REGISTRANT,"
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

ACFTREF_HEADER = (
    "﻿CODE,MFR,MODEL,TYPE-ACFT,TYPE-ENG,AC-CAT,BUILD-CERT-IND,NO-ENG,NO-SEATS,"
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


def test_batch_id_is_derived_from_content() -> None:
    """Re-running an unchanged snapshot must land on the same batch so
    the upsert is a no-op rather than a duplicate load."""
    src = inspect.getsource(faa.ingest_faa_aircraft)
    assert 'f"faa-aircraft-{sha[:12]}"' in src
