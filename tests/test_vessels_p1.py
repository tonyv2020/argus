"""Vessels P1 — fence, citation, PII redaction, parser.

The aircraft arc paid for each of these lessons once. This file asserts
they were baked in from the start rather than retrofitted, so vessels
never repeats them.
"""

from __future__ import annotations

import inspect
import pathlib

from app.models import EntityType, Vessel, VesselSourceSnapshot
from app.services.ingest import ofac_sdn_vessels as ofac

#: Owner columns that identify a person or where they live.
PII_COLUMNS = (
    "owner_name_raw",
    "owner_street",
    "owner_city",
    "owner_state",
    "owner_postal_code",
)


def _checks(model) -> dict[str, str]:
    return {
        c.name: str(c.sqltext) for c in model.__table__.constraints if hasattr(c, "sqltext")
    }


# ── the fence ────────────────────────────────────────────────────


def test_vessel_fence_is_pinned_closed() -> None:
    """P1 surfaces nothing; both gates are pinned by equality CHECK."""
    checks = _checks(Vessel)
    assert "suppress" in checks["ck_vessel_p1_suppress"]
    assert "staged" in checks["ck_vessel_p1_staged"]


def test_vessel_defaults_are_dark() -> None:
    cols = Vessel.__table__.columns
    assert cols["surface_mode"].server_default.arg == "suppress"
    assert cols["publication_state"].server_default.arg == "staged"


def test_parser_stamps_the_fence_on_every_row() -> None:
    """Explicit, not leaning on the column default — a default is one
    server_default edit away from becoming 'open'."""
    summary = ofac.VesselIngestSummary()
    rows = list(ofac.parse_sdn_vessels(_SAMPLE_CSV, summary))
    assert rows
    for r in rows:
        assert r["surface_mode"] == "suppress"
        assert r["publication_state"] == "staged"


# ── structural citation ──────────────────────────────────────────


def test_an_uncited_vessel_is_unrepresentable() -> None:
    for name in ("snapshot_id", "source_url", "source_sha256"):
        assert Vessel.__table__.columns[name].nullable is False
    # NOT NULL alone would accept '' as a citation
    assert "ck_vessel_cited" in _checks(Vessel)


def test_source_is_constrained_so_a_third_source_needs_a_migration() -> None:
    for model in (Vessel, VesselSourceSnapshot):
        found = [v for v in _checks(model).values() if "ofac_sdn" in v and "uscg_nvdc" in v]
        assert found, model.__name__


# ── the ingest-preserves-publish-state lesson ────────────────────


def test_upsert_never_refreshes_the_gate_columns() -> None:
    """THE aircraft regression, baked in from day one here.

    On aircraft this was found only after a pilot had been promoted —
    one weekly run away from silently un-publishing 5,210 rows with no
    audit row.
    """
    assert ofac._GATE_COLUMNS == {"surface_mode", "publication_state"}
    src = inspect.getsource(ofac._upsert)
    assert "_GATE_COLUMNS" in src


def test_upsert_statement_really_omits_the_gates_from_its_SET_clause() -> None:
    """Behavioural, not a re-implementation: build the real statement and
    read its actual ON CONFLICT ... DO UPDATE SET clause."""
    import asyncio

    from sqlalchemy.dialects import postgresql

    captured = {}

    class _Session:
        async def execute(self, stmt):
            captured["stmt"] = stmt

    asyncio.run(
        ofac._upsert(
            _Session(),
            [
                {
                    "source": "ofac_sdn", "source_key": "1", "vessel_name": "X",
                    "surface_mode": "suppress", "publication_state": "staged",
                    "owner_name_raw": "SOMEONE", "snapshot_id": "s",
                    "source_url": "u", "source_sha256": "d" * 64, "batch_id": "b",
                }
            ],
        )
    )
    sql = str(captured["stmt"].compile(dialect=postgresql.dialect()))
    set_clause = sql.split("DO UPDATE SET", 1)[1]
    assert "surface_mode =" not in set_clause
    assert "publication_state =" not in set_clause
    # ...while source data still refreshes
    assert "vessel_name =" in set_clause
    assert "owner_name_raw =" in set_clause


def test_chunk_size_stays_under_the_postgres_parameter_cap() -> None:
    assert ofac._chunk_size() * len(Vessel.__table__.columns) < 65535


# ── PII-safe errors ──────────────────────────────────────────────

_LEAKY = (
    '(psycopg.errors.CheckViolation) new row for relation "vessels" violates '
    'check constraint "ck_vessel_p1_suppress"\n'
    "DETAIL:  Failing row contains (id, ofac_sdn, 4238, MAR AZUL, "
    "Samir de Navegacion S.A., 12 QUAYSIDE RD, HAVANA, CU).\n"
    "[parameters: {'owner_name_raw': 'Samir de Navegacion S.A.', "
    "'owner_street': '12 QUAYSIDE RD'}]"
)


class _FakeDiag:
    constraint_name = "ck_vessel_p1_suppress"


class _FakeOrig(Exception):
    diag = _FakeDiag()


class _FakeIntegrityError(Exception):
    orig = _FakeOrig()

    def __str__(self) -> str:
        return _LEAKY


def test_write_failure_carries_no_owner_pii() -> None:
    err = ofac._redacted(
        _FakeIntegrityError(),
        [{"source_key": "4238", "owner_name_raw": "Samir de Navegacion S.A."}],
    )
    msg = str(err)
    for token in ("Samir de Navegacion", "QUAYSIDE", "HAVANA", "DETAIL:  Failing row"):
        assert token not in msg, f"leaked {token!r}"
    assert "ck_vessel_p1_suppress" in msg and "4238" in msg and "REDACTED" in msg


def test_upsert_reraises_redacted_with_the_original_suppressed() -> None:
    """``from None``: a chained exception reprints the DETAIL."""
    import asyncio

    class _Session:
        async def execute(self, *_a, **_k):
            raise _FakeIntegrityError()

    try:
        asyncio.run(ofac._upsert(_Session(), [{"source_key": "4238", "vessel_name": "X"}]))
    except ofac.VesselWriteError as err:
        assert err.__cause__ is None and err.__suppress_context__ is True
        assert "Samir de Navegacion" not in str(err)
    else:
        raise AssertionError("expected VesselWriteError")


def test_ingester_never_logs_a_raw_exception() -> None:
    src = inspect.getsource(ofac)
    for bad in ("logger.exception", "logger.error(exc", "print(exc", "str(exc)"):
        assert bad not in src, bad


# ── P1 isolation: no read path, no graph ─────────────────────────


def test_entity_type_gained_no_vessel_member() -> None:
    values = {e.value for e in EntityType}
    assert "vessel" not in values


def test_ingester_touches_no_graph_or_read_path() -> None:
    src = inspect.getsource(ofac)
    for forbidden in ("CanonicalEntity", "CanonicalEdge", "SourceCitation",
                      "neo4j", "read_gate", "app.main"):
        assert forbidden not in src, f"P1 must not reference {forbidden}"


def test_no_read_path_references_vessels_at_all() -> None:
    """P1 isolation, checked across the whole app rather than asserted."""
    app_dir = pathlib.Path(inspect.getfile(ofac)).resolve().parents[2]
    scanned = 0
    offenders = []
    for path in app_dir.rglob("*.py"):
        if path.name in {"models.py", "ofac_sdn_vessels.py"}:
            continue
        scanned += 1
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "Vessel" in text or "vessels" in text:
            offenders.append(str(path.relative_to(app_dir)))
    # Guard against a vacuous pass: an empty scan proves nothing, and the
    # first version of this test walked a directory that did not exist.
    assert scanned > 20, f"scan found only {scanned} modules — wrong root"
    assert offenders == [], f"vessels must not reach any other module in P1: {offenders}"


def test_migration_revision_id_fits_the_version_column() -> None:
    root = pathlib.Path(inspect.getfile(ofac)).resolve().parents[3]
    path = root / "alembic" / "versions" / "0015_vessel_asset_layer.py"
    assert path.exists(), f"migration not found at {path}"
    rev = next(
        ln.split("=", 1)[1].strip().strip("\"'")
        for ln in path.read_text(encoding="utf-8").splitlines()
        if ln.startswith("revision = ")
    )
    assert len(rev) <= 32, f"{rev} is {len(rev)} chars"


# ── parser against the real OFAC layout ──────────────────────────

_SAMPLE_CSV = (
    '4238,"MAR AZUL","vessel","CUBA",-0- ,"CL2192","Tug",-0- ,"212","Cuba",'
    '"Samir de Navegacion S.A.",-0- \r\n'
    '9999,"SHADOW STAR","vessel","RUSSIA",-0- ,-0- ,"Crude Oil Tanker","50000",'
    '"49000","Panama","Some Owner Ltd",'
    '"Vessel Registration Identification IMO 9223368; secondary sanctions risk"\r\n'
    '12,"NOT A SHIP","individual","CUBA",-0- ,-0- ,-0- ,-0- ,-0- ,-0- ,-0- ,-0- \r\n'
)


def test_parses_only_vessel_rows_and_maps_columns() -> None:
    summary = ofac.VesselIngestSummary()
    rows = list(ofac.parse_sdn_vessels(_SAMPLE_CSV, summary))
    assert summary.total_rows == 3
    assert len(rows) == 2, "the individual row must not be ingested"
    a, b = rows
    assert a["source_key"] == "4238"
    assert a["vessel_name"] == "MAR AZUL"
    assert a["call_sign"] == "CL2192"
    assert a["vessel_type"] == "Tug"
    assert a["flag"] == "Cuba"
    assert a["owner_name_raw"] == "Samir de Navegacion S.A."
    assert a["is_sanctioned"] is True
    assert b["imo_number"] == "9223368"
    assert b["gross_tonnage"] == "49000"


def test_ofac_null_sentinel_becomes_NULL_not_the_literal() -> None:
    """OFAC writes '-0-' for absent values; storing that literally would
    make '-0-' look like a real call sign."""
    summary = ofac.VesselIngestSummary()
    rows = list(ofac.parse_sdn_vessels(_SAMPLE_CSV, summary))
    assert rows[0]["tonnage"] is None
    assert rows[0]["sanctions_remarks"] is None
    assert rows[1]["call_sign"] is None
    assert ofac._clean("-0- ") is None
    assert ofac._clean("   ") is None


def test_imo_is_extracted_not_guessed() -> None:
    assert ofac.extract_imo("Vessel Registration Identification IMO 9223368") == "9223368"
    assert ofac.extract_imo("IMO #1234567 something") == "1234567"
    assert ofac.extract_imo("no imo here") is None
    assert ofac.extract_imo(None) is None
    # a 6-digit number is not an IMO and must not be coerced into one
    assert ofac.extract_imo("IMO 12345") is None


def test_batch_id_is_derived_from_content() -> None:
    src = inspect.getsource(ofac.ingest_ofac_vessels)
    assert 'f"ofac-sdn-{sha[:12]}"' in src
