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


def test_no_READ_PATH_module_references_vessels() -> None:
    """P1/P2 isolation, stated precisely.

    Ingest and analysis modules legitimately handle vessels — that is
    their job. The invariant is that no module on the READ or
    PROJECTION path knows vessels exist, so there is nothing to surface
    through even by accident.
    """
    app_dir = pathlib.Path(inspect.getfile(ofac)).resolve().parents[2]
    read_path = [
        app_dir / "main.py",
        app_dir / "services" / "read_gate.py",
        *(app_dir / "services" / "graph").rglob("*.py"),
        app_dir / "services" / "ingest" / "project_to_neo4j.py",
        app_dir / "static" / "index.html",
    ]
    scanned = 0
    offenders = []
    for path in read_path:
        if not path.exists():
            continue
        scanned += 1
        text = path.read_text(encoding="utf-8", errors="ignore")
        if "Vessel" in text or "vessels" in text:
            offenders.append(path.name)
    # Guard against a vacuous pass: an earlier version of this test
    # walked a directory that did not exist and passed on an empty scan.
    assert scanned >= 5, f"scan covered only {scanned} read-path files"
    assert offenders == [], f"the read path must not know vessels exist: {offenders}"


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


# ── Vessels P2 owner-resolution dry run (writes nothing) ─────────


def test_p2_dryrun_writes_nothing() -> None:
    """Read-only by construction: the DB refuses a write, and no
    INSERT/UPDATE of any vessel or edge appears in the module."""
    from app.services.ingest import ofac_vessel_owner_dryrun as p2

    src = inspect.getsource(p2)
    assert "conn.read_only = True" in src
    for forbidden in ("insert(", "INSERT", "session.add", "commit()", "AircraftRegistrationEdge"):
        assert forbidden not in src, f"P2 dry run must not {forbidden}"


def test_p2_only_counts_ownership_relations() -> None:
    """'Providing support to' and 'Associate Of' are real relationships
    but they are not ownership — treating them as such would put a
    support entity's name on an asset it does not own."""
    from app.services.ingest import ofac_vessel_owner_dryrun as p2

    assert "Owned or Controlled By" in p2.OWNERSHIP_RELATIONS
    assert "Owns, controls, or operates" in p2.OWNERSHIP_RELATIONS
    for not_ownership in ("Providing support to", "Associate Of", "Family member of",
                          "Acting for or on behalf of", "Leader or official of"):
        assert not_ownership not in p2.OWNERSHIP_RELATIONS


def test_p2_holds_individual_owners_and_blocks_non_owner_types() -> None:
    """Corporate-first: an OFAC 'Individual' owner is held for the
    curated allowlist, and a concept/place canonical can never own."""
    from app.services.ingest import ofac_vessel_owner_dryrun as p2

    src = inspect.getsource(p2.run)
    assert 'otype == "Individual"' in src
    assert "HELD_individual_owner" in src
    assert "is_owner_capable" in src
    assert "is_individual_entity" in src
    assert "HELD_privacy" in src
    assert "DROP_ambiguous" in src
    assert "DROP_single_token" in src


def test_p2_parses_the_real_enhanced_shape() -> None:
    """The vessel→owner link is <relationship><type>…</type>
    <relatedEntity entityId=…>, and the entity id IS the SDN ent_num."""
    import xml.etree.ElementTree as ET

    from app.services.ingest import ofac_vessel_owner_dryrun as p2

    xml = """<sanctionsData xmlns="urn:x">
      <entity id="15036">
        <generalInfo><entityType>Vessel</entityType></generalInfo>
        <names><name><translations><translation>
          <formattedFullName>ARTAVIL</formattedFullName>
        </translation></translations></name></names>
        <relationships>
          <relationship id="144">
            <type>Owned or Controlled By</type>
            <relatedEntity entityId="15117">NATIONAL IRANIAN TANKER COMPANY</relatedEntity>
          </relationship>
          <relationship id="145">
            <type>Providing support to</type>
            <relatedEntity entityId="999">SOME SUPPORTER</relatedEntity>
          </relationship>
        </relationships>
      </entity>
      <entity id="15117">
        <generalInfo><entityType>Entity</entityType></generalInfo>
        <names><name><translations><translation>
          <formattedFullName>NATIONAL IRANIAN TANKER COMPANY</formattedFullName>
        </translation></translations></name></names>
      </entity>
    </sanctionsData>"""
    import tempfile

    with tempfile.NamedTemporaryFile("w", suffix=".xml", delete=False) as fh:
        fh.write(xml)
        path = fh.name
    ET.parse(path)  # well-formed
    entities, links = p2.parse_entities(path)
    assert entities["15036"] == ("Vessel", "ARTAVIL")
    assert entities["15117"] == ("Entity", "NATIONAL IRANIAN TANKER COMPANY")
    # only the OWNERSHIP relationship is captured
    assert len(links) == 1
    vid, vname, rtype, oid, oname = links[0]
    assert (vid, vname, rtype, oid) == ("15036", "ARTAVIL", "Owned or Controlled By", "15117")


# ── Vessels P3 plan (dry run; creates nothing, stages nothing) ───


def test_p3_plan_creates_and_stages_nothing() -> None:
    from app.services.ingest import vessels_p3_plan as p3

    src = inspect.getsource(p3)
    assert "conn.read_only = True" in src
    for forbidden in ("INSERT", "insert(", "session.add", "commit()", "CanonicalEntity("):
        assert forbidden not in src, f"the P3 PLAN must not {forbidden}"


def test_p3_crosswalk_is_curated_with_evidence_not_scored() -> None:
    """The crosswalk is the dangerous half. Every accepted entry states
    why it is the SAME legal entity, not merely a related company."""
    from app.services.ingest import vessels_p3_plan as p3

    assert p3.CROSSWALK, "expected at least the PDVSA entry"
    for name, e in p3.CROSSWALK.items():
        assert e["canonical"], name
        assert len(e["evidence"]) > 80, f"{name} needs real evidence"


def test_p3_never_crosswalks_a_subsidiary_to_its_parent() -> None:
    """The aircraft carrier-vs-parent lesson: FedEx Freight is not FedEx
    and Rolls-Royce Corp is not the plc. Rosnefteflot is not Rosneft."""
    from app.services.ingest import vessels_p3_plan as p3

    for sub in (
        "JOINT STOCK COMPANY ROSNEFTEFLOT",
        "GAZPROMNEFT MARINE BUNKER LIMITED LIABILITY COMPANY",
    ):
        assert sub in p3.CROSSWALK_HELD, sub
        assert sub not in p3.CROSSWALK, f"{sub} must NOT be crosswalked"
        assert len(p3.CROSSWALK_HELD[sub]) > 60


def test_p3_holds_the_country_vs_state_company_trap() -> None:
    """IRISL's substring candidate is the COUNTRY 'Islamic Republic of
    Iran', which exists as an ORGANIZATION canonical — so the
    owner-capable guard does not block it. 121 vessels would have been
    attributed to a sovereign nation."""
    from app.services.ingest import vessels_p3_plan as p3

    irisl = "ISLAMIC REPUBLIC OF IRAN SHIPPING LINES"
    assert irisl in p3.CROSSWALK_HELD
    assert irisl not in p3.CROSSWALK


def test_p3_acronym_requires_four_letters() -> None:
    """At three letters the strategy collided on live data: DALIAN OCEAN
    FISHING COMPANY and DEFENSE OF FREEDOM PAC both reduce to 'dof'."""
    from app.services.ingest import vessels_p3_plan as p3

    assert p3.acronym("PETROLEOS DE VENEZUELA, S.A.") == "pdvsa"
    assert p3.acronym("DALIAN OCEAN FISHING COMPANY LIMITED") == "dof"
    assert p3.acronym("DEFENSE OF FREEDOM PAC") == "dof"
    src = inspect.getsource(p3.find_crosswalk_candidates)
    assert "len(acr) >= 4" in src
