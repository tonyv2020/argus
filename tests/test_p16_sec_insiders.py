"""P1.6 — SEC Section 16 ownership parsing tests.

Hermetic: the XML fragments below are the real shapes served from
``sec.gov/Archives/edgar/data/…`` — including the two different boolean
encodings the ownership schema uses across versions, both of which
appear inside a single issuer's filing history.
"""

from __future__ import annotations

from app.models import EdgeRelation, SourceKind
from app.services.ingest.sec_insiders import (
    OFFICER_SIGNAL_NAMESPACE,
    OWNERSHIP_FORMS,
    ReportingOwner,
    iter_ownership_filings,
    ownership_document_url,
    parse_ownership_document,
)

# Verbatim shape of Peter Thiel's 2026-03-04 Palantir Form 4.
THIEL_FORM4 = """<?xml version="1.0"?>
<ownershipDocument>
    <schemaVersion>X0508</schemaVersion>
    <documentType>4</documentType>
    <periodOfReport>2026-03-02</periodOfReport>
    <issuer>
        <issuerCik>0001321655</issuerCik>
        <issuerName>Palantir Technologies Inc.</issuerName>
        <issuerTradingSymbol>PLTR</issuerTradingSymbol>
    </issuer>
    <reportingOwner>
        <reportingOwnerId>
            <rptOwnerCik>0001211060</rptOwnerCik>
            <rptOwnerName>THIEL PETER</rptOwnerName>
        </reportingOwnerId>
        <reportingOwnerRelationship>
            <isDirector>true</isDirector>
            <isOfficer>false</isOfficer>
            <isTenPercentOwner>false</isTenPercentOwner>
            <isOther>false</isOther>
            <officerTitle></officerTitle>
        </reportingOwnerRelationship>
    </reportingOwner>
</ownershipDocument>
"""

# The other encoding — 1/0 rather than true/false — plus an officer title.
SANKAR_FORM4 = """<ownershipDocument>
    <issuer><issuerCik>0001321655</issuerCik>
      <issuerName>Palantir Technologies Inc.</issuerName></issuer>
    <reportingOwner>
        <reportingOwnerId>
            <rptOwnerCik>0001824159</rptOwnerCik>
            <rptOwnerName>Sankar Shyam</rptOwnerName>
        </reportingOwnerId>
        <reportingOwnerRelationship>
            <isDirector>0</isDirector>
            <isOfficer>1</isOfficer>
            <isTenPercentOwner>0</isTenPercentOwner>
            <officerTitle>See Remarks</officerTitle>
        </reportingOwnerRelationship>
    </reportingOwner>
</ownershipDocument>
"""

# A pure investor — 10% owner and nothing else.
INVESTOR_FORM4 = """<ownershipDocument>
    <issuer><issuerCik>0001820953</issuerCik>
      <issuerName>Affirm Holdings, Inc.</issuerName></issuer>
    <reportingOwner>
        <reportingOwnerId>
            <rptOwnerCik>0001211060</rptOwnerCik>
            <rptOwnerName>THIEL PETER</rptOwnerName>
        </reportingOwnerId>
        <reportingOwnerRelationship>
            <isDirector>0</isDirector>
            <isOfficer>0</isOfficer>
            <isTenPercentOwner>1</isTenPercentOwner>
        </reportingOwnerRelationship>
    </reportingOwner>
</ownershipDocument>
"""


def test_parses_true_false_encoding() -> None:
    doc = parse_ownership_document(THIEL_FORM4)
    assert doc is not None
    assert doc.issuer_cik == "0001321655"
    (owner,) = doc.owners
    assert owner.cik == "0001211060"
    assert owner.name == "THIEL PETER"
    assert owner.is_director is True
    assert owner.is_officer is False
    assert owner.holds_position is True


def test_parses_one_zero_encoding_and_officer_title() -> None:
    """Both encodings appear inside one issuer's filing history — a
    parser that handles only ``true``/``false`` silently drops every
    officer filed under the older schema."""
    doc = parse_ownership_document(SANKAR_FORM4)
    assert doc is not None
    (owner,) = doc.owners
    assert owner.is_officer is True
    assert owner.is_director is False
    assert owner.officer_title == "See Remarks"
    assert owner.holds_position is True


def test_ten_percent_owner_is_not_a_position() -> None:
    """``held_position`` has to mean a seat or an office. A pure
    investor stake is real, cited, and NOT a position."""
    doc = parse_ownership_document(INVESTOR_FORM4)
    assert doc is not None
    (owner,) = doc.owners
    assert owner.is_ten_percent_owner is True
    assert owner.holds_position is False


def test_officer_title_entities_are_decoded() -> None:
    """The parser is a regex, so XML entities are not decoded for us —
    and filers really do type ``COO &amp; CFO`` into ``officerTitle``.
    Left raw it lands verbatim in the edge metadata."""
    doc = parse_ownership_document(
        SANKAR_FORM4.replace(
            "<officerTitle>See Remarks</officerTitle>",
            "<officerTitle>COO &amp; CFO</officerTitle>",
        )
    )
    assert doc is not None
    assert doc.owners[0].officer_title == "COO & CFO"


def test_unparsable_document_returns_none_not_empty() -> None:
    """An error body or an XSL-rendered HTML page must be SKIPPED. If it
    returned an empty filing the caller could not tell 'no owners' from
    'could not read this'."""
    assert parse_ownership_document("<html>404 Not Found</html>") is None
    assert parse_ownership_document("") is None


def test_role_label_reads_correctly() -> None:
    owner = ReportingOwner(
        cik="0001823951", name="Karp Alexander C.",
        is_director=True, is_officer=True, is_ten_percent_owner=False,
        officer_title="Chief Executive Officer",
    )
    assert owner.role_label == "director; Chief Executive Officer"


def test_document_url_strips_the_xsl_rendering_prefix() -> None:
    """``filings.recent.primaryDocument`` points at SEC's XSL-rendered
    HTML view. The machine-readable XML is the same filename in the same
    accession directory, without the ``xsl*/`` segment."""
    url = ownership_document_url(
        1211060, "0001211060-26-000007",
        "xslF345X05/form4-03042026_090310.xml",
    )
    assert url == (
        "https://www.sec.gov/Archives/edgar/data/1211060/"
        "000121106026000007/form4-03042026_090310.xml"
    )


def test_document_url_handles_an_already_raw_document() -> None:
    url = ownership_document_url(1321655, "0001-25-000001", "form4.xml")
    assert url.endswith("/form4.xml")
    assert "xsl" not in url


def test_iter_ownership_filings_zips_the_parallel_lists() -> None:
    """SEC stores ``filings.recent`` as parallel lists, not row objects —
    a naive read pairs the wrong accession with the wrong form."""
    subs = {
        "filings": {
            "recent": {
                "form": ["4", "10-K", "3", "144"],
                "accessionNumber": ["a-4", "a-10k", "a-3", "a-144"],
                "filingDate": ["2026-03-04", "2026-02-01", "2020-09-30", "2026-01-01"],
                "primaryDocument": ["d4.xml", "d10k.htm", "d3.xml", "d144.htm"],
            }
        }
    }
    rows = iter_ownership_filings(subs)
    assert [r["form"] for r in rows] == ["4", "3"]
    assert rows[0]["accession"] == "a-4"
    assert rows[1]["primary_document"] == "d3.xml"


def test_amendments_are_ingested() -> None:
    """A 4/A carries the same issuer/owner/relationship block and is the
    corrected record — dropping it loses the correction."""
    assert "4/A" in OWNERSHIP_FORMS and "3/A" in OWNERSHIP_FORMS


def test_officer_signal_namespace_is_a_scrutiny_hard_signal() -> None:
    """A net-new insider is created ``suppress``; the ONLY thing that
    opens them is scrutiny recognising this alias namespace as a public
    signal. If the constants drift, every insider stays dark forever."""
    from app.services.scrutiny import _PUBLIC_SOURCE_SYSTEMS

    assert OFFICER_SIGNAL_NAMESPACE in _PUBLIC_SOURCE_SYSTEMS


def test_uses_the_existing_relation_and_citation_kind() -> None:
    """No new enum values — an officer edge is a ``held_position`` cited
    to the corporate registry, the same shapes P1.5 and P3b use."""
    assert EdgeRelation.HELD_POSITION.value == "held_position"
    assert SourceKind.CORPORATE_REGISTRY.value == "corporate_registry"
