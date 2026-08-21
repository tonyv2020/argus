"""P1.5.2 — contribution → member linkage tests.

Hermetic. The linkage decision is pure once the FEC bulk rows are
parsed, so the parts that decide *which member gets the money* are all
testable without HTTP or a DB:

* CCL parsing drops the file's committee-to-itself rows.
* Every hop is keyed on an external id — a name never enters the join.
* A committee shared by two current members is REFUSED, not guessed.
* Designation ranking prefers the member's principal campaign committee.
"""

from __future__ import annotations

from app.services.ingest.congress_money_link import (
    COMMITTEE_NAMESPACES,
    DEFAULT_CYCLES,
    CclRow,
    parse_ccl,
    select_member_links,
)

# CAND_ID | CAND_ELECTION_YR | FEC_ELECTION_YR | CMTE_ID | CMTE_TP | CMTE_DSGN | LINKAGE_ID
_SAMPLE = "\n".join(
    [
        "H0AL01055|2026|2026|C00697789|H|P|259584",
        "S2TX00312|2024|2024|C00492785|S|P|123456",
        # committee-to-itself row — the file carries these; not a linkage.
        "C00778159|2022|2026|C00778159|Q|D|260464",
        # malformed / short row
        "garbage",
        "",
    ]
)


def test_parse_ccl_keeps_only_candidate_rows() -> None:
    """CAND_ID must be a real candidate id (H/S/P). The committee-keyed
    rows in the file are not candidate linkages."""
    rows = parse_ccl(_SAMPLE, 2026)
    assert [r.candidate_id for r in rows] == ["H0AL01055", "S2TX00312"]
    assert all(r.committee_id.startswith("C") for r in rows)
    assert rows[0].designation == "P"
    assert rows[0].committee_type == "H"
    assert rows[0].cycle == 2026


def test_parse_ccl_tolerates_junk_lines() -> None:
    """A truncated or blank line is skipped, not fatal — the bulk file is
    fetched unattended by a job."""
    assert parse_ccl("garbage\n\n|||", 2024) == []


def test_select_links_maps_committee_to_member_by_external_id() -> None:
    """The whole join is id → id: candidate id from the roster's
    fec.candidate alias, committee id from the recipient alias."""
    rows = parse_ccl(_SAMPLE, 2026)
    links, ambiguous = select_member_links(
        rows, {"S2TX00312": "member-cruz"}
    )
    assert ambiguous == []
    assert set(links) == {"C00492785"}
    link = links["C00492785"]
    assert link.member_canonical_id == "member-cruz"
    assert link.candidate_id == "S2TX00312"
    assert link.designation_label == "principal campaign committee"


def test_select_links_ignores_candidates_who_are_not_members() -> None:
    """A challenger's committee is in the file but resolves to no member
    canonical — no edge, no guess."""
    rows = parse_ccl(_SAMPLE, 2026)
    links, _ = select_member_links(rows, {})
    assert links == {}


def test_select_links_refuses_a_committee_shared_by_two_members() -> None:
    """A joint fundraising committee genuinely serving two sitting
    members must not be attributed to one of them. It is refused and
    reported for review."""
    rows = [
        CclRow("S2TX00312", "C00JOINT", "N", "J", 2024),
        CclRow("H0AL01055", "C00JOINT", "N", "J", 2024),
    ]
    links, ambiguous = select_member_links(
        rows, {"S2TX00312": "member-a", "H0AL01055": "member-b"}
    )
    assert links == {}
    assert len(ambiguous) == 1
    assert ambiguous[0]["committee_id"] == "C00JOINT"
    assert ambiguous[0]["member_canonical_ids"] == ["member-a", "member-b"]


def test_same_member_under_two_candidate_ids_is_not_ambiguous() -> None:
    """A member who ran for the House and then the Senate carries two
    FEC candidate ids on ONE canonical — that is not a conflict."""
    rows = [
        CclRow("H2TX00001", "C00SAME", "H", "P", 2018),
        CclRow("S2TX00312", "C00SAME", "S", "P", 2024),
    ]
    links, ambiguous = select_member_links(
        rows, {"H2TX00001": "member-a", "S2TX00312": "member-a"}
    )
    assert ambiguous == []
    assert links["C00SAME"].member_canonical_id == "member-a"
    assert links["C00SAME"].cycles == [2018, 2024]


def test_designation_ranking_prefers_the_principal_committee() -> None:
    """When a committee appears under several designations across
    cycles, the edge records the most-attributable one."""
    rows = [
        CclRow("S2TX00312", "C00X", "S", "J", 2020),
        CclRow("S2TX00312", "C00X", "S", "P", 2024),
    ]
    links, _ = select_member_links(rows, {"S2TX00312": "member-a"})
    assert links["C00X"].designation == "P"


def test_committee_namespaces_cover_the_contribution_targets() -> None:
    """contributes_to targets were created as fec.disbursement.recipient
    canonicals; PAC-mode ingest uses fec.committee. Both must be in the
    lookup or the money never joins."""
    assert "fec.disbursement.recipient" in COMMITTEE_NAMESPACES
    assert "fec.committee" in COMMITTEE_NAMESPACES


def test_default_cycles_reach_back_to_2016() -> None:
    """The corpus's oldest contributions are 2016-cycle."""
    assert min(DEFAULT_CYCLES) <= 2016
    assert max(DEFAULT_CYCLES) >= 2026
