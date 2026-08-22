"""P1.7 — Musk-network anchors, donor predicate, and the noise guards.

Hermetic: no HTTP, no DB.

The whole risk of this domain is that its anchors are named with short,
common English words — "Tesla", "Boring", "Starlink", and the single
letter "X". Every string below was measured against a live authority on
2026-08-22 (SEC EDGAR, USAspending, lda.gov, FEC Schedule A) and is a
real record for a DIFFERENT entity than the anchor it looks like. If a
change to the declarations makes one of these merge or match, that is
the P1.6 Thiel bug returning in a new costume.
"""

from __future__ import annotations

import re

import pytest

from app.models import EntityType, SurfaceMode
from app.services.ingest.domain_anchors import (
    DOMAIN_SPECS,
    MUSK_ANCHORS,
    SEC_OWNER_NAMESPACE,
    USASPENDING_UEI_NAMESPACE,
)
from app.services.ingest.domain_merge import (
    AnchorNode,
    Node,
    build_merge_plan,
    mergeable_types_for,
)
from app.services.ingest.fec_individual import (
    DONOR_IDENTITIES,
    MEMO_CODE,
    MUSK,
    check_identity,
)

MUSK_ANCHOR = next(a for a in MUSK_ANCHORS if a.label == "Elon Musk")
TESLA = next(a for a in MUSK_ANCHORS if a.label == "Tesla")
SPACEX = next(a for a in MUSK_ANCHORS if a.label == "SpaceX")
XAI = next(a for a in MUSK_ANCHORS if a.label == "xAI")
XCORP = next(a for a in MUSK_ANCHORS if a.label == "X Corp")
BORING = next(a for a in MUSK_ANCHORS if a.label == "The Boring Company")
NEURALINK = next(a for a in MUSK_ANCHORS if a.label == "Neuralink")
STARLINK = next(a for a in MUSK_ANCHORS if a.label == "Starlink")


# ─── the domain is registered and keyed on external ids ─────────────────


def test_musk_network_is_registered_as_a_domain() -> None:
    assert DOMAIN_SPECS["musk_network"] is MUSK_ANCHORS


def test_the_verified_external_ids_are_the_ones_declared() -> None:
    """Each id was read off the issuing authority on 2026-08-22. Pinning
    them here means a typo shows up as a test failure rather than as an
    anchor silently resolving onto the wrong company."""
    assert MUSK_ANCHOR.sec_owner_cik == 1494730          # "Musk Elon"
    assert TESLA.sec_cik == 1318605                       # TSLA
    assert SPACEX.sec_cik == 1181412                      # SpaceX
    assert NEURALINK.sec_cik == 1708503                   # Neuralink Corp.
    assert XAI.sec_cik == 2002695                         # X.AI CORP.
    assert 2079267 in XAI.sec_ciks                        # X.AI Holdings
    assert set(SPACEX.usaspending_uei) == {"C6M7C2FLKER5", "H5JUPMRB3KX6"}
    assert set(TESLA.usaspending_uei) == {"TBTHGLM2G9D3", "VU8VCVEXW3L4"}


def test_kimbal_musk_is_not_the_elon_musk_anchor() -> None:
    """CIK 1494731 is Elon's brother — a separate SEC reporting owner and
    a different real person."""
    assert MUSK_ANCHOR.sec_owner_cik != 1494731
    assert (SEC_OWNER_NAMESPACE, "0001494731") not in MUSK_ANCHOR.identity_keys


def test_the_older_unrelated_x_ai_company_is_not_declared() -> None:
    """``x.ai, inc.`` (CIK 1609052, Delaware) filed Form D from 2014 to
    2017 — years before Musk founded xAI. It is a different company."""
    assert 1609052 not in (XAI.sec_ciks + (XAI.sec_cik,))


@pytest.mark.parametrize("spv_cik", [2071284, 2075470, 2081339, 2082210])
def test_spv_feeder_fund_ciks_are_not_declared(spv_cik: int) -> None:
    """"NEURALINK … A SERIES OF … LLC" are SPVs that HOLD Neuralink
    stock — the same class of noise as FXAIX holding Tesla."""
    assert spv_cik not in (NEURALINK.sec_ciks + (NEURALINK.sec_cik,))


def test_twitters_cik_is_not_attached_to_x_corp() -> None:
    """SEC still calls CIK 1418091 "TWITTER, INC." and it stopped filing
    in 2022. Attaching it would pull 686 Twitter-era Form 4s from
    pre-Musk executives onto X Corp."""
    assert XCORP.sec_cik is None
    assert 1418091 not in XCORP.sec_ciks


# ─── the UEI gate: no fuzzy recipient names anywhere in this domain ─────


def test_no_anchor_declares_a_fuzzy_usaspending_recipient_name() -> None:
    """Measured on USAspending 2026-08-22, the name searches for this
    domain return other companies' money: "TESLA" → $79M of TESLA
    LABORATORIES / INDUSTRIES / GOVERNMENT / OFFSHORE, "X CORP" → RTX
    CORPORATION at $83.3B, "BORING COMPANY" → ALASKA ROAD BORING. So the
    UEI is both the query and the accept gate, and the name fallback is
    left empty for the whole domain."""
    for spec in MUSK_ANCHORS:
        assert spec.usaspending_recipient_names == (), spec.label


@pytest.mark.parametrize(
    "label",
    ["xAI", "X Corp", "The Boring Company", "Neuralink", "Starlink"],
)
def test_an_anchor_with_no_verified_uei_declares_none(label: str) -> None:
    """No SAM registration was found for these on 2026-08-22. Declaring a
    near-miss UEI would attribute another company's contracts; declaring
    none simply yields no contract edges."""
    spec = next(a for a in MUSK_ANCHORS if a.label == label)
    assert spec.usaspending_uei == ()


# ─── the name-key hazards ───────────────────────────────────────────────


@pytest.mark.parametrize(
    "client_name,norm,accepted",
    [
        # X Corp's eight real lda.gov client records.
        ("TWITTER INC.", "twitter", True),
        ("TWITTER, INC.", "twitter", True),
        ("X CORP. (FORMERLY TWITTER, INC.)", "x corp formerly twitter", True),
        ("X, INC. F/K/A TWITTER, INC.", "x inc f k a twitter", True),
        ("TWINLOGIC STRATEGIES ON BEHALF OF TWITTER, INC",
         "twinlogic strategies on behalf of twitter", True),
        # The neighbours the first live run refused, correctly, and must
        # keep refusing: 143 XEROX CORPORATION filings alone.
        ("XEROX CORPORATION", "xerox", False),
        ("XEROX CORP", "xerox", False),
        ("XCEL ENERGY CORP", "xcel energy", False),
        ("XOMA CORPORATION", "xoma", False),
        ("TECH-X CORPORATION", "tech x", False),
        ("XSOC CORPORATION", "xsoc", False),
        ("XINERGY CORP", "xinergy", False),
        ("CONSTELLATION XXL CORPORATION", "constellation xxl", False),
    ],
)
def test_x_corp_lda_patterns(client_name: str, norm: str, accepted: bool) -> None:
    """The first live lobbying run refused 79 genuine X Corp filings
    because LDA registers the company as "X CORP. (FORMERLY TWITTER,
    INC.)". Fixing that must not open the door to the 143 XEROX filings
    sitting next to them in the same search."""
    from app.services.ingest.senate_lda import client_name_accepted

    assert client_name_accepted(norm, XCORP.lda_client_patterns) is accepted


def test_x_corp_declares_no_name_variants() -> None:
    """Every legal form of "X Corp" normalizes to the single token "x",
    and the live graph holds four unrelated nodes on that key —
    X [concept], X [organization], X [place], X [unknown]. Since
    ``concept`` and ``unknown`` are both org-mergeable, ONE declared
    variant would swallow two unrelated nodes into a company."""
    assert XCORP.name_variants == ()
    assert EntityType.CONCEPT.value in mergeable_types_for(
        EntityType.ORGANIZATION.value
    )


def test_a_single_letter_name_is_not_identity_evidence() -> None:
    """THE P1.7 REGRESSION. The anchor pass tries ``spec.label`` before
    the declared variants, so an anchor cannot opt out of name matching
    by declaring none — and "X Corp" normalizes to "x". On the first
    live run this resolved the X Corp anchor onto a `concept` node named
    "X". The fix reuses ``dedup_pass._name_is_evidence``, the same guard
    the dedup pass and the fragment merge already key on."""
    from app.services.graph.base import normalize_name
    from app.services.ingest.dedup_pass import _name_is_evidence

    assert normalize_name("X Corp") == "x"
    assert normalize_name("X Corp.") == "x"
    assert normalize_name("X Corporation") == "x"
    assert not _name_is_evidence("x")
    # The anchors whose labels ARE distinctive keep working.
    for spec in MUSK_ANCHORS:
        if spec.label == "X Corp":
            continue
        assert _name_is_evidence(normalize_name(spec.label)), spec.label


@pytest.mark.parametrize("label", ["X Corp", "America PAC"])
def test_an_anchor_whose_label_collapses_declares_it(label: str) -> None:
    """THE THIRD P1.7 REGRESSION. domain_merge always adds the anchor's
    OWN normalized name as a merge key, so declaring no name_variants is
    not enough. The normalizer strips trailing legal suffixes, and for
    these two the suffix is the part carrying the meaning:

        "X Corp"      -> "x"        (4 unrelated live nodes)
        "America PAC" -> "america"  (a news-tag node about the COUNTRY,
                                     typed unknown, mergeable into a pac)

    The first merge dry-run planned America -> AMERICA PAC on exactly
    this. Both anchors now declare name_is_evidence=False, which empties
    their variant set in the merge and skips the resolver's name
    fallback."""
    from app.services.graph.base import normalize_name

    spec = next(a for a in MUSK_ANCHORS if a.label == label)
    assert spec.name_is_evidence is False
    assert spec.name_variants == ()
    # The flag has to survive the round-trip into anchor_registry.
    assert spec.external_ids.get("name_is_evidence") is False
    # And the collapse it guards against is real.
    assert len(normalize_name(label).split()) < len(label.split())


def test_every_other_musk_anchor_keeps_its_name_as_evidence() -> None:
    """The opt-out is for the two colliding labels only — it must not
    quietly disable name resolution for anchors that depend on it
    (Starlink and The Boring Company have no external id at all)."""
    optouts = {a.label for a in MUSK_ANCHORS if not a.name_is_evidence}
    assert optouts == {"X Corp", "America PAC"}
    for label in ("Starlink", "The Boring Company"):
        spec = next(a for a in MUSK_ANCHORS if a.label == label)
        assert spec.name_is_evidence is True
        assert spec.name_variants


def test_a_non_evidence_anchor_contributes_no_merge_variants() -> None:
    """The merge reads the flag off anchor_registry.external_ids."""
    assert XCORP.external_ids["name_is_evidence"] is False
    pac = next(a for a in MUSK_ANCHORS if a.label == "America PAC")
    assert pac.external_ids["name_is_evidence"] is False
    # Anchors that ARE name-evidence must not carry the key at all,
    # so existing domains keep their current behaviour unchanged.
    assert "name_is_evidence" not in TESLA.external_ids
    assert "name_is_evidence" not in SPACEX.external_ids


def test_declared_keyring_never_claims_the_discovered_lda_keys() -> None:
    """``upsert_anchor`` overwrites the JSONB keyring, but lda_client_ids
    and lda_registrant_ids are DISCOVERED by senate_lda and written back
    as the audit record of what its patterns accepted. P1.6.3's "an LDA
    client id the anchor OWNS is not foreign" rule reads that list, so
    erasing it makes the fragment merge refuse the anchor's own LDA
    nodes -- which is what a mid-phase anchors re-run did on P1.7.

    No musk_network spec declares those keys, so the anchor pass must be
    carrying them over rather than overwriting."""
    for spec in MUSK_ANCHORS:
        assert "lda_client_ids" not in spec.external_ids, spec.label
        assert "lda_registrant_ids" not in spec.external_ids, spec.label


def test_america_pac_is_keyed_on_the_committee_id_that_exists() -> None:
    """The brief's C00871644 404s on the FEC registry. C00879510 is the
    real AMERICA PAC (Super PAC, Austin TX, first filed 2024-05-22) and
    is already the committee on Musk's live Schedule A receipts."""
    pac = next(a for a in MUSK_ANCHORS if a.label == "America PAC")
    assert pac.entity_type == EntityType.PAC.value
    assert pac.fec_committee_ids == ("C00879510",)
    assert ("fec.committee", "C00879510") in pac.identity_keys
    # Nine live PACs end in "AMERICA PAC"; a name key would claim them.
    assert pac.name_variants == ()


def test_every_musk_anchor_that_can_be_keyed_on_an_id_is() -> None:
    """Only the three entities with no external id anywhere — X Corp,
    The Boring Company, Starlink — may fall through to a name."""
    keyless = {s.label for s in MUSK_ANCHORS if not s.identity_keys}
    assert keyless == {"X Corp", "The Boring Company", "Starlink"}
    # Of those three, only the two with distinctive names may resolve by
    # name; X Corp has neither an id nor a usable name and is created
    # fresh + staged.
    assert not next(
        a for a in MUSK_ANCHORS if a.label == "X Corp"
    ).name_is_evidence


def test_both_boring_spellings_are_declared() -> None:
    """The normalizer keeps a leading "the", so "The Boring Company" and
    "Boring Company" land on different keys ("the boring" vs "boring")
    and would never meet unless both are declared."""
    assert set(BORING.name_variants) == {"The Boring Company", "Boring Company"}


# ─── fragment merge: what must collapse ─────────────────────────────────


def _node(nid, name, norm, **kw) -> Node:
    base = {
        "type": EntityType.ORGANIZATION.value,
        "surface_mode": SurfaceMode.OPEN.value,
        "publication_state": "published",
        "edge_count": 1,
        "alias_count": 0,
        "namespaces": frozenset(),
        "lda_client_ids": frozenset(),
    }
    base.update(kw)
    return Node(id=nid, name=name, norm=norm, **base)


def _anchor(spec, nid, norm, **kw) -> AnchorNode:
    from app.services.graph.base import normalize_name

    return AnchorNode(
        label=spec.label,
        node=_node(nid, spec.label, norm, **kw),
        entity_type=spec.entity_type,
        variants=frozenset(normalize_name(v) for v in spec.name_variants),
        client_patterns=spec.lda_client_patterns,
        lda_client_ids=frozenset(),
    )


def _plan(anchors, frags):
    nodes = {a.node.id: a.node for a in anchors}
    nodes.update({f.id: f for f in frags})
    return build_merge_plan(anchors, nodes)


def test_collapses_the_real_spacex_fragments() -> None:
    """The four LDA/news surface forms live in the graph today, each
    holding cited edges that never reach SpaceX's profile."""
    sx = _anchor(SPACEX, "sx", "spacex", namespaces=frozenset({"sec.cik"}))
    frags = [
        _node("f1", "SPACE EXPLORATION TECHNOLOGIES CORP. (SPACEX)",
              "space exploration technologies corp spacex"),
        _node("f2", "SPACE EXPLORATION TECHNOLOGIES (SPACEX)",
              "space exploration technologies spacex"),
        _node("f3", "SPACEX (AKA SPACE EXPLORATION TECHNOLOGIES CORP.)",
              "spacex aka space exploration technologies"),
        _node("f4", "SPACE EXPLORATION TECHNOLOGIES CORP",
              "space exploration technologies"),
    ]
    plan = _plan([sx], frags)
    assert {p.fragment_id for p in plan.pairs} == {"f1", "f2", "f3", "f4"}


def test_collapses_the_duplicate_boring_company_node() -> None:
    boring = _anchor(BORING, "bore", "the boring")
    frags = [_node("f1", "Boring Company", "boring", edge_count=0)]
    plan = _plan([boring], frags)
    assert {p.fragment_id for p in plan.pairs} == {"f1"}


# ─── fragment merge: the noise the brief names ──────────────────────────


@pytest.mark.parametrize(
    "name,norm",
    [
        # The two the brief calls out by name.
        ("FXAIX", "fxaix"),                       # Fidelity 500 index fund
        ("TD SYNNEX CORP", "td synnex"),          # IT distributor
        # Everything else that shares a substring with an anchor.
        ("RTX CORPORATION AND AFFILIATES", "rtx corporation and affiliates"),
        ("CXAI", "cxai"),
        ("SpaceXAI", "spacexai"),
        ("Octagon Xai Clo Income Fund", "octagon xai clo income fund"),
        ("XAI Floating Rate & Alternative Income Trust",
         "xai floating rate alternative income trust"),
        ("Grok (xAI)", "grok xai"),               # the PRODUCT, not the company
        ("Teslarati", "teslarati"),               # a news site
        ("Tesla.com", "tesla com"),
        ("notateslaapp.com", "notateslaapp com"),
        ("meta Amazon Tesla", "meta amazon tesla"),
        ("Reddit, Tesla Investors Club", "reddit tesla investors club"),
        ("SpaceXLounge", "spacexlounge"),         # a subreddit
        ("Leveraged SpaceX ETFs", "leveraged spacex etfs"),
        ("Non-SpaceX", "non spacex"),
        ("Facebook/SpaceXFP", "facebook spacexfp"),
        ("ELON MUSK REVOCABLE TRUST", "elon musk revocable trust"),
        ("MUSKEGON MICH PUB", "muskegon mich pub"),
        ("Muskogee", "muskogee"),
        ("TESLA LABORATORIES", "tesla laboratories"),
        ("STARLINK TECHNOLOGIES LLC", "starlink technologies"),
        ("ALASKA ROAD BORING COMPANY", "alaska road boring"),
        ("ATLANTIC BORING COMPANY", "atlantic boring"),
    ],
)
def test_noise_never_merges_into_any_musk_anchor(name: str, norm: str) -> None:
    """None of these is an anchor. FXAIX and TD SYNNEX *hold* or *resell*
    Tesla; the rest merely share a substring. Planning them against EVERY
    Musk anchor at once is the real-world condition — a fragment claimed
    by two anchors is refused, but a fragment claimed by ONE would merge."""
    anchors = [
        _anchor(SPACEX, "sx", "spacex"),
        _anchor(TESLA, "tsl", "tesla"),
        _anchor(XAI, "xai", "xai"),
        _anchor(XCORP, "xc", "x corp placeholder"),
        _anchor(BORING, "bore", "the boring"),
        _anchor(NEURALINK, "nl", "neuralink"),
        _anchor(STARLINK, "sl", "starlink"),
    ]
    plan = _plan(anchors, [_node("noise", name, norm)])
    assert not [p for p in plan.pairs if p.fragment_id == "noise"], (
        f"{name!r} was planned for a merge"
    )


def test_the_x_nodes_are_never_claimed() -> None:
    """The four live nodes on the key "x". This is the test that fails
    if someone "helpfully" adds a name variant to the X Corp anchor."""
    anchors = [_anchor(XCORP, "xc", "x corp placeholder")]
    frags = [
        _node("x1", "X", "x", type=EntityType.CONCEPT.value),
        _node("x2", "X", "x", type=EntityType.ORGANIZATION.value),
        _node("x3", "X", "x", type=EntityType.PLACE.value),
        _node("x4", "X", "x", type=EntityType.UNKNOWN.value),
    ]
    plan = _plan(anchors, frags)
    assert plan.pairs == []


def test_the_spacex_corporate_pac_is_not_the_company() -> None:
    """"SPACE EXPLORATION TECHNOLOGIES CORP. PAC" normalizes to the same
    key as the company, because the normalizer strips a trailing "pac"
    suffix. It is a separate entity, and ``pac`` is not an org-mergeable
    type — this asserts that guard actually holds."""
    sx = _anchor(SPACEX, "sx", "spacex")
    pac = _node("pac1", "SPACE EXPLORATION TECHNOLOGIES CORP. PAC",
                "space exploration technologies", type=EntityType.PAC.value)
    plan = _plan([sx], [pac])
    assert plan.pairs == []


def test_the_suppressed_tesla_person_node_is_refused() -> None:
    """Pre-P2 the graph typed a "Tesla" node as a suppressed person. It
    straddles both surface_mode and type, and consolidating a protected
    identity is an operator's call, not this pass's."""
    tsl = _anchor(TESLA, "tsl", "tesla", namespaces=frozenset({"sec.cik"}))
    person = _node("p1", "Tesla", "tesla", type=EntityType.PERSON.value,
                   surface_mode=SurfaceMode.SUPPRESS.value)
    plan = _plan([tsl], [person])
    assert plan.pairs == []


# ─── the donor predicate ────────────────────────────────────────────────


def _row(**kw) -> dict:
    base = {
        "contributor_name": "MUSK, ELON",
        "contributor_city": "AUSTIN",
        "contributor_state": "TX",
        "contributor_employer": "SPACE EXPLORATION TECHNOLOGIES CORP.",
        "contributor_occupation": "CEO",
        "contribution_receipt_amount": 1000.0,
        "sub_id": "1",
    }
    base.update(kw)
    return base


def test_musk_is_a_declared_donor() -> None:
    assert DONOR_IDENTITIES["musk"] is MUSK


@pytest.mark.parametrize(
    "name,employer,state",
    [
        # The verbatim shape on his America PAC receipts (FEC, 2026-08-22).
        ("MUSK, ELON", "SPACE EXPLORATION TECHNOLOGIES CORP.", "TX"),
        ("MUSK, ELON", "TESLA", "TX"),
        ("MUSK, ELON", "TESLA, INC.", "CA"),
        ("MUSK, ELON", "SPACEX", "CA"),
        ("MUSK, ELON", "X CORP", "TX"),
        ("MUSK, ELON", "X.AI CORP", "TX"),
        ("MUSK, ELON", "THE BORING COMPANY", "TX"),
        ("MUSK, ELON", "NEURALINK", "CA"),
        ("MUSK, ELON MR.", "TESLA", "TX"),
        ("MUSK, ELON R", "TESLA", "TX"),
        ("MUSK, ELON REEVE", "TESLA", "TX"),
        ("MUSK, ELON", "PAYPAL", "CA"),
    ],
)
def test_accepts_the_real_donor(name: str, employer: str, state: str) -> None:
    check = check_identity(
        _row(contributor_name=name, contributor_employer=employer,
             contributor_state=state), MUSK
    )
    assert check.accepted, check.reason


@pytest.mark.parametrize(
    "name,reason",
    [
        ("MUSKAT, DAVID", "surname_mismatch"),      # real SEC filer
        ("MUSKET, DAVID", "surname_mismatch"),      # real SEC filer
        ("MUSKE, ELON", "surname_mismatch"),
        ("MUSK, KIMBAL", "given_name_mismatch"),    # his brother
        ("MUSK, JUSTINE", "given_name_mismatch"),
        ("MUSK, ERROL", "given_name_mismatch"),
        ("MUSK, MAYE", "given_name_mismatch"),
        ("MUSKEGON, ELON", "surname_mismatch"),
    ],
)
def test_refuses_a_different_person(name: str, reason: str) -> None:
    """The Thiel bug in its exact form: a fuzzy ``contributor_name``
    search returns other real, private people and the old emitter
    attributed their giving to the billionaire."""
    check = check_identity(_row(contributor_name=name), MUSK)
    assert not check.accepted
    assert check.reason == reason


@pytest.mark.parametrize(
    "employer,occupation",
    [
        ("SELF-EMPLOYED", "INVESTOR"),
        ("SELF", "CEO"),
        ("NONE", "RETIRED"),
        ("N/A", "ENGINEER"),
        ("GENERAL MOTORS", "FACTORY WORKER"),
        ("RTX CORPORATION", "ENGINEER"),
        ("GS CALTEX CORPORATION", "MANAGER"),
        ("SAALEX CORP", "ANALYST"),
        ("SCIOLEX CORPORATION", "STAFF"),
        ("TD SYNNEX CORP", "SALES"),
        ("TESLA LABORATORIES INC", "CONSULTANT"),
    ],
)
def test_refuses_an_uncorroborated_or_neighbouring_employer(
    employer: str, occupation: str
) -> None:
    """A person genuinely named Elon Musk who is not THE Elon Musk.
    "SELF"/"INVESTOR"/"CEO" are deliberately undeclared: matching them
    would accept any such row. The CORP neighbours check that the
    word-boundary anchors hold."""
    check = check_identity(
        _row(contributor_employer=employer, contributor_occupation=occupation),
        MUSK,
    )
    assert not check.accepted
    assert check.reason.startswith("no_affiliation_match")


def test_memo_rows_are_the_same_dollars_twice() -> None:
    """FEC itemizes an earmarked contribution against both the conduit
    and the ultimate recipient. Musk's live America PAC rows include
    memo-X entries of $40,495,178 and $11,215,136 — summing them would
    inflate his total by ~$52M on that one committee alone."""
    row = _row(memo_code=MEMO_CODE, contribution_receipt_amount=40495178.0)
    assert check_identity(row, MUSK).accepted
    assert (row.get("memo_code") or "").strip().upper() == MEMO_CODE


# ─── the staged-run read-gate ───────────────────────────────────────────


def test_the_emitters_gate_published_edges_behind_batch_id() -> None:
    """THE SECOND P1.7 REGRESSION, and the more serious one.

    ``_emit_contribution`` stages the edges it CREATES, but the
    reuse-an-existing-edge branch was not gated at all: it incremented
    the weight and added a citation regardless of whether that edge was
    already published. So P1.7's "staged" Musk sweep added $30,342,500
    of weight and 41 citations to 74 LIVE published edges, and 224 more
    to the published affiliated_with edges. Staged work reaching the
    public read path is the same defect class P1.5 found in the Neo4j
    projection.

    Asserted on the source because exercising it needs a live session;
    the behaviour itself is verified against the database in the P1.7
    report. Both emitters must consult publication_state before writing.
    """
    import inspect

    from app.services.ingest import fec_individual as fi

    for fn in (fi._emit_contribution, fi._emit_employer_affiliation):
        src = inspect.getsource(fn)
        reuse = src.split("stats.edges_reused += 1", 1)[-1] if (
            fn is fi._emit_contribution
        ) else src.split("stats.affiliation_edges_reused += 1", 1)[-1]
        assert "PublicationState.PUBLISHED.value" in reuse, fn.__name__
        assert "batch_id" in reuse, fn.__name__


def test_every_staged_ingester_gates_published_edges() -> None:
    """THE SYSTEMIC VERSION. All four ingesters that P1.7 runs share one
    shape: create-new is staged behind batch_id, reuse-existing was not
    gated at all. Two of them were measured firing on live data:

        fec_individual  +$30,342,500 and 265 citations on 76 published
                        Musk edges
        senate_lda      46 citations and +46 weight on 6 published Tesla
                        `lobbies` edges

    sec_insiders and usaspending did not fire only because every edge
    they touched happened to be new. usaspending's weights are DOLLARS
    and it is the emitter family behind the $89B GEO artefact, so it is
    gated too.

    Asserted on the source: exercising the branch needs a live session,
    and the behaviour is verified against the database in the report."""
    import inspect

    from app.services.ingest import fec_individual, sec_insiders
    from app.services.ingest import senate_lda, usaspending

    cases = [
        (fec_individual._emit_contribution, "stats.edges_reused += 1"),
        (fec_individual._emit_employer_affiliation,
         "stats.affiliation_edges_reused += 1"),
        (sec_insiders._emit_position_edge, "stats.edges_reused += 1"),
        (senate_lda._emit_lobbies_edge, "reused = True"),
        (usaspending._emit_contract_edge, "reused = True"),
    ]
    for fn, marker in cases:
        src = inspect.getsource(fn)
        assert marker in src, f"{fn.__name__}: reuse marker moved"
        after = src.split(marker, 1)[1]
        assert "PublicationState.PUBLISHED.value" in after, fn.__name__
        assert "batch_id" in after, fn.__name__


def test_a_truncated_sweep_is_never_reported_as_complete() -> None:
    """Without a key the FEC API allows ~30 requests an hour and this
    sweep is 10 periods deep. The old loop broke out of a period on the
    first 429 and reported the partial total as the donor's giving."""
    from app.services.ingest.fec_individual import IndividualContribStats

    st = IndividualContribStats(donor="Elon Musk")
    assert st.sweep_complete
    st.periods_incomplete.append({"query": "MUSK, ELON", "period": 2024,
                                  "rows_before_failure": 40})
    assert not st.sweep_complete


def test_widening_the_search_query_cannot_widen_what_is_accepted() -> None:
    """``search_queries`` only decides what is FETCHED; the predicate
    decides what is kept. This is the invariant that lets an operator
    widen the net without risking misattribution."""
    assert MUSK.queries == ("MUSK, ELON",)
    assert not check_identity(
        _row(contributor_name="MUSK, KIMBAL",
             contributor_employer="TESLA"), MUSK
    ).accepted
