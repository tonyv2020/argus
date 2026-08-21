"""P1.6.1 — domain-anchor declaration + fail-closed resolution tests.

Hermetic: no HTTP, no DB. Covers the two things that decide whether an
anchor lands on the right node — the external-ID keyring the anchor
declares, and the fail-closed guard on the name fallback.
"""

from __future__ import annotations

import pytest

from app.models import EntityType, SurfaceMode
from app.services.anchor_registry import Anchor
from app.services.ingest.domain_anchors import (
    AUTHORITATIVE_NAMESPACES,
    DOMAIN_SPECS,
    SEC_OWNER_NAMESPACE,
    SURVEILLANCE_ANCHORS,
    USASPENDING_UEI_NAMESPACE,
    AnchorSpec,
    NameMatchCandidate,
    anchor_name_match_allowed,
    variant_alias_source_id,
)

PALANTIR = next(a for a in SURVEILLANCE_ANCHORS if a.label == "Palantir Technologies")
THIEL = next(a for a in SURVEILLANCE_ANCHORS if a.label == "Peter Thiel")


def test_every_surveillance_anchor_declares_an_external_id() -> None:
    """The brief's non-negotiable: an anchor is keyed on an external id,
    never on a name. An anchor with no identity key would silently fall
    through to the name fallback on every run."""
    for spec in SURVEILLANCE_ANCHORS:
        assert spec.identity_keys, f"{spec.label} declares no external id"


def test_identity_keys_are_ordered_most_authoritative_first() -> None:
    """SEC CIKs are assigned by a federal registrar and never recycled;
    an LDA client id is one of several a company may register. The
    resolution order has to reflect that."""
    keys = PALANTIR.identity_keys
    assert keys[0] == ("sec.cik", "0001321655")
    assert (USASPENDING_UEI_NAMESPACE, "FSY4LVSBGWB7") in keys


def test_person_anchor_uses_the_owner_cik_namespace() -> None:
    """A Form 3/4/5 reporting owner is a PERSON. Keying Thiel under
    ``sec.cik`` (the issuer namespace) would retype him as an
    organization in the dedup pass, which pins type from the namespace."""
    assert THIEL.entity_type == EntityType.PERSON.value
    assert THIEL.identity_keys[0] == (SEC_OWNER_NAMESPACE, "0001211060")
    assert not any(ns == "sec.cik" for ns, _ in THIEL.identity_keys)


def test_secondary_ciks_are_included_in_identity_keys() -> None:
    """Founders Fund has no single CIK — EDGAR registers one per fund
    vintage — so the anchor declares the management-company CIKs and all
    of them must be usable as identity keys."""
    ff = next(a for a in SURVEILLANCE_ANCHORS if a.label == "Founders Fund")
    ciks = [sid for ns, sid in ff.identity_keys if ns == "sec.cik"]
    assert len(ciks) >= 3
    assert all(len(c) == 10 for c in ciks), ciks


def test_uei_is_normalized_to_upper_12_char() -> None:
    """USAspending's ``Recipient UEI`` field is uppercase; a lowercase
    declaration must not produce a key that never matches."""
    spec = AnchorSpec(
        label="X", entity_type="organization", priority_domain="d",
        usaspending_uei=(" fsy4lvsbgwb7 ",),
    )
    assert (USASPENDING_UEI_NAMESPACE, "FSY4LVSBGWB7") in spec.identity_keys


def test_external_ids_round_trip_through_the_anchor_read_model() -> None:
    """The keyring the spec writes must be readable back by the typed
    accessors the ingesters use."""
    anchor = Anchor(
        label=PALANTIR.label,
        entity_type=PALANTIR.entity_type,
        sec_cik=PALANTIR.sec_cik,
        external_ids=PALANTIR.external_ids,
    )
    assert anchor.usaspending_uei == ["FSY4LVSBGWB7", "HNN4F9JZWDY8"]
    assert anchor.sec_ciks[0] == 1321655


def test_anchor_read_model_drops_malformed_ids() -> None:
    """A typo in the keyring must DROP the id, not coerce it — an
    ingest filter built from a bad id selects the wrong entity."""
    anchor = Anchor(
        label="X", entity_type="organization",
        external_ids={
            "usaspending_uei": ["TOOSHORT", "FSY4LVSBGWB7", None],
            "lda_client_ids": ["12345", "not-a-number", 67],
        },
    )
    assert anchor.usaspending_uei == ["FSY4LVSBGWB7"]
    assert anchor.lda_client_ids == [12345, 67]


# ─── the fail-closed name fallback ──────────────────────────────────────


def _cand(**kw) -> NameMatchCandidate:
    base = {
        "id": "c1",
        "name": "Palantir Technologies Inc.",
        "type": EntityType.ORGANIZATION.value,
        "surface_mode": SurfaceMode.OPEN.value,
        "publication_state": "published",
        "namespaces": frozenset(),
    }
    base.update(kw)
    return NameMatchCandidate(**base)


def test_name_match_allows_a_clean_open_unclaimed_node() -> None:
    ok, reason = anchor_name_match_allowed(_cand(), PALANTIR)
    assert ok, reason


@pytest.mark.parametrize(
    "mode", [SurfaceMode.SUPPRESS.value, SurfaceMode.ALIAS.value]
)
def test_name_match_refuses_a_protected_node(mode: str) -> None:
    """THE non-negotiable rule. Attaching an anchor identity to a
    protected node either mislabels it or relaxes its protection on the
    way to publication — there is no flag to override this."""
    ok, reason = anchor_name_match_allowed(_cand(surface_mode=mode), PALANTIR)
    assert not ok
    assert reason == "surface_mode_not_open"


def test_name_match_refuses_a_node_carrying_a_foreign_external_id() -> None:
    """The anchor's OWN ids were already tried in the id-keyed step, so
    any authoritative id still on a name-matched node names a different
    real entity."""
    ok, reason = anchor_name_match_allowed(
        _cand(namespaces=frozenset({"fec.candidate"})), PALANTIR
    )
    assert not ok
    assert reason.startswith("foreign_external_id")


def test_name_match_accepts_unknown_and_concept_for_an_org_anchor() -> None:
    """32% of the registry is typed ``unknown`` and the news-tag pipeline
    routinely types a company as a ``concept`` (the documented
    organization/concept mis-typing P2 already merges). Both are exactly
    the fragments an org anchor should claim."""
    for t in (EntityType.UNKNOWN.value, EntityType.CONCEPT.value):
        ok, reason = anchor_name_match_allowed(_cand(type=t), PALANTIR)
        assert ok, (t, reason)


def test_name_match_refuses_a_person_node_for_an_org_anchor() -> None:
    ok, reason = anchor_name_match_allowed(
        _cand(type=EntityType.PERSON.value), PALANTIR
    )
    assert not ok
    assert reason.startswith("type_mismatch")


def test_name_match_refuses_concept_for_a_person_anchor() -> None:
    """The concept escape hatch is org-only: a person is never a concept."""
    ok, reason = anchor_name_match_allowed(
        _cand(type=EntityType.CONCEPT.value, name="Peter Thiel"), THIEL
    )
    assert not ok


def test_variant_alias_source_id_is_stable_and_namespaced() -> None:
    """``(source_system, source_id)`` is UNIQUE, so the key must be
    deterministic across re-runs AND namespaced by anchor — two anchors
    can share a variant."""
    a = variant_alias_source_id(PALANTIR, "Palantir")
    assert a == variant_alias_source_id(PALANTIR, "Palantir")
    assert a != variant_alias_source_id(THIEL, "Palantir")
    assert len(a) <= 64


def test_authoritative_namespaces_cover_the_new_p16_ids() -> None:
    """A node carrying a UEI or an owner CIK must be treated as claimed
    by the name fallback, exactly like a CIK or an FEC id."""
    assert SEC_OWNER_NAMESPACE in AUTHORITATIVE_NAMESPACES
    assert USASPENDING_UEI_NAMESPACE in AUTHORITATIVE_NAMESPACES


def test_surveillance_is_a_registered_domain() -> None:
    assert DOMAIN_SPECS["surveillance"] is SURVEILLANCE_ANCHORS
