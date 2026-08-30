"""PART B data-guard — is this aircraft owned by a PERSON?

The publish path originally answered that from the FAA's
``TYPE REGISTRANT`` code. That is the wrong source of truth in both
directions:

  * **False individuals.** The FAA file miscodes companies as
    individuals — ``UNITED AIRLINES INC``, ``SOUTHWEST AIRLINES CO``,
    ``MOTOROLA SOLUTIONS INC``, ``AIR PRODUCTS AND CHEMICALS INC`` and
    others are filed under ``TYPE REGISTRANT`` 1/4. Gating on the FAA
    code wrongly drops them from publication.
  * **False organisations.** Argus itself carries some real people as
    ``type='organization'`` — the five FEC disbursement recipients
    below. Gating on the ARGUS type alone would wrongly PUBLISH them.

So identity resolves from the **Argus canonical**, which is the thing
making the claim about who owns the aircraft — with an explicit
override list for canonicals Argus is known to have mistyped. The
override is what makes the guard safe to enable BEFORE those rows are
retyped; without it, switching from the FAA code to the Argus type
would newly expose 8 aircraft belonging to real individuals.
"""

from __future__ import annotations

#: Argus canonical types that are definitively NOT a natural person.
#: Expressed as an ALLOW-list rather than a deny-list of person types:
#: ``EntityType.UNKNOWN`` is a real value, and a deny-list would let it
#: through as an organisation. Anything not named here — unknown, or a
#: type added later — is treated as a person and therefore withheld.
NON_PERSON_TYPES = frozenset(
    {
        "organization",
        "agency",
        "pac",
        "contract",
        "lobbying_reg",
        "place",
        "topic",
        "event",
        "concept",
        "bill",
    }
)

#: Canonicals that ARE real people but are stored as ``organization``.
#: All five are FEC disbursement recipients — individuals who received
#: a PAC disbursement — mistyped by the FEC ingest. Treated as people
#: here regardless of their stored type, so the data-guard cannot
#: surface them while the retype is still pending Tony's approval.
MISTYPED_PERSON_CANONICALS = frozenset(
    {
        "BROOKS, WILLIAM",
        "MURPHY, MICHAEL",
        "LEWIS, DAVID S.",
        "WILSON, DAVID A",
        "ROBINSON, MICHAEL",
    }
)


def is_individual_entity(canonical_type: str | None, canonical_name: str | None) -> bool:
    """True when the resolved canonical denotes a natural person.

    Fail-closed: an unknown or missing type is treated as a person, so a
    row Argus cannot classify is withheld rather than published.
    """
    if canonical_name in MISTYPED_PERSON_CANONICALS:
        return True
    if not canonical_type:
        return True
    return canonical_type not in NON_PERSON_TYPES
