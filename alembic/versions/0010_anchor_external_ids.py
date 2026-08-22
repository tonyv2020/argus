"""P1.6 — ``anchor_registry.external_ids``: the external-ID keyring.

Revision ID: 0010_anchor_external_ids
Revises: 0009_read_gate_publication_state
Create Date: 2026-08-21

P1.6 anchors the surveillance / tech-influence domain (Thiel, Palantir,
Founders Fund, Flock, Clearview, Axon) **on external ids, never names**.
The existing typed columns cover three of the four sources the brief
names — ``sec_cik``, ``fec_committee_ids``, ``fec_candidate_ids`` — but
the other two only had NAME columns:

  * USAspending's real key is the recipient **UEI** (12-char alphanumeric)
    — ``usaspending_recipient_names`` is a fuzzy search string.
  * Senate LDA's real key is the numeric **client id** / **registrant id**
    — ``lda_client_names`` is a server-side substring match that returns
    "GEOTHERMAL TAX GROUP" for a query of "The GEO Group".

Rather than grow one typed column per future source, this adds ONE JSONB
keyring so a new source is a data edit:

    external_ids = {
      "usaspending_uei":     ["FSY4LVSBGWB7", "HNN4F9JZWDY8"],
      "lda_client_ids":      [12345],
      "lda_registrant_ids":  [678],
      "sec_ciks":            [1321655],          # secondary CIKs
      "sec_owner_cik":       1211060             # Form 3/4/5 reporting owner
    }

Additive + defaulted, so every existing row keeps working unchanged and
the read path is untouched. A GIN index keeps "which anchor owns this
UEI?" a single index probe.
"""

from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

from alembic import op

revision = "0010_anchor_external_ids"
down_revision = "0009_read_gate_publication_state"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "anchor_registry",
        sa.Column(
            "external_ids",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
            server_default=sa.text("'{}'::jsonb"),
        ),
    )
    op.create_index(
        "ix_anchor_registry_external_ids",
        "anchor_registry",
        ["external_ids"],
        postgresql_using="gin",
    )


def downgrade() -> None:
    op.drop_index("ix_anchor_registry_external_ids", table_name="anchor_registry")
    op.drop_column("anchor_registry", "external_ids")
