"""Add ``llm_usage`` table — per-call LLM usage log (Atlas spend Part 1b).

Revision ID: 0003_llm_usage
Revises: 0002_scrutiny_and_surface_mode
Create Date: 2026-07-18

Argus side of Tony's per-app Anthropic spend build (helen 2026-07-18).
Same shape as hollywood_gen's 0067+0068 — cache-read/write split baked in
from the first migration since we're landing after helen's refinement.

Columns:
    * ``app`` — always ``argus`` from this codebase; kept for uniformity
      with the shared Atlas dashboard schema.
    * ``feature`` — caller-supplied short slug (``scrutiny.classify``,
      etc.) via the module-level ``feature_scope`` context manager.
    * ``model`` — SDK-resolved model name (``claude-sonnet-4-…``).
    * ``prompt_tokens`` / ``completion_tokens`` / ``cache_read_tokens`` /
      ``cache_write_tokens`` — from response.usage; split so Part 2
      pricing multiplies each by its own per-model rate.
    * ``call_ms`` — wall-clock milliseconds.
    * ``ok`` — False on API error; usage NULL on failed calls so the
      dashboard can chart error rate.
    * ``ts`` — server timestamp; indexed.

Pricing lives in Atlas so a rate change doesn't need an argus migration.
"""

from __future__ import annotations

import sqlalchemy as sa
from alembic import op

revision = "0003_llm_usage"
down_revision = "0002_scrutiny_and_surface_mode"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "llm_usage",
        sa.Column("id", sa.String(36), primary_key=True),
        sa.Column("app", sa.String(64), nullable=False),
        sa.Column("feature", sa.String(128), nullable=False),
        sa.Column("model", sa.String(128), nullable=False),
        sa.Column("prompt_tokens", sa.Integer, nullable=True),
        sa.Column("completion_tokens", sa.Integer, nullable=True),
        sa.Column("cache_read_tokens", sa.Integer, nullable=True),
        sa.Column("cache_write_tokens", sa.Integer, nullable=True),
        sa.Column("call_ms", sa.Integer, nullable=True),
        sa.Column("ok", sa.Boolean, nullable=False, server_default=sa.true()),
        sa.Column(
            "ts",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("ix_llm_usage_ts", "llm_usage", ["ts"])
    op.create_index("ix_llm_usage_app_feature_ts", "llm_usage", ["app", "feature", "ts"])
    op.create_index("ix_llm_usage_model_ts", "llm_usage", ["model", "ts"])


def downgrade() -> None:
    op.drop_index("ix_llm_usage_model_ts", table_name="llm_usage")
    op.drop_index("ix_llm_usage_app_feature_ts", table_name="llm_usage")
    op.drop_index("ix_llm_usage_ts", table_name="llm_usage")
    op.drop_table("llm_usage")
