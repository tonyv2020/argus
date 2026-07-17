"""Environment-driven settings for the Argus backend."""

from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Argus runtime configuration — populated from environment variables."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # --- app --------------------------------------------------------------
    public_base_url: str = "http://localhost:8000"

    # --- postgres (argus's own DB) ---------------------------------------
    database_url: str = "postgresql+psycopg://argus:argus@localhost:5432/argus"
    database_url_sync: str = "postgresql+psycopg://argus:argus@localhost:5432/argus"

    # --- hollywood postgres (READ-ONLY seed source; P0) ------------------
    hollywood_database_url: str = "postgresql+psycopg://hollywood:hollywood@postgres.hollywood.svc.cluster.local:5432/hollywood"

    # --- neo4j ------------------------------------------------------------
    neo4j_enabled: bool = True
    neo4j_uri: str = "bolt://neo4j:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "argus"

    # --- resolution parameters -------------------------------------------
    # Cosine similarity floor for a merge match — mirrors legal-lab (>=0.86).
    # Below this, keep entities separate.
    resolution_similarity_threshold: float = 0.86
    # Person-only conservative margin: a PERSON merge requires similarity
    # strictly above (threshold + this margin). Rationale: a false MERGE of
    # two real people is far worse than a false SPLIT.
    resolution_person_conservative_margin: float = 0.04
    # Top-K candidates to inspect per resolve call.
    resolution_top_k: int = 8

    # --- llm (scrutiny agent + P1 downstream) ----------------------------
    # Sonnet-floor per design §5. Scrutiny reasoning is auditable so this
    # model choice is deliberate and not a per-request override.
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-sonnet-4-6"


settings = Settings()
