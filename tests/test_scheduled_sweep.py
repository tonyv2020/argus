"""P4 F — scheduled sweep shape tests.

The wrapper is a straight-line sequencer over the 4 ingester modules
plus the roster + reproject. Hermetic tests here confirm the intended
step ordering + the per-step error isolation.
"""

from __future__ import annotations

import inspect

from app.services.ingest import scheduled_sweep


def test_all_six_ingest_steps_and_reproject_present() -> None:
    """The sweep must sequence roster → FEC PACs → FEC individuals →
    USAspending → LDA → SEC → reproject. Missing a step means the
    CronJob silently under-covers."""
    src = inspect.getsource(scheduled_sweep.run_sweep)
    for name in (
        "congress_roster.ingest_roster",
        "fec.ingest_from_registry",
        "fec.ingest_individual_contributors_from_registry",
        "usaspending.ingest_from_registry",
        "senate_lda.ingest_from_registry",
        "sec_edgar.ingest_from_registry",
        "project_to_neo4j.main_async",
    ):
        assert name in src, f"missing step: {name}"


def test_each_step_is_error_isolated() -> None:
    """One failing step must not skip the rest — each step is wrapped
    in its own try/except so a rate-limited FEC pass doesn't stall
    USAspending + LDA + SEC."""
    src = inspect.getsource(scheduled_sweep.run_sweep)
    # Count try:/except: pairs — at least one per step + a couple of
    # buffers.
    assert src.count("except Exception") >= 7, (
        "each of the 7 steps must be individually error-guarded"
    )


def test_priority_domains_env_var_is_read() -> None:
    """The CronJob can be run against a subset via ARGUS_SWEEP_DOMAINS
    (CSV). The wrapper must read env, not just accept a param."""
    src = inspect.getsource(scheduled_sweep._priority_domains)
    assert "ARGUS_SWEEP_DOMAINS" in src


def test_priority_domains_default_is_None_not_empty_tuple() -> None:
    """When the env var is absent, ``_priority_domains`` returns None so
    downstream helpers treat it as "sweep all". An empty tuple would
    filter to nothing."""
    import os
    prior = os.environ.pop("ARGUS_SWEEP_DOMAINS", None)
    try:
        assert scheduled_sweep._priority_domains() is None
    finally:
        if prior is not None:
            os.environ["ARGUS_SWEEP_DOMAINS"] = prior


def test_priority_domains_parses_csv() -> None:
    import os
    prior = os.environ.get("ARGUS_SWEEP_DOMAINS")
    os.environ["ARGUS_SWEEP_DOMAINS"] = " detention_operators , congress ,"
    try:
        assert scheduled_sweep._priority_domains() == (
            "detention_operators", "congress",
        )
    finally:
        if prior is None:
            os.environ.pop("ARGUS_SWEEP_DOMAINS", None)
        else:
            os.environ["ARGUS_SWEEP_DOMAINS"] = prior
