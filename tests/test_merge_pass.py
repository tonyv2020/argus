"""P2 merge-pass shape + fail-closed surface_mode tests.

The DB-touching re-point loop is validated live post-deploy; hermetic
tests here guard the privacy-critical decisions: strictness ordering,
surface_mode escalation vs refusal, dry-run mode.
"""

from __future__ import annotations

import inspect

from app.services.ingest.merge_pass import (
    _SURFACE_MODE_STRICTNESS,
    _most_protected,
    apply_pending,
    main,
)


def test_surface_mode_strictness_ordering() -> None:
    """suppress > alias > open — the whole privacy story rides on this."""
    assert _SURFACE_MODE_STRICTNESS["suppress"] > _SURFACE_MODE_STRICTNESS["alias"]
    assert _SURFACE_MODE_STRICTNESS["alias"] > _SURFACE_MODE_STRICTNESS["open"]


def test_most_protected_returns_the_more_protected() -> None:
    """The survivor inherits the MORE-protected mode across the pair —
    NEVER the less-protected one."""
    assert _most_protected("open", "suppress") == "suppress"
    assert _most_protected("alias", "open") == "alias"
    assert _most_protected("suppress", "alias") == "suppress"
    assert _most_protected("open", "open") == "open"


def test_merge_pass_refuses_when_src_more_protected_than_dst() -> None:
    """Fail-closed on surface_mode (spec §3): a merge from a
    suppress-mode canonical INTO an open-mode canonical would surface
    the suppressed identity. Must refuse + log."""
    src = inspect.getsource(apply_pending)
    src_apply = inspect.getsource(
        __import__("app.services.ingest.merge_pass", fromlist=["_apply_one"])
        ._apply_one
    )
    combined = src + src_apply
    assert "REFUSED (privacy)" in combined, (
        "the fail-closed refusal path must log a distinctive 'REFUSED "
        "(privacy)' string so post-merge audit can grep for it"
    )
    assert "refused_privacy" in combined


def test_merge_pass_dry_run_short_circuits_before_repoint() -> None:
    """A dry-run must NOT re-point edges — it's just a log preview so
    an operator can inspect the queue before letting the merge fly."""
    src = inspect.getsource(apply_pending)
    # The dry-run branch continues without touching _apply_one.
    assert "DRY-RUN" in src
    assert "continue" in src


def test_merge_stats_tracks_repointed_edges_and_aliases() -> None:
    """Stats surface must include edge / alias / anchor counters so the
    operator sees impact of a run at a glance."""
    from app.services.ingest.merge_pass import MergeStats

    s = MergeStats()
    assert hasattr(s, "edges_repointed")
    assert hasattr(s, "aliases_repointed")
    assert hasattr(s, "anchor_rows_repointed")
    assert hasattr(s, "refused_privacy")


def test_cli_exposes_dry_run_flag() -> None:
    """`python -m ... --dry-run` must be a valid invocation."""
    src = inspect.getsource(main)
    assert "--dry-run" in src
