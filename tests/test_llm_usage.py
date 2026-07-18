"""Argus llm_usage instrumentation (Atlas spend Part 1b).

Hermetic tests — no live network, no DB required. Covers:
    * ``feature_scope`` sets + restores the ContextVar cleanly, including
      nested scopes, exceptions, and async propagation.
    * The scrutiny classifier fires ``log_llm_usage_or_swallow`` on both
      the success and failure paths with the expected fields.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

import pytest

from app.services.llm_usage import current_feature, feature_scope


def test_current_feature_defaults_to_unknown() -> None:
    """No scope active → feature reads ``unknown``."""
    assert current_feature() == "unknown"


def test_feature_scope_sets_and_restores() -> None:
    """A single scope sets the feature and restores it on exit."""
    with feature_scope("scrutiny.classify"):
        assert current_feature() == "scrutiny.classify"
    assert current_feature() == "unknown"


def test_feature_scope_nests_correctly() -> None:
    """Nested scopes stack + unstack in LIFO order."""
    with feature_scope("outer"):
        assert current_feature() == "outer"
        with feature_scope("inner"):
            assert current_feature() == "inner"
        assert current_feature() == "outer"
    assert current_feature() == "unknown"


def test_feature_scope_restores_on_exception() -> None:
    """Scope must restore even when the body raises."""
    with pytest.raises(RuntimeError):
        with feature_scope("scrutiny.will_raise"):
            raise RuntimeError("boom")
    assert current_feature() == "unknown"


async def test_feature_scope_propagates_across_await() -> None:
    """ContextVar carries the scope through async awaits."""
    with feature_scope("scrutiny.async"):
        assert current_feature() == "scrutiny.async"
        await asyncio.sleep(0)
        assert current_feature() == "scrutiny.async"


# ─── scrutiny instrumentation ─────────────────────────────────────────


@dataclass
class _StubLogger:
    """Captures each log_llm_usage_or_swallow call for assertion."""

    calls: list[dict] = field(default_factory=list)

    async def __call__(self, **kwargs: Any) -> str | None:
        self.calls.append(kwargs)
        return "row-id-fake"


@dataclass
class _StubAnthropicUsage:
    input_tokens: int
    output_tokens: int
    cache_read_input_tokens: int | None = None
    cache_creation_input_tokens: int | None = None


@dataclass
class _StubAnthropicMessage:
    content: list
    usage: _StubAnthropicUsage
    model: str


@dataclass
class _StubMessages:
    response: _StubAnthropicMessage | None = None
    raise_exc: Exception | None = None

    async def create(self, **_kwargs: Any) -> _StubAnthropicMessage:
        if self.raise_exc is not None:
            raise self.raise_exc
        assert self.response is not None
        return self.response


@dataclass
class _StubAsyncAnthropic:
    api_key: str
    messages: _StubMessages


async def test_classify_from_llm_logs_on_success(monkeypatch) -> None:
    """Successful classify fires log_llm_usage_or_swallow with the
    ``scrutiny.classify`` feature slug + tokens from the response."""
    from app.services import scrutiny

    stub_logger = _StubLogger()
    monkeypatch.setattr(
        "app.services.llm_usage.log_llm_usage_or_swallow", stub_logger
    )
    # Force the settings to look configured.
    monkeypatch.setattr(scrutiny.settings, "anthropic_api_key", "test-key", raising=False)
    monkeypatch.setattr(scrutiny.settings, "database_url", "sqlite:///:memory:", raising=False)
    monkeypatch.setattr(scrutiny.settings, "anthropic_model", "claude-sonnet-4", raising=False)

    text_block = type("B", (), {"text": '{"class":"public","decision":"surface","reason":"stub"}'})()
    stub_message = _StubAnthropicMessage(
        content=[text_block],
        usage=_StubAnthropicUsage(
            input_tokens=15,
            output_tokens=25,
            cache_read_input_tokens=1234,
            cache_creation_input_tokens=50,
        ),
        model="claude-sonnet-4-20260101",
    )

    def _factory(*args, **kwargs):
        return _StubAsyncAnthropic(api_key="test-key", messages=_StubMessages(response=stub_message))

    # Monkey-patch the AsyncAnthropic constructor used by the classifier.
    import anthropic as _anth_mod

    monkeypatch.setattr(_anth_mod, "AsyncAnthropic", _factory)

    verdict = await scrutiny._classify_from_llm("Test Person", [])
    assert verdict.classification.value == "public"
    assert verdict.decision.value == "surface"
    assert len(stub_logger.calls) == 1
    call = stub_logger.calls[0]
    assert call["feature"] == "scrutiny.classify"
    assert call["model"] == "claude-sonnet-4-20260101"
    assert call["prompt_tokens"] == 15
    assert call["completion_tokens"] == 25
    assert call["cache_read_tokens"] == 1234
    assert call["cache_write_tokens"] == 50
    assert call["ok"] is True
    assert isinstance(call["call_ms"], int) and call["call_ms"] >= 0


async def test_classify_from_llm_logs_on_failure(monkeypatch) -> None:
    """Failed classify still fires log_llm_usage_or_swallow with ok=False +
    NULL tokens, and falls back to PRIVATE + SUPPRESS per design."""
    from app.services import scrutiny

    stub_logger = _StubLogger()
    monkeypatch.setattr(
        "app.services.llm_usage.log_llm_usage_or_swallow", stub_logger
    )
    monkeypatch.setattr(scrutiny.settings, "anthropic_api_key", "test-key", raising=False)
    monkeypatch.setattr(scrutiny.settings, "database_url", "sqlite:///:memory:", raising=False)
    monkeypatch.setattr(scrutiny.settings, "anthropic_model", "claude-sonnet-4", raising=False)

    def _factory(*args, **kwargs):
        return _StubAsyncAnthropic(
            api_key="test-key", messages=_StubMessages(raise_exc=RuntimeError("api down"))
        )

    import anthropic as _anth_mod

    monkeypatch.setattr(_anth_mod, "AsyncAnthropic", _factory)

    verdict = await scrutiny._classify_from_llm("Test Person", [])
    assert verdict.classification.value == "private"
    assert verdict.decision.value == "suppress"
    assert len(stub_logger.calls) == 1
    call = stub_logger.calls[0]
    assert call["feature"] == "scrutiny.classify"
    assert call["ok"] is False
    assert call["prompt_tokens"] is None
    assert call["completion_tokens"] is None
    assert call["cache_read_tokens"] is None
    assert call["cache_write_tokens"] is None


async def test_classify_without_api_key_does_not_log(monkeypatch) -> None:
    """No API key → no LLM call → no log row (nothing to attribute)."""
    from app.services import scrutiny

    stub_logger = _StubLogger()
    monkeypatch.setattr(
        "app.services.llm_usage.log_llm_usage_or_swallow", stub_logger
    )
    monkeypatch.setattr(scrutiny.settings, "anthropic_api_key", "", raising=False)
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)

    verdict = await scrutiny._classify_from_llm("Test Person", [])
    assert verdict.classification.value == "private"
    assert verdict.decision.value == "suppress"
    assert stub_logger.calls == []
