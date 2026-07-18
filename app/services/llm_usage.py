"""Per-call LLM usage logger (Atlas spend Part 1b — argus side).

Mirrors hollywood_gen's ``web_app.services.llm_usage`` shape:
    * ``feature_scope("scrutiny.classify")`` — ContextVar-backed scope
      that the backend reads when logging.
    * ``log_llm_usage(session, ...)`` — insert one row on the passed
      session; caller commits.
    * ``log_llm_usage_or_swallow(database_url, ...)`` — fire-and-forget
      variant: opens a short-lived engine, writes + commits, and SWALLOWS
      any DB failure. Bookkeeping failures MUST NEVER break the calling
      LLM path.

Cache tokens (helen 2026-07-18 Part 1a refinement): ``cache_read_tokens`` /
``cache_write_tokens`` mirror Anthropic's ``cache_read_input_tokens`` /
``cache_creation_input_tokens``; Part 2 pricing multiplies each by its
own per-model rate.
"""

from __future__ import annotations

import contextvars
import logging
from contextlib import contextmanager
from typing import Iterator
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.models import LlmUsage

logger = logging.getLogger(__name__)

APP_SLUG = "argus"

_FEATURE_CTX: contextvars.ContextVar[str] = contextvars.ContextVar(
    "llm_usage_feature", default="unknown"
)


@contextmanager
def feature_scope(feature: str) -> Iterator[None]:
    """Set the ``feature`` slug for LLM calls made inside this ``with`` block."""
    token = _FEATURE_CTX.set(feature)
    try:
        yield
    finally:
        _FEATURE_CTX.reset(token)


def current_feature() -> str:
    """Return the currently-scoped feature slug (or ``unknown`` if none set)."""
    return _FEATURE_CTX.get()


async def log_llm_usage(
    session: AsyncSession,
    *,
    feature: str,
    model: str,
    prompt_tokens: int | None,
    completion_tokens: int | None,
    call_ms: int | None,
    ok: bool = True,
    app: str = APP_SLUG,
    cache_read_tokens: int | None = None,
    cache_write_tokens: int | None = None,
) -> str:
    """Insert one ``llm_usage`` row + return its id. Caller commits."""
    row_id = str(uuid4())
    row = LlmUsage(
        id=row_id,
        app=app,
        feature=feature,
        model=model,
        prompt_tokens=prompt_tokens,
        completion_tokens=completion_tokens,
        cache_read_tokens=cache_read_tokens,
        cache_write_tokens=cache_write_tokens,
        call_ms=call_ms,
        ok=ok,
    )
    session.add(row)
    await session.flush()
    return row_id


async def log_llm_usage_or_swallow(
    *,
    database_url: str,
    feature: str,
    model: str,
    prompt_tokens: int | None,
    completion_tokens: int | None,
    call_ms: int | None,
    ok: bool = True,
    app: str = APP_SLUG,
    cache_read_tokens: int | None = None,
    cache_write_tokens: int | None = None,
) -> str | None:
    """Fire-and-forget logger — swallows any DB failure at WARNING level."""
    engine = create_async_engine(database_url, pool_pre_ping=True)
    try:
        factory = async_sessionmaker(engine, expire_on_commit=False)
        async with factory() as session:
            row_id = await log_llm_usage(
                session,
                feature=feature,
                model=model,
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                cache_read_tokens=cache_read_tokens,
                cache_write_tokens=cache_write_tokens,
                call_ms=call_ms,
                ok=ok,
                app=app,
            )
            await session.commit()
            return row_id
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            "llm_usage log dropped: feature=%s model=%s ok=%s error=%s",
            feature,
            model,
            ok,
            exc,
        )
        return None
    finally:
        await engine.dispose()
