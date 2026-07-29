"""
Retry + exponential backoff for the httpx transport — T-040.

Mirrors basin-js ``src/internal/retry.ts``: bounded attempts, jittered
exponential backoff, retry on transport errors / 5xx / 429, and ``Retry-After``
honoured for 429.  Only idempotent methods are retried by default — POST and
PATCH (inserts/updates/upserts/RPC) are skipped unless ``retry_writes`` is set,
since replaying them is unsafe.
"""

from __future__ import annotations

import random
from dataclasses import dataclass, field

import httpx

# HTTP methods safe to replay automatically.
_IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS"})
_DEFAULT_RETRY_STATUS = frozenset({429, 500, 502, 503, 504})


@dataclass
class RetryConfig:
    """Tunable retry policy.  Defaults mirror basin-js."""

    max_attempts: int = 3
    base_ms: float = 250.0
    max_ms: float = 5000.0
    jitter_ms: float = 50.0
    retry_status: frozenset[int] = field(default=_DEFAULT_RETRY_STATUS)
    retry_writes: bool = False

    @property
    def disabled(self) -> bool:
        return self.max_attempts <= 1


def is_idempotent(method: str) -> bool:
    return method.upper() in _IDEMPOTENT_METHODS


def should_retry_response(resp: httpx.Response, config: RetryConfig) -> bool:
    return resp.status_code in config.retry_status


def parse_retry_after_ms(header: str | None) -> float | None:
    """Parse a ``Retry-After`` header (delta-seconds or HTTP-date) to ms."""
    if not header:
        return None
    trimmed = header.strip()
    if trimmed.isdigit():
        return float(int(trimmed)) * 1000.0
    try:
        from email.utils import parsedate_to_datetime

        target = parsedate_to_datetime(trimmed)
    except (TypeError, ValueError):
        return None
    if target is None:
        return None
    import datetime as _dt

    now = _dt.datetime.now(tz=target.tzinfo)
    delta = (target - now).total_seconds()
    return max(0.0, delta * 1000.0)


def compute_backoff_ms(attempt: int, config: RetryConfig) -> float:
    """Backoff for ``attempt`` (1-based), with jitter, capped at ``max_ms``."""
    jitter = random.random() * config.jitter_ms
    raw = config.base_ms * float(2 ** (attempt - 1)) + jitter
    return min(raw, config.max_ms)


def next_delay_ms(
    attempt: int,
    resp: httpx.Response | None,
    config: RetryConfig,
) -> float:
    """Delay before the next attempt; honours ``Retry-After`` on 429."""
    if resp is not None and resp.status_code == 429:
        retry_after = parse_retry_after_ms(resp.headers.get("Retry-After"))
        if retry_after is not None:
            return min(retry_after, config.max_ms)
    return compute_backoff_ms(attempt, config)
