"""Bounded exponential-backoff-with-jitter retry helper (design component ``retry.py``).

A tiny, dependency-free async retry primitive the ingestion fetch wraps around
its single Cost_Tool call. It is deliberately NOT a general-purpose decorator: it
understands the Structured_Error contract so it retries ONLY transient
(``unreachable-source``) failures and never the deterministic categories
(``invalid-parameter`` / ``missing-data`` / ``unexpected-error``), which would
fail identically on a retry (Req 12.1, 12.2).

The backoff schedule for attempt ``k`` (1-indexed) is::

    delay_k = min(max_delay, base_delay * multiplier**(k - 1))

with optional random jitter drawing the actual sleep from ``[0, delay_k]`` so a
fleet of schedulers does not retry in lockstep (Req 12.3). Sleeps occur strictly
BETWEEN attempts; each attempt is still bounded by the shared 30s ``HttpClient``
timeout (Req 11.4), so the retry layer adds latency only through its backoff.

The ``sleep`` callable is injectable so unit/property tests can run with a no-op
sleep and never actually wait.

Requirements: 12.1, 12.2, 12.3, 12.4, 12.5.
"""

from __future__ import annotations

import asyncio
import random
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from connector.errors import ErrorCategory

__all__ = [
    "RetryPolicy",
    "is_retryable",
    "compute_delay",
    "retry_async",
]


@dataclass(frozen=True)
class RetryPolicy:
    """Bounded exponential-backoff-with-jitter parameters (Req 12.1-12.3).

    ``max_attempts`` counts the TOTAL attempts including the first, so
    ``max_attempts=1`` disables retrying. ``base_delay``/``max_delay`` are in
    seconds; ``multiplier`` grows the delay geometrically; ``jitter`` randomizes
    each sleep within ``[0, computed_delay]``.
    """

    max_attempts: int = 3
    base_delay: float = 1.0
    max_delay: float = 30.0
    multiplier: float = 2.0
    jitter: bool = True

    @classmethod
    def from_config(cls, config: Any) -> "RetryPolicy":
        """Build a policy from a :class:`connector.ccip.config.CcipConfig`.

        Reads the ``retry_*`` fields, falling back to the dataclass defaults when
        an attribute is absent (keeps fakes/minimal configs working).
        """
        return cls(
            max_attempts=int(getattr(config, "retry_max_attempts", 3)),
            base_delay=float(getattr(config, "retry_base_delay", 1.0)),
            max_delay=float(getattr(config, "retry_max_delay", 30.0)),
            multiplier=float(getattr(config, "retry_multiplier", 2.0)),
            jitter=bool(getattr(config, "retry_jitter", True)),
        )


def is_retryable(result: Any) -> bool:
    """True iff ``result`` is a Structured_Error with an ``unreachable-source`` category.

    A success payload (anything that is not a Structured_Error dict) and every
    deterministic error category are NOT retryable (Req 12.1, 12.2).
    """
    return (
        isinstance(result, dict)
        and "error" in result
        and result.get("category") == ErrorCategory.UNREACHABLE_SOURCE.value
    )


def compute_delay(attempt: int, policy: RetryPolicy, *, rng: Optional[random.Random] = None) -> float:
    """Return the backoff delay (seconds) to sleep AFTER a failed ``attempt`` (1-indexed).

    The base delay is ``min(max_delay, base_delay * multiplier**(attempt - 1))``
    (Req 12.3). When ``policy.jitter`` is set, the returned value is drawn
    uniformly from ``[0, base]`` ("full jitter"); otherwise it is ``base``. An
    optional ``rng`` makes jitter deterministic in tests.
    """
    exponent = max(0, attempt - 1)
    raw = policy.base_delay * (policy.multiplier ** exponent)
    base = min(policy.max_delay, raw)
    if base < 0:
        base = 0.0
    if not policy.jitter:
        return base
    source = rng if rng is not None else random
    return source.uniform(0.0, base)


async def retry_async(
    operation: Callable[[], Awaitable[Any]],
    policy: RetryPolicy,
    *,
    is_retryable: Callable[[Any], bool] = is_retryable,
    on_retry: Optional[Callable[[int, int, Any, float], None]] = None,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    rng: Optional[random.Random] = None,
) -> Any:
    """Invoke ``await operation()`` up to ``policy.max_attempts`` times.

    Returns the first NON-retryable result immediately (a success, or any
    deterministic error). Between retryable results it sleeps a backoff delay
    (see :func:`compute_delay`) and tries again. On exhaustion it returns the LAST
    (retryable) result unchanged — the caller is responsible for stamping the
    attempt count onto it (Req 12.5).

    ``on_retry(attempt, remaining, result, delay)`` is called just before each
    backoff sleep so the caller can emit a WARNING log naming the attempt number,
    remaining attempts, error category, and delay (Req 12.4). ``sleep`` is
    injectable so tests never actually wait.
    """
    attempts = max(1, policy.max_attempts)
    result: Any = None
    for attempt in range(1, attempts + 1):
        result = await operation()
        if not is_retryable(result):
            return result
        if attempt >= attempts:
            # Budget exhausted: return the last retryable result (Req 12.5).
            return result
        delay = compute_delay(attempt, policy, rng=rng)
        if on_retry is not None:
            on_retry(attempt, attempts - attempt, result, delay)
        await sleep(delay)
    return result
