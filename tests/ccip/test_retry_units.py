"""Unit tests for the fetch retry helper and its ingestion wiring.

Feature: ccip-langgraph-anomaly, Requirement 12 (Part A). No test here sleeps:
``retry_async`` takes an injectable ``sleep`` and the ingestion tests use a
zero-delay policy, so the suite never actually waits.
"""

from __future__ import annotations

from datetime import date

import pytest

from connector.ccip.config import CcipConfig
from connector.ccip.ingest import fetch_cost_payload
from connector.ccip.retry import (
    RetryPolicy,
    compute_delay,
    is_retryable,
    retry_async,
)
from connector.errors import ErrorCategory, structured_error


# --- is_retryable: only unreachable-source (Req 12.1, 12.2) ----------------


@pytest.mark.parametrize(
    "category,expected",
    [
        (ErrorCategory.UNREACHABLE_SOURCE, True),
        (ErrorCategory.MISSING_DATA, False),
        (ErrorCategory.INVALID_PARAMETER, False),
        (ErrorCategory.UNEXPECTED_ERROR, False),
    ],
)
def test_is_retryable_only_unreachable(category, expected):
    assert is_retryable(structured_error("x", category)) is expected


def test_is_retryable_false_for_success_payload():
    assert is_retryable({"granularity": "DAILY", "items": [1]}) is False
    assert is_retryable("not a dict") is False


# --- compute_delay schedule (Req 12.3) -------------------------------------


def test_compute_delay_no_jitter_is_exponential_and_capped():
    policy = RetryPolicy(base_delay=1.0, max_delay=10.0, multiplier=2.0, jitter=False)
    # attempt 1->1, 2->2, 3->4, 4->8, 5->10 (capped), 6->10 (capped)
    assert [compute_delay(k, policy) for k in range(1, 7)] == [1.0, 2.0, 4.0, 8.0, 10.0, 10.0]


def test_compute_delay_jitter_within_bounds():
    import random

    policy = RetryPolicy(base_delay=1.0, max_delay=30.0, multiplier=2.0, jitter=True)
    rng = random.Random(123)
    for k in range(1, 6):
        base = min(policy.max_delay, policy.base_delay * policy.multiplier ** (k - 1))
        for _ in range(50):
            d = compute_delay(k, policy, rng=rng)
            assert 0.0 <= d <= base


# --- retry_async control flow (Req 12.1, 12.2, 12.5) -----------------------


async def test_retry_async_stops_on_first_success():
    calls = {"n": 0}

    async def op():
        calls["n"] += 1
        return {"ok": True}

    slept: list[float] = []

    async def fake_sleep(d):
        slept.append(d)

    out = await retry_async(op, RetryPolicy(max_attempts=3), sleep=fake_sleep)
    assert out == {"ok": True}
    assert calls["n"] == 1
    assert slept == []


async def test_retry_async_stops_on_non_retryable_error():
    calls = {"n": 0}

    async def op():
        calls["n"] += 1
        return structured_error("bad", ErrorCategory.INVALID_PARAMETER)

    async def fake_sleep(d):
        raise AssertionError("should not sleep on a non-retryable error")

    out = await retry_async(op, RetryPolicy(max_attempts=3), sleep=fake_sleep)
    assert out["category"] == "invalid-parameter"
    assert calls["n"] == 1


async def test_retry_async_retries_then_recovers():
    calls = {"n": 0}

    async def op():
        calls["n"] += 1
        if calls["n"] < 3:
            return structured_error("down", ErrorCategory.UNREACHABLE_SOURCE)
        return {"ok": True}

    slept: list[float] = []

    async def fake_sleep(d):
        slept.append(d)

    policy = RetryPolicy(max_attempts=5, base_delay=1.0, multiplier=2.0, jitter=False)
    out = await retry_async(op, policy, sleep=fake_sleep)
    assert out == {"ok": True}
    assert calls["n"] == 3
    # Slept between attempts 1->2 and 2->3 only.
    assert slept == [1.0, 2.0]


async def test_retry_async_exhausts_and_returns_last_error():
    calls = {"n": 0}

    async def op():
        calls["n"] += 1
        return structured_error("down", ErrorCategory.UNREACHABLE_SOURCE)

    async def fake_sleep(d):
        pass

    policy = RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0, jitter=False)
    out = await retry_async(op, policy, sleep=fake_sleep)
    assert out["category"] == "unreachable-source"
    assert calls["n"] == 3


async def test_retry_async_single_attempt_never_retries():
    calls = {"n": 0}

    async def op():
        calls["n"] += 1
        return structured_error("down", ErrorCategory.UNREACHABLE_SOURCE)

    out = await retry_async(op, RetryPolicy(max_attempts=1))
    assert out["category"] == "unreachable-source"
    assert calls["n"] == 1


# --- fetch_cost_payload integration with the policy (Req 12.1, 12.5) -------


_ZERO = RetryPolicy(max_attempts=3, base_delay=0.0, max_delay=0.0, jitter=False)


async def _ok_tool(start, end, granularity="DAILY"):
    return {"granularity": "DAILY", "items": [
        {"timestamp": "2026-01-01", "cost_value": 1.0, "tags": {"service": "s"}}
    ]}


async def test_fetch_retries_transient_then_succeeds():
    calls = {"n": 0}

    async def flaky(start, end, granularity="DAILY"):
        calls["n"] += 1
        if calls["n"] < 3:
            return structured_error("down", ErrorCategory.UNREACHABLE_SOURCE)
        return await _ok_tool(start, end, granularity)

    out = await fetch_cost_payload(flaky, date(2026, 1, 1), 90, retry=_ZERO)
    assert out["granularity"] == "DAILY"
    assert calls["n"] == 3


async def test_fetch_exhaustion_stamps_attempts():
    calls = {"n": 0}

    async def always_down(start, end, granularity="DAILY"):
        calls["n"] += 1
        return structured_error("down", ErrorCategory.UNREACHABLE_SOURCE)

    out = await fetch_cost_payload(always_down, date(2026, 1, 1), 90, retry=_ZERO)
    assert out["category"] == "unreachable-source"
    assert out["attempts"] == 3
    assert calls["n"] == 3


async def test_fetch_does_not_retry_missing_data():
    calls = {"n": 0}

    async def empty(start, end, granularity="DAILY"):
        calls["n"] += 1
        return {"granularity": "DAILY", "items": []}

    out = await fetch_cost_payload(empty, date(2026, 1, 1), 90, retry=_ZERO)
    assert out["category"] == "missing-data"
    assert calls["n"] == 1  # deterministic -> never retried


async def test_fetch_does_not_retry_invalid_granularity():
    calls = {"n": 0}

    async def monthly(start, end, granularity="DAILY"):
        calls["n"] += 1
        return {"granularity": "MONTHLY", "items": [1]}

    out = await fetch_cost_payload(monthly, date(2026, 1, 1), 90, retry=_ZERO)
    assert out["category"] == "invalid-parameter"
    assert calls["n"] == 1


async def test_fetch_no_policy_single_call():
    calls = {"n": 0}

    async def always_down(start, end, granularity="DAILY"):
        calls["n"] += 1
        return structured_error("down", ErrorCategory.UNREACHABLE_SOURCE)

    out = await fetch_cost_payload(always_down, date(2026, 1, 1), 90, retry=None)
    assert out["category"] == "unreachable-source"
    assert calls["n"] == 1
    assert "attempts" not in out


def test_retry_policy_from_config_reads_fields():
    cfg = CcipConfig(retry_max_attempts=5, retry_base_delay=2.5,
                     retry_max_delay=60.0, retry_multiplier=3.0, retry_jitter=False)
    p = RetryPolicy.from_config(cfg)
    assert (p.max_attempts, p.base_delay, p.max_delay, p.multiplier, p.jitter) == (
        5, 2.5, 60.0, 3.0, False
    )
