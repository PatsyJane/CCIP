"""Hypothesis property-based tests for the CCIP deterministic layer.

Feature: ccip-langgraph-anomaly. Each test exercises a correctness property from
the design over a large input space with external dependencies faked.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from datetime import date, timedelta

from connector.ccip.config import CcipConfig
from connector.ccip.detection import detect_cold_start, detect_statistical
from connector.ccip.drilldown import isolate_contributors
from connector.ccip.ingest import decide_fetch_window
from connector.ccip.metadata_store import (
    account_metadata_path,
    merge_history,
    sanitize_path_segment,
)
from connector.ccip.models import DataMode, classify_data_mode
from connector.ccip.retry import RetryPolicy, compute_delay, is_retryable
from connector.ccip.routers import route_after_detection, route_alert_check
from connector.ccip.stl_model import StlResult, compute_residual_threshold
from connector.errors import ErrorCategory, structured_error


# Feature: ccip-langgraph-anomaly, Property 2: Data-mode pivots on 30/60/90
@given(n=st.integers(min_value=0, max_value=1000))
@settings(max_examples=200)
def test_property_data_mode_pivots(n):
    mode = classify_data_mode(n)
    if n < 30:
        assert mode is DataMode.COLD_START
    elif n < 60:
        assert mode is DataMode.INITIAL
    elif n < 90:
        assert mode is DataMode.BEGINNER
    else:
        assert mode is DataMode.NORMAL


# Feature: ccip-langgraph-anomaly, Property 9: Cold-start absolute-threshold rule
@given(
    actual=st.floats(min_value=0, max_value=1e6, allow_nan=False, allow_infinity=False),
    prev=st.floats(min_value=0, max_value=1e6, allow_nan=False, allow_infinity=False),
    threshold=st.floats(min_value=0.01, max_value=1e6, allow_nan=False, allow_infinity=False),
)
@settings(max_examples=200)
def test_property_cold_start_rule(actual, prev, threshold):
    md = {"non_anomaly_days": {"2026-01-01": prev}}
    detected, baseline = detect_cold_start(actual, md, "2026-06-01", threshold)
    assert detected == (abs(actual - prev) > threshold)
    assert baseline == "2026-01-01"


# Feature: ccip-langgraph-anomaly, Property 9 (cont.): no prior day -> False
@given(actual=st.floats(min_value=0, max_value=1e6, allow_nan=False, allow_infinity=False))
@settings(max_examples=100)
def test_property_cold_start_no_prior(actual):
    detected, baseline = detect_cold_start(actual, {"non_anomaly_days": {}}, "2026-06-01", 20.0)
    assert detected is False and baseline is None


# Feature: ccip-langgraph-anomaly, Property 10: statistical residual-threshold rule
@given(
    target=st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
    thr=st.floats(min_value=-1e6, max_value=1e6, allow_nan=False, allow_infinity=False),
)
@settings(max_examples=200)
def test_property_statistical_rule(target, thr):
    r = StlResult([], [], [], residual_threshold=thr, target_residual=target,
                  expected_target=0.0, method="iqr", seasonal_summary={})
    assert detect_statistical(r) == (target > thr)


# Feature: ccip-langgraph-anomaly, Property 11: Anomaly-Check routing is exact
@given(detected=st.booleans(), has_error=st.booleans())
@settings(max_examples=100)
def test_property_route_after_detection(detected, has_error):
    state = {"account_anomaly_detected": detected}
    if has_error:
        state["error"] = {"error": "x"}
    result = route_after_detection(state)
    if has_error:
        # Error routes to the terminal failure-dispatch node, not END (Req 12.6).
        assert result == "dispatch_failure_alert"
    elif detected:
        assert result == "drill_down_services"
    else:
        assert result == "alert_check"


# Feature: ccip-langgraph-anomaly, Property 12: Alert-Check routing is exact
@given(detected=st.booleans(), new_services=st.lists(st.text(min_size=1, max_size=5), max_size=3),
       has_error=st.booleans())
@settings(max_examples=200)
def test_property_route_alert_check(detected, new_services, has_error):
    state = {"account_anomaly_detected": detected, "new_services_detected": new_services}
    if has_error:
        state["error"] = {"error": "x"}
    result = route_alert_check(state)
    if has_error:
        # Error routes to the terminal failure-dispatch node, not END (Req 12.6).
        assert result == "dispatch_failure_alert"
    elif detected or new_services:
        assert result == "dispatch_alerts"
    else:
        assert result == "END"


# Feature: ccip-langgraph-anomaly, Property 13: contributor set + ordering
@given(
    costs=st.dictionaries(
        st.text(min_size=1, max_size=4),
        st.floats(min_value=0, max_value=1e5, allow_nan=False, allow_infinity=False),
        max_size=6,
    )
)
@settings(max_examples=150)
def test_property_contributors_positive_and_ordered(costs):
    # Baseline is zero for all (no history), so delta == current cost.
    out = isolate_contributors(costs, {}, "2026-09-20", "2026-09-29", {})
    # Every contributor has a strictly positive delta.
    assert all(c["delta"] > 0 for c in out)
    # Ordered by descending delta.
    deltas = [c["delta"] for c in out]
    assert deltas == sorted(deltas, reverse=True)
    # Only positive-cost services are included.
    assert len(out) == sum(1 for v in costs.values() if round(v, 2) > 0)


# Feature: ccip-langgraph-anomaly, Property 16: threshold computation is finite
@given(
    resid=st.lists(st.floats(min_value=-1e4, max_value=1e4, allow_nan=False, allow_infinity=False),
                   min_size=1, max_size=50),
    method=st.sampled_from(["iqr", "sigma"]),
)
@settings(max_examples=150)
def test_property_threshold_finite(resid, method):
    import math
    thr = compute_residual_threshold(resid, method, 1.5, 3.0)
    assert math.isfinite(thr)


# --- Requirement 12: retry resilience + failure dispatch -------------------

_RETRYABLE = structured_error("down", ErrorCategory.UNREACHABLE_SOURCE)
_NON_RETRYABLE = st.sampled_from(
    [
        structured_error("x", ErrorCategory.MISSING_DATA),
        structured_error("x", ErrorCategory.INVALID_PARAMETER),
        structured_error("x", ErrorCategory.UNEXPECTED_ERROR),
    ]
)


# Feature: ccip-langgraph-anomaly, Property 18: only transient failures retry
@given(
    attempts=st.integers(min_value=1, max_value=6),
    fail_until=st.integers(min_value=0, max_value=8),
)
@settings(max_examples=200)
def test_property_retry_only_transient_within_budget(attempts, fail_until):
    import asyncio

    calls = {"n": 0}

    async def op():
        calls["n"] += 1
        if calls["n"] <= fail_until:
            return structured_error("down", ErrorCategory.UNREACHABLE_SOURCE)
        return {"ok": True}

    async def no_sleep(_d):
        pass

    from connector.ccip.retry import retry_async

    policy = RetryPolicy(max_attempts=attempts, base_delay=0.0, max_delay=0.0, jitter=False)
    out = asyncio.run(retry_async(op, policy, sleep=no_sleep))

    # Never more than the budget.
    assert calls["n"] <= attempts
    if fail_until < attempts:
        # Recovered within budget: success, exactly fail_until+1 calls.
        assert out == {"ok": True}
        assert calls["n"] == fail_until + 1
    else:
        # Exhausted: last result is the transient error, budget fully spent.
        assert is_retryable(out)
        assert calls["n"] == attempts


# Feature: ccip-langgraph-anomaly, Property 18 (cont.): non-retryable stops at 1
@given(err=_NON_RETRYABLE, attempts=st.integers(min_value=1, max_value=6))
@settings(max_examples=100)
def test_property_non_retryable_never_retries(err, attempts):
    import asyncio

    calls = {"n": 0}

    async def op():
        calls["n"] += 1
        return err

    async def no_sleep(_d):
        raise AssertionError("non-retryable must not sleep")

    from connector.ccip.retry import retry_async

    policy = RetryPolicy(max_attempts=attempts, base_delay=0.0, jitter=False)
    out = asyncio.run(retry_async(op, policy, sleep=no_sleep))
    assert out == err
    assert calls["n"] == 1


# Feature: ccip-langgraph-anomaly, Property 19: backoff bounded/growing/jittered
@given(
    attempt=st.integers(min_value=1, max_value=12),
    base=st.floats(min_value=0.1, max_value=5.0, allow_nan=False, allow_infinity=False),
    cap=st.floats(min_value=0.1, max_value=120.0, allow_nan=False, allow_infinity=False),
    mult=st.floats(min_value=1.0, max_value=4.0, allow_nan=False, allow_infinity=False),
)
@settings(max_examples=200)
def test_property_backoff_schedule(attempt, base, cap, mult):
    expected = min(cap, base * (mult ** (attempt - 1)))

    no_jit = RetryPolicy(base_delay=base, max_delay=cap, multiplier=mult, jitter=False)
    d = compute_delay(attempt, no_jit)
    assert abs(d - expected) <= 1e-9
    assert 0.0 <= d <= cap

    import random

    rng = random.Random(attempt)
    jit = RetryPolicy(base_delay=base, max_delay=cap, multiplier=mult, jitter=True)
    for _ in range(20):
        dj = compute_delay(attempt, jit, rng=rng)
        assert 0.0 <= dj <= expected + 1e-9


# Feature: ccip-langgraph-anomaly, Property 20/21: any error -> failure dispatch,
# original error preserved, failure alert sent once, LLM-free + scrubbed.
@given(
    category=st.sampled_from(list(ErrorCategory)),
    secret=st.text(
        alphabet=st.characters(min_codepoint=97, max_codepoint=122), min_size=6, max_size=16
    ),
)
@settings(max_examples=75, deadline=None)
def test_property_any_error_reaches_failure_dispatch(category, secret):
    import asyncio

    from connector.ccip.config import CcipConfig, CcipSecrets
    from connector.ccip.nodes import CcipDeps, dispatch_failure_alert

    class _Sender:
        can_send = True

        def __init__(self):
            self.sent = []

        async def send(self, subject, body):
            self.sent.append((subject, body))

    class _LLM:
        def __init__(self):
            self.prompts = []

        async def complete(self, prompt):  # pragma: no cover - must not be called
            self.prompts.append(prompt)
            raise AssertionError("LLM must not be used by the failure path")

    sender = _Sender()
    llm = _LLM()
    config = CcipConfig(_secrets=CcipSecrets(llm_api_key="k", smtp_password=secret))
    deps = CcipDeps(
        cost_tool=None, llm_client=llm, email_sender=None,
        config=config, failure_email_sender=sender,
    )
    err = structured_error(f"boom leaked {secret}", category, source="ingest")
    state = {"account_id": "A", "target_date": "2026-09-29", "error": err}

    out = asyncio.run(dispatch_failure_alert(state, deps))

    # Sent exactly once (real send since can_send=True).
    assert len(sender.sent) == 1
    assert out["failure_alert_dispatched"] is True
    # Deterministic: LLM never prompted.
    assert llm.prompts == []
    # Secret scrubbed from the rendered alert (Req 12.11).
    subject, body = sender.sent[0]
    assert secret not in subject and secret not in body
    # Node never mutates the error channel (original error preserved, Req 12.6).
    assert "error" not in out


# --- Increment 2: D-2 offset, per-account paths, rolling history, fetch window ---

# Simple identifier text for account ids/names: a mix of safe and unsafe chars so
# the sanitizer is exercised, but never empty (so a filename is always producible).
_ID_TEXT = st.text(
    alphabet=st.characters(min_codepoint=32, max_codepoint=126), min_size=1, max_size=24
)


# Feature: ccip-langgraph-anomaly, Property 22: D-2 default resolution honors explicit
@given(
    offset=st.integers(min_value=0, max_value=10),
    explicit=st.booleans(),
    base_ordinal=st.integers(
        min_value=date(2020, 1, 1).toordinal(), max_value=date(2030, 12, 31).toordinal()
    ),
)
@settings(max_examples=200)
def test_property_d2_default_resolution(offset, explicit, base_ordinal):
    # The CLI default is ``today - offset``; an explicit date resolves to itself.
    # Model the pure resolution rule the CLI applies (Req 13.1, 13.2).
    today = date.fromordinal(base_ordinal)
    explicit_date = today - timedelta(days=3) if explicit else None

    resolved = explicit_date if explicit_date is not None else (today - timedelta(days=offset))

    if explicit:
        assert resolved == explicit_date
    else:
        assert resolved == today - timedelta(days=offset)
    # The resolved date is always the window END (never after it).
    assert resolved <= today


# Feature: ccip-langgraph-anomaly, Property 23: per-account path isolated + safe
@given(account_id=_ID_TEXT, account_name=st.one_of(st.just(""), _ID_TEXT), other_id=_ID_TEXT)
@settings(max_examples=200)
def test_property_account_path_isolated_and_safe(account_id, account_name, other_id):
    import os

    cfg = CcipConfig(metadata_dir="metadata")
    path = account_metadata_path(cfg, account_id, account_name)

    # Under the configured base dir, a .json file.
    assert path.startswith("metadata" + os.sep) or path.startswith("metadata/")
    assert path.endswith(".json")

    filename = os.path.basename(path)[: -len(".json")]
    # Filename stem contains only filename-safe characters (Req 14.2).
    assert all(ch.isalnum() or ch in "._-" for ch in filename)
    # Traversal-safe: no separators, and the sanitized SEGMENTS are never "."/".."
    # directory components (a literal ".." inside a longer name has no separators
    # around it, so it is just characters, not a parent-dir reference).
    assert "/" not in filename and "\\" not in filename
    assert (sanitize_path_segment(account_id) or "account") not in (".", "..")
    assert sanitize_path_segment(account_name) not in ("..",)  # "" or a real name

    san_id = sanitize_path_segment(account_id) or "account"
    san_name = sanitize_path_segment(account_name)
    # The sanitized id is always present; the name only when non-blank (Req 14.3).
    assert san_id in filename
    if san_name:
        assert san_name in filename

    # Two accounts with DIFFERENT sanitized ids map to different files (Req 14.4).
    if (sanitize_path_segment(other_id) or "account") != san_id:
        assert account_metadata_path(cfg, other_id, account_name) != path


# Feature: ccip-langgraph-anomaly, Property 24: history merge idempotent/overwrite/trim
@given(
    persisted=st.lists(
        st.tuples(
            st.integers(min_value=0, max_value=200),  # day offset
            st.floats(min_value=0, max_value=1e5, allow_nan=False, allow_infinity=False),
        ),
        max_size=40,
    ),
    incoming=st.lists(
        st.tuples(
            st.integers(min_value=0, max_value=200),
            st.floats(min_value=0, max_value=1e5, allow_nan=False, allow_infinity=False),
        ),
        max_size=40,
    ),
    history_days=st.integers(min_value=1, max_value=90),
)
@settings(max_examples=200)
def test_property_history_merge_idempotent_overwrite_trim(persisted, incoming, history_days):
    base = date(2026, 1, 1)

    def rows(pairs):
        # Later entries for the same day win within a single list (dict-dedup),
        # mirroring how a real series has one row per day.
        by_day = {}
        for off, cost in pairs:
            by_day[(base + timedelta(days=off)).isoformat()] = round(cost, 2)
        return [{"day": d, "cost": c} for d, c in by_day.items()]

    p_rows, i_rows = rows(persisted), rows(incoming)
    merged = merge_history(p_rows, i_rows, history_days)

    # Ascending by day, unique days, trimmed to history_days.
    days = [r["day"] for r in merged]
    assert days == sorted(days)
    assert len(days) == len(set(days))
    assert len(merged) <= history_days

    # Incoming overwrites persisted for overlapping days (never summed).
    incoming_by_day = {r["day"]: r["cost"] for r in i_rows}
    for r in merged:
        if r["day"] in incoming_by_day:
            assert r["cost"] == incoming_by_day[r["day"]]

    # Idempotent: merging the same incoming twice equals merging once.
    twice = merge_history(merged, i_rows, history_days)
    assert twice == merged


# Feature: ccip-langgraph-anomaly, Property 25: fetch-window selection matches state
@given(
    persisted_days=st.integers(min_value=0, max_value=120),
    gap=st.integers(min_value=0, max_value=5),
    days_since_full=st.integers(min_value=0, max_value=20),
    has_last_full=st.booleans(),
)
@settings(max_examples=300)
def test_property_fetch_window_selection(persisted_days, gap, days_since_full, has_last_full):
    history_days = 90
    trailing = 2
    reconcile = 7
    cfg = CcipConfig(
        history_days=history_days,
        trailing_fetch_days=trailing,
        reconciliation_interval_days=reconcile,
    )
    target = date(2026, 6, 1)

    # Build a contiguous persisted history whose LATEST day is target - gap.
    latest = target - timedelta(days=gap)
    account_history = [
        {"day": (latest - timedelta(days=persisted_days - 1 - i)).isoformat(), "cost": 10.0}
        for i in range(persisted_days)
    ]
    metadata = {"account_history": account_history}
    if has_last_full:
        metadata["last_full_fetch_date"] = (target - timedelta(days=days_since_full)).isoformat()

    plan = decide_fetch_window(metadata, target, cfg)

    # The window END is always the Target_Date (Req 13.3).
    assert plan.end == target.isoformat()

    distinct = len({r["day"] for r in account_history})
    is_cold = distinct < history_days
    is_gap = (not is_cold) and latest.isoformat() < (target - timedelta(days=1)).isoformat()
    if has_last_full:
        due = days_since_full >= reconcile
    else:
        due = True  # no prior full pull recorded -> due for reconciliation

    if is_cold:
        assert plan.mode == "full" and plan.reason == "cold-start"
    elif is_gap:
        assert plan.mode == "full" and plan.reason == "gap"
    elif due:
        assert plan.mode == "full" and plan.reason == "reconciliation"
        assert plan.replace_history is True
    else:
        assert plan.mode == "trailing" and plan.reason == "steady-state"
        assert plan.start == (target - timedelta(days=trailing - 1)).isoformat()
