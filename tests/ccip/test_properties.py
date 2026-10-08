"""Hypothesis property-based tests for the CCIP deterministic layer.

Feature: ccip-langgraph-anomaly. Each test exercises a correctness property from
the design over a large input space with external dependencies faked.
"""

from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from connector.ccip.detection import detect_cold_start, detect_statistical
from connector.ccip.drilldown import isolate_contributors
from connector.ccip.models import DataMode, classify_data_mode
from connector.ccip.routers import route_after_detection, route_alert_check
from connector.ccip.stl_model import StlResult, compute_residual_threshold


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
        assert result == "END"
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
        assert result == "END"
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
