"""Conditional routers (design component ``routers.py``).

The two conditional-routing functions from the architecture flowchart:

- :func:`route_after_detection` (Anomaly-Check) — after Phase 4, route to
  ``drill_down_services`` iff an account anomaly was detected, else to the
  Alert-Check router (Req 7.1, 7.2).
- :func:`route_alert_check` (Alert-Check) — route to ``dispatch_alerts`` iff an
  account anomaly was detected OR new services were seen, else to ``END``
  (Req 7.3, 7.4).

Both treat a present ``error`` in the state as "route to END" so the first
Structured_Error short-circuits the graph (Req 10.2). The labels returned here
are mapped to concrete nodes by :func:`connector.ccip.graph.build_graph`.

Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6.
"""

from __future__ import annotations

from typing import Any

__all__ = ["ROUTE_END", "ROUTE_DRILL_DOWN", "ROUTE_ALERT_CHECK", "ROUTE_DISPATCH",
           "route_after_detection", "route_alert_check"]

ROUTE_END = "END"
ROUTE_DRILL_DOWN = "drill_down_services"
ROUTE_ALERT_CHECK = "alert_check"
ROUTE_DISPATCH = "dispatch_alerts"


def route_after_detection(state: dict[str, Any]) -> str:
    """Anomaly-Check router (Req 7.1, 7.2). Error -> END (Req 10.2)."""
    if state.get("error"):
        return ROUTE_END
    if state.get("account_anomaly_detected"):
        return ROUTE_DRILL_DOWN
    return ROUTE_ALERT_CHECK


def route_alert_check(state: dict[str, Any]) -> str:
    """Alert-Check router (Req 7.3, 7.4). Error -> END (Req 10.2)."""
    if state.get("error"):
        return ROUTE_END
    if state.get("account_anomaly_detected") or state.get("new_services_detected"):
        return ROUTE_DISPATCH
    return ROUTE_END
