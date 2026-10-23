"""Conditional routers (design component ``routers.py``).

The two conditional-routing functions from the architecture flowchart:

- :func:`route_after_detection` (Anomaly-Check) — after Phase 4, route to
  ``drill_down_services`` iff an account anomaly was detected, else to the
  Alert-Check router (Req 7.1, 7.2).
- :func:`route_alert_check` (Alert-Check) — route to ``dispatch_alerts`` iff an
  account anomaly was detected OR new services were seen, else to ``END``
  (Req 7.3, 7.4).

Both treat a present ``error`` in the state as "route to ``dispatch_failure_alert``"
so the first Structured_Error short-circuits the detection/alert work but is still
surfaced to a human via the terminal failure-notification node (Req 10.2, 12.6).
:func:`route_on_error` provides the same error-first behavior for the three
sequential early-phase edges. The labels returned here are mapped to concrete
nodes by :func:`connector.ccip.graph.build_graph`.

Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 12.6.
"""

from __future__ import annotations

from typing import Any

__all__ = ["ROUTE_END", "ROUTE_DRILL_DOWN", "ROUTE_ALERT_CHECK", "ROUTE_DISPATCH",
           "ROUTE_FAILURE", "route_after_detection", "route_alert_check",
           "route_on_error"]

ROUTE_END = "END"
ROUTE_DRILL_DOWN = "drill_down_services"
ROUTE_ALERT_CHECK = "alert_check"
ROUTE_DISPATCH = "dispatch_alerts"
#: An error on the state routes to the terminal failure-dispatch node rather than
#: straight to END, so no error exit is silent (Req 12.6).
ROUTE_FAILURE = "dispatch_failure_alert"

from typing import Callable  # noqa: E402 - keep stdlib import grouped with usage


def route_on_error(ok_node: str) -> Callable[[dict[str, Any]], str]:
    """Build a linear-edge router: ``ROUTE_FAILURE`` on error, else ``ok_node``.

    Used for the early phase edges (Phases 1-3) so an error raised before the
    first conditional router still reaches ``dispatch_failure_alert`` instead of
    being carried to the router (Req 12.6).
    """

    def _route(state: dict[str, Any]) -> str:
        return ROUTE_FAILURE if state.get("error") else ok_node

    return _route


def route_after_detection(state: dict[str, Any]) -> str:
    """Anomaly-Check router (Req 7.1, 7.2). Error -> failure dispatch (Req 12.6)."""
    if state.get("error"):
        return ROUTE_FAILURE
    if state.get("account_anomaly_detected"):
        return ROUTE_DRILL_DOWN
    return ROUTE_ALERT_CHECK


def route_alert_check(state: dict[str, Any]) -> str:
    """Alert-Check router (Req 7.3, 7.4). Error -> failure dispatch (Req 12.6)."""
    if state.get("error"):
        return ROUTE_FAILURE
    if state.get("account_anomaly_detected") or state.get("new_services_detected"):
        return ROUTE_DISPATCH
    return ROUTE_END
