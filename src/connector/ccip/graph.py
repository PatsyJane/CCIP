"""Graph assembly (design component ``graph.py``).

Wires the six nodes and two conditional routers into a LangGraph ``StateGraph``:

    START
      -> determine_data_mode        (Phase 1: single MCP fetch + data-mode)
      -> ingest_daily_data          (Phase 2: pure normalization)
      -> update_metadata_and_model  (Phase 3)
      -> detect_account_anomaly     (Phase 4)
      -> [Anomaly-Check router]
           True  -> drill_down_services (Phase 5) -> [Alert-Check router]
           False -> _alert_check         -> [Alert-Check router]
      -> [Alert-Check router]
           dispatch -> dispatch_alerts (Phase 6) -> END
           else     -> END

Any error on the state (from any phase or either router) routes to the terminal
``dispatch_failure_alert`` node instead of straight to END, so no failure exits
silently (Req 12.6). The early phase edges are conditional for the same reason.
``dispatch_failure_alert`` emits a best-effort failure email and ends; the
original error remains the run result.

The ``_alert_check`` pass-through node lets both the "no anomaly" branch and the
post-drill-down branch share one Alert-Check router, matching the architecture's
flowchart while respecting LangGraph's node-bound edge model (approved design
point #2).

Requirements: 2.4, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 12.6.
"""

from __future__ import annotations

from functools import partial
from typing import Any

from langgraph.graph import END, StateGraph

from .nodes import (
    CcipDeps,
    detect_account_anomaly,
    determine_data_mode,
    dispatch_alerts,
    dispatch_failure_alert,
    drill_down_services,
    ingest_daily_data,
    update_metadata_and_model,
)
from .routers import (
    ROUTE_ALERT_CHECK,
    ROUTE_DISPATCH,
    ROUTE_DRILL_DOWN,
    ROUTE_END,
    ROUTE_FAILURE,
    route_after_detection,
    route_alert_check,
    route_on_error,
)
from .state import CcipState

__all__ = ["build_graph"]

_ALERT_CHECK_NODE = "_alert_check"


def build_graph(deps: CcipDeps) -> Any:
    """Build and compile the CCIP workflow graph with ``deps`` bound to each node."""
    g = StateGraph(CcipState)

    g.add_node("determine_data_mode", partial(determine_data_mode, deps=deps))
    g.add_node("ingest_daily_data", partial(ingest_daily_data, deps=deps))
    g.add_node("update_metadata_and_model", partial(update_metadata_and_model, deps=deps))
    g.add_node("detect_account_anomaly", partial(detect_account_anomaly, deps=deps))
    g.add_node("drill_down_services", partial(drill_down_services, deps=deps))
    g.add_node("dispatch_alerts", partial(dispatch_alerts, deps=deps))
    # Terminal failure-notification node: the single sink for any error (Req 12.6).
    g.add_node("dispatch_failure_alert", partial(dispatch_failure_alert, deps=deps))
    # Pass-through node so both branches share the Alert-Check router.
    g.add_node(_ALERT_CHECK_NODE, _passthrough)

    g.set_entry_point("determine_data_mode")

    # Early phases (1-3) use conditional edges so an error raised there reaches the
    # failure-dispatch node rather than being carried to the first router (Req 12.6).
    g.add_conditional_edges(
        "determine_data_mode",
        route_on_error("ingest_daily_data"),
        {"ingest_daily_data": "ingest_daily_data", ROUTE_FAILURE: "dispatch_failure_alert"},
    )
    g.add_conditional_edges(
        "ingest_daily_data",
        route_on_error("update_metadata_and_model"),
        {"update_metadata_and_model": "update_metadata_and_model", ROUTE_FAILURE: "dispatch_failure_alert"},
    )
    g.add_conditional_edges(
        "update_metadata_and_model",
        route_on_error("detect_account_anomaly"),
        {"detect_account_anomaly": "detect_account_anomaly", ROUTE_FAILURE: "dispatch_failure_alert"},
    )

    # Anomaly-Check router (Req 7.1, 7.2); error -> failure dispatch (Req 12.6).
    g.add_conditional_edges(
        "detect_account_anomaly",
        route_after_detection,
        {
            ROUTE_DRILL_DOWN: "drill_down_services",
            ROUTE_ALERT_CHECK: _ALERT_CHECK_NODE,
            ROUTE_FAILURE: "dispatch_failure_alert",
        },
    )

    # Alert-Check router, reached from drill-down (Req 7.5) and the pass-through.
    alert_edges = {
        ROUTE_DISPATCH: "dispatch_alerts",
        ROUTE_FAILURE: "dispatch_failure_alert",
        ROUTE_END: END,
    }
    g.add_conditional_edges("drill_down_services", route_alert_check, alert_edges)
    g.add_conditional_edges(_ALERT_CHECK_NODE, route_alert_check, alert_edges)

    g.add_edge("dispatch_alerts", END)
    # Terminal: the failure node runs, then ends; state['error'] is unchanged.
    g.add_edge("dispatch_failure_alert", END)
    return g.compile()


def _passthrough(state: CcipState) -> dict:
    """No-op node: return no update so the Alert-Check router can run next."""
    return {}
