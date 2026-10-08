"""CCIP graph state (design component ``state.py``).

Defines :class:`CcipState` — the ``TypedDict`` LangGraph passes between nodes —
and :func:`build_initial_state`, which validates the run inputs and seeds every
key with a safe default. Errors travel *in the state* on the ``error`` key: a
node that sets ``error`` short-circuits the rest of the graph (Req 10.2), so the
first error becomes the final result unchanged.

Requirements: 2.1, 2.2, 2.3.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Optional, TypedDict

from connector.errors import ErrorCategory, structured_error

__all__ = ["CcipState", "build_initial_state", "has_error"]

_DATE_SHAPE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class CcipState(TypedDict, total=False):
    """The shared workflow state (Req 2.2).

    All keys are optional (``total=False``) so each node returns a *partial*
    update that LangGraph merges. The ``error`` channel carries a Structured_Error
    that short-circuits the graph when set (Req 10.2).
    """

    # Inputs (Req 2.1)
    account_id: str
    target_date: str  # "YYYY-MM-DD"
    # Phase 1 — the single MCP fetch happens here (decision #3)
    data_mode: str
    available_days: int
    raw_payload: dict
    # Phase 2 — pure normalization of raw_payload (no network)
    account_total_cost: float
    service_costs: dict  # service -> cost on target_date
    history_series: list  # [{"day": "YYYY-MM-DD", "cost": float}, ...] ascending
    service_history: dict  # service -> [{"day", "cost"}, ...]
    # Phase 3
    metadata: dict
    new_services_detected: list
    stl_result: Optional[dict]
    # Phase 4
    account_anomaly_detected: bool
    baseline_day: Optional[str]
    # Phase 5
    anomaly_contributors: list  # [{service, delta, seasonal_context, ...}, ...]
    # Phase 6
    alert_payload: dict
    alert_dispatched: bool
    alert_dry_run: bool
    narrative_templated: bool
    # Error channel (Req 10) — a Structured_Error dict, or None.
    error: Optional[dict]


def has_error(state: CcipState) -> bool:
    """True when the state carries a Structured_Error (short-circuit, Req 10.2)."""
    return bool(state.get("error"))


def _valid_date(value: Any) -> bool:
    if not isinstance(value, str) or not _DATE_SHAPE.match(value):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def build_initial_state(account_id: str, target_date: str) -> CcipState:
    """Seed a :class:`CcipState` from the run inputs (Req 2.1, 2.2, 2.3).

    Validates ``account_id`` (non-blank) and ``target_date`` (a real
    ``YYYY-MM-DD`` calendar date). On a bad input the returned state carries an
    ``invalid-parameter`` Structured_Error on ``error`` naming the offending
    input, so the runner can surface it before invoking the graph (Req 2.3). On
    valid input, every key is populated with a safe default.
    """
    error: Optional[dict] = None
    if not isinstance(account_id, str) or not account_id.strip():
        error = structured_error(
            "account_id is required and must be a non-empty string",
            ErrorCategory.INVALID_PARAMETER,
            field="account_id",
        )
    elif not _valid_date(target_date):
        error = structured_error(
            f"target_date must be a valid YYYY-MM-DD calendar date, got {target_date!r}",
            ErrorCategory.INVALID_PARAMETER,
            field="target_date",
            value=target_date,
        )

    return CcipState(
        account_id=account_id.strip() if isinstance(account_id, str) else account_id,
        target_date=target_date,
        data_mode="",
        available_days=0,
        raw_payload={},
        account_total_cost=0.0,
        service_costs={},
        history_series=[],
        service_history={},
        metadata={},
        new_services_detected=[],
        stl_result=None,
        account_anomaly_detected=False,
        baseline_day=None,
        anomaly_contributors=[],
        alert_payload={},
        alert_dispatched=False,
        alert_dry_run=False,
        narrative_templated=False,
        error=error,
    )
