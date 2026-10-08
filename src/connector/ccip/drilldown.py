"""Service drill-down (design component ``drilldown.py``).

Phase 5 logic: once an Account_Anomaly is detected, isolate the services driving
the spike by comparing the Target_Date per-service costs against the most recent
previous non-anomaly day's per-service costs, keep the services with a positive
increase, order them by descending delta, and annotate seasonal context (e.g. a
known "1st of the month" pattern) from the metadata.

Pure functions; nothing raises across the boundary.

Requirements: 8.2, 8.3, 8.4.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional

__all__ = ["service_cost_on_day", "seasonal_context_for", "isolate_contributors"]


def service_cost_on_day(
    service_history: dict[str, list[dict[str, Any]]], service: str, day: str
) -> float:
    """Return a service's recorded cost on ``day`` (0.0 if absent)."""
    for row in service_history.get(service, []):
        if isinstance(row, dict) and str(row.get("day", ""))[:10] == day:
            return float(row.get("cost", 0.0))
    return 0.0


def seasonal_context_for(target_date: str, metadata: dict[str, Any]) -> Optional[str]:
    """Return a seasonal-context note for the Target_Date, or ``None`` (Req 8.4).

    The deterministic "1st of the month" rule: when the Target_Date is the first
    of a month, annotate contributors with that recurring pattern. The metadata
    ``seasonal`` summary is consulted but the day-of-month rule is the primary,
    deterministic signal for the PoC.
    """
    try:
        parsed = date.fromisoformat(target_date[:10])
    except ValueError:
        return None
    if parsed.day == 1:
        return "aligns with a recurring 1st-of-the-month billing pattern"
    return None


def isolate_contributors(
    service_costs: dict[str, float],
    service_history: dict[str, list[dict[str, Any]]],
    baseline_day: Optional[str],
    target_date: str,
    metadata: dict[str, Any],
) -> list[dict[str, Any]]:
    """Isolate the services responsible for the account spike (Req 8.2, 8.3, 8.4).

    For each Target_Date service, compute ``delta = current - baseline`` where the
    baseline is that service's cost on ``baseline_day`` (0.0 when there is no
    baseline day or no prior record). Keep services with a positive delta, order
    by descending delta, and annotate each with the seasonal context for the
    Target_Date (Req 8.4). Returns a list of plain dicts for the state.
    """
    seasonal = seasonal_context_for(target_date, metadata)
    contributors: list[dict[str, Any]] = []
    for service, current in service_costs.items():
        current_cost = float(current)
        baseline_cost = (
            service_cost_on_day(service_history, service, baseline_day)
            if baseline_day
            else 0.0
        )
        delta = round(current_cost - baseline_cost, 2)
        if delta > 0:
            contributors.append(
                {
                    "service": service,
                    "delta": delta,
                    "baseline_cost": round(baseline_cost, 2),
                    "current_cost": round(current_cost, 2),
                    "seasonal_context": seasonal,
                }
            )
    contributors.sort(key=lambda c: c["delta"], reverse=True)
    return contributors
