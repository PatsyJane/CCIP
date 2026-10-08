"""Ingestion stage (design component ``ingest.py``).

Per the approved architecture (decision #3), the single MCP call per run happens
in **Phase 1**; this module provides both:

- :func:`fetch_cost_payload` — Phase 1's one ``DAILY`` ``aws_cur_cost`` call over
  the 90-day window ending at the Target_Date, returning the raw success payload
  (``{granularity, items}``) for Phase 1 to store on the state. It preserves a
  Cost_Tool error's category, rejects non-``DAILY`` granularity, and treats zero
  items as ``missing-data`` (Req 3.1, 3.3, 3.4, 3.5).
- :func:`normalize_payload` — Phase 2's PURE normalization of that raw payload
  into the standardized account/service structures, performing NO network call
  (Req 4.1, 4.2, 4.3).

Nothing raises across either boundary.

Requirements: 3.1, 3.3, 3.4, 3.5, 4.1, 4.2, 4.3.
"""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Awaitable, Callable, Union

from connector.errors import ErrorCategory, structured_error

__all__ = [
    "MAX_HISTORY_DAYS",
    "INGEST_STAGE",
    "CostTool",
    "fetch_cost_payload",
    "normalize_payload",
]

#: The single-fetch window length (days) ending at the Target_Date (Req 3.1).
MAX_HISTORY_DAYS: int = 90

#: Source label attached to Structured_Errors from this stage.
INGEST_STAGE: str = "ingest"

#: The injected Cost_Tool matches ``aws_cur_cost``'s agent-facing signature.
CostTool = Callable[..., Awaitable[dict]]


async def fetch_cost_payload(
    cost_tool: CostTool,
    target_date: date,
    history_days: int = MAX_HISTORY_DAYS,
) -> Union[dict[str, Any], dict]:
    """Phase 1's single DAILY MCP fetch over the history window (Req 3.1).

    Calls ``cost_tool(start, end, "DAILY")`` where ``start = target_date -
    history_days`` and ``end = target_date``, each ``YYYY-MM-DD``. Returns the raw
    ``aws_cur_cost`` success payload on success, otherwise a Structured_Error:

    - a Cost_Tool error has its ``category`` preserved and the ingest stage named
      as source (Req 3.3);
    - a non-``DAILY`` granularity -> ``invalid-parameter`` (Req 3.4);
    - zero items -> ``missing-data`` (Req 3.5).

    Never raises.
    """
    try:
        start = (target_date - timedelta(days=history_days)).isoformat()
        end = target_date.isoformat()
        payload = await cost_tool(start, end, "DAILY")

        if isinstance(payload, dict) and "error" in payload:
            category_value = payload.get("category", ErrorCategory.UNEXPECTED_ERROR.value)
            try:
                category = ErrorCategory(category_value)
            except ValueError:
                category = ErrorCategory.UNEXPECTED_ERROR
            return structured_error(
                f"cost tool failed during ingestion: {payload.get('error', '')}",
                category,
                source=INGEST_STAGE,
            )

        if not isinstance(payload, dict):
            return structured_error(
                "cost tool returned a non-dict payload",
                ErrorCategory.UNEXPECTED_ERROR,
                source=INGEST_STAGE,
            )

        granularity = payload.get("granularity")
        if granularity != "DAILY":
            return structured_error(
                f"expected DAILY granularity from the cost tool but received {granularity!r}",
                ErrorCategory.INVALID_PARAMETER,
                source=INGEST_STAGE,
                field="granularity",
                value=granularity,
            )

        items = payload.get("items")
        if not isinstance(items, list) or len(items) == 0:
            return structured_error(
                "the cost tool returned an empty result set for the history window",
                ErrorCategory.MISSING_DATA,
                source=INGEST_STAGE,
            )

        return payload
    except Exception:  # noqa: BLE001 - stage totality: never raise.
        return structured_error(
            "an unexpected error occurred during the cost fetch",
            ErrorCategory.UNEXPECTED_ERROR,
            source=INGEST_STAGE,
        )


def _service_of(item: dict[str, Any]) -> str:
    """Return the ``service`` dimension tag of a Data_Item (``UNKNOWN`` if absent)."""
    tags = item.get("tags")
    if isinstance(tags, dict):
        service = tags.get("service")
        if isinstance(service, str) and service:
            return service
    return "UNKNOWN"


def _day_of(item: dict[str, Any]) -> str:
    """Return the calendar-day portion of a Data_Item timestamp (empty if absent)."""
    timestamp = item.get("timestamp")
    if isinstance(timestamp, str) and timestamp:
        return timestamp[:10]
    return ""


def normalize_payload(
    raw_payload: dict[str, Any], target_date: str
) -> Union[dict[str, Any], dict]:
    """Phase 2's PURE normalization of the Phase-1 payload — no network (Req 4.1).

    Reads ``raw_payload['items']`` (the ``aws_cur_cost`` success items) and builds:

    - ``account_total_cost`` — sum of per-service costs on the Target_Date
      (``0.00`` when the Target_Date has no items, imputed);
    - ``service_costs`` — ``{service: cost}`` for the Target_Date;
    - ``history_series`` — ``[{"day", "cost"}]`` of the account total per day,
      ascending;
    - ``service_history`` — ``{service: [{"day", "cost"}]}`` ascending.

    An absent/malformed payload returns an ``unexpected-error`` naming the stage
    (Req 4.3). Never raises.
    """
    try:
        if not isinstance(raw_payload, dict) or not isinstance(raw_payload.get("items"), list):
            return structured_error(
                "the raw cost payload is absent or malformed in the state",
                ErrorCategory.UNEXPECTED_ERROR,
                source=INGEST_STAGE,
            )

        items = raw_payload["items"]
        target = target_date[:10]

        # Aggregate per (service, day) and per day.
        service_day: dict[str, dict[str, float]] = {}
        account_day: dict[str, float] = {}
        service_costs: dict[str, float] = {}

        for item in items:
            if not isinstance(item, dict):
                continue
            day = _day_of(item)
            if not day:
                continue
            service = _service_of(item)
            try:
                cost = float(item.get("cost_value", 0.0))
            except (TypeError, ValueError):
                continue

            service_day.setdefault(service, {})
            service_day[service][day] = service_day[service].get(day, 0.0) + cost
            account_day[day] = account_day.get(day, 0.0) + cost
            if day == target:
                service_costs[service] = round(service_costs.get(service, 0.0) + cost, 2)

        history_series = [
            {"day": day, "cost": round(total, 2)}
            for day, total in sorted(account_day.items())
        ]
        service_history = {
            service: [
                {"day": day, "cost": round(cost, 2)}
                for day, cost in sorted(day_map.items())
            ]
            for service, day_map in service_day.items()
        }
        account_total_cost = round(account_day.get(target, 0.0), 2)

        return {
            "account_total_cost": account_total_cost,
            "service_costs": service_costs,
            "history_series": history_series,
            "service_history": service_history,
        }
    except Exception:  # noqa: BLE001 - stage totality: never raise.
        return structured_error(
            "an unexpected error occurred while normalizing the cost payload",
            ErrorCategory.UNEXPECTED_ERROR,
            source=INGEST_STAGE,
        )
