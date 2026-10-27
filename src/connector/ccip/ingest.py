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

from dataclasses import dataclass
from datetime import date, timedelta
from typing import Any, Awaitable, Callable, Optional, Union

from connector.errors import ErrorCategory, structured_error
from connector.logging_config import get_logger, log_event

from .retry import RetryPolicy, is_retryable, retry_async

__all__ = [
    "MAX_HISTORY_DAYS",
    "INGEST_STAGE",
    "CostTool",
    "FetchPlan",
    "decide_fetch_window",
    "fetch_cost_payload",
    "normalize_payload",
]

#: Module logger for the ingestion stage (JSON formatter scrubs secrets).
_LOGGER = get_logger(__name__)

#: The single-fetch window length (days) ending at the Target_Date (Req 3.1).
MAX_HISTORY_DAYS: int = 90

#: Source label attached to Structured_Errors from this stage.
INGEST_STAGE: str = "ingest"

#: The injected Cost_Tool matches ``aws_cur_cost``'s agent-facing signature.
CostTool = Callable[..., Awaitable[dict]]


@dataclass(frozen=True)
class FetchPlan:
    """The decided Phase-1 fetch window (increment 2, Req 13, 16).

    ``start``/``end`` are the inclusive ``YYYY-MM-DD`` bounds (``end`` is always the
    Target_Date, Req 13.3). ``span_days`` is ``(end - start).days`` — the
    ``history_days`` value passed to :func:`fetch_cost_payload`. ``mode`` is
    ``"full"`` or ``"trailing"``; ``reason`` is why (``"cold-start"`` / ``"gap"`` /
    ``"reconciliation"`` / ``"steady-state"``). ``replace_history`` is True only for
    a reconciliation FULL pull, which REPLACES the persisted Rolling_History_Window
    instead of merging into it (Req 16.4).
    """

    start: str
    end: str
    span_days: int
    mode: str  # "full" | "trailing"
    reason: str  # "cold-start" | "gap" | "reconciliation" | "steady-state"
    replace_history: bool


def _latest_history_day(account_history: list[dict[str, Any]]) -> Optional[str]:
    """Return the most recent ``YYYY-MM-DD`` in a persisted history series, or None."""
    days = [
        str(row["day"])[:10]
        for row in (account_history or [])
        if isinstance(row, dict) and row.get("day")
    ]
    return max(days) if days else None


def decide_fetch_window(
    metadata: dict[str, Any], target_date: date, config: Any
) -> FetchPlan:
    """Choose the Phase-1 fetch window from the persisted store + config (Req 16).

    The window END is always the Target_Date (Req 13.3). The rule, in priority
    order, decides a FULL ``history_days`` pull vs a small TRAILING window:

    1. persisted history has ``< history_days`` distinct days -> FULL (cold-start,
       Req 16.2);
    2. latest persisted day ``< target_date - 1`` (a gap a trailing window cannot
       bridge) -> FULL (gap, Req 16.3);
    3. the reconciliation interval has elapsed since ``last_full_fetch_date`` ->
       FULL that REPLACES the persisted history (reconciliation, Req 16.4);
    4. otherwise -> TRAILING of ``trailing_fetch_days`` days (steady-state,
       Req 16.1).

    Pure and deterministic over ``(metadata, target_date, config)`` so it is
    directly unit/property-testable (Property 25).
    """
    history_days = int(getattr(config, "history_days", 90))
    trailing_days = max(1, int(getattr(config, "trailing_fetch_days", 2)))
    reconcile_interval = int(getattr(config, "reconciliation_interval_days", 7))

    account_history = metadata.get("account_history") or []
    distinct_days = {
        str(row["day"])[:10]
        for row in account_history
        if isinstance(row, dict) and row.get("day")
    }
    latest_day = _latest_history_day(account_history)

    def _full(reason: str, replace: bool = False) -> FetchPlan:
        start = (target_date - timedelta(days=history_days)).isoformat()
        return FetchPlan(
            start=start,
            end=target_date.isoformat(),
            span_days=history_days,
            mode="full",
            reason=reason,
            replace_history=replace,
        )

    # 1. Cold start for this account: not enough persisted history yet.
    if len(distinct_days) < history_days:
        return _full("cold-start")

    # 2. Gap the trailing window could not bridge (latest persisted day is older
    #    than the day before the Target_Date).
    if latest_day is not None and latest_day < (target_date - timedelta(days=1)).isoformat():
        return _full("gap")

    # 3. Periodic reconciliation: a full pull that REPLACES the persisted history.
    last_full = metadata.get("last_full_fetch_date")
    due_for_reconcile = True
    if isinstance(last_full, str) and last_full:
        try:
            last_full_date = date.fromisoformat(last_full[:10])
            due_for_reconcile = (target_date - last_full_date).days >= reconcile_interval
        except ValueError:
            due_for_reconcile = True
    if due_for_reconcile:
        return _full("reconciliation", replace=True)

    # 4. Steady state: a small trailing window merged into the persisted history.
    start = (target_date - timedelta(days=trailing_days - 1)).isoformat()
    return FetchPlan(
        start=start,
        end=target_date.isoformat(),
        span_days=trailing_days - 1,
        mode="trailing",
        reason="steady-state",
        replace_history=False,
    )


async def _fetch_once(
    cost_tool: CostTool, target_date: date, history_days: int
) -> Union[dict[str, Any], dict]:
    """One attempt of the DAILY fetch + its deterministic validation.

    Returns the raw success payload, or a Structured_Error. A Cost_Tool error has
    its ``category`` PRESERVED (so an ``unreachable-source`` stays retryable while
    ``invalid-parameter`` / ``missing-data`` do not); non-``DAILY`` ->
    ``invalid-parameter`` (Req 3.4); zero items -> ``missing-data`` (Req 3.5).
    Deterministic outcomes carry a non-retryable category so a retry wrapper stops
    immediately on them. Never raises.
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


async def fetch_cost_payload(
    cost_tool: CostTool,
    target_date: date,
    history_days: int = MAX_HISTORY_DAYS,
    retry: Optional[RetryPolicy] = None,
) -> Union[dict[str, Any], dict]:
    """Phase 1's single DAILY MCP fetch over the history window (Req 3.1).

    Calls ``cost_tool(start, end, "DAILY")`` where ``start = target_date -
    history_days`` and ``end = target_date``, each ``YYYY-MM-DD``. Returns the raw
    ``aws_cur_cost`` success payload on success, otherwise a Structured_Error:

    - a Cost_Tool error has its ``category`` preserved and the ingest stage named
      as source (Req 3.3);
    - a non-``DAILY`` granularity -> ``invalid-parameter`` (Req 3.4);
    - zero items -> ``missing-data`` (Req 3.5).

    When a :class:`RetryPolicy` is supplied, the single logical fetch is retried
    on an ``unreachable-source`` failure using bounded exponential backoff with
    jitter (Req 12.1-12.3). Deterministic categories are returned on the first
    occurrence, never retried (Req 12.2). On exhaustion the LAST
    ``unreachable-source`` error is returned with an ``attempts`` field recording
    how many attempts were made (Req 12.5). Never raises.
    """

    async def _operation() -> Union[dict[str, Any], dict]:
        return await _fetch_once(cost_tool, target_date, history_days)

    if retry is None or retry.max_attempts <= 1:
        return await _operation()

    def _on_retry(attempt: int, remaining: int, result: Any, delay: float) -> None:
        # Req 12.4 — WARNING naming attempt/remaining/category/delay. The JSON
        # formatter scrubs secrets; none of these fields are sensitive.
        import logging

        log_event(
            _LOGGER, "ccip.ingest.fetch_retry",
            level=logging.WARNING,
            payload={
                "attempt": attempt,
                "remaining_attempts": remaining,
                "error_category": result.get("category") if isinstance(result, dict) else None,
                "backoff_delay_seconds": round(delay, 3),
                "target_date": target_date.isoformat(),
            },
        )

    result = await retry_async(
        _operation, retry, is_retryable=is_retryable, on_retry=_on_retry
    )
    # Stamp how many attempts were made onto an exhausted transient error so the
    # failure alert can report it (Req 12.5). A success / deterministic error is
    # returned unchanged.
    if is_retryable(result) and isinstance(result, dict):
        result = {**result, "attempts": retry.max_attempts}
    return result


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
    - ``account_history`` — ``[{"day", "cost"}]`` of the account total per day,
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

        account_history = [
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
            "account_history": account_history,
            "service_history": service_history,
        }
    except Exception:  # noqa: BLE001 - stage totality: never raise.
        return structured_error(
            "an unexpected error occurred while normalizing the cost payload",
            ErrorCategory.UNEXPECTED_ERROR,
            source=INGEST_STAGE,
        )
