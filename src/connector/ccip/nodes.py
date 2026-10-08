"""The six LangGraph nodes (design component ``nodes.py``).

Each node is an ``async def node(state, deps) -> dict`` returning a PARTIAL state
update that LangGraph merges. Shared behavior (Req 10):

- If ``state['error']`` is already set, the node is a no-op pass-through so the
  first Structured_Error short-circuits the graph (Req 10.2).
- The node body is wrapped in a totality guard: any unhandled exception becomes an
  ``unexpected-error`` (<= 500 chars) placed on ``error`` (Req 10.3).
- Every Structured_Error is secret-scrubbed before it is placed on ``error``
  (Req 10.4).

Phases:
  1. :func:`determine_data_mode`      — the single MCP fetch + data-mode (Req 3).
  2. :func:`ingest_daily_data`        — pure normalization of the payload (Req 4).
  3. :func:`update_metadata_and_model`— new services + STL model persist (Req 5).
  4. :func:`detect_account_anomaly`   — cold-start / statistical detection (Req 6).
  5. :func:`drill_down_services`      — contributor isolation (Req 8).
  6. :func:`dispatch_alerts`          — payload + narrative + delivery (Req 9).

Requirements: 3, 4, 5, 6, 8, 9, 10.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import Any, Awaitable, Callable

from connector.errors import ErrorCategory, structured_error

from .detection import (
    detect_cold_start,
    detect_statistical,
    most_recent_non_anomaly_day,
)
from .drilldown import isolate_contributors
from .email import build_alert_payload, build_subject, deliver
from .history import classify_data_mode, count_available_days, is_above_cold_start
from .ingest import fetch_cost_payload, normalize_payload
from .metadata_store import load_metadata, save_metadata
from .models import DataMode
from .narrative import render_narrative
from .redaction import scrub_secrets
from .stl_model import run_stl

__all__ = [
    "CcipDeps",
    "determine_data_mode",
    "ingest_daily_data",
    "update_metadata_and_model",
    "detect_account_anomaly",
    "drill_down_services",
    "dispatch_alerts",
]

_MAX_ERROR_CHARS = 500


@dataclass(frozen=True)
class CcipDeps:
    """Injected dependencies shared by every node.

    ``cost_tool`` matches ``aws_cur_cost``'s signature; ``llm_client`` and
    ``email_sender`` are the narrative/delivery dependencies; ``config`` is the
    frozen :class:`connector.ccip.config.CcipConfig`.
    """

    cost_tool: Callable[..., Awaitable[dict]]
    llm_client: Any
    email_sender: Any
    config: Any


def _secrets_of(deps: CcipDeps) -> list[str]:
    secrets = getattr(deps.config, "secrets", None)
    if secrets is None:
        return []
    return [secrets.llm_api_key, secrets.smtp_password]


def _error_update(deps: CcipDeps, error: dict) -> dict:
    """Return a scrubbed ``{"error": ...}`` state update (Req 10.4)."""
    return {"error": scrub_secrets(error, _secrets_of(deps))}


def _unexpected(deps: CcipDeps, stage: str, exc: Exception) -> dict:
    message = f"an unexpected error occurred in the {stage} node: {exc}"
    return _error_update(
        deps,
        structured_error(message[:_MAX_ERROR_CHARS], ErrorCategory.UNEXPECTED_ERROR, source=stage),
    )


# ---------------------------------------------------------------------------
# Phase 1: determine_data_mode — the single MCP fetch happens here (decision #3)
# ---------------------------------------------------------------------------


async def determine_data_mode(state: dict[str, Any], deps: CcipDeps) -> dict[str, Any]:
    """Phase 1 (Req 3): single DAILY fetch, store raw payload, classify data mode."""
    if state.get("error"):
        return {}
    try:
        target = date.fromisoformat(state["target_date"][:10])
        payload = await fetch_cost_payload(
            deps.cost_tool, target, getattr(deps.config, "history_days", 90)
        )
        if isinstance(payload, dict) and "error" in payload:
            return _error_update(deps, payload)

        # Count available complete days from the fetched items (Req 3.2).
        days: set[str] = set()
        for item in payload.get("items", []):
            if isinstance(item, dict):
                ts = item.get("timestamp")
                if isinstance(ts, str) and ts:
                    days.add(ts[:10])
        available_days = len(days)
        data_mode = classify_data_mode(available_days)

        return {
            "raw_payload": payload,
            "available_days": available_days,
            "data_mode": data_mode.value,
        }
    except Exception as exc:  # noqa: BLE001 - node totality (Req 10.3).
        return _unexpected(deps, "determine_data_mode", exc)


# ---------------------------------------------------------------------------
# Phase 2: ingest_daily_data — pure normalization of raw_payload (no network)
# ---------------------------------------------------------------------------


async def ingest_daily_data(state: dict[str, Any], deps: CcipDeps) -> dict[str, Any]:
    """Phase 2 (Req 4): normalize the Phase-1 payload into account/service structures."""
    if state.get("error"):
        return {}
    try:
        normalized = normalize_payload(state.get("raw_payload", {}), state["target_date"])
        if isinstance(normalized, dict) and "error" in normalized:
            return _error_update(deps, normalized)
        return {
            "account_total_cost": normalized["account_total_cost"],
            "service_costs": normalized["service_costs"],
            "history_series": normalized["history_series"],
            "service_history": normalized["service_history"],
        }
    except Exception as exc:  # noqa: BLE001 - node totality (Req 10.3).
        return _unexpected(deps, "ingest_daily_data", exc)


# ---------------------------------------------------------------------------
# Phase 3: update_metadata_and_model
# ---------------------------------------------------------------------------


async def update_metadata_and_model(state: dict[str, Any], deps: CcipDeps) -> dict[str, Any]:
    """Phase 3 (Req 5): load metadata, detect+persist new services, run+persist STL."""
    if state.get("error"):
        return {}
    try:
        path = getattr(deps.config, "metadata_path", "ccip_metadata.json")
        metadata = load_metadata(path)
        if isinstance(metadata, dict) and "error" in metadata:
            return _error_update(deps, metadata)

        known = set(metadata.get("known_active_services") or [])
        today_services = set(state.get("service_costs") or {})
        new_services = sorted(today_services - known)

        # Immediately register new services so they are known next run (Req 5.3).
        metadata["known_active_services"] = sorted(known | today_services)

        stl_summary: dict | None = None
        data_mode = state.get("data_mode")
        if data_mode and is_above_cold_start(DataMode(data_mode)):
            stl_result = run_stl(state.get("history_series", []), deps.config)
            if isinstance(stl_result, dict) and "error" in stl_result:
                # A too-short series is not fatal: Phase 4 falls back. Record no model.
                stl_summary = None
            else:
                stl_summary = {
                    "residual_threshold": stl_result.residual_threshold,
                    "target_residual": stl_result.target_residual,
                    "method": stl_result.method,
                    "seasonal_by_weekday": stl_result.seasonal_summary,
                }
                # Persist the revised seasonal summary (Req 5.5).
                metadata.setdefault("seasonal", {})["account"] = stl_summary["seasonal_by_weekday"]

        save_error = save_metadata(path, metadata)
        if save_error is not None:
            return _error_update(deps, save_error)

        return {
            "metadata": metadata,
            "new_services_detected": new_services,
            "stl_result": stl_summary,
        }
    except Exception as exc:  # noqa: BLE001 - node totality (Req 10.3).
        return _unexpected(deps, "update_metadata_and_model", exc)


# ---------------------------------------------------------------------------
# Phase 4: detect_account_anomaly
# ---------------------------------------------------------------------------


async def detect_account_anomaly(state: dict[str, Any], deps: CcipDeps) -> dict[str, Any]:
    """Phase 4 (Req 6): cold-start absolute check or STL residual check."""
    if state.get("error"):
        return {}
    try:
        data_mode = DataMode(state["data_mode"])
        metadata = state.get("metadata", {})
        account_total = float(state.get("account_total_cost", 0.0))

        if data_mode == DataMode.COLD_START:
            detected, baseline_day = detect_cold_start(
                account_total,
                metadata,
                state["target_date"],
                float(getattr(deps.config, "cold_start_absolute_threshold", 20.0)),
            )
        else:
            # Normal/Initial/Beginner: statistical detection via STL.
            stl_result = run_stl(state.get("history_series", []), deps.config)
            if isinstance(stl_result, dict) and "error" in stl_result:
                # STL could not run (too-short series). Fall back to the cold-start
                # absolute comparison so detection has a defined answer (Req 6.6).
                detected, baseline_day = detect_cold_start(
                    account_total,
                    metadata,
                    state["target_date"],
                    float(getattr(deps.config, "cold_start_absolute_threshold", 20.0)),
                )
            else:
                detected = detect_statistical(stl_result)
                prev = most_recent_non_anomaly_day(metadata, state["target_date"])
                baseline_day = prev[0] if prev else None

        update: dict[str, Any] = {
            "account_anomaly_detected": detected,
            "baseline_day": baseline_day,
        }

        # Always persist a non-anomaly baseline day so future cold-start/drill-down
        # comparisons have a reference, even on a clean run that routes to END
        # before Phase 6 (Req 6.2, 6.6). An anomaly day is NOT recorded as a
        # baseline. The last-anomaly record is written in Phase 6 (Req 9.7).
        if not detected:
            path = getattr(deps.config, "metadata_path", "ccip_metadata.json")
            metadata = dict(metadata)
            non_anomaly = dict(metadata.get("non_anomaly_days") or {})
            non_anomaly[state["target_date"]] = account_total
            metadata["non_anomaly_days"] = non_anomaly
            save_error = save_metadata(path, metadata)
            if save_error is not None:
                return _error_update(deps, save_error)
            update["metadata"] = metadata

        return update
    except Exception as exc:  # noqa: BLE001 - node totality (Req 10.3).
        return _unexpected(deps, "detect_account_anomaly", exc)


# ---------------------------------------------------------------------------
# Phase 5: drill_down_services
# ---------------------------------------------------------------------------


async def drill_down_services(state: dict[str, Any], deps: CcipDeps) -> dict[str, Any]:
    """Phase 5 (Req 8): isolate contributors (only runs on an account anomaly)."""
    if state.get("error"):
        return {}
    try:
        if not state.get("account_anomaly_detected"):
            return {"anomaly_contributors": []}
        contributors = isolate_contributors(
            state.get("service_costs", {}),
            state.get("service_history", {}),
            state.get("baseline_day"),
            state["target_date"],
            state.get("metadata", {}),
        )
        return {"anomaly_contributors": contributors}
    except Exception as exc:  # noqa: BLE001 - node totality (Req 10.3).
        return _unexpected(deps, "drill_down_services", exc)


# ---------------------------------------------------------------------------
# Phase 6: dispatch_alerts
# ---------------------------------------------------------------------------


async def dispatch_alerts(state: dict[str, Any], deps: CcipDeps) -> dict[str, Any]:
    """Phase 6 (Req 9): build payload, render narrative, deliver, record anomaly."""
    if state.get("error"):
        return {}
    try:
        metadata = dict(state.get("metadata", {}))
        payload = build_alert_payload(state, metadata)

        narrative, templated = await render_narrative(payload, deps.llm_client)
        subject = build_subject(payload)

        delivery_error = await deliver(deps.email_sender, subject, narrative)
        if delivery_error is not None:
            return _error_update(deps, delivery_error)

        dry_run = not getattr(deps.email_sender, "can_send", False)

        # Record this run as the new last-known anomaly when one was detected
        # (Req 9.7). The non-anomaly baseline is recorded in Phase 4, which always
        # runs; Phase 6 only runs when alerting, so it owns the last-anomaly write.
        path = getattr(deps.config, "metadata_path", "ccip_metadata.json")
        if state.get("account_anomaly_detected"):
            metadata["last_anomaly"] = {
                "date": state["target_date"],
                "contributors": [c["service"] for c in state.get("anomaly_contributors", [])],
                "context": "account-level anomaly detected and alerted",
            }
            save_error = save_metadata(path, metadata)
            if save_error is not None:
                return _error_update(deps, save_error)

        payload = {**payload, "narrative": narrative, "subject": subject}
        return {
            "alert_payload": payload,
            "alert_dispatched": True,
            "alert_dry_run": dry_run,
            "narrative_templated": templated,
            "metadata": metadata,
        }
    except Exception as exc:  # noqa: BLE001 - node totality (Req 10.3).
        return _unexpected(deps, "dispatch_alerts", exc)
