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

import logging
from dataclasses import dataclass
from datetime import date
from typing import Any, Awaitable, Callable

from connector.errors import ErrorCategory, structured_error
from connector.logging_config import get_logger, log_event, log_structured_error

from .detection import (
    detect_cold_start,
    detect_statistical,
    most_recent_non_anomaly_day,
)
from .drilldown import isolate_contributors
from .email import (
    build_alert_payload,
    build_failure_payload,
    build_failure_subject,
    build_subject,
    deliver,
    render_failure_narrative,
)
from .history import classify_data_mode, count_available_days, is_above_cold_start
from .ingest import decide_fetch_window, fetch_cost_payload, normalize_payload
from .metadata_store import (
    account_metadata_path,
    load_metadata,
    merge_history,
    merge_service_history,
    save_metadata,
)
from .models import DataMode
from .narrative import render_narrative
from .redaction import scrub_secrets
from .retry import RetryPolicy
from .stl_model import run_stl

__all__ = [
    "CcipDeps",
    "determine_data_mode",
    "ingest_daily_data",
    "update_metadata_and_model",
    "detect_account_anomaly",
    "drill_down_services",
    "dispatch_alerts",
    "dispatch_failure_alert",
]

_MAX_ERROR_CHARS = 500
_WARNING_LEVEL = logging.WARNING

#: Module logger for the CCIP nodes. The JSON formatter + process-wide secret
#: registry (see ``connector.logging_config``) guarantee payloads are scrubbed.
_LOGGER = get_logger(__name__)


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
    # Separate sender for terminal failure-notification alerts, addressed to the
    # ops recipient (``email_failure_to``). Defaults to ``None`` so existing
    # construction sites keep working; the failure node falls back to a dry-run
    # when it is absent (Req 12.8).
    failure_email_sender: Any = None


def _secrets_of(deps: CcipDeps) -> list[str]:
    secrets = getattr(deps.config, "secrets", None)
    if secrets is None:
        return []
    return [secrets.llm_api_key, secrets.smtp_password]


def _account_context(deps: CcipDeps, state: dict[str, Any]) -> dict[str, Any]:
    """Safe diagnostic context shared by every error/event log for a node run."""
    return {
        "account_id": state.get("account_id"),
        "account_name": getattr(deps.config, "account_name", "") or None,
        "target_date": state.get("target_date"),
    }


def _metadata_path(deps: CcipDeps, state: dict[str, Any]) -> str:
    """Resolve the PER-ACCOUNT Metadata_Store path for this run (increment 2, Req 14).

    Derives the path from ``config.metadata_dir``, the state's ``account_id``, and
    the config's ``account_name`` so each account reads/writes only its own file.
    Falls back to the legacy single-file ``config.metadata_path`` only if the path
    cannot be resolved (defensive; should not happen with a valid account_id).
    """
    account_id = state.get("account_id") or ""
    account_name = getattr(deps.config, "account_name", "") or ""
    try:
        return account_metadata_path(deps.config, account_id, account_name)
    except Exception:  # noqa: BLE001 - never let path resolution break a node.
        return getattr(deps.config, "metadata_path", "ccip_metadata.json")


def _error_update(
    deps: CcipDeps,
    error: dict,
    *,
    state: dict[str, Any] | None = None,
    stage: str | None = None,
) -> dict:
    """Return a scrubbed ``{"error": ...}`` state update and log it (Req 10.4).

    Every Structured_Error produced at a node boundary writes a corresponding
    ERROR log carrying ``error_category`` / ``error_message`` plus safe account
    context ("Errors over exceptions at boundaries" alignment). The log, like the
    returned error, is secret-scrubbed before it is written.
    """
    scrubbed = scrub_secrets(error, _secrets_of(deps))
    context = _account_context(deps, state) if state is not None else {}
    log_structured_error(
        _LOGGER,
        scrubbed,
        event=f"ccip.{stage}.error" if stage else "ccip.node.error",
        **context,
    )
    return {"error": scrubbed}


def _unexpected(deps: CcipDeps, stage: str, exc: Exception, state: dict[str, Any]) -> dict:
    message = f"an unexpected error occurred in the {stage} node: {exc}"
    return _error_update(
        deps,
        structured_error(message[:_MAX_ERROR_CHARS], ErrorCategory.UNEXPECTED_ERROR, source=stage),
        state=state,
        stage=stage,
    )


# ---------------------------------------------------------------------------
# Phase 1: determine_data_mode — the single MCP fetch happens here (decision #3)
# ---------------------------------------------------------------------------


async def determine_data_mode(state: dict[str, Any], deps: CcipDeps) -> dict[str, Any]:
    """Phase 1 (Req 3, 13, 15, 16): load the per-account store, choose the fetch
    window (trailing vs full), run the single DAILY fetch, and classify the data
    mode from the MERGED (persisted + fetched) day set.

    Increment 2 reorders the metadata load into Phase 1 so the fetch window can be
    chosen from the persisted Rolling_History_Window (Req 16). The loaded metadata
    and the chosen :class:`FetchPlan` are carried forward on the state for Phases
    2-3 to merge/persist, avoiding a second store read.
    """
    if state.get("error"):
        return {}
    try:
        target = date.fromisoformat(state["target_date"][:10])

        # Load the PER-ACCOUNT store up front so the fetch window can be decided
        # from the persisted history (Req 14, 16). A bad store short-circuits here.
        path = _metadata_path(deps, state)
        metadata = load_metadata(path)
        if isinstance(metadata, dict) and "error" in metadata:
            return _error_update(deps, metadata, state=state, stage="determine_data_mode")

        # Decide trailing vs full window from the persisted history + config
        # (Req 16.1-16.4). ``span_days`` is the history_days value the fetch uses.
        plan = decide_fetch_window(metadata, target, deps.config)

        payload = await fetch_cost_payload(
            deps.cost_tool,
            target,
            plan.span_days,
            retry=RetryPolicy.from_config(deps.config),
        )
        if isinstance(payload, dict) and "error" in payload:
            # A fetch failure (e.g. unreachable-source) is a boundary error:
            # log it with the delivery/source diagnostics (Req 10, logging #4).
            return _error_update(deps, payload, state=state, stage="determine_data_mode")

        # Count available complete days from the MERGED day set: the persisted
        # Rolling_History_Window plus the days present in this fetch (Req 15.4). On
        # a trailing fetch the persisted days dominate; on a full/reconciliation
        # pull the fetch dominates. A reconciliation pull REPLACES history, so only
        # the fetched days count (Req 16.4).
        fetched_days: set[str] = set()
        for item in payload.get("items", []):
            if isinstance(item, dict):
                ts = item.get("timestamp")
                if isinstance(ts, str) and ts:
                    fetched_days.add(ts[:10])
        if plan.replace_history:
            merged_days = set(fetched_days)
        else:
            persisted_days = {
                str(row["day"])[:10]
                for row in (metadata.get("history_series") or [])
                if isinstance(row, dict) and row.get("day")
            }
            merged_days = persisted_days | fetched_days
        available_days = len(merged_days)
        data_mode = classify_data_mode(available_days)

        log_event(
            _LOGGER, "ccip.determine_data_mode",
            payload={
                **_account_context(deps, state),
                "available_days": available_days,
                "data_mode": data_mode.value,
                "history_days": getattr(deps.config, "history_days", 90),
                "fetch_mode": plan.mode,
                "fetch_reason": plan.reason,
                "fetch_start": plan.start,
                "fetch_end": plan.end,
                "metadata_path": path,
            },
        )
        return {
            "raw_payload": payload,
            "available_days": available_days,
            "data_mode": data_mode.value,
            "metadata": metadata,
            "fetch_plan": {
                "mode": plan.mode,
                "reason": plan.reason,
                "replace_history": plan.replace_history,
                "start": plan.start,
                "end": plan.end,
            },
        }
    except Exception as exc:  # noqa: BLE001 - node totality (Req 10.3).
        return _unexpected(deps, "determine_data_mode", exc, state)


# ---------------------------------------------------------------------------
# Phase 2: ingest_daily_data — pure normalization of raw_payload (no network)
# ---------------------------------------------------------------------------


async def ingest_daily_data(state: dict[str, Any], deps: CcipDeps) -> dict[str, Any]:
    """Phase 2 (Req 4, 15, 16): normalize the Phase-1 payload, then MERGE it into the
    persisted Rolling_History_Window so later phases see the full window.

    Normalization is pure (no network). The fetched window may be only a small
    trailing span (Req 16.1); increment 2 merges those days into the persisted
    ``history_series`` / ``service_history`` (idempotent per-day overwrite, trimmed
    to ``history_days``, Req 15.2-15.3) and exposes the MERGED series on the state.
    A reconciliation pull REPLACES rather than merges (Req 16.4). The Target_Date
    ``account_total_cost`` / ``service_costs`` always come from the fetch, since the
    window always ends at the Target_Date (Req 13.3).
    """
    if state.get("error"):
        return {}
    try:
        normalized = normalize_payload(state.get("raw_payload", {}), state["target_date"])
        if isinstance(normalized, dict) and "error" in normalized:
            return _error_update(deps, normalized, state=state, stage="ingest_daily_data")

        history_days = int(getattr(deps.config, "history_days", 90))
        plan = state.get("fetch_plan") or {}
        replace = bool(plan.get("replace_history"))
        metadata = state.get("metadata") or {}

        fetched_history = normalized["history_series"]
        fetched_service_history = normalized["service_history"]

        if replace:
            # Reconciliation: the full pull is authoritative; discard prior drift.
            merged_history = merge_history([], fetched_history, history_days)
            merged_service_history = merge_service_history(
                {}, fetched_service_history, history_days
            )
        else:
            merged_history = merge_history(
                metadata.get("history_series") or [], fetched_history, history_days
            )
            merged_service_history = merge_service_history(
                metadata.get("service_history") or {}, fetched_service_history, history_days
            )

        return {
            "account_total_cost": normalized["account_total_cost"],
            "service_costs": normalized["service_costs"],
            "history_series": merged_history,
            "service_history": merged_service_history,
        }
    except Exception as exc:  # noqa: BLE001 - node totality (Req 10.3).
        return _unexpected(deps, "ingest_daily_data", exc, state)


# ---------------------------------------------------------------------------
# Phase 3: update_metadata_and_model
# ---------------------------------------------------------------------------


async def update_metadata_and_model(state: dict[str, Any], deps: CcipDeps) -> dict[str, Any]:
    """Phase 3 (Req 5, 14, 15, 16): persist the merged Rolling_History_Window + new
    services + STL to the PER-ACCOUNT store.

    Increment 2: the store was already loaded in Phase 1 (carried on
    ``state['metadata']``); reuse it instead of a second read. The merged
    ``history_series`` / ``service_history`` from Phase 2 are persisted here, and a
    FULL pull stamps ``last_full_fetch_date`` so the reconciliation clock advances
    (Req 16.4). All writes go to the per-account path (Req 14).
    """
    if state.get("error"):
        return {}
    try:
        path = _metadata_path(deps, state)
        # Reuse the store loaded in Phase 1 (Req 16 reorder); fall back to a fresh
        # load only if it is somehow absent from the state (defensive).
        metadata = state.get("metadata")
        if not isinstance(metadata, dict) or not metadata:
            metadata = load_metadata(path)
            if isinstance(metadata, dict) and "error" in metadata:
                return _error_update(deps, metadata, state=state, stage="update_metadata_and_model")
        else:
            metadata = dict(metadata)  # copy before mutating

        # Persist the merged Rolling_History_Window produced in Phase 2 (Req 15.6).
        metadata["history_series"] = state.get("history_series") or []
        metadata["service_history"] = state.get("service_history") or {}
        # Advance the reconciliation clock on any FULL pull (Req 16.4).
        plan = state.get("fetch_plan") or {}
        if plan.get("mode") == "full":
            metadata["last_full_fetch_date"] = state["target_date"][:10]

        known = set(metadata.get("known_active_services") or [])
        # New-service detection is sourced from the full 90-day history window
        # (``service_history``), NOT the Target_Date-only ``service_costs``
        # (Req 5.2). A service is "new" the first time it appears anywhere in the
        # window; sourcing from the window means an existing service that is
        # idle ($0 / no line item) on the Target_Date is still recognized and
        # registered, instead of being re-flagged as new on a later run.
        #
        # ``UNKNOWN`` is the dimension-tagging sentinel for a missing/blank
        # service tag (see ``connector.dimensions``); it is a collapsed bucket,
        # not a real service identity, so it is excluded from both the new-service
        # signal and the persisted known set to avoid spurious "new service"
        # alerts (Req 5.2, 5.3).
        window_services = {
            s for s in (state.get("service_history") or {}) if s != "UNKNOWN"
        }
        new_services = sorted(window_services - known)

        # Immediately register new services so they are known next run (Req 5.3).
        metadata["known_active_services"] = sorted(known | window_services)

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
            return _error_update(deps, save_error, state=state, stage="update_metadata_and_model")

        log_event(
            _LOGGER, "ccip.update_metadata_and_model",
            payload={
                **_account_context(deps, state),
                "data_mode": data_mode,
                "new_services_detected": new_services,
                "stl_model_available": stl_summary is not None,
                "residual_threshold": (stl_summary or {}).get("residual_threshold"),
                "fetch_mode": plan.get("mode"),
                "fetch_reason": plan.get("reason"),
                "persisted_history_days": len(metadata.get("history_series") or []),
                "metadata_path": path,
            },
        )
        return {
            "metadata": metadata,
            "new_services_detected": new_services,
            "stl_result": stl_summary,
        }
    except Exception as exc:  # noqa: BLE001 - node totality (Req 10.3).
        return _unexpected(deps, "update_metadata_and_model", exc, state)


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
        cold_start_threshold = float(
            getattr(deps.config, "cold_start_absolute_threshold", 20.0)
        )

        # Detection method captured for the log so the exact deterministic math
        # that produced the verdict is reconstructable (logging requirement #2).
        detection_method: str  # "cold-start-absolute" | "stl-residual"
        expected_cost: float | None = None  # baseline total (cold-start branch)
        actual_residual: float | None = None  # STL target residual (stat branch)
        residual_threshold: float | None = None  # STL residual threshold

        if data_mode == DataMode.COLD_START:
            detection_method = "cold-start-absolute"
            detected, baseline_day = detect_cold_start(
                account_total,
                metadata,
                state["target_date"],
                cold_start_threshold,
            )
            prev = most_recent_non_anomaly_day(metadata, state["target_date"])
            expected_cost = prev[1] if prev else None
        else:
            # Normal/Initial/Beginner: statistical detection via STL.
            stl_result = run_stl(state.get("history_series", []), deps.config)
            if isinstance(stl_result, dict) and "error" in stl_result:
                # STL could not run (too-short series). Fall back to the cold-start
                # absolute comparison so detection has a defined answer (Req 6.6).
                detection_method = "cold-start-absolute"
                detected, baseline_day = detect_cold_start(
                    account_total,
                    metadata,
                    state["target_date"],
                    cold_start_threshold,
                )
                prev = most_recent_non_anomaly_day(metadata, state["target_date"])
                expected_cost = prev[1] if prev else None
            else:
                detection_method = "stl-residual"
                detected = detect_statistical(stl_result)
                actual_residual = stl_result.target_residual
                residual_threshold = stl_result.residual_threshold
                prev = most_recent_non_anomaly_day(metadata, state["target_date"])
                baseline_day = prev[0] if prev else None
                expected_cost = prev[1] if prev else None

        # Capture the exact math/state used in the evaluation (logging #2). The
        # formatter scrubs secrets; none of these are sensitive.
        log_event(
            _LOGGER, "ccip.detect_account_anomaly",
            payload={
                "account_id": state.get("account_id"),
                "account_name": getattr(deps.config, "account_name", "") or None,
                "target_date": state.get("target_date"),
                "data_mode": data_mode.value,
                "detection_method": detection_method,
                "actual_cost": account_total,
                "expected_cost": expected_cost,
                "actual_residual": actual_residual,
                "residual_threshold": residual_threshold,
                "cold_start_absolute_threshold": cold_start_threshold,
                "baseline_day": baseline_day,
                "anomaly_threshold_breached": bool(detected),
            },
        )

        update: dict[str, Any] = {
            "account_anomaly_detected": detected,
            "baseline_day": baseline_day,
        }

        # Always persist a non-anomaly baseline day so future cold-start/drill-down
        # comparisons have a reference, even on a clean run that routes to END
        # before Phase 6 (Req 6.2, 6.6). An anomaly day is NOT recorded as a
        # baseline. The last-anomaly record is written in Phase 6 (Req 9.7).
        if not detected:
            path = _metadata_path(deps, state)
            metadata = dict(metadata)
            non_anomaly = dict(metadata.get("non_anomaly_days") or {})
            non_anomaly[state["target_date"]] = account_total
            metadata["non_anomaly_days"] = non_anomaly
            save_error = save_metadata(path, metadata)
            if save_error is not None:
                return _error_update(deps, save_error, state=state, stage="detect_account_anomaly")
            update["metadata"] = metadata

        return update
    except Exception as exc:  # noqa: BLE001 - node totality (Req 10.3).
        return _unexpected(deps, "detect_account_anomaly", exc, state)


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
        return _unexpected(deps, "drill_down_services", exc, state)


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

        dry_run = not getattr(deps.email_sender, "can_send", False)

        delivery_error = await deliver(deps.email_sender, subject, narrative)
        if delivery_error is not None:
            # Alert dispatch failure: log an ERROR aligned with the
            # Structured_Error, carrying safe delivery diagnostics + a redacted
            # SMTP config snapshot (never host password — scrubbed anyway).
            return _error_update(
                deps,
                delivery_error,
                state=state,
                stage="dispatch_alerts",
            )

        # Record this run as the new last-known anomaly when one was detected
        # (Req 9.7). The non-anomaly baseline is recorded in Phase 4, which always
        # runs; Phase 6 only runs when alerting, so it owns the last-anomaly write.
        path = _metadata_path(deps, state)
        if state.get("account_anomaly_detected"):
            metadata["last_anomaly"] = {
                "date": state["target_date"],
                "contributors": [c["service"] for c in state.get("anomaly_contributors", [])],
                "context": "account-level anomaly detected and alerted",
            }
            save_error = save_metadata(path, metadata)
            if save_error is not None:
                return _error_update(deps, save_error, state=state, stage="dispatch_alerts")

        log_event(
            _LOGGER, "ccip.dispatch_alerts",
            payload={
                **_account_context(deps, state),
                "data_mode": state.get("data_mode"),
                "account_anomaly_detected": bool(state.get("account_anomaly_detected")),
                "new_services_detected": list(state.get("new_services_detected") or []),
                "contributor_count": len(state.get("anomaly_contributors") or []),
                "delivery_method": "smtp" if not dry_run else "dry-run",
                "alert_dry_run": dry_run,
                "narrative_templated": templated,
                "subject": subject,
            },
        )

        payload = {**payload, "narrative": narrative, "subject": subject}
        return {
            "alert_payload": payload,
            "alert_dispatched": True,
            "alert_dry_run": dry_run,
            "narrative_templated": templated,
            "metadata": metadata,
        }
    except Exception as exc:  # noqa: BLE001 - node totality (Req 10.3).
        return _unexpected(deps, "dispatch_alerts", exc, state)


# ---------------------------------------------------------------------------
# Terminal failure dispatch — the one node that runs BECAUSE an error is set
# ---------------------------------------------------------------------------


async def dispatch_failure_alert(state: dict[str, Any], deps: CcipDeps) -> dict[str, Any]:
    """Terminal node: email a deterministic failure alert when the run errored (Req 12.6-12.9).

    Unlike the six phase nodes (which short-circuit to a no-op when
    ``state['error']`` is set), this node does its work precisely because an error
    is present. It:

    - is a no-op when there is NO error (defensive; it should only be routed to on
      an error);
    - builds a DETERMINISTIC failure payload WITHOUT the LLM (Req 12.7), since the
      LLM may itself be the unreachable dependency;
    - re-scrubs the rendered alert against the configured secrets (Req 12.11);
    - delivers best-effort via the SEPARATE ``failure_email_sender`` addressed to
      the ops recipient; a missing sender or unconfigured SMTP dry-runs (Req 12.8);
    - NEVER clears or mutates ``state['error']`` so the original error remains the
      run result (Req 12.6), and NEVER re-raises or re-enters failure handling even
      if the failure send itself fails (best-effort, non-recursive, Req 12.9).

    Returns only ``failure_alert_dispatched`` / ``failure_alert_dry_run`` flags;
    the ``error`` channel is deliberately left untouched.
    """
    error = state.get("error")
    if not error:
        return {}

    sender = deps.failure_email_sender
    context = _account_context(deps, state)

    try:
        payload = build_failure_payload(state, error)
        subject = build_failure_subject(payload)
        body = render_failure_narrative(payload)

        # Defense-in-depth: re-scrub the outgoing subject/body (Req 12.11). The
        # error on the state is already scrubbed, but the rendered strings are
        # freshly assembled here.
        secrets = _secrets_of(deps)
        scrubbed = scrub_secrets({"subject": subject, "body": body}, secrets)
        subject, body = scrubbed["subject"], scrubbed["body"]

        if sender is None:
            # No failure sender wired: safe dry run (Req 12.8).
            log_event(
                _LOGGER, "ccip.dispatch_failure_alert",
                payload={
                    **context,
                    "error_category": error.get("category"),
                    "delivery_method": "dry-run",
                    "failure_alert_dry_run": True,
                    "reason": "no failure sender configured",
                    "subject": subject,
                },
            )
            return {"failure_alert_dispatched": False, "failure_alert_dry_run": True}

        dry_run = not getattr(sender, "can_send", False)
        delivery_error = await deliver(sender, subject, body)

        if delivery_error is not None:
            # Best-effort + non-recursive: log the delivery failure and stop. Do
            # NOT return it on the error channel (that would overwrite the real
            # failure) and do NOT retry (Req 12.9).
            log_structured_error(
                _LOGGER,
                scrub_secrets(delivery_error, _secrets_of(deps)),
                event="ccip.dispatch_failure_alert.delivery_failed",
                original_error_category=error.get("category"),
                **context,
            )
            return {"failure_alert_dispatched": False, "failure_alert_dry_run": dry_run}

        log_event(
            _LOGGER, "ccip.dispatch_failure_alert",
            payload={
                **context,
                "error_category": error.get("category"),
                "delivery_method": "smtp" if not dry_run else "dry-run",
                "failure_alert_dry_run": dry_run,
                "subject": subject,
            },
        )
        return {"failure_alert_dispatched": not dry_run, "failure_alert_dry_run": dry_run}
    except Exception as exc:  # noqa: BLE001 - must never raise; non-recursive.
        # Even an unexpected error in failure handling must not crash the run or
        # loop back (Req 12.9). Log and leave state['error'] intact.
        log_event(
            _LOGGER, "ccip.dispatch_failure_alert.unexpected",
            level=_WARNING_LEVEL,
            payload={**context, "detail": str(exc)[:_MAX_ERROR_CHARS]},
        )
        return {"failure_alert_dispatched": False, "failure_alert_dry_run": True}
