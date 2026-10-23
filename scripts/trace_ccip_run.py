#!/usr/bin/env python
"""Phase-by-phase trace runner for the CCIP LangGraph anomaly workflow.

Unlike ``run_ccip_check.py`` (which prints only the final result), this script
runs the six phases IN SEQUENCE and prints, for each phase, the inputs it reads,
the outputs it writes onto the shared state, and the key intermediate
calculations (data-mode counting, STL decomposition + residual threshold math,
the detection comparison, and the per-service drill-down deltas).

It reuses the REAL dependencies via ``build_live_deps`` (real AWS CUR Cost_Tool +
real Gemini + SMTP), but forces the email stage into a safe DRY RUN so no email
is sent while tracing. It does NOT modify the production nodes; it simply invokes
them one at a time and inspects the state between phases.

Usage:
    python scripts/trace_ccip_run.py --account-id 647096474516 --target-date 2026-10-05
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
from datetime import date, datetime

_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from connector.ccip.config import ConfigError, load_ccip_config  # noqa: E402
from connector.ccip.detection import most_recent_non_anomaly_day  # noqa: E402
from connector.ccip.ingest import decide_fetch_window  # noqa: E402
from connector.ccip.nodes import (  # noqa: E402
    detect_account_anomaly,
    determine_data_mode,
    dispatch_alerts,
    drill_down_services,
    ingest_daily_data,
    update_metadata_and_model,
)
from connector.ccip.runner import build_live_deps  # noqa: E402
from connector.ccip.state import build_initial_state  # noqa: E402
from connector.ccip.stl_model import run_stl  # noqa: E402
from connector.logging_config import configure_logging  # noqa: E402

_RULE = "=" * 78
_SUB = "-" * 78


def _hdr(title: str) -> None:
    print(f"\n{_RULE}\n{title}\n{_RULE}")


def _kv(label: str, value) -> None:
    print(f"  {label:<26}: {value}")


def _preview_series(series, n: int = 5):
    """Show the first and last n rows of a day/cost series compactly."""
    if not series:
        return "(empty)"
    head = series[:n]
    tail = series[-n:]
    fmt = lambda rows: ", ".join(f"{r['day']}={r['cost']}" for r in rows)
    if len(series) <= 2 * n:
        return fmt(series)
    return f"{fmt(head)}  ...  {fmt(tail)}"


def _merge_error(state) -> bool:
    if state.get("error"):
        _hdr("RUN HALTED BY A STRUCTURED_ERROR")
        print(f"  {state['error']}")
        return True
    return False


async def trace(account_id: str, target_date: str, config_path: str) -> int:
    try:
        config = load_ccip_config(config_path)
    except ConfigError as exc:
        print(f"Cannot start: {exc}")
        return 2

    # Register secrets with the logger so nothing leaks, but keep logging quiet so
    # the phase prints are the star of the show.
    secrets = config.secrets
    secret_values = [secrets.llm_api_key, secrets.smtp_password] if secrets else []
    configure_logging("WARNING", secrets=secret_values)

    # Real deps, but force the email stage into a safe dry run while tracing.
    deps, http = build_live_deps(config, force_email_dry_run=True)

    try:
        # ---- INPUT -------------------------------------------------------
        _hdr("INPUT")
        _kv("account_id", account_id)
        _kv("account_name (config)", config.account_name or "(blank)")
        _kv("target_date", target_date)
        _kv("history_days", config.history_days)
        _kv("trailing_fetch_days", config.trailing_fetch_days)
        _kv("reconciliation_interval", config.reconciliation_interval_days)
        _kv("statistical_method", config.statistical_method)
        _kv("iqr_factor / sigma_factor", f"{config.iqr_factor} / {config.sigma_factor}")
        _kv("stl period / seasonal", f"{config.stl_period} / {config.stl_seasonal}")
        _kv("cold_start_abs_threshold", config.cold_start_absolute_threshold)

        state = build_initial_state(account_id, target_date)
        if _merge_error(state):
            return 1

        # ---- PHASE 1 -----------------------------------------------------
        _hdr("PHASE 1 - determine_data_mode (the single MCP fetch)")
        # Show the fetch-window decision BEFORE running the node.
        from connector.ccip.metadata_store import account_metadata_path, load_metadata

        path = account_metadata_path(config, account_id, config.account_name)
        pre_md = load_metadata(path)
        persisted = len(pre_md.get("history_series") or []) if isinstance(pre_md, dict) else 0
        plan = decide_fetch_window(pre_md if isinstance(pre_md, dict) else {},
                                   date.fromisoformat(target_date[:10]), config)
        print("  Fetch-window decision (from persisted store + config):")
        _kv("    per-account store", path)
        _kv("    persisted history days", persisted)
        _kv("    fetch mode", plan.mode)
        _kv("    fetch reason", plan.reason)
        _kv("    window", f"{plan.start} -> {plan.end}  (span {plan.span_days}d)")
        _kv("    replace_history", plan.replace_history)

        upd = await determine_data_mode(state, deps)
        state.update(upd)
        if _merge_error(state):
            return 1
        print("  Outputs:")
        _kv("    raw items fetched", len(state["raw_payload"].get("items", [])))
        _kv("    available_days (merged)", state["available_days"])
        _kv("    data_mode", state["data_mode"])
        print("  Data-mode rule: <30 Cold Start | 30-59 Initial | 60-89 Beginner | >=90 Normal")

        # ---- PHASE 2 -----------------------------------------------------
        _hdr("PHASE 2 - ingest_daily_data (pure normalize + merge history)")
        upd = await ingest_daily_data(state, deps)
        state.update(upd)
        if _merge_error(state):
            return 1
        print("  Outputs:")
        _kv("    account_total_cost (target)", state["account_total_cost"])
        _kv("    service_costs (target day)", f"{len(state['service_costs'])} services")
        _kv("    merged history_series", f"{len(state['history_series'])} days")
        _kv("      series preview", _preview_series(state["history_series"]))
        _kv("    merged service_history", f"{len(state['service_history'])} services")
        # Show the account total as the sum of per-service target costs.
        svc_sum = round(sum(state["service_costs"].values()), 2)
        _kv("    sum(service_costs) check", f"{svc_sum} (== account_total_cost)")

        # ---- PHASE 3 -----------------------------------------------------
        _hdr("PHASE 3 - update_metadata_and_model (new services + STL)")
        # Recompute STL here purely to SHOW the decomposition math; the node also
        # runs it. This does not alter the node's own computation.
        known_before = set((pre_md or {}).get("known_active_services") or [])
        upd = await update_metadata_and_model(state, deps)
        state.update(upd)
        if _merge_error(state):
            return 1
        print("  New-service detection (window services - known, excluding UNKNOWN):")
        _kv("    known before run", len(known_before))
        _kv("    new_services_detected", len(state["new_services_detected"]))
        if state["new_services_detected"]:
            preview = ", ".join(state["new_services_detected"][:8])
            more = "" if len(state["new_services_detected"]) <= 8 else f"  (+{len(state['new_services_detected']) - 8} more)"
            _kv("      sample", preview + more)
        print("  STL decomposition (fit on history EXCLUDING the target day):")
        stl = run_stl(state["history_series"], config)
        if isinstance(stl, dict) and "error" in stl:
            _kv("    STL", f"not available: {stl['error']}")
            _kv("    -> Phase 4 fallback", "cold-start absolute comparison")
        else:
            n = len(stl.resid)
            ordered = sorted(stl.resid)
            def pct(p):
                if not ordered:
                    return 0.0
                rank = (p / 100) * (len(ordered) - 1)
                lo = int(rank); hi = min(lo + 1, len(ordered) - 1)
                return ordered[lo] + (ordered[hi] - ordered[lo]) * (rank - lo)
            q1, q3 = pct(25), pct(75)
            _kv("    fit points (resid)", n)
            _kv("    last trend level", round(stl.trend[-1], 4) if stl.trend else None)
            _kv("    expected_target", round(stl.expected_target, 4))
            _kv("    actual_target", state["account_total_cost"])
            _kv("    target_residual", f"{round(stl.target_residual, 4)}  (actual - expected)")
            if config.statistical_method == "iqr":
                _kv("    Q1 / Q3 / IQR", f"{round(q1,4)} / {round(q3,4)} / {round(q3-q1,4)}")
                _kv("    threshold formula", f"Q3 + {config.iqr_factor}*IQR")
            else:
                _kv("    threshold formula", f"mean + {config.sigma_factor}*std")
            _kv("    residual_threshold", round(stl.residual_threshold, 4))

        # ---- PHASE 4 -----------------------------------------------------
        _hdr("PHASE 4 - detect_account_anomaly")
        prev = most_recent_non_anomaly_day(state.get("metadata", {}), target_date)
        _kv("  most-recent prior non-anomaly day", prev if prev else "(none)")
        upd = await detect_account_anomaly(state, deps)
        state.update(upd)
        if _merge_error(state):
            return 1
        print("  Detection:")
        if isinstance(stl, dict) and "error" in stl:
            _kv("    method", "cold-start-absolute (STL unavailable)")
            _kv("    rule", f"|actual - prev_non_anomaly| > {config.cold_start_absolute_threshold}")
        else:
            _kv("    method", "stl-residual")
            _kv("    rule", "target_residual > residual_threshold")
            _kv("    comparison",
                f"{round(stl.target_residual,4)} > {round(stl.residual_threshold,4)} "
                f"=> {state['account_anomaly_detected']}")
        _kv("    account_anomaly_detected", state["account_anomaly_detected"])
        _kv("    baseline_day", state.get("baseline_day"))

        # ---- PHASE 5 -----------------------------------------------------
        _hdr("PHASE 5 - drill_down_services (only runs on an anomaly)")
        upd = await drill_down_services(state, deps)
        state.update(upd)
        if _merge_error(state):
            return 1
        contributors = state.get("anomaly_contributors") or []
        if not state.get("account_anomaly_detected"):
            print("  Skipped: no account anomaly, so no contributor isolation.")
        else:
            print(f"  Contributors (delta = target_cost - baseline_cost, baseline_day="
                  f"{state.get('baseline_day')}):")
            _kv("    contributor count", len(contributors))
            for c in contributors[:10]:
                print(f"      {c['service']:<48} +{c['delta']:<10} "
                      f"({c['baseline_cost']} -> {c['current_cost']})")
            if len(contributors) > 10:
                print(f"      ... (+{len(contributors) - 10} more)")

        # ---- PHASE 6 -----------------------------------------------------
        _hdr("PHASE 6 - dispatch_alerts (narrative + delivery; DRY RUN here)")
        upd = await dispatch_alerts(state, deps)
        state.update(upd)
        if _merge_error(state):
            return 1
        _kv("  alert_dispatched", state.get("alert_dispatched"))
        _kv("  alert_dry_run", state.get("alert_dry_run"))
        _kv("  narrative_templated (LLM?)",
            f"{state.get('narrative_templated')} "
            f"({'templated fallback' if state.get('narrative_templated') else 'LLM-generated'})")
        payload = state.get("alert_payload") or {}
        _kv("  subject", payload.get("subject"))
        print("\n  --- Narrative ---")
        print("  " + (payload.get("narrative", "") or "").replace("\n", "\n  "))

        # ---- FINAL -------------------------------------------------------
        _hdr("FINAL STATE SUMMARY")
        _kv("data_mode", state["data_mode"])
        _kv("available_days", state["available_days"])
        _kv("account_total_cost", state["account_total_cost"])
        _kv("account_anomaly_detected", state["account_anomaly_detected"])
        _kv("new_services_detected", len(state["new_services_detected"]))
        _kv("contributors", len(contributors))
        _kv("alert_dispatched (dry-run)", state.get("alert_dispatched"))
        return 0
    finally:
        if http is not None:
            await http.aclose()


def _parse_date(value: str) -> str:
    try:
        datetime.strptime(value, "%Y-%m-%d")
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"invalid date {value!r}; expected YYYY-MM-DD") from exc
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Phase-by-phase trace of the CCIP workflow.")
    parser.add_argument("--account-id", required=True)
    parser.add_argument("--target-date", type=_parse_date, required=True)
    parser.add_argument("--config", default=os.path.join(_ROOT, "config.yaml"))
    args = parser.parse_args(argv)
    return asyncio.run(trace(args.account_id, args.target_date, args.config))


if __name__ == "__main__":
    raise SystemExit(main())
