#!/usr/bin/env python
"""End-to-end runner for the CCIP LangGraph anomaly workflow.

Wires the dependencies (real or fake) into the compiled LangGraph and runs a
single evaluation for a ``(account_id, target_date)``.

Modes:
  - LIVE (default): the real AWS CUR Cost_Tool + real Gemini LLM + real SMTP
    sender, all driven by ``config.yaml`` (with ``.env`` injection). If SMTP is
    not fully configured the email stage performs a safe dry run (logs the alert).
  - ``--dry-run-email``: LIVE (real AWS + Gemini) but the email stage logs the
    alert instead of sending it, even when SMTP is fully configured.
  - ``--no-live``: in-memory fakes, zero network — verifies the wiring/flow only.

The runner prints only what the workflow actually produced; it fabricates
nothing. It exits non-zero when the run ends in a Structured_Error.

Examples
--------
    python scripts/run_ccip_check.py --account-id 123456789012
    python scripts/run_ccip_check.py --account-id A --target-date 2026-09-29
    python scripts/run_ccip_check.py --account-id A --dry-run-email  # live, no send
    python scripts/run_ccip_check.py --account-id A --no-live        # fakes, zero network
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from datetime import date, datetime, timedelta

# Make ``src`` importable when run as a plain script.
_HERE = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_HERE)
_SRC = os.path.join(_ROOT, "src")
if _SRC not in sys.path:
    sys.path.insert(0, _SRC)

from connector.ccip.config import ConfigError, load_ccip_config  # noqa: E402
from connector.ccip.runner import build_fake_deps, build_live_deps, run_once  # noqa: E402


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"invalid date {value!r}; expected YYYY-MM-DD"
        ) from exc


def _default_target_date() -> date:
    """Default to yesterday - the latest complete day for daily cost data."""
    return date.today() - timedelta(days=1)


def _is_structured_error(result) -> bool:
    """True when ``run_once`` returned a bare Structured_Error (failure).

    ``run_once`` returns the full terminal state on success (its ``error`` key is
    ``None``) and a bare ``{"error", "category", ...}`` dict on any failure, so a
    top-level ``category`` key uniquely identifies the failure shape.
    """
    return isinstance(result, dict) and "category" in result and "data_mode" not in result


def _print_result(result) -> None:
    print("\n=== CCIP run result ===")
    if _is_structured_error(result):
        print("Structured_Error returned (no alert sent):")
        print(json.dumps(result, indent=2))
        return
    # Terminal state (a dict-like).
    state = result
    print(f"data_mode               : {state.get('data_mode')}")
    print(f"available_days          : {state.get('available_days')}")
    print(f"account_total_cost      : {state.get('account_total_cost')}")
    print(f"account_anomaly_detected: {state.get('account_anomaly_detected')}")
    print(f"new_services_detected   : {state.get('new_services_detected')}")
    contributors = state.get("anomaly_contributors") or []
    print(f"anomaly_contributors    : {len(contributors)}")
    for c in contributors:
        ctx = f" | {c['seasonal_context']}" if c.get("seasonal_context") else ""
        print(f"  - {c['service']}: +{c['delta']} "
              f"({c.get('baseline_cost')} -> {c.get('current_cost')}){ctx}")
    print(f"alert_dispatched        : {state.get('alert_dispatched')} "
          f"(dry_run={state.get('alert_dry_run')}, templated={state.get('narrative_templated')})")
    payload = state.get("alert_payload") or {}
    if payload.get("subject"):
        print(f"\n--- Alert ---\nsubject: {payload['subject']}\n")
        print(payload.get("narrative", ""))
    if state.get("error"):
        print("\nStructured_Error:", json.dumps(state["error"], indent=2))


async def _run(
    account_id: str, target_date: date, live: bool, config_path: str,
    dry_run_email: bool,
) -> int:
    smtp_note = " | EMAIL DRY-RUN (log only)" if (live and dry_run_email) else ""
    mode = "LIVE (real AWS + Gemini + SMTP)" if live else "FAKE (no network)"
    print(f"Running CCIP check | mode={mode}{smtp_note} | account_id={account_id} | target_date={target_date}")

    http = None
    try:
        if live:
            try:
                config = load_ccip_config(config_path)
            except ConfigError as exc:
                print(f"Cannot start live run: {exc}")
                print("Populate config.yaml / the CCIP_* keys in .env, or use --no-live.")
                return 2
            deps, http = build_live_deps(config, force_email_dry_run=dry_run_email)
        else:
            # A minimal config that needs no secrets; a tiny fake dataset.
            from connector.ccip.config import CcipConfig, CcipSecrets

            config = CcipConfig(
                metadata_path=os.path.join(_ROOT, "ccip_metadata.json"),
                _secrets=CcipSecrets(llm_api_key="fake", smtp_password="fake"),
            )
            deps = build_fake_deps(config, cost_payload={"granularity": "DAILY", "items": []})

        result = await run_once(account_id, target_date, deps)
    finally:
        if http is not None:
            await http.aclose()

    _print_result(result)
    return 1 if _is_structured_error(result) else 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the CCIP anomaly workflow end to end.")
    parser.add_argument("--account-id", required=True, help="Target AWS account id.")
    parser.add_argument(
        "--target-date", type=_parse_date, default=None,
        help="Evaluation date (YYYY-MM-DD). Defaults to yesterday.",
    )
    parser.add_argument(
        "--config", default=os.path.join(_ROOT, "config.yaml"),
        help="Path to config.yaml (default: repo root).",
    )
    parser.add_argument(
        "--no-live", action="store_true",
        help="Use in-memory fakes (no AWS/Gemini/SMTP calls) to verify wiring.",
    )
    parser.add_argument(
        "--dry-run-email", action="store_true",
        help="Live run, but force the email stage to log the alert instead of "
             "sending it over SMTP (safe first live run).",
    )
    args = parser.parse_args(argv)
    target_date = args.target_date or _default_target_date()
    return asyncio.run(
        _run(args.account_id, target_date, not args.no_live, args.config, args.dry_run_email)
    )


if __name__ == "__main__":
    raise SystemExit(main())
