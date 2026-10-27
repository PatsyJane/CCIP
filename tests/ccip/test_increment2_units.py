"""Unit tests for increment 2: incremental ingestion + per-account metadata.

Feature: ccip-langgraph-anomaly (Requirements 13-16). These cover the concrete
edge cases that complement the property tests: the blank-name filename fallback,
the pre-increment-2 metadata-store upgrade path, the idempotent/overwrite merge,
and the end-to-end incremental-vs-full fetch behavior driven through the graph
with fakes. No network/SMTP/external-filesystem (tmp_path only) calls occur.
"""

from __future__ import annotations

import json
import os
from datetime import date, timedelta

from connector.ccip.config import CcipConfig, CcipSecrets
from connector.ccip.ingest import decide_fetch_window
from connector.ccip.metadata_store import (
    account_metadata_path,
    default_metadata,
    load_metadata,
    merge_history,
    merge_service_history,
    sanitize_path_segment,
)
from connector.ccip.nodes import CcipDeps
from connector.ccip.runner import run_once

from .fakes import FakeCostTool, FakeEmailSender, FakeLLMClient, make_daily_payload


# --- Filename sanitization + per-account path (Req 14) ---------------------


def test_sanitize_replaces_unsafe_collapses_and_trims():
    assert sanitize_path_segment("Payments Prod (US)") == "Payments_Prod_US"
    assert sanitize_path_segment("a///b:::c") == "a_b_c"
    # Runs of underscores collapse to one, leading/trailing separators trimmed.
    assert sanitize_path_segment("__lead__trail__") == "lead_trail"
    # Trimmed of leading/trailing separators.
    assert sanitize_path_segment("..hidden..") == "hidden"
    # All-unsafe / blank -> empty (callers supply a fallback).
    assert sanitize_path_segment("///") == ""
    assert sanitize_path_segment("") == ""


def test_sanitize_is_traversal_safe():
    # The real safety invariant: the output is a SINGLE path segment — it contains
    # no path separator and is not a "." / ".." directory component. A literal ".."
    # embedded in a longer name (e.g. "a..b") is harmless because it is not a whole
    # path component and has no separators around it.
    for evil in ["../../etc/passwd", r"..\..\windows", "C:\\abs\\path", "a/../b"]:
        out = sanitize_path_segment(evil)
        assert "/" not in out and "\\" not in out
        assert out not in (".", "..")


def test_account_path_blank_name_fallback():
    cfg = CcipConfig(metadata_dir="metadata")
    # Blank name -> name segment omitted (Req 14.3).
    p = account_metadata_path(cfg, "123456789012", "")
    assert os.path.basename(p) == "ccip_metadata_123456789012.json"
    # Non-blank name -> both segments present.
    p2 = account_metadata_path(cfg, "123456789012", "Payments Prod")
    assert os.path.basename(p2) == "ccip_metadata_Payments_Prod_123456789012.json"


def test_account_path_all_unsafe_id_uses_placeholder():
    cfg = CcipConfig(metadata_dir="metadata")
    p = account_metadata_path(cfg, "///", "")
    # An all-unsafe id falls back to the stable placeholder so a name is producible.
    assert os.path.basename(p) == "ccip_metadata_account.json"


# --- Pre-increment-2 store upgrade (Req 15.1) ------------------------------


def test_pre_increment2_store_upgrades_with_new_keys(tmp_path):
    # A store written by a pre-increment-2 build lacks the rolling-history keys.
    legacy = {
        "known_active_services": ["ec2"],
        "seasonal": {},
        "last_anomaly": None,
        "non_anomaly_days": {"2026-09-20": 20.0},
    }
    path = tmp_path / "legacy.json"
    path.write_text(json.dumps(legacy), encoding="utf-8")

    loaded = load_metadata(str(path))
    # Legacy values preserved.
    assert loaded["known_active_services"] == ["ec2"]
    assert loaded["non_anomaly_days"] == {"2026-09-20": 20.0}
    # New increment-2 keys present with safe defaults.
    assert loaded["account_history"] == []
    assert loaded["service_history"] == {}
    assert loaded["last_full_fetch_date"] is None
    # All default keys exist after load.
    for key in default_metadata():
        assert key in loaded


def test_legacy_history_series_key_migrates_to_account_history(tmp_path):
    # A store written before the rename used ``history_series`` for the
    # Rolling_History_Window. load_metadata SHALL carry its value over to
    # ``account_history`` and drop the legacy key (Req 15.1 back-compat).
    hist = [{"day": "2026-09-28", "cost": 10.0}, {"day": "2026-09-29", "cost": 11.0}]
    legacy = {
        "known_active_services": ["ec2"],
        "history_series": hist,
    }
    path = tmp_path / "legacy_history_series.json"
    path.write_text(json.dumps(legacy), encoding="utf-8")

    loaded = load_metadata(str(path))
    # Value preserved under the new key; legacy key removed.
    assert loaded["account_history"] == hist
    assert "history_series" not in loaded


def test_account_history_key_wins_over_legacy_when_both_present(tmp_path):
    # If a store somehow carries BOTH keys, the new key is authoritative and the
    # legacy key is discarded without clobbering it.
    new_hist = [{"day": "2026-09-29", "cost": 99.0}]
    old_hist = [{"day": "2026-09-29", "cost": 1.0}]
    both = {"account_history": new_hist, "history_series": old_hist}
    path = tmp_path / "both_keys.json"
    path.write_text(json.dumps(both), encoding="utf-8")

    loaded = load_metadata(str(path))
    assert loaded["account_history"] == new_hist
    assert "history_series" not in loaded


# --- Merge helpers: concrete overwrite + trim (Req 15.2, 15.3) -------------


def test_merge_history_overwrites_not_sums():
    persisted = [{"day": "2026-10-01", "cost": 5.0}, {"day": "2026-10-02", "cost": 7.0}]
    incoming = [{"day": "2026-10-02", "cost": 99.0}, {"day": "2026-10-03", "cost": 3.0}]
    out = merge_history(persisted, incoming, history_days=90)
    by_day = {r["day"]: r["cost"] for r in out}
    assert by_day == {"2026-10-01": 5.0, "2026-10-02": 99.0, "2026-10-03": 3.0}
    # Ascending order preserved.
    assert [r["day"] for r in out] == ["2026-10-01", "2026-10-02", "2026-10-03"]


def test_merge_history_trims_to_history_days():
    rows = [{"day": (date(2026, 1, 1) + timedelta(days=i)).isoformat(), "cost": float(i)}
            for i in range(10)]
    out = merge_history([], rows, history_days=3)
    # Keeps only the 3 most-recent days.
    assert [r["day"] for r in out] == [
        "2026-01-08", "2026-01-09", "2026-01-10"
    ]


def test_merge_service_history_is_per_service():
    persisted = {"ec2": [{"day": "2026-10-01", "cost": 1.0}]}
    incoming = {
        "ec2": [{"day": "2026-10-01", "cost": 2.0}],  # overwrite
        "s3": [{"day": "2026-10-01", "cost": 5.0}],   # new service
    }
    out = merge_service_history(persisted, incoming, history_days=90)
    assert out["ec2"] == [{"day": "2026-10-01", "cost": 2.0}]
    assert out["s3"] == [{"day": "2026-10-01", "cost": 5.0}]


# --- Fetch-window decision edge cases (Req 16) -----------------------------


def _cfg(tmp_path, **kw):
    base = dict(
        metadata_dir=str(tmp_path),
        _secrets=CcipSecrets(llm_api_key="k", smtp_password="p"),
        retry_base_delay=0.0,
        retry_max_delay=0.0,
        retry_jitter=False,
    )
    base.update(kw)
    return CcipConfig(**base)


def test_decide_cold_start_when_history_empty(tmp_path):
    cfg = _cfg(tmp_path, history_days=90)
    plan = decide_fetch_window({"account_history": []}, date(2026, 6, 1), cfg)
    assert plan.mode == "full" and plan.reason == "cold-start"
    assert plan.span_days == 90
    assert plan.end == "2026-06-01"


def test_decide_trailing_when_steady_state(tmp_path):
    cfg = _cfg(tmp_path, history_days=90, trailing_fetch_days=2, reconciliation_interval_days=7)
    target = date(2026, 6, 1)
    # 90 contiguous days ending yesterday (no gap), last full pull 2 days ago.
    hist = [
        {"day": (target - timedelta(days=90 - i)).isoformat(), "cost": 10.0}
        for i in range(90)
    ]
    md = {
        "account_history": hist,
        "last_full_fetch_date": (target - timedelta(days=2)).isoformat(),
    }
    plan = decide_fetch_window(md, target, cfg)
    assert plan.mode == "trailing" and plan.reason == "steady-state"
    assert plan.start == (target - timedelta(days=1)).isoformat()
    assert plan.end == target.isoformat()


def test_decide_reconciliation_replaces_history(tmp_path):
    cfg = _cfg(tmp_path, history_days=90, reconciliation_interval_days=7)
    target = date(2026, 6, 1)
    hist = [
        {"day": (target - timedelta(days=90 - i)).isoformat(), "cost": 10.0}
        for i in range(90)
    ]
    md = {
        "account_history": hist,
        "last_full_fetch_date": (target - timedelta(days=7)).isoformat(),  # due
    }
    plan = decide_fetch_window(md, target, cfg)
    assert plan.mode == "full" and plan.reason == "reconciliation"
    assert plan.replace_history is True


def test_decide_gap_triggers_full(tmp_path):
    cfg = _cfg(tmp_path, history_days=90)
    target = date(2026, 6, 1)
    # 90 days but the latest is 5 days before the target -> unbridgeable gap.
    latest = target - timedelta(days=5)
    hist = [
        {"day": (latest - timedelta(days=90 - 1 - i)).isoformat(), "cost": 10.0}
        for i in range(90)
    ]
    plan = decide_fetch_window({"account_history": hist}, target, cfg)
    assert plan.mode == "full" and plan.reason == "gap"


# --- End-to-end incremental behavior through the graph (Req 15, 16) --------


def _weekly_series(end_day: str, n: int):
    weekly = [20, 21, 22, 21, 20, 12, 11]
    end = date.fromisoformat(end_day)
    return [float(weekly[(end - timedelta(days=n - 1 - i)).weekday()]) for i in range(n)]


async def test_cold_start_then_trailing_run_persists_and_reuses_history(tmp_path):
    """A first (cold-start) run persists ~90 days; a later run on the next day does
    a TRAILING fetch yet still sees Normal mode from the persisted history."""
    cfg = _cfg(tmp_path)

    # Run 1: cold start. Full 90-day payload ending 2026-09-29.
    payload1 = make_daily_payload({"ec2": _weekly_series("2026-09-29", 90)}, end_day="2026-09-29")
    tool1 = FakeCostTool(payload1)
    deps1 = CcipDeps(tool1, FakeLLMClient(), FakeEmailSender(), cfg)
    r1 = await run_once("A", "2026-09-29", deps1)
    assert r1.get("error") is None
    assert r1["data_mode"] == "Normal"
    # Full pull requested the 90-day window.
    assert tool1.calls[0][1] == "2026-09-29"  # end == target
    # The per-account store now holds the rolling history + last_full_fetch_date.
    store_path = account_metadata_path(cfg, "A", "")
    stored = json.loads(open(store_path, encoding="utf-8").read())
    assert len(stored["account_history"]) == 90
    assert stored["last_full_fetch_date"] == "2026-09-29"

    # Run 2: next day. Only a 2-day trailing payload is available from the tool.
    payload2 = make_daily_payload({"ec2": _weekly_series("2026-09-30", 2)}, end_day="2026-09-30")
    tool2 = FakeCostTool(payload2)
    deps2 = CcipDeps(tool2, FakeLLMClient(), FakeEmailSender(), cfg)
    r2 = await run_once("A", "2026-09-30", deps2)
    assert r2.get("error") is None
    # Trailing fetch requested only a 2-day window (start == target - 1).
    assert tool2.calls[0][0] == "2026-09-29"
    assert tool2.calls[0][1] == "2026-09-30"
    # Yet the MERGED persisted history keeps the account in Normal mode.
    assert r2["data_mode"] == "Normal"
    assert r2["available_days"] >= 90

    # The store history advanced to include the new day, trimmed to <= 90.
    stored2 = json.loads(open(store_path, encoding="utf-8").read())
    assert len(stored2["account_history"]) <= 90
    assert stored2["account_history"][-1]["day"] == "2026-09-30"


async def test_two_accounts_use_separate_files(tmp_path):
    cfg = _cfg(tmp_path)
    payload = make_daily_payload({"ec2": _weekly_series("2026-09-29", 90)}, end_day="2026-09-29")
    for acct in ("A", "B"):
        deps = CcipDeps(FakeCostTool(payload), FakeLLMClient(), FakeEmailSender(), cfg)
        res = await run_once(acct, "2026-09-29", deps)
        assert res.get("error") is None
    # Each account wrote its OWN file; no collision (Req 14.4).
    assert (tmp_path / "ccip_metadata_A.json").exists()
    assert (tmp_path / "ccip_metadata_B.json").exists()
