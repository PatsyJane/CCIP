"""End-to-end graph smoke/integration tests driven entirely by fakes.

Feature: ccip-langgraph-anomaly. The Cost_Tool, LLM, and Email_Sender are fakes;
the metadata store writes to tmp_path. No network/SMTP calls occur.
"""

from __future__ import annotations

import random
from datetime import date, timedelta

import pytest

from connector.ccip.config import CcipConfig, CcipSecrets
from connector.ccip.graph import build_graph
from connector.ccip.nodes import CcipDeps
from connector.ccip.runner import run_once
from connector.ccip.state import build_initial_state

from .fakes import FakeCostTool, FakeEmailSender, FakeLLMClient, make_daily_payload


def _config(tmp_path, **kw):
    base = dict(
        metadata_path=str(tmp_path / "md.json"),
        _secrets=CcipSecrets(llm_api_key="k", smtp_password="p"),
    )
    base.update(kw)
    return CcipConfig(**base)


def _noisy_series(end_day: str, n: int, weekly, spike=None, seed=7):
    rng = random.Random(seed)
    end = date.fromisoformat(end_day)
    costs = []
    for i in range(n):
        d = end - timedelta(days=n - 1 - i)
        costs.append(round(weekly[d.weekday()] + rng.gauss(0, 1.0), 2))
    if spike is not None:
        costs[-1] = spike
    return costs


async def test_graph_compiles():
    deps = CcipDeps(FakeCostTool({"granularity": "DAILY", "items": []}),
                    FakeLLMClient(), FakeEmailSender(), CcipConfig())
    graph = build_graph(deps)
    assert graph is not None


async def test_happy_path_normal_mode_anomaly_dispatches(tmp_path):
    weekly = [20, 22, 25, 24, 23, 12, 10]
    payload = make_daily_payload(
        {"ec2": _noisy_series("2026-09-29", 120, weekly, spike=400.0),
         "s3": _noisy_series("2026-09-29", 120, [5] * 7)},
        end_day="2026-09-29",
    )
    email = FakeEmailSender()
    deps = CcipDeps(FakeCostTool(payload), FakeLLMClient("LLM narrative"), email, _config(tmp_path))

    result = await run_once("A", "2026-09-29", deps)

    assert result.get("error") is None
    assert result["data_mode"] == "Normal"
    assert result["available_days"] == 120
    assert result["account_anomaly_detected"] is True
    assert result["alert_dispatched"] is True
    assert result["narrative_templated"] is False
    assert any(c["service"] == "ec2" for c in result["anomaly_contributors"])


async def test_new_services_trigger_alert_without_anomaly(tmp_path):
    # Clean series, no spike -> no account anomaly, but services are brand new,
    # so the Alert-Check router still dispatches (Req 7.3).
    weekly = [20, 21, 22, 21, 20, 12, 11]
    payload = make_daily_payload(
        {"ec2": _noisy_series("2026-09-29", 120, weekly)}, end_day="2026-09-29"
    )
    deps = CcipDeps(FakeCostTool(payload), FakeLLMClient(), FakeEmailSender(), _config(tmp_path))
    result = await run_once("A", "2026-09-29", deps)
    assert result.get("error") is None
    assert result["new_services_detected"] == ["ec2"]
    assert result["alert_dispatched"] is True


async def test_cost_tool_error_halts_and_surfaces(tmp_path):
    deps = CcipDeps(
        FakeCostTool({"error": "down", "category": "unreachable-source"}),
        FakeLLMClient(), FakeEmailSender(), _config(tmp_path),
    )
    result = await run_once("A", "2026-09-29", deps)
    assert result["category"] == "unreachable-source"


async def test_empty_dataset_is_missing_data(tmp_path):
    deps = CcipDeps(FakeCostTool({"granularity": "DAILY", "items": []}),
                    FakeLLMClient(), FakeEmailSender(), _config(tmp_path))
    result = await run_once("A", "2026-09-29", deps)
    assert result["category"] == "missing-data"


async def test_bad_input_short_circuits_before_graph(tmp_path):
    deps = CcipDeps(FakeCostTool({"granularity": "DAILY", "items": []}),
                    FakeLLMClient(), FakeEmailSender(), _config(tmp_path))
    result = await run_once("", "2026-09-29", deps)
    assert result["category"] == "invalid-parameter"


async def test_llm_unreachable_falls_back_to_template(tmp_path):
    weekly = [20, 22, 25, 24, 23, 12, 10]
    payload = make_daily_payload(
        {"ec2": _noisy_series("2026-09-29", 120, weekly, spike=400.0)}, end_day="2026-09-29"
    )
    llm = FakeLLMClient(raise_exc=RuntimeError("llm down"))
    deps = CcipDeps(FakeCostTool(payload), llm, FakeEmailSender(), _config(tmp_path))
    result = await run_once("A", "2026-09-29", deps)
    assert result.get("error") is None
    assert result["alert_dispatched"] is True
    assert result["narrative_templated"] is True


async def test_no_anomaly_no_new_services_routes_to_end(tmp_path):
    # Pre-seed metadata so services are known and a prior non-anomaly day exists.
    import json
    weekly = [20, 21, 22, 21, 20, 12, 11]
    payload = make_daily_payload(
        {"ec2": _noisy_series("2026-09-29", 120, weekly)}, end_day="2026-09-29"
    )
    md_path = tmp_path / "md.json"
    md_path.write_text(json.dumps({
        "known_active_services": ["ec2"],
        "seasonal": {}, "last_anomaly": None,
        "non_anomaly_days": {"2026-09-20": 20.0},
    }), encoding="utf-8")
    deps = CcipDeps(FakeCostTool(payload), FakeLLMClient(), FakeEmailSender(),
                    _config(tmp_path))
    result = await run_once("A", "2026-09-29", deps)
    assert result.get("error") is None
    assert result["account_anomaly_detected"] is False
    assert result["new_services_detected"] == []
    assert result["alert_dispatched"] is False


async def test_secret_scrubbed_from_error(tmp_path):
    # Force an email delivery failure that embeds nothing secret, but verify the
    # scrubbing path runs by configuring a secret and a failing sender.
    weekly = [20, 22, 25, 24, 23, 12, 10]
    payload = make_daily_payload(
        {"ec2": _noisy_series("2026-09-29", 120, weekly, spike=400.0)}, end_day="2026-09-29"
    )
    email = FakeEmailSender(raise_exc=RuntimeError("smtp boom with secret topsecret"))
    config = _config(tmp_path, _secrets=CcipSecrets(llm_api_key="k", smtp_password="topsecret"))
    deps = CcipDeps(FakeCostTool(payload), FakeLLMClient(), email, config)
    result = await run_once("A", "2026-09-29", deps)
    assert result["category"] == "unreachable-source"
    assert "topsecret" not in str(result)
