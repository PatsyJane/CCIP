"""Integration tests injecting the REAL aws_cur_cost over an httpx.MockTransport.

Feature: ccip-langgraph-anomaly (tasks.md task 15).

These exercise the genuine ingestion path end to end: the real
``connector.tools.aws_cur_cost`` pipeline (validate -> build request -> POST ->
parse/adapt -> numeric coerce -> tag) is wired into the CCIP graph via
``make_aws_cur_cost(config, http)``, with the Cost_API faked by an
``httpx.MockTransport`` and the LLM + SMTP replaced by in-memory fakes. No real
network, SMTP, or external-filesystem call occurs (metadata writes go to
``tmp_path``).

Covered:
  - happy path: a Normal-mode run with a spike drives the whole graph to a
    rendered, dry-run alert;
  - one representative failure per ErrorCategory:
      * invalid-parameter  (non-DAILY handled upstream; here a Cost_API 4xx maps
        to unreachable-source, so invalid-parameter is exercised via a bad date);
      * unreachable-source (Cost_API returns HTTP 503);
      * missing-data       (Cost_API returns an empty result set);
      * unexpected-error   (Cost_API returns an unparseable / unexpected shape).
"""

from __future__ import annotations

import json
from datetime import date, timedelta

import httpx
import pytest

from connector.config import AppConfig
from connector.http_client import HttpClient
from connector.tools.aws_cur_cost import make_aws_cur_cost

from connector.ccip.config import CcipConfig, CcipSecrets
from connector.ccip.nodes import CcipDeps
from connector.ccip.runner import run_once

from .fakes import FakeEmailSender, FakeLLMClient

TARGET = "2026-09-29"


def _app_config() -> AppConfig:
    """A fully-populated AppConfig built directly (no .env needed)."""
    return AppConfig(
        aws_account_id="123456789012",
        aws_account_name="test-account",
        aws_region="us-east-1",
        cost_api_base_url="http://cost-api.test",
        provider="aws",
        project="ccip-poc",
    )


def _ccip_config(tmp_path, **kw) -> CcipConfig:
    base = dict(
        metadata_path=str(tmp_path / "md.json"),
        # Increment 2: per-account store under metadata_dir; isolate to tmp_path.
        metadata_dir=str(tmp_path),
        _secrets=CcipSecrets(llm_api_key="test-llm-key", smtp_password="test-smtp-pw"),
    )
    base.update(kw)
    return CcipConfig(**base)


def _daily_rows(spike: float | None = None) -> list[dict]:
    """Build 120 days of two-service daily cost rows in the live Cost_API shape.

    The response_adapter looks for rows under the ``daily`` container key with a
    recognized cost field (``cost``) and a timestamp field (``date``), plus
    ``service``/``environment``. Matches the real wire assumption.
    """
    end = date.fromisoformat(TARGET)
    rows: list[dict] = []
    weekly = [50, 55, 60, 58, 56, 30, 25]
    for i in range(120):
        d = end - timedelta(days=119 - i)
        rows.append({"date": d.isoformat(), "cost": float(weekly[d.weekday()]),
                     "service": "ec2", "environment": "prod"})
        rows.append({"date": d.isoformat(), "cost": 10.0,
                     "service": "s3", "environment": "prod"})
    if spike is not None:
        # Spike ec2 on the target day (last ec2 row).
        rows[-2]["cost"] = spike
    return rows


def _make_real_cost_tool(handler):
    """Wire make_aws_cur_cost with an httpx.MockTransport using ``handler``."""
    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport, base_url="http://cost-api.test")
    http = HttpClient(client=client)
    cost_tool = make_aws_cur_cost(_app_config(), http)
    return cost_tool, http


async def _run_with_handler(tmp_path, handler, *, llm=None, email=None):
    cost_tool, http = _make_real_cost_tool(handler)
    deps = CcipDeps(cost_tool, llm or FakeLLMClient("narrative"),
                    email or FakeEmailSender(), _ccip_config(tmp_path))
    try:
        return await run_once("123456789012", TARGET, deps)
    finally:
        await http.aclose()


# --- Happy path -----------------------------------------------------------


async def test_real_tool_happy_path_dispatches_alert(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/costs"
        body = json.loads(request.content)
        assert body["granularity"] == "DAILY"  # the single DAILY fetch
        return httpx.Response(200, json={"daily": _daily_rows(spike=900.0)})

    email = FakeEmailSender()
    result = await _run_with_handler(tmp_path, handler, email=email)

    assert result.get("error") is None
    assert result["data_mode"] == "Normal"
    assert result["available_days"] == 120
    assert result["account_anomaly_detected"] is True
    assert any(c["service"] == "ec2" for c in result["anomaly_contributors"])
    assert result["alert_dispatched"] is True


# --- missing-data ---------------------------------------------------------


async def test_real_tool_empty_result_is_missing_data(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"daily": []})

    result = await _run_with_handler(tmp_path, handler)
    assert result["category"] == "missing-data"


# --- unreachable-source ---------------------------------------------------


async def test_real_tool_http_503_is_unreachable(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="service unavailable")

    result = await _run_with_handler(tmp_path, handler)
    assert result["category"] == "unreachable-source"


# --- unexpected-error -----------------------------------------------------


async def test_real_tool_unexpected_shape_is_unexpected_error(tmp_path):
    def handler(request: httpx.Request) -> httpx.Response:
        # 200 but a body with no recognizable cost container -> unexpected shape.
        return httpx.Response(200, json={"totally": "unexpected"})

    result = await _run_with_handler(tmp_path, handler)
    assert result["category"] == "unexpected-error"


# --- invalid-parameter (validated before any HTTP call) -------------------


async def test_real_tool_invalid_date_is_invalid_parameter(tmp_path):
    # A bad target_date is rejected by build_initial_state before the graph runs,
    # but to exercise the REAL tool's own invalid-parameter path we drive the tool
    # through the graph with a valid date and a handler that should never be hit
    # for an invalid *tool* parameter. Here we assert the tool-level validation by
    # calling the real tool directly with an inverted range via a short window.
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        return httpx.Response(200, json={"daily": _daily_rows()})

    cost_tool, http = _make_real_cost_tool(handler)
    try:
        # Inverted range: end before start -> the real tool returns
        # invalid-parameter and issues NO HTTP request.
        result = await cost_tool("2026-09-29", "2026-09-01", "DAILY")
    finally:
        await http.aclose()

    assert result["category"] == "invalid-parameter"
    assert calls["n"] == 0  # no HTTP call was issued for invalid params

