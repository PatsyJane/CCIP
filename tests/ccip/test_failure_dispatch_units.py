"""Unit + smoke tests for terminal failure notification (Part B).

Feature: ccip-langgraph-anomaly, Requirement 12 (Part B). The failure alert is
deterministic (no LLM), secret-scrubbed, best-effort, and non-recursive, and the
graph routes any error through it while preserving the original error as the
result.
"""

from __future__ import annotations

from datetime import date

import pytest

from connector.ccip.config import CcipConfig, CcipSecrets
from connector.ccip.email import (
    build_failure_payload,
    build_failure_subject,
    render_failure_narrative,
)
from connector.ccip.nodes import CcipDeps, dispatch_failure_alert
from connector.ccip.retry import RetryPolicy
from connector.ccip.runner import run_once
from connector.errors import ErrorCategory, structured_error

from .fakes import FakeCostTool, FakeEmailSender, FakeLLMClient


def _config(tmp_path, **kw):
    base = dict(
        metadata_path=str(tmp_path / "md.json"),
        # Increment 2: per-account store under metadata_dir; point at tmp_path for
        # test isolation (Req 14).
        metadata_dir=str(tmp_path),
        _secrets=CcipSecrets(llm_api_key="k", smtp_password="p"),
        retry_base_delay=0.0,
        retry_max_delay=0.0,
        retry_jitter=False,
    )
    base.update(kw)
    return CcipConfig(**base)


# --- Deterministic failure payload builders (Req 12.7) ---------------------


def test_build_failure_payload_pulls_from_state_and_error():
    state = {"account_id": "A", "target_date": "2026-09-29"}
    error = structured_error("boom", ErrorCategory.UNREACHABLE_SOURCE,
                             source="ingest", attempts=3)
    payload = build_failure_payload(state, error)
    assert payload == {
        "account_id": "A",
        "target_date": "2026-09-29",
        "stage": "ingest",
        "category": "unreachable-source",
        "message": "boom",
        "attempts": 3,
    }


def test_failure_subject_non_empty_and_bounded():
    payload = {"account_id": "A", "target_date": "2026-09-29", "category": "missing-data"}
    subject = build_failure_subject(payload)
    assert subject and len(subject) <= 200
    assert "FAILED" in subject and "missing-data" in subject


def test_failure_narrative_is_template_and_reports_fields():
    payload = {
        "account_id": "A", "target_date": "2026-09-29", "stage": "ingest",
        "category": "unreachable-source", "message": "cost api down", "attempts": 3,
    }
    body = render_failure_narrative(payload)
    assert "A" in body and "2026-09-29" in body
    assert "ingest" in body and "unreachable-source" in body
    assert "cost api down" in body and "3" in body


# --- dispatch_failure_alert node behavior (Req 12.6-12.11) -----------------


def _deps(tmp_path, *, failure_sender, llm=None):
    return CcipDeps(
        cost_tool=FakeCostTool({"granularity": "DAILY", "items": []}),
        llm_client=llm or FakeLLMClient(),
        email_sender=FakeEmailSender(),
        config=_config(tmp_path),
        failure_email_sender=failure_sender,
    )


async def test_node_noop_when_no_error(tmp_path):
    sender = FakeEmailSender(can_send=True)
    deps = _deps(tmp_path, failure_sender=sender)
    out = await dispatch_failure_alert({"account_id": "A", "target_date": "2026-09-29"}, deps)
    assert out == {}
    assert sender.sent == []


async def test_node_sends_when_can_send(tmp_path):
    sender = FakeEmailSender(can_send=True)
    deps = _deps(tmp_path, failure_sender=sender)
    state = {
        "account_id": "A", "target_date": "2026-09-29",
        "error": structured_error("boom", ErrorCategory.UNREACHABLE_SOURCE, source="ingest"),
    }
    out = await dispatch_failure_alert(state, deps)
    assert out["failure_alert_dispatched"] is True
    assert out["failure_alert_dry_run"] is False
    assert len(sender.sent) == 1


async def test_node_dry_runs_when_cannot_send(tmp_path):
    sender = FakeEmailSender(can_send=False)
    deps = _deps(tmp_path, failure_sender=sender)
    state = {
        "account_id": "A", "target_date": "2026-09-29",
        "error": structured_error("boom", ErrorCategory.MISSING_DATA, source="ingest"),
    }
    out = await dispatch_failure_alert(state, deps)
    assert out["failure_alert_dispatched"] is False
    assert out["failure_alert_dry_run"] is True


async def test_node_dry_runs_when_sender_missing(tmp_path):
    deps = CcipDeps(
        cost_tool=FakeCostTool({"granularity": "DAILY", "items": []}),
        llm_client=FakeLLMClient(),
        email_sender=FakeEmailSender(),
        config=_config(tmp_path),
        failure_email_sender=None,
    )
    state = {
        "account_id": "A", "target_date": "2026-09-29",
        "error": structured_error("boom", ErrorCategory.UNEXPECTED_ERROR),
    }
    out = await dispatch_failure_alert(state, deps)
    assert out["failure_alert_dry_run"] is True
    assert out["failure_alert_dispatched"] is False


async def test_node_best_effort_when_send_fails(tmp_path):
    # Failure sender raises -> delivery error is logged, not re-raised, not put on
    # the error channel, and NOT retried (Req 12.9).
    sender = FakeEmailSender(can_send=True, raise_exc=RuntimeError("smtp down"))
    deps = _deps(tmp_path, failure_sender=sender)
    err = structured_error("boom", ErrorCategory.UNREACHABLE_SOURCE, source="ingest")
    state = {"account_id": "A", "target_date": "2026-09-29", "error": err}
    out = await dispatch_failure_alert(state, deps)
    assert out["failure_alert_dispatched"] is False
    # The node returns only its own flags; it never clears the caller's error.
    assert "error" not in out


async def test_failure_alert_scrubs_secret(tmp_path):
    # A secret leaked into the error message must not reach the rendered alert.
    sender = FakeEmailSender(can_send=True)
    config = _config(tmp_path, _secrets=CcipSecrets(llm_api_key="k", smtp_password="topsecret"))
    deps = CcipDeps(
        cost_tool=FakeCostTool({"granularity": "DAILY", "items": []}),
        llm_client=FakeLLMClient(),
        email_sender=FakeEmailSender(),
        config=config,
        failure_email_sender=sender,
    )
    err = structured_error("boom leaked topsecret", ErrorCategory.UNREACHABLE_SOURCE, source="ingest")
    state = {"account_id": "A", "target_date": "2026-09-29", "error": err}
    await dispatch_failure_alert(state, deps)
    subject, body = sender.sent[0]
    assert "topsecret" not in subject
    assert "topsecret" not in body


async def test_failure_alert_does_not_use_llm(tmp_path):
    # The LLM must never be touched by the failure path (Req 12.7).
    llm = FakeLLMClient(raise_exc=AssertionError("LLM must not be called"))
    sender = FakeEmailSender(can_send=True)
    deps = _deps(tmp_path, failure_sender=sender, llm=llm)
    err = structured_error("boom", ErrorCategory.UNREACHABLE_SOURCE, source="ingest")
    state = {"account_id": "A", "target_date": "2026-09-29", "error": err}
    await dispatch_failure_alert(state, deps)
    assert llm.prompts == []  # never prompted


# --- Smoke: full graph routes any error through failure dispatch (Req 12.6) -


async def test_graph_transient_fetch_retries_then_failure_dispatch(tmp_path):
    calls = {"n": 0}

    async def always_down(start, end, granularity="DAILY"):
        calls["n"] += 1
        return structured_error("down", ErrorCategory.UNREACHABLE_SOURCE)

    failure = FakeEmailSender(can_send=True)
    deps = CcipDeps(
        cost_tool=always_down,
        llm_client=FakeLLMClient(),
        email_sender=FakeEmailSender(),
        config=_config(tmp_path),
        failure_email_sender=failure,
    )
    result = await run_once("A", "2026-09-29", deps)
    # Original error surfaces unchanged; retried to exhaustion (3 attempts).
    assert result["category"] == "unreachable-source"
    assert result["attempts"] == 3
    assert calls["n"] == 3
    # Failure alert went out exactly once.
    assert len(failure.sent) == 1
    subject, _ = failure.sent[0]
    assert "FAILED" in subject


async def test_graph_missing_data_reaches_failure_dispatch_without_retry(tmp_path):
    calls = {"n": 0}

    async def empty(start, end, granularity="DAILY"):
        calls["n"] += 1
        return {"granularity": "DAILY", "items": []}

    failure = FakeEmailSender(can_send=True)
    deps = CcipDeps(
        cost_tool=empty,
        llm_client=FakeLLMClient(),
        email_sender=FakeEmailSender(),
        config=_config(tmp_path),
        failure_email_sender=failure,
    )
    result = await run_once("A", "2026-09-29", deps)
    assert result["category"] == "missing-data"
    assert calls["n"] == 1  # deterministic -> no retry
    assert len(failure.sent) == 1


async def test_graph_failure_send_failure_is_non_fatal(tmp_path):
    # If the failure email itself fails, the run still returns the original error.
    async def down(start, end, granularity="DAILY"):
        return structured_error("down", ErrorCategory.UNREACHABLE_SOURCE)

    failure = FakeEmailSender(can_send=True, raise_exc=RuntimeError("smtp boom"))
    deps = CcipDeps(
        cost_tool=down,
        llm_client=FakeLLMClient(),
        email_sender=FakeEmailSender(),
        config=_config(tmp_path),
        failure_email_sender=failure,
    )
    result = await run_once("A", "2026-09-29", deps)
    assert result["category"] == "unreachable-source"
