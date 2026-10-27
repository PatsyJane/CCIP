"""Unit tests for the ingestion stage and the secret scrubber.

Feature: ccip-langgraph-anomaly.
"""

from __future__ import annotations

from datetime import date

from connector.ccip.ingest import fetch_cost_payload, normalize_payload
from connector.ccip.redaction import scrub_secrets

from .fakes import FakeCostTool, make_daily_payload


async def test_fetch_single_call_well_formed():
    tool = FakeCostTool({"granularity": "DAILY", "items": [{"timestamp": "2026-09-29",
                        "cost_value": 1.0, "tags": {"service": "ec2"}}]})
    out = await fetch_cost_payload(tool, date(2026, 9, 29), history_days=90)
    assert out["granularity"] == "DAILY"
    assert len(tool.calls) == 1
    start, end, gran = tool.calls[0]
    assert gran == "DAILY"
    assert end == "2026-09-29"
    assert start == "2026-07-01"  # 90 days before


async def test_fetch_preserves_cost_tool_error():
    tool = FakeCostTool({"error": "boom", "category": "unreachable-source"})
    out = await fetch_cost_payload(tool, date(2026, 9, 29))
    assert out["category"] == "unreachable-source"
    assert out["source"] == "ingest"


async def test_fetch_rejects_non_daily():
    tool = FakeCostTool({"granularity": "HOURLY", "items": [{"timestamp": "x", "cost_value": 1}]})
    out = await fetch_cost_payload(tool, date(2026, 9, 29))
    assert out["category"] == "invalid-parameter"


async def test_fetch_empty_is_missing_data():
    tool = FakeCostTool({"granularity": "DAILY", "items": []})
    out = await fetch_cost_payload(tool, date(2026, 9, 29))
    assert out["category"] == "missing-data"


def test_normalize_payload_sums_and_groups():
    payload = make_daily_payload(
        {"ec2": [10.0, 20.0, 30.0], "s3": [1.0, 2.0, 3.0]}, end_day="2026-09-29"
    )
    out = normalize_payload(payload, "2026-09-29")
    assert out["account_total_cost"] == 33.0  # 30 + 3 on the target day
    assert out["service_costs"] == {"ec2": 30.0, "s3": 3.0}
    # account_history account total per day ascending
    assert out["account_history"][0] == {"day": "2026-09-27", "cost": 11.0}
    assert out["account_history"][-1] == {"day": "2026-09-29", "cost": 33.0}
    assert len(out["service_history"]["ec2"]) == 3


def test_normalize_malformed_payload_is_error():
    out = normalize_payload({"no_items": True}, "2026-09-29")
    assert out["category"] == "unexpected-error"


def test_normalize_absent_target_imputes_zero():
    # Target day has no items (series ends before target).
    payload = make_daily_payload({"ec2": [10.0, 20.0]}, end_day="2026-09-20")
    out = normalize_payload(payload, "2026-09-29")
    assert out["account_total_cost"] == 0.0


def test_scrub_secrets_nested():
    err = {"error": "key=abc123 failed", "extra": {"note": "pw=topsecret"}, "list": ["abc123"]}
    scrubbed = scrub_secrets(err, ["abc123", "topsecret"])
    assert "abc123" not in str(scrubbed)
    assert "topsecret" not in str(scrubbed)
    # Original untouched.
    assert "abc123" in err["error"]


def test_scrub_secrets_ignores_blank():
    err = {"error": "hello"}
    assert scrub_secrets(err, ["", "   "]) == {"error": "hello"}


def test_smtp_sender_force_dry_run_overrides_configured_send():
    from connector.ccip.email import SmtpEmailSender

    # Fully configured SMTP would normally send...
    live = SmtpEmailSender(host="smtp.example.com", port=587, username="u",
                           password="pw", email_from="a@b.c", email_to="d@e.f")
    assert live.can_send is True
    # ...but force_dry_run flips can_send to False so delivery logs only.
    dry = SmtpEmailSender(host="smtp.example.com", port=587, username="u",
                          password="pw", email_from="a@b.c", email_to="d@e.f",
                          force_dry_run=True)
    assert dry.can_send is False


def test_scrub_secrets_ignores_too_short():
    # A 1-char "secret" must NOT corrupt ordinary words (regression: a single
    # 'p' once turned 'unexpected-error' into 'unex***ected-error').
    err = {"error": "unexpected-error", "category": "unexpected-error"}
    assert scrub_secrets(err, ["p", "x"]) == err
