"""Unit / example-edge tests for the CCIP deterministic modules.

Feature: ccip-langgraph-anomaly. External dependencies are faked; no test here
performs a network, SMTP, or external-filesystem call (writes go to tmp_path).
"""

from __future__ import annotations

import os

import pytest

from connector.ccip import config as cfg
from connector.ccip.config import CcipConfig, CcipSecrets, ConfigError, resolve_env_placeholders
from connector.ccip.detection import detect_cold_start, detect_statistical, most_recent_non_anomaly_day
from connector.ccip.drilldown import isolate_contributors, seasonal_context_for
from connector.ccip.history import count_available_days
from connector.ccip.metadata_store import default_metadata, load_metadata, save_metadata
from connector.ccip.models import DataMode, classify_data_mode, is_above_cold_start
from connector.ccip.routers import route_after_detection, route_alert_check
from connector.ccip.state import build_initial_state
from connector.ccip.stl_model import StlResult, compute_residual_threshold


# --- Data-mode boundaries (Req 3.2) ---------------------------------------


@pytest.mark.parametrize(
    "days,mode",
    [
        (0, DataMode.COLD_START), (29, DataMode.COLD_START),
        (30, DataMode.INITIAL), (59, DataMode.INITIAL),
        (60, DataMode.BEGINNER), (89, DataMode.BEGINNER),
        (90, DataMode.NORMAL), (400, DataMode.NORMAL),
    ],
)
def test_data_mode_boundaries(days, mode):
    assert classify_data_mode(days) is mode


def test_is_above_cold_start():
    assert not is_above_cold_start(DataMode.COLD_START)
    assert is_above_cold_start(DataMode.INITIAL)
    assert is_above_cold_start(DataMode.NORMAL)


def test_count_available_days_distinct():
    series = [{"day": "2026-01-01", "cost": 1.0}, {"day": "2026-01-01", "cost": 2.0},
              {"day": "2026-01-02", "cost": 3.0}]
    assert count_available_days(series) == 2


# --- Config ${VAR} injection + redaction (Req 1.2, 1.3, 1.5) --------------


def test_resolve_placeholders_env_and_default(monkeypatch):
    monkeypatch.setenv("MYVAR", "resolved")
    text, missing = resolve_env_placeholders("a=${MYVAR} b=${OTHER:-fallback} c=${EMPTY:-}")
    assert "a=resolved" in text
    assert "b=fallback" in text
    assert "c=" in text
    assert missing == []


def test_resolve_placeholders_required_missing(monkeypatch):
    monkeypatch.delenv("NO_SUCH", raising=False)
    _text, missing = resolve_env_placeholders("x=${NO_SUCH}")
    assert missing == ["NO_SUCH"]


def test_load_config_required_missing(tmp_path, monkeypatch):
    # Neutralize load_dotenv so the developer's real .env cannot repopulate the
    # variable this test deliberately removes (test isolation).
    monkeypatch.setattr(cfg, "load_dotenv", lambda *a, **k: False)
    monkeypatch.delenv("CCIP_LLM_API_KEY", raising=False)
    path = tmp_path / "config.yaml"
    path.write_text("llm:\n  api_key: ${CCIP_LLM_API_KEY}\n", encoding="utf-8")
    with pytest.raises(ConfigError) as exc:
        cfg.load_ccip_config(str(path))
    assert "CCIP_LLM_API_KEY" in str(exc.value)


def test_load_config_missing_file(tmp_path):
    with pytest.raises(ConfigError):
        cfg.load_ccip_config(str(tmp_path / "nope.yaml"))


def test_load_config_values_and_redaction(tmp_path, monkeypatch):
    monkeypatch.setattr(cfg, "load_dotenv", lambda *a, **k: False)
    monkeypatch.setenv("CCIP_LLM_API_KEY", "secret-key")
    monkeypatch.setenv("CCIP_SMTP_PASSWORD", "secret-pw")
    path = tmp_path / "config.yaml"
    path.write_text(
        "detection:\n  cold_start_absolute_threshold: 25\n  statistical_method: sigma\n"
        "stl:\n  period: 7\n"
        "llm:\n  model: m1\n  api_key: ${CCIP_LLM_API_KEY}\n"
        "email:\n  smtp_password: ${CCIP_SMTP_PASSWORD}\n",
        encoding="utf-8",
    )
    config = cfg.load_ccip_config(str(path))
    assert config.cold_start_absolute_threshold == 25.0
    assert config.statistical_method == "sigma"
    assert config.llm_model == "m1"
    assert config.secrets.llm_api_key == "secret-key"
    # Secrets never appear in repr (Req 1.5).
    assert "secret-key" not in repr(config)
    assert "secret-pw" not in repr(config.secrets)


# --- Metadata store (Req 5.1, 5.7, 5.8) -----------------------------------


def test_metadata_absent_returns_default(tmp_path):
    md = load_metadata(str(tmp_path / "missing.json"))
    assert md == default_metadata()


def test_metadata_roundtrip_and_atomic(tmp_path):
    path = str(tmp_path / "md.json")
    md = default_metadata()
    md["known_active_services"] = ["ec2"]
    assert save_metadata(path, md) is None
    assert os.path.exists(path)
    assert not os.path.exists(path + ".tmp")  # temp cleaned up
    reloaded = load_metadata(path)
    assert reloaded["known_active_services"] == ["ec2"]


def test_metadata_invalid_json_is_error(tmp_path):
    path = tmp_path / "bad.json"
    path.write_text("{not valid json", encoding="utf-8")
    result = load_metadata(str(path))
    assert result.get("category") == "unexpected-error"


# --- STL threshold math (Req 6.3) -----------------------------------------


def test_residual_threshold_iqr_and_sigma():
    resid = [0.0, 1.0, 2.0, 3.0, 4.0]
    iqr = compute_residual_threshold(resid, "iqr", 1.5, 3.0)
    sigma = compute_residual_threshold(resid, "sigma", 1.5, 3.0)
    assert iqr > 0
    assert sigma > 0


# --- Detection rules (Req 6.2, 6.3, 6.6) ----------------------------------


def test_cold_start_no_prior_day_returns_false():
    detected, baseline = detect_cold_start(1000.0, default_metadata(), "2026-09-29", 20.0)
    assert detected is False and baseline is None


def test_cold_start_absolute_threshold():
    md = default_metadata()
    md["non_anomaly_days"] = {"2026-09-20": 100.0}
    detected, baseline = detect_cold_start(130.0, md, "2026-09-29", 20.0)
    assert detected is True and baseline == "2026-09-20"
    detected2, _ = detect_cold_start(110.0, md, "2026-09-29", 20.0)
    assert detected2 is False


def test_most_recent_non_anomaly_day_picks_latest_before():
    md = {"non_anomaly_days": {"2026-09-10": 1.0, "2026-09-20": 2.0, "2026-09-29": 3.0}}
    assert most_recent_non_anomaly_day(md, "2026-09-29") == ("2026-09-20", 2.0)


def test_detect_statistical_rule():
    r = StlResult([], [], [], residual_threshold=5.0, target_residual=10.0,
                  expected_target=0.0, method="iqr", seasonal_summary={})
    assert detect_statistical(r) is True
    r2 = StlResult([], [], [], residual_threshold=5.0, target_residual=1.0,
                   expected_target=0.0, method="iqr", seasonal_summary={})
    assert detect_statistical(r2) is False


# --- Drill-down (Req 8.3, 8.4) --------------------------------------------


def test_isolate_contributors_orders_by_delta_desc():
    service_costs = {"ec2": 100.0, "s3": 50.0, "rds": 5.0}
    service_history = {
        "ec2": [{"day": "2026-09-20", "cost": 10.0}],
        "s3": [{"day": "2026-09-20", "cost": 10.0}],
        "rds": [{"day": "2026-09-20", "cost": 10.0}],  # negative delta -> excluded
    }
    out = isolate_contributors(service_costs, service_history, "2026-09-20", "2026-09-29", {})
    assert [c["service"] for c in out] == ["ec2", "s3"]
    assert out[0]["delta"] == 90.0


def test_seasonal_context_first_of_month():
    assert seasonal_context_for("2026-09-01", {}) is not None
    assert seasonal_context_for("2026-09-15", {}) is None


# --- Routing (Req 7) ------------------------------------------------------


def test_route_after_detection():
    assert route_after_detection({"account_anomaly_detected": True}) == "drill_down_services"
    assert route_after_detection({"account_anomaly_detected": False}) == "alert_check"
    assert route_after_detection({"error": {"error": "x"}}) == "END"


def test_route_alert_check():
    assert route_alert_check({"account_anomaly_detected": True}) == "dispatch_alerts"
    assert route_alert_check({"new_services_detected": ["x"]}) == "dispatch_alerts"
    assert route_alert_check({"account_anomaly_detected": False, "new_services_detected": []}) == "END"
    assert route_alert_check({"error": {"error": "x"}}) == "END"


# --- State init (Req 2.3) -------------------------------------------------


def test_build_initial_state_valid():
    s = build_initial_state("acct", "2026-09-29")
    assert s["error"] is None
    assert s["account_id"] == "acct"
    assert s["new_services_detected"] == []
    assert s["account_anomaly_detected"] is False


def test_build_initial_state_bad_account():
    s = build_initial_state("   ", "2026-09-29")
    assert s["error"]["category"] == "invalid-parameter"


def test_build_initial_state_bad_date():
    s = build_initial_state("acct", "2026-13-40")
    assert s["error"]["category"] == "invalid-parameter"
