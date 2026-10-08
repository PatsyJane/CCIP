"""CCIP LangGraph Anomaly Detection Workflow (spec: ccip-langgraph-anomaly).

A stateful, graph-orchestrated cloud cost anomaly detector built on LangGraph.
Six phases (determine_data_mode, ingest_daily_data, update_metadata_and_model,
detect_account_anomaly, drill_down_services, dispatch_alerts) plus two
conditional routers drive a shared ``CcipState`` from a manually supplied
``(account_id, target_date)`` through data-mode classification, ingestion,
metadata/model update, account-level STL anomaly detection, service drill-down,
and alert dispatch.

This package consumes cloud cost data through the preserved FastMCP Connector's
AWS CUR Cost_Tool (``connector.tools.aws_cur_cost``) and reuses the shared error
contract (``connector.errors``). It replaces the discarded
``connector.anomaly`` pipeline.
"""

from __future__ import annotations

__all__: list[str] = []
