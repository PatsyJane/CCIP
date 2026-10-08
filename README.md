# Cloud Cost Intelligence Pipeline (PoC)

Two spec-driven pieces:

1. **Cloud Billing & Telemetry Connector** — a FastMCP server hosting the single
   async MCP tool **AWS CUR Cost_Tool** (`aws_cur_cost`), which fetches cost data
   via `POST {COST_API_BASE_URL}/api/costs`, then validates, normalizes, and tags
   it. See `.kiro/specs/billing-usage-ingestion/`.
2. **CCIP LangGraph Anomaly Detection Workflow** — a stateful LangGraph graph that
   consumes the Cost_Tool, classifies the account's data mode, detects
   account-level cost anomalies with RobustSTL/STL, drills into service
   contributors, and dispatches a Gemini-written email alert. See
   `.kiro/specs/ccip-langgraph-anomaly/`.

## Layout

```
src/connector/
  server.py            # FastMCP bootstrap + wiring + run entry point
  config.py            # .env loader / AppConfig (fail-fast, credential redaction)
  http_client.py       # shared httpx.AsyncClient wrapper
  validation.py        # date / range / granularity validation
  dimensions.py        # account/service/environment tagging + normalization
  numeric.py           # Cost_Value coercion + range enforcement
  errors.py            # structured-error factory + category enum
  response_adapter.py  # defensive Cost_API response parsing
  tools/aws_cur_cost.py  # the AWS CUR Cost_Tool pipeline
  ccip/                # the CCIP LangGraph anomaly workflow
    config.py          # config.yaml + ${VAR} loader / CcipConfig (redacted secrets)
    state.py           # CcipState TypedDict + initial-state builder
    models.py          # frozen domain models + DataMode enum
    history.py         # history counting + data-mode classification
    metadata_store.py  # single JSON store, atomic writes
    ingest.py          # Phase 1 fetch + Phase 2 pure normalization
    stl_model.py       # statsmodels STL + residual thresholds
    detection.py       # cold-start + statistical account detection
    drilldown.py       # service contributor isolation + seasonal context
    narrative.py       # Gemini narrative + deterministic fallback
    llm_client.py      # Gemini (LangChain) adapter
    email.py           # payload build + SMTP delivery (dry-run fallback)
    nodes.py           # the six LangGraph nodes
    routers.py         # the two conditional routers
    graph.py           # build_graph(): the compiled StateGraph
    redaction.py       # secret scrubber
    runner.py          # dependency wiring + run_once
config.yaml            # CCIP thresholds + STL params (${VAR} injected from .env)
scripts/run_ccip_check.py   # CCIP workflow CLI
tests/                 # pytest + pytest-asyncio + Hypothesis
```

## Setup

```bash
python -m venv .venv
# Windows: .venv\Scripts\Activate.ps1   |   Unix: source .venv/bin/activate
pip install -e ".[dev]"
cp .env.example .env    # then fill in the required values
```

## Run

```bash
pytest                                              # all tests
connector                                           # the MCP server
python -m connector.server                          # equivalent entry point
python scripts/run_ccip_check.py --account-id A --no-live   # CCIP, zero network
python scripts/run_ccip_check.py --account-id A             # live CCIP run (.env + config.yaml)
```

## Configuration

- Connector keys live in `.env` (see `.env.example`): `AWS_ACCOUNT_ID`,
  `AWS_ACCOUNT_NAME`, `AWS_REGION`, `COST_API_BASE_URL`, `PROVIDER`, `PROJECT`,
  plus optional `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`.
- CCIP thresholds and STL parameters live in `config.yaml`; secrets and
  environment-specific values are injected from `.env` via `${VAR}` placeholders
  (`CCIP_LLM_API_KEY`, `CCIP_SMTP_*`, etc.). If SMTP is not fully configured the
  email stage performs a safe dry run (logs the alert, sends nothing).
- Secrets never appear in logs, `repr`, or node output; every returned error is
  secret-scrubbed.
```
