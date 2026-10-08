# Product

**Cloud Cost Intelligence Pipeline** — a Proof of Concept that ingests cloud
billing/usage data and detects material cost anomalies, alerting humans via
email with LLM-generated explanations.

The repository currently contains two spec-driven pipelines, both documented
under `.kiro/specs/`:

## 1. Billing & Usage Ingestion (`billing-usage-ingestion`)

A **FastMCP server** — the *Cloud Billing & Telemetry Connector* — that exposes
a single MCP tool, the **AWS CUR Cost_Tool** (`aws_cur_cost`). The tool fetches
cost data from an external Cost_API via `POST {COST_API_BASE_URL}/api/costs`,
then validates, normalizes, and tags the results before returning them to the
calling Agent.

Key behaviors:
- Validates date / range / granularity inputs before calling the Cost_API.
- Tags results with account / service / environment Dimension_Tags.
- Coerces and range-checks numeric Cost_Value fields.
- Returns **Structured_Error** objects (never raises/crashes) with a bounded
  `category` set: `missing-data`, `unreachable-source`, `invalid-parameter`,
  `unexpected-error`.
- Fails fast at startup on missing config or tool-registration failure, serving
  no tools and exiting non-zero.

## 2. CCIP LangGraph Anomaly Detection (`ccip-langgraph-anomaly`)

A stateful **LangGraph** workflow (`src/connector/ccip/`) that supersedes the
former linear anomaly pipeline. Six phases + two conditional routers drive a
shared `CcipState` from a `(account_id, target_date)` through:

1. `determine_data_mode` — the single MCP `aws_cur_cost` fetch (90-day window),
   count complete days, classify Data_Mode (`Cold Start` <30 / `Initial` 30-59 /
   `Beginner` 60-89 / `Normal` >=90).
2. `ingest_daily_data` — pure normalization of the fetched payload (no network).
3. `update_metadata_and_model` — detect/persist new services; run RobustSTL/STL
   (statsmodels) on the account series when above Cold Start.
4. `detect_account_anomaly` — Cold Start: absolute $ diff vs the last non-anomaly
   day; else: STL residual vs a statistical threshold (IQR or sigma).
5. `drill_down_services` — isolate contributor services + seasonal context.
6. `dispatch_alerts` — Gemini-written narrative (deterministic fallback) over
   SMTP (safe dry run when SMTP is unconfigured).

A single JSON Metadata_Store (atomic writes) tracks known services, seasonal
context, and the last anomaly. Thresholds/STL params live in `config.yaml` with
`${VAR}` injection from `.env`. Run via `scripts/run_ccip_check.py`
(`--account-id`, `--target-date`, `--no-live`).

## Guiding principles
- This is a **PoC** — prefer clear, correct, well-documented code over premature
  optimization or extra abstraction.
- Requirements are authoritative. Source files cite requirement IDs (e.g.
  `Req 12.4`) in docstrings/comments; preserve and respect those references.
- **Never** log, return, or otherwise expose secrets (AWS credentials, API keys,
  SMTP passwords) in tool output or logs.
