# Cloud Cost Intelligence Pipeline — Project Overview

A detailed explanation of everything built so far. This is a **Proof of Concept
(PoC)** that ingests cloud billing/usage data and detects material cost
anomalies, then alerts a human by email with an LLM-generated explanation.

> Status: both features' core implementation is complete and importable. The full
> test suite (`pytest`) is **100 tests passing**. Feature 2 was redesigned from a
> linear pipeline into the **CCIP Archi-3 LangGraph workflow**; the old
> `connector.anomaly` package has been discarded. The workflow has since been
> hardened with bounded **fetch retry**, a terminal **failure-notification**
> alert, and shared **structured JSON logging** (see §4.6).

---

## 1. The big picture

Two spec-driven pieces stack on top of each other:

```
   AWS Cost_API ◄──POST── Feature 1: Billing & Usage Ingestion
 (/api/costs, HTTP)        FastMCP server + AWS CUR Cost_Tool (aws_cur_cost)
                                  │ {granularity, items[]}
                                  ▼
                          Feature 2: CCIP LangGraph Anomaly Workflow
                          determine_data_mode (fetch + retry) → ingest
                          → update_metadata/model → detect_account_anomaly
                          → [router] → drill_down → [router] → dispatch_alerts
                                  │                      │
                            Gemini (narrative)      SMTP (alert / dry-run)
                                  │
                          single JSON Metadata_Store (known services, seasonal,
                          last anomaly, non-anomaly baseline days)

   any node error ───────► dispatch_failure_alert (deterministic, no LLM)
                                  │
                            SMTP ops recipient (failure alert / dry-run)
```

Every stage emits **structured JSON logs** (one object per line, secrets scrubbed)
through the shared `connector.logging_config` module.

- **Feature 1 — Billing & Usage Ingestion** (`billing-usage-ingestion`): a
  FastMCP server (the *Cloud Billing & Telemetry Connector*) exposing one MCP
  tool, the **AWS CUR Cost_Tool** (`aws_cur_cost`), which fetches cost data from
  an external Cost_API and validates, normalizes, and tags it. **Unchanged** by
  the redesign.
- **Feature 2 — CCIP LangGraph Anomaly Detection** (`ccip-langgraph-anomaly`): a
  stateful LangGraph workflow that consumes the Cost_Tool, classifies the
  account's data mode, detects account-level anomalies with STL, drills into
  service contributors, and dispatches a Gemini-written email alert.

Both are driven by authoritative specs under `.kiro/specs/<feature>/`
(`requirements.md`, `design.md`, `tasks.md`). Source modules cite requirement IDs
(e.g. `Req 6.3`) in docstrings.

### Guiding principles baked into the code
- **Deterministic math before the LLM** — all detection arithmetic is pure
  Python/`statsmodels`; the LLM only writes the email narrative and never changes
  a number.
- **Errors over exceptions at boundaries** — tools and nodes return a
  `Structured_Error` dict instead of raising. The system never crashes.
- **Secret safety** — credentials/keys/passwords never appear in logs, `repr`, or
  output. Config models redact secrets, a scrubber sweeps every returned error,
  and the JSON log formatter runs every record through the same scrubber against a
  process-wide secret registry.
- **Immutability** — domain models are `@dataclass(frozen=True)`.

---

## 2. Tech stack

| Area | Choice |
|------|--------|
| Language / runtime | Python ≥ 3.11, `src/` layout, package `connector` |
| MCP server | `fastmcp>=2.12,<3` |
| HTTP client | `httpx>=0.28,<0.29` (async) |
| Graph orchestration | `langgraph>=0.2,<2` (StateGraph) |
| Statistics | `statsmodels>=0.14,<0.15` (STL decomposition) |
| Config | `python-dotenv>=1.0,<2` + `pyyaml>=6,<7` (`config.yaml` with `${VAR}`) |
| LLM adapter | `langchain-google-genai>=2,<3` (Gemini) |
| Tests | `pytest`, `pytest-asyncio` (`asyncio_mode=auto`), `hypothesis`, `respx` |

---

## 3. Feature 1 — Billing & Usage Ingestion (the Connector)

A FastMCP server hosting one async MCP tool that runs a fixed, **total** pipeline
per call (always returns a success payload or a `Structured_Error`, never raises):

```
validate → build request → POST (shared client) → parse/adapt
        → numeric coerce/validate → tag/normalize → {granularity, items}
```

Modules (each maps to a numbered design component):
- `errors.py` — the error contract: `ErrorCategory` (`missing-data`,
  `unreachable-source`, `invalid-parameter`, `unexpected-error`) + `structured_error`.
- `config.py` — `.env` loader, fail-fast on the six required keys, redacted creds.
- `validation.py` — strict `YYYY-MM-DD`, range checks, granularity decomposition.
- `numeric.py` — Cost_Value coercion into `0.00`–`999,999,999.99`.
- `dimensions.py` — account/service/environment tagging (`UNKNOWN` fallback).
- `http_client.py` — one shared `httpx.AsyncClient` (30s timeout).
- `response_adapter.py` — defensive Cost_API parsing isolating wire-format assumptions.
- `tools/aws_cur_cost.py` — the tool pipeline + `make_aws_cur_cost(config, http)`.
- `server.py` — `FastMCP("Cloud Billing & Telemetry Connector")` bootstrap; it
  configures structured logging at startup and logs startup / tool-registration /
  abort events as JSON.
- `logging_config.py` — the shared structured JSON logger used by *both*
  pipelines (`configure_logging`, `log_event`, `log_structured_error`, secret
  scrubbing). See §4.6.

Success payload:
`{"granularity": "DAILY", "items": [{"timestamp", "cost_value", "tags": {account, service, environment}}]}`.

Only the AWS CUR Cost_Tool is implemented; GCP/CloudWatch tools are deferred.

---

## 4. Feature 2 — CCIP LangGraph Anomaly Detection

A stateful LangGraph `StateGraph`: six phase nodes + a terminal
`dispatch_failure_alert` node, driven by conditional routers over a shared
`CcipState` (a `TypedDict`). Errors travel on `state["error"]`; every phase node
short-circuits when it is set, so the first error becomes the final result, and
the routers send that error to the terminal failure node before `END`.

### 4.1 Graph topology

```
START → determine_data_mode → ingest_daily_data → update_metadata_and_model
      → detect_account_anomaly → [Anomaly-Check router]
            True  → drill_down_services → [Alert-Check router]
            False → _alert_check        → [Alert-Check router]
      → [Alert-Check router]
            anomaly OR new services → dispatch_alerts → END
            else                    → END

      (any node error on state["error"]) → dispatch_failure_alert → END
```

The `_alert_check` pass-through node lets both branches share one Alert-Check
router within LangGraph's node-bound edge model.

Error routing changed with Req 12: instead of routing straight to `END`, a
Structured_Error on `state["error"]` now routes to the terminal
`dispatch_failure_alert` node (the early sequential edges became conditional
edges too, so an error in any phase reaches it). That node sends a best-effort
failure email and then goes to `END`; the **original error stays the run
result** unchanged, so the CLI still exits non-zero.

### 4.2 The six phases (plus a terminal failure node)

1. **`determine_data_mode`** — makes the **single** MCP `aws_cur_cost` fetch for
   the run (one `DAILY` call over the 90-day window ending at the Target_Date),
   stores the raw payload on the state, counts distinct complete days, and
   classifies the Data_Mode: `Cold Start` (<30), `Initial` (30–59),
   `Beginner` (60–89), `Normal` (≥90). The fetch is wrapped in a **bounded
   exponential-backoff-with-jitter retry** that re-attempts only transient
   `unreachable-source` failures (see §4.6); deterministic errors are returned on
   the first occurrence.
2. **`ingest_daily_data`** — PURE normalization of that payload (no network):
   `account_total_cost`, `service_costs`, `account_history`, `service_history`.
3. **`update_metadata_and_model`** — loads the JSON Metadata_Store, detects and
   immediately persists new services, and (above Cold Start) runs STL and
   persists the seasonal summary. A **New_Service** is any service seen anywhere
   in the fetched 90-day history window (not just on the Target_Date) that is not
   already known; the synthetic `UNKNOWN` tagging bucket is never treated as a
   new service nor persisted.
4. **`detect_account_anomaly`** — Cold Start: absolute `$` diff vs the most recent
   non-anomaly day (> threshold, default `$20`). Initial/Beginner/Normal: STL
   residual vs a statistical threshold (IQR `Q3+1.5·IQR`, or `mean+3σ`). Always
   records a non-anomaly baseline day for future comparisons.
5. **`drill_down_services`** — (only on an account anomaly) per-service delta vs
   the baseline day, positive contributors ordered by descending delta, annotated
   with "1st-of-the-month" seasonal context.
6. **`dispatch_alerts`** — (only if anomaly OR new services) builds the
   deterministic payload (incl. the last-anomaly record), asks Gemini for a
   narrative (deterministic templated fallback on failure), delivers over SMTP
   (safe dry run when SMTP is unconfigured), and records this run's anomaly.

**`dispatch_failure_alert`** (terminal failure node) — runs **only** when
`state["error"]` is set. Unlike the six phase nodes, it does its work *because*
an error is present: it builds a **deterministic** failure payload (account id,
target date, failing stage/source, error category, scrubbed message, retry
attempts made) **without the LLM** — the LLM may itself be the thing that failed
— and delivers it best-effort over SMTP to a **separate ops recipient**
(`CCIP_ALERT_FAILURE_TO`), dry-running when that recipient or SMTP is
unconfigured. It never clears `state["error"]`, never retries its own send, and
never re-enters failure handling, so handling is non-recursive and the original
error remains the run result.

### 4.3 Key detail — STL fits on history excluding the Target_Date

STL is fit on the history **without** the evaluation day, then the expected
Target_Date value is reconstructed (last trend level + matching seasonal phase)
and the residual is `actual − expected`. This avoids a robust STL fit absorbing
the very spike it should flag. Verified on noisy data: a large spike is detected
while a typical day is not.

### 4.4 Supporting modules (`src/connector/ccip/`)

`config.py` (YAML + `${VAR}`/`${VAR:-default}` loader, redacted secrets),
`state.py` (`CcipState` + `build_initial_state` input validation), `models.py`
(frozen models + `DataMode`), `history.py`, `metadata_store.py` (atomic
temp-file + `os.replace` writes), `ingest.py` (`fetch_cost_payload` +
`normalize_payload`), `stl_model.py`, `detection.py`, `drilldown.py`,
`narrative.py`, `llm_client.py` (Gemini via LangChain), `email.py`
(`SmtpEmailSender` with dry-run, plus the deterministic failure-alert builders
`build_failure_payload` / `build_failure_subject` / `render_failure_narrative`),
`retry.py` (bounded backoff retry helper — `RetryPolicy`, `is_retryable`,
`retry_async`), `nodes.py` (the six phase nodes + `dispatch_failure_alert`),
`routers.py` (routers that send errors to failure dispatch), `graph.py`,
`redaction.py`, `runner.py` (`build_live_deps` / `build_fake_deps` / `run_once`,
wiring both the anomaly and failure email senders plus the `RetryPolicy`).

### 4.5 CLI

`scripts/run_ccip_check.py --account-id A [--target-date YYYY-MM-DD]
[--no-live] [--config config.yaml]`. Default date is yesterday; `--no-live` uses
in-memory fakes (zero network); exits non-zero on a Structured_Error. The
printout surfaces whether a failure alert was dispatched (or dry-run), but the
exit code is driven solely by the Structured_Error — failure notification is
additive and never masks it.

### 4.6 Fetch retry + structured logging (hardening)

**Fetch retry (`retry.py`, Req 12.1–12.5).** The single Phase 1 fetch is wrapped
in `retry_async(operation, policy)`. The policy is a frozen `RetryPolicy`
(`max_attempts=3`, `base_delay=1.0s`, `max_delay=30.0s`, `multiplier=2.0`,
`jitter=True`), built from config. Only `unreachable-source` errors are retried;
every success and every deterministic error category returns on the first
occurrence. The delay for attempt *k* is
`min(max_delay, base_delay · multiplier^(k−1))`, with full jitter drawn from
`[0, delay]` when enabled. Sleeps happen strictly **between** attempts, each
still bounded by the shared 30s `HttpClient` timeout. On exhaustion the last
`unreachable-source` error is returned with an `attempts` count stamped on it for
the failure alert. The `sleep` callable is injectable, so tests never actually
wait (fakes use a zero-delay policy).

**Structured JSON logging (`connector.logging_config`).** One shared module
configures logging for *both* pipelines so every line is a single JSON object
(`timestamp`, `level`, `logger`, `module`, `event`, optional `payload`). Nodes
log the deterministic math/state (the exact numbers used in an evaluation) via
`log_event(..., payload={...})`, and Structured_Errors are mirrored with
`log_structured_error`. A process-wide secret registry feeds the formatter, which
runs every record through `ccip/redaction.scrub_secrets` so no credential, API
key, or SMTP password can reach a sink — layered on top of the `repr` redaction
in the config models. Logs go to `stderr` (keeping `stdout` clean for the MCP
stdio transport / CLI), and verbosity comes from `LOG_LEVEL` (default `INFO`,
unknown values fall back to `INFO` rather than crashing). It is stdlib-only — no
new dependency.

---

## 5. Shared error model

Every tool and node returns, on failure, `{"error", "category", ...extra}` with
`category` from the four `ErrorCategory` values. Callers branch on
`isinstance(result, dict)`. This uniform contract makes the whole system total.

---

## 6. Configuration

- **Connector** (`.env`): `AWS_ACCOUNT_ID`, `AWS_ACCOUNT_NAME`, `AWS_REGION`,
  `COST_API_BASE_URL`, `PROVIDER`, `PROJECT` (required), plus optional AWS creds.
- **CCIP** (`config.yaml` with `${VAR}` from `.env`): detection thresholds, STL
  params, ingestion window, LLM model, email/SMTP settings, metadata path, plus
  the failure-alert recipient (`CCIP_ALERT_FAILURE_TO`) and the fetch-retry knobs
  (`CCIP_RETRY_MAX_ATTEMPTS`, `CCIP_RETRY_BASE_DELAY`, `CCIP_RETRY_MAX_DELAY`,
  `CCIP_RETRY_MULTIPLIER`, `CCIP_RETRY_JITTER`). Secrets (`CCIP_LLM_API_KEY`,
  `CCIP_SMTP_PASSWORD`) are redacted from `repr` and scrubbed from errors.
  Unconfigured SMTP (or a blank failure recipient) → safe dry run.
- **Logging** (optional, shared): `LOG_LEVEL` (`DEBUG`/`INFO`/`WARNING`/`ERROR`/
  `CRITICAL`, default `INFO`). Never a startup-abort key; an unknown value falls
  back to `INFO`. Secrets are scrubbed from logs regardless of level.

---

## 7. Testing & verification

- `pytest` with `asyncio_mode="auto"`, `pythonpath=["src"]`.
- CCIP suites under `tests/ccip/`: `test_units.py` (config injection, data-mode
  boundaries, metadata atomicity, detection, drill-down, routing, state),
  `test_ingest_units.py` (single-fetch contract, normalization, scrubber),
  `test_retry_units.py` (retry decision + backoff schedule, non-retryable
  categories never retried, exhaustion stamps `attempts`),
  `test_failure_dispatch_units.py` (the failure payload is LLM-free and
  secret-scrubbed, dry-run vs send, non-recursive delivery failure),
  `test_properties.py` (Hypothesis properties for data-mode, detection, routing,
  thresholds, retry, and failure dispatch — Properties 18–21), `test_graph_smoke.py`
  (end-to-end graph runs with fakes: happy path, new-service trigger, each error
  category, LLM fallback, no-op routing, secret scrubbing, and an
  `unreachable-source` fetch that retries then reaches a dry-run failure alert).
- All external I/O is faked; no unit/property/smoke test performs a real network,
  SMTP, or external-filesystem call (metadata writes go to `tmp_path`).

- `test_integration_real_cost_tool.py` — the **real** `aws_cur_cost` pipeline
  wired through an `httpx.MockTransport` Cost_API (LLM/SMTP still fakes): a
  Normal-mode happy path to a dry-run alert plus one case per error category
  (`invalid-parameter`, `unreachable-source`, `missing-data`, `unexpected-error`).

**Current result:** `pytest` passes **100 tests** (0 failures) — the Connector
scaffolding test plus the CCIP unit/property/graph/integration suites (including
the retry and failure-dispatch tests).

Common commands:

```powershell
pytest                                                      # all tests
pytest tests/ccip                                           # CCIP subset
connector                                                   # the MCP server
python scripts/run_ccip_check.py --account-id A --no-live   # CCIP, zero network
python scripts/run_ccip_check.py --account-id A             # live CCIP run
```

---

## 8. How far along is it?

**Feature 1 — Billing & Usage Ingestion:** complete; all nine design components
implemented and preserved through the redesign.

**Feature 2 — CCIP LangGraph Anomaly Detection:** complete — the six phase nodes
plus the terminal `dispatch_failure_alert` node, the conditional routers, graph
assembly, real Gemini/SMTP adapters, and the CLI. It has since been hardened
(spec Req 12 / task 18) with bounded fetch retry (`retry.py`), the deterministic
failure-notification alert, and shared structured JSON logging
(`logging_config.py`). Unit + property + end-to-end-with-fakes + real-`aws_cur_cost`
integration tests (over `httpx.MockTransport`) all pass. The only unverified path
is a genuine live run against real AWS/Gemini/SMTP (needs real `.env` /
`config.yaml` credentials).

**Discarded:** the former `src/connector/anomaly/` linear pipeline,
`tests/anomaly/`, and `scripts/run_anomaly_check.py` (replaced wholesale).

**Deferred (out of scope for the PoC):** GCP/Azure, automated remediation,
multi-account batch scheduling, database-backed metadata.

### Suggested next steps
1. Do a guarded live run of `scripts/run_ccip_check.py` once real `.env` /
   `config.yaml` values are in place, starting with `--no-live`, then a single
   live account/date.
2. Optionally flesh out the remaining Hypothesis properties toward the full set
   of 21 in the spec (core logic boundaries, retry, and failure dispatch are
   already covered by the current mix).
3. Deferred to v2 (per Req 12): honor an upstream `Retry-After` header and add
   cross-run failure-alert dedup/throttling.
```
