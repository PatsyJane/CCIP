# Implementation Plan: CCIP LangGraph Anomaly Detection Workflow

## Overview

This plan implements the CCIP Archi-3 workflow in a new package
`src/connector/ccip/`, replacing the discarded `src/connector/anomaly/` pipeline
while preserving the FastMCP Connector unchanged. It is built bottom-up: new
dependencies and config scaffolding first; then the pure deterministic modules
(state, config loader, metadata store, models, history/data-mode, STL adapter,
detection, drill-down) that need no I/O; then the I/O stages (ingestion over the
MCP Cost_Tool, Gemini narrative, SMTP email); then the six LangGraph nodes and two
routers; then graph assembly and the runner/CLI; and finally the real-`aws_cur_cost`
integration path. Testing follows a dual approach — example/edge unit tests plus
Hypothesis property-based tests (17 properties) — with the Cost_Tool, LLM, and
Email_Sender always replaced by injected fakes so no unit/property/smoke test
performs a network, SMTP, or external-filesystem call.

Language: Python. New deps: `langgraph`, `statsmodels`, `pyyaml`. Reused:
`connector.errors` (error contract), `connector.tools.aws_cur_cost` (Cost_Tool),
`connector.config` + `connector.http_client` (live wiring), the
dotenv/repr-redaction config style, and the SMTP dry-run sender pattern.

## Tasks

- [x] 1. Add dependencies and config scaffolding
  - Add `langgraph`, `statsmodels`, `pyyaml` to `pyproject.toml` runtime deps
    (pinned ranges); keep existing pins intact
  - Create `config.yaml` at the repo root with detection thresholds, STL params,
    LLM/email settings, and the Metadata_Store path, using `${VAR}` placeholders
    for secrets and environment-specific values
  - Update `.env.example` with the CCIP keys (LLM, SMTP, metadata path) and prune
    the obsolete `ANOMALY_*` keys that no longer apply
  - Create the `src/connector/ccip/__init__.py` package and `tests/ccip/__init__.py`
  - _Requirements: 1.1, 1.2, 1.4_

- [x] 2. Port the secret-redaction scrubber
  - [x] 2.1 Implement `redaction.py` `scrub_secrets(error, secrets)` in the ccip package
    - Recursively replace every configured secret substring in all fields of a
      Structured_Error (nested dicts/lists/tuples/sets), longest-first, skipping
      blank secrets; never mutate the input
    - _Requirements: 10.4_

- [x] 3. Implement the configuration loader with `${VAR}` injection
  - [x] 3.1 Implement `config.py`: `CcipConfig`, `CcipSecrets`, `load_ccip_config`
    - Read `config.yaml` text, `load_dotenv()`, resolve `${VAR}` / `${VAR:-default}`
      against the environment, then `yaml.safe_load`; map into a frozen `CcipConfig`
    - Required unresolved `${VAR}` -> `ConfigError` naming the variable; missing/invalid
      YAML -> `ConfigError` naming the file; secrets on a repr-excluded `_secrets`
      field with redacted `__repr__`
    - Reuse `connector.config.ConfigError`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6_

  - [x]* 3.2 Property test for `${VAR}` injection and required-missing failure
    - **Property 1: `${VAR}` injection resolves from the environment**
    - _Requirements: 1.2, 1.3_

  - [x]* 3.3 Unit test for secret redaction in `repr`
    - `repr(CcipConfig)` and `repr(CcipSecrets)` never expose key/password values
    - _Requirements: 1.5_

- [x] 4. Implement the graph state and data models
  - [x] 4.1 Implement `state.py`: `CcipState` TypedDict + `build_initial_state`
    - Seed `account_id`/`target_date`, default all other keys (empty lists, False,
      None); validate inputs (blank account_id, non-`YYYY-MM-DD` date) returning a
      Structured_Error on failure
    - _Requirements: 2.1, 2.2, 2.3_

  - [x] 4.2 Implement `models.py`: frozen `ServiceCost`, `Contributor`,
        `LastAnomaly`, `AlertPayload`, and the `DataMode` enum + ordering helpers
    - _Requirements: 2.2, 3.2, 8.3, 9.3_

- [x] 5. Implement history counting and data-mode classification
  - [x] 5.1 Implement `history.py`: `count_available_days`, `classify_data_mode`,
        `is_above_cold_start`
    - Classify Cold Start (<30) / Initial (30-59) / Beginner (60-89) / Normal (>=90)
    - _Requirements: 3.1, 3.2, 5.4_

  - [x]* 5.2 Property test for the data-mode boundaries
    - **Property 2: Data-mode classification pivots on 30/60/90**
    - _Requirements: 3.2_

- [x] 6. Implement the persistent JSON metadata store
  - [x] 6.1 Implement `metadata_store.py`: `load_metadata`, `save_metadata`,
        `DEFAULT_METADATA`
    - Absent file -> default store; unreadable/invalid JSON -> `unexpected-error`
      naming the file; atomic write (temp file + `os.replace`); failed write ->
      `unexpected-error`
    - _Requirements: 5.1, 5.7, 5.8, 10.5_

  - [x]* 6.2 Property test for atomic/crash-safe writes
    - **Property 8: Metadata writes are atomic and crash-safe**
    - _Requirements: 5.8, 10.5_

- [x] 7. Implement the STL adapter and detection logic
  - [x] 7.1 Implement `stl_model.py`: `StlResult`, `run_stl`
    - Run `statsmodels.tsa.seasonal.STL` on the account total series; compute the
      residual threshold by IQR (`Q3 + factor*IQR`) or sigma (`mean + factor*std`)
      per config; return the Target_Date residual; too-short series -> error so
      Phase 4 can fall back
    - _Requirements: 5.4, 6.3_

  - [x] 7.2 Implement `detection.py`: `detect_cold_start`, `detect_statistical`
    - Cold start: `abs(A - P) > cold_start_absolute_threshold` vs the most recent
      non-anomaly day; no prior day -> `(False, None)`. Statistical:
      `target_residual > residual_threshold`
    - _Requirements: 6.2, 6.3, 6.6_

  - [x]* 7.3 Property test for cold-start detection rule
    - **Property 9: Cold-start detection follows the absolute-threshold rule**
    - _Requirements: 6.2, 6.6_

  - [x]* 7.4 Property test for statistical detection rule
    - **Property 10: Statistical detection follows the residual-threshold rule**
    - _Requirements: 6.3_

- [x] 8. Implement service drill-down
  - [x] 8.1 Implement `drilldown.py`: `isolate_contributors`
    - Per-service delta vs the baseline (previous non-anomaly) day; keep positive
      increases, sort by descending delta; annotate seasonal context from metadata
      (e.g. day-of-month == 1)
    - _Requirements: 8.2, 8.3, 8.4_

  - [x]* 8.2 Property test for contributor isolation and ordering
    - **Property 13: Drill-down runs only on an account anomaly** (contributor set +
      ordering portion)
    - _Requirements: 8.3_

- [x] 9. Checkpoint - Ensure all pure-module tests pass
  - Ensure all deterministic-module tests pass; ask the user if questions arise.

- [x] 10. Implement the ingestion stage (single fetch + pure normalizer)
  - [x] 10.1 Implement `ingest.py`: `fetch_cost_payload` (Phase 1) and
        `normalize_payload` (Phase 2)
    - `fetch_cost_payload`: one DAILY MCP call over the 90-day window ending at the
      Target_Date; preserve a Cost_Tool error's category (name the phase as source);
      non-DAILY -> `invalid-parameter`; zero items -> `missing-data`; return the raw
      `{granularity, items}` payload for Phase 1 to store on state
    - `normalize_payload`: PURE, no network; read the raw payload and standardize into
      `account_total_cost`, `service_costs`, `history_series`, `service_history`;
      absent/malformed payload -> `unexpected-error`
    - _Requirements: 3.1, 3.3, 3.4, 3.5, 4.1, 4.2, 4.3_

  - [x]* 10.2 Property test for the single well-formed fetch + standardized normalize
    - **Property 3: The single fetch is well-formed and normalization is standardized**
    - _Requirements: 3.1, 4.1, 4.2_

  - [x]* 10.3 Property tests for Cost_Tool error / non-DAILY / empty
    - **Property 4 and Property 5**
    - _Requirements: 3.3, 3.4, 3.5_

- [x] 11. Implement the Gemini narrative and SMTP email stages
  - [x] 11.1 Implement `llm_client.py`: `GeminiLLMClient` (reuse the LangChain
        adapter) and the `LLMClient` protocol
    - _Requirements: 9.4_

  - [x] 11.2 Implement `narrative.py`: `render_narrative` (LLM) with deterministic
        templated fallback on unreachable/timeout; numbers never changed
    - _Requirements: 9.4, 9.6_

  - [x] 11.3 Implement `email.py`: `build_alert_payload`, `EmailSender` protocol,
        `deliver`, and the stdlib SMTP sender with dry-run-when-unconfigured
    - _Requirements: 9.2, 9.3, 9.5_

  - [x]* 11.4 Property test for LLM number-safety and fallback
    - **Property 15: LLM never changes numbers; fallback is deterministic**
    - _Requirements: 9.4, 9.6_

- [x] 12. Implement the six nodes and two routers
  - [x] 12.1 Implement `nodes.py`: the six async node functions (Phases 1-6)
    - Each reads `state`, short-circuits when `state["error"]` is set, wraps its body
      in a totality guard mapping exceptions to `unexpected-error`, and returns a
      partial state update; `update_metadata_and_model` detects + persists new
      services and runs STL only above Cold Start; `dispatch_alerts` builds the
      payload, renders the narrative, delivers (or dry-runs), and records the
      last-anomaly
    - _Requirements: 3.3, 4.6, 5.2, 5.3, 5.4, 5.5, 5.6, 6.1, 6.4, 6.5, 8.1, 8.5, 9.1, 9.7, 9.8, 10.1, 10.3_

  - [x] 12.2 Implement `routers.py`: `route_after_detection`, `route_alert_check`
    - _Requirements: 7.1, 7.2, 7.3, 7.4, 7.5, 7.6_

  - [x]* 12.3 Property tests for both routers
    - **Property 11 and Property 12**
    - _Requirements: 7.1, 7.2, 7.3, 7.4_

  - [x]* 12.4 Property tests for totality/scrubbing and error short-circuit
    - **Property 16 and Property 17** (covered by graph smoke tests:
      secret-scrubbing + first-error-short-circuit paths)
    - _Requirements: 10.1, 10.2, 10.3, 10.4_

  - [x]* 12.5 Property tests for new-service persistence, STL-gating, dispatch gate
    - **Property 6, Property 7, Property 14** (covered by graph smoke tests:
      new-service trigger, Normal-mode STL run, dispatch gate)
    - _Requirements: 5.2, 5.3, 5.4, 9.1, 9.8_

- [x] 13. Assemble the graph and the runner/CLI
  - [x] 13.1 Implement `graph.py`: `build_graph(deps)` wiring nodes, edges, the two
        conditional edges, and the `_alert_check` pass-through node; compile
    - _Requirements: 2.4, 7.1-7.6_

  - [x] 13.2 Implement `runner.py`: `CcipDeps`, `run_once`, `build_live_deps`,
        `build_fake_deps`
    - `build_live_deps` wires the real `aws_cur_cost` (`load_config` + `HttpClient` +
      `make_aws_cur_cost`), the real Gemini client, and the real SMTP sender
    - _Requirements: 2.1, 9.5_

  - [x] 13.3 Implement `scripts/run_ccip_check.py` CLI
    - `--account-id`, `--target-date` (default yesterday), `--no-live` (fakes, zero
      network); print the final data_mode, anomaly flag, new services, contributors,
      and dispatch/dry-run outcome; exit non-zero on a Structured_Error
    - _Requirements: 11.1, 11.2, 11.3, 11.4_

- [x] 14. Checkpoint - Ensure the graph compiles and a fake run succeeds
  - Ensure `build_graph` compiles and `run_once` on a fake dataset returns a
    well-formed terminal state; ask the user if questions arise.

- [x] 15. Add end-to-end integration tests injecting the real `aws_cur_cost`
  - [x]* 15.1 Integration tests for the real-Cost_Tool path
    - Inject the real `aws_cur_cost` via `make_aws_cur_cost(config, http)` backed by
      an `httpx.MockTransport` Cost_API, with fake LLM + fake SMTP; run the whole
      graph to a dry-run alert; cover a happy path plus one representative failure
      per category (`invalid-parameter`, `unreachable-source`, `missing-data`,
      `unexpected-error`)
    - _Requirements: 4.1-4.5, 10.1, 10.2_

- [x] 16. Remove the discarded anomaly package and reconcile
  - [x] 16.1 Delete `src/connector/anomaly/` and `tests/anomaly/`
  - [x] 16.2 Delete `scripts/run_anomaly_check.py`
  - [x] 16.3 Remove obsolete `ANOMALY_*` references and update `PROJECT_OVERVIEW.md`
        and `README.md` to describe the CCIP workflow (also updated the three
        steering files)
  - _Requirements: (housekeeping; preserves the Connector)_

- [x] 17. Final checkpoint - Ensure all tests pass
  - `pytest` passes: 58 tests (56 CCIP + Connector scaffolding), 0 failures.

## Notes

- Tasks marked `*` are optional test sub-tasks and can be skipped for a faster
  MVP; core implementation tasks are never optional.
- Each of the 17 correctness properties is a single Hypothesis property-based test
  (≥100 iterations), tagged `# Feature: ccip-langgraph-anomaly, Property {n}: {text}`,
  with the Cost_Tool, LLM, and Email_Sender always faked.
- The FastMCP Connector and `connector.errors` are preserved and reused, not modified.
- Deferred (no tasks this iteration): GCP/Azure, automated remediation, multi-account
  batch scheduling, and database-backed metadata.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1"] },
    { "id": 1, "tasks": ["2.1", "3.1", "4.1", "4.2", "5.1", "6.1", "7.1", "8.1"] },
    { "id": 2, "tasks": ["3.2", "3.3", "5.2", "6.2", "7.2", "7.3", "7.4", "8.2"] },
    { "id": 3, "tasks": ["10.1", "11.1", "11.2", "11.3"] },
    { "id": 4, "tasks": ["10.2", "10.3", "11.4"] },
    { "id": 5, "tasks": ["12.1", "12.2"] },
    { "id": 6, "tasks": ["12.3", "12.4", "12.5"] },
    { "id": 7, "tasks": ["13.1", "13.2", "13.3"] },
    { "id": 8, "tasks": ["15.1"] },
    { "id": 9, "tasks": ["16.1", "16.2", "16.3"] }
  ]
}
```
