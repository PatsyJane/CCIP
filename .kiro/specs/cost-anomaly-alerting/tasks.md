# Implementation Plan: Cost Anomaly Alerting

## Overview

This plan implements the **Cost Anomaly Alerting pipeline** — a composable, locally testable module layered on top of the existing AWS CUR Cost_Tool (`aws_cur_cost`). It fetches daily per-service cost over a rolling window, computes deterministic baselines and deviation metrics in pure Python, filters results through a materiality + cold-start gate, hands surviving Candidate_Anomalies to an injected LLM for a strict structured diagnosis, and renders an email alert. Every stage returns either its output or a Structured_Error from the shared `connector.errors.structured_error` factory; nothing raises across a stage boundary.

The implementation is built bottom-up: the new config and frozen data models first, then the pure deterministic stages (baseline, metrics, materiality) that need no I/O, then the async stages (ingest, LLM analyzer, email delivery) with injected dependencies, then the orchestrator that wires them, and finally the real-`aws_cur_cost` integration path. Testing follows the design's dual approach — example/edge unit tests with injected fakes (`FakeCostTool`, `FakeLLMClient`, `FakeEmailSender`) plus Hypothesis property-based tests (34 properties) — with all external dependencies faked so no unit or property test performs a network call.

Language: Python. New subpackage `src/connector/anomaly/` (modules: `config.py`, `models.py`, `ingest.py`, `baseline.py`, `metrics.py`, `materiality.py`, `llm.py`, `email.py`, `pipeline.py`, `redaction.py`). Reused primitives: `connector.errors.structured_error` / `ErrorCategory`, the return-error-not-raise stage convention, frozen dataclass models, and the dotenv + repr-redacted-secrets config pattern from `connector.config`. Libraries: python-dotenv; tests use pytest, pytest-asyncio, Hypothesis. The real `aws_cur_cost` is injected via `make_aws_cur_cost` backed by `httpx.MockTransport` only in the integration tests.

## Tasks

- [x] 1. Set up the anomaly subpackage and injected-fake test scaffolding
  - Create `src/connector/anomaly/__init__.py` and empty stubs for `config.py`, `models.py`, `ingest.py`, `baseline.py`, `metrics.py`, `materiality.py`, `llm.py`, `email.py`, `pipeline.py`, `redaction.py`
  - Add `tests/anomaly/__init__.py` and a `tests/anomaly/fakes.py` housing `FakeCostTool` (returns fixture Cost_Data or a chosen Structured_Error), `FakeLLMClient` (returns valid / invalid / delayed / failing outputs), and `FakeEmailSender` (captures the alert, or fails / delays), each matching the real dependency interface
  - Add shared Hypothesis strategies/fixtures module `tests/anomaly/strategies.py` for daily-spend series, flagged-day sets, threshold values (valid / invalid / out-of-range), metric tuples, and Diagnostic_Report lists
  - _Requirements: 7.1_

- [x] 2. Implement the frozen data models
  - [x] 2.1 Implement `models.py` frozen dataclasses
    - Define `DailySpend`, `ServiceMetricResult`, `CandidateAnomaly`, `DiagnosticReport`, `EmailAlert`, `ColdStartSummary`, `SuppressedSummary`, `PipelineResult`, and the `ProbableCause` enum (`expected-workload-growth`, `unexplained-spike`) per the design Data Models section
    - All dataclasses `frozen=True`, matching `dimensions.DataItem` / `numeric` style
    - _Requirements: 2.1, 2.2, 2.3, 2.4, 2.5, 2.7, 3.2, 3.5, 4.3, 4.7, 5.1, 5.3, 5.7_

- [x] 3. Implement the anomaly configuration loader with secret redaction
  - [x] 3.1 Implement `AnomalyConfig`, `AnomalySecrets`, and loader in `config.py`
    - Define frozen `AnomalyConfig` (`rolling_window_days=90`, `min_absolute_delta=10.00`, `min_deviation_percent=30.0`, `llm_model`, `email_from`, `email_to`) and a repr-excluded `_secrets` field holding `AnomalySecrets` (`llm_api_key`, `smtp_password`) with a redacting `__repr__`, mirroring `config.AwsCredentials`
    - Load via python-dotenv; treat blank/whitespace-only required values as missing; raise `ConfigError` naming the first missing key
    - _Requirements: 1.2, 3.1, 6.5_

  - [ ]* 3.2 Write unit tests for config defaults and credential redaction
    - Rolling_Window defaults to 90 (1.2); materiality thresholds default to `$10.00` and `30%` (3.1)
    - `repr()`/serialization of `AnomalyConfig` never exposes `llm_api_key` or `smtp_password` values (6.5)
    - _Requirements: 1.2, 3.1, 6.5_

- [x] 4. Implement the secret-redaction scrubber
  - [x] 4.1 Implement `scrub_secrets` in `redaction.py`
    - Walk every value of a Structured_Error dict and replace any configured secret substring, so no returned field (`error`, `extra`, nested context) can contain a secret — even one placed there by a dependency
    - _Requirements: 6.5_

  - [x]* 4.2 Write property test for secret non-disclosure
    - **Property 31: No configured secret ever appears in a returned field**
    - **Validates: Requirements 6.5**
    - _Requirements: 6.5_

- [x] 5. Implement the deterministic baseline stage
  - [x] 5.1 Implement `most_recent_history_window`, `compute_expected_spend`, `classify_start_mode` in `baseline.py`
    - `most_recent_history_window`: return exactly the most recent 90 complete days strictly preceding the Evaluation_Date, ignoring older days even when more were fetched
    - `compute_expected_spend`: Expected_Spend = arithmetic mean of daily spend over the 90-day window excluding Anomaly_Flagged_Days; if exclusion leaves fewer than 90 complete days, signal Cold_Start instead of computing a mean; never invoke the LLM
    - `classify_start_mode`: Cold_Start iff complete days < 90, else Normal_Start
    - _Requirements: 1.9, 2.1, 2.8, 3.4_

  - [x]* 5.2 Write property test for the most-recent-90-day window cap
    - **Property 7: Normal_Start computation considers exactly the most recent 90 complete days**
    - **Validates: Requirements 1.9**
    - _Requirements: 1.9_

  - [x]* 5.3 Write property test for the anomaly-excluding mean without LLM
    - **Property 8: Expected_Spend is the anomaly-excluding mean and never invokes the LLM**
    - **Validates: Requirements 2.1**
    - _Requirements: 2.1_

  - [x]* 5.4 Write property test for exclusion forcing Cold_Start
    - **Property 9: Excluding flagged days below 90 forces Cold_Start**
    - **Validates: Requirements 2.8**
    - _Requirements: 2.8_

  - [x]* 5.5 Write unit test for the anomaly-exclusion edge
    - Flagging enough days to drop below 90 → Cold_Start, no Expected_Spend computed from the remaining days
    - _Requirements: 2.8_

- [x] 6. Implement the deterministic metric stage
  - [x] 6.1 Implement metric functions in `metrics.py`
    - `compute_absolute_cost_delta` = `round(A - E, 2)`; `compute_spend_deviation_percent` = `round((delta / E) * 100, 2)` when `E > 0` else `None` (delta retained); `compute_cloud_anomaly_score` = deterministic function of Actual_Spend vs History_Window distribution clamped to `[0.0, 100.0]`; `resolve_actual_spend` returns `(0.00, imputed=True)` when the Evaluation_Date item is absent; `compute_service_metrics` assembles a `ServiceMetricResult`
    - _Requirements: 2.2, 2.3, 2.4, 2.5, 2.6, 2.7_

  - [x]* 6.2 Write property test for Absolute_Cost_Delta
    - **Property 10: Absolute_Cost_Delta equals round(A - E, 2)**
    - **Validates: Requirements 2.2**
    - _Requirements: 2.2_

  - [x]* 6.3 Write property test for Spend_Deviation_Percent definedness
    - **Property 11: Spend_Deviation_Percent is defined exactly when E > 0**
    - **Validates: Requirements 2.3, 2.4**
    - _Requirements: 2.3, 2.4_

  - [x]* 6.4 Write property test for the bounded Cloud_Anomaly_Score
    - **Property 12: Cloud_Anomaly_Score is always within [0.0, 100.0]**
    - **Validates: Requirements 2.5**
    - _Requirements: 2.5_

  - [x]* 6.5 Write property test for imputed absent spend
    - **Property 14: Absent Evaluation_Date spend is imputed as 0.00**
    - **Validates: Requirements 2.7**
    - _Requirements: 2.7_

  - [x]* 6.6 Write unit test for the E == 0 deviation edge
    - Deviation omitted/undefined while the Absolute_Cost_Delta is retained
    - _Requirements: 2.4_

- [x] 7. Implement the materiality filter stage
  - [x] 7.1 Implement `validate_thresholds` and `apply_materiality_filter` in `materiality.py`
    - `validate_thresholds`: reject non-numeric or out-of-range thresholds before processing any service (min_absolute_delta in `[$0.01, $1,000,000.00]`, min_deviation in `[0%, 100000%]`) returning an `invalid-parameter` Structured_Error naming the bad parameter
    - `apply_materiality_filter`: suppress iff delta `<` min_absolute_delta AND deviation `<` min_deviation; Candidate iff delta `>=` min_absolute_delta OR deviation `>=` min_deviation; when deviation is undefined (E == 0) apply only the absolute-delta criterion; exclude Cold_Start services and record their available-complete-days count; return retained candidates + suppressed/cold-start summary without raising
    - _Requirements: 3.1, 3.2, 3.3, 3.4, 3.5, 3.6, 3.7, 3.8_

  - [x]* 7.2 Write property test for the Cold_Start 90-day pivot
    - **Property 6: Cold_Start classification pivots exactly on the 90-day boundary**
    - **Validates: Requirements 1.8, 3.4, 3.5**
    - _Requirements: 1.8, 3.4, 3.5_

  - [x]* 7.3 Write property test for the materiality threshold logic
    - **Property 15: Materiality classification follows the threshold logic exactly**
    - **Validates: Requirements 3.2, 3.3**
    - _Requirements: 3.2, 3.3_

  - [x]* 7.4 Write property test for the undefined-deviation criterion
    - **Property 16: Undefined deviation applies only the absolute-delta criterion**
    - **Validates: Requirements 3.7**
    - _Requirements: 3.7_

  - [x]* 7.5 Write property test for filter totality
    - **Property 17: The Materiality_Filter is total**
    - **Validates: Requirements 3.6**
    - _Requirements: 3.6_

  - [x]* 7.6 Write property test for invalid-threshold rejection
    - **Property 18: Invalid thresholds are rejected before any classification**
    - **Validates: Requirements 3.8**
    - _Requirements: 3.8_

- [x] 8. Checkpoint - Ensure all deterministic-stage tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 9. Implement the rolling-window ingestion stage
  - [x] 9.1 Implement `ingest_rolling_window` and `count_complete_days` in `ingest.py`
    - Validate `rolling_window_days` in `[90, 365]`, else `invalid-parameter` with the length and violated bound and no Cost_Tool call
    - Invoke the injected async Cost_Tool with `granularity="DAILY"`, `start_date` = Evaluation_Date minus window, `end_date` = Evaluation_Date, each `YYYY-MM-DD`
    - Preserve a Cost_Tool Structured_Error's category and name the ingestion stage as source; reject non-`DAILY` granularity as `invalid-parameter`; zero items → `missing-data`
    - Group Data_Items by `service` tag ordered ascending by `timestamp`; count distinct complete days per service and classify Cold_Start (<90) / Normal_Start (>=90)
    - _Requirements: 1.1, 1.3, 1.4, 1.5, 1.6, 1.7, 1.8_

  - [ ]* 9.2 Write property test for the well-formed rolling-window request
    - **Property 1: The rolling-window request is well-formed**
    - **Validates: Requirements 1.1**
    - _Requirements: 1.1_

  - [ ]* 9.3 Write property test for out-of-range window rejection
    - **Property 2: Out-of-range Rolling_Window is rejected without a Cost_Tool call**
    - **Validates: Requirements 1.3**
    - _Requirements: 1.3_

  - [ ]* 9.4 Write property test for group-by-service ordering
    - **Property 3: Grouping partitions items by service and orders each group ascending**
    - **Validates: Requirements 1.4**
    - _Requirements: 1.4_

  - [ ]* 9.5 Write property test for Cost_Tool error preservation
    - **Property 4: A Cost_Tool error is preserved and halts the pipeline**
    - **Validates: Requirements 1.5**
    - _Requirements: 1.5_

  - [ ]* 9.6 Write property test for non-DAILY rejection
    - **Property 5: Non-DAILY Cost_Data is rejected**
    - **Validates: Requirements 1.6**
    - _Requirements: 1.6_

  - [ ]* 9.7 Write unit test for the zero-items empty case
    - Zero Data_Items → `missing-data` Structured_Error, no baseline computation
    - _Requirements: 1.7_

- [x] 10. Implement the LLM analyzer stage
  - [x] 10.1 Implement `PromptTemplate`, `LLMClient` protocol, and `LLMAnalyzer` in `llm.py`
    - `PromptTemplate.render`: inject Service, Actual_Spend, Expected_Spend, Absolute_Cost_Delta, Spend_Deviation_Percent (when defined), Cloud_Anomaly_Score verbatim; instruct the LLM to output only a structured Diagnostic_Report and not to recompute, alter, or infer the numbers
    - `LLMAnalyzer.analyze`: submit each Candidate_Anomaly via the injected `LLMClient`; parse output against the fixed schema; retry up to 3 total attempts on non-conformance; on persistent violation return `unexpected-error` naming the schema violation while preserving prior reports and emitting no partial report; unreachable/30s timeout → `unreachable-source` with no fabricated report; carry supplied metrics into each `DiagnosticReport` unchanged; empty candidate list → skip invocation and report no anomalies
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 4.7, 4.8_

  - [ ]* 10.2 Write property test for prompt metric coverage
    - **Property 19: Each candidate's supplied metrics reach the prompt**
    - **Validates: Requirements 4.1**
    - _Requirements: 4.1_

  - [ ]* 10.3 Write property test for the strict Diagnostic_Report schema
    - **Property 20: Every emitted Diagnostic_Report satisfies the strict schema**
    - **Validates: Requirements 4.3, 4.4**
    - _Requirements: 4.3, 4.4_

  - [ ]* 10.4 Write property test for retry-then-fail-clean behavior
    - **Property 21: Persistent schema violations retry at most twice then fail cleanly**
    - **Validates: Requirements 4.5**
    - _Requirements: 4.5_

  - [ ]* 10.5 Write property test for unreachable/timeout handling
    - **Property 22: LLM unreachability or timeout yields unreachable-source with no fabricated report**
    - **Validates: Requirements 4.6**
    - _Requirements: 4.6_

  - [ ]* 10.6 Write property test for metric pass-through
    - **Property 23: Metrics pass through the LLM unchanged**
    - **Validates: Requirements 4.7**
    - _Requirements: 4.7_

  - [ ]* 10.7 Write unit tests for prompt content and the no-candidates case
    - Rendered Prompt_Template contains the output-only and do-not-recompute instructions (4.2)
    - No Candidate_Anomalies → LLM skipped, "no anomalies detected" reported (4.8)
    - _Requirements: 4.2, 4.8_

- [ ] 11. Implement the email rendering and delivery stage
  - [-] 11.1 Implement `render_email_alert`, `EmailSender` protocol, and `deliver_alert` in `email.py`
    - `render_email_alert`: produce exactly one `EmailAlert` with a non-empty subject ≤ 200 chars and a body including, per report, the service, spike magnitude, probable cause, recommended action, Absolute_Cost_Delta, and Spend_Deviation_Percent; when > 50 reports, render at most 50 ordered by descending Absolute_Cost_Delta and state the omitted count
    - `deliver_alert`: hand the rendered alert to the injected sender with content unchanged; delivery failure or 30s timeout → `unreachable-source` naming the email stage while retaining the rendered alert in the result; return `None` on success
    - _Requirements: 5.1, 5.2, 5.3, 5.4, 5.5, 5.6_

  - [ ]* 11.2 Write property test for a well-formed alert
    - **Property 24: The Email_Alert subject and body are well-formed**
    - **Validates: Requirements 5.1, 5.2**
    - _Requirements: 5.1, 5.2_

  - [ ]* 11.3 Write property test for the 50-report cap
    - **Property 25: The email body caps at 50 reports ordered by descending delta**
    - **Validates: Requirements 5.3**
    - _Requirements: 5.3_

  - [ ]* 11.4 Write property test for unchanged alert reaching the sender
    - **Property 26: The rendered alert reaches the sender unchanged**
    - **Validates: Requirements 5.4**
    - _Requirements: 5.4_

  - [ ]* 11.5 Write property test for delivery failure/timeout handling
    - **Property 27: Delivery failure or timeout yields unreachable-source and retains the alert**
    - **Validates: Requirements 5.5, 5.6**
    - _Requirements: 5.5, 5.6_

- [x] 12. Checkpoint - Ensure all stage-level tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 13. Implement the pipeline orchestrator and wire the stages
  - [x] 13.1 Implement `AnomalyPipeline` in `pipeline.py`
    - Constructor-inject `cost_tool`, `llm_client`, `email_sender`, and `AnomalyConfig`
    - `run(evaluation_date)`: wire ingest → group/window-cap → baseline → metrics → materiality → LLM → email in order, stopping at the first Structured_Error and returning it unchanged in `error`/`category`; no Candidate_Anomaly after filtering → skip LLM and email and report no anomalies with no Email_Alert produced
    - Wrap each stage boundary so any unhandled exception maps to `unexpected-error` (error string non-empty, ≤ 500 chars); apply the `redaction.py` scrubber to every returned Structured_Error before it leaves the pipeline
    - Export `ingest_rolling_window`, `compute_expected_spend`, `compute_service_metrics`, `apply_materiality_filter` as individually callable stage entry points over plain in-memory structures; validation failures return a Structured_Error identifying the failed input and leave supplied structures unmodified
    - _Requirements: 4.8, 5.7, 6.1, 6.2, 6.3, 6.4, 6.6, 7.1, 7.2, 7.3, 7.5_

  - [ ]* 13.2 Write property test for determinism and stage-callable round-trip consistency
    - **Property 13: Metric computation is deterministic and stage-callable round-trip-consistent**
    - **Validates: Requirements 2.6, 7.4**
    - _Requirements: 2.6, 7.4_

  - [ ]* 13.3 Write property test for stage totality
    - **Property 28: Every stage is total — failures are structured, never raised**
    - **Validates: Requirements 6.1**
    - _Requirements: 6.1_

  - [ ]* 13.4 Write property test for correct category mapping
    - **Property 29: Every Structured_Error carries a correctly-mapped category**
    - **Validates: Requirements 6.2**
    - _Requirements: 6.2_

  - [ ]* 13.5 Write property test for short-circuit and unchanged error return
    - **Property 30: A stage error short-circuits the pipeline and is returned unchanged**
    - **Validates: Requirements 6.3**
    - _Requirements: 6.3_

  - [ ]* 13.6 Write property test for empty/absent input → missing-data
    - **Property 32: Empty or absent input yields missing-data**
    - **Validates: Requirements 6.6, 1.7**
    - _Requirements: 6.6, 1.7_

  - [ ]* 13.7 Write property test for zero-network compute stages with fakes
    - **Property 33: The compute stages perform zero outbound network calls with fakes** (patch the socket layer so any real network attempt raises)
    - **Validates: Requirements 7.2**
    - _Requirements: 7.2_

  - [ ]* 13.8 Write property test for invalid-input immutability
    - **Property 34: Invalid stage input returns an error and leaves inputs unmodified**
    - **Validates: Requirements 7.5**
    - _Requirements: 7.5_

  - [ ]* 13.9 Write unit tests for error shape, dependency injection, and end-to-end with fakes
    - Every error is built via `structured_error` and carries `error` + `category` keys (6.4)
    - The pipeline is constructed with fakes and each compute stage is invoked directly on plain in-memory structures without stage-code changes (7.1, 7.3)
    - A fake LLM returning a valid report drives the full pipeline to an Email_Alert with zero outbound network calls (7.6)
    - No Candidate_Anomalies → sender not invoked, "no Email_Alert produced" reported (5.7)
    - _Requirements: 5.7, 6.4, 7.1, 7.3, 7.6_

- [x] 14. Add end-to-end integration tests injecting the real aws_cur_cost
  - [x]* 14.1 Write integration tests for the real-Cost_Tool path
    - Inject the real `aws_cur_cost` callable via `make_aws_cur_cost(config, http)` backed by an `httpx.MockTransport` Cost_API, keeping `FakeLLMClient` and `FakeEmailSender`
    - Run all stages unmodified through to a rendered Email_Alert handed to the fake sender, verifying the real-data path composes without touching stage code
    - Cover a happy path plus one representative failure per category (`invalid-parameter`, `unreachable-source`, `missing-data`, `unexpected-error`)
    - _Requirements: 7.7_

- [x] 15. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional test sub-tasks and can be skipped for a faster MVP; core implementation tasks are never optional.
- Each of the 34 correctness properties is implemented by a single Hypothesis property-based test running a minimum of 100 iterations, tagged `# Feature: cost-anomaly-alerting, Property {number}: {property_text}`, with the Cost_Tool, LLM_Client, and Email_Sender always replaced by injected fakes (`FakeCostTool`, `FakeLLMClient`, `FakeEmailSender`) so no unit or property test performs a network call.
- Determinism and no-network properties (P13, P33) assert equality across repeated invocations and patch the socket layer so any real network attempt raises.
- Each task references specific requirement sub-clauses for traceability; property tasks additionally reference their design property number.
- Checkpoints ensure incremental validation as data models, deterministic stages, async stages, and orchestration come together.
- The real `aws_cur_cost` is exercised only in the integration tests (task 14) via `make_aws_cur_cost` + `httpx.MockTransport`; LLM and email transport remain fakes there so no real model or SMTP call occurs.
- Deferred work (GCP/multi-cloud, non-cost usage anomalies, automated remediation, delivery-transport config, persistent baselines) is out of scope and has no tasks in this iteration.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "3.1", "4.1"] },
    { "id": 2, "tasks": ["3.2", "4.2", "5.1", "6.1", "7.1", "9.1", "10.1", "11.1"] },
    { "id": 3, "tasks": ["5.2", "5.3", "5.4", "5.5", "6.2", "6.3", "6.4", "6.5", "6.6", "7.2", "7.3", "7.4", "7.5", "7.6", "9.2", "9.3", "9.4", "9.5", "9.6", "9.7", "10.2", "10.3", "10.4", "10.5", "10.6", "10.7", "11.2", "11.3", "11.4", "11.5"] },
    { "id": 4, "tasks": ["13.1"] },
    { "id": 5, "tasks": ["13.2", "13.3", "13.4", "13.5", "13.6", "13.7", "13.8", "13.9"] },
    { "id": 6, "tasks": ["14.1"] }
  ]
}
```
