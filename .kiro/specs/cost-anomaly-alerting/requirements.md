# Requirements Document

## Introduction

This feature implements a **Cost Anomaly Alerting pipeline module** that sits on top of the existing AWS CUR Cost_Tool (`aws_cur_cost`) delivered by the `billing-usage-ingestion` spec. The pipeline ingests daily per-service AWS cost data over a rolling historical window, computes deterministic statistical baselines and deviation metrics in plain Python, filters the results through a materiality and cold-start gate to suppress noise, and passes the surviving Candidate_Anomalies plus their pre-computed metrics to a Large Language Model (LLM) that produces a strict, structured diagnostic report. That report is rendered into an actionable email alert.

The design intent is a clear separation of concerns: **all baseline and metric math is deterministic Python computed BEFORE the LLM is invoked**, so the LLM performs diagnosis and triage recommendation only, never arithmetic. This keeps anomaly detection reproducible, testable, and auditable, and confines non-determinism to the reasoning/narrative layer.

The module follows the existing Connector's "never crash / short-circuit to a Structured_Error" philosophy. Each stage of the Anomaly_Pipeline returns either its expected output or a Structured_Error object drawn from the same bounded category set (`missing-data`, `unreachable-source`, `invalid-parameter`, `unexpected-error`) used by the Ingestion_Tools, so the pipeline can be composed and reasoned about the same way the ingestion layer is.

The module is a step-by-step, locally testable pipeline: every stage accepts plain data structures so it can be exercised with mocked Cost_Data fixtures or with a real `aws_cur_cost` call, and the LLM step and the email delivery step are injected dependencies that can be replaced with fakes in tests.

**Scope for this iteration:**
- In scope: rolling-window ingestion via the existing `aws_cur_cost` tool, per-service deterministic baseline/metric computation, materiality + cold-start filtering, LLM structured-report generation with a strict output contract, rendering of an email alert artifact, and structured-error handling throughout.
- Out of scope / deferred: multi-cloud (GCP) anomaly detection, usage-metric (non-cost) anomalies, automated remediation actions, alert delivery transport configuration beyond producing a well-formed email artifact and handing it to an injected sender, and persistent storage of historical baselines beyond the rolling window fetched per run.

## Glossary

- **Anomaly_Pipeline**: The end-to-end module that orchestrates ingestion, baseline/metric computation, materiality filtering, LLM analysis, and email alert production. Composed of discrete stages that each return output or a Structured_Error.
- **Cost_Tool**: The existing AWS CUR Cost_Tool `aws_cur_cost(start_date, end_date, granularity="DAILY")` defined in `src/connector/tools/aws_cur_cost.py`, invoked by the Anomaly_Pipeline to obtain cost data.
- **Cost_Data**: The successful payload returned by the Cost_Tool: a dict `{"granularity": <"DAILY"|"HOURLY">, "items": [ {"timestamp": <str>, "cost_value": <float>, "tags": {"account": <str>, "service": <str>, "environment": <str>}} ]}`.
- **Data_Item**: A single record within Cost_Data, comprising a `timestamp`, a `cost_value`, and its Dimension_Tags.
- **Service**: The value of the `service` Dimension_Tag on a Data_Item; the unit at which anomalies are detected.
- **Rolling_Window**: The contiguous span of trailing days of daily Cost_Data used to build baselines, no shorter than a configured minimum of 90 days ending at the Evaluation_Date.
- **Evaluation_Date**: The single day whose actual spend per Service is compared against its expected spend to detect anomalies; by default the most recent complete day in the Rolling_Window.
- **History_Window**: The most recent 90 complete days of a Service's daily spend strictly preceding the Evaluation_Date, used to compute the Expected_Spend. A Service qualifies for computation only when at least 90 complete days are available (Normal_Start).
- **Anomaly_Flagged_Day**: A calendar day within a Service's History_Window whose spend was previously classified as an anomaly and is therefore excluded when computing that Service's Expected_Spend baseline.
- **Actual_Spend (A)**: The observed `cost_value` for a Service on the Evaluation_Date.
- **Expected_Spend (E)**: The deterministically computed baseline daily spend for a Service, derived from that Service's History_Window.
- **Absolute_Cost_Delta**: The metric `A - E` for a Service on the Evaluation_Date, expressed in the same currency units as `cost_value`.
- **Spend_Deviation_Percent**: The metric `(A - E) / E`, expressed as a percentage, defined only when `E` is greater than 0.
- **Cloud_Anomaly_Score**: A deterministic bounded numeric score summarizing how unusual the Actual_Spend is relative to the History_Window distribution for a Service.
- **Materiality_Filter**: The deterministic gate that suppresses Service results failing minimum absolute-delta and deviation thresholds, and that classifies remaining results.
- **Materiality_Thresholds**: The configured minimum absolute delta (default $10.00 per day) and minimum deviation (default 30%) a Service result must meet to be retained.
- **Cold_Start**: The mode in which a Service has fewer than 90 complete days of historic data available and therefore cannot yet support reliable anomaly detection.
- **Normal_Start**: The mode in which a Service has 90 or more complete days of historic data available, enabling baseline computation, metric computation, materiality filtering, and LLM analysis.
- **Candidate_Anomaly**: A per-Service result that passed the Materiality_Filter and is forwarded to the LLM_Analyzer, carrying the Service name, Actual_Spend, Expected_Spend, and all computed metrics.
- **LLM_Analyzer**: The stage that submits Candidate_Anomalies and their metrics to the LLM using the Prompt_Template and validates the LLM's structured output against the Diagnostic_Report contract.
- **Prompt_Template**: The fixed prompt text that instructs the LLM to emit only a structured Diagnostic_Report and forbids the LLM from performing or altering the arithmetic.
- **Diagnostic_Report**: The structured LLM output for a Candidate_Anomaly, containing at minimum a Service name and spike magnitude, a probable cause classification, and a recommended triage action.
- **Email_Alert**: The rendered artifact produced from one or more Diagnostic_Reports, comprising a subject line and a body, suitable for handing to an email sender.
- **Email_Sender**: The injected dependency that transmits an Email_Alert; replaceable with a fake in tests.
- **Structured_Error**: A JSON-serializable dict of the form `{"error": <message>, "category": <category>, ...extra}` returned by a pipeline stage in place of raising an exception, where `category` is one of `missing-data`, `unreachable-source`, `invalid-parameter`, or `unexpected-error`.
- **LLM_Client**: The injected dependency that performs the actual model invocation; replaceable with a fake in tests.
- **Complete_Day**: A calendar day within the History_Window that has a recorded spend value for a Service; used to count a Service's available history when evaluating Cold_Start.
- **Insufficient_Daily_Coverage**: The condition in which a Service's distinct complete daily records number fewer than the 90 complete days required for Normal_Start, causing that Service to be classified as Cold_Start and recorded with the count of available complete days.

## Requirements

### Requirement 1: Rolling-Window Cost Ingestion

**User Story:** As a pipeline operator, I want the Anomaly_Pipeline to fetch daily per-service cost data over a rolling historical window using the existing Cost_Tool, so that baselines are built from real recent spend without a new data source.

#### Acceptance Criteria

1. WHEN the Anomaly_Pipeline runs for an Evaluation_Date, THE Anomaly_Pipeline SHALL invoke the Cost_Tool with `granularity` set to `DAILY`, a `start_date` equal to the Evaluation_Date minus the configured Rolling_Window length, and an `end_date` equal to the Evaluation_Date, each formatted as `YYYY-MM-DD`.
2. THE Anomaly_Pipeline SHALL accept a configurable Rolling_Window length in whole days from a minimum of 90 to a maximum of 365, with a default value of 90.
3. IF the configured Rolling_Window length is less than 90 days or greater than 365 days, THEN THE Anomaly_Pipeline SHALL return a Structured_Error categorized as `invalid-parameter` identifying the Rolling_Window length and the violated bound, and SHALL NOT invoke the Cost_Tool.
4. WHEN the Cost_Tool returns Cost_Data, THE Anomaly_Pipeline SHALL group the Data_Items by their `service` Dimension_Tag and order each Service's Data_Items by `timestamp` ascending before computing baselines.
5. IF the Cost_Tool returns a Structured_Error, THEN THE Anomaly_Pipeline SHALL return a Structured_Error that preserves the Cost_Tool's `category` value and identifies the ingestion stage as the source, and SHALL NOT proceed to baseline computation.
6. WHEN the Anomaly_Pipeline receives Cost_Data whose `granularity` is not `DAILY`, THE Anomaly_Pipeline SHALL return a Structured_Error categorized as `invalid-parameter` identifying the unexpected granularity, and SHALL NOT proceed to baseline computation.
7. IF the Cost_Tool returns Cost_Data with zero Data_Items, THEN THE Anomaly_Pipeline SHALL return a Structured_Error categorized as `missing-data` identifying the empty result set, and SHALL NOT proceed to baseline computation.
8. IF a Service has fewer than 90 distinct complete daily records within the fetched data, THEN THE Anomaly_Pipeline SHALL classify that Service as Cold_Start and record the count of available complete days.
9. WHERE a Service is in Normal_Start, WHEN the Anomaly_Pipeline computes baselines, metrics, materiality classification, and LLM analysis for that Service, THE Anomaly_Pipeline SHALL consider exactly the most recent 90 complete days of that Service's data, ignoring any older days even when the Rolling_Window fetched more.

### Requirement 2: Deterministic Baseline and Metric Computation

**User Story:** As a data engineer, I want expected spend and deviation metrics computed by deterministic Python functions before any LLM is involved, so that anomaly detection is reproducible and auditable.

#### Acceptance Criteria

1. WHEN the Anomaly_Pipeline computes a baseline for a Service in Normal_Start, THE Anomaly_Pipeline SHALL derive the Expected_Spend as the arithmetic mean of that Service's daily spend over the 90-day History_Window, excluding any day within the History_Window that was previously flagged as an anomaly, and SHALL NOT invoke the LLM during baseline or metric computation.
2. WHEN the Anomaly_Pipeline computes metrics for a Service on the Evaluation_Date, THE Anomaly_Pipeline SHALL compute the Absolute_Cost_Delta as Actual_Spend minus Expected_Spend, rounded to 2 decimal places.
3. WHERE the Expected_Spend for a Service is greater than 0, WHEN the Anomaly_Pipeline computes metrics for that Service, THE Anomaly_Pipeline SHALL compute the Spend_Deviation_Percent as the Absolute_Cost_Delta divided by the Expected_Spend expressed as a percentage rounded to 2 decimal places.
4. IF the Expected_Spend for a Service is 0, THEN THE Anomaly_Pipeline SHALL omit the Spend_Deviation_Percent for that Service and record that the deviation is undefined, and SHALL retain the computed Absolute_Cost_Delta.
5. WHEN the Anomaly_Pipeline computes metrics for a Service on the Evaluation_Date, THE Anomaly_Pipeline SHALL compute a Cloud_Anomaly_Score as a deterministic function of the Actual_Spend and the History_Window distribution, bounded within the range 0.0 to 100.0.
6. WHEN the Anomaly_Pipeline computes any metric, THE Anomaly_Pipeline SHALL produce identical metric values for identical Cost_Data inputs across repeated runs.
7. IF a Service's Data_Item on the Evaluation_Date is absent from the Cost_Data, THEN THE Anomaly_Pipeline SHALL treat that Service's Actual_Spend as 0.00 for the Evaluation_Date and SHALL record that the Actual_Spend was imputed.
8. IF excluding previously flagged anomalous days from a Service's History_Window leaves fewer than 90 complete days, THEN THE Anomaly_Pipeline SHALL classify that Service as Cold_Start rather than computing an Expected_Spend from the remaining days.

### Requirement 3: Materiality and Cold-Start Filtering

**User Story:** As an on-call engineer, I want minor cost noise suppressed and services with too little history handled gracefully, so that I only receive alerts that are material and trustworthy.

#### Acceptance Criteria

1. THE Materiality_Filter SHALL accept a configurable minimum absolute delta, expressed in whole cents from $0.01 to $1,000,000.00 with a default value of $10.00 per day, and a configurable minimum deviation, expressed as a percentage from 0% to 100,000% with a default value of 30%.
2. IF a Service's Absolute_Cost_Delta is less than the configured minimum absolute delta AND its Spend_Deviation_Percent is less than the configured minimum deviation, THEN THE Materiality_Filter SHALL suppress that Service result and SHALL NOT classify it as a Candidate_Anomaly.
3. IF a Service's Absolute_Cost_Delta is greater than or equal to the configured minimum absolute delta OR its Spend_Deviation_Percent is greater than or equal to the configured minimum deviation, THEN THE Materiality_Filter SHALL classify that Service result as a Candidate_Anomaly.
4. THE Anomaly_Pipeline SHALL classify a Service as Cold_Start when it has fewer than 90 complete days of historic data and as Normal_Start when it has 90 or more complete days of historic data.
5. IF a Service has fewer than 90 complete days of historic data, where a complete day is a calendar day with a recorded spend value, THEN THE Materiality_Filter SHALL classify that Service as Cold_Start, SHALL exclude it from the Candidate_Anomaly set, and SHALL record the Cold_Start reason together with the count of available complete history days.
6. WHEN the Materiality_Filter completes, THE Materiality_Filter SHALL return the retained Candidate_Anomalies together with a summary of suppressed and Cold_Start Services, without raising an exception.
7. WHERE a Service's Spend_Deviation_Percent is undefined because Expected_Spend is 0, WHEN the Materiality_Filter evaluates that Service, THE Materiality_Filter SHALL apply only the minimum absolute delta criterion to decide whether the Service is a Candidate_Anomaly.
8. IF the Materiality_Filter or Anomaly_Pipeline is configured with a minimum absolute delta or minimum deviation that is non-numeric or outside its defined valid range, THEN THE Materiality_Filter SHALL reject the configuration before processing any Service, SHALL return a Structured_Error categorized as `invalid-parameter` indicating which parameter is invalid, and SHALL NOT classify any Service result.

### Requirement 4: LLM Diagnostic Analysis with Strict Output Contract

**User Story:** As an on-call engineer, I want the LLM to turn each candidate anomaly plus its pre-computed metrics into a structured diagnostic report, so that I receive an interpreted, actionable explanation rather than raw numbers.

#### Acceptance Criteria

1. WHEN the Anomaly_Pipeline has one or more Candidate_Anomalies, THE LLM_Analyzer SHALL submit to the LLM_Client, for each Candidate_Anomaly, the Service name, the Actual_Spend, the Expected_Spend, the Absolute_Cost_Delta, the Spend_Deviation_Percent when defined, and the Cloud_Anomaly_Score, using the Prompt_Template.
2. THE Prompt_Template SHALL instruct the LLM to output only a structured Diagnostic_Report and SHALL instruct the LLM not to recompute, alter, or infer the numeric metrics supplied to it.
3. WHEN the LLM_Analyzer produces a Diagnostic_Report for a Candidate_Anomaly, THE Diagnostic_Report SHALL contain the Service name, a spike magnitude grounded in the supplied Absolute_Cost_Delta and Spend_Deviation_Percent, a probable-cause classification set to exactly one enumerated value distinguishing at least expected workload growth from an unexplained spike, and a recommended triage action as a non-empty text field.
4. THE LLM_Analyzer SHALL require the LLM output to conform to a fixed structured schema, and SHALL parse the LLM output into a validated Diagnostic_Report object.
5. IF the LLM output does not conform to the fixed structured schema, THEN THE LLM_Analyzer SHALL attempt reparsing at most 2 additional times (3 total parse attempts), and IF the output still does not conform, THEN THE LLM_Analyzer SHALL return a Structured_Error categorized as `unexpected-error` identifying the schema violation, preserving any Diagnostic_Reports already produced for other Candidate_Anomalies, without emitting a partial Diagnostic_Report for the failing Candidate_Anomaly.
6. IF the LLM_Client is unreachable or does not respond within 30 seconds, THEN THE LLM_Analyzer SHALL return a Structured_Error categorized as `unreachable-source` identifying the LLM step, without fabricating a Diagnostic_Report.
7. WHEN the LLM_Analyzer emits a Diagnostic_Report, THE LLM_Analyzer SHALL carry the deterministically computed metrics through to the Diagnostic_Report with values exactly equal to those supplied in the prompt.
8. WHERE there are no Candidate_Anomalies after the Materiality_Filter, WHEN the Anomaly_Pipeline reaches the LLM_Analyzer stage, THE Anomaly_Pipeline SHALL skip LLM invocation and SHALL report that no anomalies were detected.

### Requirement 5: Email Alert Production

**User Story:** As an on-call engineer, I want the structured diagnostic reports rendered into an email alert, so that anomalies reach me through an actionable channel.

#### Acceptance Criteria

1. WHEN the LLM_Analyzer produces one or more Diagnostic_Reports, THE Anomaly_Pipeline SHALL render exactly one Email_Alert containing a non-empty subject line of at most 200 characters and a body derived from all provided Diagnostic_Reports.
2. WHEN the Anomaly_Pipeline renders an Email_Alert, THE Email_Alert body SHALL include, for each Diagnostic_Report, the Service name, the spike magnitude, the probable-cause classification, the recommended triage action, and the Absolute_Cost_Delta and Spend_Deviation_Percent values.
3. WHEN more than 50 Diagnostic_Reports are provided, THE Anomaly_Pipeline SHALL render at most 50 in the Email_Alert body ordered by descending Absolute_Cost_Delta and SHALL indicate the count of omitted reports.
4. WHEN the Anomaly_Pipeline hands an Email_Alert to the Email_Sender, THE Anomaly_Pipeline SHALL pass the rendered Email_Alert with Diagnostic_Report content unchanged.
5. IF the Email_Sender reports a delivery failure, THEN THE Anomaly_Pipeline SHALL return a Structured_Error categorized as `unreachable-source` identifying the email delivery stage, and SHALL retain the rendered Email_Alert in the returned result for inspection.
6. IF the Email_Sender does not complete delivery within 30 seconds, THEN THE Anomaly_Pipeline SHALL treat it as a delivery failure and return a Structured_Error categorized as `unreachable-source` identifying the email delivery stage.
7. WHERE the Anomaly_Pipeline detected no anomalies, WHEN the Anomaly_Pipeline reaches the email stage, THE Anomaly_Pipeline SHALL NOT invoke the Email_Sender and SHALL report that no Email_Alert was produced.

### Requirement 6: Structured Error Handling Consistent with the Connector

**User Story:** As a pipeline integrator, I want every pipeline stage to return structured JSON errors instead of crashing, so that failures compose the same way as the existing ingestion tools.

#### Acceptance Criteria

1. IF any stage of the Anomaly_Pipeline encounters an unreachable dependency, a missing-data condition, an invalid parameter, or any otherwise unhandled exception, THEN THE stage SHALL return a Structured_Error as a JSON-serializable dict containing a non-empty `error` string field of at most 500 characters, and SHALL NOT propagate a raised exception past the stage boundary.
2. WHEN a pipeline stage returns a Structured_Error, THE Structured_Error SHALL include a `category` field set to exactly one value from the set {`missing-data`, `unreachable-source`, `invalid-parameter`, `unexpected-error`}, where an unreachable dependency maps to `unreachable-source`, a missing-data condition maps to `missing-data`, an invalid parameter maps to `invalid-parameter`, and any otherwise unhandled exception maps to `unexpected-error`.
3. WHEN a pipeline stage returns a Structured_Error, THE Anomaly_Pipeline SHALL execute no further stages and SHALL return that same Structured_Error dict, unchanged in its `error` and `category` fields, as the pipeline result.
4. WHEN the Anomaly_Pipeline builds a Structured_Error, THE Anomaly_Pipeline SHALL construct it using the Connector's shared structured-error factory contract used by the Ingestion_Tools, producing a dict whose keys are `error`, `category`, and zero or more additional context fields.
5. WHERE a dependency such as the Cost_Tool or LLM_Client exposes credentials or secret material in an error context, WHEN the Anomaly_Pipeline returns a Structured_Error, THE Anomaly_Pipeline SHALL exclude every configured secret value from the `error` field and from all additional context fields, such that no returned field's value contains any configured secret substring.
6. IF a pipeline stage receives an empty or absent input dataset, THEN THE stage SHALL return a Structured_Error with `category` equal to `missing-data`, rather than returning an empty success result or raising an exception.

### Requirement 7: Local Testability with Mocked or Real MCP Data

**User Story:** As a developer, I want to run and test each pipeline stage locally with either mocked or real cost data, so that I can validate detection logic without live dependencies.

#### Acceptance Criteria

1. THE Anomaly_Pipeline SHALL accept the Cost_Tool, the LLM_Client, and the Email_Sender as constructor-injected dependencies, each replaceable with a substitute implementation that satisfies the same interface, without modification to Anomaly_Pipeline stage code.
2. WHEN the Anomaly_Pipeline is invoked with a fake Cost_Tool returning fixture Cost_Data, THE Anomaly_Pipeline SHALL compute baselines, metrics, and materiality classifications while performing zero outbound network calls to any external service.
3. THE Anomaly_Pipeline SHALL expose the baseline computation stage, the metric computation stage, and the Materiality_Filter stage as individually callable functions, each accepting one or more plain in-memory data structures as input and returning either a plain in-memory data structure or a Structured_Error.
4. WHEN the baseline, metric, and materiality stages are invoked more than once with identical fixed Cost_Data inputs, THE stages SHALL return outputs that are equal across all invocations, with metric computation yielding round-trip-consistent results for identical inputs.
5. IF a stage is invoked with input that fails validation, THEN THE stage SHALL return a Structured_Error identifying the failed input and SHALL leave the supplied input data structures unmodified.
6. WHERE a test supplies a fake LLM_Client returning a valid structured Diagnostic_Report, WHEN the Anomaly_Pipeline runs end to end, THE Anomaly_Pipeline SHALL produce an Email_Alert while performing zero outbound network calls to any real model provider or email transport.
7. WHEN the real `aws_cur_cost` callable is injected as the Cost_Tool, THE Anomaly_Pipeline SHALL execute all stages using the real Cost_Data without any modification to the pipeline stage code.
