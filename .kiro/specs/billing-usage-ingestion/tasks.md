# Implementation Plan: Billing & Usage Ingestion (AWS CUR Cost_Tool PoC)

## Overview

This plan implements the PoC "Cloud Billing & Telemetry Connector" — a FastMCP server hosting a single async MCP tool, the AWS CUR Cost_Tool, which fetches cost data via HTTP `POST {base_url}/api/costs`. The implementation is built bottom-up: foundational pure-logic modules first (structured errors, config, validation, numeric, dimensions), then the I/O layers (HTTP client wrapper, response adapter), then the orchestration pipeline of the tool, and finally the connector bootstrap/wiring and entry point. Testing follows a dual approach — example/edge unit tests plus Hypothesis property-based tests (25 properties) — with the Cost_API always mocked via `httpx.MockTransport`/`respx`.

Language: Python. Libraries: FastMCP, httpx, python-dotenv; tests use pytest, pytest-asyncio, Hypothesis. Only the AWS CUR Cost_Tool is implemented; GCP/CloudWatch/Application-Telemetry tools (Requirements 7, 8, 9) are deferred and out of scope.

## Tasks

- [x] 1. Set up project scaffolding and dependencies
  - Create the package layout per design: `server.py`, `config.py`, `http_client.py`, `validation.py`, `dimensions.py`, `numeric.py`, `errors.py`, `response_adapter.py`, and `tools/aws_cur_cost.py` (with `tools/__init__.py`)
  - Add dependency manifest (`pyproject.toml` or `requirements.txt`) pinning `fastmcp`, `httpx`, `python-dotenv` and dev deps `pytest`, `pytest-asyncio`, `hypothesis`, `respx`
  - Add `.env.example` documenting required keys: `AWS_ACCOUNT_ID`, `AWS_ACCOUNT_NAME`, `AWS_REGION`, `COST_API_BASE_URL`, `PROVIDER`, `PROJECT`, and optional `AWS_ACCESS_KEY_ID`/`AWS_SECRET_ACCESS_KEY`
  - Configure pytest (`pytest.ini`/`pyproject`) with `asyncio_mode = auto` and a `tests/` directory
  - _Requirements: 1.1, 12.1, 12.2, 12.3_

- [x] 2. Implement the structured-error factory and category enum
  - [x] 2.1 Implement `errors.py` with `ErrorCategory` enum and `structured_error` factory
    - Define `ErrorCategory` with exactly the four values `missing-data`, `unreachable-source`, `invalid-parameter`, `unexpected-error`
    - Implement `structured_error(message, category, **extra)` returning `{"error", "category", **extra}` as the single source of the error contract
    - _Requirements: 10.1, 10.3, 10.4, 10.5_

  - [ ]* 2.2 Write property test for the error contract and category enum
    - **Property 23: Every Structured_Error carries a category from the fixed enum, with correct Cold-Start signalling**
    - **Validates: Requirements 10.3, 10.4, 10.5**
    - _Requirements: 10.3, 10.4, 10.5_

- [x] 3. Implement the config loader with fail-fast and credential redaction
  - [x] 3.1 Implement `AppConfig`/`AwsCredentials` models and `load_config` in `config.py`
    - Define frozen `AppConfig` (`aws_account_id`, `aws_account_name`, `aws_region`, `cost_api_base_url`, `provider`, `project`) and `_credentials` field excluded from `__repr__`
    - Load `.env` via `python-dotenv`; treat empty/whitespace-only required values as missing
    - Raise `ConfigError` naming the first missing/blank required key; load AWS credentials only where required
    - _Requirements: 1.5, 12.1, 12.2, 12.3, 12.4, 12.6_

  - [ ]* 3.2 Write property test for fail-fast config validation
    - **Property 1: Missing or blank required config aborts startup naming the value**
    - **Validates: Requirements 1.5, 12.4**
    - _Requirements: 1.5, 12.4_

  - [ ]* 3.3 Write unit tests for config load and credential redaction
    - A complete `.env` populates all `AppConfig` fields (12.1–12.3)
    - `repr()`/serialization of `AppConfig` never exposes credential values (12.6)
    - _Requirements: 12.1, 12.2, 12.3, 12.6_

- [x] 4. Implement validation utilities (dates, range, granularity)
  - [x] 4.1 Implement `parse_date`, `validate_range`, `normalize_granularity` in `validation.py`
    - `parse_date(name, value)`: reject absent/non-string/non-`YYYY-MM-DD`/non-calendar dates, naming the offending parameter
    - `validate_range(start, end)`: reject `end < start` (echoing both dates) and ranges exceeding 366 days
    - `normalize_granularity(value)`: accept `{DAILY, MONTHLY, HOURLY, WEEKLY}`, reject missing/empty/unsupported (listing accepted values), decompose coarser-than-daily (`WEEKLY`/`MONTHLY`) to `DAILY`, constrain effective granularity to `DAILY`/`HOURLY`
    - _Requirements: 2.4, 2.5, 2.6, 3.2, 3.3, 3.4, 3.5, 6.2_

  - [ ]* 4.2 Write property test for invalid dates
    - **Property 2: Invalid dates produce a structured error naming the offending date and issue no request**
    - **Validates: Requirements 2.4, 6.2**
    - _Requirements: 2.4, 6.2_

  - [ ]* 4.3 Write property test for inverted range
    - **Property 3: Inverted range produces a structured error containing both dates**
    - **Validates: Requirements 2.5, 6.2**
    - _Requirements: 2.5, 6.2_

  - [ ]* 4.4 Write property test for invalid granularity
    - **Property 4: Invalid granularity produces a structured error listing the accepted values**
    - **Validates: Requirements 2.6, 3.3, 3.4**
    - _Requirements: 2.6, 3.3, 3.4_

  - [ ]* 4.5 Write property test for granularity decomposition and effective granularity
    - **Property 5: Coarser-than-daily granularity decomposes to DAILY and effective granularity is always DAILY or HOURLY**
    - **Validates: Requirements 3.2, 3.5**
    - _Requirements: 3.2, 3.5_

  - [ ]* 4.6 Write unit test for granularity default vs explicit-empty
    - Omitting `granularity` yields default `DAILY` (2.2); an explicitly empty granularity errors (3.4)
    - _Requirements: 2.2, 3.4_

- [x] 5. Implement numeric normalization
  - [x] 5.1 Implement `coerce_cost_value` in `numeric.py`
    - Coerce text-encoded numerics to `float`; enforce inclusive range `0.00`–`999,999,999.99`
    - Unconvertible → error naming field and value; missing/empty → error naming field; out-of-range → error naming field, value, and range
    - _Requirements: 5.1, 5.3, 5.4, 5.5, 5.6, 6.4_

  - [ ]* 5.2 Write property test for float type and range
    - **Property 10: Every returned Cost_Value is a float within the permitted range**
    - **Validates: Requirements 5.1, 6.4**
    - _Requirements: 5.1, 6.4_

  - [ ]* 5.3 Write property test for text-to-float coercion equality
    - **Property 11: Text-encoded numerics coerce to the equal float**
    - **Validates: Requirements 5.3**
    - _Requirements: 5.3_

  - [ ]* 5.4 Write property test for unconvertible numeric values
    - **Property 12: Unconvertible numeric values produce an error naming field and value, excluded from the response**
    - **Validates: Requirements 5.4**
    - _Requirements: 5.4_

  - [ ]* 5.5 Write property test for missing required numeric values
    - **Property 13: Missing required numeric values produce an error naming the field**
    - **Validates: Requirements 5.5**
    - _Requirements: 5.5_

  - [ ]* 5.6 Write property test for out-of-range numeric values
    - **Property 14: Out-of-range numeric values produce an error naming field, value, and range**
    - **Validates: Requirements 5.6**
    - _Requirements: 5.6_

- [x] 6. Implement dimension tagging and normalization
  - [x] 6.1 Implement `DimensionTags`, `DataItem`, and `tag_item` in `dimensions.py`
    - Attach `account` (fixed from config), `service`, `environment` to every item
    - Normalize missing/empty/whitespace-only `service`/`environment` to `"UNKNOWN"`; preserve non-blank values
    - Guarantee all three tags present and non-empty; on untaggable item, surface a tagging-failure structured error while preserving correctly-tagged items
    - _Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 12.5_

  - [ ]* 6.2 Write property test for presence of all three non-empty tags
    - **Property 7: Every Data_Item carries all three dimension tags, non-empty**
    - **Validates: Requirements 4.1, 4.5**
    - _Requirements: 4.1, 4.5_

  - [ ]* 6.3 Write property test for the fixed account tag
    - **Property 8: The account tag is always the configured account, independent of source content**
    - **Validates: Requirements 4.2, 12.5**
    - _Requirements: 4.2, 12.5_

  - [ ]* 6.4 Write property test for UNKNOWN normalization
    - **Property 9: Missing or blank service/environment normalizes to "UNKNOWN"**
    - **Validates: Requirements 4.3, 4.4**
    - _Requirements: 4.3, 4.4_

  - [ ]* 6.5 Write unit test for partial tagging preservation
    - An untaggable record among valid ones yields a tagging-failure error while preserving the valid items
    - _Requirements: 4.6_

- [x] 7. Checkpoint - Ensure all foundational-module tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 8. Implement the shared async HTTP client wrapper
  - [x] 8.1 Implement `HttpClient` and `HttpResult` in `http_client.py`
    - Construct one shared `httpx.AsyncClient` with a 30s timeout, reused across invocations (never per-request)
    - `post_json(url, body, headers)` awaits the call; map non-2xx to a status structured error (status code + source id), timeout to `unreachable-source` (elapsed 30s + source id), connection error to `unreachable-source` (source id)
    - _Requirements: 11.1, 11.2, 11.3, 11.4, 11.5, 6.5, 6.7_

  - [ ]* 8.2 Write property test for shared client reuse
    - **Property 24: A single shared HTTP client instance is reused across invocations**
    - **Validates: Requirements 11.1**
    - _Requirements: 11.1_

  - [ ]* 8.3 Write property test for non-2xx status handling
    - **Property 17: Non-2xx responses produce a structured error containing the status code and no partial data**
    - **Validates: Requirements 6.5, 11.3**
    - _Requirements: 6.5, 11.3_

  - [ ]* 8.4 Write property test for transport failures
    - **Property 19: Transport failures produce an unreachable-source error and no partial data**
    - **Validates: Requirements 6.7, 11.4, 11.5**
    - _Requirements: 6.7, 11.4, 11.5_

  - [ ]* 8.5 Write unit test for non-blocking concurrency
    - Concurrent calls against a slow mock transport overlap in time, demonstrating the event loop is not blocked
    - _Requirements: 11.2_

- [x] 9. Implement the response parser/adapter
  - [x] 9.1 Implement defensive parsing in `response_adapter.py`
    - Parse the (currently unknown-shape) Cost_API JSON into internal records, isolating all wire-format assumptions here
    - On non-JSON or missing expected cost fields, return an `unexpected-error` with `subcategory: "unexpected-response-shape"`; on empty result set, signal a `missing-data` condition
    - _Requirements: 6.6, 6.8, 10.4_

  - [ ]* 9.2 Write property test for malformed/unexpected response bodies
    - **Property 18: Malformed or unexpected response bodies produce an unexpected-response-shape error and no partial data**
    - **Validates: Requirements 6.6**
    - _Requirements: 6.6_

  - [ ]* 9.3 Write property test for empty result sets
    - **Property 20: Empty result sets produce a missing-data error**
    - **Validates: Requirements 6.8, 10.4**
    - _Requirements: 6.8, 10.4_

- [x] 10. Checkpoint - Ensure I/O-layer tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [x] 11. Implement the AWS CUR Cost_Tool orchestration pipeline
  - [x] 11.1 Implement request-body building in `tools/aws_cur_cost.py`
    - Build `CostApiRequest` mapping `start_date→start`, `end_date→end`, `granularity→granularity` (post-decomposition), with `provider`/`project` from config; target `{base_url}/api/costs` with headers `Content-Type: application/json`, `accept: */*`
    - _Requirements: 6.1, 6.3_

  - [x] 11.2 Implement the async `aws_cur_cost` pipeline orchestration
    - Wire the pipeline: validate → build request → POST via shared client → parse/adapt → tag/normalize → numeric coerce/validate → return success payload `{granularity, items}`
    - Short-circuit each stage to the structured-error factory; skip the HTTP call entirely on invalid parameters
    - Wrap the tool body in a top-level guard mapping any unhandled exception to `unexpected-error` (no secrets), guaranteeing totality
    - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8, 3.1, 3.5, 10.1, 10.2_

  - [ ]* 11.3 Write property test for well-formed request construction
    - **Property 15: Valid parameters produce a correctly-formed Cost_API request**
    - **Validates: Requirements 6.1, 6.3**
    - _Requirements: 6.1, 6.3_

  - [ ]* 11.4 Write property test that invalid parameters issue no request
    - **Property 16: Invalid parameters issue no Cost_API request**
    - **Validates: Requirements 6.2**
    - _Requirements: 6.2_

  - [ ]* 11.5 Write property test for non-overlapping aligned buckets
    - **Property 6: Returned Data_Items occupy non-overlapping buckets aligned to the effective granularity**
    - **Validates: Requirements 3.1**
    - _Requirements: 3.1_

  - [ ]* 11.6 Write property test for tool totality (never raises)
    - **Property 21: The tool is total — it always returns a JSON error object on failure and never raises**
    - **Validates: Requirements 10.1**
    - _Requirements: 10.1_

  - [ ]* 11.7 Write property test for credential non-disclosure
    - **Property 25: Credential values never appear in outputs or logs**
    - **Validates: Requirements 12.6, 12.7**
    - _Requirements: 12.6, 12.7_

- [x] 12. Implement connector bootstrap, tool registration, and run entry point
  - [x] 12.1 Implement `build_server`, tool registration, and `main` in `server.py`
    - Create `FastMCP("Cloud Billing & Telemetry Connector")`; load config (fail-fast); build the shared `HttpClient`; register the AWS CUR Cost_Tool as the only async tool discoverable by name
    - `main()` guards `ConfigError`/`ToolRegistrationError` (abort startup, serve no tools, name the failure) and MCP start failure (non-zero exit with descriptive message); call `mcp.run()` under `if __name__ == "__main__"`
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6, 1.7_

  - [ ]* 12.2 Write unit tests for connector wiring and startup error paths
    - Server name is exactly `"Cloud Billing & Telemetry Connector"` (1.1); AWS CUR Cost_Tool is the only tool and discoverable by name (1.2); tool is a coroutine function (1.3); `main()` invokes the FastMCP run entry point (1.4)
    - Tool registration failure aborts startup, serves no tools, and names the failing tool (1.6); MCP start failure exits non-zero with a descriptive message (1.7)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.6, 1.7_

  - [ ]* 12.3 Write property test for connector availability after errors
    - **Property 22: The Connector remains available after any error**
    - **Validates: Requirements 10.2**
    - _Requirements: 10.2_

- [ ] 13. Add end-to-end integration tests against a mocked Cost_API
  - [ ]* 13.1 Write integration tests for the full pipeline
    - Run the tool against a mocked Cost_API returning representative payloads, verifying validate → request → parse → tag → type-check → return for a happy path
    - Cover one representative failure per category (`invalid-parameter`, `unreachable-source`, `unexpected-error`, `missing-data`)
    - _Requirements: 6.1, 6.4, 6.5, 6.6, 6.7, 6.8, 10.3_

- [x] 14. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

## Notes

- Tasks marked with `*` are optional test sub-tasks and can be skipped for a faster MVP; core implementation tasks are never optional.
- Each of the 25 correctness properties is implemented by a single Hypothesis property-based test running a minimum of 100 iterations, tagged `# Feature: billing-usage-ingestion, Property {number}: {property_text}`, with the Cost_API mocked via `httpx.MockTransport`/`respx`.
- Each task references specific requirement sub-clauses for traceability; property tasks additionally reference their design property number.
- Checkpoints ensure incremental validation as foundational modules, I/O layers, and orchestration come together.
- Deferred tools (Requirements 7, 8, 9) are out of scope and have no tasks or tests in this iteration.

## Task Dependency Graph

```json
{
  "waves": [
    { "id": 0, "tasks": ["1.1"] },
    { "id": 1, "tasks": ["2.1", "3.1", "4.1", "5.1", "6.1", "8.1", "9.1"] },
    { "id": 2, "tasks": ["2.2", "3.2", "3.3", "4.2", "4.3", "4.4", "4.5", "4.6", "5.2", "5.3", "5.4", "5.5", "5.6", "6.2", "6.3", "6.4", "6.5", "8.2", "8.3", "8.4", "8.5", "9.2", "9.3"] },
    { "id": 3, "tasks": ["11.1"] },
    { "id": 4, "tasks": ["11.2"] },
    { "id": 5, "tasks": ["11.3", "11.4", "11.5", "11.6", "11.7"] },
    { "id": 6, "tasks": ["12.1"] },
    { "id": 7, "tasks": ["12.2", "12.3"] },
    { "id": 8, "tasks": ["13.1"] }
  ]
}
```
