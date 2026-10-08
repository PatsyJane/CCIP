# Requirements Document

## Introduction

This feature implements Stage 1 ("Ingest Billing & Usage Data") of a Cloud Cost Intelligence Pipeline built with LangGraph. It exposes Model Context Protocol (MCP) tools, implemented with FastMCP in Python, that LangGraph agents call to fetch cost and usage metrics.

This iteration is a **Proof of Concept (PoC) scoped to a single AWS account**. The target AWS account identifier, AWS region, and connection details are supplied through a `.env` file (the Env_Config) loaded as environment variables, so the Connector can be pointed at one account without code changes. The only Ingestion_Tool implemented in the PoC is the **AWS CUR Cost_Tool**, which retrieves cost data by making an HTTP POST to a configurable Cost API endpoint rather than by calling the AWS Cost Explorer SDK or querying Athena/S3 directly.

The following data sources are **planned future sources** and are **DEFERRED / OUT OF SCOPE** for the PoC iteration:

1. **GCP Billing Export** — via BigQuery queries against the GCP billing export dataset.
2. **CloudWatch / Monitoring** — via AWS CloudWatch (or an equivalent such as Datadog/Prometheus) usage metrics APIs.
3. **Application Telemetry** — via internal observability APIs that report requests, jobs, or images processed.

The AWS CUR Cost_Tool accepts standardized parameters, returns data at daily or hourly granularity tagged by account, service, and environment, enforces consistent data types, and returns structured JSON errors instead of crashing. This design lets a LangGraph agent detect insufficient historical data and trigger downstream "Cold Start Mode" (Stage 2), and feeds the Baseline / Forecast Engine (Stage 3).

## Glossary

- **MCP**: Model Context Protocol, the interface standard through which LangGraph agents invoke tools.
- **FastMCP**: The Python framework used to define and serve MCP tools (`mcp = FastMCP(...)`, `mcp.run()`).
- **Connector**: The FastMCP server instance that hosts ingestion tools in this feature. For the PoC iteration the Connector hosts the AWS CUR Cost_Tool only; the GCP Billing Export, CloudWatch/Monitoring, and Application Telemetry tools are deferred.
- **Env_Config**: The `.env` file, loaded as environment variables, that supplies configuration for the PoC: the target AWS account identifier and name, the AWS region, the Cost_API base URL, the `provider` value, the `project` value, and, WHERE AWS credentials are required to reach the Cost_API, the AWS credentials. The AWS account identifier/name is used to populate the `account` Dimension_Tag, and the Cost_API base URL, `provider`, and `project` are used to query cost data.
- **Cost_API**: The external HTTP service exposing `POST /api/costs` that returns cloud cost data. The Cost_API base URL, and the `provider` and `project` values sent in the request body, are supplied by the Env_Config.
- **Ingestion_Tool**: Any MCP tool defined by the Connector that fetches cost or usage data from a data source.
- **Cost_Tool**: An Ingestion_Tool that returns monetary spend metrics (AWS CUR, GCP Billing Export).
- **Usage_Tool**: An Ingestion_Tool that returns non-monetary usage metrics (CloudWatch/Monitoring, Application Telemetry).
- **Granularity**: The time-bucket size of returned data; the allowed values are `DAILY` and `HOURLY`.
- **Data_Item**: A single time-bucketed record returned by an Ingestion_Tool, containing a value and its dimension tags.
- **Dimension_Tags**: The metadata attached to every Data_Item: `account`, `service`, and `environment`. For the PoC, the `account` tag is fixed to the single AWS account configured in the Env_Config.
- **Cost_Value**: A monetary spend amount, represented as a floating-point number.
- **Usage_Value**: A non-monetary usage count or measure (requests, jobs, images processed), represented as an integer or floating-point number.
- **Structured_Error**: A JSON object returned by an Ingestion_Tool describing a failure, of the form `{"error": <message>}`, returned in place of a crash or raised exception.
- **Cold_Start_Mode**: A downstream (Stage 2) behavior in which the LangGraph agent applies safety thresholds and peer benchmarks when historical data is insufficient.
- **Agent**: A LangGraph agent that invokes Ingestion_Tools through the Connector.

## Requirements

### Requirement 1: FastMCP Connector Server

**User Story:** As a pipeline developer, I want a single FastMCP server that hosts the ingestion tool, so that LangGraph agents have one connector endpoint to call for billing data.

#### Acceptance Criteria

1. THE Connector SHALL instantiate a FastMCP server whose name is exactly "Cloud Billing & Telemetry Connector".
2. THE Connector SHALL register the AWS CUR Cost_Tool as the only Ingestion_Tool for the PoC iteration, such that the AWS CUR Cost_Tool is individually discoverable by name through the MCP tool-listing interface.
3. THE Connector SHALL define the AWS CUR Cost_Tool as an asynchronous function.
4. WHEN the Connector module is run as the program entry point, THE Connector SHALL start the MCP server via the FastMCP run entry point.
5. WHEN the Connector starts, THE Connector SHALL load the target AWS account identifier, the AWS region, the Cost_API base URL, the `provider` value, and the `project` value from the Env_Config, and IF any required Env_Config value is missing, THEN THE Connector SHALL abort startup with an error identifying the missing configuration value.
6. IF registration of the AWS CUR Cost_Tool fails, THEN THE Connector SHALL abort startup, SHALL NOT serve any MCP tools, and SHALL emit an error indication identifying the Ingestion_Tool that failed to register.
7. IF the MCP server fails to start, THEN THE Connector SHALL exit with a non-successful status and SHALL emit an error indication describing the startup failure.

### Requirement 2: Standardized Tool Parameters

**User Story:** As a LangGraph agent, I want every ingestion tool to accept the same standardized parameters, so that I can call any data source with a consistent interface.

**Note:** For the PoC, the `account` is NOT an agent-supplied parameter — it is fixed by the Env_Config target account. Criteria referencing the `metric_name` parameter apply to Usage_Tools, which are deferred for the PoC iteration.

#### Acceptance Criteria

1. THE Connector SHALL require each Ingestion_Tool to accept a `start_date` parameter and an `end_date` parameter, each provided as a string in `YYYY-MM-DD` format, that together identify the inclusive query time range from the start of `start_date` through the end of `end_date`.
2. THE Connector SHALL require each Ingestion_Tool to accept a `granularity` parameter whose accepted string values are `DAILY`, `MONTHLY`, and `HOURLY`, and whose default value when the parameter is absent is `DAILY`.
3. (Deferred — applies to Usage_Tools) WHERE an Ingestion_Tool is a Usage_Tool, THE Connector SHALL require the tool to accept a `metric_name` parameter, provided as a non-empty string of 1 to 256 characters, identifying the usage metric to fetch.
4. IF `start_date` or `end_date` is absent, is not a string, does not match the `YYYY-MM-DD` format, or does not represent a valid calendar date, THEN THE Ingestion_Tool SHALL return a Structured_Error that identifies which date parameter was invalid and the reason, and SHALL NOT return any usage or cost data.
5. IF `end_date` precedes `start_date`, THEN THE Ingestion_Tool SHALL return a Structured_Error that identifies the invalid range and includes the offending `start_date` and `end_date` values, and SHALL NOT return any usage or cost data.
6. IF `granularity` is provided but is not one of the accepted values `DAILY`, `MONTHLY`, or `HOURLY`, THEN THE Ingestion_Tool SHALL return a Structured_Error that identifies the invalid `granularity` value and the accepted values, and SHALL NOT return any usage or cost data.
7. (Deferred — applies to Usage_Tools) WHERE an Ingestion_Tool is a Usage_Tool, IF `metric_name` is absent, is not a string, or is an empty string, THEN THE Ingestion_Tool SHALL return a Structured_Error identifying the invalid `metric_name` parameter, and SHALL NOT return any usage data.

### Requirement 3: Uniform Granularity Validation

**User Story:** As a baseline engine consumer, I want all data returned strictly at daily or hourly granularity, so that historical data is bucketed consistently across sources.

#### Acceptance Criteria

1. WHEN an Agent requests granularity `DAILY` or `HOURLY`, THE Ingestion_Tool SHALL return Data_Items in non-overlapping time buckets aligned to the requested Granularity.
2. WHEN an Agent requests a granularity coarser than `DAILY`, such as `WEEKLY` or `MONTHLY`, THE Ingestion_Tool SHALL return Data_Items decomposed to `DAILY` Granularity.
3. IF an Agent requests a granularity value that is not `DAILY`, `HOURLY`, `WEEKLY`, or `MONTHLY`, THEN THE Ingestion_Tool SHALL return a Structured_Error describing the unsupported granularity and SHALL NOT return any Data_Items.
4. IF the requested granularity is missing, null, or empty, THEN THE Ingestion_Tool SHALL return a Structured_Error describing the missing granularity and SHALL NOT return any Data_Items.
5. WHEN an Ingestion_Tool returns Data_Items, THE Ingestion_Tool SHALL include in the response the effective Granularity, constrained to `DAILY` or `HOURLY`.

### Requirement 4: Strict Dimension Tagging

**User Story:** As the downstream baseline engine, I want every cost and usage item tagged by account, service, and environment, so that I can partition historical data by these dimensions.

#### Acceptance Criteria

1. WHEN an Ingestion_Tool returns a response, THE Ingestion_Tool SHALL attach Dimension_Tags containing `account`, `service`, and `environment` to every Data_Item in the response.
2. WHEN an Ingestion_Tool returns a Data_Item during the PoC iteration, THE Ingestion_Tool SHALL populate the `account` Dimension_Tag with the target account identifier supplied by the Env_Config.
3. WHEN a source record is missing a value for `service` or `environment`, THE Ingestion_Tool SHALL populate the missing Dimension_Tag with the value `"UNKNOWN"` rather than omitting the tag.
4. WHEN a source record provides an empty or whitespace-only value for `service` or `environment`, THE Ingestion_Tool SHALL treat that value as missing and populate the Dimension_Tag with the value `"UNKNOWN"`.
5. WHEN an Ingestion_Tool returns a Data_Item, THE Ingestion_Tool SHALL return the Data_Item with all three Dimension_Tags present and each tag value non-empty.
6. IF all three Dimension_Tags cannot be attached to a Data_Item, THEN THE Ingestion_Tool SHALL return a Structured_Error describing the tagging failure while preserving in the response any Data_Items that were correctly tagged.

### Requirement 5: Data Type Consistency

**User Story:** As a forecast engine consumer, I want cost and usage values in predictable numeric types, so that I can perform arithmetic without type coercion errors.

#### Acceptance Criteria

1. THE Cost_Tool SHALL return each Cost_Value as a floating-point number representing actual spend within the inclusive range 0.00 to 999,999,999.99.
2. THE Usage_Tool SHALL return each Usage_Value as an integer within the inclusive range 0 to 999,999,999 or as a floating-point number within the inclusive range 0.00 to 999,999,999.99, representing a usage count or measure.
3. WHEN a source returns a numeric value as text, THE Ingestion_Tool SHALL convert the value to its numeric type before returning it.
4. IF a source value cannot be converted to the expected numeric type, THEN THE Ingestion_Tool SHALL return a Structured_Error that identifies the field name and the failing value, and SHALL exclude the unconverted value from the response.
5. IF a required numeric value is missing, empty, or absent from a source record, THEN THE Ingestion_Tool SHALL return a Structured_Error that identifies the field name of the missing value.
6. IF a source value converts successfully but falls outside the defined range for its type, THEN THE Ingestion_Tool SHALL return a Structured_Error that identifies the field name, the out-of-range value, and the permitted range.

### Requirement 6: AWS CUR Cost Tool

**User Story:** As an Agent, I want a tool that fetches AWS cost data, so that I can retrieve actual AWS spend for a time range and granularity.

#### Acceptance Criteria

1. WHEN an Agent invokes the AWS CUR Cost_Tool with `start_date` and `end_date` as ISO 8601 dates in `YYYY-MM-DD` format where `start_date` is less than or equal to `end_date` and the range does not exceed 366 days, and a `granularity` value from the set {DAILY, MONTHLY, HOURLY}, THE AWS CUR Cost_Tool SHALL issue an HTTP POST request to `{Cost_API base URL}/api/costs`, with headers `Content-Type: application/json` and `accept: */*`, and a JSON request body containing `provider` and `project` from the Env_Config and `start`, `end`, and `granularity` derived from the tool parameters.
2. IF the `start_date`, `end_date`, or `granularity` is missing, malformed, or outside its defined bounds, including `start_date` greater than `end_date` or a range exceeding 366 days, THEN THE AWS CUR Cost_Tool SHALL return a Structured_Error identifying the invalid parameter and SHALL NOT issue a request to the Cost_API.
3. WHEN the AWS CUR Cost_Tool builds the Cost_API request body, THE AWS CUR Cost_Tool SHALL map the tool `start_date` parameter to the body `start` field, the tool `end_date` parameter to the body `end` field, and the tool `granularity` parameter to the body `granularity` field, and SHALL populate the body `provider` and `project` fields from the Env_Config.
4. WHEN the Cost_API returns a successful 2xx JSON response, THE AWS CUR Cost_Tool SHALL parse the cost data and return each Cost_Value as a floating-point number greater than or equal to 0.0, with `account`, `service`, and `environment` Dimension_Tags, where the `account` tag is populated with the target account identifier supplied by the Env_Config and the `service` and `environment` tags are populated with their resolved value or an explicit empty indicator when the source provides no value.
5. IF the Cost_API returns a status code outside the 200 to 299 range, THEN THE AWS CUR Cost_Tool SHALL return a Structured_Error containing the received status code, without returning partial Cost_Values.
6. IF the Cost_API response is not valid JSON or is missing the expected cost fields, THEN THE AWS CUR Cost_Tool SHALL return a Structured_Error describing the unexpected response shape, without returning partial Cost_Values.
7. IF the Cost_API is unreachable or does not respond within its configured timeout, THEN THE AWS CUR Cost_Tool SHALL return a Structured_Error categorized as `unreachable-source`, without returning partial Cost_Values.
8. IF the Cost_API response contains no cost data for the requested range, THEN THE AWS CUR Cost_Tool SHALL return a Structured_Error categorized as `missing-data` indicating no data was found for the range.

### Requirement 7: GCP Billing Export Cost Tool

> **DEFERRED — OUT OF SCOPE for the current PoC iteration.** This requirement is documented for a later iteration and is not implemented in the single-AWS-account PoC.

**User Story:** As an Agent, I want a tool that fetches GCP billing data, so that I can retrieve actual GCP spend for a time range and granularity.

#### Acceptance Criteria

1. WHEN an Agent invokes the GCP Billing Export Cost_Tool with `start_date` and `end_date` as ISO 8601 dates where `start_date` is less than or equal to `end_date`, and a `granularity` value from the set {DAILY, MONTHLY, HOURLY}, THE Cost_Tool SHALL query the GCP billing export dataset via BigQuery for cost data over the range.
2. IF the `start_date` or `end_date` is malformed, the range is inverted such that `start_date` is greater than `end_date`, or the `granularity` is not one of {DAILY, MONTHLY, HOURLY}, THEN THE GCP Billing Export Cost_Tool SHALL return a Structured_Error identifying the invalid parameter and SHALL NOT perform a BigQuery query.
3. WHEN the BigQuery data source returns cost data, THE GCP Billing Export Cost_Tool SHALL return each Cost_Value as a floating-point number rounded to 2 decimal places, with `account`, `service`, and `environment` Dimension_Tags, each tag populated with a non-empty string value or an explicit empty indicator when the source provides no value.
4. IF the BigQuery data source is unreachable or does not respond within 30 seconds, THEN THE GCP Billing Export Cost_Tool SHALL return a Structured_Error indicating the data source was unreachable, without returning partial Cost_Values.
5. IF no cost data exists for the requested range, THEN THE GCP Billing Export Cost_Tool SHALL return a Structured_Error indicating no data was found for the range.

### Requirement 8: CloudWatch / Monitoring Usage Tool

> **DEFERRED — OUT OF SCOPE for the current PoC iteration.** This requirement is documented for a later iteration and is not implemented in the single-AWS-account PoC.

**User Story:** As an Agent, I want a tool that fetches monitoring usage metrics, so that I can retrieve infrastructure usage measures for a time range and granularity.

#### Acceptance Criteria

1. WHEN an Agent invokes the CloudWatch/Monitoring Usage_Tool with a `metric_name` (1 to 255 characters), `start_date` and `end_date` (ISO 8601 date-time values where `start_date` is strictly earlier than `end_date` and the range does not exceed 366 days), and a `granularity` value from the set {HOURLY, DAILY, MONTHLY}, THE Usage_Tool SHALL query the CloudWatch or equivalent monitoring API for the named metric over the range and return Usage_Values within 10 seconds.
2. IF the `metric_name`, `start_date`, `end_date`, or `granularity` is missing, malformed, or outside its defined bounds (including `start_date` not earlier than `end_date` or a range exceeding 366 days), THEN THE CloudWatch/Monitoring Usage_Tool SHALL reject the request, return a Structured_Error identifying the invalid parameter, and not query the monitoring API.
3. WHEN the monitoring API returns usage data, THE CloudWatch/Monitoring Usage_Tool SHALL return each Usage_Value as an integer or floating-point number with `account`, `service`, and `environment` Dimension_Tags, each tag populated with its resolved value or an explicit empty indicator when the source provides no value.
4. IF the monitoring data source is unreachable or does not respond within 10 seconds, THEN THE CloudWatch/Monitoring Usage_Tool SHALL abort the request after at most 3 retry attempts and return a Structured_Error indicating the data source was unreachable, without returning partial Usage_Values.
5. IF the monitoring API responds successfully but contains no usage data for the requested metric and range, THEN THE CloudWatch/Monitoring Usage_Tool SHALL return a Structured_Error indicating no data was found for the range.

### Requirement 9: Application Telemetry Usage Tool

> **DEFERRED — OUT OF SCOPE for the current PoC iteration.** This requirement is documented for a later iteration and is not implemented in the single-AWS-account PoC.

**User Story:** As an Agent, I want a tool that fetches application telemetry, so that I can retrieve counts of requests, jobs, or images processed for a time range.

#### Acceptance Criteria

1. WHEN an Agent invokes the Application Telemetry Usage_Tool with a `metric_name` from the set {requests, jobs, images}, `start_date` and `end_date` as ISO 8601 dates where `start_date` is less than or equal to `end_date` and the range does not exceed 366 days, and a `granularity` value from the set {HOURLY, DAILY, MONTHLY}, THE Usage_Tool SHALL query the internal observability API for the named metric over the range.
2. IF the `metric_name`, `start_date`, `end_date`, or `granularity` is missing, malformed, or outside its defined bounds, THEN THE Application Telemetry Usage_Tool SHALL return a Structured_Error identifying the invalid parameter and SHALL NOT query the observability API.
3. WHEN the observability API returns telemetry data, THE Application Telemetry Usage_Tool SHALL return one Usage_Value per granularity interval as an integer or floating-point number with `account`, `service`, and `environment` Dimension_Tags present.
4. IF the observability API is unreachable or does not respond within 30 seconds, THEN THE Application Telemetry Usage_Tool SHALL return a Structured_Error indicating the data source was unreachable, without returning partial Usage_Values.
5. IF the observability API responds successfully but contains no telemetry data for the requested metric and range, THEN THE Application Telemetry Usage_Tool SHALL return a Structured_Error indicating no data was found for the range.

### Requirement 10: Graceful Error Handling for LangGraph

**User Story:** As a LangGraph agent, I want tools to return structured JSON errors instead of crashing, so that I can handle failures and trigger Cold_Start_Mode when historical data is insufficient.

#### Acceptance Criteria

1. IF an Ingestion_Tool encounters an unreachable data source, a missing-data condition, an invalid parameter, or an unexpected exception, THEN THE Ingestion_Tool SHALL return a Structured_Error as a JSON object with an `error` field carrying a human-readable message, without raising an uncaught exception or terminating the process.
2. WHEN an Ingestion_Tool returns a Structured_Error, THE Connector SHALL remain available to accept and process subsequent tool calls without requiring a restart.
3. WHEN an Ingestion_Tool returns a Structured_Error, THE Ingestion_Tool SHALL include a machine-readable `category` field whose value is exactly one of `missing-data`, `unreachable-source`, `invalid-parameter`, or `unexpected-error`.
4. IF an Ingestion_Tool encounters a missing-data condition, THEN THE Ingestion_Tool SHALL return a Structured_Error categorized as `missing-data` that signals the Agent may trigger Cold_Start_Mode.
5. IF an Ingestion_Tool encounters an unreachable-source condition, THEN THE Ingestion_Tool SHALL return a Structured_Error categorized as `unreachable-source` that does not signal the Agent to trigger Cold_Start_Mode.

### Requirement 11: HTTP Client Usage for Remote Sources

**User Story:** As a pipeline developer, I want tools that call remote HTTP APIs, including the AWS CUR Cost_Tool call to the Cost_API, to use a shared asynchronous HTTP client, so that concurrent agent calls are efficient and non-blocking.

#### Acceptance Criteria

1. WHERE an Ingestion_Tool fetches data over HTTP, including the AWS CUR Cost_Tool request to the Cost_API, THE Ingestion_Tool SHALL perform the request using a shared asynchronous HTTP client instance (httpx.AsyncClient) that is reused across concurrent tool invocations rather than instantiating a new client per request.
2. WHILE two or more Ingestion_Tool invocations execute concurrently, THE Ingestion_Tool SHALL perform each HTTP request without blocking the event loop, such that a request in progress does not delay the initiation of other concurrent requests.
3. IF an HTTP request returns a status code outside the 200 to 299 range, THEN THE Ingestion_Tool SHALL return a Structured_Error containing the received status code and the requested source identifier, and SHALL NOT return partial or fabricated data.
4. WHEN an HTTP request exceeds its configured timeout of 30 seconds, THE Ingestion_Tool SHALL cancel the request and return a Structured_Error indicating a timeout, including the requested source identifier and the elapsed timeout duration.
5. IF an HTTP request fails due to a connection or network error before a response is received, THEN THE Ingestion_Tool SHALL return a Structured_Error indicating a connection failure, including the requested source identifier.

### Requirement 12: Environment-Based Configuration

**User Story:** As a PoC developer, I want AWS account and connection details supplied via a `.env` file, so that I can point the connector at one account without changing code.

#### Acceptance Criteria

1. WHEN the Connector starts, THE Connector SHALL load the target AWS account identifier, the target AWS account name, and the AWS region from the Env_Config.
2. WHEN the Connector starts, THE Connector SHALL load the Cost_API base URL, the `provider` value, and the `project` value from the Env_Config.
3. WHERE AWS credentials are required to reach the Cost_API, WHEN the Connector starts, THE Connector SHALL load the AWS credentials from the Env_Config.
4. IF any required Env_Config value is missing, empty, or whitespace-only, THEN THE Connector SHALL abort startup and emit a startup error that identifies the missing configuration value by name.
5. WHEN an Ingestion_Tool returns a Data_Item, THE Ingestion_Tool SHALL populate the `account` Dimension_Tag with the single AWS account identifier supplied by the Env_Config.
6. WHERE AWS credentials are loaded from the Env_Config, WHEN an Ingestion_Tool returns a Structured_Error or a successful response, THE Ingestion_Tool SHALL exclude the AWS credential values from the returned content.
7. WHERE AWS credentials are loaded from the Env_Config, WHEN the Connector or an Ingestion_Tool writes a log entry, THE Connector SHALL exclude the AWS credential values from the log entry.
