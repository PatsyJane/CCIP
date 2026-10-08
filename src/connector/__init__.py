"""Cloud Billing & Telemetry Connector.

FastMCP server package for Stage 1 ("Ingest Billing & Usage Data") of the
Cloud Cost Intelligence Pipeline. For the PoC iteration this package hosts a
single MCP tool, the AWS CUR Cost_Tool, which fetches cost data via an HTTP
POST to a configurable Cost_API endpoint.

See .kiro/specs/billing-usage-ingestion/design.md for the full design.
"""

__all__: list[str] = []
