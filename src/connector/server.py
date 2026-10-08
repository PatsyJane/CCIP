"""Connector bootstrap (design component 1).

Creates ``FastMCP("Cloud Billing & Telemetry Connector")``, triggers config
load, builds the shared HTTP client, registers the AWS CUR Cost_Tool, and calls
``mcp.run()``.

Responsibilities (Requirements 1.1-1.7):
    - Instantiate the FastMCP server with the exact required name (Req 1.1).
    - Register the AWS CUR Cost_Tool as the only Ingestion_Tool, discoverable by
      name through the MCP tool-listing interface, defined as an async function
      (Req 1.2, 1.3).
    - Fail fast on missing config (``ConfigError``) or tool-registration failure
      (``ToolRegistrationError``), aborting startup, serving no tools, and
      naming the failure (Req 1.5, 1.6).
    - Run the MCP server via the FastMCP run entry point when executed as the
      program entry point (Req 1.4), exiting non-zero on an MCP start failure
      (Req 1.7).

FastMCP API used (fastmcp 2.14.x)
---------------------------------
- ``FastMCP(name)`` — constructs the server with the given display name (Req 1.1).
- ``mcp.tool(name="aws_cur_cost")(callable)`` — registers ``callable`` as a tool
  discoverable under the name ``"aws_cur_cost"`` and returns a ``FunctionTool``.
  The registered ``callable`` is the coroutine function produced by
  :func:`connector.tools.aws_cur_cost.make_aws_cur_cost`, so the tool satisfies
  the "asynchronous function" requirement (Req 1.3).
- ``mcp.run()`` — the blocking run entry point started from ``main`` (Req 1.4).
"""

from __future__ import annotations

import sys

from fastmcp import FastMCP

from .config import AppConfig, ConfigError, load_config
from .http_client import HttpClient
from .tools.aws_cur_cost import make_aws_cur_cost

__all__ = [
    "SERVER_NAME",
    "AWS_CUR_COST_TOOL_NAME",
    "ToolRegistrationError",
    "register_aws_cur_cost_tool",
    "build_server",
    "main",
]

#: The exact FastMCP server name required by the spec (Req 1.1). Must not change.
SERVER_NAME: str = "Cloud Billing & Telemetry Connector"

#: The name under which the AWS CUR Cost_Tool is registered and discoverable via
#: the MCP tool-listing interface (Req 1.2).
AWS_CUR_COST_TOOL_NAME: str = "aws_cur_cost"


class ToolRegistrationError(Exception):
    """Raised when an Ingestion_Tool fails to register on the Connector.

    The message names the Ingestion_Tool that failed to register so startup can
    abort while serving no tools and emitting an error identifying the offending
    tool (Requirement 1.6).
    """


def register_aws_cur_cost_tool(
    mcp: FastMCP,
    config: AppConfig,
    http: HttpClient,
) -> None:
    """Register the AWS CUR Cost_Tool as the only Ingestion_Tool (Req 1.2, 1.3).

    Binds ``config`` and ``http`` into the agent-facing async tool callable via
    :func:`~connector.tools.aws_cur_cost.make_aws_cur_cost` and registers it with
    FastMCP under :data:`AWS_CUR_COST_TOOL_NAME`, so the tool is individually
    discoverable by name through ``mcp.get_tools()`` (Req 1.2). The registered
    callable is a coroutine function, satisfying the async-tool requirement
    (Req 1.3).

    Any failure while building or registering the tool is wrapped in a
    :class:`ToolRegistrationError` naming the failing tool, so the caller can
    abort startup and serve no tools (Req 1.6).
    """
    try:
        tool = make_aws_cur_cost(config, http)
        # ``mcp.tool(name=...)`` returns the registered FunctionTool; the tool is
        # then discoverable by name via ``mcp.get_tools()`` (Req 1.2).
        mcp.tool(name=AWS_CUR_COST_TOOL_NAME)(tool)
    except Exception as exc:  # noqa: BLE001 — surface as a named registration error
        raise ToolRegistrationError(
            f"failed to register Ingestion_Tool '{AWS_CUR_COST_TOOL_NAME}': {exc}"
        ) from exc


def build_server() -> FastMCP:
    """Wire the Connector together and return the configured FastMCP instance.

    Steps (Req 1.1, 1.2, 1.3, 1.5, 1.6):

    1. Load and validate the Env_Config, failing fast with a ``ConfigError``
       naming the first missing/blank required value (Req 1.5).
    2. Construct the single shared :class:`~connector.http_client.HttpClient`
       with the 30s timeout (Req 11.1).
    3. Instantiate ``FastMCP`` with the exact required name (Req 1.1).
    4. Register the AWS CUR Cost_Tool as the only tool; a registration failure
       raises :class:`ToolRegistrationError` (Req 1.6).

    Config is loaded *before* the server is constructed so that a missing-config
    abort serves no tools. Raises ``ConfigError`` or ``ToolRegistrationError``;
    ``main`` converts these into a non-zero exit (Req 1.5, 1.6, 1.7).
    """
    # Req 1.5 / 12.* — fail fast on missing/blank required config before wiring.
    config = load_config()

    # Req 11.1 — single shared async HTTP client reused across invocations.
    http = HttpClient(timeout=30.0)

    # Req 1.1 — server name is exactly the required string.
    mcp = FastMCP(SERVER_NAME)

    # Req 1.2, 1.3, 1.6 — register the only tool (async, discoverable by name).
    register_aws_cur_cost_tool(mcp, config, http)

    return mcp


def main() -> None:
    """Program entry point: build the Connector and run the MCP server.

    Guards the two startup-abort paths and the run failure (Req 1.5, 1.6, 1.7):

    - ``ConfigError`` — a required Env_Config value is missing/blank; abort
      startup, serve no tools, name the value, exit non-zero (Req 1.5).
    - ``ToolRegistrationError`` — the AWS CUR Cost_Tool failed to register; abort
      startup, serve no tools, name the failing tool, exit non-zero (Req 1.6).
    - Any exception from ``mcp.run()`` — the MCP server failed to start; exit
      non-zero with a descriptive message (Req 1.7).

    On each failure a message is written to ``stderr`` and the process exits with
    a non-successful status via :func:`sys.exit`.
    """
    try:
        server = build_server()
    except ConfigError as exc:
        # Req 1.5 — abort startup naming the missing configuration value.
        sys.stderr.write(f"startup aborted: {exc}\n")
        sys.exit(1)
    except ToolRegistrationError as exc:
        # Req 1.6 — abort startup, serve no tools, name the failing tool.
        sys.stderr.write(f"tool registration failed: {exc}\n")
        sys.exit(1)

    try:
        # Req 1.4 — start the MCP server via the FastMCP run entry point.
        server.run()
    except Exception as exc:  # noqa: BLE001 — Req 1.7: non-zero exit on start failure
        sys.stderr.write(f"server failed to start: {exc}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
