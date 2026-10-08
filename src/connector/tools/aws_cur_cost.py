"""AWS CUR Cost_Tool (design component 3).

The single async MCP tool for the PoC. Orchestrates the per-call pipeline:
validate -> build request -> HTTP call -> parse/adapt -> tag/normalize ->
type-check -> return, with every stage able to short-circuit into the
structured-error factory. Wrapped so no exception escapes the tool.

Requirements: 1.2, 1.3, 2.x, 3.x, 4.x, 5.x, 6.x, 10.x, 11.x.

This module implements two layers:

**Request-body building** (task 11.1): the ``CostApiRequest`` data model plus
the pure helpers that turn validated tool parameters + ``AppConfig`` into the
Cost_API request URL, headers, and JSON body.

**Pipeline orchestration** (task 11.2): the async :func:`aws_cur_cost` core
function that wires validation -> build request -> POST (shared client) ->
parse/adapt -> tag/normalize -> numeric coerce/validate -> success payload,
with every stage short-circuiting to the structured-error factory and the
whole body wrapped in a top-level guard that maps any unhandled exception to
``unexpected-error`` (totality, Req 10.1). :func:`make_aws_cur_cost` binds an
``AppConfig`` and ``HttpClient`` into a clean agent-facing async callable
(signature ``start_date, end_date, granularity="DAILY"``) that task 12.1's
FastMCP ``@mcp.tool()`` wrapper can register directly.

Function shape (chosen)
-----------------------
- :func:`aws_cur_cost(config, http, start_date, end_date, granularity="DAILY")`
  — the testable, reusable async core. Dependencies (config, http client) are
  explicit parameters so tests can inject an ``httpx.MockTransport``-backed
  :class:`~connector.http_client.HttpClient`.
- :func:`make_aws_cur_cost(config, http)` — a factory returning
  ``async def tool(start_date, end_date, granularity="DAILY") -> dict`` with the
  agent-facing signature, with ``config``/``http`` closed over. The server
  binds these once at startup and hands the returned coroutine to FastMCP.

Requirements addressed by this module: 6.1, 6.2, 6.3, 6.4, 6.5, 6.6, 6.7, 6.8,
3.1, 3.5, 10.1, 10.2.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Awaitable, Callable

from ..config import AppConfig
from ..dimensions import DataItem, TaggingError, tag_item
from ..errors import ErrorCategory, structured_error
from ..http_client import HttpClient, HttpResult
from ..numeric import coerce_cost_value
from ..response_adapter import parse_cost_response
from ..validation import (
    ValidationError,
    normalize_granularity,
    parse_date,
    validate_range,
)

__all__ = [
    "COST_API_PATH",
    "REQUEST_HEADERS",
    "COST_VALUE_FIELD",
    "CostApiRequest",
    "build_cost_api_url",
    "build_request_headers",
    "build_cost_api_request",
    "build_request_body",
    "aws_cur_cost",
    "make_aws_cur_cost",
]

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Path appended to the Env_Config base URL to reach the Cost_API endpoint.
#: The tool issues ``POST {base_url}/api/costs`` (Req 6.1).
COST_API_PATH: str = "/api/costs"

#: Fixed request headers for every Cost_API call (Req 6.1). ``Content-Type``
#: declares the JSON request body; ``accept`` mirrors the design's ``*/*``.
#: A fresh dict is what callers should send; this module-level mapping is the
#: canonical source, copied per request by :func:`build_request_headers` so a
#: caller can never mutate the shared constant.
REQUEST_HEADERS: dict[str, str] = {
    "Content-Type": "application/json",
    "accept": "*/*",
}

#: Field name used when coercing/validating a parsed record's cost value, so
#: numeric errors name the field consistently (Req 5.4, 5.5, 5.6).
COST_VALUE_FIELD: str = "cost_value"


# ---------------------------------------------------------------------------
# Cost_API request body model (design Data Models: CostApiRequest)
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CostApiRequest:
    """The JSON request body sent to the Cost_API (design Data Models).

    Field mapping (Req 6.3):

    - ``provider`` / ``project`` come from the Env_Config (:class:`AppConfig`).
    - ``start``       <- the tool ``start_date`` parameter.
    - ``end``         <- the tool ``end_date`` parameter.
    - ``granularity`` <- the *effective* (post-decomposition) granularity
      returned by :func:`connector.validation.normalize_granularity`
      (``DAILY`` or ``HOURLY``).

    The dataclass is frozen so a built request is immutable. Use
    :meth:`to_body` (or :func:`build_request_body`) to obtain the plain ``dict``
    that is serialized as the JSON body.
    """

    provider: str
    project: str
    start: str
    end: str
    granularity: str

    def to_body(self) -> dict[str, Any]:
        """Return the request as a plain JSON-serializable ``dict``.

        The key order matches the design body contract
        (``provider``, ``project``, ``start``, ``end``, ``granularity``).
        """
        return asdict(self)


# ---------------------------------------------------------------------------
# Pure request-building helpers (Req 6.1, 6.3)
# ---------------------------------------------------------------------------


def build_cost_api_url(config: AppConfig) -> str:
    """Build the Cost_API endpoint URL ``{base_url}/api/costs`` (Req 6.1).

    Any trailing ``/`` on the configured base URL is stripped before appending
    :data:`COST_API_PATH`, so both ``http://host:8001`` and
    ``http://host:8001/`` produce ``http://host:8001/api/costs``.
    """
    base = config.cost_api_base_url.rstrip("/")
    return f"{base}{COST_API_PATH}"


def build_request_headers() -> dict[str, str]:
    """Return a fresh copy of the fixed Cost_API request headers (Req 6.1).

    A copy is returned so callers may not mutate the shared
    :data:`REQUEST_HEADERS` constant.
    """
    return dict(REQUEST_HEADERS)


def build_cost_api_request(
    config: AppConfig,
    start_date: str,
    end_date: str,
    granularity: str,
) -> CostApiRequest:
    """Assemble the :class:`CostApiRequest` for a validated call (Req 6.3).

    Args:
        config: The loaded Env_Config; supplies ``provider`` and ``project``.
        start_date: The validated ``start_date`` tool parameter (``YYYY-MM-DD``),
            mapped to the body ``start`` field.
        end_date: The validated ``end_date`` tool parameter (``YYYY-MM-DD``),
            mapped to the body ``end`` field.
        granularity: The *effective* (post-decomposition) granularity
            (``DAILY`` or ``HOURLY``) from ``normalize_granularity``, mapped to
            the body ``granularity`` field.

    Returns:
        The immutable :class:`CostApiRequest`.
    """
    return CostApiRequest(
        provider=config.provider,
        project=config.project,
        start=start_date,
        end=end_date,
        granularity=granularity,
    )


def build_request_body(
    config: AppConfig,
    start_date: str,
    end_date: str,
    granularity: str,
) -> dict[str, Any]:
    """Build the Cost_API JSON request body ``dict`` (Req 6.1, 6.3).

    Convenience wrapper over :func:`build_cost_api_request` that returns the
    plain ``dict`` ready to hand to the HTTP client's ``post_json``. Maps
    ``start_date`` -> ``start``, ``end_date`` -> ``end``, ``granularity`` ->
    ``granularity`` (effective/post-decomposition), and populates ``provider``
    and ``project`` from ``config``.
    """
    return build_cost_api_request(
        config, start_date, end_date, granularity
    ).to_body()


# ---------------------------------------------------------------------------
# Pipeline orchestration (task 11.2) — Req 6.1-6.8, 3.1, 3.5, 10.1, 10.2
# ---------------------------------------------------------------------------


def _item_to_dict(item: DataItem) -> dict[str, Any]:
    """Convert a :class:`DataItem` (and its ``DimensionTags``) to a plain dict.

    The success payload must be JSON-serializable, so the frozen dataclasses are
    flattened to nested plain ``dict`` s of the shape::

        {"timestamp": ..., "cost_value": <float>,
         "tags": {"account": ..., "service": ..., "environment": ...}}
    """
    return {
        "timestamp": item.timestamp,
        "cost_value": item.cost_value,
        "tags": {
            "account": item.tags.account,
            "service": item.tags.service,
            "environment": item.tags.environment,
        },
    }


async def aws_cur_cost(
    config: AppConfig,
    http: HttpClient,
    start_date: str,
    end_date: str,
    granularity: str = "DAILY",
) -> dict[str, Any]:
    """Fetch AWS cost data from the Cost_API (the reusable async core).

    Orchestrates the fixed pipeline, short-circuiting each stage to a
    Structured_Error and never letting an exception escape (Req 10.1):

    1. **Validate** ``start_date``/``end_date``/``granularity`` (Req 2.4-2.6,
       3.3, 3.4, 6.2). Any failure returns an ``invalid-parameter`` error and
       **no** Cost_API request is issued (Req 6.2).
    2. **Decompose** granularity: the effective value is ``DAILY`` or ``HOURLY``
       (Req 3.2, 3.5), and is echoed in the success payload.
    3. **Build request** body/URL/headers from config + params (Req 6.1, 6.3).
    4. **POST** via the shared client (Req 6.1, 11.1); transport/status failures
       already arrive as ``unreachable-source`` errors (Req 6.5, 6.7).
    5. **Parse/adapt** the JSON body (Req 6.6); an empty result set becomes
       ``missing-data`` (Req 6.8).
    6. **Numeric coerce/validate** each record's ``cost_value`` (Req 5.1, 5.3-
       5.6, 6.4); the first failure short-circuits with no partial data.
    7. **Tag/normalize** dimensions with the configured account (Req 4.1-4.5,
       12.5).
    8. **Return** ``{"granularity": <effective>, "items": [...]}`` (Req 3.5).

    Args:
        config: Loaded Env_Config; supplies ``provider``/``project`` for the
            request body and ``aws_account_id`` for the ``account`` tag.
        http: The shared :class:`~connector.http_client.HttpClient`.
        start_date: ``YYYY-MM-DD`` inclusive range start.
        end_date: ``YYYY-MM-DD`` inclusive range end.
        granularity: One of ``DAILY``/``MONTHLY``/``HOURLY``/``WEEKLY``;
            defaults to ``DAILY`` (Req 2.2).

    Returns:
        A JSON-serializable success payload ``{"granularity", "items"}`` on
        success, otherwise a Structured_Error ``dict``. Never raises.
    """
    try:
        # --- 1. Validate parameters (Req 6.2 — no request on invalid input) --
        try:
            start = parse_date("start_date", start_date)
            end = parse_date("end_date", end_date)
            validate_range(start, end)
            effective_granularity = normalize_granularity(granularity)
        except ValidationError as exc:
            return exc.structured_error

        # --- 2/3. Build request (start/end echoed as validated ISO strings) --
        url = build_cost_api_url(config)
        headers = build_request_headers()
        body = build_request_body(
            config,
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            granularity=effective_granularity,
        )

        # --- 4. POST via the shared async client (Req 6.1, 11.1) -------------
        result = await http.post_json(url, body, headers)
        if isinstance(result, dict):
            # Mapped transport/status failure (Req 6.5, 6.7, 11.3-11.5).
            return result
        assert isinstance(result, HttpResult)  # narrow for type-checkers

        # --- 5. Parse/adapt the response body (Req 6.6, 6.8) -----------------
        parsed = parse_cost_response(result.json)
        if isinstance(parsed, dict):
            # unexpected-response-shape (Req 6.6) or missing-data (Req 6.8).
            return parsed

        # --- 6/7. Numeric coerce + tag each record (no partial data) ---------
        items: list[dict[str, Any]] = []
        for record in parsed:
            coerced = coerce_cost_value(COST_VALUE_FIELD, record.get("cost_value"))
            if isinstance(coerced, dict):
                # Coercion/range failure short-circuits (Req 5.4-5.6): return
                # the error with no partial data.
                return coerced

            # Carry the validated float onto the record before tagging so the
            # emitted DataItem holds a range-checked float (Req 5.1, 6.4).
            tagged_source = {**record, "cost_value": coerced}
            item = tag_item(tagged_source, config.aws_account_id)
            items.append(_item_to_dict(item))

        # --- 8. Success payload (effective granularity DAILY|HOURLY) ---------
        return {"granularity": effective_granularity, "items": items}

    except TaggingError as exc:
        # A record could not be tagged with all three dimensions (Req 4.6).
        return structured_error(
            f"failed to tag cost data: {exc}",
            ErrorCategory.UNEXPECTED_ERROR,
            subcategory="tagging-failure",
        )
    except Exception:  # noqa: BLE001 — top-level totality guard (Req 10.1)
        # Any unhandled exception maps to unexpected-error with a safe message
        # (no secrets, no exception detail that could echo config/credentials).
        return structured_error(
            "an unexpected error occurred while fetching AWS cost data",
            ErrorCategory.UNEXPECTED_ERROR,
        )


def make_aws_cur_cost(
    config: AppConfig,
    http: HttpClient,
) -> Callable[..., Awaitable[dict[str, Any]]]:
    """Bind ``config`` + ``http`` into the agent-facing async tool callable.

    Returns an ``async def tool(start_date, end_date, granularity="DAILY")``
    coroutine function with exactly the agent-facing signature FastMCP will
    expose (task 12.1). The dependencies are closed over so the server wires
    them once at startup and registers the returned callable with
    ``@mcp.tool()`` / ``mcp.tool()(...)``.
    """

    async def tool(
        start_date: str,
        end_date: str,
        granularity: str = "DAILY",
    ) -> dict[str, Any]:
        """Fetch AWS cost data from the Cost_API for the given range/granularity."""
        return await aws_cur_cost(
            config, http, start_date, end_date, granularity
        )

    return tool
