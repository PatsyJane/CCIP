"""HTTP client wrapper (design component 4).

Owns the single shared ``httpx.AsyncClient`` reused across concurrent tool
invocations (never per-request). Maps transport/status failures to structured
errors.

Requirements: 6.5, 6.7, 11.1, 11.2, 11.3, 11.4, 11.5.

Error-return convention
-----------------------
Consistent with ``numeric.py`` and ``dimensions.py``, the wrapper does not raise
for mapped failures. ``post_json`` returns EITHER an :class:`HttpResult` on a
2xx response OR a Structured_Error ``dict`` (built via
:func:`errors.structured_error`) on a mapped failure. Callers distinguish the
two with ``isinstance(result, dict)`` (or ``isinstance(result, HttpResult)``)
and propagate the error dict unchanged. This keeps the "never crash / Connector
stays available" guarantee (Req 10.1, 10.2) without a bespoke exception layer.

The mapped failures are:

- **non-2xx status** -> ``unreachable-source`` with ``status_code`` + ``source``
  (Req 6.5, 11.3). The response body is discarded so no partial data leaks.
- **timeout (30s)** -> ``unreachable-source`` with ``source`` and the elapsed
  ``timeout`` duration (Req 6.7, 11.4).
- **connection / network error** -> ``unreachable-source`` with ``source``
  (Req 6.7, 11.5).

Any ``httpx`` error that is neither a timeout nor a well-formed response (for
example a malformed-response transport error) is also mapped to
``unreachable-source`` so no exception escapes the wrapper.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional, Union

import httpx

from .errors import ErrorCategory, structured_error

__all__ = [
    "DEFAULT_TIMEOUT_SECONDS",
    "SOURCE_COST_API",
    "HttpResult",
    "HttpClient",
]

#: The configured request timeout in seconds (Req 11.4).
DEFAULT_TIMEOUT_SECONDS: float = 30.0

#: Source identifier attached to transport/status errors (Req 11.3-11.5).
SOURCE_COST_API: str = "cost_api"


@dataclass(frozen=True)
class HttpResult:
    """Outcome of a successful (2xx) HTTP call (design Data Models).

    - ``ok`` is ``True`` when the status code is within 200-299.
    - ``status`` is the received HTTP status code.
    - ``json`` is the parsed JSON body, or ``None`` when the body could not be
      parsed as JSON. Distinguishing a 2xx-with-unparseable-body from a proper
      JSON body is left to the response adapter (Req 6.6), which owns all
      wire-format assumptions; the wrapper only reports the transport outcome.
    """

    ok: bool
    status: int
    json: Optional[Any]


class HttpClient:
    """Owns the one shared ``httpx.AsyncClient`` (Req 11.1, 11.2).

    A single client is constructed here and reused for every ``post_json`` call;
    nothing instantiates a client per request. Because every call is awaited on
    that shared client, concurrent invocations do not block the event loop
    (Req 11.2).
    """

    def __init__(
        self,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        *,
        client: Optional[httpx.AsyncClient] = None,
    ) -> None:
        """Create (or adopt) the shared async client.

        ``timeout`` defaults to 30 seconds (Req 11.4). A pre-built ``client`` may
        be injected (e.g. one wired to an ``httpx.MockTransport`` in tests); when
        omitted, a client is constructed once with the given timeout.
        """
        self._timeout = timeout
        self._client = client if client is not None else httpx.AsyncClient(timeout=timeout)

    @property
    def client(self) -> httpx.AsyncClient:
        """The shared underlying client instance (reused across invocations)."""
        return self._client

    async def post_json(
        self,
        url: str,
        body: Mapping[str, Any],
        headers: Mapping[str, str],
    ) -> Union[HttpResult, dict[str, Any]]:
        """POST ``body`` as JSON to ``url`` using the shared async client.

        Returns an :class:`HttpResult` on a 2xx response, otherwise a
        Structured_Error ``dict`` (category ``unreachable-source``) describing the
        mapped failure. Never raises for transport/status failures.
        """
        try:
            response = await self._client.post(url, json=dict(body), headers=dict(headers))
        except httpx.TimeoutException:
            # Req 6.7, 11.4 — request exceeded the configured timeout.
            return structured_error(
                f"request to {SOURCE_COST_API} timed out after "
                f"{self._timeout:g}s",
                ErrorCategory.UNREACHABLE_SOURCE,
                source=SOURCE_COST_API,
                timeout=self._timeout,
            )
        except httpx.HTTPError as exc:
            # Req 6.7, 11.5 — connection/network error before a usable response.
            return structured_error(
                f"could not reach {SOURCE_COST_API}: connection failure",
                ErrorCategory.UNREACHABLE_SOURCE,
                source=SOURCE_COST_API,
                reason=type(exc).__name__,
            )

        status = response.status_code

        # Req 6.5, 11.3 — non-2xx status. Discard the body: no partial data.
        if not 200 <= status <= 299:
            return structured_error(
                f"{SOURCE_COST_API} returned status {status}",
                ErrorCategory.UNREACHABLE_SOURCE,
                status_code=status,
                source=SOURCE_COST_API,
            )

        # 2xx — attempt to parse the JSON body. A body that is not valid JSON is
        # reported as ``json=None`` so the response adapter (Req 6.6) can raise
        # the unexpected-response-shape error; the wrapper does not judge shape.
        try:
            parsed: Optional[Any] = response.json()
        except (ValueError, UnicodeDecodeError):
            parsed = None

        return HttpResult(ok=True, status=status, json=parsed)

    async def aclose(self) -> None:
        """Close the shared client. Called on Connector shutdown."""
        await self._client.aclose()
