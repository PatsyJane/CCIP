"""Structured-error factory (design component 8).

Single source of the ``category`` enum and the error contract. Builds
``{"error", "category", ...extra}`` objects returned by Ingestion_Tools in
place of raising/crashing.

Requirements: 10.1, 10.3, 10.4, 10.5.
"""

from __future__ import annotations

from enum import Enum
from typing import Any


class ErrorCategory(str, Enum):
    """The bounded set of Structured_Error categories (Req 10.3).

    Exactly these four values are permitted. ``missing-data`` signals the Agent
    may trigger Cold_Start_Mode (Req 10.4); ``unreachable-source`` explicitly
    does not (Req 10.5).
    """

    MISSING_DATA = "missing-data"
    UNREACHABLE_SOURCE = "unreachable-source"
    INVALID_PARAMETER = "invalid-parameter"
    UNEXPECTED_ERROR = "unexpected-error"


def structured_error(message: str, category: ErrorCategory, **extra: Any) -> dict[str, Any]:
    """Build a Structured_Error object.

    Returns a JSON-serializable ``dict`` with a human-readable ``error`` message
    (Req 10.1) and a machine-readable ``category`` drawn from
    :class:`ErrorCategory` (Req 10.3). Any additional context (e.g. ``field``,
    ``value``, ``status_code``, ``source``, ``accepted``, ``subcategory``) is
    merged in via ``extra``.
    """
    return {"error": message, "category": category.value, **extra}
