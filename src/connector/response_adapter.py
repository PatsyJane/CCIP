"""Response parser / adapter (design component 9).

Defensively parses the Cost_API JSON into internal records. Any deviation from
expectations yields an ``unexpected-error`` structured error with subcategory
``unexpected-response-shape`` rather than a crash. Isolates *all* assumptions
about the wire format in this one module, so adapting to the real Cost_API
shape later is a localized change.

Requirements: 6.6, 6.8, 10.4.

Error-return convention
-----------------------
Consistent with :mod:`numeric` and :mod:`dimensions`, the pipeline
"short-circuits to the structured-error factory" rather than raising. Therefore
:func:`parse_cost_response` returns EITHER:

  - a ``list[dict]`` of internal records on success (each record is a plain
    mapping with ``timestamp``, ``cost_value``, ``service``, ``environment``
    keys) that ``dimensions.tag_item`` and ``numeric.coerce_cost_value``
    downstream consume; OR
  - a Structured_Error ``dict`` (built via :func:`errors.structured_error`) on
    failure.

The two are unambiguously distinguished by ``isinstance(result, dict)`` (an
error) vs ``isinstance(result, list)`` (success), matching the convention the
rest of the connector already uses.

Assumed wire format (ISOLATED HERE)
-----------------------------------
The real Cost_API response shape is **currently UNKNOWN**. This module encodes
a single, best-guess assumption so it can be corrected in one place:

  - The 2xx body decodes (or is already decoded) to a JSON **object**
    (``dict``).
  - The cost rows live under one of a small set of candidate container keys
    (``results``, ``data``, ``costs``, ``items``, ``rows``) whose value is a
    JSON **array**. A top-level array body is also accepted directly.
  - Each row is a JSON object carrying at least one recognized *cost* field
    (one of ``cost``, ``cost_value``, ``amount``, ``value``, ``unblended_cost``)
    and, ideally, a *timestamp* field (one of ``timestamp``, ``time``, ``date``,
    ``start``, ``period``, ``bucket``). Optional ``service`` and
    ``environment`` fields are passed through when present.

Anything that does not fit â€” a non-object/non-array body, no recognizable
container, rows that are not objects, or rows with no recognizable cost field â€”
is treated as an unexpected response shape (Req 6.6). A well-formed body that
simply carries zero rows is a ``missing-data`` condition (Req 6.8, 10.4).

The cost/service/environment values are copied through *verbatim*; this module
does NOT coerce numerics or normalize tags â€” that is the responsibility of
:mod:`numeric` and :mod:`dimensions`, which run after parsing.
"""

from __future__ import annotations

import json
from typing import Any, Mapping, Sequence, Union

from .errors import ErrorCategory, structured_error

__all__ = [
    "SUBCATEGORY_UNEXPECTED_SHAPE",
    "CONTAINER_KEYS",
    "COST_FIELDS",
    "TIMESTAMP_FIELDS",
    "SERVICE_FIELDS",
    "ENVIRONMENT_FIELDS",
    "parse_cost_response",
]

#: ``subcategory`` marker attached to every unexpected-response-shape error so
#: callers/tests can distinguish it from other ``unexpected-error`` causes
#: (Req 6.6; design Error Handling table).
SUBCATEGORY_UNEXPECTED_SHAPE = "unexpected-response-shape"

#: Candidate top-level keys under which the array of cost rows may live. Tried
#: in order; the first key present with a list value wins. (WIRE ASSUMPTION.)
#:
#: NOTE: the live Cost_API (PoC) returns cost rows under "daily" (alongside
#: "last_month"/"month_to_date" summary series). "daily" is listed first so a
#: DAILY query ingests the per-day rows; the remaining keys are retained as
#: fallbacks for other/older shapes.
CONTAINER_KEYS: tuple[str, ...] = ("daily", "results", "data", "costs", "items", "rows")

#: Recognized per-row cost fields, in preference order. A row missing all of
#: these is an unexpected shape (Req 6.6). (WIRE ASSUMPTION.)
COST_FIELDS: tuple[str, ...] = (
    "cost",
    "cost_value",
    "amount",
    "value",
    "unblended_cost",
)

#: Recognized per-row timestamp fields, in preference order. (WIRE ASSUMPTION.)
TIMESTAMP_FIELDS: tuple[str, ...] = (
    "timestamp",
    "time",
    "date",
    "start",
    "period",
    "bucket",
)

#: Recognized per-row service fields, in preference order. (WIRE ASSUMPTION.)
SERVICE_FIELDS: tuple[str, ...] = ("service", "service_name", "product")

#: Recognized per-row environment fields, in preference order. (WIRE ASSUMPTION.)
ENVIRONMENT_FIELDS: tuple[str, ...] = ("environment", "env", "stage")

# Return type: success -> list of internal record dicts; failure -> error dict.
ParseResult = Union[list[dict[str, Any]], dict[str, Any]]


def _unexpected_shape(reason: str, **extra: Any) -> dict[str, Any]:
    """Build an ``unexpected-error`` / ``unexpected-response-shape`` error.

    Centralizes the Req 6.6 error contract so every bad-shape exit carries the
    same ``category``/``subcategory`` pair and a descriptive message.
    """
    return structured_error(
        f"unexpected Cost_API response shape: {reason}",
        ErrorCategory.UNEXPECTED_ERROR,
        subcategory=SUBCATEGORY_UNEXPECTED_SHAPE,
        **extra,
    )


def _missing_data(**extra: Any) -> dict[str, Any]:
    """Build a ``missing-data`` error for an empty result set (Req 6.8, 10.4).

    A well-formed but empty response is not a failure of the wire format; it is
    a genuine "no data for the requested range" condition that signals the Agent
    may trigger Cold_Start_Mode (Req 10.4).
    """
    return structured_error(
        "Cost_API returned no cost data for the requested range",
        ErrorCategory.MISSING_DATA,
        **extra,
    )


def _decode_body(body: Any) -> tuple[Any, dict[str, Any] | None]:
    """Return ``(decoded, None)`` or ``(None, error)`` for a raw response body.

    Accepts a body that is already a decoded Python object (``dict``/``list`` â€”
    the common case once the HTTP layer has called ``response.json()``), or a
    raw ``str``/``bytes`` payload which is decoded here as JSON. A non-JSON
    string/bytes payload is an unexpected shape (Req 6.6).
    """
    if isinstance(body, (str, bytes, bytearray)):
        try:
            return json.loads(body), None
        except (ValueError, UnicodeDecodeError) as exc:
            return None, _unexpected_shape(
                f"response body is not valid JSON ({exc})"
            )
    return body, None


def _first_present(row: Mapping[str, Any], keys: Sequence[str]) -> tuple[bool, Any]:
    """Return ``(found, value)`` for the first of ``keys`` present in ``row``.

    "Present" means the key exists; its value may still be ``None`` (left for
    downstream numeric/dimension normalization to judge). Returns
    ``(False, None)`` when none of the keys are present.
    """
    for key in keys:
        if key in row:
            return True, row[key]
    return False, None


def _extract_rows(decoded: Any) -> tuple[list[Any] | None, dict[str, Any] | None]:
    """Locate the array of cost rows within a decoded body.

    Accepts either a top-level array, or an object carrying the array under one
    of :data:`CONTAINER_KEYS`. Returns ``(rows, None)`` on success or
    ``(None, error)`` when the body is neither a recognizable object nor an
    array (Req 6.6).
    """
    if isinstance(decoded, list):
        return list(decoded), None

    if isinstance(decoded, Mapping):
        for key in CONTAINER_KEYS:
            if key in decoded:
                container = decoded[key]
                if isinstance(container, list):
                    return list(container), None
                return None, _unexpected_shape(
                    f"container field '{key}' is not a list "
                    f"(got {type(container).__name__})"
                )
        return None, _unexpected_shape(
            "no recognized cost container field present; expected one of "
            f"{list(CONTAINER_KEYS)}"
        )

    return None, _unexpected_shape(
        "top-level body is neither a JSON object nor an array "
        f"(got {type(decoded).__name__})"
    )


def parse_cost_response(body: Any) -> ParseResult:
    """Defensively parse a Cost_API 2xx response into internal cost records.

    Args:
        body: The Cost_API response body, either already decoded to a Python
            object (``dict``/``list``) or a raw ``str``/``bytes`` JSON payload.

    Returns:
        On success, a ``list`` of internal record ``dict`` s â€” each with
        ``timestamp``, ``cost_value``, ``service``, and ``environment`` keys â€”
        ready for :func:`dimensions.tag_item` and
        :func:`numeric.coerce_cost_value`. On failure, a Structured_Error
        ``dict``:

          - non-JSON body, unrecognized container, non-object rows, or rows
            missing a recognized cost field -> ``unexpected-error`` with
            ``subcategory: "unexpected-response-shape"`` (Req 6.6);
          - a well-formed body with zero rows -> ``missing-data`` (Req 6.8,
            10.4).

    This function never raises for a malformed body; every deviation is mapped
    to a Structured_Error, upholding the "never crash" guarantee.
    """
    decoded, error = _decode_body(body)
    if error is not None:
        return error

    rows, error = _extract_rows(decoded)
    if error is not None:
        return error

    # Empty result set is a genuine missing-data condition, not a bad shape.
    if not rows:
        return _missing_data()

    records: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if not isinstance(row, Mapping):
            return _unexpected_shape(
                f"row {index} is not a JSON object (got {type(row).__name__})",
                row_index=index,
            )

        has_cost, cost_value = _first_present(row, COST_FIELDS)
        if not has_cost:
            return _unexpected_shape(
                f"row {index} is missing an expected cost field; expected one "
                f"of {list(COST_FIELDS)}",
                row_index=index,
            )

        _, timestamp = _first_present(row, TIMESTAMP_FIELDS)
        _, service = _first_present(row, SERVICE_FIELDS)
        _, environment = _first_present(row, ENVIRONMENT_FIELDS)

        records.append(
            {
                "timestamp": timestamp,
                "cost_value": cost_value,
                "service": service,
                "environment": environment,
            }
        )

    return records
