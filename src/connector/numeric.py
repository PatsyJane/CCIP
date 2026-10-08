"""Numeric normalization (design component 7).

Coerces text-encoded numerics to ``float`` and enforces the permitted
Cost_Value range ``0.00``..``999,999,999.99``. Produces structured errors
naming the field (and, where applicable, the value and range) on a missing
value, a coercion failure, or an out-of-range value.

Requirements: 5.1, 5.3, 5.4, 5.5, 5.6, 6.4.

Error-return convention
-----------------------
No custom exception layer exists in the design; every failure path in the tool
pipeline "short-circuits to the structured-error factory" (design: Components &
Flow). Accordingly ``coerce_cost_value`` returns EITHER a ``float`` on success
OR a Structured_Error ``dict`` (built via :func:`errors.structured_error`) on
failure. Callers distinguish the two with ``isinstance(result, dict)`` and
propagate the error dict unchanged. This keeps numeric normalization pure and
free of control-flow exceptions while satisfying the "never crash" guarantee.
"""

from __future__ import annotations

from typing import Any, Union

from .errors import ErrorCategory, structured_error

# Inclusive permitted range for a Cost_Value (Req 5.1, 6.4).
MIN_COST_VALUE: float = 0.00
MAX_COST_VALUE: float = 999_999_999.99

# Human-readable range description reused in the out-of-range error (Req 5.6).
_RANGE_TEXT = "0.00 to 999,999,999.99"


def coerce_cost_value(field: str, value: Any) -> Union[float, dict[str, Any]]:
    """Coerce ``value`` to a range-checked ``float`` for the named ``field``.

    Success returns a ``float`` within the inclusive range
    ``0.00``..``999,999,999.99`` (Req 5.1, 6.4). Text-encoded numerics are
    converted to their float value (Req 5.3).

    Failure returns a Structured_Error ``dict`` (category ``invalid-parameter``):
      - missing / empty / whitespace-only value -> names the field (Req 5.5);
      - unconvertible value -> names the field and the failing value (Req 5.4);
      - out-of-range value -> names the field, the value, and the range (Req 5.6).
    """
    # --- Missing / empty (Req 5.5) ---------------------------------------
    # None, or a string that is empty or whitespace-only, is a missing value.
    if value is None:
        return structured_error(
            f"missing required numeric value for field '{field}'",
            ErrorCategory.INVALID_PARAMETER,
            field=field,
        )
    if isinstance(value, str) and value.strip() == "":
        return structured_error(
            f"missing required numeric value for field '{field}'",
            ErrorCategory.INVALID_PARAMETER,
            field=field,
        )

    # --- Coercion (Req 5.3, 5.4) -----------------------------------------
    coerced = _to_float(value)
    if coerced is None:
        return structured_error(
            f"could not convert value {value!r} for field '{field}' to a number",
            ErrorCategory.INVALID_PARAMETER,
            field=field,
            value=value,
        )

    # --- Range check (Req 5.1, 5.6, 6.4) ---------------------------------
    if coerced < MIN_COST_VALUE or coerced > MAX_COST_VALUE:
        return structured_error(
            f"value {value!r} for field '{field}' is outside the permitted "
            f"range {_RANGE_TEXT}",
            ErrorCategory.INVALID_PARAMETER,
            field=field,
            value=value,
            permitted_range=_RANGE_TEXT,
        )

    return coerced


def _to_float(value: Any) -> Union[float, None]:
    """Best-effort conversion of ``value`` to ``float``.

    Accepts real numbers and text-encoded numerics; returns ``None`` when the
    value cannot be interpreted as a finite number. ``bool`` is rejected because
    a boolean is not a meaningful Cost_Value even though ``bool`` is a subclass
    of ``int``. NaN and infinities are rejected as non-finite.
    """
    # Reject booleans explicitly (bool is a subclass of int).
    if isinstance(value, bool):
        return None

    if isinstance(value, (int, float)):
        f = float(value)
        return f if _is_finite(f) else None

    if isinstance(value, str):
        text = value.strip()
        if text == "":
            return None
        try:
            f = float(text)
        except ValueError:
            return None
        return f if _is_finite(f) else None

    # Any other type (list, dict, object, ...) is unconvertible.
    return None


def _is_finite(f: float) -> bool:
    """Return True only for finite floats (excludes NaN and +/-inf)."""
    # NaN is never equal to itself; inf has infinite magnitude.
    return f == f and f not in (float("inf"), float("-inf"))
