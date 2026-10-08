"""Validation utilities (design component 5).

Date parsing, range checks, and granularity validation/decomposition:
    - ``parse_date(name, value)`` -> date (Req 2.4)
    - ``validate_range(start, end)`` -> None (Req 2.5, 6.2)
    - ``normalize_granularity(value)`` -> str (Req 2.6, 3.2, 3.3, 3.4, 3.5)

Error-return convention
-----------------------
These utilities return their successful result directly (a ``date`` for
``parse_date``, ``None`` for ``validate_range``, the effective granularity
string for ``normalize_granularity``) so the happy path stays clean and typed.

On any invalid input they raise :class:`ValidationError`, which carries a
fully-formed Structured_Error dict (built by
:func:`connector.errors.structured_error` with category ``invalid-parameter``)
on its ``.structured_error`` attribute. The orchestration pipeline
(``tools/aws_cur_cost.py``) is expected to wrap calls to these utilities in a
``try/except ValidationError`` and return ``exc.structured_error`` to the
Agent, short-circuiting before any Cost_API request is issued (Req 2.4, 2.5,
2.6, 6.2). This keeps the single-source error contract in ``errors.py`` while
letting callers compose validation naturally.

Requirements: 2.4, 2.5, 2.6, 3.2, 3.3, 3.4, 3.5, 6.2.
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Any

from .errors import ErrorCategory, structured_error

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

#: Expected date wire format for ``start_date`` / ``end_date`` (Req 2.1).
_DATE_FORMAT = "%Y-%m-%d"

#: Strict ``YYYY-MM-DD`` shape: 4-digit year, zero-padded 2-digit month/day.
#: ``datetime.strptime`` alone accepts unpadded forms (e.g. ``2023-6-1``), so we
#: gate on this pattern first to enforce the exact wire format (Req 2.1, 2.4).
_DATE_SHAPE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

#: Maximum inclusive range span, in days, between start and end (Req 6.1/6.2).
_MAX_RANGE_DAYS = 366

#: Granularity values accepted as *requests* by ``normalize_granularity``.
#: Per design Property 4 / Req 3.3, this set includes ``WEEKLY`` in addition to
#: the tool-parameter set of {DAILY, MONTHLY, HOURLY} (Req 2.6).
ACCEPTED_GRANULARITIES: tuple[str, ...] = ("DAILY", "MONTHLY", "HOURLY", "WEEKLY")

#: Granularities coarser than DAILY that decompose to ``DAILY`` (Req 3.2).
_COARSER_THAN_DAILY: frozenset[str] = frozenset({"WEEKLY", "MONTHLY"})

#: The effective granularity is always constrained to one of these (Req 3.5).
_EFFECTIVE_GRANULARITIES: frozenset[str] = frozenset({"DAILY", "HOURLY"})


class ValidationError(Exception):
    """Raised when a validation utility rejects its input.

    Carries the machine-readable Structured_Error dict (category
    ``invalid-parameter``) on :attr:`structured_error` so the caller can return
    it verbatim to the Agent.
    """

    def __init__(self, structured: dict[str, Any]) -> None:
        super().__init__(structured.get("error", "validation error"))
        self.structured_error: dict[str, Any] = structured


def _invalid_parameter(message: str, **extra: Any) -> ValidationError:
    """Build a :class:`ValidationError` wrapping an ``invalid-parameter`` error."""
    return ValidationError(
        structured_error(message, ErrorCategory.INVALID_PARAMETER, **extra)
    )


# ---------------------------------------------------------------------------
# Date parsing (Req 2.4)
# ---------------------------------------------------------------------------


def parse_date(name: str, value: Any) -> date:
    """Parse and validate a ``YYYY-MM-DD`` date parameter.

    Args:
        name: The parameter name (e.g. ``"start_date"``), echoed in errors so
            the Agent can tell which date was invalid.
        value: The raw parameter value supplied by the Agent.

    Returns:
        The parsed :class:`datetime.date`.

    Raises:
        ValidationError: If ``value`` is absent (``None``), not a string, does
            not match ``YYYY-MM-DD``, or is not a real calendar date. The
            wrapped Structured_Error names ``name`` and describes the reason
            (Req 2.4).
    """
    if value is None:
        raise _invalid_parameter(
            f"{name} is required and must be a YYYY-MM-DD date string",
            field=name,
            reason="missing",
        )

    if not isinstance(value, str):
        raise _invalid_parameter(
            f"{name} must be a string in YYYY-MM-DD format, "
            f"got {type(value).__name__}",
            field=name,
            value=repr(value),
            reason="not-a-string",
        )

    # Enforce the exact zero-padded shape before parsing, then let strptime
    # validate the calendar date (rejects e.g. 2023-02-30, 2023-13-01).
    if not _DATE_SHAPE.match(value):
        raise _invalid_parameter(
            f"{name} must be in YYYY-MM-DD format, got '{value}'",
            field=name,
            value=value,
            reason="bad-format",
        )

    try:
        parsed = datetime.strptime(value, _DATE_FORMAT).date()
    except ValueError:
        raise _invalid_parameter(
            f"{name} must be a valid calendar date in YYYY-MM-DD format, "
            f"got '{value}'",
            field=name,
            value=value,
            reason="not-a-calendar-date",
        ) from None

    return parsed


# ---------------------------------------------------------------------------
# Range validation (Req 2.5, 6.2)
# ---------------------------------------------------------------------------


def validate_range(start: date, end: date) -> None:
    """Validate that ``start``..``end`` is a well-formed query range.

    Args:
        start: The parsed start date (inclusive).
        end: The parsed end date (inclusive).

    Raises:
        ValidationError: If ``end`` precedes ``start`` (the error echoes both
            dates, Req 2.5), or if the range span exceeds 366 days (Req 6.2).
    """
    if end < start:
        raise _invalid_parameter(
            f"end_date ({end.isoformat()}) precedes start_date "
            f"({start.isoformat()})",
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            reason="inverted-range",
        )

    span_days = (end - start).days
    if span_days > _MAX_RANGE_DAYS:
        raise _invalid_parameter(
            f"date range exceeds the maximum of {_MAX_RANGE_DAYS} days: "
            f"start_date ({start.isoformat()}) to end_date ({end.isoformat()}) "
            f"spans {span_days} days",
            start_date=start.isoformat(),
            end_date=end.isoformat(),
            span_days=span_days,
            max_days=_MAX_RANGE_DAYS,
            reason="range-too-large",
        )


# ---------------------------------------------------------------------------
# Granularity validation & decomposition (Req 2.6, 3.2, 3.3, 3.4, 3.5)
# ---------------------------------------------------------------------------


def normalize_granularity(value: Any) -> str:
    """Validate a granularity request and return the effective granularity.

    Accepted request values are ``DAILY``, ``MONTHLY``, ``HOURLY``, ``WEEKLY``
    (Req 3.3). Coarser-than-daily requests (``WEEKLY``, ``MONTHLY``) decompose
    to ``DAILY`` (Req 3.2). The returned effective granularity is always
    ``DAILY`` or ``HOURLY`` (Req 3.5).

    Args:
        value: The raw ``granularity`` value supplied by the Agent.

    Returns:
        The effective granularity, one of ``"DAILY"`` or ``"HOURLY"``.

    Raises:
        ValidationError: If ``value`` is missing/null/empty/whitespace-only
            (Req 3.4), not a string, or not one of the accepted values. The
            wrapped Structured_Error lists the ``accepted`` values (Req 2.6,
            3.3).
    """
    if value is None:
        raise _invalid_parameter(
            "granularity is required",
            accepted=list(ACCEPTED_GRANULARITIES),
            reason="missing",
        )

    if not isinstance(value, str):
        raise _invalid_parameter(
            f"granularity must be a string, got {type(value).__name__}",
            value=repr(value),
            accepted=list(ACCEPTED_GRANULARITIES),
            reason="not-a-string",
        )

    if value.strip() == "":
        raise _invalid_parameter(
            "granularity must not be empty",
            value=value,
            accepted=list(ACCEPTED_GRANULARITIES),
            reason="empty",
        )

    # Case-insensitive match, normalized to canonical upper-case form.
    canonical = value.strip().upper()
    if canonical not in ACCEPTED_GRANULARITIES:
        raise _invalid_parameter(
            f"unsupported granularity '{value}'; accepted values are "
            f"{', '.join(ACCEPTED_GRANULARITIES)}",
            value=value,
            accepted=list(ACCEPTED_GRANULARITIES),
            reason="unsupported",
        )

    # Decompose coarser-than-daily to DAILY (Req 3.2); HOURLY passes through.
    effective = "DAILY" if canonical in _COARSER_THAN_DAILY else canonical

    # Invariant: effective granularity is constrained to DAILY/HOURLY (Req 3.5).
    assert effective in _EFFECTIVE_GRANULARITIES, effective
    return effective
