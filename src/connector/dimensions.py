"""Dimension tagging + normalization (design component 6).

Attaches ``account``/``service``/``environment`` tags to every Data_Item,
applying the ``"UNKNOWN"`` fallback for missing/blank values. The ``account``
tag is fixed from the Env_Config, independent of any account value present in
the source record.

Requirements: 4.1, 4.2, 4.3, 4.4, 4.5, 4.6, 6.4, 12.5.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .errors import ErrorCategory, structured_error

__all__ = [
    "UNKNOWN",
    "DimensionTags",
    "DataItem",
    "TaggingError",
    "tag_item",
    "tag_items",
]

#: Sentinel value used when a source record provides no usable ``service`` or
#: ``environment`` (missing, empty, or whitespace-only) — Req 4.3, 4.4.
UNKNOWN = "UNKNOWN"


@dataclass(frozen=True)
class DimensionTags:
    """The metadata attached to every Data_Item (design Data Models).

    All three tags are always present and each value is non-empty (Req 4.5):

    - ``account`` is fixed from the Env_Config account id (Req 4.2, 12.5).
    - ``service`` is the resolved source value or ``"UNKNOWN"`` (Req 4.3, 4.4).
    - ``environment`` is the resolved source value or ``"UNKNOWN"``
      (Req 4.3, 4.4).
    """

    account: str
    service: str
    environment: str


@dataclass(frozen=True)
class DataItem:
    """A single time-bucketed cost record (design Data Models).

    - ``timestamp`` is the bucket start, aligned to the effective granularity
      (Req 3.1).
    - ``cost_value`` is a validated float in ``0.00``..``999,999,999.99``
      (Req 5.1).
    - ``tags`` carries all three dimension tags, present and non-empty
      (Req 4.5).
    """

    timestamp: str
    cost_value: float
    tags: DimensionTags


class TaggingError(Exception):
    """Raised when a source record cannot be tagged with all three dimensions.

    Signals that :func:`tag_item` was unable to guarantee the Req 4.5 invariant
    (all three tags present and non-empty) for a given item — for example when
    the configured ``account_id`` is itself missing or blank. The orchestration
    layer catches this to build a tagging-failure Structured_Error while
    preserving correctly-tagged items in the response (Req 4.6).
    """


def _resolve_tag(value: Any) -> str:
    """Resolve a source ``service``/``environment`` value to a tag string.

    Applies the normalization rules of Req 4.3/4.4: a missing (``None``),
    empty, or whitespace-only value becomes :data:`UNKNOWN`; any other value is
    coerced to ``str`` and surrounding whitespace stripped, preserving the
    non-blank content.

    Args:
        value: The raw source value (possibly ``None`` or absent).

    Returns:
        The resolved, non-empty tag string.
    """
    if value is None:
        return UNKNOWN
    text = value if isinstance(value, str) else str(value)
    stripped = text.strip()
    return stripped if stripped else UNKNOWN


def tag_item(raw: Mapping[str, Any], account_id: str) -> DataItem:
    """Attach ``account``/``service``/``environment`` tags to one source record.

    The ``account`` tag is always the configured ``account_id`` regardless of
    any account value present in ``raw`` (Req 4.2, 12.5). Missing, empty, or
    whitespace-only ``service``/``environment`` values normalize to
    :data:`UNKNOWN`; non-blank values are preserved (Req 4.3, 4.4). The
    returned Data_Item is guaranteed to carry all three tags, each non-empty
    (Req 4.1, 4.5).

    The ``timestamp`` and ``cost_value`` are carried through from ``raw`` as
    provided; numeric coercion and range validation of ``cost_value`` are the
    responsibility of the numeric-normalization component and are expected to
    have run before tagging.

    Args:
        raw: A source record mapping. Recognized keys are ``timestamp``,
            ``cost_value`` (or ``cost``), ``service``, and ``environment``.
        account_id: The configured Env_Config account identifier.

    Returns:
        A fully tagged :class:`DataItem`.

    Raises:
        TaggingError: If the item cannot be tagged with all three non-empty
            dimensions — i.e. ``raw`` is not a mapping or ``account_id`` is
            missing, empty, or whitespace-only (Req 4.6).
    """
    if not isinstance(raw, Mapping):
        raise TaggingError(
            f"cannot tag item: expected a source record mapping, got "
            f"{type(raw).__name__}"
        )
    if not isinstance(account_id, str) or not account_id.strip():
        raise TaggingError(
            "cannot tag item: configured account identifier is missing or blank"
        )

    tags = DimensionTags(
        account=account_id,
        service=_resolve_tag(raw.get("service")),
        environment=_resolve_tag(raw.get("environment")),
    )

    cost_value = raw.get("cost_value", raw.get("cost"))
    return DataItem(
        timestamp=raw.get("timestamp"),
        cost_value=cost_value,
        tags=tags,
    )


def tag_items(
    raws: Iterable[Mapping[str, Any]],
    account_id: str,
) -> tuple[list[DataItem], dict[str, Any] | None]:
    """Tag a batch of source records, preserving correctly-tagged items.

    Applies :func:`tag_item` to each record. If any record cannot be tagged, the
    correctly-tagged items are still returned alongside a tagging-failure
    Structured_Error describing the failure, rather than discarding good data
    (Req 4.6). The error is categorized ``unexpected-error`` per the design
    Error Handling table.

    Args:
        raws: An iterable of source record mappings.
        account_id: The configured Env_Config account identifier.

    Returns:
        A ``(items, error)`` tuple where ``items`` is the list of successfully
        tagged Data_Items and ``error`` is either ``None`` (all items tagged)
        or a Structured_Error dict describing the tagging failure.
    """
    items: list[DataItem] = []
    failures: list[str] = []
    for index, raw in enumerate(raws):
        try:
            items.append(tag_item(raw, account_id))
        except TaggingError as exc:
            failures.append(f"item {index}: {exc}")

    if not failures:
        return items, None

    error = structured_error(
        "one or more items could not be tagged with all three dimensions; "
        "correctly-tagged items are preserved in the response",
        ErrorCategory.UNEXPECTED_ERROR,
        subcategory="tagging-failure",
        failures=failures,
        tagged_count=len(items),
    )
    return items, error
