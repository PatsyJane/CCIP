"""Persistent JSON metadata store (design component ``metadata_store.py``).

A per-account JSON file tracks, across runs:

- ``known_active_services`` — services seen on any prior run (list[str]);
- ``seasonal`` — per-service seasonal multipliers / moving-average summary;
- ``last_anomaly`` — the last recorded anomaly ``{date, contributors, context}``;
- ``non_anomaly_days`` — account days considered non-anomalous
  (``{day: total}``), used as the Cold_Start / drill-down baseline;
- ``account_history`` — the persisted Rolling_History_Window of account totals
  (``[{day, cost}]`` ascending), the source of truth for data-mode counting and
  STL (increment 2, Req 15.1);
- ``service_history`` — the per-service daily series (``{service: [{day, cost}]}``)
  of the same window (Req 15.1);
- ``last_full_fetch_date`` — the Target_Date of the last FULL pull, the clock for
  the periodic reconciliation (Req 16.4).

As of increment 2 the store is **partitioned per account** (Req 14): each account
gets its own file at
``<metadata_dir>/ccip_metadata_<sanitized_account_name>_<sanitized_account_id>.json``
(:func:`account_metadata_path`), so accounts never collide. Loading an absent file
yields a fresh default store (Req 5.1); a store written by a pre-increment-2 build
is merged over the current defaults so the new keys are always present. Writes are
atomic — the payload is dumped to a temp file and ``os.replace``-d onto the target,
creating the parent directory first — so a crash mid-write cannot corrupt the store
(Req 5.8, 10.5, 14.5). Unreadable / invalid JSON and failed writes surface as
``unexpected-error`` Structured_Errors (Req 5.7, 10.5); nothing raises across the
boundary.

Requirements: 5.1, 5.7, 5.8, 10.5, 14, 15.
"""

from __future__ import annotations

import copy
import json
import os
import re
from typing import Any, Optional, Union

from connector.errors import ErrorCategory, structured_error

__all__ = [
    "DEFAULT_METADATA",
    "default_metadata",
    "load_metadata",
    "save_metadata",
    "sanitize_path_segment",
    "account_metadata_path",
    "merge_history",
    "merge_service_history",
]

#: Canonical empty store. Use :func:`default_metadata` for an independent copy.
DEFAULT_METADATA: dict[str, Any] = {
    "known_active_services": [],
    "seasonal": {},
    "last_anomaly": None,
    "non_anomaly_days": {},
    # --- increment 2: persisted Rolling_History_Window (Req 15.1) ---
    "account_history": [],
    "service_history": {},
    "last_full_fetch_date": None,
}

#: Characters OUTSIDE the filename-safe set are replaced with ``_`` (Req 14.2).
_UNSAFE_SEGMENT = re.compile(r"[^A-Za-z0-9._-]+")
#: Runs of underscores are collapsed to a single one so the name stays readable.
_UNDERSCORE_RUN = re.compile(r"_{2,}")


def default_metadata() -> dict[str, Any]:
    """Return a fresh deep copy of the empty metadata store (never shared)."""
    return copy.deepcopy(DEFAULT_METADATA)


def sanitize_path_segment(value: str) -> str:
    """Make ``value`` safe as ONE path segment on Windows and Linux (Req 14.2).

    Replaces every character outside ``[A-Za-z0-9._-]`` (spaces, path separators,
    ``:``, drive letters' colon, control chars, etc.) with ``_``, collapses runs
    of ``_`` to one, and trims leading/trailing ``._-``. Because separators are
    stripped and the result is trimmed of dots, the output can never be ``.`` /
    ``..`` or contain a ``/`` or ``\\``, so it cannot cause path traversal. An
    all-unsafe or blank input returns ``""`` (callers supply a fallback).
    """
    if not isinstance(value, str):
        value = "" if value is None else str(value)
    cleaned = _UNSAFE_SEGMENT.sub("_", value)
    cleaned = _UNDERSCORE_RUN.sub("_", cleaned).strip("._-")
    return cleaned


def account_metadata_path(config: Any, account_id: str, account_name: str = "") -> str:
    """Resolve the per-account Metadata_Store path (Req 14.1, 14.2, 14.3).

    Returns ``<metadata_dir>/ccip_metadata_<san_name>_<san_id>.json`` when the
    account name is non-blank, else ``<metadata_dir>/ccip_metadata_<san_id>.json``
    (the id segment is ALWAYS present). Both segments are sanitized via
    :func:`sanitize_path_segment`; an all-unsafe id falls back to the stable
    placeholder ``account`` so a filename is always producible. ``metadata_dir``
    is read from ``config`` (default ``"metadata"``) so accounts never collide.
    """
    metadata_dir = getattr(config, "metadata_dir", "metadata") or "metadata"
    san_id = sanitize_path_segment(account_id) or "account"
    san_name = sanitize_path_segment(account_name)
    stem = f"ccip_metadata_{san_name}_{san_id}" if san_name else f"ccip_metadata_{san_id}"
    return os.path.join(metadata_dir, f"{stem}.json")


def _trim_series(series: list[dict[str, Any]], history_days: int) -> list[dict[str, Any]]:
    """Sort day rows ascending by ``day`` and keep the most-recent ``history_days``."""
    ordered = sorted(
        (row for row in series if isinstance(row, dict) and row.get("day")),
        key=lambda row: str(row["day"])[:10],
    )
    if history_days is not None and history_days > 0 and len(ordered) > history_days:
        ordered = ordered[-history_days:]
    return ordered


def merge_history(
    persisted: list[dict[str, Any]],
    incoming: list[dict[str, Any]],
    history_days: int,
) -> list[dict[str, Any]]:
    """Merge account-total day rows, OVERWRITING by day, trimmed + ascending (Req 15.2, 15.3).

    The merge is idempotent per day: a day present in both ``persisted`` and
    ``incoming`` takes the INCOMING cost (never summed), so re-ingesting a day or an
    overlapping trailing window cannot double-count. The result is sorted ascending
    by ``day`` and trimmed to the most-recent ``history_days`` days.
    """
    by_day: dict[str, dict[str, Any]] = {}
    for row in persisted or []:
        if isinstance(row, dict) and row.get("day"):
            day = str(row["day"])[:10]
            by_day[day] = {"day": day, "cost": row.get("cost", 0.0)}
    for row in incoming or []:  # incoming overwrites persisted for the same day
        if isinstance(row, dict) and row.get("day"):
            day = str(row["day"])[:10]
            by_day[day] = {"day": day, "cost": row.get("cost", 0.0)}
    return _trim_series(list(by_day.values()), history_days)


def merge_service_history(
    persisted: dict[str, list[dict[str, Any]]],
    incoming: dict[str, list[dict[str, Any]]],
    history_days: int,
) -> dict[str, list[dict[str, Any]]]:
    """Per-service day-keyed OVERWRITE merge + trim (Req 15.2, 15.3).

    Applies :func:`merge_history` per service across the union of service keys, so
    each service's series is independently idempotent, ascending, and trimmed.
    """
    persisted = persisted or {}
    incoming = incoming or {}
    merged: dict[str, list[dict[str, Any]]] = {}
    for service in set(persisted) | set(incoming):
        merged[service] = merge_history(
            persisted.get(service, []), incoming.get(service, []), history_days
        )
    return merged


def load_metadata(path: str) -> Union[dict[str, Any], dict]:
    """Load the Metadata_Store JSON, or a fresh default when the file is absent.

    An absent file returns :func:`default_metadata` (Req 5.1). An unreadable file
    or invalid JSON returns an ``unexpected-error`` Structured_Error naming the
    file (Req 5.7). The loaded store is merged over the default so missing keys
    are always present.
    """
    if not os.path.exists(path):
        return default_metadata()
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, ValueError, UnicodeDecodeError) as exc:
        return structured_error(
            f"could not read metadata store {path}: {exc}",
            ErrorCategory.UNEXPECTED_ERROR,
            source="metadata-store",
            path=path,
        )
    if not isinstance(data, dict):
        return structured_error(
            f"metadata store {path} does not contain a JSON object",
            ErrorCategory.UNEXPECTED_ERROR,
            source="metadata-store",
            path=path,
        )
    # Back-compat: the Rolling_History_Window key was renamed
    # ``history_series`` -> ``account_history``. Migrate a store written by an
    # older build so its persisted history is preserved rather than silently
    # re-seeded empty. Only migrate when the new key is absent, so a store that
    # already uses the new key is never clobbered (Req 15.1).
    if "history_series" in data and "account_history" not in data:
        data["account_history"] = data["history_series"]
    data.pop("history_series", None)
    merged = default_metadata()
    merged.update(data)
    return merged


def save_metadata(path: str, metadata: dict[str, Any]) -> Optional[dict]:
    """Atomically persist the Metadata_Store (Req 5.8, 10.5).

    Writes the JSON payload to ``<path>.tmp`` and ``os.replace``-s it onto
    ``path`` so a crash mid-write cannot corrupt the store; the prior file is
    left intact on failure. Returns ``None`` on success or an ``unexpected-error``
    Structured_Error on failure. Never raises.
    """
    tmp_path = f"{path}.tmp"
    try:
        directory = os.path.dirname(os.path.abspath(path))
        os.makedirs(directory, exist_ok=True)
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(metadata, handle, indent=2, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_path, path)
    except (OSError, TypeError, ValueError) as exc:
        # Clean up a partial temp file; never leave the store partially written.
        try:
            if os.path.exists(tmp_path):
                os.remove(tmp_path)
        except OSError:
            pass
        return structured_error(
            f"could not write metadata store {path}: {exc}",
            ErrorCategory.UNEXPECTED_ERROR,
            source="metadata-store",
            path=path,
        )
    return None
