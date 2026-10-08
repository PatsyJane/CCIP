"""Persistent JSON metadata store (design component ``metadata_store.py``).

A single JSON file tracks, across runs:

- ``known_active_services`` — services seen on any prior run (list[str]);
- ``seasonal`` — per-service seasonal multipliers / moving-average summary;
- ``last_anomaly`` — the last recorded anomaly ``{date, contributors, context}``;
- ``non_anomaly_days`` — account days considered non-anomalous
  (``{day: total}``), used as the Cold_Start / drill-down baseline.

Loading an absent file yields a fresh default store (Req 5.1). Writes are atomic
— the payload is dumped to a temp file and ``os.replace``-d onto the target — so
a crash mid-write cannot corrupt the store (Req 5.8, 10.5). Unreadable / invalid
JSON and failed writes surface as ``unexpected-error`` Structured_Errors
(Req 5.7, 10.5); nothing raises across the boundary.

Requirements: 5.1, 5.7, 5.8, 10.5.
"""

from __future__ import annotations

import copy
import json
import os
from typing import Any, Optional, Union

from connector.errors import ErrorCategory, structured_error

__all__ = ["DEFAULT_METADATA", "default_metadata", "load_metadata", "save_metadata"]

#: Canonical empty store. Use :func:`default_metadata` for an independent copy.
DEFAULT_METADATA: dict[str, Any] = {
    "known_active_services": [],
    "seasonal": {},
    "last_anomaly": None,
    "non_anomaly_days": {},
}


def default_metadata() -> dict[str, Any]:
    """Return a fresh deep copy of the empty metadata store (never shared)."""
    return copy.deepcopy(DEFAULT_METADATA)


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
