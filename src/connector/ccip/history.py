"""History counting + data-mode classification (design component ``history.py``).

Pure helpers that count an account's available complete days from the fetched
history series and classify the Data_Mode on the fixed 30/60/90 boundaries.

Requirements: 3.1, 3.2, 5.4.
"""

from __future__ import annotations

from typing import Any

from .models import DataMode, classify_data_mode, is_above_cold_start

__all__ = [
    "count_available_days",
    "classify_data_mode",
    "is_above_cold_start",
    "DataMode",
]


def count_available_days(account_history: list[dict[str, Any]]) -> int:
    """Count distinct calendar days present in the account history series (Req 3.1).

    ``account_history`` is a list of ``{"day": "YYYY-MM-DD", "cost": float}`` rows.
    Only rows with a usable ``day`` string contribute; duplicate days count once.
    """
    days: set[str] = set()
    for row in account_history:
        if not isinstance(row, dict):
            continue
        day = row.get("day")
        if isinstance(day, str) and day:
            days.add(day[:10])
    return len(days)
