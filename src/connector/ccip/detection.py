"""Account anomaly detection (design component ``detection.py``).

Pure functions implementing Phase 4's two detection methods:

- :func:`detect_cold_start` — Cold_Start mode: compare the Target_Date account
  total against the most recent previous non-anomaly day's total and flag an
  anomaly when the absolute difference exceeds the configured absolute threshold
  (default ``$20``). With no prior day, returns ``(False, None)`` (Req 6.2, 6.6).
- :func:`detect_statistical` — Initial/Beginner/Normal modes: flag an anomaly
  when the STL Target_Date residual exceeds the residual threshold (Req 6.3).

Requirements: 6.2, 6.3, 6.6.
"""

from __future__ import annotations

from typing import Any, Optional

from .stl_model import StlResult

__all__ = ["most_recent_non_anomaly_day", "detect_cold_start", "detect_statistical"]


def most_recent_non_anomaly_day(
    metadata: dict[str, Any], before_day: str
) -> Optional[tuple[str, float]]:
    """Return ``(day, total)`` for the latest non-anomaly day strictly before ``before_day``.

    Reads ``metadata['non_anomaly_days']`` (a ``{day: total}`` map). Returns
    ``None`` when no earlier non-anomaly day is recorded (Req 6.6).
    """
    non_anomaly = metadata.get("non_anomaly_days") or {}
    candidates = [
        (day, float(total))
        for day, total in non_anomaly.items()
        if isinstance(day, str) and day[:10] < before_day
    ]
    if not candidates:
        return None
    candidates.sort(key=lambda pair: pair[0])
    return candidates[-1]


def detect_cold_start(
    account_total_cost: float,
    metadata: dict[str, Any],
    target_date: str,
    absolute_threshold: float,
) -> tuple[bool, Optional[str]]:
    """Cold_Start detection vs the most recent non-anomaly day (Req 6.2, 6.6).

    Returns ``(detected, baseline_day)``. An anomaly is detected when
    ``abs(target_total - previous_total) > absolute_threshold``. When no prior
    non-anomaly day exists, returns ``(False, None)`` and defers to the
    new-service trigger (Req 6.6).
    """
    previous = most_recent_non_anomaly_day(metadata, target_date)
    if previous is None:
        return False, None
    baseline_day, baseline_total = previous
    detected = abs(account_total_cost - baseline_total) > absolute_threshold
    return detected, baseline_day


def detect_statistical(stl_result: StlResult) -> bool:
    """Statistical detection: Target_Date residual exceeds the threshold (Req 6.3)."""
    return stl_result.target_residual > stl_result.residual_threshold
