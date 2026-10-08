"""CCIP domain models (design component ``models.py``).

Frozen dataclasses and the :class:`DataMode` enum shared across the workflow.
All models are immutable in the :mod:`connector.dimensions` / :mod:`connector.numeric`
style. The LangGraph state stores plain dicts (serializable); these dataclasses
are used for construction/validation inside pure functions and converted to
dicts at the state boundary.

Requirements: 2.2, 3.2, 8.3, 9.3.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional

__all__ = [
    "DataMode",
    "classify_data_mode",
    "is_above_cold_start",
    "ServiceCost",
    "Contributor",
    "LastAnomaly",
    "AlertPayload",
]


class DataMode(str, Enum):
    """Account history-depth classification (Req 3.2).

    Subclassing ``str`` keeps the value comparable/serializable as its string
    form (e.g. ``"Normal"``), matching the Connector's plain-value style.
    """

    COLD_START = "Cold Start"  # < 30 complete days
    INITIAL = "Initial"        # 30-59
    BEGINNER = "Beginner"      # 60-89
    NORMAL = "Normal"          # >= 90


#: Ascending order of data modes, for ``>`` comparisons (e.g. "above Cold Start").
DATA_MODE_ORDER: tuple[DataMode, ...] = (
    DataMode.COLD_START,
    DataMode.INITIAL,
    DataMode.BEGINNER,
    DataMode.NORMAL,
)


def classify_data_mode(available_days: int) -> DataMode:
    """Classify an account on the fixed 30/60/90 boundaries (Req 3.2)."""
    if available_days < 30:
        return DataMode.COLD_START
    if available_days < 60:
        return DataMode.INITIAL
    if available_days < 90:
        return DataMode.BEGINNER
    return DataMode.NORMAL


def is_above_cold_start(mode: DataMode) -> bool:
    """True when the mode has enough history to run STL (``> Cold Start``) — Req 5.4."""
    return mode != DataMode.COLD_START


@dataclass(frozen=True)
class ServiceCost:
    """One service's cost on a single day."""

    service: str
    cost: float


@dataclass(frozen=True)
class Contributor:
    """A service identified as driving an Account_Anomaly (Req 8.3, 8.4)."""

    service: str
    delta: float
    baseline_cost: float
    current_cost: float
    seasonal_context: Optional[str] = None


@dataclass(frozen=True)
class LastAnomaly:
    """The last recorded anomaly for an account, for alert context (Req 9.2, 9.3)."""

    date: str
    contributors: tuple[str, ...]
    context: str


@dataclass(frozen=True)
class AlertPayload:
    """The deterministic alert payload rendered into an email (Req 9.3)."""

    account_id: str
    target_date: str
    data_mode: str
    new_services: tuple[str, ...]
    contributors: tuple[Contributor, ...]
    last_anomaly: Optional[LastAnomaly]
    account_total_cost: float
    account_anomaly_detected: bool
