"""STL seasonal-trend decomposition adapter (design component ``stl_model.py``).

Wraps ``statsmodels.tsa.seasonal.STL`` to decompose the account-level daily total
series into Trend / Seasonal / Residual components, then derives a residual
threshold used by Phase 4 statistical detection. Two threshold methods are
supported (config ``statistical_method``):

- ``iqr``   : threshold = ``Q3 + iqr_factor * (Q3 - Q1)`` over the residuals;
- ``sigma`` : threshold = ``mean + sigma_factor * std`` over the residuals.

The Target_Date residual is the last residual in the (ascending) series. STL
requires at least two full seasonal periods of observations; when the series is
too short, :func:`run_stl` returns a Structured_Error so Phase 4 can fall back
rather than crash. Nothing raises across the boundary.

Requirements: 5.4, 6.3.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Union

from connector.errors import ErrorCategory, structured_error

__all__ = ["StlResult", "run_stl", "compute_residual_threshold"]

_STL_STAGE = "stl"


@dataclass(frozen=True)
class StlResult:
    """Summary of an STL decomposition + the detection threshold (Req 5.4, 6.3).

    STL is fit on the history **excluding** the Target_Date so the Target_Date's
    own cost cannot distort the trend/seasonal fit or the threshold (otherwise a
    robust fit absorbs the spike and the residual collapses to ~0). The expected
    Target_Date value is reconstructed from the fitted trend + the seasonal
    component for its position, and the Target_Date residual is
    ``actual_target - expected_target``.

    - ``trend`` / ``seasonal`` / ``resid`` are the per-day component values of the
      fit over the history window (ascending by day), excluding the Target_Date.
    - ``residual_threshold`` is the IQR- or sigma-based bound over those residuals.
    - ``target_residual`` is ``actual_target - expected_target`` (Req 6.3).
    - ``expected_target`` is the model's expectation for the Target_Date.
    - ``method`` records which threshold method was used.
    - ``seasonal_summary`` maps each day-of-week index (0-6) to its mean seasonal
      component, suitable for persisting to ``metadata['seasonal']`` (Req 5.5).
    """

    trend: list[float]
    seasonal: list[float]
    resid: list[float]
    residual_threshold: float
    target_residual: float
    expected_target: float
    method: str
    seasonal_summary: dict[int, float]


def compute_residual_threshold(
    resid: list[float], method: str, iqr_factor: float, sigma_factor: float
) -> float:
    """Return the residual threshold for the configured method (Req 6.3)."""
    if not resid:
        return 0.0
    if method == "sigma":
        n = len(resid)
        mean = sum(resid) / n
        var = sum((r - mean) ** 2 for r in resid) / n
        std = var ** 0.5
        return mean + sigma_factor * std
    # Default: IQR.
    ordered = sorted(resid)
    q1 = _percentile(ordered, 25.0)
    q3 = _percentile(ordered, 75.0)
    return q3 + iqr_factor * (q3 - q1)


def _percentile(ordered: list[float], pct: float) -> float:
    """Linear-interpolation percentile over an already-sorted list."""
    if not ordered:
        return 0.0
    if len(ordered) == 1:
        return ordered[0]
    rank = (pct / 100.0) * (len(ordered) - 1)
    low = int(rank)
    high = min(low + 1, len(ordered) - 1)
    frac = rank - low
    return ordered[low] + (ordered[high] - ordered[low]) * frac


def run_stl(account_history: list[dict[str, Any]], config: Any) -> Union[StlResult, dict]:
    """Decompose the account total series and compute the residual threshold.

    ``account_history`` is ascending ``[{"day", "cost"}, ...]``. Returns an
    :class:`StlResult` on success, or a Structured_Error when the series is too
    short for the configured STL period (so Phase 4 can fall back). Never raises.
    """
    try:
        rows = [row for row in account_history if isinstance(row, dict)]
        all_values = [float(row.get("cost", 0.0)) for row in rows]
        all_days = [str(row.get("day", "")) for row in rows]
        period = int(getattr(config, "stl_period", 7))

        # Fit on the history EXCLUDING the Target_Date (the last point) so the
        # Target_Date's own cost cannot distort the fit or threshold.
        if len(all_values) < 1:
            return structured_error(
                "insufficient history for STL: no data points",
                ErrorCategory.MISSING_DATA,
                source=_STL_STAGE,
                available_points=0,
                required_points=2 * period,
            )
        fit_values = all_values[:-1]
        fit_days = all_days[:-1]
        actual_target = all_values[-1]

        # STL needs at least two full periods of observations in the fit window.
        if len(fit_values) < 2 * period:
            return structured_error(
                f"insufficient history for STL: {len(fit_values)} points, need at "
                f"least {2 * period} for period {period}",
                ErrorCategory.MISSING_DATA,
                source=_STL_STAGE,
                available_points=len(fit_values),
                required_points=2 * period,
            )

        # Import locally so merely importing this module needs no heavy deps.
        from statsmodels.tsa.seasonal import STL

        seasonal = int(getattr(config, "stl_seasonal", 7))
        robust = bool(getattr(config, "stl_robust", True))
        # ``seasonal`` must be an odd integer >= 7.
        if seasonal % 2 == 0:
            seasonal += 1

        result = STL(fit_values, period=period, seasonal=seasonal, robust=robust).fit()
        trend = [float(x) for x in result.trend]
        seasonal_comp = [float(x) for x in result.seasonal]
        resid = [float(x) for x in result.resid]

        threshold = compute_residual_threshold(
            resid,
            getattr(config, "statistical_method", "iqr"),
            float(getattr(config, "iqr_factor", 1.5)),
            float(getattr(config, "sigma_factor", 3.0)),
        )

        # Expected Target_Date value = last fitted trend + the seasonal component
        # for the Target_Date's phase in the weekly cycle. The Target_Date residual
        # is actual minus expected (Req 6.3).
        expected_target = _expected_next(trend, seasonal_comp, period)
        target_residual = actual_target - expected_target
        seasonal_summary = _summarize_seasonal_by_weekday(fit_days, seasonal_comp)

        return StlResult(
            trend=trend,
            seasonal=seasonal_comp,
            resid=resid,
            residual_threshold=threshold,
            target_residual=target_residual,
            expected_target=expected_target,
            method=getattr(config, "statistical_method", "iqr"),
            seasonal_summary=seasonal_summary,
        )
    except Exception as exc:  # noqa: BLE001 - never raise across the boundary.
        return structured_error(
            f"STL decomposition failed: {exc}",
            ErrorCategory.UNEXPECTED_ERROR,
            source=_STL_STAGE,
        )


def _expected_next(trend: list[float], seasonal: list[float], period: int) -> float:
    """Expected value for the day immediately after the fit window.

    Uses the last fitted trend level (a persistence/naive trend forecast for one
    step ahead) plus the seasonal component for the matching phase — the value one
    full ``period`` before the end of the seasonal series, which shares the next
    day's position in the weekly cycle.
    """
    if not trend:
        return 0.0
    last_trend = trend[-1]
    seasonal_component = 0.0
    if seasonal:
        # The next day's phase matches the point `period` steps before the end
        # of the fitted seasonal series.
        idx = len(seasonal) - period
        if 0 <= idx < len(seasonal):
            seasonal_component = seasonal[idx]
        else:
            seasonal_component = seasonal[-1]
    return last_trend + seasonal_component


def _summarize_seasonal_by_weekday(
    days: list[str], seasonal: list[float]
) -> dict[int, float]:
    """Average the seasonal component per weekday (0=Mon..6=Sun) for persistence."""
    from datetime import date

    buckets: dict[int, list[float]] = {}
    for day, value in zip(days, seasonal):
        try:
            weekday = date.fromisoformat(day[:10]).weekday()
        except ValueError:
            continue
        buckets.setdefault(weekday, []).append(value)
    return {wd: sum(vals) / len(vals) for wd, vals in buckets.items() if vals}
