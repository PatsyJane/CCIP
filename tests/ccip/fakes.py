"""Injected fakes for the CCIP workflow tests.

Replace the Cost_Tool, LLM client, and Email_Sender so no unit/property/smoke
test performs a network, SMTP, or external call. Mirror the real dependency
interfaces exactly.
"""

from __future__ import annotations

from typing import Any, Optional


class FakeCostTool:
    """Async Cost_Tool returning a fixed payload or a chosen Structured_Error.

    Records every call's ``(start, end, granularity)`` so tests can assert the
    single-fetch contract.
    """

    def __init__(self, payload: dict) -> None:
        self._payload = payload
        self.calls: list[tuple[str, str, str]] = []

    async def __call__(self, start: str, end: str, granularity: str = "DAILY") -> dict:
        self.calls.append((start, end, granularity))
        return self._payload


class FakeLLMClient:
    """Async LLM client returning fixed text, or raising to simulate unreachability."""

    def __init__(self, text: str = "fake narrative", *, raise_exc: Optional[Exception] = None) -> None:
        self._text = text
        self._raise = raise_exc
        self.prompts: list[str] = []

    async def complete(self, prompt: str) -> str:
        self.prompts.append(prompt)
        if self._raise is not None:
            raise self._raise
        return self._text


class FakeEmailSender:
    """Async Email_Sender capturing sent (subject, body), or failing on demand."""

    def __init__(self, *, can_send: bool = False, raise_exc: Optional[Exception] = None) -> None:
        self.can_send = can_send
        self._raise = raise_exc
        self.sent: list[tuple[str, str]] = []

    async def send(self, subject: str, body: str) -> None:
        if self._raise is not None:
            raise self._raise
        self.sent.append((subject, body))


def make_daily_payload(
    service_series: dict[str, list[float]],
    end_day: str,
    *,
    account: str = "A",
    environment: str = "prod",
) -> dict:
    """Build an aws_cur_cost-shaped payload from per-service daily cost lists.

    ``service_series`` maps a service name to its ascending daily costs ending on
    ``end_day``. The resulting items carry the ``tags.service`` dimension the
    normalizer groups on.
    """
    from datetime import date, timedelta

    end = date.fromisoformat(end_day)
    items: list[dict[str, Any]] = []
    for service, costs in service_series.items():
        n = len(costs)
        for i, cost in enumerate(costs):
            day = (end - timedelta(days=n - 1 - i)).isoformat()
            items.append(
                {
                    "timestamp": day,
                    "cost_value": float(cost),
                    "tags": {"account": account, "service": service, "environment": environment},
                }
            )
    return {"granularity": "DAILY", "items": items}
