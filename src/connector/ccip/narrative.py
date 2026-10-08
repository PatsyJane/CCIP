"""Alert narrative generation (design component ``narrative.py``).

Phase 6's narrative step: turn the deterministically computed alert payload into
a human-readable explanation. The Gemini LLM is prompted with the payload and
instructed NOT to recompute, alter, or invent any number (Req 9.4). When the LLM
is unreachable or exceeds its timeout, the stage falls back to a deterministic
templated narrative and records that it was templated (Req 9.6) rather than
failing the whole run.

Both the LLM narrative and the fallback are built from the SAME deterministic
payload, so numeric content is identical either way.

Requirements: 9.4, 9.6.
"""

from __future__ import annotations

import asyncio
from typing import Any, Protocol, runtime_checkable

__all__ = ["LLMClient", "NARRATIVE_TIMEOUT_SECONDS", "render_narrative", "build_templated_narrative"]

#: Per-invocation timeout for the narrative LLM call, in seconds (Req 9.6).
NARRATIVE_TIMEOUT_SECONDS: int = 30


@runtime_checkable
class LLMClient(Protocol):
    """The injected model-invocation dependency.

    Any object exposing ``async def complete(self, prompt: str) -> str`` — a real
    provider client or a test fake — satisfies this protocol.
    """

    async def complete(self, prompt: str) -> str:  # pragma: no cover - protocol
        ...


def _render_prompt(payload: dict[str, Any]) -> str:
    """Render the narrative prompt from the deterministic payload (Req 9.4)."""
    lines = [
        "You are a cloud cost anomaly alert writer. Turn the facts below into a "
        "concise, professional email body for an operations team.",
        "",
        "STRICT RULES:",
        "- Do NOT recompute, alter, round, or invent any number. Use the figures "
        "exactly as given.",
        "- Be factual and brief. No speculation beyond the provided seasonal context.",
        "",
        "FACTS:",
        f"- Account: {payload.get('account_id')}",
        f"- Date: {payload.get('target_date')}",
        f"- Data mode: {payload.get('data_mode')}",
        f"- Account total cost: {payload.get('account_total_cost')}",
        f"- Account anomaly detected: {payload.get('account_anomaly_detected')}",
    ]
    new_services = payload.get("new_services") or []
    if new_services:
        lines.append(f"- Newly detected services: {', '.join(new_services)}")
    contributors = payload.get("contributors") or []
    if contributors:
        lines.append("- Top contributors:")
        for c in contributors:
            ctx = f" ({c['seasonal_context']})" if c.get("seasonal_context") else ""
            lines.append(
                f"    * {c['service']}: +{c['delta']} "
                f"(from {c.get('baseline_cost')} to {c.get('current_cost')}){ctx}"
            )
    last = payload.get("last_anomaly")
    if last:
        lines.append(
            f"- Last recorded anomaly: {last.get('date')} — {last.get('context')}"
        )
    return "\n".join(lines)


def build_templated_narrative(payload: dict[str, Any]) -> str:
    """Deterministic templated narrative built from the payload (Req 9.6 fallback)."""
    parts: list[str] = []
    parts.append(
        f"Cloud cost anomaly report for account {payload.get('account_id')} "
        f"on {payload.get('target_date')} (data mode: {payload.get('data_mode')})."
    )
    parts.append(f"Account total cost: {payload.get('account_total_cost')}.")
    if payload.get("account_anomaly_detected"):
        parts.append("An account-level cost anomaly was detected.")
    new_services = payload.get("new_services") or []
    if new_services:
        parts.append("Newly detected services: " + ", ".join(new_services) + ".")
    contributors = payload.get("contributors") or []
    if contributors:
        parts.append("Top contributors to the increase:")
        for c in contributors:
            ctx = f" ({c['seasonal_context']})" if c.get("seasonal_context") else ""
            parts.append(
                f"  - {c['service']}: +{c['delta']} "
                f"(from {c.get('baseline_cost')} to {c.get('current_cost')}){ctx}"
            )
    last = payload.get("last_anomaly")
    if last:
        parts.append(
            f"Last recorded anomaly on this account: {last.get('date')} — "
            f"{last.get('context')}."
        )
    return "\n".join(parts)


async def render_narrative(
    payload: dict[str, Any], llm_client: LLMClient
) -> tuple[str, bool]:
    """Render the alert narrative, returning ``(text, templated)`` (Req 9.4, 9.6).

    Prompts ``llm_client`` under a :data:`NARRATIVE_TIMEOUT_SECONDS` guard. On any
    failure (unreachable, timeout, empty output) returns the deterministic
    templated narrative with ``templated=True`` rather than raising, so a flaky
    LLM never fails the whole run.
    """
    prompt = _render_prompt(payload)
    try:
        text = await asyncio.wait_for(
            llm_client.complete(prompt), timeout=NARRATIVE_TIMEOUT_SECONDS
        )
        if isinstance(text, str) and text.strip():
            return text, False
    except Exception:  # noqa: BLE001 - any failure => deterministic fallback.
        pass
    return build_templated_narrative(payload), True
