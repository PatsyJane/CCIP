"""Dependency wiring + run entry point (design component ``runner.py``).

Builds the :class:`connector.ccip.nodes.CcipDeps` container (real or fake) and
runs the compiled graph once for a ``(account_id, target_date)``:

- :func:`build_live_deps` wires the REAL ``aws_cur_cost`` (via ``load_config`` +
  ``HttpClient`` + ``make_aws_cur_cost``), the real Gemini client, the real SMTP
  sender for anomaly alerts, and a SEPARATE SMTP sender addressed to
  ``email_failure_to`` for terminal failure alerts (Req 12.8) — using the loaded
  :class:`CcipConfig`.
- :func:`build_fake_deps` wires in-memory fakes for an offline run (no network),
  including a separate fake failure sender.
- :func:`run_once` seeds the state, short-circuits on a seed/validation error, and
  invokes the compiled graph. A graph-internal error still routes through the
  terminal failure-notification node and is surfaced unchanged as the result
  (Req 12.6).

Requirements: 2.1, 9.5, 12.8.
"""

from __future__ import annotations

from datetime import date
from typing import Any, Optional, Union

from .config import CcipConfig
from .email import SmtpEmailSender
from .graph import build_graph
from .nodes import CcipDeps
from .state import CcipState, build_initial_state

__all__ = ["build_live_deps", "build_fake_deps", "run_once"]


def build_live_deps(
    config: CcipConfig, *, force_email_dry_run: bool = False
) -> tuple[CcipDeps, Optional[Any]]:
    """Wire the real Cost_Tool, Gemini client, and SMTP sender (Req 9.5).

    When ``force_email_dry_run`` is True the SMTP sender is forced into its safe
    log-only path even if host/username/password are all configured, so a live
    run exercises the real Cost_API + Gemini without sending a real email.

    Returns ``(deps, http)`` where ``http`` is the shared :class:`HttpClient` the
    caller must close after the run (or ``None`` if not applicable).
    """
    from connector.config import load_config
    from connector.http_client import HttpClient
    from connector.tools.aws_cur_cost import make_aws_cur_cost

    from .llm_client import GeminiLLMClient

    app_config = load_config()
    http = HttpClient()
    cost_tool = make_aws_cur_cost(app_config, http)

    secrets = config.secrets
    llm_client = GeminiLLMClient(
        api_key=secrets.llm_api_key if secrets else "",
        model=config.llm_model,
    )
    email_sender = SmtpEmailSender(
        host=config.smtp_host,
        port=config.smtp_port,
        username=config.smtp_username or config.email_from,
        password=secrets.smtp_password if secrets else None,
        email_from=config.email_from,
        email_to=config.email_to,
        force_dry_run=force_email_dry_run,
    )
    # A SEPARATE sender for terminal failure-notification alerts, addressed to the
    # ops recipient ``email_failure_to`` (Req 12.8). It reuses the same SMTP
    # settings; when the ops recipient is blank it is forced into a safe dry run so
    # a misconfigured recipient never raises. ``force_email_dry_run`` propagates so
    # a safe live run never sends a real failure email either.
    failure_recipient = config.email_failure_to or config.email_to
    failure_email_sender = SmtpEmailSender(
        host=config.smtp_host,
        port=config.smtp_port,
        username=config.smtp_username or config.email_from,
        password=secrets.smtp_password if secrets else None,
        email_from=config.email_from,
        email_to=failure_recipient,
        force_dry_run=force_email_dry_run or not config.email_failure_to,
    )
    deps = CcipDeps(
        cost_tool=cost_tool,
        llm_client=llm_client,
        email_sender=email_sender,
        failure_email_sender=failure_email_sender,
        config=config,
    )
    return deps, http


def build_fake_deps(
    config: CcipConfig,
    *,
    cost_payload: Optional[dict] = None,
    narrative_text: str = "Fake narrative (offline run).",
) -> CcipDeps:
    """Wire safe in-memory fakes so an offline run performs no network/SMTP call."""

    async def _fake_cost_tool(start: str, end: str, granularity: str = "DAILY") -> dict:
        return cost_payload or {"granularity": "DAILY", "items": []}

    class _FakeLLM:
        async def complete(self, prompt: str) -> str:
            return narrative_text

    class _FakeEmailSender:
        can_send = False

        def __init__(self) -> None:
            self.sent: list[tuple[str, str]] = []

        async def send(self, subject: str, body: str) -> None:
            self.sent.append((subject, body))

    # A SEPARATE fake sender for failure alerts so an offline run exercises the
    # terminal failure-notification path (Req 12.8) without crossing wires with
    # the anomaly-alert sender.
    return CcipDeps(
        cost_tool=_fake_cost_tool,
        llm_client=_FakeLLM(),
        email_sender=_FakeEmailSender(),
        failure_email_sender=_FakeEmailSender(),
        config=config,
    )


async def run_once(
    account_id: str, target_date: Union[str, date], deps: CcipDeps
) -> Union[CcipState, dict]:
    """Run the workflow once; return the terminal state or a Structured_Error.

    Uniform result contract:

    - **Success** -> the full terminal :class:`CcipState` (its ``error`` key is
      ``None``).
    - **Failure** (seed/validation OR any node) -> the bare Structured_Error
      ``dict`` (``{"error", "category", ...}``), the SAME shape in both cases so
      callers have one rule: ``isinstance(result, dict) and "category" in result``
      -> failure (Req 2.3, 10.2).
    """
    target_str = target_date.isoformat() if isinstance(target_date, date) else str(target_date)
    seed = build_initial_state(account_id, target_str)
    if seed.get("error"):
        return seed["error"]
    graph = build_graph(deps)
    final_state = await graph.ainvoke(seed)
    # Uniform contract: a node failure surfaces as the bare Structured_Error, the
    # same shape as a seed-validation failure, so callers have one rule
    # (``isinstance(result, dict) and "category" in result`` -> error).
    if isinstance(final_state, dict) and final_state.get("error"):
        return final_state["error"]
    return final_state
