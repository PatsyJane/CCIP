"""Alert payload build + email delivery (design component ``email.py``).

Phase 6 helpers:

- :func:`build_alert_payload` — assemble the deterministic payload from the state
  and metadata: account/date/data_mode, new services, contributors (with seasonal
  context), the account total, and the last-known-anomaly date/context (Req 9.2,
  9.3). Pure.
- :class:`EmailSender` — the injected delivery protocol (async ``send``).
- :func:`deliver` — hand a rendered subject/body to the sender under a 30s guard;
  failure/timeout -> ``unreachable-source`` (Req 9.5). The concrete
  :class:`SmtpEmailSender` sends via stdlib ``smtplib`` and performs a safe dry
  run (log only) when SMTP is not fully configured (Req 9.5).

Requirements: 9.2, 9.3, 9.5.
"""

from __future__ import annotations

import asyncio
import smtplib
from email.message import EmailMessage
from typing import Any, Callable, Optional, Protocol, runtime_checkable

from connector.errors import ErrorCategory, structured_error

__all__ = [
    "EMAIL_TIMEOUT_SECONDS",
    "EmailSender",
    "build_alert_payload",
    "build_subject",
    "deliver",
    "SmtpEmailSender",
]

#: Delivery must complete within this many seconds or it is a failure (Req 9.5).
EMAIL_TIMEOUT_SECONDS: int = 30

_EMAIL_STAGE = "email-delivery"
_IMPLICIT_TLS_PORT = 465


@runtime_checkable
class EmailSender(Protocol):
    """The injected Email_Sender dependency (async ``send(subject, body)``)."""

    async def send(self, subject: str, body: str) -> None:  # pragma: no cover - protocol
        ...


def build_alert_payload(state: dict[str, Any], metadata: dict[str, Any]) -> dict[str, Any]:
    """Assemble the deterministic alert payload (Req 9.2, 9.3).

    Includes the current data_mode, new services, anomaly contributors (with
    seasonal context), the account total, and the last-known-anomaly record from
    the Metadata_Store. Pure; fabricates nothing.
    """
    last_anomaly = metadata.get("last_anomaly")
    return {
        "account_id": state.get("account_id"),
        "target_date": state.get("target_date"),
        "data_mode": state.get("data_mode"),
        "account_total_cost": state.get("account_total_cost"),
        "account_anomaly_detected": bool(state.get("account_anomaly_detected")),
        "new_services": list(state.get("new_services_detected") or []),
        "contributors": list(state.get("anomaly_contributors") or []),
        "last_anomaly": last_anomaly,
    }


def build_subject(payload: dict[str, Any]) -> str:
    """Build a non-empty subject line, clamped to <= 200 characters."""
    account = payload.get("account_id", "unknown")
    date = payload.get("target_date", "")
    if payload.get("account_anomaly_detected"):
        subject = f"Cloud cost anomaly: account {account} on {date}"
    elif payload.get("new_services"):
        n = len(payload["new_services"])
        subject = f"New cloud services detected: account {account} on {date} ({n})"
    else:
        subject = f"Cloud cost report: account {account} on {date}"
    return subject[:200]


async def deliver(sender: EmailSender, subject: str, body: str) -> Optional[dict]:
    """Hand the alert to the injected sender under a 30s guard (Req 9.5).

    Returns ``None`` on success, otherwise an ``unreachable-source``
    Structured_Error. Never raises across the boundary.
    """
    try:
        await asyncio.wait_for(sender.send(subject, body), timeout=EMAIL_TIMEOUT_SECONDS)
    except asyncio.TimeoutError:
        return structured_error(
            f"email delivery did not complete within {EMAIL_TIMEOUT_SECONDS}s",
            ErrorCategory.UNREACHABLE_SOURCE,
            source=_EMAIL_STAGE,
        )
    except Exception as exc:  # noqa: BLE001 - never raise across the boundary.
        return structured_error(
            f"email delivery failed: {exc}",
            ErrorCategory.UNREACHABLE_SOURCE,
            source=_EMAIL_STAGE,
        )
    return None


class SmtpEmailSender:
    """Concrete :class:`EmailSender` using stdlib SMTP, with a dry-run fallback.

    When ``host``, ``username``, and ``password`` are all set, the blocking SMTP
    send runs in a worker thread via :func:`asyncio.to_thread`. Otherwise the
    rendered alert is logged and the method returns normally (safe dry run,
    Req 9.5). Port 465 uses implicit TLS; any other port uses STARTTLS.
    """

    def __init__(
        self,
        *,
        host: Optional[str],
        port: int,
        username: Optional[str],
        password: Optional[str],
        email_from: str,
        email_to: str,
        log: Callable[[str], None] = print,
        force_dry_run: bool = False,
    ) -> None:
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._email_from = email_from
        self._email_to = email_to
        self._log = log
        # When True, behave as if SMTP were unconfigured: log the alert and send
        # nothing, even when host/username/password are all present. A safety
        # switch for live-but-safe runs (CLI --dry-run-email).
        self._force_dry_run = force_dry_run

    @property
    def can_send(self) -> bool:
        """True iff a live SMTP send is possible (host + username + password).

        Always ``False`` when ``force_dry_run`` was set, so the delivery stage
        takes the safe log-only path regardless of configuration.
        """
        if self._force_dry_run:
            return False
        return bool(self._host and self._username and self._password)

    async def send(self, subject: str, body: str) -> None:
        if not self.can_send:
            self._log_dry_run(subject, body)
            return
        await asyncio.to_thread(self._send_blocking, subject, body)

    def _log_dry_run(self, subject: str, body: str) -> None:
        self._log(
            "[SmtpEmailSender] SMTP not configured - DRY RUN, no email sent.\n"
            f"  From: {self._email_from}\n"
            f"  To: {self._email_to}\n"
            f"  Subject: {subject}\n"
            "  --- Body ---\n"
            f"{body}\n"
            "  --- End body ---"
        )

    def _send_blocking(self, subject: str, body: str) -> None:
        message = EmailMessage()
        message["Subject"] = subject
        message["From"] = self._email_from
        message["To"] = self._email_to
        message.set_content(body)
        if self._port == _IMPLICIT_TLS_PORT:
            with smtplib.SMTP_SSL(self._host, self._port, timeout=30) as server:
                server.login(self._username, self._password)
                server.send_message(message)
            return
        with smtplib.SMTP(self._host, self._port, timeout=30) as server:
            server.starttls()
            server.login(self._username, self._password)
            server.send_message(message)
