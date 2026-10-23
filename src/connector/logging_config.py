"""Centralized structured JSON logging (shared across both pipelines).

One place to configure logging for the whole PoC — the Billing & Usage
Ingestion FastMCP server *and* the CCIP LangGraph workflow — so every log line
is a single JSON object on one line, easy to grep/ship to a log aggregator.

Design goals (from the logging requirements):

1. **Structured JSON** — every record carries standard metadata
   (``timestamp``, ``level``, ``module``, ``logger``) plus an ``event`` key
   naming the action, and any structured payload fields the caller attached.
2. **Deterministic math/state capture** — callers log plain ``dict`` payloads
   (e.g. the Phase-4 detection variables) via ``extra={"payload": {...}}``; the
   formatter serializes them verbatim so the exact numbers used in an
   evaluation are preserved alongside the ``event``.
3. **Strict secret scrubbing** — the formatter runs every emitted record
   through :func:`connector.ccip.redaction.scrub_secrets` against the process's
   registered secrets, so a credential/API key/SMTP password can never reach a
   log sink even if a caller accidentally places one in a payload. This layers
   on top of the ``__repr__`` redaction in :class:`connector.config.AwsCredentials`
   / :class:`connector.ccip.config.CcipSecrets`.
4. **Stdlib only** — Python's standard :mod:`logging` plus a custom
   :class:`logging.Formatter`; no new dependencies (Python >= 3.11 stack).

Public surface:

- :func:`configure_logging` — install the JSON handler on the root logger at the
  configured level (idempotent). Call once at process startup.
- :func:`register_secrets` — register secret substrings to scrub from every log
  record (called from the config loaders / entry points).
- :func:`get_logger` — thin wrapper over :func:`logging.getLogger`.
- :func:`log_event` — convenience: emit ``event`` + structured ``payload`` in
  one call.
- :func:`log_structured_error` — emit an ``ERROR`` record aligned with a
  Structured_Error object (Errors-over-exceptions principle).
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from connector.ccip.redaction import scrub_secrets

__all__ = [
    "DEFAULT_LOG_LEVEL",
    "JsonFormatter",
    "configure_logging",
    "register_secrets",
    "get_logger",
    "log_event",
    "log_structured_error",
]

#: Fallback level when nothing is configured via ``LOG_LEVEL`` / arguments.
DEFAULT_LOG_LEVEL = "INFO"

#: Process-wide registry of secret substrings to scrub from every log record.
#: Populated by :func:`register_secrets` (from the config loaders / entry
#: points). Deliberately module-global so the formatter can scrub regardless of
#: which logger emitted the record.
_REGISTERED_SECRETS: set[str] = set()

#: The standard ``LogRecord`` attributes we never copy into the JSON output as
#: ad-hoc fields (they are either rendered explicitly or are noise).
_RESERVED_RECORD_KEYS = frozenset(
    {
        "name", "msg", "args", "levelname", "levelno", "pathname", "filename",
        "module", "exc_info", "exc_text", "stack_info", "lineno", "funcName",
        "created", "msecs", "relativeCreated", "thread", "threadName",
        "processName", "process", "taskName", "event", "payload",
    }
)


def register_secrets(secrets: Iterable[str]) -> None:
    """Register secret substrings to scrub from every subsequent log record.

    Non-string, empty, or whitespace-only entries are ignored. Safe to call
    multiple times (e.g. once per loaded config); registrations accumulate so
    both the Connector's AWS credentials and the CCIP LLM/SMTP secrets are
    scrubbed in a process that runs both.
    """
    for secret in secrets:
        if isinstance(secret, str) and secret.strip():
            _REGISTERED_SECRETS.add(secret)


class JsonFormatter(logging.Formatter):
    """Render a :class:`logging.LogRecord` as a single-line JSON object.

    Standard metadata always present: ``timestamp`` (UTC ISO-8601), ``level``,
    ``logger``, ``module``, and ``event`` (the action; falls back to the log
    message when no explicit ``event`` was supplied). A structured ``payload``
    dict and any other non-reserved ``extra`` attributes are merged in.

    Every assembled record is passed through
    :func:`connector.ccip.redaction.scrub_secrets` against the registered
    secrets so no credential/API key/SMTP password can leak into a sink.
    """

    def format(self, record: logging.LogRecord) -> str:
        message = record.getMessage()
        entry: dict[str, Any] = {
            "timestamp": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            # ``event`` names the action; default to the message so a plain
            # ``logger.info("...")`` still produces a meaningful ``event``.
            "event": getattr(record, "event", None) or message,
        }

        # If a distinct human message exists alongside an explicit event, keep
        # it so nothing is lost.
        if getattr(record, "event", None) and message and message != entry["event"]:
            entry["message"] = message

        # Structured math/state payload (the primary contextual channel).
        payload = getattr(record, "payload", None)
        if isinstance(payload, dict):
            entry["payload"] = payload

        # Any other caller-supplied ``extra`` fields (non-reserved).
        for key, value in record.__dict__.items():
            if key not in _RESERVED_RECORD_KEYS and key not in entry:
                entry[key] = value

        if record.exc_info:
            entry["exc_info"] = self.formatException(record.exc_info)

        # Strict scrub: run the whole record through the shared redactor so a
        # stray secret in any string value is replaced with ``***`` before it is
        # ever written (reuses ccip/redaction.scrub_secrets).
        scrubbed = scrub_secrets(entry, _REGISTERED_SECRETS)
        return json.dumps(scrubbed, default=str, ensure_ascii=False)


def _resolve_level(level: Optional[str]) -> int:
    """Map a level name to a :mod:`logging` level int, defaulting safely.

    Order of precedence: explicit ``level`` arg -> ``LOG_LEVEL`` env var ->
    :data:`DEFAULT_LOG_LEVEL`. An unrecognized name falls back to ``INFO``
    rather than raising, so a typo never crashes startup.
    """
    name = (level or os.environ.get("LOG_LEVEL") or DEFAULT_LOG_LEVEL).strip().upper()
    resolved = logging.getLevelName(name)
    return resolved if isinstance(resolved, int) else logging.INFO


def configure_logging(
    level: Optional[str] = None, *, secrets: Optional[Iterable[str]] = None
) -> None:
    """Install the JSON handler on the root logger (idempotent).

    Call once at process startup (the FastMCP server ``main`` and the CCIP CLI).
    Replaces any handlers this module previously installed so repeated calls do
    not duplicate output. Logs are written to ``stderr`` to keep ``stdout`` clean
    for the MCP stdio transport / CLI result printing.

    ``level`` overrides the ``LOG_LEVEL`` environment variable when given. Any
    ``secrets`` are registered for scrubbing before the handler is attached.
    """
    if secrets:
        register_secrets(secrets)

    root = logging.getLogger()
    root.setLevel(_resolve_level(level))

    # Remove only handlers we installed to stay idempotent without clobbering
    # handlers a host application may have added.
    for handler in list(root.handlers):
        if getattr(handler, "_ccip_json_handler", False):
            root.removeHandler(handler)

    handler = logging.StreamHandler()  # defaults to stderr
    handler.setFormatter(JsonFormatter())
    handler._ccip_json_handler = True  # type: ignore[attr-defined]
    root.addHandler(handler)


def get_logger(name: str) -> logging.Logger:
    """Return a module logger (thin wrapper over :func:`logging.getLogger`)."""
    return logging.getLogger(name)


def log_event(
    logger: logging.Logger,
    event: str,
    *,
    level: int = logging.INFO,
    payload: Optional[dict[str, Any]] = None,
    **fields: Any,
) -> None:
    """Emit a structured record: an ``event`` name + optional ``payload`` dict.

    ``payload`` is intended for the deterministic math/state variables so the
    exact numbers used in an evaluation are captured verbatim. Extra keyword
    ``fields`` become top-level record attributes. The formatter scrubs secrets
    from everything before writing.
    """
    extra: dict[str, Any] = {"event": event}
    if payload is not None:
        extra["payload"] = payload
    extra.update(fields)
    # stacklevel=2 so ``record.module`` reflects the caller, not this helper.
    logger.log(level, event, extra=extra, stacklevel=2)


def log_structured_error(
    logger: logging.Logger,
    error: dict[str, Any],
    *,
    event: str,
    **fields: Any,
) -> None:
    """Emit an ``ERROR`` record aligned with a Structured_Error object.

    Mirrors the "Errors over exceptions at boundaries" principle: whenever a
    Structured_Error (``{"error", "category", ...}``) is produced, log a
    corresponding ERROR line carrying ``error_category`` and ``error_message``
    plus any safe diagnostic ``fields`` (e.g. ``account_id``, ``account_name``,
    ``delivery_method``, a redacted config snapshot). Secrets are scrubbed by the
    formatter.
    """
    payload: dict[str, Any] = {
        "error_category": error.get("category"),
        "error_message": error.get("error"),
    }
    # Carry through any additional safe context already on the Structured_Error
    # (e.g. ``source``, ``field``, ``status_code``) without overwriting caller
    # fields.
    for key, value in error.items():
        if key not in {"error", "category"} and key not in payload:
            payload[key] = value
    payload.update({k: v for k, v in fields.items() if v is not None})
    # stacklevel=2 so ``record.module`` reflects the caller, not this helper.
    logger.error(event, extra={"event": event, "payload": payload}, stacklevel=2)
