"""Secret-redaction scrubber for the CCIP workflow (design component ``redaction.py``).

Defines :func:`scrub_secrets`, which walks every value of a Structured_Error dict
(shape ``{"error": str, "category": str, ...extra}`` where ``extra`` may hold
nested dicts/lists/tuples/sets/strings) and replaces any configured secret
substring with ``***``, so no returned field can contain a secret value — even
one placed there by a dependency such as the Cost_Tool or LLM client.

Ported from the discarded ``connector.anomaly.redaction`` module; behavior is
identical.

Requirements: 10.4.
"""

from __future__ import annotations

from typing import Any, Iterable

__all__ = ["scrub_secrets"]

_REDACTED = "***"


#: Secrets shorter than this are not redacted. A 1-2 character "secret" would
#: match substrings of ordinary words (e.g. redacting "p" would corrupt
#: "unexpected-error"), doing more harm than good. Real credentials are far
#: longer than this floor, so the guard never weakens genuine redaction.
_MIN_SECRET_LEN = 4


def _clean_secrets(secrets: Iterable[str]) -> list[str]:
    """Return the meaningful secret substrings to redact.

    Empty, whitespace-only, or too-short secrets are skipped so we never replace
    fragments of ordinary words (a 1-char "secret" would match everywhere).
    Longer secrets are ordered first so a secret which contains a shorter secret
    is redacted as a whole before its fragment.
    """
    cleaned = [
        s for s in secrets
        if isinstance(s, str) and len(s.strip()) >= _MIN_SECRET_LEN
    ]
    unique = list(dict.fromkeys(cleaned))
    unique.sort(key=len, reverse=True)
    return unique


def _scrub_value(value: Any, secrets: list[str]) -> Any:
    """Recursively scrub secret substrings from ``value`` (never mutates input)."""
    if isinstance(value, str):
        for secret in secrets:
            value = value.replace(secret, _REDACTED)
        return value
    if isinstance(value, dict):
        return {key: _scrub_value(item, secrets) for key, item in value.items()}
    if isinstance(value, list):
        return [_scrub_value(item, secrets) for item in value]
    if isinstance(value, tuple):
        return tuple(_scrub_value(item, secrets) for item in value)
    if isinstance(value, set):
        return {_scrub_value(item, secrets) for item in value}
    return value


def scrub_secrets(error: dict, secrets: Iterable[str]) -> dict:
    """Return a copy of a Structured_Error with every secret substring redacted.

    Walks all values of ``error`` (including nested dicts/lists/tuples/sets) and
    replaces any configured secret substring with ``***`` in every string, so no
    returned field's value can contain a configured secret (Req 10.4). Blank
    secrets are ignored. The input is never mutated; a new dict is returned.
    """
    cleaned = _clean_secrets(secrets)
    if not cleaned:
        return dict(error)
    return {key: _scrub_value(value, cleaned) for key, value in error.items()}
