"""CCIP configuration loader (design component ``config.py``).

Parses ``config.yaml`` and injects ``${VAR}`` / ``${VAR:-default}`` placeholders
from the process environment (loaded from ``.env`` via ``python-dotenv``) into the
parsed VALUES — not the raw text — so ``${...}`` examples in YAML comments are
never mistaken for variables. The result is mapped into an immutable
:class:`CcipConfig`.
Secrets (the LLM API key and SMTP password) live on a ``repr``-excluded
``_secrets`` field whose ``__repr__`` is redacted, mirroring
:class:`connector.config.AwsCredentials` so secret values never leak into logs
or node output.

Reuses :class:`connector.config.ConfigError` so the whole project shares one
config-error contract.

Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 1.6.
"""

from __future__ import annotations

import os
import re
from dataclasses import dataclass, field
from typing import Any, Optional

import yaml
from dotenv import load_dotenv

# Share the Connector's ConfigError rather than redefining it.
from connector.config import ConfigError

__all__ = [
    "CcipConfig",
    "CcipSecrets",
    "ConfigError",
    "load_ccip_config",
    "resolve_env_placeholders",
]

# ${VAR} or ${VAR:-default}. VAR is an identifier; default runs to the closing
# brace and may be empty (``${VAR:-}``).
_PLACEHOLDER = re.compile(r"\$\{(?P<name>[A-Za-z_][A-Za-z0-9_]*)(?::-(?P<default>[^}]*))?\}")

#: Sentinel used while resolving: a required placeholder that could not be
#: resolved (no env value AND no ``:-default``) is recorded for a single,
#: deterministic error naming the first offender.
_UNRESOLVED = object()


@dataclass(frozen=True)
class CcipSecrets:
    """LLM and SMTP secrets. Values are redacted from ``repr`` (Req 1.5)."""

    llm_api_key: str
    smtp_password: str

    def __repr__(self) -> str:  # pragma: no cover - trivial redaction
        return f"{type(self).__name__}(llm_api_key=***, smtp_password=***)"


@dataclass(frozen=True)
class CcipConfig:
    """Immutable CCIP workflow configuration (Req 1.4).

    Detection thresholds, STL parameters, ingestion window, LLM/email settings,
    and the Metadata_Store path. Secrets are stored on the ``repr``-excluded
    ``_secrets`` field so they never leak (Req 1.5).
    """

    # Detection (Req 6)
    cold_start_absolute_threshold: float = 20.0
    statistical_method: str = "iqr"  # "iqr" | "sigma"
    iqr_factor: float = 1.5
    sigma_factor: float = 3.0
    # STL (Req 5.4)
    stl_period: int = 7
    stl_seasonal: int = 7
    stl_robust: bool = True
    # Ingestion (Req 3.1)
    history_days: int = 90
    # Incremental ingestion + per-account metadata (increment 2, Req 13, 15, 16).
    # ``target_date_offset_days`` is the D-2 default offset (a run on day D with no
    # explicit Target_Date evaluates D-N); ``trailing_fetch_days`` is the small
    # steady-state window merged into the persisted Rolling_History_Window;
    # ``reconciliation_interval_days`` is the cadence of the periodic FULL pull
    # that replaces the persisted history to self-heal drift.
    target_date_offset_days: int = 2
    trailing_fetch_days: int = 2
    reconciliation_interval_days: int = 7
    # LLM + email (Req 9)
    llm_model: str = "gemini-2.5-flash-lite"
    email_from: str = "ccip-alerts@localhost"
    email_to: str = "ccip-alerts@localhost"
    # Separate operations recipient for terminal failure-notification alerts
    # (Req 12.8). Blank => the failure alert dry-runs (log only, no send).
    email_failure_to: str = ""
    smtp_host: Optional[str] = None
    smtp_port: int = 587
    smtp_username: Optional[str] = None
    # Fetch retry: bounded exponential backoff with jitter applied ONLY to
    # transient (unreachable-source) Phase-1 fetch failures (Req 12.1-12.3).
    retry_max_attempts: int = 3
    retry_base_delay: float = 1.0
    retry_max_delay: float = 30.0
    retry_multiplier: float = 2.0
    retry_jitter: bool = True
    # Storage (Req 5.1). ``metadata_path`` is the legacy single-file path retained
    # for backward compatibility; increment 2 partitions the store per account
    # under ``metadata_dir`` via ``metadata_store.account_metadata_path`` (Req 14).
    metadata_path: str = "ccip_metadata.json"
    metadata_dir: str = "metadata"
    # Observability: structured-logging level (defaults to INFO) and the human
    # account name used to enrich contextual log payloads. ``account_name`` is
    # sourced from the shared ``AWS_ACCOUNT_NAME`` env value so detection logs can
    # carry it alongside ``account_id`` (the CcipState only tracks the id).
    log_level: str = "INFO"
    account_name: str = ""
    # Excluded from repr so secret values never leak (Req 1.5).
    _secrets: Optional[CcipSecrets] = field(default=None, repr=False)

    @property
    def secrets(self) -> Optional[CcipSecrets]:
        """The loaded LLM/SMTP secrets, or ``None`` if none were configured."""
        return self._secrets


def resolve_env_placeholders(text: str) -> tuple[str, list[str]]:
    """Substitute ``${VAR}`` / ``${VAR:-default}`` in a single string.

    Returns ``(resolved_text, missing_required)`` where ``missing_required`` lists,
    in order of first appearance, the names of placeholders that had neither an
    environment value nor a ``:-default`` fallback (Req 1.2, 1.3). A placeholder
    with an empty environment value AND no default is considered missing; one with
    an explicit (possibly empty) default resolves to that default.

    NOTE: this operates on a single value string, not the whole YAML document, so
    ``${...}`` text appearing in YAML comments is never seen here — resolution is
    applied only to parsed values via :func:`_resolve_in_data`.
    """
    missing: list[str] = []

    def _sub(match: re.Match[str]) -> str:
        name = match.group("name")
        default = match.group("default")  # None when no ``:-`` was present
        raw = os.environ.get(name)
        value = raw.strip() if isinstance(raw, str) else None
        if value:
            return value
        if default is not None:
            return default
        # No env value and no default => required-but-missing.
        if name not in missing:
            missing.append(name)
        return ""

    resolved = _PLACEHOLDER.sub(_sub, text)
    return resolved, missing


def _resolve_in_data(data: Any, missing: list[str]) -> Any:
    """Recursively resolve ``${VAR}`` placeholders in every string value.

    Walks the parsed YAML structure (dicts/lists/strings) and resolves
    placeholders only inside string *values* — comments were already stripped by
    the YAML parser, so a ``${VAR_NAME}`` example in a comment can never trigger a
    spurious "missing variable" error. Appends any required-but-missing variable
    names to ``missing`` (in first-seen order).
    """
    if isinstance(data, str):
        resolved, miss = resolve_env_placeholders(data)
        for name in miss:
            if name not in missing:
                missing.append(name)
        return resolved
    if isinstance(data, dict):
        return {key: _resolve_in_data(value, missing) for key, value in data.items()}
    if isinstance(data, list):
        return [_resolve_in_data(item, missing) for item in data]
    return data


def _as_bool(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _as_float(value: Any, default: float, key: str) -> float:
    if value is None or (isinstance(value, str) and not value.strip()):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        raise ConfigError(f"Invalid numeric configuration value for '{key}': {value!r}")


def _as_int(value: Any, default: int, key: str) -> int:
    if value is None or (isinstance(value, str) and not value.strip()):
        return default
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ConfigError(f"Invalid integer configuration value for '{key}': {value!r}")


def _opt_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def load_ccip_config(config_path: str = "config.yaml") -> CcipConfig:
    """Load ``config.yaml`` with ``${VAR}`` injection into an immutable config.

    Steps (Req 1.1, 1.2, 1.3, 1.5, 1.6):

    1. Load ``.env`` into the environment via ``python-dotenv``.
    2. Read + parse the YAML file; a missing/unreadable file or invalid YAML
       raises :class:`ConfigError` naming the file (Req 1.6).
    3. Resolve every ``${VAR}`` / ``${VAR:-default}`` found in a parsed *value*
       against the environment (comments are already stripped, so a ``${...}``
       example in a comment is never treated as a variable); a required unresolved
       placeholder raises :class:`ConfigError` naming the first offender (Req 1.3).
    4. Map values into :class:`CcipConfig`, pulling secrets onto :class:`CcipSecrets`
       (Req 1.5).
    """
    load_dotenv()

    try:
        with open(config_path, "r", encoding="utf-8") as handle:
            raw_text = handle.read()
    except FileNotFoundError as exc:
        raise ConfigError(f"configuration file not found: {config_path}") from exc
    except OSError as exc:
        raise ConfigError(f"could not read configuration file {config_path}: {exc}") from exc

    try:
        parsed = yaml.safe_load(raw_text) or {}
    except yaml.YAMLError as exc:
        raise ConfigError(f"invalid YAML in configuration file {config_path}: {exc}") from exc
    if not isinstance(parsed, dict):
        raise ConfigError(f"configuration file {config_path} must contain a mapping at the top level")

    # Resolve ${VAR} placeholders only within parsed VALUES (not comments).
    missing: list[str] = []
    data = _resolve_in_data(parsed, missing)
    if missing:
        raise ConfigError(
            "missing required environment value(s) referenced by "
            f"{config_path}: {missing[0]}"
        )

    detection = data.get("detection") or {}
    stl = data.get("stl") or {}
    ingestion = data.get("ingestion") or {}
    llm = data.get("llm") or {}
    email = data.get("email") or {}
    retry = data.get("retry") or {}
    storage = data.get("storage") or {}

    method = (_opt_str(detection.get("statistical_method")) or "iqr").lower()
    if method not in {"iqr", "sigma"}:
        raise ConfigError(
            f"detection.statistical_method must be 'iqr' or 'sigma', got {method!r}"
        )

    secrets = CcipSecrets(
        llm_api_key=_opt_str(llm.get("api_key")) or "",
        smtp_password=_opt_str(email.get("smtp_password")) or "",
    )

    return CcipConfig(
        cold_start_absolute_threshold=_as_float(
            detection.get("cold_start_absolute_threshold"), 20.0,
            "detection.cold_start_absolute_threshold",
        ),
        statistical_method=method,
        iqr_factor=_as_float(detection.get("iqr_factor"), 1.5, "detection.iqr_factor"),
        sigma_factor=_as_float(detection.get("sigma_factor"), 3.0, "detection.sigma_factor"),
        stl_period=_as_int(stl.get("period"), 7, "stl.period"),
        stl_seasonal=_as_int(stl.get("seasonal"), 7, "stl.seasonal"),
        stl_robust=_as_bool(stl.get("robust"), True),
        history_days=_as_int(ingestion.get("history_days"), 90, "ingestion.history_days"),
        target_date_offset_days=_as_int(
            ingestion.get("target_date_offset_days"), 2, "ingestion.target_date_offset_days"
        ),
        trailing_fetch_days=_as_int(
            ingestion.get("trailing_fetch_days"), 2, "ingestion.trailing_fetch_days"
        ),
        reconciliation_interval_days=_as_int(
            ingestion.get("reconciliation_interval_days"), 7,
            "ingestion.reconciliation_interval_days",
        ),
        llm_model=_opt_str(llm.get("model")) or "gemini-2.5-flash-lite",
        email_from=_opt_str(email.get("from")) or "ccip-alerts@localhost",
        email_to=_opt_str(email.get("to")) or "ccip-alerts@localhost",
        email_failure_to=_opt_str(email.get("failure_to")) or "",
        smtp_host=_opt_str(email.get("smtp_host")),
        smtp_port=_as_int(email.get("smtp_port"), 587, "email.smtp_port"),
        smtp_username=_opt_str(email.get("smtp_username")),
        retry_max_attempts=_as_int(retry.get("max_attempts"), 3, "retry.max_attempts"),
        retry_base_delay=_as_float(retry.get("base_delay"), 1.0, "retry.base_delay"),
        retry_max_delay=_as_float(retry.get("max_delay"), 30.0, "retry.max_delay"),
        retry_multiplier=_as_float(retry.get("multiplier"), 2.0, "retry.multiplier"),
        retry_jitter=_as_bool(retry.get("jitter"), True),
        metadata_path=_opt_str(storage.get("metadata_path")) or "ccip_metadata.json",
        metadata_dir=_opt_str(storage.get("metadata_dir")) or "metadata",
        log_level=_opt_str(os.environ.get("LOG_LEVEL")) or "INFO",
        account_name=_opt_str(os.environ.get("AWS_ACCOUNT_NAME")) or "",
        _secrets=secrets,
    )
