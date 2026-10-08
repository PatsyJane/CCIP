"""Config loader (design component 2).

Loads ``.env`` via ``python-dotenv`` and validates required keys, treating
empty/whitespace-only values as missing. Exposes an immutable ``AppConfig`` and
raises ``ConfigError`` naming the first missing/blank required key.

The Env_Config contract (see design "Config Loader" and "Config Model"):

===================================  ==============================  ========
Env key                              Maps to                         Required
===================================  ==============================  ========
``AWS_ACCOUNT_ID``                   ``AppConfig.aws_account_id``    Yes
``AWS_ACCOUNT_NAME``                 ``AppConfig.aws_account_name``  Yes
``AWS_REGION``                       ``AppConfig.aws_region``        Yes
``COST_API_BASE_URL``                ``AppConfig.cost_api_base_url`` Yes
``PROVIDER``                         ``AppConfig.provider``          Yes
``PROJECT``                          ``AppConfig.project``           Yes
``AWS_ACCESS_KEY_ID`` /              ``AppConfig._credentials``      Only where
``AWS_SECRET_ACCESS_KEY``                                            required
===================================  ==============================  ========

Requirements: 1.5, 12.1, 12.2, 12.3, 12.4, 12.6, 12.7.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from dotenv import load_dotenv

import os

__all__ = ["AppConfig", "AwsCredentials", "ConfigError", "load_config"]


class ConfigError(Exception):
    """Raised when the Env_Config is missing or invalid.

    The message names the first missing/blank required key so startup can
    abort with an error that identifies the offending configuration value
    (Requirements 1.5, 12.4).
    """


@dataclass(frozen=True)
class AwsCredentials:
    """AWS credentials loaded from the Env_Config.

    These are only loaded WHERE credentials are required to reach the Cost_API
    (Requirement 12.3). The credential values must never appear in tool output
    or log entries (Requirements 12.6, 12.7); ``AppConfig`` stores an instance
    of this model on a field excluded from ``__repr__`` and ``repr`` here is
    overridden so the secret values are not exposed even if an instance is
    logged directly.
    """

    access_key_id: str
    secret_access_key: str

    def __repr__(self) -> str:  # pragma: no cover - trivial redaction
        # Never expose the credential values (Req 12.6, 12.7).
        return f"{type(self).__name__}(access_key_id=***, secret_access_key=***)"


@dataclass(frozen=True)
class AppConfig:
    """Immutable application configuration loaded from the Env_Config.

    The six string fields are all required (Requirements 12.1, 12.2). AWS
    credentials, when present, are stored on ``_credentials`` which is excluded
    from ``__repr__`` so credential values are not exposed when the config is
    logged or serialized (Requirements 12.6, 12.7).
    """

    aws_account_id: str
    aws_account_name: str
    aws_region: str
    cost_api_base_url: str
    provider: str
    project: str
    # Excluded from repr so credential values never leak into logs/output.
    _credentials: Optional[AwsCredentials] = field(default=None, repr=False)

    @property
    def credentials(self) -> Optional[AwsCredentials]:
        """The loaded AWS credentials, or ``None`` if none were configured."""
        return self._credentials


# Env keys for the six required values, in the order they are validated.
_REQUIRED_KEYS: tuple[str, ...] = (
    "AWS_ACCOUNT_ID",
    "AWS_ACCOUNT_NAME",
    "AWS_REGION",
    "COST_API_BASE_URL",
    "PROVIDER",
    "PROJECT",
)

# Optional AWS credential keys (Req 12.3).
_ACCESS_KEY_ID = "AWS_ACCESS_KEY_ID"
_SECRET_ACCESS_KEY = "AWS_SECRET_ACCESS_KEY"


def _clean(value: Optional[str]) -> Optional[str]:
    """Return the trimmed value, or ``None`` if it is absent or blank.

    Empty and whitespace-only values are treated as missing (Req 12.4).
    """
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


def load_config() -> AppConfig:
    """Load and validate the Env_Config, returning an immutable ``AppConfig``.

    Loads ``.env`` via ``python-dotenv`` and reads the required keys from the
    environment. Empty or whitespace-only required values are treated as
    missing. Raises :class:`ConfigError` naming the first missing/blank
    required key (Requirements 1.5, 12.4).

    AWS credentials are loaded only where required to reach the Cost_API
    (Requirement 12.3): when both credential keys are present and non-blank an
    :class:`AwsCredentials` is attached; otherwise credentials are ``None``.
    """
    # Load .env into the process environment (does not override existing vars).
    load_dotenv()

    values: dict[str, str] = {}
    for key in _REQUIRED_KEYS:
        cleaned = _clean(os.environ.get(key))
        if cleaned is None:
            raise ConfigError(f"Missing required configuration value: {key}")
        values[key] = cleaned

    credentials = _load_credentials()

    return AppConfig(
        aws_account_id=values["AWS_ACCOUNT_ID"],
        aws_account_name=values["AWS_ACCOUNT_NAME"],
        aws_region=values["AWS_REGION"],
        cost_api_base_url=values["COST_API_BASE_URL"],
        provider=values["PROVIDER"],
        project=values["PROJECT"],
        _credentials=credentials,
    )


def _load_credentials() -> Optional[AwsCredentials]:
    """Load AWS credentials from the environment where required (Req 12.3).

    Credentials are optional for the PoC: they are loaded only when both the
    access key id and secret access key are present and non-blank. If only one
    is provided, that is a misconfiguration and a :class:`ConfigError` is
    raised naming the missing counterpart.
    """
    access_key_id = _clean(os.environ.get(_ACCESS_KEY_ID))
    secret_access_key = _clean(os.environ.get(_SECRET_ACCESS_KEY))

    if access_key_id is None and secret_access_key is None:
        # No credentials configured — allowed (Req 12.3: only where required).
        return None

    if access_key_id is None:
        raise ConfigError(
            f"Missing required configuration value: {_ACCESS_KEY_ID}"
        )
    if secret_access_key is None:
        raise ConfigError(
            f"Missing required configuration value: {_SECRET_ACCESS_KEY}"
        )

    return AwsCredentials(
        access_key_id=access_key_id,
        secret_access_key=secret_access_key,
    )
