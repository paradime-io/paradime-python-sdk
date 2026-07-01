import logging
import os
import sys
from pathlib import Path
from typing import List, Optional

from paradime.client.paradime_client import Paradime

logger = logging.getLogger(__name__)


def get_credentials_path() -> Path:
    return Path.home() / ".paradime" / "credentials"


def get_cli_client_or_exit() -> Paradime:
    try:
        return get_cli_client()
    except ValueError as e:
        logger.error(e)
        sys.exit(1)


def get_cli_client() -> Paradime:
    """
    Build a Paradime client from environment variables.

    Supported authentication modes:

    1. **Company-level** (preferred): Set ``PARADIME_API_TOKEN`` (and optionally
       ``PARADIME_WORKSPACE_UID``) along with ``PARADIME_API_ENDPOINT``.
    2. **Workspace-level** (legacy): Set ``PARADIME_API_KEY`` and ``PARADIME_API_SECRET``
       along with ``PARADIME_API_ENDPOINT``.

    The ``PARADIME_`` prefix is recommended; un-prefixed aliases are kept for backward
    compatibility.
    """

    api_endpoint = get_env_var_from_aliases(["PARADIME_API_ENDPOINT", "API_ENDPOINT"])

    api_token = get_optional_env_var_from_aliases(["PARADIME_API_TOKEN"])

    if api_token:
        workspace_uid = get_optional_env_var_from_aliases(["PARADIME_WORKSPACE_UID"])
        return Paradime(
            api_endpoint=api_endpoint,
            api_token=api_token,
            workspace_uid=workspace_uid,
        )

    return Paradime(
        api_endpoint=api_endpoint,
        api_key=get_env_var_from_aliases(["PARADIME_API_KEY", "API_KEY"]),
        api_secret=get_env_var_from_aliases(["PARADIME_API_SECRET", "API_SECRET"]),
    )


def get_env_var_from_aliases(
    env_var_aliases: List[str],
) -> str:
    """
    Go through the list of environment variable aliases and return the first one that is set.
    If none are set, raise an error.
    """

    for env_var in env_var_aliases:
        value = os.getenv(env_var)
        if value:
            return value

    raise ValueError(
        f"{env_var_aliases[0]} environment variable is not set! To fix this either: \n"
        f" 1. Export the environment variable (export {env_var_aliases[0]}=...) or, \n"
        f" 2. Use the `paradime login` command to set the API credentials locally."
    )


def get_optional_env_var_from_aliases(
    env_var_aliases: List[str],
) -> Optional[str]:
    """
    Go through the list of environment variable aliases and return the first one that is set.
    Returns None if none are set.
    """

    for env_var in env_var_aliases:
        value = os.getenv(env_var)
        if value:
            return value

    return None
