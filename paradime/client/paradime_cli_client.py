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
    It is recommended to use `PARADIME_` prefixed environment variables to set the API credentials.
    The ones without the prefix are deprecated and present for backward compatibility only.

    `PARADIME_API_SECRET` may be a legacy API secret (used together with `PARADIME_API_KEY`)
    or a bearer token (workspace-level `prdm_wsp_...` or company-level `prdm_cmp_...`) — the
    right auth mechanism is detected automatically.
    """

    return Paradime(
        api_endpoint=get_env_var_from_aliases(["PARADIME_API_ENDPOINT", "API_ENDPOINT"]),
        api_key=get_optional_env_var_from_aliases(["PARADIME_API_KEY", "API_KEY"]),
        api_secret=get_env_var_from_aliases(["PARADIME_API_SECRET", "API_SECRET"]),
        workspace_uid=get_optional_env_var_from_aliases(
            ["PARADIME_WORKSPACE_UID", "WORKSPACE_UID"]
        ),
    )


def get_env_var_from_aliases(
    env_var_aliases: List[str],
) -> str:
    """
    Go through the list of environment variable aliases and return the first one that is set.
    If none are set, raise an error.
    """

    value = get_optional_env_var_from_aliases(env_var_aliases)
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
    If none are set, return None.
    """

    for env_var in env_var_aliases:
        value = os.getenv(env_var)
        if value:
            return value

    return None
