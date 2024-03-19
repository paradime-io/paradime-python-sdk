import logging
import os
import sys
from pathlib import Path

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
    for env_var in ["API_ENDPOINT", "API_KEY", "API_SECRET"]:
        if os.getenv(env_var) is None:
            raise ValueError(
                f"{env_var} environment variable is not set! To fix this either: \n 1. Export the environment variable (export {env_var}=...) or, \n 2. Use the `paradime login` command to set the API credentials locally."
            )

    return Paradime(
        api_endpoint=os.getenv("API_ENDPOINT", ""),
        api_key=os.getenv("API_KEY", ""),
        api_secret=os.getenv("API_SECRET", ""),
    )
