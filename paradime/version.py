import logging

logger = logging.getLogger(__name__)


def get_sdk_version() -> str:
    """
    Get the version of the Paradime SDK.

    Returns:
        str: The version of the Paradime SDK.
    """

    try:
        import importlib.metadata

        return importlib.metadata.version("paradime-io")
    except Exception as e:
        logger.error(e)
        return "N/A"
