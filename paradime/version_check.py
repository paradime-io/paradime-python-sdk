import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

from paradime.version import get_sdk_version

logger = logging.getLogger(__name__)

_PYPI_URL = "https://pypi.org/pypi/paradime-io/json"
_CACHE_TTL_SECONDS = 24 * 60 * 60
_NETWORK_TIMEOUT_SECONDS = 2.0
_DISABLE_ENV_VAR = "PARADIME_DISABLE_VERSION_CHECK"

_already_checked_this_process = False


def _cache_path() -> Path:
    return Path.home() / ".paradime" / "version_check.json"


def _read_cached_latest_version() -> Optional[str]:
    try:
        with _cache_path().open("r") as f:
            cache = json.load(f)
        if not isinstance(cache, dict):
            return None
        checked_at = cache.get("checked_at")
        latest = cache.get("latest_version")
        if not isinstance(checked_at, (int, float)) or not isinstance(latest, str):
            return None
        if time.time() - checked_at > _CACHE_TTL_SECONDS:
            return None
        return latest
    except (OSError, ValueError):
        return None


def _write_cached_latest_version(latest_version: str) -> None:
    path = _cache_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w") as f:
            json.dump({"checked_at": time.time(), "latest_version": latest_version}, f)
    except OSError:
        pass


def _fetch_latest_version_from_pypi() -> Optional[str]:
    import requests

    response = requests.get(_PYPI_URL, timeout=_NETWORK_TIMEOUT_SECONDS)
    response.raise_for_status()
    data = response.json()
    version = data.get("info", {}).get("version")
    if isinstance(version, str) and version:
        return version
    return None


def _get_latest_version() -> Optional[str]:
    cached = _read_cached_latest_version()
    if cached is not None:
        return cached
    try:
        latest = _fetch_latest_version_from_pypi()
    except Exception as e:
        logger.debug("Paradime SDK version check failed: %s", e)
        return None
    if latest is not None:
        _write_cached_latest_version(latest)
    return latest


def _is_newer(latest: str, current: str) -> bool:
    try:
        from packaging.version import InvalidVersion, parse

        try:
            return parse(latest) > parse(current)
        except InvalidVersion:
            return False
    except Exception:
        return False


def _emit_upgrade_notice(current: str, latest: str) -> None:
    message = (
        f"A new version of the Paradime SDK is available: {current} -> {latest}. "
        f"Upgrade with: pip install --upgrade paradime-io"
    )
    print(f"⚠ {message}", file=sys.stderr)


def check_for_new_version() -> None:
    """
    Check PyPI for a newer release of paradime-io and emit a notice if one is available.

    Runs at most once per process. Results are cached on disk for 24 hours so we do
    not hit PyPI on every invocation. Set PARADIME_DISABLE_VERSION_CHECK=1 to disable.
    All network and filesystem errors are swallowed so this never breaks user code.
    """
    global _already_checked_this_process
    if _already_checked_this_process:
        return
    _already_checked_this_process = True

    if os.environ.get(_DISABLE_ENV_VAR, "").lower() in ("1", "true", "yes"):
        return

    try:
        current = get_sdk_version()
        if current == "N/A":
            return
        latest = _get_latest_version()
        if latest is None:
            return
        if _is_newer(latest, current):
            _emit_upgrade_notice(current=current, latest=latest)
    except Exception as e:
        logger.debug("Paradime SDK version check failed: %s", e)
