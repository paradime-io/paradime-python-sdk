import logging
from typing import Any, Callable, Optional, TypeVar

from paradime.client.api_exception import ParadimeRateLimitedException

logger = logging.getLogger(__name__)

DEFAULT_MAX_RATE_LIMIT_RETRIES = 3
DEFAULT_MAX_RETRY_WAIT_SECONDS = 60.0
DEFAULT_RETRY_WAIT_SECONDS = 1.0

T = TypeVar("T")


def parse_retry_after(value: Any) -> Optional[float]:
    try:
        seconds = float(value)
    except (TypeError, ValueError):
        return None
    return seconds if seconds >= 0 else None


def retry_rate_limited(
    send: Callable[[], T],
    *,
    max_rate_limit_retries: int,
    max_retry_wait: float,
    sleep: Callable[[float], None],
) -> T:
    retries = 0
    while True:
        try:
            return send()
        except ParadimeRateLimitedException as e:
            wait = e.retry_after if e.retry_after is not None else DEFAULT_RETRY_WAIT_SECONDS
            if retries >= max_rate_limit_retries or wait > max_retry_wait:
                raise
            retries += 1
            logger.info(
                f"Rate limited by the Paradime API; retrying in {wait:g}s"
                f" ({retries}/{max_rate_limit_retries})."
            )
            sleep(wait)
