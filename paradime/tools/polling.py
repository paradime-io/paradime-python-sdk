"""A single polling loop shared by the SDK's blocking `*_and_wait` helpers."""

import time
from typing import Callable, Optional, TypeVar

T = TypeVar("T")

DEFAULT_POLL_INTERVAL_SECONDS = 10.0


def poll_until(
    fetch: Callable[[], T],
    is_done: Callable[[T], bool],
    *,
    timeout: float,
    interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    on_poll: Optional[Callable[[T], None]] = None,
    timeout_message: Callable[[T], str] = lambda state: f"Timed out. Last state: {state}",
) -> T:
    """Call ``fetch`` every ``interval`` seconds until ``is_done`` returns True.

    The deadline is measured with a monotonic clock, so a system clock change
    mid-wait cannot extend or cut short the wait.

    Args:
        fetch: Retrieves the current state. Called at least once.
        is_done: Returns True when polling should stop and the state be returned.
        timeout: Maximum seconds to wait before raising ``TimeoutError``.
        interval: Seconds to sleep between polls.
        on_poll: Called with each non-terminal state, for progress logging.
        timeout_message: Builds the ``TimeoutError`` message from the last state.

    Returns:
        The first state for which ``is_done`` returned True.

    Raises:
        TimeoutError: If ``timeout`` elapses before ``is_done`` returns True.
    """

    deadline = time.monotonic() + timeout

    while True:
        state = fetch()

        if is_done(state):
            return state

        if time.monotonic() >= deadline:
            raise TimeoutError(timeout_message(state))

        if on_poll is not None:
            on_poll(state)

        time.sleep(interval)
