from typing import Any, Optional


class ParadimeException(Exception):
    """
    Base exception for the Paradime API client.
    """

    pass


class ParadimeAPIException(ParadimeException):
    """
    Exception for errors in the Paradime API.
    """

    pass


class ParadimeRateLimitedException(ParadimeAPIException):
    """
    The Paradime API refused the request for its rate limit, after the SDK's retries.
    ``retry_after`` is how many seconds to wait, when the server said.
    """

    def __init__(self, message: str, *, retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after

    def __reduce__(self) -> Any:
        return (type(self), self.args, self.__dict__)


class ParadimeWorkspaceRequiredException(ParadimeAPIException):
    """
    A workspace-level call was made with a company-level API token and no `workspace_uid`.
    """

    pass
