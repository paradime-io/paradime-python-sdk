from typing import Any, Optional


class ParadimeException(Exception):
    """
    Base exception for the Paradime API client.
    """

    pass


class ParadimeAPIException(ParadimeException):
    """
    Exception for errors in the Paradime API.

    Attributes:
        status_code (int, optional): The HTTP status of the response, when the error is an
            HTTP error status; None otherwise.
        response_text (str, optional): The body of that response, with the company token
            replaced by ``***``; None otherwise.
    """

    def __init__(
        self,
        *args: Any,
        status_code: Optional[int] = None,
        response_text: Optional[str] = None,
    ):
        super().__init__(*args)
        self.status_code = status_code
        self.response_text = response_text

    def __reduce__(self) -> Any:
        return (type(self), self.args, self.__dict__)


class ParadimeRateLimitedException(ParadimeAPIException):
    """
    The Paradime API refused the request for its rate limit, after the SDK's retries.
    ``retry_after`` is how many seconds to wait, when the server said.
    """

    def __init__(self, message: str, *, retry_after: Optional[float] = None, **kwargs: Any):
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class ParadimeWorkspaceRequiredException(ParadimeAPIException):
    """
    A workspace-level call was made with a company-level API token and no `workspace_uid`.
    """

    pass
