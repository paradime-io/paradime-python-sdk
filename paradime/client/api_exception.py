from typing import Any, Dict, List, Optional


class ParadimeException(Exception):
    """
    Base exception for the Paradime API client.

    Every exception raised by the SDK derives from this, so
    ``except ParadimeException`` catches everything the SDK can raise.
    """

    pass


class ParadimeAPIException(ParadimeException):
    """
    Exception for errors in the Paradime API.

    Base class for every failure that reached the API and came back as an error
    response. The subclasses below narrow this by HTTP status so callers can
    branch on the failure kind instead of matching on the message text.

    Attributes:
        status_code (int, optional): The HTTP status code of the response, when the
            failure came from an HTTP status. ``None`` for GraphQL body errors,
            which are returned with a 200.
        errors (list, optional): The raw GraphQL ``errors`` array, when the failure
            came from a GraphQL response body.
        response_text (str, optional): The raw response body, for diagnostics.
        request_id (str, optional): The value of the response's request-id header,
            when present. Quote this to Paradime support.
    """

    def __init__(
        self,
        message: str,
        *,
        status_code: Optional[int] = None,
        errors: Optional[List[Dict[str, Any]]] = None,
        response_text: Optional[str] = None,
        request_id: Optional[str] = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.errors = errors
        self.response_text = response_text
        self.request_id = request_id


class ParadimeAuthException(ParadimeAPIException):
    """Raised on 401/403 — the credentials are missing, invalid, or lack permission."""

    pass


class ParadimeNotFoundException(ParadimeAPIException):
    """Raised on 404 — the endpoint or resource does not exist."""

    pass


class ParadimeRateLimitException(ParadimeAPIException):
    """
    Raised on 429 — too many requests.

    Attributes:
        retry_after (float, optional): Seconds to wait before retrying, parsed from
            the ``Retry-After`` response header when the server sends one.
    """

    def __init__(self, message: str, *, retry_after: Optional[float] = None, **kwargs: Any) -> None:
        super().__init__(message, **kwargs)
        self.retry_after = retry_after


class ParadimeServerException(ParadimeAPIException):
    """Raised on 5xx — the Paradime API failed to handle an otherwise valid request."""

    pass


class ParadimeConnectionError(ParadimeException):
    """Raised when the request never reached the API (DNS, TLS, refused connection)."""

    pass


class ParadimeTimeoutError(ParadimeException):
    """Raised when the request did not complete within the client's timeout."""

    pass
