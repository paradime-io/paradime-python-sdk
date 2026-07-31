from typing import Any, Dict, List, Optional

import requests
from tenacity import Retrying, retry_if_exception, stop_after_attempt, wait_exponential

from paradime.client.api_exception import (
    ParadimeAPIException,
    ParadimeAuthException,
    ParadimeConnectionError,
    ParadimeNotFoundException,
    ParadimeRateLimitException,
    ParadimeServerException,
    ParadimeTimeoutError,
)
from paradime.client.runtime import detect_runtime, get_python_version, is_telemetry_enabled
from paradime.version import get_sdk_version

# Prefixes used by the bearer-token style API secrets. A company token is valid across a
# set of workspaces, so every request must select its target workspace via the
# X-Paradime-Workspace header. A workspace token is scoped to a single workspace already,
# same as a legacy key/secret pair.
COMPANY_API_TOKEN_PREFIX = "prdm_cmp_"
WORKSPACE_API_TOKEN_PREFIX = "prdm_wsp_"

WORKSPACE_SELECTION_HEADER = "X-Paradime-Workspace"

DEFAULT_TIMEOUT_SECONDS = 60
DEFAULT_MAX_RETRIES = 3

# Response headers the API may use to carry a request id, in preference order.
_REQUEST_ID_HEADERS = ("X-Request-Id", "X-Request-ID", "Request-Id")

# Every SDK operation is a POST, so blanket 5xx retries would risk re-running
# mutations. Only statuses the server uses to mean "try again" are retried.
RETRYABLE_STATUS_CODES = frozenset({429, 502, 503, 504})


def _is_bearer_token(api_secret: str) -> bool:
    """Return True if `api_secret` is actually a bearer token rather than a legacy secret."""

    return api_secret.startswith(COMPANY_API_TOKEN_PREFIX) or api_secret.startswith(
        WORKSPACE_API_TOKEN_PREFIX
    )


def _parse_retry_after(response: requests.Response) -> Optional[float]:
    """Parse the Retry-After header as seconds, ignoring HTTP-date form."""

    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _request_id(response: requests.Response) -> Optional[str]:
    for header in _REQUEST_ID_HEADERS:
        value = response.headers.get(header)
        if value:
            return value
    return None


def _is_retryable(exception: BaseException) -> bool:
    """Retry transport failures and the statuses that mean 'try again'."""

    if isinstance(exception, (ParadimeConnectionError, ParadimeTimeoutError)):
        return True
    if isinstance(exception, ParadimeAPIException):
        return exception.status_code in RETRYABLE_STATUS_CODES
    return False


class APIClient:
    """
    A client for making API requests to the Paradime API.

    `api_secret` accepts either a legacy API secret (used together with `api_key`), or a
    bearer token generated from Paradime account settings: a workspace-level token
    (`prdm_wsp_...`) or a company-level token (`prdm_cmp_...`). The right auth mechanism is
    detected automatically from the `api_secret` prefix.

    - Legacy secret: `api_key` must also be provided.
    - Workspace token (`prdm_wsp_...`): `api_key` is not needed.
    - Company token (`prdm_cmp_...`): `api_key` is not needed, but `workspace_uid` must be
      provided to select which workspace the requests should target.

    Args:
        api_key (str, optional): The API key for authentication. Required when `api_secret`
            is a legacy secret; not needed when `api_secret` is a bearer token.
        api_secret (str): The API secret or bearer token for authentication.
        workspace_uid (str, optional): The workspace uid to target. Required when
            `api_secret` is a company-level (`prdm_cmp_`) token; not used otherwise.
        api_endpoint (str): The endpoint URL for the API.
        timeout (int, optional): The timeout for API requests in seconds. Defaults to 60 seconds.
        max_retries (int, optional): How many times to attempt a request before giving up.
            Retries cover connection errors, timeouts, and the 429/502/503/504 statuses,
            with exponential backoff. Set to 1 to disable retries. Defaults to 3.
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        api_secret: str,
        workspace_uid: Optional[str] = None,
        api_endpoint: str,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
    ):
        if _is_bearer_token(api_secret):
            if api_secret.startswith(COMPANY_API_TOKEN_PREFIX) and not workspace_uid:
                raise ValueError(
                    "workspace_uid is required when authenticating with a company-level "
                    f"API token (one that starts with {COMPANY_API_TOKEN_PREFIX!r})."
                )
        elif not api_key:
            raise ValueError(
                "api_key is required when api_secret is a legacy API secret (i.e. does not "
                f"start with {WORKSPACE_API_TOKEN_PREFIX!r} or {COMPANY_API_TOKEN_PREFIX!r})."
            )

        if max_retries < 1:
            raise ValueError(f"max_retries must be >= 1, got {max_retries}")

        self.api_key = api_key
        self.api_secret = api_secret
        self.workspace_uid = workspace_uid
        self.api_endpoint = api_endpoint
        self.timeout = timeout
        self.max_retries = max_retries

        # One session for the client's lifetime, so connections are pooled across calls
        # rather than reopened per request.
        self.session = requests.Session()

    def close(self) -> None:
        """Release the pooled connections held by the underlying session."""

        self.session.close()

    def __enter__(self) -> "APIClient":
        return self

    def __exit__(self, *exc_info: Any) -> None:
        self.close()

    def _get_request_headers(self) -> Dict[str, str]:
        """
        Get the request headers for Paradime API requests.

        Returns:
            dict: The request headers.
        """

        headers: Dict[str, str] = {
            "Content-Type": "application/json",
            "X-PYTHON-SDK-VERSION": get_sdk_version(),
        }

        if _is_bearer_token(self.api_secret):
            auth_scheme = "Bearer"
            headers["Authorization"] = auth_scheme + " " + self.api_secret
        else:
            # Legacy workspace-level key/secret auth. api_key is guaranteed to be set here
            # (validated in __init__ when api_secret is not a bearer token).
            assert self.api_key
            headers["X-API-KEY"] = self.api_key
            headers["X-API-SECRET"] = self.api_secret

        if self.workspace_uid:
            headers[WORKSPACE_SELECTION_HEADER] = self.workspace_uid

        if is_telemetry_enabled():
            headers["X-PYTHON-VERSION"] = get_python_version()
            headers["X-PARADIME-RUNTIME"] = detect_runtime()

        return headers

    def _raise_for_gql_response_body_errors(self, response: requests.Response) -> None:
        """
        Raise an exception for GraphQL response body errors.

        Args:
            response (requests.Response): The API response.

        Raises:
            ParadimeAPIException: If there are errors in the response body, or if the
                body is not valid JSON.
        """

        try:
            response_json = response.json()
        except ValueError as e:
            raise ParadimeAPIException(
                f"The API returned a {response.status_code} response that is not valid JSON.",
                status_code=response.status_code,
                response_text=response.text,
                request_id=_request_id(response),
            ) from e

        if isinstance(response_json, dict) and "errors" in response_json:
            error_message = self._get_error_message_from_response(response_json)
            raise ParadimeAPIException(
                error_message,
                status_code=response.status_code,
                errors=response_json.get("errors"),
                response_text=response.text,
                request_id=_request_id(response),
            )

    def _get_error_message_from_response(self, response: Dict[str, Any]) -> str:
        try:
            return response["errors"][0]["message"]
        except Exception:
            return str(response["errors"])

    def _raise_for_response_status_errors(self, response: requests.Response) -> None:
        """
        Raise the exception matching the response's HTTP status.

        Args:
            response (requests.Response): The API response.

        Raises:
            ParadimeAuthException: On 401 and 403.
            ParadimeNotFoundException: On 404.
            ParadimeRateLimitException: On 429.
            ParadimeServerException: On 5xx.
            ParadimeAPIException: On any other error status.
        """

        if response.ok:
            return

        status = response.status_code
        message = f"Error: {status} - {response.text}"
        details: Dict[str, Any] = {
            "status_code": status,
            "response_text": response.text,
            "request_id": _request_id(response),
        }

        if status in (401, 403):
            raise ParadimeAuthException(message, **details)
        if status == 404:
            raise ParadimeNotFoundException(message, **details)
        if status == 429:
            raise ParadimeRateLimitException(
                message, retry_after=_parse_retry_after(response), **details
            )
        if status >= 500:
            raise ParadimeServerException(message, **details)
        raise ParadimeAPIException(message, **details)

    def _raise_for_errors(self, response: requests.Response) -> None:
        """
        Raise an exception for any errors in the API response.

        Args:
            response (requests.Response): The API response.

        Raises:
            ParadimeAPIException: If there are errors in the API response.
        """

        self._raise_for_response_status_errors(response)
        self._raise_for_gql_response_body_errors(response)

    def _post_gql(self, query: str, variables: Dict[str, Any]) -> Dict[str, Any]:
        """Issue one GraphQL request, translating transport failures into SDK exceptions."""

        try:
            response = self.session.post(
                url=self.api_endpoint,
                json={"query": query, "variables": variables},
                headers=self._get_request_headers(),
                timeout=self.timeout,
            )
        except requests.Timeout as e:
            raise ParadimeTimeoutError(
                f"Request to {self.api_endpoint} timed out after {self.timeout}s."
            ) from e
        except requests.RequestException as e:
            raise ParadimeConnectionError(
                f"Could not reach the Paradime API at {self.api_endpoint}: {e}"
            ) from e

        self._raise_for_errors(response)

        return response.json()["data"]

    def execute(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Make a GraphQL API request, retrying transient failures.

        Args:
            query (str): The GraphQL query.
            variables (dict, optional): The variables for the query. Defaults to {}.

        Returns:
            dict: The response data from the API.

        Raises:
            ParadimeAPIException: If the API returns an error response.
            ParadimeConnectionError: If the API could not be reached.
            ParadimeTimeoutError: If the request timed out.
        """

        retrying = Retrying(
            stop=stop_after_attempt(self.max_retries),
            wait=wait_exponential(multiplier=1, min=1, max=30),
            retry=retry_if_exception(_is_retryable),
            reraise=True,
        )

        return retrying(self._post_gql, query, variables or {})

    def _call_gql(self, query: str, variables: Dict[str, Any] = {}) -> Dict[str, Any]:
        """Deprecated internal alias for :meth:`execute`."""

        return self.execute(query, variables)


__all__: List[str] = [
    "APIClient",
    "COMPANY_API_TOKEN_PREFIX",
    "DEFAULT_MAX_RETRIES",
    "DEFAULT_TIMEOUT_SECONDS",
    "RETRYABLE_STATUS_CODES",
    "WORKSPACE_API_TOKEN_PREFIX",
    "WORKSPACE_SELECTION_HEADER",
]
