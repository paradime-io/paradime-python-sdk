import time
from typing import Any, Callable, Collection, Dict, List, Optional

import requests

from paradime.apis.admin.exception import (
    AdminConnectionException,
    AdminRateLimitedException,
    ErrorCode,
    admin_exception_from_errors,
    parse_error_code,
)
from paradime.client.api_client import WORKSPACE_SELECTION_HEADER, APIClient
from paradime.client.api_exception import ParadimeAPIException
from paradime.client.rate_limit import parse_retry_after, retry_rate_limited
from paradime.client.redaction import (
    COMPANY_TOKEN_PATH,
    redact_company_token,
    redact_endpoint,
    send_redacted,
)

UNREADABLE_LINK_CODES = frozenset({ErrorCode.MISSING_CAPABILITY, ErrorCode.PLAN_UPGRADE_REQUIRED})


def _connection_error(endpoint: str, error: requests.RequestException) -> AdminConnectionException:
    reason = type(error).__name__
    if COMPANY_TOKEN_PATH.search(endpoint):
        reason = f"{reason}: {redact_company_token(str(error), endpoint)}"
    return AdminConnectionException(
        f"Could not reach the Paradime API at {redact_endpoint(endpoint)} ({reason})."
    )


class AdminTransport:
    """
    Sends Admin API requests through an `APIClient`'s endpoint and credentials.

    A request the server refuses with `RATE_LIMITED` (HTTP 429) is retried after the
    `retryAfter` the server sends, up to `max_rate_limit_retries` times; then, or at once when
    the server asks for a longer wait than `max_retry_wait`, the `AdminRateLimitedException`
    is raised. Any other error raises the typed exception of its
    `extensions.code`, except a linked object the key can't read: see `execute`.

    Args:
        client (APIClient): The client whose endpoint, credentials, timeout and rate limit
            retry settings to use, read on every request.
        max_rate_limit_retries (int, optional): How many times to retry a rate-limited
            request, in place of the client's current setting.
        max_retry_wait (float, optional): The longest wait to retry after, in seconds, in
            place of the client's current setting.
        sleep (Callable[[float], None]): How to wait between retries.
    """

    def __init__(
        self,
        client: APIClient,
        *,
        max_rate_limit_retries: Optional[int] = None,
        max_retry_wait: Optional[float] = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.client = client
        self._max_rate_limit_retries = max_rate_limit_retries
        self._max_retry_wait = max_retry_wait
        self.sleep = sleep

    @property
    def max_rate_limit_retries(self) -> int:
        if self._max_rate_limit_retries is None:
            return self.client.max_rate_limit_retries
        return self._max_rate_limit_retries

    @max_rate_limit_retries.setter
    def max_rate_limit_retries(self, value: Optional[int]) -> None:
        self._max_rate_limit_retries = value

    @property
    def max_retry_wait(self) -> float:
        if self._max_retry_wait is None:
            return self.client.max_retry_wait
        return self._max_retry_wait

    @max_retry_wait.setter
    def max_retry_wait(self, value: Optional[float]) -> None:
        self._max_retry_wait = value

    def execute(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        *,
        workspace_uid: Optional[str] = None,
        linked_fields: Collection[str] = (),
    ) -> Dict[str, Any]:
        """
        Runs a GraphQL query or mutation and returns its `data`.

        Args:
            query (str): The GraphQL document.
            variables (dict, optional): Its variables.
            workspace_uid (str, optional): The workspace to send in `X-Paradime-Workspace` for
                this request, in place of the client's.
            linked_fields (Collection[str]): Fields of the document that link another object.
                A `MISSING_CAPABILITY` or `PLAN_UPGRADE_REQUIRED` error on one of them, when the
                field reads null and the object holding it was read, leaves the link None in
                place of raising: the key can read the object but not the one it links to.

        Raises:
            AdminApiException: The typed exception of the first error's `extensions.code`.
            ParadimeAPIException: For an HTTP error without a GraphQL error body.
            AdminConnectionException: When the API can't be reached; the message keeps the
                company token out.
        """

        return retry_rate_limited(
            lambda: self._execute_once(
                query, variables or {}, workspace_uid, frozenset(linked_fields)
            ),
            max_rate_limit_retries=self.max_rate_limit_retries,
            max_retry_wait=self.max_retry_wait,
            sleep=self.sleep,
        )

    def _headers(self, workspace_uid: Optional[str]) -> Dict[str, str]:
        headers = self.client._get_request_headers()
        if workspace_uid:
            headers[WORKSPACE_SELECTION_HEADER] = workspace_uid
        return headers

    def _execute_once(
        self,
        query: str,
        variables: Dict[str, Any],
        workspace_uid: Optional[str],
        linked_fields: Collection[str],
    ) -> Dict[str, Any]:
        response = send_redacted(
            lambda: requests.post(
                url=self.client.api_endpoint,
                json={"query": query, "variables": variables},
                headers=self._headers(workspace_uid),
                timeout=self.client.timeout,
            ),
            lambda e: _connection_error(self.client.api_endpoint, e),
        )
        retry_after_header = response.headers.get("Retry-After")
        body = _json_body(response)
        errors = body.get("errors") if isinstance(body, dict) else None
        if errors and linked_fields:
            errors = _without_unreadable_links(errors, body.get("data"), linked_fields)
        if errors:
            raise admin_exception_from_errors(errors, retry_after_header=retry_after_header)
        response_text = redact_company_token(response.text, self.client.api_endpoint)
        if response.status_code == 429:
            raise AdminRateLimitedException(
                "Too many requests.",
                retry_after=parse_retry_after(retry_after_header),
                status_code=response.status_code,
                response_text=response_text,
            )
        if not response.ok:
            raise ParadimeAPIException(
                f"Error: {response.status_code} - {response_text}",
                status_code=response.status_code,
                response_text=response_text,
            )
        if not isinstance(body, dict) or body.get("data") is None:
            raise ParadimeAPIException(
                f"Error: {response.status_code} - {response_text}",
                status_code=response.status_code,
                response_text=response_text,
            )
        return body["data"]


def _without_unreadable_links(errors: Any, data: Any, linked_fields: Collection[str]) -> Any:
    if not isinstance(errors, list) or not isinstance(data, dict):
        return errors
    return [error for error in errors if not _is_unreadable_link(error, data, linked_fields)]


def _is_unreadable_link(error: Any, data: Dict[str, Any], linked_fields: Collection[str]) -> bool:
    if not isinstance(error, dict):
        return False
    extensions = error.get("extensions")
    if not isinstance(extensions, dict):
        return False
    if parse_error_code(extensions.get("code")) not in UNREADABLE_LINK_CODES:
        return False
    path = error.get("path")
    if not isinstance(path, list) or len(path) < 2 or path[-1] not in linked_fields:
        return False
    parent = _value_at(data, path[:-1])
    return isinstance(parent, dict) and parent.get(path[-1]) is None


def _value_at(data: Any, path: List[Any]) -> Any:
    for part in path:
        if isinstance(data, dict) and isinstance(part, str):
            data = data.get(part)
        elif isinstance(data, list) and isinstance(part, int) and 0 <= part < len(data):
            data = data[part]
        else:
            return None
    return data


def _json_body(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return None
