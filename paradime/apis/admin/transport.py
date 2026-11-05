import logging
import time
from typing import Any, Callable, Dict, Optional

import requests

from paradime.apis.admin.exception import (
    AdminRateLimitedException,
    admin_exception_from_errors,
    parse_retry_after,
)
from paradime.client.api_client import WORKSPACE_SELECTION_HEADER, APIClient
from paradime.client.api_exception import ParadimeAPIException

logger = logging.getLogger(__name__)

DEFAULT_MAX_RATE_LIMIT_RETRIES = 3
DEFAULT_MAX_RETRY_WAIT_SECONDS = 60.0
DEFAULT_RETRY_WAIT_SECONDS = 1.0


class AdminTransport:
    """
    Sends Admin API requests through an `APIClient`'s endpoint and credentials.

    A request the server refuses with `RATE_LIMITED` (HTTP 429) is retried after the
    `retryAfter` the server sends, up to `max_rate_limit_retries` times; then the
    `AdminRateLimitedException` is raised. Any other error raises the typed exception of its
    `extensions.code`.

    Args:
        client (APIClient): The client whose endpoint, credentials and timeout to use.
        max_rate_limit_retries (int): How many times to retry a rate-limited request.
        max_retry_wait (float): The longest wait between retries, in seconds.
        sleep (Callable[[float], None]): How to wait between retries.
    """

    def __init__(
        self,
        client: APIClient,
        *,
        max_rate_limit_retries: int = DEFAULT_MAX_RATE_LIMIT_RETRIES,
        max_retry_wait: float = DEFAULT_MAX_RETRY_WAIT_SECONDS,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.client = client
        self.max_rate_limit_retries = max_rate_limit_retries
        self.max_retry_wait = max_retry_wait
        self.sleep = sleep

    def execute(
        self,
        query: str,
        variables: Optional[Dict[str, Any]] = None,
        *,
        workspace_uid: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Runs a GraphQL query or mutation and returns its `data`.

        Args:
            query (str): The GraphQL document.
            variables (dict, optional): Its variables.
            workspace_uid (str, optional): The workspace to send in `X-Paradime-Workspace` for
                this request, in place of the client's.

        Raises:
            AdminApiException: The typed exception of the first error's `extensions.code`.
            ParadimeAPIException: For an HTTP error without a GraphQL error body.
        """

        retries = 0
        while True:
            try:
                return self._execute_once(query, variables or {}, workspace_uid)
            except AdminRateLimitedException as e:
                if retries >= self.max_rate_limit_retries:
                    raise
                retries += 1
                wait = min(
                    e.retry_after if e.retry_after is not None else DEFAULT_RETRY_WAIT_SECONDS,
                    self.max_retry_wait,
                )
                logger.info(
                    f"Rate limited by the Paradime API; retrying in {wait:g}s"
                    f" ({retries}/{self.max_rate_limit_retries})."
                )
                self.sleep(wait)

    def _headers(self, workspace_uid: Optional[str]) -> Dict[str, str]:
        headers = self.client._get_request_headers()
        if workspace_uid:
            headers[WORKSPACE_SELECTION_HEADER] = workspace_uid
        return headers

    def _execute_once(
        self, query: str, variables: Dict[str, Any], workspace_uid: Optional[str]
    ) -> Dict[str, Any]:
        response = requests.post(
            url=self.client.api_endpoint,
            json={"query": query, "variables": variables},
            headers=self._headers(workspace_uid),
            timeout=self.client.timeout,
        )
        retry_after_header = response.headers.get("Retry-After")
        body = _json_body(response)
        errors = body.get("errors") if isinstance(body, dict) else None
        if errors:
            raise admin_exception_from_errors(errors, retry_after_header=retry_after_header)
        if response.status_code == 429:
            raise AdminRateLimitedException(
                "Too many requests.", retry_after=parse_retry_after(retry_after_header)
            )
        if not response.ok:
            raise ParadimeAPIException(f"Error: {response.status_code} - {response.text}")
        if not isinstance(body, dict) or body.get("data") is None:
            raise ParadimeAPIException(f"Error: {response.status_code} - {response.text}")
        return body["data"]


def _json_body(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return None
