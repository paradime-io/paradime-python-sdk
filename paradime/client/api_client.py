from typing import Any, Dict, Optional

import requests

from paradime.client.api_exception import ParadimeAPIException
from paradime.client.runtime import detect_runtime, get_python_version, is_telemetry_enabled
from paradime.version import get_sdk_version


class APIClient:
    """
    A client for making API requests to the Paradime API.

    Supports two authentication modes:

    1. **Workspace-level** (legacy): Uses an API key and secret scoped to a single workspace.
    2. **Company-level**: Uses a ``prdm_cmp_`` bearer token that spans multiple workspaces.
       Each request targets a workspace via the ``workspace_uid`` parameter.

    Args:
        api_endpoint (str): The endpoint URL for the API.
        api_key (str, optional): The API key for workspace-level authentication.
        api_secret (str, optional): The API secret for workspace-level authentication.
        api_token (str, optional): A company-level API token (``prdm_cmp_`` prefix).
        workspace_uid (str, optional): The target workspace UID for company-level auth.
        timeout (int, optional): The timeout for API requests in seconds. Defaults to 60 seconds.
    """

    def __init__(
        self,
        *,
        api_endpoint: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_token: Optional[str] = None,
        workspace_uid: Optional[str] = None,
        timeout: int = 60,
    ):
        self.api_endpoint = api_endpoint
        self.timeout = timeout
        self.api_token: Optional[str]
        self.workspace_uid: Optional[str]

        if api_token:
            self.api_key = ""
            self.api_secret = ""
            self.api_token = api_token
            self.workspace_uid = workspace_uid
        elif api_key and api_secret:
            self.api_key = api_key
            self.api_secret = api_secret
            self.api_token = None
            self.workspace_uid = None
        else:
            raise ValueError(
                "Provide either (api_key + api_secret) or api_token for authentication."
            )

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

        if self.api_token:
            headers["Authorization"] = f"Bearer {self.api_token}"
            if self.workspace_uid:
                headers["X-Paradime-Workspace"] = self.workspace_uid
        else:
            headers["X-API-KEY"] = self.api_key
            headers["X-API-SECRET"] = self.api_secret

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
            ParadimeAPIException: If there are errors in the response body.
        """

        response_json = response.json()
        if "errors" in response_json:
            error_message = self._get_error_message_from_response(response_json)
            raise ParadimeAPIException(error_message)

    def _get_error_message_from_response(self, response: Dict[str, Any]) -> str:
        try:
            return response["errors"][0]["message"]
        except Exception:
            return str(response["errors"])

    def _raise_for_response_status_errors(self, response: requests.Response) -> None:
        """
        Raise an exception for response status errors.

        Args:
            response (requests.Response): The API response.

        Raises:
            ParadimeException: If there is an error in the response status.
        """

        try:
            response.raise_for_status()
        except Exception as e:
            raise ParadimeAPIException(f"Error: {response.status_code} - {response.text}") from e

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

    def _call_gql(self, query: str, variables: Dict[str, Any] = {}) -> Dict[str, Any]:
        """
        Make a GraphQL API request.

        Args:
            query (str): The GraphQL query.
            variables (dict, optional): The variables for the query. Defaults to {}.

        Returns:
            dict: The response data from the API.

        Raises:
            ParadimeAPIException: If there are errors in the API response.
        """

        response = requests.post(
            url=self.api_endpoint,
            json={"query": query, "variables": variables},
            headers=self._get_request_headers(),
            timeout=self.timeout,
        )
        self._raise_for_errors(response)

        return response.json()["data"]
