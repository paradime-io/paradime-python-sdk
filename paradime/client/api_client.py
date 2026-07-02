from typing import Any, Dict, Optional

import requests

from paradime.client.api_exception import ParadimeAPIException
from paradime.client.runtime import detect_runtime, get_python_version, is_telemetry_enabled
from paradime.version import get_sdk_version

# Prefixes used by the bearer-token style API secrets. A company token is valid across a
# set of workspaces, so every request must select its target workspace via the
# X-Paradime-Workspace header. A workspace token is scoped to a single workspace already,
# same as a legacy key/secret pair.
COMPANY_API_TOKEN_PREFIX = "prdm_cmp_"
WORKSPACE_API_TOKEN_PREFIX = "prdm_wsp_"

WORKSPACE_SELECTION_HEADER = "X-Paradime-Workspace"


def _is_bearer_token(api_secret: str) -> bool:
    """Return True if `api_secret` is actually a bearer token rather than a legacy secret."""

    return api_secret.startswith(COMPANY_API_TOKEN_PREFIX) or api_secret.startswith(
        WORKSPACE_API_TOKEN_PREFIX
    )


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
    """

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        api_secret: str,
        workspace_uid: Optional[str] = None,
        api_endpoint: str,
        timeout: int = 60,
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

        self.api_key = api_key
        self.api_secret = api_secret
        self.workspace_uid = workspace_uid
        self.api_endpoint = api_endpoint
        self.timeout = timeout

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
