from typing import Any, Dict

import requests

from paradime.client.api_exception import ParadimeAPIException
from paradime.version import get_sdk_version


class APIClient:
    """
    A client for making API requests to the Paradime API.

    Args:
        api_key (str): The API key for authentication.
        api_secret (str): The API secret for authentication.
        api_endpoint (str): The endpoint URL for the API.
        timeout (int, optional): The timeout for API requests in seconds. Defaults to 60 seconds.
    """

    def __init__(
        self,
        *,
        api_key: str,
        api_secret: str,
        api_endpoint: str,
        timeout: int = 60,
    ):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_endpoint = api_endpoint
        self.timeout = timeout

    def _get_request_headers(self) -> Dict[str, str]:
        """
        Get the request headers for Paradime API requests.

        Returns:
            dict: The request headers.
        """

        return {
            "Content-Type": "application/json",
            "X-API-KEY": self.api_key,
            "X-API-SECRET": self.api_secret,
            "X-PYTHON-SDK-VERSION": get_sdk_version(),
        }

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
