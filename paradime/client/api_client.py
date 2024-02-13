from typing import Any
import requests

from paradime.client.api_exception import ParadimeException


class APIClient:
    def __init__(self, *, api_key: str, api_secret: str, api_endpoint: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.api_endpoint = api_endpoint

    def _get_request_headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-API-KEY": self.api_key,
            "X-API-SECRET": self.api_secret,
        }

    def _raise_for_gql_response_body_errors(self, response: requests.Response) -> None:
        response_json = response.json()
        if "errors" in response_json:
            raise ParadimeException(f"{response_json['errors']}")

    def _raise_for_response_status_errors(self, response: requests.Response) -> None:
        try:
            response.raise_for_status()
        except Exception as e:
            raise ParadimeException(
                f"Error: {response.status_code} - {response.text}"
            ) from e

    def _raise_for_errors(self, response: requests.Response) -> None:
        self._raise_for_response_status_errors(response)
        self._raise_for_gql_response_body_errors(response)

    def _call_gql(self, query: str, variables: dict[str, Any] = {}) -> dict[str, Any]:
        response = requests.post(
            url=self.api_endpoint,
            json={"query": query, "variables": variables},
            headers=self._get_request_headers(),
            timeout=60,
        )
        self._raise_for_errors(response)

        return response.json()["data"]
