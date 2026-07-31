from paradime.client.api_client import APIClient
from paradime.graphql import load_operation


class CatalogClient:
    def __init__(self, client: APIClient):
        self.client = client

    def refresh(self) -> None:
        """
        Triggers a background refresh of the Paradime catalog.
        """

        query = load_operation("catalog", "refresh")

        self.client._call_gql(query)
