from paradime.client.api_client import APIClient


class CatalogClient:
    def __init__(self, client: APIClient):
        self.client = client

    def refresh(self) -> None:
        """
        Triggers a background refresh of the Paradime catalog.
        """

        query = """
            mutation refreshCatalog {
                refreshCatalog {
                    ok
                }
            }
        """

        self.client._call_gql(query)
