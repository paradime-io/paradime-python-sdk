from paradime.apis.custom_integration import CustomIntegration
from paradime.client.api_client import APIClient


class Paradime(APIClient):
    """
    A client for making API requests to the Paradime API.

    Attributes:
        custom_integration (CustomIntegration): The custom integration API client.

    Args:
        api_key (str): The API key for authentication. Generate this from Paradime account settings.
        api_secret (str): The API secret for authentication. Generate this from Paradime account settings.
        api_endpoint (str): The API endpoint URL. Generate this from Paradime account settings.
    """

    custom_integration: CustomIntegration

    def __init__(self, *, api_key: str, api_secret: str, api_endpoint: str):
        super().__init__(api_key=api_key, api_secret=api_secret, api_endpoint=api_endpoint)

        self.custom_integration = CustomIntegration(client=self)
