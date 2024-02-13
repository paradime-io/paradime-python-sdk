from paradime.apis.custom_integration import CustomIntegration
from paradime.client.api_client import APIClient


class Paradime(APIClient):
    custom_integration: CustomIntegration

    def __init__(self, *, api_key: str, api_secret: str, api_endpoint: str):
        super().__init__(
            api_key=api_key, api_secret=api_secret, api_endpoint=api_endpoint
        )

        self.custom_integration = CustomIntegration(client=self)
