from paradime.apis.audit_log.client import AuditLogClient
from paradime.apis.bolt.client import BoltClient
from paradime.apis.custom_integration.client import CustomIntegrationClient
from paradime.client.api_client import APIClient


class Paradime(APIClient):
    """
    A client for making API requests to the Paradime API.

    Attributes:
        custom_integration (CustomIntegrationClient): The custom integration API client.
        audit_log (AuditLogClient): The audit log API client.
        bolt (BoltClient): The bolt API client.

    Args:
        api_key (str): The API key for authentication. Generate this from Paradime account settings.
        api_secret (str): The API secret for authentication. Generate this from Paradime account settings.
        api_endpoint (str): The API endpoint URL. Generate this from Paradime account settings.
    """

    custom_integration: CustomIntegrationClient
    audit_log: AuditLogClient
    bolt: BoltClient

    def __init__(self, *, api_key: str, api_secret: str, api_endpoint: str):
        super().__init__(api_key=api_key, api_secret=api_secret, api_endpoint=api_endpoint)

        self.custom_integration = CustomIntegrationClient(client=self)
        self.audit_log = AuditLogClient(client=self)
        self.bolt = BoltClient(client=self)
