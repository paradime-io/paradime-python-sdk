from typing import Optional

from paradime.apis.audit_log.client import AuditLogClient
from paradime.apis.bolt.client import BoltClient
from paradime.apis.catalog.client import CatalogClient
from paradime.apis.custom_integration.client import CustomIntegrationClient
from paradime.apis.dinoai_agents.client import DinoaiAgentsClient
from paradime.apis.lineage_diff.client import LineageDiffClient
from paradime.apis.metadata.client import MetadataClient
from paradime.apis.users.client import UsersClient
from paradime.apis.workspaces.client import WorkspacesClient
from paradime.client.api_client import APIClient
from paradime.version_check import check_for_new_version


class Paradime(APIClient):
    """
    A client for making API requests to the Paradime API.

    Supports two authentication modes:

    1. **Workspace-level** (legacy)::

        client = Paradime(api_key="...", api_secret="...", api_endpoint="...")

    2. **Company-level** — uses a ``prdm_cmp_`` bearer token that spans multiple workspaces::

        client = Paradime(api_token="prdm_cmp_...", api_endpoint="...", workspace_uid="...")

    Attributes:
        audit_log (AuditLogClient): The audit log API client.
        bolt (BoltClient): The bolt API client.
        catalog (CatalogClient): The catalog API client.
        custom_integration (CustomIntegrationClient): The custom integration API client.
        dinoai_agents (DinoaiAgentsClient): The DinoAI programmable agents API client.
        lineage_diff (LineageDiffClient): The lineage diff API client.
        metadata (MetadataClient): The metadata API client.
        users (UsersClient): The users API client.
        workspaces (WorkspacesClient): The workspaces API client.

    Args:
        api_endpoint (str): The API endpoint URL. Generate this from Paradime account settings.
        api_key (str, optional): The API key for workspace-level authentication.
        api_secret (str, optional): The API secret for workspace-level authentication.
        api_token (str, optional): A company-level API token (``prdm_cmp_`` prefix).
        workspace_uid (str, optional): The target workspace UID for company-level auth.
    """

    audit_log: AuditLogClient
    bolt: BoltClient
    catalog: CatalogClient
    custom_integration: CustomIntegrationClient
    dinoai_agents: DinoaiAgentsClient
    lineage_diff: LineageDiffClient
    metadata: MetadataClient
    users: UsersClient
    workspaces: WorkspacesClient

    def __init__(
        self,
        *,
        api_endpoint: str,
        api_key: Optional[str] = None,
        api_secret: Optional[str] = None,
        api_token: Optional[str] = None,
        workspace_uid: Optional[str] = None,
    ):
        super().__init__(
            api_endpoint=api_endpoint,
            api_key=api_key,
            api_secret=api_secret,
            api_token=api_token,
            workspace_uid=workspace_uid,
        )

        check_for_new_version()

        self.audit_log = AuditLogClient(client=self)
        self.bolt = BoltClient(client=self)
        self.catalog = CatalogClient(client=self)
        self.custom_integration = CustomIntegrationClient(client=self)
        self.dinoai_agents = DinoaiAgentsClient(client=self)
        self.lineage_diff = LineageDiffClient(client=self)
        self.metadata = MetadataClient(bolt_client=self.bolt)
        self.users = UsersClient(client=self)
        self.workspaces = WorkspacesClient(client=self)
