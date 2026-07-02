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
        api_key (str, optional): The API key for authentication. Required when `api_secret`
            is a legacy secret; not needed when `api_secret` is a bearer token. Generate this
            from Paradime account settings.
        api_secret (str): The API secret, or a workspace-level (`prdm_wsp_`) or company-level
            (`prdm_cmp_`) bearer token, for authentication. Generate this from Paradime
            account settings.
        workspace_uid (str, optional): The workspace uid to target. Required when
            `api_secret` is a company-level (`prdm_cmp_`) token; not used otherwise.
        api_endpoint (str): The API endpoint URL. Generate this from Paradime account settings.
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
        api_key: Optional[str] = None,
        api_secret: str,
        workspace_uid: Optional[str] = None,
        api_endpoint: str,
    ):
        super().__init__(
            api_key=api_key,
            api_secret=api_secret,
            workspace_uid=workspace_uid,
            api_endpoint=api_endpoint,
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
