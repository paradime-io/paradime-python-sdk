from typing import Optional

from paradime.apis.admin.client import AdminClient
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
from paradime.client.rate_limit import (
    DEFAULT_MAX_RATE_LIMIT_RETRIES,
    DEFAULT_MAX_RETRY_WAIT_SECONDS,
)
from paradime.version_check import check_for_new_version


class Paradime(APIClient):
    """
    A client for making API requests to the Paradime API.

    Attributes:
        admin (AdminClient): The Administration API client (Enterprise plan).
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
        workspace_uid (str, optional): The workspace uid to target with a company-level
            (`prdm_cmp_`) token. Needed for workspace-level calls, which raise
            `ParadimeWorkspaceRequiredException` without it; not used otherwise.
        api_endpoint (str): The API endpoint URL. Generate this from Paradime account settings.
        timeout (int, optional): The timeout for API requests in seconds. Defaults to 60 seconds.
        max_rate_limit_retries (int, optional): How many times to retry a request the API
            refuses for its rate limit (HTTP 429), after the wait the server asks for. Set 0
            to raise `ParadimeRateLimitedException` at once. Defaults to 3.
        max_retry_wait (float, optional): The longest wait to retry after, in seconds; a
            longer one raises at once. Defaults to 60 seconds.
    """

    admin: AdminClient
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
        timeout: int = 60,
        max_rate_limit_retries: int = DEFAULT_MAX_RATE_LIMIT_RETRIES,
        max_retry_wait: float = DEFAULT_MAX_RETRY_WAIT_SECONDS,
    ):
        super().__init__(
            api_key=api_key,
            api_secret=api_secret,
            workspace_uid=workspace_uid,
            api_endpoint=api_endpoint,
            timeout=timeout,
            max_rate_limit_retries=max_rate_limit_retries,
            max_retry_wait=max_retry_wait,
        )

        check_for_new_version()

        self.admin = AdminClient(client=self)
        self.audit_log = AuditLogClient(client=self)
        self.bolt = BoltClient(client=self)
        self.catalog = CatalogClient(client=self)
        self.custom_integration = CustomIntegrationClient(client=self)
        self.dinoai_agents = DinoaiAgentsClient(client=self)
        self.lineage_diff = LineageDiffClient(client=self)
        self.metadata = MetadataClient(bolt_client=self.bolt)
        self.users = UsersClient(client=self)
        self.workspaces = WorkspacesClient(client=self)
