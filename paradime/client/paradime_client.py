from typing import TYPE_CHECKING, Optional

from paradime.apis.audit_log.client import AuditLogClient
from paradime.apis.bolt.client import BoltClient
from paradime.apis.catalog.client import CatalogClient
from paradime.apis.custom_integration.client import CustomIntegrationClient
from paradime.apis.dinoai_agents.client import DinoaiAgentsClient
from paradime.apis.lineage_diff.client import LineageDiffClient
from paradime.apis.users.client import UsersClient
from paradime.apis.workspaces.client import WorkspacesClient
from paradime.client.api_client import DEFAULT_MAX_RETRIES, DEFAULT_TIMEOUT_SECONDS, APIClient
from paradime.version_check import check_for_new_version

if TYPE_CHECKING:
    from paradime.apis.metadata.client import MetadataClient

_METADATA_EXTRA_HINT = (
    "The metadata API needs extra dependencies that are not installed by default. "
    "Install them with:\n\n    pip install 'paradime-io[metadata]'\n"
)


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
        metadata (MetadataClient): The metadata API client. Constructed on first
            access; requires the `metadata` extra (`pip install 'paradime-io[metadata]'`).
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
        timeout (int, optional): The timeout for API requests in seconds. Defaults to 60.
        max_retries (int, optional): How many times to attempt a request before giving up.
            Retries cover connection errors, timeouts, and the 429/502/503/504 statuses,
            with exponential backoff. Set to 1 to disable retries. Defaults to 3.
        check_for_updates (bool, optional): Check PyPI for a newer SDK release and print a
            notice to stderr if one exists. Off by default so that constructing a client
            never makes an unexpected outbound request. Defaults to False.
    """

    audit_log: AuditLogClient
    bolt: BoltClient
    catalog: CatalogClient
    custom_integration: CustomIntegrationClient
    dinoai_agents: DinoaiAgentsClient
    lineage_diff: LineageDiffClient
    users: UsersClient
    workspaces: WorkspacesClient

    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        api_secret: str,
        workspace_uid: Optional[str] = None,
        api_endpoint: str,
        timeout: int = DEFAULT_TIMEOUT_SECONDS,
        max_retries: int = DEFAULT_MAX_RETRIES,
        check_for_updates: bool = False,
    ):
        super().__init__(
            api_key=api_key,
            api_secret=api_secret,
            workspace_uid=workspace_uid,
            api_endpoint=api_endpoint,
            timeout=timeout,
            max_retries=max_retries,
        )

        # Opt-in only. A library constructor should not reach out to pypi.org — that is
        # surprising in a server or a Lambda. The CLI still checks on every invocation.
        if check_for_updates:
            check_for_new_version()

        self.audit_log = AuditLogClient(client=self)
        self.bolt = BoltClient(client=self)
        self.catalog = CatalogClient(client=self)
        self.custom_integration = CustomIntegrationClient(client=self)
        self.dinoai_agents = DinoaiAgentsClient(client=self)
        self.lineage_diff = LineageDiffClient(client=self)
        self.users = UsersClient(client=self)
        self.workspaces = WorkspacesClient(client=self)

        self._metadata: Optional["MetadataClient"] = None

    @property
    def metadata(self) -> "MetadataClient":
        """The metadata API client, constructed on first access.

        Unlike the other sub-clients this one is not an HTTP client — it downloads
        dbt artifacts and queries them locally through DuckDB and polars. Those
        dependencies ship in the `metadata` extra, so it is built lazily and only
        raises if the caller actually reaches for it.
        """

        if self._metadata is None:
            try:
                from paradime.apis.metadata.client import MetadataClient
            except ImportError as e:
                raise ImportError(f"{_METADATA_EXTRA_HINT}\nOriginal error: {e}") from e

            self._metadata = MetadataClient(bolt_client=self.bolt)

        return self._metadata
