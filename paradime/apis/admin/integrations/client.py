from typing import Any, Dict, Iterator, Optional, Type, Union

from paradime.apis.admin.integrations.types import (
    AGENT_INTEGRATION_FIELDS,
    AGENT_INTEGRATION_INSTALL_FIELDS,
    AGENT_INTEGRATION_LINKS,
    EXTERNAL_SECRETS_PROVIDER_FIELDS,
    WORKSPACE_INTEGRATION_FIELDS,
    AgentIntegration,
    AgentIntegrationName,
    AirbyteIntegrationInput,
    AwsSecretsManagerConfigInput,
    AzureKeyVaultConfigInput,
    DatadogIntegrationInput,
    DataHubIntegrationInput,
    ExternalSecretReferenceTestResult,
    ExternalSecretsProvider,
    ExternalSecretsProviderConfigInput,
    ExternalSecretsProviderName,
    FivetranIntegrationInput,
    GcpSecretManagerConfigInput,
    HightouchIntegrationInput,
    IncidentIoIntegrationInput,
    IntegrationConfigInput,
    IntegrationName,
    IntegrationTestResult,
    LightdashIntegrationInput,
    LookerIntegrationInput,
    MetabaseIntegrationInput,
    NewRelicIntegrationInput,
    OmniIntegrationInput,
    PagerDutyIntegrationInput,
    PowerBiIntegrationInput,
    RootlyIntegrationInput,
    SigmaIntegrationInput,
    TableauIntegrationInput,
    ThoughtSpotIntegrationInput,
    WorkspaceIntegration,
)
from paradime.apis.admin.transport import AdminTransport
from paradime.apis.admin.types import (
    CONNECTION_FIELDS,
    DEFAULT_PAGE_SIZE,
    AdminInputModel,
    Page,
    check_page_size,
    iterate_pages,
    parse_page,
    serialize_input,
)

IntegrationConfig = Union[
    IntegrationConfigInput,
    LookerIntegrationInput,
    FivetranIntegrationInput,
    DatadogIntegrationInput,
    TableauIntegrationInput,
    PowerBiIntegrationInput,
    DataHubIntegrationInput,
    SigmaIntegrationInput,
    ThoughtSpotIntegrationInput,
    LightdashIntegrationInput,
    OmniIntegrationInput,
    MetabaseIntegrationInput,
    AirbyteIntegrationInput,
    HightouchIntegrationInput,
    NewRelicIntegrationInput,
    PagerDutyIntegrationInput,
    IncidentIoIntegrationInput,
    RootlyIntegrationInput,
]
SecretsProviderConfig = Union[
    ExternalSecretsProviderConfigInput,
    AwsSecretsManagerConfigInput,
    AzureKeyVaultConfigInput,
    GcpSecretManagerConfigInput,
]

INTEGRATION_KEYS: Dict[Type[AdminInputModel], str] = {
    LookerIntegrationInput: "looker",
    FivetranIntegrationInput: "fivetran",
    DatadogIntegrationInput: "datadog",
    TableauIntegrationInput: "tableau",
    PowerBiIntegrationInput: "power_bi",
    DataHubIntegrationInput: "datahub",
    SigmaIntegrationInput: "sigma",
    ThoughtSpotIntegrationInput: "thoughtspot",
    LightdashIntegrationInput: "lightdash",
    OmniIntegrationInput: "omni",
    MetabaseIntegrationInput: "metabase",
    AirbyteIntegrationInput: "airbyte",
    HightouchIntegrationInput: "hightouch",
    NewRelicIntegrationInput: "new_relic",
    PagerDutyIntegrationInput: "pagerduty",
    IncidentIoIntegrationInput: "incident_io",
    RootlyIntegrationInput: "rootly",
}
SECRETS_PROVIDER_KEYS: Dict[Type[AdminInputModel], str] = {
    AwsSecretsManagerConfigInput: "aws",
    AzureKeyVaultConfigInput: "azure",
    GcpSecretManagerConfigInput: "gcp",
}


def _wrap(
    config: Any, wrapper: Type[AdminInputModel], keys: Dict[Type[AdminInputModel], str]
) -> Dict[str, Any]:
    if isinstance(config, wrapper):
        return serialize_input(config)
    key = keys.get(type(config))
    if key is None:
        raise TypeError(f"Unsupported configuration: {type(config).__name__}")
    return serialize_input(wrapper.parse_obj({key: config}))


class AdminIntegrationsClient:
    """
    A workspace's credential integrations (BI tools such as Looker, Tableau and Power BI, ETL
    tools such as Fivetran and Airbyte, and observability tools such as Datadog and PagerDuty),
    the integrations its DinoAI agents connect through Nango, and the external secret managers
    connection secrets can reference. Secrets are write-only.
    """

    def __init__(self, transport: AdminTransport) -> None:
        self.transport = transport

    def list(
        self, *, workspace_uid: Optional[str] = None, page_size: int = DEFAULT_PAGE_SIZE
    ) -> Iterator[WorkspaceIntegration]:
        """
        Iterates over the workspace's integrations.

        Args:
            workspace_uid (str, optional): The workspace to read, for a company API key.
            page_size (int): How many integrations to read per request, at most 500.

        Returns:
            Iterator[WorkspaceIntegration]: The integrations, read a page at a time.
        """

        check_page_size(page_size)
        query = f"""
            query adminIntegrations($workspaceUid: String, $first: Int, $after: String) {{
                workspace(workspaceUid: $workspaceUid) {{
                    integrations(first: $first, after: $after) {{
                        edges {{
                            node {{
                                {WORKSPACE_INTEGRATION_FIELDS}
                            }}
                        }}
                        {CONNECTION_FIELDS}
                    }}
                }}
            }}
        """

        def fetch_page(after: Optional[str]) -> Page[WorkspaceIntegration]:
            response = self.transport.execute(
                query,
                {"workspaceUid": workspace_uid, "first": page_size, "after": after},
                workspace_uid=workspace_uid,
            )
            workspace = response["workspace"] or {}
            return parse_page(workspace.get("integrations"), WorkspaceIntegration.parse_obj)

        return iterate_pages(fetch_page)

    def set(
        self, config: IntegrationConfig, *, workspace_uid: Optional[str] = None
    ) -> WorkspaceIntegration:
        """
        Tests, then saves an integration's credentials. Sending the same input again changes
        nothing.

        Args:
            config: One integration's settings, such as `LookerIntegrationInput` or
                `MetabaseIntegrationInput`, or an `IntegrationConfigInput`. A secret left out
                keeps the saved one, unless an endpoint setting such as the host changes.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            WorkspaceIntegration: The integration.
        """

        query = f"""
            mutation adminSetIntegration($input: SetIntegrationInput!) {{
                setIntegration(input: $input) {{
                    integration {{
                        {WORKSPACE_INTEGRATION_FIELDS}
                    }}
                }}
            }}
        """

        config_input = _wrap(config, IntegrationConfigInput, INTEGRATION_KEYS)
        response = self.transport.execute(
            query, {"input": {"config": config_input}}, workspace_uid=workspace_uid
        )
        return WorkspaceIntegration.parse_obj(response["setIntegration"]["integration"])

    def test(
        self, config: IntegrationConfig, *, workspace_uid: Optional[str] = None
    ) -> IntegrationTestResult:
        """
        Checks an integration accepts the credentials, and saves nothing.

        Args:
            config: One integration's settings; see `set`.
            workspace_uid (str, optional): The workspace to use, for a company API key.

        Returns:
            IntegrationTestResult: The integration, and when the test ran.
        """

        query = """
            mutation adminTestIntegration($input: TestIntegrationInput!) {
                testIntegration(input: $input) {
                    name
                    testedAt
                }
            }
        """

        config_input = _wrap(config, IntegrationConfigInput, INTEGRATION_KEYS)
        response = self.transport.execute(
            query, {"input": {"config": config_input}}, workspace_uid=workspace_uid
        )
        return IntegrationTestResult.parse_obj(response["testIntegration"])

    def disconnect(
        self, name: IntegrationName, *, workspace_uid: Optional[str] = None
    ) -> WorkspaceIntegration:
        """
        Disconnects an integration.

        Args:
            name (IntegrationName): The integration.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            WorkspaceIntegration: The integration, now disconnected.
        """

        query = f"""
            mutation adminDisconnectIntegration($input: DisconnectIntegrationInput!) {{
                disconnectIntegration(input: $input) {{
                    integration {{
                        {WORKSPACE_INTEGRATION_FIELDS}
                    }}
                }}
            }}
        """

        response = self.transport.execute(
            query,
            {"input": {"name": IntegrationName(name).value}},
            workspace_uid=workspace_uid,
        )
        return WorkspaceIntegration.parse_obj(response["disconnectIntegration"]["integration"])

    def list_agent_integrations(
        self,
        *,
        include_install_urls: bool = False,
        workspace_uid: Optional[str] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterator[AgentIntegration]:
        """
        Iterates over the integrations the workspace's DinoAI agents can connect through Nango,
        connected or not, ordered by name. A `PENDING_USER_ACTION` one waits for a person to
        connect it in a browser.

        Args:
            include_install_urls (bool): Also read `install_url`, a Nango Connect link a person
                opens to connect a `PENDING_USER_ACTION` integration, and when it stops working.
                Each read makes new links, which work for 30 minutes; anyone who opens one can
                connect an account to the workspace's agents. Reading them needs
                `integration_settings:write`; a key without it reads None in their place.
            workspace_uid (str, optional): The workspace to read, for a company API key.
            page_size (int): How many integrations to read per request, at most 500.

        Returns:
            Iterator[AgentIntegration]: The integrations, read a page at a time.
        """

        check_page_size(page_size)
        install_fields = AGENT_INTEGRATION_INSTALL_FIELDS if include_install_urls else ""
        query = f"""
            query adminAgentIntegrations($workspaceUid: String, $first: Int, $after: String) {{
                workspace(workspaceUid: $workspaceUid) {{
                    agentIntegrations(first: $first, after: $after) {{
                        edges {{
                            node {{
                                {AGENT_INTEGRATION_FIELDS}
                                {install_fields}
                            }}
                        }}
                        {CONNECTION_FIELDS}
                    }}
                }}
            }}
        """

        def fetch_page(after: Optional[str]) -> Page[AgentIntegration]:
            response = self.transport.execute(
                query,
                {"workspaceUid": workspace_uid, "first": page_size, "after": after},
                workspace_uid=workspace_uid,
                linked_fields=AGENT_INTEGRATION_LINKS if include_install_urls else (),
            )
            workspace = response["workspace"] or {}
            return parse_page(workspace.get("agentIntegrations"), AgentIntegration.parse_obj)

        return iterate_pages(fetch_page)

    def disconnect_agent_integration(
        self, name: AgentIntegrationName, *, workspace_uid: Optional[str] = None
    ) -> AgentIntegration:
        """
        Disconnects one of the workspace's DinoAI agent integrations and deletes its Nango
        connection. Disconnecting it again is not an error.

        Args:
            name (AgentIntegrationName): The integration.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            AgentIntegration: The integration, now disconnected.
        """

        query = f"""
            mutation adminDisconnectAgentIntegration($input: DisconnectAgentIntegrationInput!) {{
                disconnectAgentIntegration(input: $input) {{
                    agentIntegration {{
                        {AGENT_INTEGRATION_FIELDS}
                    }}
                }}
            }}
        """

        response = self.transport.execute(
            query,
            {"input": {"name": AgentIntegrationName(name).value}},
            workspace_uid=workspace_uid,
        )
        return AgentIntegration.parse_obj(
            response["disconnectAgentIntegration"]["agentIntegration"]
        )

    def list_external_secrets_providers(
        self, *, workspace_uid: Optional[str] = None, page_size: int = DEFAULT_PAGE_SIZE
    ) -> Iterator[ExternalSecretsProvider]:
        """
        Iterates over the workspace's external secret managers.

        Args:
            workspace_uid (str, optional): The workspace to read, for a company API key.
            page_size (int): How many providers to read per request, at most 500.

        Returns:
            Iterator[ExternalSecretsProvider]: The providers, read a page at a time.
        """

        check_page_size(page_size)
        query = f"""
            query adminExternalSecretsProviders(
                $workspaceUid: String
                $first: Int
                $after: String
            ) {{
                workspace(workspaceUid: $workspaceUid) {{
                    externalSecretsProviders(first: $first, after: $after) {{
                        edges {{
                            node {{
                                {EXTERNAL_SECRETS_PROVIDER_FIELDS}
                            }}
                        }}
                        {CONNECTION_FIELDS}
                    }}
                }}
            }}
        """

        def fetch_page(after: Optional[str]) -> Page[ExternalSecretsProvider]:
            response = self.transport.execute(
                query,
                {"workspaceUid": workspace_uid, "first": page_size, "after": after},
                workspace_uid=workspace_uid,
            )
            workspace = response["workspace"] or {}
            return parse_page(
                workspace.get("externalSecretsProviders"), ExternalSecretsProvider.parse_obj
            )

        return iterate_pages(fetch_page)

    def set_external_secrets_provider(
        self,
        provider: SecretsProviderConfig,
        *,
        test_reference: Optional[str] = None,
        workspace_uid: Optional[str] = None,
    ) -> ExternalSecretsProvider:
        """
        Saves an external secret manager's settings.

        Args:
            provider: One provider's settings, such as `AwsSecretsManagerConfigInput`, or an
                `ExternalSecretsProviderConfigInput`.
            test_reference (str, optional): A secret reference to read with the new settings
                before saving them.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            ExternalSecretsProvider: The provider.
        """

        query = f"""
            mutation adminSetExternalSecretsProvider($input: SetExternalSecretsProviderInput!) {{
                setExternalSecretsProvider(input: $input) {{
                    provider {{
                        {EXTERNAL_SECRETS_PROVIDER_FIELDS}
                    }}
                }}
            }}
        """

        provider_input: Dict[str, Any] = {
            "provider": _wrap(provider, ExternalSecretsProviderConfigInput, SECRETS_PROVIDER_KEYS)
        }
        if test_reference is not None:
            provider_input["testReference"] = test_reference
        response = self.transport.execute(
            query, {"input": provider_input}, workspace_uid=workspace_uid
        )
        return ExternalSecretsProvider.parse_obj(response["setExternalSecretsProvider"]["provider"])

    def test_external_secret_reference(
        self, reference: str, *, workspace_uid: Optional[str] = None
    ) -> ExternalSecretReferenceTestResult:
        """
        Reads a secret reference with the saved provider settings, and returns no value.

        Args:
            reference (str): The external-secret reference.
            workspace_uid (str, optional): The workspace to use, for a company API key.

        Returns:
            ExternalSecretReferenceTestResult: The provider that resolved it, and when.
        """

        query = """
            mutation adminTestExternalSecretReference($input: TestExternalSecretReferenceInput!) {
                testExternalSecretReference(input: $input) {
                    provider
                    testedAt
                }
            }
        """

        response = self.transport.execute(
            query, {"input": {"reference": reference}}, workspace_uid=workspace_uid
        )
        return ExternalSecretReferenceTestResult.parse_obj(response["testExternalSecretReference"])

    def delete_external_secrets_provider(
        self, name: ExternalSecretsProviderName, *, workspace_uid: Optional[str] = None
    ) -> ExternalSecretsProviderName:
        """
        Deletes an external secret manager's settings.

        Args:
            name (ExternalSecretsProviderName): The provider.
            workspace_uid (str, optional): The workspace to change, for a company API key.

        Returns:
            ExternalSecretsProviderName: The deleted provider.
        """

        query = """
            mutation adminDeleteExternalSecretsProvider(
                $input: DeleteExternalSecretsProviderInput!
            ) {
                deleteExternalSecretsProvider(input: $input) {
                    name
                }
            }
        """

        response = self.transport.execute(
            query,
            {"input": {"name": ExternalSecretsProviderName(name).value}},
            workspace_uid=workspace_uid,
        )
        return ExternalSecretsProviderName(response["deleteExternalSecretsProvider"]["name"])
