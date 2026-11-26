from datetime import datetime
from typing import Any, Dict, List, Optional, Type

from paradime.apis.admin.types import AdminInputModel, AdminModel, OpenEnum, known_member
from paradime.tools.pydantic import StrictStr, validator

WORKSPACE_INTEGRATION_FIELDS = """
    name
    status
    syncProgress
    updatedAt
    settings {
        __typename
        ... on LookerSettings {
            baseUrl
            clientId
            hasClientSecret
        }
        ... on FivetranSettings {
            apiKeyHint
            hasApiSecret
        }
        ... on DatadogSettings {
            site
            hasApiKey
            hasApplicationKey
        }
        ... on TableauSettings {
            url
            patName
            hasPatSecret
            projectIds
        }
        ... on PowerBiSettings {
            tenantId
            clientId
            hasClientSecret
            workspaceIds
        }
        ... on DataHubSettings {
            gmsUrl
            domain
            hasPatToken
        }
        ... on SigmaSettings {
            host
            clientId
            hasClientSecret
        }
        ... on ThoughtSpotSettings {
            host
            username
            hasPassword
        }
        ... on LightdashSettings {
            instanceUrl
            hasApiToken
        }
        ... on OmniSettings {
            host
            hasApiKey
        }
        ... on MetabaseSettings {
            host
            hasApiKey
        }
        ... on AirbyteSettings {
            clientId
            hasClientSecret
        }
        ... on HightouchSettings {
            hasApiKey
        }
        ... on NewRelicSettings {
            accountId
            hasApiKey
        }
        ... on PagerDutySettings {
            apiRegion
            hasApiKey
        }
        ... on IncidentIoSettings {
            hasApiKey
        }
        ... on RootlySettings {
            hasApiKey
        }
    }
"""

EXTERNAL_SECRETS_PROVIDER_FIELDS = """
    name
    lastValidatedAt
    settings {
        __typename
        ... on AwsSecretsManagerSettings {
            accessKeyIdHint
            roleArn
            region
            hasSecretAccessKey
        }
        ... on AzureKeyVaultSettings {
            tenantId
            clientId
            defaultVaultUrl
            hasClientSecret
        }
        ... on GcpSecretManagerSettings {
            defaultProject
            hasServiceAccountJson
        }
    }
"""

AGENT_INTEGRATION_FIELDS = """
    name
    displayName
    status
    reconnectRequired
"""

AGENT_INTEGRATION_INSTALL_FIELDS = """
    installUrl
    installUrlExpiresAt
"""

AGENT_INTEGRATION_LINKS = frozenset({"installUrl", "installUrlExpiresAt"})


class IntegrationName(OpenEnum):
    LOOKER = "LOOKER"
    FIVETRAN = "FIVETRAN"
    DATADOG = "DATADOG"
    TABLEAU = "TABLEAU"
    POWER_BI = "POWER_BI"
    DATAHUB = "DATAHUB"
    SIGMA = "SIGMA"
    THOUGHTSPOT = "THOUGHTSPOT"
    LIGHTDASH = "LIGHTDASH"
    OMNI = "OMNI"
    METABASE = "METABASE"
    AIRBYTE = "AIRBYTE"
    HIGHTOUCH = "HIGHTOUCH"
    NEW_RELIC = "NEW_RELIC"
    PAGERDUTY = "PAGERDUTY"
    INCIDENT_IO = "INCIDENT_IO"
    ROOTLY = "ROOTLY"
    UNKNOWN = "UNKNOWN"


class IntegrationStatus(OpenEnum):
    NOT_CONNECTED = "NOT_CONNECTED"
    CONNECTED = "CONNECTED"
    SYNCING = "SYNCING"
    UNKNOWN = "UNKNOWN"


class DatadogSite(OpenEnum):
    US1 = "US1"
    US3 = "US3"
    US5 = "US5"
    EU = "EU"
    AP1 = "AP1"
    AP2 = "AP2"
    US1_FED = "US1_FED"
    UNKNOWN = "UNKNOWN"


class PagerDutyRegion(OpenEnum):
    US = "US"
    EU = "EU"
    UNKNOWN = "UNKNOWN"


class AgentIntegrationName(OpenEnum):
    ASANA = "ASANA"
    AZURE_DEVOPS = "AZURE_DEVOPS"
    BITBUCKET = "BITBUCKET"
    BOX = "BOX"
    CLICKUP = "CLICKUP"
    DROPBOX = "DROPBOX"
    FIGJAM = "FIGJAM"
    GITLAB = "GITLAB"
    GOOGLE_DOCS = "GOOGLE_DOCS"
    GOOGLE_DRIVE = "GOOGLE_DRIVE"
    GOOGLE_SHEETS = "GOOGLE_SHEETS"
    GOOGLE_SLIDES = "GOOGLE_SLIDES"
    HUBSPOT = "HUBSPOT"
    JIRA_CLOUD = "JIRA_CLOUD"
    JIRA_CLOUD_SERVICE_ACCOUNT = "JIRA_CLOUD_SERVICE_ACCOUNT"
    JIRA_DATA_CENTER = "JIRA_DATA_CENTER"
    MICROSOFT_TEAMS = "MICROSOFT_TEAMS"
    MIRO = "MIRO"
    NOTION = "NOTION"
    ONEDRIVE = "ONEDRIVE"
    SHAREPOINT = "SHAREPOINT"
    SLACK = "SLACK"
    UNKNOWN = "UNKNOWN"


class AgentIntegrationStatus(OpenEnum):
    CONNECTED = "CONNECTED"
    PENDING_USER_ACTION = "PENDING_USER_ACTION"
    UNAVAILABLE = "UNAVAILABLE"
    UNKNOWN = "UNKNOWN"


class ExternalSecretsProviderName(OpenEnum):
    AWS = "AWS"
    AZURE = "AZURE"
    GCP = "GCP"
    UNKNOWN = "UNKNOWN"


class LookerSettings(AdminModel):
    base_url: Optional[str] = None
    client_id: Optional[str] = None
    has_client_secret: Optional[bool] = None


class FivetranSettings(AdminModel):
    api_key_hint: Optional[str] = None
    has_api_secret: Optional[bool] = None


class DatadogSettings(AdminModel):
    site: Optional[DatadogSite] = None
    has_api_key: Optional[bool] = None
    has_application_key: Optional[bool] = None


class TableauSettings(AdminModel):
    url: Optional[str] = None
    pat_name: Optional[str] = None
    has_pat_secret: Optional[bool] = None
    project_ids: Optional[List[str]] = None


class PowerBiSettings(AdminModel):
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    has_client_secret: Optional[bool] = None
    workspace_ids: Optional[List[str]] = None


class DataHubSettings(AdminModel):
    gms_url: Optional[str] = None
    domain: Optional[str] = None
    has_pat_token: Optional[bool] = None


class SigmaSettings(AdminModel):
    host: Optional[str] = None
    client_id: Optional[str] = None
    has_client_secret: Optional[bool] = None


class ThoughtSpotSettings(AdminModel):
    host: Optional[str] = None
    username: Optional[str] = None
    has_password: Optional[bool] = None


class LightdashSettings(AdminModel):
    instance_url: Optional[str] = None
    has_api_token: Optional[bool] = None


class OmniSettings(AdminModel):
    host: Optional[str] = None
    has_api_key: Optional[bool] = None


class MetabaseSettings(AdminModel):
    host: Optional[str] = None
    has_api_key: Optional[bool] = None


class AirbyteSettings(AdminModel):
    client_id: Optional[str] = None
    has_client_secret: Optional[bool] = None


class HightouchSettings(AdminModel):
    has_api_key: Optional[bool] = None


class NewRelicSettings(AdminModel):
    account_id: Optional[str] = None
    has_api_key: Optional[bool] = None


class PagerDutySettings(AdminModel):
    api_region: Optional[PagerDutyRegion] = None
    has_api_key: Optional[bool] = None


class IncidentIoSettings(AdminModel):
    has_api_key: Optional[bool] = None


class RootlySettings(AdminModel):
    has_api_key: Optional[bool] = None


class AwsSecretsManagerSettings(AdminModel):
    access_key_id_hint: Optional[str] = None
    role_arn: Optional[str] = None
    region: Optional[str] = None
    has_secret_access_key: Optional[bool] = None


class AzureKeyVaultSettings(AdminModel):
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    default_vault_url: Optional[str] = None
    has_client_secret: Optional[bool] = None


class GcpSecretManagerSettings(AdminModel):
    default_project: Optional[str] = None
    has_service_account_json: Optional[bool] = None


def _settings_by_typename(*classes: Type[AdminModel]) -> Dict[str, Type[AdminModel]]:
    return {settings.__name__: settings for settings in classes}


INTEGRATION_SETTINGS = _settings_by_typename(
    LookerSettings,
    FivetranSettings,
    DatadogSettings,
    TableauSettings,
    PowerBiSettings,
    DataHubSettings,
    SigmaSettings,
    ThoughtSpotSettings,
    LightdashSettings,
    OmniSettings,
    MetabaseSettings,
    AirbyteSettings,
    HightouchSettings,
    NewRelicSettings,
    PagerDutySettings,
    IncidentIoSettings,
    RootlySettings,
)
SECRETS_PROVIDER_SETTINGS = _settings_by_typename(
    AwsSecretsManagerSettings, AzureKeyVaultSettings, GcpSecretManagerSettings
)


def _parse_settings(value: Any, classes: Dict[str, Type[AdminModel]]) -> Any:
    if not isinstance(value, dict):
        return value
    settings_class = classes.get(value.get("__typename", ""))
    return settings_class.parse_obj(value) if settings_class else None


class WorkspaceIntegration(AdminModel):
    name: IntegrationName
    status: IntegrationStatus
    sync_progress: Optional[float] = None
    updated_at: Optional[datetime] = None
    settings: Optional[Any] = None

    @validator("settings", pre=True)
    def _settings(cls, value: Any) -> Any:
        return _parse_settings(value, INTEGRATION_SETTINGS)


class ExternalSecretsProvider(AdminModel):
    name: ExternalSecretsProviderName
    last_validated_at: Optional[datetime] = None
    settings: Optional[Any] = None

    @validator("settings", pre=True)
    def _settings(cls, value: Any) -> Any:
        return _parse_settings(value, SECRETS_PROVIDER_SETTINGS)


class AgentIntegration(AdminModel):
    name: AgentIntegrationName
    display_name: Optional[str] = None
    status: AgentIntegrationStatus
    reconnect_required: Optional[bool] = None
    install_url: Optional[str] = None
    install_url_expires_at: Optional[datetime] = None


class IntegrationTestResult(AdminModel):
    name: IntegrationName
    tested_at: datetime


class ExternalSecretReferenceTestResult(AdminModel):
    provider: ExternalSecretsProviderName
    tested_at: datetime


class LookerIntegrationInput(AdminInputModel):
    base_url: StrictStr
    client_id: StrictStr
    client_secret: Optional[StrictStr] = None


class FivetranIntegrationInput(AdminInputModel):
    api_key: StrictStr
    api_secret: Optional[StrictStr] = None


class DatadogIntegrationInput(AdminInputModel):
    site: DatadogSite
    api_key: Optional[StrictStr] = None
    application_key: Optional[StrictStr] = None

    @validator("site", pre=True)
    def _site(cls, value: Any) -> Any:
        return known_member(DatadogSite, value)


class TableauIntegrationInput(AdminInputModel):
    url: StrictStr
    pat_name: StrictStr
    pat_secret: Optional[StrictStr] = None
    project_ids: Optional[List[StrictStr]] = None


class PowerBiIntegrationInput(AdminInputModel):
    tenant_id: StrictStr
    client_id: StrictStr
    client_secret: Optional[StrictStr] = None
    workspace_ids: List[StrictStr]


class DataHubIntegrationInput(AdminInputModel):
    gms_url: StrictStr
    pat_token: Optional[StrictStr] = None
    domain: Optional[StrictStr] = None


class SigmaIntegrationInput(AdminInputModel):
    host: StrictStr
    client_id: StrictStr
    client_secret: Optional[StrictStr] = None


class ThoughtSpotIntegrationInput(AdminInputModel):
    host: StrictStr
    username: StrictStr
    password: Optional[StrictStr] = None


class LightdashIntegrationInput(AdminInputModel):
    instance_url: StrictStr
    api_token: Optional[StrictStr] = None


class OmniIntegrationInput(AdminInputModel):
    host: StrictStr
    api_key: Optional[StrictStr] = None


class MetabaseIntegrationInput(AdminInputModel):
    host: StrictStr
    api_key: Optional[StrictStr] = None


class AirbyteIntegrationInput(AdminInputModel):
    client_id: StrictStr
    client_secret: Optional[StrictStr] = None


class HightouchIntegrationInput(AdminInputModel):
    api_key: Optional[StrictStr] = None


class NewRelicIntegrationInput(AdminInputModel):
    account_id: StrictStr
    api_key: Optional[StrictStr] = None


class PagerDutyIntegrationInput(AdminInputModel):
    api_region: PagerDutyRegion
    api_key: Optional[StrictStr] = None

    @validator("api_region", pre=True)
    def _api_region(cls, value: Any) -> Any:
        return known_member(PagerDutyRegion, value)


class IncidentIoIntegrationInput(AdminInputModel):
    api_key: Optional[StrictStr] = None


class RootlyIntegrationInput(AdminInputModel):
    api_key: Optional[StrictStr] = None


class IntegrationConfigInput(AdminInputModel):
    looker: Optional[LookerIntegrationInput] = None
    fivetran: Optional[FivetranIntegrationInput] = None
    datadog: Optional[DatadogIntegrationInput] = None
    tableau: Optional[TableauIntegrationInput] = None
    power_bi: Optional[PowerBiIntegrationInput] = None
    datahub: Optional[DataHubIntegrationInput] = None
    sigma: Optional[SigmaIntegrationInput] = None
    thoughtspot: Optional[ThoughtSpotIntegrationInput] = None
    lightdash: Optional[LightdashIntegrationInput] = None
    omni: Optional[OmniIntegrationInput] = None
    metabase: Optional[MetabaseIntegrationInput] = None
    airbyte: Optional[AirbyteIntegrationInput] = None
    hightouch: Optional[HightouchIntegrationInput] = None
    new_relic: Optional[NewRelicIntegrationInput] = None
    pagerduty: Optional[PagerDutyIntegrationInput] = None
    incident_io: Optional[IncidentIoIntegrationInput] = None
    rootly: Optional[RootlyIntegrationInput] = None


class AwsSecretsManagerConfigInput(AdminInputModel):
    access_key_id: str
    secret_access_key: Optional[str] = None
    role_arn: str
    region: str


class AzureKeyVaultConfigInput(AdminInputModel):
    tenant_id: str
    client_id: str
    client_secret: Optional[str] = None
    default_vault_url: Optional[str] = None


class GcpSecretManagerConfigInput(AdminInputModel):
    service_account_json: Optional[str] = None
    default_project: Optional[str] = None


class ExternalSecretsProviderConfigInput(AdminInputModel):
    aws: Optional[AwsSecretsManagerConfigInput] = None
    azure: Optional[AzureKeyVaultConfigInput] = None
    gcp: Optional[GcpSecretManagerConfigInput] = None
