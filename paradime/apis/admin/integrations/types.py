from datetime import datetime
from typing import Any, Dict, Optional, Type

from paradime.apis.admin.types import AdminInputModel, AdminModel, OpenEnum
from paradime.tools.pydantic import validator

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
            clientSecretIsSet
        }
        ... on FivetranSettings {
            apiKeyHint
            apiSecretIsSet
        }
        ... on DatadogSettings {
            site
            apiKeyIsSet
            applicationKeyIsSet
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
            secretAccessKeyIsSet
        }
        ... on AzureKeyVaultSettings {
            tenantId
            clientId
            defaultVaultUrl
            clientSecretIsSet
        }
        ... on GcpSecretManagerSettings {
            defaultProject
            serviceAccountJsonIsSet
        }
    }
"""


class IntegrationName(OpenEnum):
    LOOKER = "LOOKER"
    FIVETRAN = "FIVETRAN"
    DATADOG = "DATADOG"
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


class ExternalSecretsProviderName(OpenEnum):
    AWS = "AWS"
    AZURE = "AZURE"
    GCP = "GCP"
    UNKNOWN = "UNKNOWN"


class LookerSettings(AdminModel):
    base_url: Optional[str] = None
    client_id: Optional[str] = None
    client_secret_is_set: Optional[bool] = None


class FivetranSettings(AdminModel):
    api_key_hint: Optional[str] = None
    api_secret_is_set: Optional[bool] = None


class DatadogSettings(AdminModel):
    site: Optional[DatadogSite] = None
    api_key_is_set: Optional[bool] = None
    application_key_is_set: Optional[bool] = None


class AwsSecretsManagerSettings(AdminModel):
    access_key_id_hint: Optional[str] = None
    role_arn: Optional[str] = None
    region: Optional[str] = None
    secret_access_key_is_set: Optional[bool] = None


class AzureKeyVaultSettings(AdminModel):
    tenant_id: Optional[str] = None
    client_id: Optional[str] = None
    default_vault_url: Optional[str] = None
    client_secret_is_set: Optional[bool] = None


class GcpSecretManagerSettings(AdminModel):
    default_project: Optional[str] = None
    service_account_json_is_set: Optional[bool] = None


def _settings_by_typename(*classes: Type[AdminModel]) -> Dict[str, Type[AdminModel]]:
    return {settings.__name__: settings for settings in classes}


INTEGRATION_SETTINGS = _settings_by_typename(LookerSettings, FivetranSettings, DatadogSettings)
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


class IntegrationTestResult(AdminModel):
    name: IntegrationName
    tested_at: datetime


class ExternalSecretReferenceTestResult(AdminModel):
    provider: ExternalSecretsProviderName
    tested_at: datetime


class LookerIntegrationInput(AdminInputModel):
    base_url: str
    client_id: str
    client_secret: Optional[str] = None


class FivetranIntegrationInput(AdminInputModel):
    api_key: str
    api_secret: Optional[str] = None


class DatadogIntegrationInput(AdminInputModel):
    site: DatadogSite
    api_key: Optional[str] = None
    application_key: Optional[str] = None


class IntegrationConfigInput(AdminInputModel):
    looker: Optional[LookerIntegrationInput] = None
    fivetran: Optional[FivetranIntegrationInput] = None
    datadog: Optional[DatadogIntegrationInput] = None


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
