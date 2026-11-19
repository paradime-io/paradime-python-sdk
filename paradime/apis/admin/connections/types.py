from datetime import datetime
from typing import Any, Dict, List, Optional, Type, Union

from paradime.apis.admin.types import (
    ENVIRONMENT_LINK_FIELDS,
    AdminInputModel,
    AdminModel,
    EnvironmentLink,
    OpenEnum,
)
from paradime.tools.pydantic import StrictStr, validator

WAREHOUSE_CONNECTION_FIELDS = f"""
    scope
    environment {{
        {ENVIRONMENT_LINK_FIELDS}
    }}
    connectionId
    type
    isDefault
    name
    targetName
    threads
    updatedAt
    settings {{
        __typename
        ... on SnowflakeConnectionSettings {{
            account
            role
            database
            warehouse
            schema
            username
            useKeyPairAuth
            useSso
            clientId
            hasPassword
            hasPrivateKey
            hasPrivateKeyPassphrase
            hasClientSecret
            hasProfile
        }}
        ... on BigQueryConnectionSettings {{
            bigqueryConnectionType: connectionType
            location
            datasetName
            executionProject
            projectId
            clientId
            costGcsBucketName
            costProjectIds
            hasServiceAccountJson
            hasClientSecret
            hasProfile
        }}
        ... on DatabricksConnectionSettings {{
            host
            httpPath
            catalog
            schema
            hasToken
            hasProfile
        }}
        ... on RedshiftConnectionSettings {{
            hostName
            port
            database
            schema
            username
            hasPassword
            hasProfile
        }}
        ... on DuckDbConnectionSettings {{
            duckdbConnectionType: connectionType
            schema
            hasProfile
        }}
        ... on FireboltConnectionSettings {{
            hostName
            accountName
            database
            engineName
            schema
            username
            hasPassword
            hasProfile
        }}
        ... on ProfileConnectionSettings {{
            warehouseType
            schema
            hasProfile
        }}
    }}
"""

WAREHOUSE_CONNECTION_LINKS = frozenset({"environment"})


CONNECTION_TYPE_ALIASES = ("bigqueryConnectionType", "duckdbConnectionType")


class ConnectionScope(OpenEnum):
    SCHEDULER = "SCHEDULER"
    AGENT = "AGENT"
    DEVELOPMENT_SHARED = "DEVELOPMENT_SHARED"
    COST = "COST"
    UNKNOWN = "UNKNOWN"


class WarehouseType(OpenEnum):
    SNOWFLAKE = "SNOWFLAKE"
    BIGQUERY = "BIGQUERY"
    DATABRICKS = "DATABRICKS"
    REDSHIFT = "REDSHIFT"
    DUCKDB = "DUCKDB"
    FIREBOLT = "FIREBOLT"
    TRINO = "TRINO"
    SQL_SERVER = "SQL_SERVER"
    CLICKHOUSE = "CLICKHOUSE"
    FABRIC = "FABRIC"
    UNKNOWN = "UNKNOWN"


class BigQueryConnectionType(OpenEnum):
    OAUTH = "OAUTH"
    SERVICE_ACCOUNT = "SERVICE_ACCOUNT"
    UNKNOWN = "UNKNOWN"


class DuckDbConnectionType(OpenEnum):
    MOTHERDUCK = "MOTHERDUCK"
    DUCKDB = "DUCKDB"
    UNKNOWN = "UNKNOWN"


class SnowflakeConnectionSettings(AdminModel):
    account: Optional[str] = None
    role: Optional[str] = None
    database: Optional[str] = None
    warehouse: Optional[str] = None
    schema_name: Optional[str] = None
    username: Optional[str] = None
    use_key_pair_auth: Optional[bool] = None
    use_sso: Optional[bool] = None
    client_id: Optional[str] = None
    has_password: Optional[bool] = None
    has_private_key: Optional[bool] = None
    has_private_key_passphrase: Optional[bool] = None
    has_client_secret: Optional[bool] = None
    has_profile: Optional[bool] = None


class BigQueryConnectionSettings(AdminModel):
    connection_type: Optional[BigQueryConnectionType] = None
    location: Optional[str] = None
    dataset_name: Optional[str] = None
    execution_project: Optional[str] = None
    project_id: Optional[str] = None
    client_id: Optional[str] = None
    cost_gcs_bucket_name: Optional[str] = None
    cost_project_ids: Optional[List[str]] = None
    has_service_account_json: Optional[bool] = None
    has_client_secret: Optional[bool] = None
    has_profile: Optional[bool] = None


class DatabricksConnectionSettings(AdminModel):
    host: Optional[str] = None
    http_path: Optional[str] = None
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    has_token: Optional[bool] = None
    has_profile: Optional[bool] = None


class RedshiftConnectionSettings(AdminModel):
    host_name: Optional[str] = None
    port: Optional[str] = None
    database: Optional[str] = None
    schema_name: Optional[str] = None
    username: Optional[str] = None
    has_password: Optional[bool] = None
    has_profile: Optional[bool] = None


class DuckDbConnectionSettings(AdminModel):
    connection_type: Optional[DuckDbConnectionType] = None
    schema_name: Optional[str] = None
    has_profile: Optional[bool] = None


class FireboltConnectionSettings(AdminModel):
    host_name: Optional[str] = None
    account_name: Optional[str] = None
    database: Optional[str] = None
    engine_name: Optional[str] = None
    schema_name: Optional[str] = None
    username: Optional[str] = None
    has_password: Optional[bool] = None
    has_profile: Optional[bool] = None


class ProfileConnectionSettings(AdminModel):
    warehouse_type: Optional[WarehouseType] = None
    schema_name: Optional[str] = None
    has_profile: Optional[bool] = None


WarehouseConnectionSettings = Union[
    SnowflakeConnectionSettings,
    BigQueryConnectionSettings,
    DatabricksConnectionSettings,
    RedshiftConnectionSettings,
    DuckDbConnectionSettings,
    FireboltConnectionSettings,
    ProfileConnectionSettings,
]

SETTINGS_BY_TYPENAME: Dict[str, Type[AdminModel]] = {
    settings.__name__: settings
    for settings in (
        SnowflakeConnectionSettings,
        BigQueryConnectionSettings,
        DatabricksConnectionSettings,
        RedshiftConnectionSettings,
        DuckDbConnectionSettings,
        FireboltConnectionSettings,
        ProfileConnectionSettings,
    )
}


class WarehouseConnection(AdminModel):
    scope: ConnectionScope
    environment: Optional[EnvironmentLink] = None
    connection_id: str
    type: WarehouseType
    is_default: Optional[bool] = None
    name: Optional[str] = None
    target_name: Optional[str] = None
    threads: Optional[int] = None
    updated_at: Optional[datetime] = None
    settings: Optional[Any] = None

    @property
    def environment_slug(self) -> Optional[str]:
        return self.environment.slug if self.environment else None

    @validator("settings", pre=True)
    def _parse_settings(cls, value: Any) -> Any:
        if not isinstance(value, dict):
            return value
        settings_class = SETTINGS_BY_TYPENAME.get(value.get("__typename", ""))
        if settings_class is None:
            return None
        settings = dict(value)
        for alias in CONNECTION_TYPE_ALIASES:
            if alias in settings:
                settings["connectionType"] = settings.pop(alias)
        return settings_class.parse_obj(settings)


class WarehouseConnectionTestResult(AdminModel):
    type: WarehouseType
    tested_at: datetime


class SnowflakeConnectionInput(AdminInputModel):
    account: Optional[StrictStr] = None
    role: Optional[StrictStr] = None
    database: Optional[StrictStr] = None
    warehouse: Optional[StrictStr] = None
    schema_name: Optional[StrictStr] = None
    username: Optional[StrictStr] = None
    use_key_pair_auth: Optional[bool] = None
    use_sso: Optional[bool] = None
    password: Optional[StrictStr] = None
    private_key: Optional[StrictStr] = None
    private_key_passphrase: Optional[StrictStr] = None
    client_id: Optional[StrictStr] = None
    client_secret: Optional[StrictStr] = None
    profile: Optional[StrictStr] = None


class BigQueryConnectionInput(AdminInputModel):
    connection_type: Optional[BigQueryConnectionType] = None
    location: Optional[StrictStr] = None
    dataset_name: Optional[StrictStr] = None
    execution_project: Optional[StrictStr] = None
    service_account_json: Optional[StrictStr] = None
    profile: Optional[StrictStr] = None
    project_id: Optional[StrictStr] = None
    client_id: Optional[StrictStr] = None
    client_secret: Optional[StrictStr] = None
    cost_gcs_bucket_name: Optional[StrictStr] = None
    cost_project_ids: Optional[List[StrictStr]] = None


class DatabricksConnectionInput(AdminInputModel):
    host: Optional[StrictStr] = None
    http_path: Optional[StrictStr] = None
    catalog: Optional[StrictStr] = None
    schema_name: Optional[StrictStr] = None
    token: Optional[StrictStr] = None
    profile: Optional[StrictStr] = None


class RedshiftConnectionInput(AdminInputModel):
    host_name: Optional[StrictStr] = None
    port: Optional[StrictStr] = None
    database: Optional[StrictStr] = None
    schema_name: Optional[StrictStr] = None
    username: Optional[StrictStr] = None
    password: Optional[StrictStr] = None
    profile: Optional[StrictStr] = None


class DuckDbConnectionInput(AdminInputModel):
    connection_type: Optional[DuckDbConnectionType] = None
    schema_name: Optional[StrictStr] = None
    profile: Optional[StrictStr] = None


class FireboltConnectionInput(AdminInputModel):
    host_name: Optional[StrictStr] = None
    account_name: Optional[StrictStr] = None
    database: Optional[StrictStr] = None
    engine_name: Optional[StrictStr] = None
    schema_name: Optional[StrictStr] = None
    username: Optional[StrictStr] = None
    password: Optional[StrictStr] = None
    profile: Optional[StrictStr] = None


class ProfileConnectionInput(AdminInputModel):
    warehouse_type: Optional[WarehouseType] = None
    schema_name: Optional[StrictStr] = None
    profile: Optional[StrictStr] = None


class WarehouseAdapterInput(AdminInputModel):
    snowflake: Optional[SnowflakeConnectionInput] = None
    bigquery: Optional[BigQueryConnectionInput] = None
    databricks: Optional[DatabricksConnectionInput] = None
    redshift: Optional[RedshiftConnectionInput] = None
    duckdb: Optional[DuckDbConnectionInput] = None
    firebolt: Optional[FireboltConnectionInput] = None
    profile_connection: Optional[ProfileConnectionInput] = None


ConnectionAdapter = Union[
    WarehouseAdapterInput,
    SnowflakeConnectionInput,
    BigQueryConnectionInput,
    DatabricksConnectionInput,
    RedshiftConnectionInput,
    DuckDbConnectionInput,
    FireboltConnectionInput,
    ProfileConnectionInput,
]

ADAPTER_KEYS: Dict[Type[AdminInputModel], str] = {
    SnowflakeConnectionInput: "snowflake",
    BigQueryConnectionInput: "bigquery",
    DatabricksConnectionInput: "databricks",
    RedshiftConnectionInput: "redshift",
    DuckDbConnectionInput: "duckdb",
    FireboltConnectionInput: "firebolt",
    ProfileConnectionInput: "profile_connection",
}


class WarehouseConnectionInput(AdminInputModel):
    scope: ConnectionScope
    environment_slug: Optional[StrictStr] = None
    connection_id: Optional[StrictStr] = None
    name: Optional[StrictStr] = None
    target_name: Optional[StrictStr] = None
    threads: Optional[int] = None
    adapter: WarehouseAdapterInput


class WarehouseConnectionKeyInput(AdminInputModel):
    scope: ConnectionScope
    environment_slug: Optional[str] = None
    connection_id: Optional[str] = None
