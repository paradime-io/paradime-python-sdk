from datetime import datetime
from typing import Any, Dict, List, Optional, Type, Union

from paradime.apis.admin.types import AdminInputModel, AdminModel, OpenEnum
from paradime.tools.pydantic import StrictStr, validator

WAREHOUSE_CONNECTION_FIELDS = """
    scope
    environmentSlug
    connectionId
    type
    isDefault
    connectionName
    targetName
    threads
    updatedAt
    settings {
        __typename
        ... on SnowflakeConnectionSettings {
            account
            role
            database
            warehouse
            schema
            username
            useKeyPairAuth
            useSso
            clientId
            passwordIsSet
            privateKeyIsSet
            privateKeyPassphraseIsSet
            clientSecretIsSet
            profileIsSet
        }
        ... on BigQueryConnectionSettings {
            bigqueryConnectionType: connectionType
            location
            datasetName
            executionProject
            projectId
            clientId
            costGcsBucketName
            costProjectIds
            serviceAccountJsonIsSet
            clientSecretIsSet
            profileIsSet
        }
        ... on DatabricksConnectionSettings {
            host
            httpPath
            catalog
            schema
            tokenIsSet
            profileIsSet
        }
        ... on RedshiftConnectionSettings {
            hostName
            port
            database
            schema
            username
            passwordIsSet
            profileIsSet
        }
        ... on DuckDbConnectionSettings {
            duckdbConnectionType: connectionType
            schema
            profileIsSet
        }
        ... on FireboltConnectionSettings {
            hostName
            accountName
            database
            engineName
            schema
            username
            passwordIsSet
            profileIsSet
        }
        ... on ProfileConnectionSettings {
            warehouseType
            schema
            profileIsSet
        }
    }
"""


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
    password_is_set: Optional[bool] = None
    private_key_is_set: Optional[bool] = None
    private_key_passphrase_is_set: Optional[bool] = None
    client_secret_is_set: Optional[bool] = None
    profile_is_set: Optional[bool] = None


class BigQueryConnectionSettings(AdminModel):
    connection_type: Optional[BigQueryConnectionType] = None
    location: Optional[str] = None
    dataset_name: Optional[str] = None
    execution_project: Optional[str] = None
    project_id: Optional[str] = None
    client_id: Optional[str] = None
    cost_gcs_bucket_name: Optional[str] = None
    cost_project_ids: Optional[List[str]] = None
    service_account_json_is_set: Optional[bool] = None
    client_secret_is_set: Optional[bool] = None
    profile_is_set: Optional[bool] = None


class DatabricksConnectionSettings(AdminModel):
    host: Optional[str] = None
    http_path: Optional[str] = None
    catalog: Optional[str] = None
    schema_name: Optional[str] = None
    token_is_set: Optional[bool] = None
    profile_is_set: Optional[bool] = None


class RedshiftConnectionSettings(AdminModel):
    host_name: Optional[str] = None
    port: Optional[str] = None
    database: Optional[str] = None
    schema_name: Optional[str] = None
    username: Optional[str] = None
    password_is_set: Optional[bool] = None
    profile_is_set: Optional[bool] = None


class DuckDbConnectionSettings(AdminModel):
    connection_type: Optional[DuckDbConnectionType] = None
    schema_name: Optional[str] = None
    profile_is_set: Optional[bool] = None


class FireboltConnectionSettings(AdminModel):
    host_name: Optional[str] = None
    account_name: Optional[str] = None
    database: Optional[str] = None
    engine_name: Optional[str] = None
    schema_name: Optional[str] = None
    username: Optional[str] = None
    password_is_set: Optional[bool] = None
    profile_is_set: Optional[bool] = None


class ProfileConnectionSettings(AdminModel):
    warehouse_type: Optional[WarehouseType] = None
    schema_name: Optional[str] = None
    profile_is_set: Optional[bool] = None


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
    environment_slug: Optional[str] = None
    connection_id: str
    type: WarehouseType
    is_default: Optional[bool] = None
    connection_name: Optional[str] = None
    target_name: Optional[str] = None
    threads: Optional[int] = None
    updated_at: Optional[datetime] = None
    settings: Optional[Any] = None

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
    connection_name: Optional[StrictStr] = None
    target_name: Optional[StrictStr] = None
    threads: Optional[int] = None
    adapter: WarehouseAdapterInput


class WarehouseConnectionKeyInput(AdminInputModel):
    scope: ConnectionScope
    environment_slug: Optional[str] = None
    connection_id: Optional[str] = None
