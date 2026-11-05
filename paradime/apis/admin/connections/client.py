from typing import Any, Dict, Iterator, Optional

from paradime.apis.admin.connections.types import (
    ADAPTER_KEYS,
    WAREHOUSE_CONNECTION_FIELDS,
    ConnectionAdapter,
    ConnectionScope,
    WarehouseAdapterInput,
    WarehouseConnection,
    WarehouseConnectionInput,
    WarehouseConnectionKeyInput,
    WarehouseConnectionTestResult,
)
from paradime.apis.admin.transport import AdminTransport
from paradime.apis.admin.types import (
    CONNECTION_FIELDS,
    DEFAULT_PAGE_SIZE,
    UNSET,
    Page,
    check_page_size,
    given,
    iterate_pages,
    parse_page,
    serialize_input,
)


def adapter_input(adapter: ConnectionAdapter) -> WarehouseAdapterInput:
    if isinstance(adapter, WarehouseAdapterInput):
        return adapter
    key = ADAPTER_KEYS.get(type(adapter))
    if key is None:
        raise TypeError(f"Unsupported warehouse adapter: {type(adapter).__name__}")
    return WarehouseAdapterInput.parse_obj({key: adapter})


class AdminConnectionsClient:
    """
    Data warehouse connections of Bolt (`SCHEDULER`) and agent (`AGENT`) environments.
    Secrets are write-only: reads say only whether each is set, and an update that leaves one
    out keeps the stored value.
    """

    def __init__(self, transport: AdminTransport) -> None:
        self.transport = transport

    def list(
        self,
        *,
        scope: Optional[ConnectionScope] = None,
        environment_slug: Optional[str] = None,
        workspace_uid: Optional[str] = None,
        page_size: int = DEFAULT_PAGE_SIZE,
    ) -> Iterator[WarehouseConnection]:
        """
        Iterates over the workspace's warehouse connections, ordered by environment slug and
        connection id.

        Args:
            scope (ConnectionScope, optional): Only connections of this scope.
            environment_slug (str, optional): Only this environment's connections.
            workspace_uid (str, optional): The workspace to read, for a company-level key.
            page_size (int): How many connections to read per request, at most 500.

        Returns:
            Iterator[WarehouseConnection]: The connections, read a page at a time.
        """

        check_page_size(page_size)
        query = f"""
            query adminWarehouseConnections(
                $workspaceUid: String
                $scope: ConnectionScope
                $environmentSlug: String
                $first: Int
                $after: String
            ) {{
                workspace(uid: $workspaceUid) {{
                    warehouseConnections(
                        scope: $scope
                        environmentSlug: $environmentSlug
                        first: $first
                        after: $after
                    ) {{
                        edges {{
                            node {{
                                {WAREHOUSE_CONNECTION_FIELDS}
                            }}
                        }}
                        {CONNECTION_FIELDS}
                    }}
                }}
            }}
        """

        def fetch_page(after: Optional[str]) -> Page[WarehouseConnection]:
            response = self.transport.execute(
                query,
                {
                    "workspaceUid": workspace_uid,
                    "scope": ConnectionScope(scope).value if scope else None,
                    "environmentSlug": environment_slug,
                    "first": page_size,
                    "after": after,
                },
                workspace_uid=workspace_uid,
            )
            workspace = response["workspace"] or {}
            return parse_page(workspace.get("warehouseConnections"), WarehouseConnection.parse_obj)

        return iterate_pages(fetch_page)

    def test(
        self,
        adapter: ConnectionAdapter,
        *,
        scope: ConnectionScope,
        environment_slug: Optional[str] = None,
        connection_id: Optional[str] = None,
        connection_name: Optional[str] = None,
        target_name: Optional[str] = None,
        threads: Optional[int] = None,
        workspace_uid: Optional[str] = None,
    ) -> WarehouseConnectionTestResult:
        """
        Opens a connection with these settings and saves nothing. Secrets left out are taken
        from the saved connection.

        Args:
            adapter: The warehouse settings: a `WarehouseAdapterInput` or one adapter's input,
                such as `SnowflakeConnectionInput`.
            scope (ConnectionScope): `SCHEDULER` or `AGENT`.
            environment_slug (str, optional): The environment's slug.
            connection_id (str, optional): The connection's id within its environment;
                `default` when left out.
            connection_name (str, optional): A name for the connection.
            target_name (str, optional): The dbt target name runs see.
            threads (int, optional): How many threads dbt runs with.
            workspace_uid (str, optional): The workspace to use, for a company-level key.

        Returns:
            WarehouseConnectionTestResult: The warehouse the connection opened, and when.

        Raises:
            AdminConnectionTestFailedException: When the warehouse refuses the connection.
        """

        query = """
            mutation adminTestWarehouseConnection($input: TestWarehouseConnectionInput!) {
                testWarehouseConnection(input: $input) {
                    type
                    testedAt
                }
            }
        """

        variables = self._connection_variables(
            adapter,
            scope=scope,
            environment_slug=environment_slug,
            connection_id=connection_id,
            connection_name=connection_name,
            target_name=target_name,
            threads=threads,
        )
        response = self.transport.execute(query, variables, workspace_uid=workspace_uid)
        return WarehouseConnectionTestResult.parse_obj(response["testWarehouseConnection"])

    def set(
        self,
        adapter: ConnectionAdapter,
        *,
        scope: ConnectionScope,
        environment_slug: Optional[str] = None,
        connection_id: Optional[str] = None,
        connection_name: Optional[str] = None,
        target_name: Optional[str] = None,
        threads: Optional[int] = None,
        workspace_uid: Optional[str] = None,
    ) -> WarehouseConnection:
        """
        Tests, then saves a connection: an upsert by scope, environment and connection id.
        Sending the same input again saves nothing new.

        Args:
            adapter: The warehouse settings; see `test`.
            scope (ConnectionScope): `SCHEDULER` or `AGENT`.
            environment_slug (str, optional): The environment's slug.
            connection_id (str, optional): The connection's id; `default` when left out.
            connection_name (str, optional): A name; required for a new connection.
            target_name (str, optional): The dbt target name; required for a new connection.
            threads (int, optional): dbt threads; required for a new connection.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            WarehouseConnection: The saved connection.

        Raises:
            AdminConnectionTestFailedException: When the warehouse refuses the connection.
        """

        query = f"""
            mutation adminSetWarehouseConnection($input: SetWarehouseConnectionInput!) {{
                setWarehouseConnection(input: $input) {{
                    warehouseConnection {{
                        {WAREHOUSE_CONNECTION_FIELDS}
                    }}
                }}
            }}
        """

        variables = self._connection_variables(
            adapter,
            scope=scope,
            environment_slug=environment_slug,
            connection_id=connection_id,
            connection_name=connection_name,
            target_name=target_name,
            threads=threads,
        )
        response = self.transport.execute(query, variables, workspace_uid=workspace_uid)
        return WarehouseConnection.parse_obj(
            response["setWarehouseConnection"]["warehouseConnection"]
        )

    def delete(
        self,
        *,
        scope: ConnectionScope,
        environment_slug: Optional[str] = None,
        connection_id: Optional[str] = None,
        workspace_uid: Optional[str] = None,
    ) -> str:
        """
        Deletes a connection. Deleting it again is not an error.

        Args:
            scope (ConnectionScope): `SCHEDULER` or `AGENT`.
            environment_slug (str, optional): The environment's slug.
            connection_id (str, optional): The connection's id; `default` when left out.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            str: The id of the deleted connection.
        """

        query = """
            mutation adminDeleteWarehouseConnection($input: DeleteWarehouseConnectionInput!) {
                deleteWarehouseConnection(input: $input) {
                    deletedConnectionId
                }
            }
        """

        key = WarehouseConnectionKeyInput(
            scope=scope,
            **given(
                environment_slug=environment_slug or UNSET,
                connection_id=connection_id or UNSET,
            ),
        )
        response = self.transport.execute(
            query, {"input": serialize_input(key)}, workspace_uid=workspace_uid
        )
        return response["deleteWarehouseConnection"]["deletedConnectionId"]

    def set_default(
        self,
        connection_id: str,
        *,
        scope: ConnectionScope,
        environment_slug: Optional[str] = None,
        workspace_uid: Optional[str] = None,
    ) -> WarehouseConnection:
        """
        Makes a connection its environment's default.

        Args:
            connection_id (str): The connection's id within its environment.
            scope (ConnectionScope): `SCHEDULER` or `AGENT`.
            environment_slug (str, optional): The environment's slug.
            workspace_uid (str, optional): The workspace to change, for a company-level key.

        Returns:
            WarehouseConnection: The connection, now the default.
        """

        query = f"""
            mutation adminSetDefaultWarehouseConnection(
                $input: SetDefaultWarehouseConnectionInput!
            ) {{
                setDefaultWarehouseConnection(input: $input) {{
                    warehouseConnection {{
                        {WAREHOUSE_CONNECTION_FIELDS}
                    }}
                }}
            }}
        """

        key = WarehouseConnectionKeyInput(
            scope=scope,
            connection_id=connection_id,
            **given(environment_slug=environment_slug or UNSET),
        )
        response = self.transport.execute(
            query, {"input": serialize_input(key)}, workspace_uid=workspace_uid
        )
        return WarehouseConnection.parse_obj(
            response["setDefaultWarehouseConnection"]["warehouseConnection"]
        )

    def _connection_variables(
        self, adapter: ConnectionAdapter, *, scope: ConnectionScope, **settings: Any
    ) -> Dict[str, Any]:
        connection_input = WarehouseConnectionInput(
            scope=scope,
            adapter=adapter_input(adapter),
            **{name: value for name, value in settings.items() if value is not None},
        )
        return {"input": serialize_input(connection_input)}
