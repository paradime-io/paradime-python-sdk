from typing import List, Optional

from paradime.apis.custom_integration.types import (
    Integration,
    Node,
    NodeChartLike,
    NodeDashboardLike,
    NodeDatasourceLike,
    NodeType,
)
from paradime.client.api_client import APIClient
from paradime.client.api_exception import ParadimeException


class CustomIntegrationClient:
    def __init__(self, client: APIClient):
        self.client = client

    def create(self, *, name: str, logo_url: Optional[str], node_types: List[NodeType]) -> str:
        """
        Creates a custom integration with the specified name, logo URL, and node types.

        Args:
            name (str): The name of the custom integration.
            logo_url (Optional[str]): The URL of the logo for the custom integration. Optional. If not provided, a default logo will be used.
            node_types (List[NodeType]): A list of NodeType objects representing the node types for the custom integration.

        Returns:
            str: The integration UID of the created custom integration.
        """

        query = """
            mutation addCustomIntegration(
                $logoUrl: String,
                $name: String!,
                $nodeTypes: [IntegrationNodeTypeInfo!]!
            ) {
                addCustomIntegration(
                    logoUrl: $logoUrl,
                    name: $name,
                    nodeTypes: $nodeTypes
                ) {
                    ok
                    integrationUid
                }
            }
        """

        variables = {
            "logoUrl": logo_url,
            "name": name,
            "nodeTypes": [
                {
                    "nodeType": node_type.node_type,
                    "iconName": node_type.icon_name,
                    "color": node_type.color,
                }
                for node_type in node_types
            ],
        }

        response = self.client._call_gql(query, variables)

        return response["addCustomIntegration"]["integrationUid"]

    def update(
        self,
        *,
        integration_uid: str,
        name: Optional[str] = None,
        logo_url: Optional[str] = None,
        node_types: Optional[List[NodeType]] = None,
        active: Optional[bool] = None,
    ) -> None:
        """
        Update a custom integration with the specified parameters.
        Only the parameters that are not None will be updated.

        Args:
            integration_uid (str): The unique identifier of the integration.
            name (str, optional): The new name of the integration. Defaults to None.
            logo_url (str, optional): The new logo URL of the integration. Defaults to None.
            node_types (List[NodeType], optional): The new list of node types for the integration. Overrides the existing node types. Defaults to None.
            active (bool, optional): Whether the integration should be active. Defaults to None.
        """

        query = """
            mutation updateCustomIntegration(
                $integrationUid: String!,
                $name: String,
                $logoUrl: String,
                $nodeTypes: [IntegrationNodeTypeInfo!],
                $active: Boolean
            ) {
                updateCustomIntegration(
                    integrationUid: $integrationUid,
                    name: $name,
                    logoUrl: $logoUrl,
                    nodeTypes: $nodeTypes,
                    active: $active
                ) {
                    ok
                }
            }
        """

        variables = {
            "integrationUid": integration_uid,
            "name": name,
            "logoUrl": logo_url,
            "nodeTypes": (
                [node_type._to_gql_dict() for node_type in node_types] if node_types else None
            ),
            "active": active,
        }

        self.client._call_gql(query, variables)

        return None

    def list_all(self) -> List[Integration]:
        """
        Retrieves a list of all custom integrations. This includes both active and inactive integrations.

        Returns:
            List[Integration]: A list of Integration objects representing the custom integrations.
        """

        query = """
            query listCustomIntegrations {
                listCustomIntegrations {
                    ok
                    integrations {
                        uid
                        name
                        isActive
                        nodeTypes {
                            nodeType
                            iconName
                            color
                        }
                    }
                }
            }
        """

        response = self.client._call_gql(query)

        return [
            Integration(
                uid=integration["uid"],
                name=integration["name"],
                is_active=integration["isActive"],
                node_types=[
                    NodeType(
                        node_type=node_type["nodeType"],
                        icon_name=node_type["iconName"],
                        color=node_type["color"],
                    )
                    for node_type in integration["nodeTypes"]
                ],
            )
            for integration in response["listCustomIntegrations"]["integrations"]
        ]

    def list_active(self) -> List[Integration]:
        """
        Retrieves a list of all active custom integrations.

        Returns:
            List[Integration]: A list of Integration objects representing the active custom integrations.
        """

        all_integrations = self.list_all()
        return [integration for integration in all_integrations if integration.is_active]

    def get_by_name(self, name: str) -> Optional[Integration]:
        """
        Retrieves active custom integration with the specified name.

        Args:
            name (str): The name of the integration to retrieve.

        Returns:
            Integration | None: The integration object if found, None otherwise.
        """

        active_integrations = self.list_active()
        for integration in active_integrations:
            if integration.name == name:
                return integration

        return None

    def get(self, uid: str) -> Optional[Integration]:
        """
        Retrieves an integration with the specified UID.

        Args:
            uid (str): The UID of the integration to retrieve.

        Returns:
            Integration | None: The integration object if found, None otherwise.
        """

        all_integrations = self.list_active()
        for integration in all_integrations:
            if integration.uid == uid:
                return integration

        return None

    def upsert(
        self,
        *,
        name: str,
        logo_url: Optional[str] = None,
        node_types: List[NodeType],
    ) -> Integration:
        """
        Upserts an integration by either updating an existing integration with the given name or creating a new integration if it doesn't exist.

        Args:
            name (str): The name of the integration.
            logo_url (str): The URL of the integration's logo.
            node_types (List[NodeType]): A list of node types associated with the integration.

        Returns:
            Integration: The upserted integration.
        """

        integration = self.get_by_name(name)
        if integration:
            self.update(
                integration_uid=integration.uid,
                name=name,
                logo_url=logo_url,
                node_types=node_types,
                active=True,
            )
        else:
            integration_uid = self.create(name=name, logo_url=logo_url, node_types=node_types)
            integration = self.get(integration_uid)
            if not integration:
                raise ParadimeException(
                    f"Failed to find integration with uid {integration_uid!r}. This should not have happened and is likely a bug. Please contact support."
                )

        return integration

    def add_nodes_to_snapshot(
        self,
        *,
        integration_uid: str,
        nodes: List[Node],
        snapshot_has_more_nodes: bool,
        snapshot_id: Optional[int] = None,
    ) -> int:
        """
        Adds nodes to a snapshot in the custom integration.

        A snapshot is a collection of nodes that are added to the custom integration.
        The nodes are added in batches, and the snapshot ID is used to keep track of the nodes added to the snapshot.
        To update the nodes of an integration, a new snapshot is created and the nodes are added to the snapshot.

        This method is useful when adding a large number of nodes to the custom integration.
        It allows adding nodes in batches and keeps track of the nodes added to the snapshot.

        Args:
            integration_uid (str): The UID of the custom integration.
            nodes (List[Node]): The list of nodes to be added to the snapshot.
            snapshot_has_more_nodes (bool): Indicates whether the snapshot has more nodes. Set False when adding the last batch of nodes.
            snapshot_id (int, optional): The ID of the snapshot. Defaults to None. If not provided, a new snapshot will be created. If provided, the nodes will be added to the existing snapshot.

        Returns:
            int: The ID of the snapshot after adding the nodes.
        """

        query = """
            mutation addCustomIntegrationNodes(
                $chartLikeNodes: [IntegrationNodeChartLike!]!,
                $dashboardLikeNodes: [IntegrationNodeDashboardLike!]!,
                $datasourceLikeNodes: [IntegrationNodeDatasourceLike!]!,
                $integrationUid: String!,
                $snapshotHasMoreNodes: Boolean!,
                $snapshotId: Int
            ) {
                addCustomIntegrationNodes(
                    chartLikeNodes: $chartLikeNodes,
                    dashboardLikeNodes: $dashboardLikeNodes,
                    datasourceLikeNodes: $datasourceLikeNodes,
                    integrationUid: $integrationUid,
                    snapshotHasMoreNodes: $snapshotHasMoreNodes,
                    snapshotId: $snapshotId
                ) {
                    ok
                    snapshotId
                }
            }
        """

        variables = {
            "chartLikeNodes": [
                node._to_gql_dict() for node in nodes if isinstance(node, NodeChartLike)
            ],
            "dashboardLikeNodes": [
                node._to_gql_dict() for node in nodes if isinstance(node, NodeDashboardLike)
            ],
            "datasourceLikeNodes": [
                node._to_gql_dict() for node in nodes if isinstance(node, NodeDatasourceLike)
            ],
            "integrationUid": integration_uid,
            "snapshotHasMoreNodes": snapshot_has_more_nodes,
            "snapshotId": snapshot_id,
        }

        response = self.client._call_gql(query, variables)

        return response["addCustomIntegrationNodes"]["snapshotId"]

    def add_nodes(
        self,
        *,
        integration_uid: str,
        nodes: List[Node],
    ) -> None:
        """
        Adds all nodes to a new snapshot in the custom integration.

        To add nodes in a streaming fashion, use the add_nodes_to_snapshot method.

        Args:
            integration_uid (str): The unique identifier of the integration.
            nodes (List[Node]): The list of all nodes to be added to the integration.
        """
        num_nodes_per_request = 10
        num_of_requests = len(nodes) // num_nodes_per_request + 1

        snapshot_id = None

        for i in range(num_of_requests):
            start = i * num_nodes_per_request
            end = (i + 1) * num_nodes_per_request
            nodes_to_add = nodes[start:end]

            if i == num_of_requests - 1:
                snapshot_has_more_nodes = False
            else:
                snapshot_has_more_nodes = True

            snapshot_id = self.add_nodes_to_snapshot(
                integration_uid=integration_uid,
                nodes=nodes_to_add,
                snapshot_has_more_nodes=snapshot_has_more_nodes,
                snapshot_id=snapshot_id,
            )
