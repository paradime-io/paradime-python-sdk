from typing import List

from paradime.apis.custom_integration_types import (
    Integration,
    Node,
    NodeChartLike,
    NodeDashboardLike,
    NodeDatasourceLike,
    NodeType,
)
from paradime.client.api_client import APIClient
from paradime.client.api_exception import ParadimeException


class CustomIntegration:
    def __init__(self, client: APIClient):
        self.client = client

    def create(self, *, name: str, logo_url: str, node_types: List[NodeType]) -> str:
        query = """
            mutation addCustomIntegration($logoUrl: String!, $name: String!, $nodeTypes: [IntegrationNodeTypeInfo!]!) {
                addCustomIntegration(logoUrl: $logoUrl, name: $name, nodeTypes: $nodeTypes) {
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
        name: str | None = None,
        logo_url: str | None = None,
        node_types: List[NodeType] | None = None,
        active: bool | None = None,
    ) -> None:
        query = """
            mutation updateCustomIntegration($integrationUid: String!, $name: String, $logoUrl: String, $nodeTypes: [IntegrationNodeTypeInfo!], $active: Boolean) {
                updateCustomIntegration(integrationUid: $integrationUid, name: $name, logoUrl: $logoUrl, nodeTypes: $nodeTypes, active: $active) {
                    ok
                }
            }
        """

        variables = {
            "integrationUid": integration_uid,
            "name": name,
            "logoUrl": logo_url,
            "nodeTypes": [
                {
                    "nodeType": node_type.node_type,
                    "iconName": node_type.icon_name,
                    "color": node_type.color,
                }
                for node_type in node_types
            ]
            if node_types
            else None,
            "active": active,
        }

        self.client._call_gql(query, variables)

        return None

    def list(self) -> List[Integration]:
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

    def get_by_name(self, name: str) -> Integration:
        all_integrations = self.list()
        for integration in all_integrations:
            if integration.name == name and integration.is_active:
                return integration

        raise ParadimeException(f"Integration with name {name!r} not found")

    def get(self, uid: str) -> Integration:
        all_integrations = self.list()
        for integration in all_integrations:
            if integration.uid == uid and integration.is_active:
                return integration

        raise ParadimeException(f"Integration with uid {uid!r} not found")

    def upsert(
        self,
        *,
        name: str,
        logo_url: str,
        node_types: List[NodeType],
    ) -> Integration:
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
            integration_uid = self.create(
                name=name, logo_url=logo_url, node_types=node_types
            )
            integration = self.get(integration_uid)

        return integration

    def add_nodes_to_snapshot(
        self,
        *,
        integration_uid: str,
        nodes: List[Node],
        snapshot_has_more_nodes: bool,
        snapshot_id: int | None = None,
    ) -> int:
        query = """
            mutation addCustomIntegrationNodes($chartLikeNodes: [IntegrationNodeChartLike!]!, $dashboardLikeNodes: [IntegrationNodeDashboardLike!]!, $datasourceLikeNodes: [IntegrationNodeDatasourceLike!]!, $integrationUid: String!, $snapshotHasMoreNodes: Boolean!, $snapshotId: Int) {
                addCustomIntegrationNodes(chartLikeNodes: $chartLikeNodes, dashboardLikeNodes: $dashboardLikeNodes, datasourceLikeNodes: $datasourceLikeNodes, integrationUid: $integrationUid, snapshotHasMoreNodes: $snapshotHasMoreNodes, snapshotId: $snapshotId) {
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
                node._to_gql_dict()
                for node in nodes
                if isinstance(node, NodeDashboardLike)
            ],
            "datasourceLikeNodes": [
                node._to_gql_dict()
                for node in nodes
                if isinstance(node, NodeDatasourceLike)
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
