from paradime.apis.custom_integration_types import Integration, NodeType
from paradime.client.api_client import APIClient


class CustomIntegration:
    def __init__(self, client: APIClient):
        self.client = client

    def new(
        self, *, name: str, logo_url: str, node_types: list[NodeType]
    ) -> Integration:
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

        return Integration(
            uid=response["addCustomIntegration"]["integrationUid"],
            name=name,
            is_active=True,
            node_types=node_types,
        )
