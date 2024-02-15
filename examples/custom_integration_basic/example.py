from paradime import Paradime
from paradime.apis.custom_integration_types import (
    Lineage,
    LineageDependencyCustomIntegration,
    LineageDependencyDbtObject,
    NodeChartLike,
    NodeChartLikeAttributes,
    NodeColor,
    NodeDashboardLike,
    NodeDashboardLikeAttributes,
    NodeDatasourceLike,
    NodeDatasourceLikeAttributes,
    NodeType,
)

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

# Setup a custom integration
my_integration = paradime.custom_integration.upsert(
    name="MyParadimeIntegration",
    logo_url="https://example.com/logo.png",  # Optional, replace with your logo URL, or remove this line.
    node_types=[
        NodeType(
            node_type="ParaDatasource",
            icon_name="database",  # Optional, replace with your icon name, or remove this line. Icons are from: https://blueprintjs.com/docs/#icons/icons-list
            color=NodeColor.ORANGE,  # Optional, replace with your color, or remove this line.
        ),
        NodeType(
            node_type="ParaChart",
            icon_name="pie-chart",
            color=NodeColor.TEAL,
        ),
        NodeType(
            node_type="ParaDashboard",
            icon_name="dashboard",
            color=NodeColor.CORAL,
        ),
    ],
)

# Add nodes to the custom integration.
#
# This example adds a datasource, a chart, and a dashboard to the custom integration.
# The chart has an upstream dependency on the datasource and a downstream dependency on the dashboard.
# The datasource has an upstream dependency on a dbt model named "order_items".
#
# So effectively,'order_items' -> 'My Datasource 1' -> 'My Chart 1' -> 'My Dashboard 1'
paradime.custom_integration.add_nodes(
    integration_uid=my_integration.uid,
    nodes=[
        NodeDatasourceLike(
            name="My Datasource 1",
            node_type="ParaDatasource",
            attributes=NodeDatasourceLikeAttributes(
                description="This is my first datasource",
                # Add more attributes here
            ),
            lineage=Lineage(
                upstream_dependencies=[
                    LineageDependencyDbtObject(
                        table_name="order_items",
                    ),
                ],
            ),
        ),
        NodeChartLike(
            name="My Chart 1",
            node_type="ParaChart",
            attributes=NodeChartLikeAttributes(
                description="This is my first chart",
                # Add more attributes here
            ),
            lineage=Lineage(
                upstream_dependencies=[
                    LineageDependencyCustomIntegration(
                        integration_name=my_integration.name,
                        node_type="ParaDatasource",
                        node_name="My Datasource 1",
                    ),
                ],
                downstream_dependencies=[
                    LineageDependencyCustomIntegration(
                        integration_name=my_integration.name,
                        node_type="ParaDashboard",
                        node_name="My Dashboard 1",
                    ),
                ],
            ),
        ),
        NodeDashboardLike(
            name="My Dashboard 1",
            node_type="ParaDashboard",
            attributes=NodeDashboardLikeAttributes(
                description="This is my first dashboard",
                # Add more attributes here
            ),
            lineage=Lineage(
                upstream_dependencies=[],
                downstream_dependencies=[],
            ),
        ),
    ],
)
