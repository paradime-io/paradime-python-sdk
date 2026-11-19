from paradime import Paradime
from paradime.apis.custom_integration.types import (
    FieldDependencyCustomIntegrationField,
    FieldDependencySqlColumn,
    FieldTransformation,
    Lineage,
    LineageDependencyCustomIntegration,
    LineageDependencyDbtObject,
    NodeChartLike,
    NodeChartLikeAttributes,
    NodeChartLikeAttributesField,
    NodeDatasourceLike,
    NodeDatasourceLikeAttributes,
    NodeDatasourceLikeAttributesField,
    NodeType,
)

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

my_integration = paradime.custom_integration.upsert(
    name="MyFieldLineageIntegration",
    node_types=[
        NodeType(node_type="Loader", icon_name="import"),
        NodeType(node_type="Chart", icon_name="pie-chart"),
    ],
)

# A loader that writes into a warehouse table. The table is downstream of the loader, so its
# fields declare the warehouse columns they feed with `downstream_fields`.
loader = NodeDatasourceLike(
    name="Orders loader",
    node_type="Loader",
    attributes=NodeDatasourceLikeAttributes(
        fields=[
            NodeDatasourceLikeAttributesField(
                name="order_amount",
                description="Order amount, as read from the source system",
                type="dimension",
                data_type="number",
                downstream_fields=[
                    FieldDependencySqlColumn(
                        database_name="analytics",
                        schema_name="raw",
                        table_name="orders",
                        column_name="amount",
                    ),
                ],
            ),
        ],
    ),
    lineage=Lineage(
        downstream_dependencies=[
            LineageDependencyDbtObject(
                database_name="analytics", schema_name="raw", table_name="orders"
            ),
        ],
    ),
)

# A chart built on a dbt model. Its fields declare the columns they read with `upstream_fields`.
chart = NodeChartLike(
    name="Revenue by month",
    node_type="Chart",
    attributes=NodeChartLikeAttributes(
        fields=[
            NodeChartLikeAttributesField(
                name="revenue",
                description="Total revenue",
                type="measure",
                data_type="number",
                upstream_fields=[
                    FieldDependencySqlColumn(
                        schema_name="marts",
                        table_name="orders",
                        column_name="amount",
                        transformation=FieldTransformation.AGGREGATION,
                        expression="SUM(amount)",
                    ),
                ],
            ),
            NodeChartLikeAttributesField(
                name="order_amount_from_loader",
                description="The loader's field, referenced directly",
                type="dimension",
                data_type="number",
                upstream_fields=[
                    FieldDependencyCustomIntegrationField(
                        node_type="Loader",
                        node_name="Orders loader",
                        field_name="order_amount",
                    ),
                ],
            ),
        ],
    ),
    lineage=Lineage(
        upstream_dependencies=[
            LineageDependencyDbtObject(schema_name="marts", table_name="orders"),
            LineageDependencyCustomIntegration(node_type="Loader", node_name="Orders loader"),
        ],
    ),
)

paradime.custom_integration.add_nodes(integration_uid=my_integration.uid, nodes=[loader, chart])
