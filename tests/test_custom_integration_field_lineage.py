import pytest

from paradime.apis.custom_integration.types import (
    FieldDependencyCustomIntegrationField,
    FieldDependencyNativeIntegrationField,
    FieldDependencySqlColumn,
    FieldTransformation,
    NativeIntegrationNodeType,
    NodeChartLikeAttributesField,
    NodeDatasourceLikeAttributesField,
)


def test_a_field_without_lineage_serialises_as_before() -> None:
    field = NodeChartLikeAttributesField(
        name="revenue", description=None, type="measure", data_type="number"
    )
    assert field._to_gql_dict() == {
        "name": "revenue",
        "description": "",
        "type": "measure",
        "dataType": "number",
    }


def test_upstream_fields_serialise_each_target() -> None:
    field = NodeChartLikeAttributesField(
        name="revenue",
        description="",
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
            FieldDependencyNativeIntegrationField(
                node_type=NativeIntegrationNodeType.SIGMA_ELEMENT,
                node_id="element-1",
                field_name="Amount",
            ),
            FieldDependencyCustomIntegrationField(
                node_type="Loader", node_name="Orders loader", field_name="order_amount"
            ),
        ],
    )
    assert field._to_gql_dict()["upstreamFields"] == [
        {
            "sqlColumn": {
                "database": None,
                "schema": "marts",
                "table": "orders",
                "column": "amount",
            },
            "transformation": "AGGREGATION",
            "expression": "SUM(amount)",
        },
        {
            "nativeIntegrationField": {
                "nodeType": "SigmaElement",
                "nodeId": "element-1",
                "nodeName": None,
                "fieldName": "Amount",
            }
        },
        {
            "customIntegrationField": {
                "nodeType": "Loader",
                "integrationUid": None,
                "integrationName": None,
                "nodeStableId": None,
                "nodeName": "Orders loader",
                "fieldName": "order_amount",
            }
        },
    ]
    assert "downstreamFields" not in field._to_gql_dict()


def test_downstream_fields_on_a_datasource_field() -> None:
    field = NodeDatasourceLikeAttributesField(
        name="order_amount",
        description="",
        type="dimension",
        data_type="number",
        downstream_fields=[
            FieldDependencySqlColumn(table_name="orders", column_name="amount"),
        ],
    )
    assert field._to_gql_dict()["downstreamFields"] == [
        {
            "sqlColumn": {
                "database": None,
                "schema": None,
                "table": "orders",
                "column": "amount",
            }
        }
    ]


def test_a_field_reference_needs_a_field_name() -> None:
    with pytest.raises(ValueError):
        FieldDependencyCustomIntegrationField(node_type="Loader", node_name="Orders loader")  # type: ignore[call-arg]
