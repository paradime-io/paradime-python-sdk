from enum import Enum
from typing import Any, Dict, List, Optional, Union

from paradime.apis.custom_integration.utils import generate_uid
from paradime.tools.pydantic import BaseModel, Extra


class ParadimeBaseModel(BaseModel):
    class Config:
        allow_mutation = False
        frozen = True
        validate_all = True
        validate_assignment = True
        extra = Extra.forbid


class NodeColor(str, Enum):
    """
    Represents the colors available for the node types.
    """

    LEAF = "LEAF"
    CYAN = "CYAN"
    CORAL = "CORAL"
    VIOLET = "VIOLET"
    ORANGE = "ORANGE"
    MANDY = "MANDY"
    TEAL = "TEAL"
    GREEN = "GREEN"


class NodeType(ParadimeBaseModel):
    """
    Represents a node type for the integration.

    Attributes:
        node_type (str): Name of the node type. This can be any string. For example, "Dashboard", "Look", "Worksheet", etc.
        icon_name (str, optional): The name of the icon associated with the node type. Refer: https://blueprintjs.com/docs/#icons/icons-list for the list of available icons. Default is 'intersection'.
        color (NodeColor, optional): The color associated with the node type. Available colors are: LEAF, CYAN, CORAL, VIOLET, ORANGE, MANDY, TEAL, GREEN. Default is ORANGE.
    """

    node_type: str
    icon_name: Optional[str] = None
    color: Optional[NodeColor] = None

    def _to_gql_dict(self) -> Dict[str, Any]:
        return {"nodeType": self.node_type, "iconName": self.icon_name, "color": self.color}


class Integration(ParadimeBaseModel):
    """
    Represents a custom integration.

    Attributes:
        uid (str): The unique identifier of the integration.
        name (str): The name of the integration.
        is_active (bool): Indicates whether the integration is active or not.
        node_types (list[NodeType]): The list of node types associated with the integration.
    """

    uid: str
    name: str
    is_active: bool
    node_types: List[NodeType]


# -------- Lineage --------


class LineageDependencyDbtObject(ParadimeBaseModel):
    """
    Represents a dbt object in the lineage. Use this to connect the integration nodes with dbt objects.
    At least the table_name should be provided. All other fields are optional, but it is recommended to provide them for better lineage tracking.

    Attributes:
        database_name (str, optional): The name of the database.
        schema_name (str, optional): The name of the schema.
        table_name (str): The name of the table.
    """

    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: str

    def _to_gql_dict(self) -> Dict[str, Any]:
        return {
            "database": self.database_name or "",
            "schema": self.schema_name or "",
            "table": self.table_name,
        }


class NativeIntegrationNodeType(Enum):
    """
    Represents the types of integration nodes that are native to Paradime.
    Use these to connect the custom integration nodes with native integration nodes.
    """

    TABLEAU_DATASOURCE = "TableauDatasource"
    TABLEAU_WORKSHEET = "TableauWorksheet"
    TABLEAU_DASHBOARD = "TableauDashboard"
    TABLEAU_WORKBOOK = "TableauWorkbook"
    TABLEAU_FLOW = "TableauFlow"
    LOOKER_LOOK = "LookerLook"
    LOOKER_VIEW = "LookerView"
    LOOKER_MODEL = "LookerModel"
    LOOKER_EXPLORE = "LookerExplore"
    LOOKER_DASHBOARD = "LookerDashboard"
    LOOKER_SCHEDULE = "LookerSchedule"
    FIVETRAN_CONNECTOR = "FivetranConnector"
    HIGHTOUCH_MODEL = "HightouchModel"
    HIGHTOUCH_SYNC = "HightouchSync"
    POWER_BI_DATASET = "PowerBIDataset"
    POWER_BI_REPORT = "PowerBIReport"
    POWER_BI_DASHBOARD = "PowerBIDashboard"
    THOUGHTSPOT_WORKSHEET = "ThoughtSpotWorksheet"
    THOUGHTSPOT_ANSWER = "ThoughtSpotAnswer"
    THOUGHTSPOT_VISUALIZATION = "ThoughtSpotVisualization"
    THOUGHTSPOT_LIVEBOARD = "ThoughtSpotLiveboard"
    SIGMA_WORKBOOK = "SigmaWorkbook"
    SIGMA_PAGE = "SigmaPage"
    SIGMA_ELEMENT = "SigmaElement"


class LineageDependencyNativeIntegration(ParadimeBaseModel):
    """
    Represents a native integration in the lineage dependency. Use this to connect the integration nodes with Paradime native integration nodes.
    node_type should be one of the values from NativeIntegrationNodeType.
    At least one of node_id or node_name should be provided. Both can be provided as well.

    Attributes:
        node_type (NativeIntegrationNodeType): The type of the native integration node.
        node_id (str, optional): The unique identifier of the native integration node. Optional, but required if node_name is not provided.
        node_name (str, optional): The name of the native integration node. Optional, but required if node_id is not provided.
    """

    node_type: NativeIntegrationNodeType
    node_id: Optional[str]
    node_name: Optional[str]

    def _to_gql_dict(self) -> Dict[str, Any]:
        return {
            "nodeType": self.node_type.value,
            "nodeId": self.node_id or "",
            "nodeName": self.node_name or "",
        }


class LineageDependencyCustomIntegration(ParadimeBaseModel):
    """
    Represents a custom integration for lineage dependency. Use this to connect the integration nodes with other custom integration nodes.
    At least one of integration_uid or integration_name should be provided. Both can be provided as well.
    At least one of node_id or node_name should be provided. Both can be provided as well.

    Attributes:
        node_type (str): The type of the custom integration node.
        integration_uid (str, optional): The unique identifier of the custom integration. Optional, but required if integration_name is not provided.
        integration_name (str, optional): The name of the custom integration. Optional, but required if integration_uid is not provided.
        node_id (str, optional): The unique identifier of the custom integration node. Optional, but required if node_name is not provided.
        node_name (str, optional): The name of the custom integration node. Optional, but required if node_id is not provided.
    """

    node_type: str
    integration_uid: Optional[str] = None
    integration_name: Optional[str] = None
    node_id: Optional[str] = None
    node_name: Optional[str] = None

    def _to_gql_dict(self) -> Dict[str, Any]:
        return {
            "nodeType": self.node_type or "",
            "integrationUid": self.integration_uid or "",
            "integrationName": self.integration_name or "",
            "nodeStableId": self.node_id or "",
            "nodeName": self.node_name or "",
        }


LineageDependency = Union[
    LineageDependencyDbtObject,
    LineageDependencyNativeIntegration,
    LineageDependencyCustomIntegration,
]


class Lineage(ParadimeBaseModel):
    """
    Represents the lineage of a node. Use this to connect the integration nodes with other nodes.

    Attributes:
        upstream_dependencies (list[LineageDependency], optional): The list of upstream dependencies. Defaults to [].
        downstream_dependencies (list[LineageDependency], optional): The list of downstream dependencies. Defaults to [].
    """

    upstream_dependencies: List[LineageDependency] = []
    downstream_dependencies: List[LineageDependency] = []

    def _to_gql_dict(self) -> Dict[str, Any]:
        return {
            "upstreamReferences": {
                "referencesToSqlTables": [
                    edge._to_gql_dict()
                    for edge in self.upstream_dependencies
                    if isinstance(edge, LineageDependencyDbtObject)
                ],
                "referencesToNativeIntegrations": [
                    edge._to_gql_dict()
                    for edge in self.upstream_dependencies
                    if isinstance(edge, LineageDependencyNativeIntegration)
                ],
                "referencesToCustomIntegrations": [
                    edge._to_gql_dict()
                    for edge in self.upstream_dependencies
                    if isinstance(edge, LineageDependencyCustomIntegration)
                ],
            },
            "downstreamReferences": {
                "referencesToSqlTables": [
                    edge._to_gql_dict()
                    for edge in self.downstream_dependencies
                    if isinstance(edge, LineageDependencyDbtObject)
                ],
                "referencesToNativeIntegrations": [
                    edge._to_gql_dict()
                    for edge in self.downstream_dependencies
                    if isinstance(edge, LineageDependencyNativeIntegration)
                ],
                "referencesToCustomIntegrations": [
                    edge._to_gql_dict()
                    for edge in self.downstream_dependencies
                    if isinstance(edge, LineageDependencyCustomIntegration)
                ],
            },
        }


# -------- Field-level lineage --------


class FieldTransformation(str, Enum):
    """
    How a field is derived from the field it references.

    DIRECT: the value passes through unchanged (renamed or not).
    AGGREGATION: the value is aggregated, e.g. SUM(amount).
    EXPRESSION: the value is computed from one or more fields.
    FILTER: the field only filters the result; it does not contribute to the value.
    """

    DIRECT = "DIRECT"
    AGGREGATION = "AGGREGATION"
    EXPRESSION = "EXPRESSION"
    FILTER = "FILTER"


def _field_reference(
    target_key: str,
    target: Dict[str, Any],
    transformation: Optional[FieldTransformation],
    expression: Optional[str],
) -> Dict[str, Any]:
    reference: Dict[str, Any] = {target_key: target}
    if transformation is not None:
        reference["transformation"] = transformation.value
    if expression is not None:
        reference["expression"] = expression
    return reference


class FieldDependencySqlColumn(ParadimeBaseModel):
    """
    A warehouse column a field reads from (upstream) or writes to (downstream).

    Attributes:
        database_name (str, optional): The name of the database.
        schema_name (str, optional): The name of the schema.
        table_name (str): The name of the table.
        column_name (str): The name of the column.
        transformation (FieldTransformation, optional): How the field relates to the column. Default is DIRECT.
        expression (str, optional): The expression behind the relationship, e.g. "SUM(amount)".
    """

    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: str
    column_name: str
    transformation: Optional[FieldTransformation] = None
    expression: Optional[str] = None

    def _to_gql_dict(self) -> Dict[str, Any]:
        return _field_reference(
            "sqlColumn",
            {
                "database": self.database_name,
                "schema": self.schema_name,
                "table": self.table_name,
                "column": self.column_name,
            },
            self.transformation,
            self.expression,
        )


class FieldDependencyNativeIntegrationField(ParadimeBaseModel):
    """
    A field of a native integration node. At least one of node_id or node_name should be provided.

    Attributes:
        node_type (NativeIntegrationNodeType): The type of the native integration node.
        node_id (str, optional): The unique identifier of the node. Required if node_name is not provided.
        node_name (str, optional): The name of the node. Required if node_id is not provided.
        field_name (str): The name of the field on that node.
        transformation (FieldTransformation, optional): How the fields relate. Default is DIRECT.
        expression (str, optional): The expression behind the relationship.
    """

    node_type: NativeIntegrationNodeType
    node_id: Optional[str] = None
    node_name: Optional[str] = None
    field_name: str
    transformation: Optional[FieldTransformation] = None
    expression: Optional[str] = None

    def _to_gql_dict(self) -> Dict[str, Any]:
        return _field_reference(
            "nativeIntegrationField",
            {
                "nodeType": self.node_type.value,
                "nodeId": self.node_id,
                "nodeName": self.node_name,
                "fieldName": self.field_name,
            },
            self.transformation,
            self.expression,
        )


class FieldDependencyCustomIntegrationField(ParadimeBaseModel):
    """
    A field of a custom integration node, in this integration or another one.
    At least one of node_id or node_name should be provided; for a node of another integration, provide node_id.

    Attributes:
        node_type (str): The type of the custom integration node.
        integration_uid (str, optional): The unique identifier of the custom integration.
        integration_name (str, optional): The name of the custom integration.
        node_id (str, optional): The stable id of the node. Required if node_name is not provided.
        node_name (str, optional): The name of the node. Required if node_id is not provided.
        field_name (str): The name of the field on that node.
        transformation (FieldTransformation, optional): How the fields relate. Default is DIRECT.
        expression (str, optional): The expression behind the relationship.
    """

    node_type: str
    integration_uid: Optional[str] = None
    integration_name: Optional[str] = None
    node_id: Optional[str] = None
    node_name: Optional[str] = None
    field_name: str
    transformation: Optional[FieldTransformation] = None
    expression: Optional[str] = None

    def _to_gql_dict(self) -> Dict[str, Any]:
        return _field_reference(
            "customIntegrationField",
            {
                "nodeType": self.node_type,
                "integrationUid": self.integration_uid,
                "integrationName": self.integration_name,
                "nodeStableId": self.node_id,
                "nodeName": self.node_name,
                "fieldName": self.field_name,
            },
            self.transformation,
            self.expression,
        )


FieldDependency = Union[
    FieldDependencySqlColumn,
    FieldDependencyNativeIntegrationField,
    FieldDependencyCustomIntegrationField,
]


def _field_lineage(
    upstream_fields: Optional[List[FieldDependency]],
    downstream_fields: Optional[List[FieldDependency]],
) -> Dict[str, Any]:
    lineage: Dict[str, Any] = {}
    if upstream_fields is not None:
        lineage["upstreamFields"] = [field._to_gql_dict() for field in upstream_fields]
    if downstream_fields is not None:
        lineage["downstreamFields"] = [field._to_gql_dict() for field in downstream_fields]
    return lineage


# -------- ChartLike Node --------


class NodeChartLikeAttributesField(ParadimeBaseModel):
    """
    Represents a field associated with a chart-like node.

    Attributes:
        name (str, optional): The name of the field.
        description (str, optional): The description of the field.
        type (str, optional): The type of the field.
        data_type (str, optional): The data type of the field.
        upstream_fields (list[FieldDependency], optional): The fields and warehouse columns this field reads from.
        downstream_fields (list[FieldDependency], optional): The fields and warehouse columns this field feeds, for targets that cannot declare it themselves.
    """

    name: Optional[str]
    description: Optional[str]
    type: Optional[str]
    data_type: Optional[str]
    upstream_fields: Optional[List[FieldDependency]] = None
    downstream_fields: Optional[List[FieldDependency]] = None

    def _to_gql_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name or "",
            "description": self.description or "",
            "type": self.type or "",
            "dataType": self.data_type or "",
            **_field_lineage(self.upstream_fields, self.downstream_fields),
        }


class NodeChartLikeAttributes(ParadimeBaseModel):
    """
    Represents the attributes of a chart-like node.

    Attributes:
        created_at (int, optional): The epoch timestamp when the node was created.
        last_modified_at (int, optional): The epoch timestamp when the node was last modified.
        url (str, optional): The URL of the node.
        owner (str, optional): The owner of the node.
        description (str, optional): The description of the node.
        tags (list[str], optional): The tags associated with the node.
        fields (list[NodeChartLikeAttributesField], optional): The fields associated with the node.
    """

    created_at: Optional[int] = None
    last_modified_at: Optional[int] = None
    url: Optional[str] = None
    owner: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None
    fields: Optional[List[NodeChartLikeAttributesField]] = None

    def _to_gql_dict(self, name: str) -> Dict[str, Any]:
        return {
            "name": name,
            "createdAt": self.created_at or 0,
            "lastModifiedAt": self.last_modified_at or 0,
            "url": self.url or "",
            "owner": self.owner or "",
            "description": self.description or "",
            "tags": self.tags or [],
            "fields": ([field._to_gql_dict() for field in self.fields] if self.fields else []),
        }


class NodeChartLike(ParadimeBaseModel):
    """
    Represents a node that is similar to a chart. Use this to create a chart-like node in the integration.

    Attributes:
        name (str): The name of the node.
        node_type (str): The type of the node.
        id (str, optional): The ID of the node. Optional. If not provided, a unique ID will be generated.
        lineage (Lineage): The lineage of the node.
        attributes (NodeChartLikeAttributes): The attributes of the node.
    """

    name: str
    node_type: str
    id: Optional[str] = None
    lineage: Lineage
    attributes: NodeChartLikeAttributes

    def _to_gql_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "nodeType": self.node_type,
            "stableId": self.id or generate_uid(self.name),
            "lineage": self.lineage._to_gql_dict(),
            "attributes": self.attributes._to_gql_dict(self.name),
        }


# -------- DashboardLike Node --------


class NodeDashboardLikeAttributes(ParadimeBaseModel):
    """
    Represents the attributes of a dashboard-like node.

    Attributes:
        created_at (int, optional): The epoch timestamp when the node was created.
        last_modified_at (int, optional): The epoch timestamp when the node was last modified.
        url (str, optional): The URL of the node.
        owner (str, optional): The owner of the node.
        description (str, optional): The description of the node.
        tags (list[str], optional): The tags associated with the node.
    """

    created_at: Optional[int] = None
    last_modified_at: Optional[int] = None
    url: Optional[str] = None
    owner: Optional[str] = None
    description: Optional[str] = None
    tags: Optional[List[str]] = None

    def _to_gql_dict(self, name: str) -> Dict[str, Any]:
        return {
            "name": name,
            "createdAt": self.created_at or 0,
            "lastModifiedAt": self.last_modified_at or 0,
            "url": self.url or "",
            "owner": self.owner or "",
            "description": self.description or "",
            "tags": self.tags or [],
        }


class NodeDashboardLike(ParadimeBaseModel):
    """
    Represents a node that is similar to a dashboard. Use this to create a dashboard-like node in the integration.

    Attributes:
        name (str): The name of the node.
        node_type (str): The type of the node.
        id (str, optional): The ID of the node. Optional. If not provided, a unique ID will be generated.
        lineage (Lineage): The lineage of the node.
        attributes (NodeDashboardLikeAttributes): The attributes of the node.
    """

    name: str
    node_type: str
    id: Optional[str] = None
    lineage: Lineage
    attributes: NodeDashboardLikeAttributes

    def _to_gql_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "nodeType": self.node_type,
            "stableId": self.id or generate_uid(self.name),
            "lineage": self.lineage._to_gql_dict(),
            "attributes": self.attributes._to_gql_dict(self.name),
        }


# -------- DatasourceLike Node --------


class NodeDatasourceLikeAttributesField(ParadimeBaseModel):
    """
    Represents a field associated with a datasource-like node.

    Attributes:
        name (str, optional): The name of the field.
        description (str, optional): The description of the field.
        type (str, optional): The type of the field.
        data_type (str, optional): The data type of the field.
        upstream_fields (list[FieldDependency], optional): The fields and warehouse columns this field reads from.
        downstream_fields (list[FieldDependency], optional): The fields and warehouse columns this field feeds, for targets that cannot declare it themselves.
    """

    name: Optional[str]
    description: Optional[str]
    type: Optional[str]
    data_type: Optional[str]
    upstream_fields: Optional[List[FieldDependency]] = None
    downstream_fields: Optional[List[FieldDependency]] = None

    def _to_gql_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name or "",
            "description": self.description or "",
            "type": self.type or "",
            "dataType": self.data_type or "",
            **_field_lineage(self.upstream_fields, self.downstream_fields),
        }


class NodeDatasourceLikeAttributes(ParadimeBaseModel):
    """
    Represents the attributes of a datasource-like node.

    Attributes:
        created_at (int, optional): The epoch timestamp when the node was created.
        description (str, optional): The description of the node.
        url (str, optional): The URL of the node.
        database_name (str, optional): The name of the database.
        schema_name (str, optional): The name of the schema.
        table_name (str, optional): The name of the table.
        fields (list[NodeDatasourceLikeAttributesField], optional): The fields associated with the node.
    """

    created_at: Optional[int] = None
    description: Optional[str] = None
    url: Optional[str] = None
    database_name: Optional[str] = None
    schema_name: Optional[str] = None
    table_name: Optional[str] = None
    fields: Optional[List[NodeDatasourceLikeAttributesField]] = None

    def _to_gql_dict(self, name: str) -> Dict[str, Any]:
        return {
            "name": name,
            "createdAt": self.created_at or 0,
            "description": self.description or "",
            "url": self.url or "",
            "database": self.database_name or "",
            "schema": self.schema_name or "",
            "table": self.table_name or "",
            "fields": ([field._to_gql_dict() for field in self.fields] if self.fields else []),
        }


class NodeDatasourceLike(ParadimeBaseModel):
    """
    Represents a node that is similar to a datasource. Use this to create a datasource-like node in the integration.

    Attributes:
        name (str): The name of the node.
        node_type (str): The type of the node.
        id (str, optional): The ID of the node. Optional. If not provided, a unique ID will be generated.
        lineage (Lineage): The lineage of the node.
        attributes (NodeDatasourceLikeAttributes): The attributes of the node.
    """

    name: str
    node_type: str
    id: Optional[str] = None
    lineage: Lineage
    attributes: NodeDatasourceLikeAttributes

    def _to_gql_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "nodeType": self.node_type,
            "stableId": self.id or generate_uid(self.name),
            "lineage": self.lineage._to_gql_dict(),
            "attributes": self.attributes._to_gql_dict(self.name),
        }


Node = Union[NodeChartLike, NodeDashboardLike, NodeDatasourceLike]
