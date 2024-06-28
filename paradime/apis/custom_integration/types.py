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


# -------- ChartLike Node --------


class NodeChartLikeAttributesField(ParadimeBaseModel):
    """
    Represents a field associated with a chart-like node.

    Attributes:
        name (str, optional): The name of the field.
        description (str, optional): The description of the field.
        type (str, optional): The type of the field.
        data_type (str, optional): The data type of the field.
    """

    name: Optional[str]
    description: Optional[str]
    type: Optional[str]
    data_type: Optional[str]

    def _to_gql_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name or "",
            "description": self.description or "",
            "type": self.type or "",
            "dataType": self.data_type or "",
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
    """

    name: Optional[str]
    description: Optional[str]
    type: Optional[str]
    data_type: Optional[str]

    def _to_gql_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name or "",
            "description": self.description or "",
            "type": self.type or "",
            "dataType": self.data_type or "",
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
