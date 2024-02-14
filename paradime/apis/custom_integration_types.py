from enum import Enum, StrEnum
from typing import Any, Union

from pydantic import BaseModel, Extra


class ParadimeBaseModel(BaseModel):
    class Config:
        allow_mutation = False
        frozen = True
        validate_all = True
        validate_assignment = True
        extra = Extra.forbid


class NodeColor(StrEnum):
    LEAF = "LEAF"
    CYAN = "CYAN"
    CORAL = "CORAL"
    VIOLET = "VIOLET"
    ORANGE = "ORANGE"
    MANDY = "MANDY"
    TEAL = "TEAL"
    GREEN = "GREEN"


class NodeType(ParadimeBaseModel):
    node_type: str
    icon_name: str | None = None
    color: NodeColor | None = None


class Integration(ParadimeBaseModel):
    uid: str
    name: str
    is_active: bool
    node_types: list[NodeType]


# -------- Lineage --------


class LineageEdgeSqlModel(ParadimeBaseModel):
    database_name: str | None
    schema_name: str | None
    table_name: str

    def _to_gql_dict(self) -> dict[str, Any]:
        return {
            "database": self.database_name or "",
            "schema": self.schema_name or "",
            "table": self.table_name,
        }


class NativeIntegrationNodeType(Enum):
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


class LineageEdgeNativeIntegration(ParadimeBaseModel):
    node_type: NativeIntegrationNodeType
    node_id: str | None
    node_name: str | None

    def _to_gql_dict(self) -> dict[str, Any]:
        return {
            "nodeType": self.node_type.value,
            "nodeId": self.node_id or "",
            "nodeName": self.node_name or "",
        }


class LineageEdgeCustomIntegration(ParadimeBaseModel):
    integration_uid: str | None
    integration_name: str | None
    node_type: str
    node_stable_id: str | None
    node_name: str | None

    def _to_gql_dict(self) -> dict[str, Any]:
        return {
            "integrationUid": self.integration_uid or "",
            "integrationName": self.integration_name or "",
            "nodeType": self.node_type or "",
            "nodeStableId": self.node_stable_id or "",
            "nodeName": self.node_name or "",
        }


LineageEdge = LineageEdgeSqlModel | LineageEdgeNativeIntegration | LineageEdgeCustomIntegration


class NodeLineage(ParadimeBaseModel):
    upstream_edges: list[LineageEdge] = []
    downstream_edges: list[LineageEdge] = []

    def _to_gql_dict(self) -> dict[str, Any]:
        return {
            "upstreamReferences": {
                "referencesToSqlTables": [
                    edge._to_gql_dict()
                    for edge in self.upstream_edges
                    if isinstance(edge, LineageEdgeSqlModel)
                ],
                "referencesToNativeIntegrations": [
                    edge._to_gql_dict()
                    for edge in self.upstream_edges
                    if isinstance(edge, LineageEdgeNativeIntegration)
                ],
                "referencesToCustomIntegrations": [
                    edge._to_gql_dict()
                    for edge in self.upstream_edges
                    if isinstance(edge, LineageEdgeCustomIntegration)
                ],
            },
            "downstreamReferences": {
                "referencesToSqlTables": [
                    edge._to_gql_dict()
                    for edge in self.downstream_edges
                    if isinstance(edge, LineageEdgeSqlModel)
                ],
                "referencesToNativeIntegrations": [
                    edge._to_gql_dict()
                    for edge in self.downstream_edges
                    if isinstance(edge, LineageEdgeNativeIntegration)
                ],
                "referencesToCustomIntegrations": [
                    edge._to_gql_dict()
                    for edge in self.downstream_edges
                    if isinstance(edge, LineageEdgeCustomIntegration)
                ],
            },
        }


# -------- ChartLike Node --------


class NodeChartLikeAttributesField(ParadimeBaseModel):
    name: str | None
    description: str | None
    type: str | None
    data_type: str | None

    def _to_gql_dict(self) -> dict[str, Any]:
        return {
            "name": self.name or "",
            "description": self.description or "",
            "type": self.type or "",
            "dataType": self.data_type or "",
        }


class NodeChartLikeAttributes(ParadimeBaseModel):
    created_at: int | None = None
    last_modified_at: int | None = None
    url: str | None = None
    owner: str | None = None
    description: str | None = None
    tags: list[str] | None = None
    fields: list[NodeChartLikeAttributesField] | None = None

    def _to_gql_dict(self, name: str) -> dict[str, Any]:
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
    name: str
    node_type: str
    stable_id: str
    lineage: NodeLineage
    attributes: NodeChartLikeAttributes

    def _to_gql_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "nodeType": self.node_type,
            "stableId": self.stable_id,
            "lineage": self.lineage._to_gql_dict(),
            "attributes": self.attributes._to_gql_dict(self.name),
        }


# -------- DashboardLike Node --------


class NodeDashboardLikeAttributes(ParadimeBaseModel):
    created_at: int | None = None
    last_modified_at: int | None = None
    url: str | None = None
    owner: str | None = None
    description: str | None = None
    tags: list[str] | None = None

    def _to_gql_dict(self, name: str) -> dict[str, Any]:
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
    name: str
    node_type: str
    stable_id: str
    lineage: NodeLineage
    attributes: NodeDashboardLikeAttributes

    def _to_gql_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "nodeType": self.node_type,
            "stableId": self.stable_id,
            "lineage": self.lineage._to_gql_dict(),
            "attributes": self.attributes._to_gql_dict(self.name),
        }


# -------- DatasourceLike Node --------


class NodeDatasourceLikeAttributesField(ParadimeBaseModel):
    name: str | None
    description: str | None
    type: str | None
    data_type: str | None

    def _to_gql_dict(self) -> dict[str, Any]:
        return {
            "name": self.name or "",
            "description": self.description or "",
            "type": self.type or "",
            "dataType": self.data_type or "",
        }


class NodeDatasourceLikeAttributes(ParadimeBaseModel):
    created_at: int | None = None
    description: str | None = None
    url: str | None = None
    database_name: str | None = None
    schema_name: str | None = None
    table_name: str | None = None
    fields: list[NodeDatasourceLikeAttributesField] | None = None

    def _to_gql_dict(self, name: str) -> dict[str, Any]:
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
    name: str
    node_type: str
    stable_id: str
    lineage: NodeLineage
    attributes: NodeDatasourceLikeAttributes

    def _to_gql_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "nodeType": self.node_type,
            "stableId": self.stable_id,
            "lineage": self.lineage._to_gql_dict(),
            "attributes": self.attributes._to_gql_dict(self.name),
        }


Node = Union[NodeChartLike, NodeDashboardLike, NodeDatasourceLike]
