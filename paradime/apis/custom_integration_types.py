from enum import StrEnum
from pydantic import BaseModel


class NodeColor(StrEnum):
    LEAF = "LEAF"
    CYAN = "CYAN"
    CORAL = "CORAL"
    VIOLET = "VIOLET"
    ORANGE = "ORANGE"
    MANDY = "MANDY"
    TEAL = "TEAL"
    GREEN = "GREEN"


class NodeType(BaseModel):
    node_type: str
    icon_name: str
    color: str


class Integration(BaseModel):
    uid: str
    name: str
    is_active: bool
    node_types: list[NodeType]
