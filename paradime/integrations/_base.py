"""
SDK UI Manifest Schema - Pydantic models for integration manifests.

These models define the vocabulary for describing integrations, their fields,
commands, and UI rendering hints. They are used by both the CLI builder and
the frontend UI to auto-generate forms and commands.
"""

from enum import Enum
from typing import Any, List, Optional, Union

from paradime.tools.pydantic import BaseModel, Field as PydanticField

# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class FieldType(str, Enum):
    TEXT = "text"
    SECRET = "secret"
    NUMBER = "number"
    DROPDOWN = "dropdown"
    CHECKBOX = "checkbox"
    SWITCH = "switch"
    TEXTAREA = "textarea"


class ConditionOperator(str, Enum):
    EQ = "eq"
    NEQ = "neq"
    IN = "in"
    NOT_IN = "not_in"
    TRUTHY = "truthy"
    FALSY = "falsy"


class ConditionEffect(str, Enum):
    SHOW = "show"
    HIDE = "hide"
    REQUIRE = "require"
    UNREQUIRE = "unrequire"


class CommandType(str, Enum):
    ACTION = "action"
    LIST = "list"


# ---------------------------------------------------------------------------
# Supporting models
# ---------------------------------------------------------------------------


class Validation(BaseModel):
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    pattern: Optional[str] = None
    pattern_message: Optional[str] = None


class Condition(BaseModel):
    field: Optional[str] = None
    operator: Optional[ConditionOperator] = None
    value: Optional[Any] = None
    effect: Optional[ConditionEffect] = None
    all: Optional[List["Condition"]] = None


class DynamicOptions(BaseModel):
    resolver: str
    depends_on_fields: List[str] = PydanticField(default_factory=list)
    label_key: str = "label"
    value_key: str = "value"
    refresh_on: Optional[List[str]] = None


class DropdownOption(BaseModel):
    label: str
    value: str


# ---------------------------------------------------------------------------
# Field
# ---------------------------------------------------------------------------


class Field(BaseModel):
    id: str
    label: str
    description: str = ""
    type: FieldType = FieldType.TEXT
    required: bool = False
    default: Optional[Any] = None
    env_var: Optional[str] = None
    repeatable: bool = False
    validation: Optional[Validation] = None
    depends_on: Optional[Condition] = None
    dynamic_options: Optional[DynamicOptions] = None
    static_options: Optional[List[DropdownOption]] = None
    group: Optional[str] = None
    order: int = 0
    help_url: Optional[str] = None
    encode: Optional[str] = None


# ---------------------------------------------------------------------------
# Repeatable group
# ---------------------------------------------------------------------------


class RepeatableGroup(BaseModel):
    id: str
    label: str
    description: str = ""
    min_items: int = 1
    max_items: Optional[int] = None
    fields: List[Field] = PydanticField(default_factory=list)


# ---------------------------------------------------------------------------
# Command & Integration manifests
# ---------------------------------------------------------------------------


class CommandManifest(BaseModel):
    id: str
    name: str
    description: str = ""
    type: CommandType = CommandType.ACTION
    core_function: str
    fields: List[Union[Field, RepeatableGroup]] = PydanticField(default_factory=list)


class IntegrationManifest(BaseModel):
    id: str
    name: str
    description: str = ""
    icon: str = ""
    category: str = ""
    help_url: Optional[str] = None
    auth_fields: List[Field] = PydanticField(default_factory=list)
    commands: List[CommandManifest] = PydanticField(default_factory=list)
    schema_version: str = "1.0.0"
