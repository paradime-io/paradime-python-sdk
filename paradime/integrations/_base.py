import base64
import shlex
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from paradime.tools.pydantic import BaseModel

SCHEMA_VERSION = "1.0.0"


class FieldType(str, Enum):
    TEXT = "text"
    SECRET = "secret"
    NUMBER = "number"
    DROPDOWN = "dropdown"
    CHECKBOX = "checkbox"
    SWITCH = "switch"
    TEXTAREA = "textarea"


class CommandType(str, Enum):
    ACTION = "action"
    LIST = "list"


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


class Validation(BaseModel):
    min_length: Optional[int] = None
    max_length: Optional[int] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    pattern: Optional[str] = None
    pattern_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        if self.min_length is not None:
            result["min_length"] = self.min_length
        if self.max_length is not None:
            result["max_length"] = self.max_length
        if self.min_value is not None:
            result["min_value"] = self.min_value
        if self.max_value is not None:
            result["max_value"] = self.max_value
        if self.pattern is not None:
            result["pattern"] = self.pattern
        if self.pattern_message is not None:
            result["pattern_message"] = self.pattern_message
        return result


class Condition(BaseModel):
    field: str
    operator: ConditionOperator
    value: Optional[Any] = None
    effect: ConditionEffect

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "field": self.field,
            "operator": self.operator.value,
            "effect": self.effect.value,
        }
        if self.value is not None:
            result["value"] = self.value
        return result


class ConditionGroup(BaseModel):
    all: Optional[List[Condition]] = None
    any: Optional[List[Condition]] = None
    effect: ConditionEffect

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {"effect": self.effect.value}
        if self.all is not None:
            result["all"] = [c.to_dict() for c in self.all]
        if self.any is not None:
            result["any"] = [c.to_dict() for c in self.any]
        return result


DependsOn = Union[Condition, ConditionGroup]


class DynamicOptions(BaseModel):
    resolver: str
    depends_on_fields: List[str] = []
    label_key: str
    value_key: str
    refresh_on: List[str] = []

    def to_dict(self) -> Dict[str, Any]:
        return {
            "resolver": self.resolver,
            "depends_on_fields": self.depends_on_fields,
            "label_key": self.label_key,
            "value_key": self.value_key,
            "refresh_on": self.refresh_on,
        }


class DropdownOption(BaseModel):
    label: str
    value: str

    def to_dict(self) -> Dict[str, Any]:
        return {"label": self.label, "value": self.value}


class Field(BaseModel):
    id: str
    label: str
    description: str = ""
    type: FieldType
    required: bool = False
    default: Optional[Any] = None
    env_var: Optional[str] = None
    repeatable: bool = False
    validation: Optional[Validation] = None
    depends_on: Optional[DependsOn] = None
    dynamic_options: Optional[DynamicOptions] = None
    options: Optional[List[DropdownOption]] = None
    group: Optional[str] = None
    order: int = 0
    help_url: Optional[str] = None
    encode: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "id": self.id,
            "label": self.label,
            "description": self.description,
            "type": self.type.value,
            "required": self.required,
            "default": self.default,
            "env_var": self.env_var,
            "repeatable": self.repeatable,
            "group": self.group,
            "order": self.order,
            "help_url": self.help_url,
            "encode": self.encode,
        }
        if self.validation is not None:
            result["validation"] = self.validation.to_dict()
        if self.depends_on is not None:
            result["depends_on"] = self.depends_on.to_dict()
        if self.dynamic_options is not None:
            result["dynamic_options"] = self.dynamic_options.to_dict()
        if self.options is not None:
            result["options"] = [o.to_dict() for o in self.options]
        return result


class RepeatableGroup(BaseModel):
    id: str
    label: str
    description: str = ""
    min_items: int = 1
    max_items: Optional[int] = None
    fields: List[Field] = []

    def to_dict(self) -> Dict[str, Any]:
        return {
            "kind": "group",
            "id": self.id,
            "label": self.label,
            "description": self.description,
            "min_items": self.min_items,
            "max_items": self.max_items,
            "fields": [f.to_dict() for f in self.fields],
        }


FieldOrGroup = Union[Field, RepeatableGroup]


class CommandManifest(BaseModel):
    id: str
    name: str
    description: str = ""
    type: CommandType
    core_function: str
    cli_command: str  # Click command name, e.g. "fivetran-sync"
    fields: List[FieldOrGroup] = []

    def to_dict(self) -> Dict[str, Any]:
        serialized_fields = []
        for f in self.fields:
            d = f.to_dict()
            if isinstance(f, Field):
                d["kind"] = "field"
            serialized_fields.append(d)

        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "type": self.type.value,
            "core_function": self.core_function,
            "cli_command": self.cli_command,
            "fields": serialized_fields,
        }


class CommandExecutionRequest(BaseModel):
    fields: Dict[str, Any] = {}

    def decode_fields(self, command: "CommandManifest") -> Dict[str, Any]:
        """Return a copy of fields with base64-encoded values decoded.

        Only decodes fields whose manifest definition has ``encode="base64"``.
        """
        encode_fields = _collect_encode_fields(command.fields)
        decoded = dict(self.fields)
        for field_id, encoding in encode_fields.items():
            if field_id in decoded and encoding == "base64":
                decoded[field_id] = base64.b64decode(decoded[field_id]).decode("utf-8")
        return decoded


def _collect_encode_fields(fields: List[FieldOrGroup]) -> Dict[str, str]:
    """Walk fields/groups and return ``{field_id: encode}`` for encoded fields."""
    result: Dict[str, str] = {}
    for f in fields:
        if isinstance(f, RepeatableGroup):
            result.update(_collect_encode_fields(f.fields))  # type: ignore[arg-type]
        elif isinstance(f, Field) and f.encode:
            result[f.id] = f.encode
    return result


def _field_id_to_cli_option(field_id: str) -> str:
    """Convert a snake_case field id to a --kebab-case CLI option."""
    return "--" + field_id.replace("_", "-")


def build_cli_command(
    manifest: "IntegrationManifest",
    command: "CommandManifest",
    field_values: Dict[str, Any],
) -> str:
    """Build a CLI command string from manifest definitions and user-supplied values.

    Auth fields that have ``env_var`` set are referenced as ``$ENV_VAR`` so the
    schedule runner resolves them at execution time.  Secret fields without an
    ``env_var`` are skipped entirely (they should not be inlined).

    Boolean fields (checkbox / switch) emit ``--flag`` when *True* and nothing
    when *False* (matching Click ``is_flag=True`` behaviour).

    Repeatable fields emit the option multiple times, once per value.

    Fields whose manifest declares ``encode="base64"`` will have their values
    base64-encoded in the CLI string.

    Returns a string like:
        ``paradime run fivetran-sync --connector-id abc --force --timeout-minutes 60``
    """
    parts: List[str] = ["paradime", "run", command.cli_command]

    # --- auth fields first ---
    for field in manifest.auth_fields:
        option = _field_id_to_cli_option(field.id)
        if field.env_var:
            parts.append(option)
            parts.append(f"${field.env_var}")
        # Secret fields without env_var are omitted — never inline secrets.

    # --- command fields ---
    all_fields: List[Field] = []
    for f in command.fields:
        if isinstance(f, RepeatableGroup):
            all_fields.extend(f.fields)  # type: ignore[arg-type]
        elif isinstance(f, Field):
            all_fields.append(f)

    for field in all_fields:
        value = field_values.get(field.id)

        # Skip fields not supplied and not required with a default
        if value is None:
            continue

        option = _field_id_to_cli_option(field.id)

        # Boolean flags
        if field.type in (FieldType.CHECKBOX, FieldType.SWITCH):
            if value:
                parts.append(option)
            # False = omit the flag (Click is_flag default behaviour)
            continue

        # Apply base64 encoding if declared
        if field.encode == "base64":
            if isinstance(value, list):
                value = [base64.b64encode(str(v).encode("utf-8")).decode("ascii") for v in value]
            else:
                value = base64.b64encode(str(value).encode("utf-8")).decode("ascii")

        # Repeatable fields emit the option multiple times
        if field.repeatable and isinstance(value, list):
            for v in value:
                parts.append(option)
                parts.append(shlex.quote(str(v)))
        else:
            parts.append(option)
            parts.append(shlex.quote(str(value)))

    return " ".join(parts)


class IntegrationManifest(BaseModel):
    id: str
    name: str
    description: str = ""
    icon: Optional[str] = None
    category: Optional[str] = None
    help_url: Optional[str] = None
    auth_fields: List[Field] = []
    commands: List[CommandManifest] = []
    schema_version: str = SCHEMA_VERSION

    def get_command(self, command_id: str) -> CommandManifest:
        for command in self.commands:
            if command.id == command_id:
                return command
        raise KeyError(f"Command '{command_id}' not found in integration '{self.id}'")

    def build_cli_command(self, command_id: str, field_values: Dict[str, Any]) -> str:
        """Build a CLI command string for the given command and field values."""
        command = self.get_command(command_id)
        return build_cli_command(self, command, field_values)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "name": self.name,
            "description": self.description,
            "icon": self.icon,
            "category": self.category,
            "help_url": self.help_url,
            "auth_fields": [f.to_dict() for f in self.auth_fields],
            "commands": [c.to_dict() for c in self.commands],
            "schema_version": self.schema_version,
        }
