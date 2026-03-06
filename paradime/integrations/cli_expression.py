"""
CLI Expression Generator.

Given an integration manifest, a command ID, and field values,
generates the full CLI expression that users would type.
Auth fields are excluded since they come from environment variables.
"""

from typing import Any

from paradime.integrations._base import (
    Field,
    FieldType,
    IntegrationManifest,
    RepeatableGroup,
)


def generate_cli_expression(
    manifest: IntegrationManifest,
    command_id: str,
    field_values: dict[str, Any],
) -> str:
    """Generate the full CLI expression for a command.

    Args:
        manifest: The integration manifest.
        command_id: The command ID within the integration.
        field_values: Dict of field_id -> value for the command fields.

    Returns:
        The CLI expression string, e.g.:
        "paradime run fivetran-sync --connector-id abc123 --force --timeout-minutes 60"

    Raises:
        KeyError: If command_id is not found in the manifest.
    """
    # Find the command
    command = None
    for cmd in manifest.commands:
        if cmd.id == command_id:
            command = cmd
            break
    if command is None:
        raise KeyError(f"Command '{command_id}' not found in integration '{manifest.id}'")

    # Build command name
    cmd_name = f"{manifest.id}-{command.id}".replace("_", "-")
    parts = ["paradime", "run", cmd_name]

    # Collect all fields (flatten groups)
    all_fields: list[Field] = []
    for item in command.fields:
        if isinstance(item, Field):
            all_fields.append(item)
        elif isinstance(item, RepeatableGroup):
            all_fields.extend(item.fields)

    # Sort by order
    all_fields.sort(key=lambda f: f.order)

    for field in all_fields:
        if field.id not in field_values:
            continue

        value = field_values[field.id]
        option_name = f"--{field.id.replace('_', '-')}"

        if field.type in (FieldType.CHECKBOX, FieldType.SWITCH):
            if value:
                parts.append(option_name)
            else:
                parts.append(f"--no-{field.id.replace('_', '-')}")
        elif field.repeatable and isinstance(value, (list, tuple)):
            for v in value:
                parts.append(option_name)
                parts.append(_quote_if_needed(str(v)))
        else:
            parts.append(option_name)
            parts.append(_quote_if_needed(str(value)))

    return " ".join(parts)


def _quote_if_needed(value: str) -> str:
    """Quote a value if it contains spaces or special characters."""
    if " " in value or "'" in value or '"' in value:
        # Use single quotes, escaping any single quotes in the value
        escaped = value.replace("'", "'\\''")
        return f"'{escaped}'"
    return value
