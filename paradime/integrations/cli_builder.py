"""
CLI Builder - Generates Click commands from integration manifests.

Reads a CommandManifest and produces a Click command that can be registered
with the CLI. Auth fields are resolved from environment variables automatically.
"""

import importlib
import os
import sys
from typing import Any, Callable, List

import click

from paradime.integrations._base import (
    CommandManifest,
    Field,
    FieldType,
    IntegrationManifest,
    RepeatableGroup,
)


def _resolve_auth_from_env(auth_fields: List[Field]) -> dict[str, str]:
    """Resolve auth field values from environment variables.

    Raises click.ClickException if any required env var is missing.
    """
    result: dict[str, str] = {}
    missing: list[str] = []
    for field in auth_fields:
        if field.env_var:
            value = os.environ.get(field.env_var)
            if value is None:
                missing.append(f"{field.label} ({field.env_var})")
            else:
                result[field.id] = value
        else:
            missing.append(f"{field.label} (no env_var configured)")
    if missing:
        raise click.ClickException(f"Missing required environment variables: {', '.join(missing)}")
    return result


def _import_function(dotted_path: str) -> Callable:
    """Import a function from a dotted path like 'paradime.core.scripts.fivetran.trigger_fivetran_sync'."""
    module_path, func_name = dotted_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, func_name)


def _field_to_click_option(field: Field) -> Callable:
    """Convert a Field to a click.option decorator."""
    option_name = f"--{field.id.replace('_', '-')}"
    kwargs: dict[str, Any] = {
        "help": field.description,
        "required": field.required,
    }

    if field.default is not None:
        kwargs["default"] = field.default

    if field.env_var:
        kwargs["envvar"] = field.env_var

    if field.repeatable:
        kwargs["multiple"] = True

    if field.type in (FieldType.CHECKBOX, FieldType.SWITCH):
        kwargs["is_flag"] = True
        kwargs.pop("required", None)
        if field.default is None:
            kwargs["default"] = False
    elif field.type == FieldType.NUMBER:
        kwargs["type"] = int
    elif field.type == FieldType.DROPDOWN and field.static_options:
        kwargs["type"] = click.Choice([opt.value for opt in field.static_options])

    return click.option(option_name, **kwargs)


def build_click_command(
    command_manifest: CommandManifest,
    auth_fields: List[Field],
    integration_id: str,
) -> click.Command:
    """Build a Click command from a CommandManifest.

    Auth fields are NOT exposed as CLI options. They are resolved
    from environment variables when the command is invoked.

    Args:
        command_manifest: The command manifest to build from.
        auth_fields: Auth fields from the integration (resolved from env vars).
        integration_id: The integration ID (used for naming the command).

    Returns:
        A Click command ready to be registered.
    """
    # Collect only non-group fields for Click options
    command_fields: list[Field] = []
    for item in command_manifest.fields:
        if isinstance(item, Field):
            command_fields.append(item)
        elif isinstance(item, RepeatableGroup):
            for group_field in item.fields:
                command_fields.append(group_field)

    # Sort by order
    command_fields.sort(key=lambda f: f.order)

    def make_callback(
        cmd_manifest: CommandManifest,
        auth: List[Field],
        fields: list[Field],
    ) -> Callable:
        def callback(**kwargs: Any) -> None:
            # Resolve auth from env vars
            auth_values = _resolve_auth_from_env(auth)

            # Import and call the core function
            core_fn = _import_function(cmd_manifest.core_function)

            # Build function arguments: auth + command fields
            fn_kwargs: dict[str, Any] = {}
            fn_kwargs.update(auth_values)

            for field in fields:
                param_name = field.id
                if param_name in kwargs:
                    value = kwargs[param_name]
                    # Convert tuple from multiple=True to list
                    if field.repeatable and isinstance(value, tuple):
                        value = list(value)
                    fn_kwargs[param_name] = value

            try:
                result = core_fn(**fn_kwargs)
                # For list commands, the function typically prints output directly
                if result is not None and cmd_manifest.type.value == "action":
                    # Check if any results indicate failure
                    if isinstance(result, list):
                        failed = [
                            r
                            for r in result
                            if isinstance(r, str) and ("FAILED" in r or "PAUSED" in r)
                        ]
                        if failed:
                            sys.exit(1)
            except Exception as e:
                raise click.ClickException(str(e))

        return callback

    cb = make_callback(command_manifest, auth_fields, command_fields)

    # Apply click.option decorators in reverse order (Click processes them bottom-up)
    for field in reversed(command_fields):
        cb = _field_to_click_option(field)(cb)

    # Create the command
    cmd_name = f"{integration_id}-{command_manifest.id}".replace("_", "-")

    return click.command(
        name=cmd_name,
        help=command_manifest.description,
        context_settings=dict(max_content_width=160),
    )(cb)


def build_integration_commands(manifest: IntegrationManifest) -> list[click.Command]:
    """Build all Click commands for an integration manifest.

    Returns a list of Click commands ready to be registered with a click.Group.
    """
    commands = []
    for cmd in manifest.commands:
        click_cmd = build_click_command(cmd, manifest.auth_fields, manifest.id)
        commands.append(click_cmd)
    return commands
