from typing import Any, Optional, TextIO

import click
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from paradime.apis.admin.connections.types import ConnectionScope, WarehouseAdapterInput
from paradime.cli import console
from paradime.cli.admin import common
from paradime.tools.pydantic import ValidationError

SCOPE_CHOICE = click.Choice(["SCHEDULER", "AGENT"], case_sensitive=False)

scope_option = click.option(
    "--scope", type=SCOPE_CHOICE, required=True, help="Who uses the connection."
)
environment_option = click.option(
    "--environment",
    "environment_slug",
    required=True,
    help="The slug of the scheduler or agent environment.",
)


def connection_options(f: Any) -> Any:
    for option in reversed(
        [
            scope_option,
            environment_option,
            click.option(
                "--config",
                "config",
                type=click.File("r"),
                required=True,
                help="A YAML or JSON file with the warehouse settings, one adapter block such "
                "as `snowflake: {account: ..., password: ...}`. Use - for stdin. Secrets "
                "left out keep their stored value.",
            ),
            click.option("--connection-id", default=None, help="Defaults to `default`."),
            click.option("--name", "connection_name", default=None, help="The connection's name."),
            click.option("--target", "target_name", default=None, help="The dbt target name."),
            click.option("--threads", type=int, default=None, help="dbt threads."),
        ]
    ):
        f = option(f)
    return f


def _adapter(config: TextIO) -> WarehouseAdapterInput:
    try:
        data = YAML(typ="safe").load(config.read())
    except YAMLError as e:
        raise click.UsageError(f"--config is not valid YAML or JSON: {e}")
    if not isinstance(data, dict):
        raise click.UsageError("--config must hold one adapter block, such as `snowflake: {...}`.")
    try:
        return WarehouseAdapterInput.parse_obj(data)
    except ValidationError as e:
        raise click.UsageError(f"--config has settings the API does not take:\n{e}")


@click.group()
def connections() -> None:
    """
    Manage the data warehouse connections of Bolt (SCHEDULER) and agent (AGENT) environments.

    Secrets are read only from the --config file or stdin, never from the command line.
    """
    pass


@connections.command(name="list")
@click.option("--scope", type=SCOPE_CHOICE, default=None, help="Only this scope.")
@click.option("--environment", "environment_slug", default=None, help="Only this environment.")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def list_connections(
    scope: Optional[str],
    environment_slug: Optional[str],
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    List the workspace's warehouse connections. Secrets show only as set or not.
    """
    common.print_models(
        common.get_admin().connections.list(
            scope=ConnectionScope(scope.upper()) if scope else None,
            environment_slug=environment_slug,
            workspace_uid=workspace_uid,
        ),
        columns=["Scope", "Environment", "Id", "Type", "Default", "Name", "Target", "Updated"],
        fields=[
            "scope",
            "environment_slug",
            "connection_id",
            "type",
            "is_default",
            "connection_name",
            "target_name",
            "updated_at",
        ],
        as_json=as_json,
    )


@connections.command()
@connection_options
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def test(
    scope: str,
    environment_slug: str,
    config: TextIO,
    connection_id: Optional[str],
    connection_name: Optional[str],
    target_name: Optional[str],
    threads: Optional[int],
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    Open a connection with these settings, and save nothing.
    """
    result = common.get_admin().connections.test(
        _adapter(config),
        scope=ConnectionScope(scope.upper()),
        environment_slug=environment_slug,
        connection_id=connection_id,
        connection_name=connection_name,
        target_name=target_name,
        threads=threads,
        workspace_uid=workspace_uid,
    )
    if as_json:
        console.json_out(common.to_json(result))
        return
    console.success(f"The {result.type.value} connection opened.")


@connections.command(name="set")
@connection_options
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def set_connection(
    scope: str,
    environment_slug: str,
    config: TextIO,
    connection_id: Optional[str],
    connection_name: Optional[str],
    target_name: Optional[str],
    threads: Optional[int],
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    Test, then save a connection. A new connection needs --name, --target and --threads.
    """
    connection = common.get_admin().connections.set(
        _adapter(config),
        scope=ConnectionScope(scope.upper()),
        environment_slug=environment_slug,
        connection_id=connection_id,
        connection_name=connection_name,
        target_name=target_name,
        threads=threads,
        workspace_uid=workspace_uid,
    )
    common.print_model(connection, as_json=as_json)


@connections.command()
@scope_option
@environment_option
@click.option("--connection-id", default=None, help="Defaults to `default`.")
@click.confirmation_option(prompt="Delete this connection?")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def delete(
    scope: str,
    environment_slug: str,
    connection_id: Optional[str],
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    Delete a connection. Deleting it again is not an error.
    """
    deleted = common.get_admin().connections.delete(
        scope=ConnectionScope(scope.upper()),
        environment_slug=environment_slug,
        connection_id=connection_id,
        workspace_uid=workspace_uid,
    )
    if as_json:
        console.json_out({"deletedConnectionId": deleted})
        return
    console.success(f"Deleted connection {deleted}.")


@connections.command(name="set-default")
@click.argument("connection_id")
@scope_option
@environment_option
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def set_default(
    connection_id: str,
    scope: str,
    environment_slug: str,
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    Make a connection its environment's default.
    """
    connection = common.get_admin().connections.set_default(
        connection_id,
        scope=ConnectionScope(scope.upper()),
        environment_slug=environment_slug,
        workspace_uid=workspace_uid,
    )
    common.print_model(connection, as_json=as_json)
