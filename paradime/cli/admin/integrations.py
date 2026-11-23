from typing import Any, Optional, TextIO

import click
from ruamel.yaml import YAML
from ruamel.yaml.error import YAMLError

from paradime.apis.admin.integrations.types import IntegrationConfigInput, IntegrationName
from paradime.cli import console
from paradime.cli.admin import common
from paradime.tools.pydantic import BaseModel, ValidationError

NAME_CHOICE = click.Choice(
    [name.value for name in IntegrationName if name != IntegrationName.UNKNOWN],
    case_sensitive=False,
)

config_option = click.option(
    "--config",
    "config",
    type=click.File("r"),
    required=True,
    help="A YAML or JSON file with one integration block, such as "
    "`metabase: {host: ..., apiKey: ...}`. Use - for stdin. Secrets left out keep their "
    "saved value, unless an endpoint setting such as the host changes.",
)


def _config(config: TextIO) -> IntegrationConfigInput:
    try:
        data = YAML(typ="safe").load(config.read())
    except YAMLError as e:
        raise click.UsageError(f"--config is not valid YAML or JSON: {e}")
    if not isinstance(data, dict):
        raise click.UsageError(
            "--config must hold one integration block, such as `metabase: {...}`."
        )
    try:
        return IntegrationConfigInput.parse_obj(data)
    except ValidationError as e:
        raise click.UsageError(f"--config has settings the API does not take:\n{e}")


def _settings(settings: Any) -> str:
    if not isinstance(settings, BaseModel):
        return ""
    return ", ".join(
        f"{name}={common.show(value)}" for name, value in settings if value is not None
    )


@click.group()
def integrations() -> None:
    """
    Manage the workspace's BI, ETL and observability integrations, such as Looker, Tableau,
    Power BI, Fivetran, Airbyte, Datadog and PagerDuty.

    Secrets are read only from the --config file or stdin, never from the command line.
    """
    pass


@integrations.command(name="list")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def list_integrations(workspace_uid: Optional[str], as_json: bool) -> None:
    """
    List the workspace's integrations. Secrets show only as set or not.
    """
    listed = list(common.get_admin().integrations.list(workspace_uid=workspace_uid))
    if as_json:
        console.json_out([common.to_json(integration) for integration in listed])
        return
    console.table(
        ["Name", "Status", "Sync progress", "Updated", "Settings"],
        [
            [
                integration.name.value,
                integration.status.value,
                common.show(integration.sync_progress),
                common.show(integration.updated_at),
                _settings(integration.settings),
            ]
            for integration in listed
        ],
    )


@integrations.command()
@config_option
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def test(config: TextIO, workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Check the integration accepts these credentials, and save nothing.
    """
    result = common.get_admin().integrations.test(_config(config), workspace_uid=workspace_uid)
    if as_json:
        console.json_out(common.to_json(result))
        return
    console.success(f"The {result.name.value} integration accepted the credentials.")


@integrations.command(name="set")
@config_option
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def set_integration(config: TextIO, workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Test, then save an integration's credentials. The same settings again change nothing.
    """
    integration = common.get_admin().integrations.set(_config(config), workspace_uid=workspace_uid)
    common.print_model(integration, as_json=as_json)


@integrations.command()
@click.argument("name", type=NAME_CHOICE)
@click.confirmation_option(prompt="Disconnect this integration?")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def disconnect(name: str, workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Disconnect an integration, such as METABASE.
    """
    integration = common.get_admin().integrations.disconnect(
        IntegrationName(name.upper()), workspace_uid=workspace_uid
    )
    if as_json:
        console.json_out(common.to_json(integration))
        return
    console.success(f"Disconnected {integration.name.value}.")
