from typing import Any, Dict, Optional

import click

from paradime.apis.admin.exception import AdminNotFoundException
from paradime.apis.admin.types import UNSET
from paradime.cli import console
from paradime.cli.admin import common

WORKSPACE_COLUMNS = ["UID", "Name", "dbt version", "Default branch", "Created"]
WORKSPACE_FIELDS = ["uid", "name", "dbt_version", "default_branch", "created_at"]


@click.group()
def workspaces() -> None:
    """
    Manage the workspaces the API key can reach.
    """
    pass


@workspaces.command(name="list")
@common.json_option
@common.handle_admin_errors
def list_workspaces(as_json: bool) -> None:
    """
    List the workspaces the API key can reach.
    """
    common.print_models(
        common.get_admin().workspaces.list(),
        columns=WORKSPACE_COLUMNS,
        fields=WORKSPACE_FIELDS,
        as_json=as_json,
    )


@workspaces.command()
@click.argument("uid", required=False)
@common.json_option
@common.handle_admin_errors
def get(uid: Optional[str], as_json: bool) -> None:
    """
    Show a workspace and its settings.

    UID is the workspace's uid; leave it out for the key's workspace.
    """
    workspace = common.get_admin().workspaces.get(uid)
    if workspace is None:
        raise AdminNotFoundException(f"No workspace {uid or 'for this key'!r}.")
    common.print_model(workspace, as_json=as_json)


@workspaces.command()
@click.argument("name")
@click.option("--dbt-version", default=None, help="The dbt version to run; the latest if unset.")
@common.json_option
@common.handle_admin_errors
def create(name: str, dbt_version: Optional[str], as_json: bool) -> None:
    """
    Create a workspace. Needs a company-level key. Running it again with the same NAME
    returns the existing workspace.
    """
    workspace = common.get_admin().workspaces.create(name, dbt_version=dbt_version)
    common.print_model(workspace, as_json=as_json)


@workspaces.command()
@click.argument("uid")
@click.option("--name", default=None, help="A new name.")
@click.option("--dbt-version", default=None, help="The dbt version to run.")
@click.option(
    "--partial-parsing/--no-partial-parsing",
    "partial_parsing",
    default=None,
    help="Whether dbt uses partial parsing.",
)
@click.option("--default-branch", default=None, help="The branch Paradime treats as the default.")
@click.option("--clear-default-branch", is_flag=True, help="Unset the default branch.")
@click.option(
    "--schedule-deployment-branch", default=None, help="The branch Bolt schedules deploy from."
)
@click.option(
    "--clear-schedule-deployment-branch",
    is_flag=True,
    help="Deploy Bolt schedules from the default branch.",
)
@common.json_option
@common.handle_admin_errors
def update(
    uid: str,
    name: Optional[str],
    dbt_version: Optional[str],
    partial_parsing: Optional[bool],
    default_branch: Optional[str],
    clear_default_branch: bool,
    schedule_deployment_branch: Optional[str],
    clear_schedule_deployment_branch: bool,
    as_json: bool,
) -> None:
    """
    Change a workspace's settings. Settings left out keep their value.

    UID is the workspace's uid.
    """
    settings: Dict[str, Any] = {
        "name": name if name is not None else UNSET,
        "dbt_version": dbt_version if dbt_version is not None else UNSET,
        "dbt_partial_parsing": partial_parsing if partial_parsing is not None else UNSET,
        "default_branch": _branch(default_branch, clear_default_branch, "default-branch"),
        "schedule_deployment_branch": _branch(
            schedule_deployment_branch,
            clear_schedule_deployment_branch,
            "schedule-deployment-branch",
        ),
    }
    workspace = common.get_admin().workspaces.update(uid, **settings)
    common.print_model(workspace, as_json=as_json)


def _branch(branch: Optional[str], clear: bool, option: str) -> Any:
    if branch is not None and clear:
        raise click.UsageError(f"Use --{option} or --clear-{option}, not both.")
    if clear:
        return None
    return branch if branch is not None else UNSET


@workspaces.command()
@click.argument("uid")
@click.confirmation_option(prompt="Delete this workspace?")
@common.json_option
@common.handle_admin_errors
def delete(uid: str, as_json: bool) -> None:
    """
    Delete a workspace. Deleting it again is not an error.

    UID is the workspace's uid.
    """
    workspace = common.get_admin().workspaces.delete(uid)
    if as_json:
        common.print_model(workspace, as_json=True)
        return
    console.success(f"Deleted workspace {workspace.name} ({workspace.uid}).")
