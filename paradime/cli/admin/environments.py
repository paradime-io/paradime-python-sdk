from typing import Any, Optional

import click

from paradime.apis.admin.environments.types import EnvironmentType
from paradime.apis.admin.types import UNSET
from paradime.cli import console
from paradime.cli.admin import common

TYPE_CHOICE = click.Choice(["SCHEDULER", "AGENT"], case_sensitive=False)


@click.group()
def environments() -> None:
    """
    Manage a workspace's Bolt (SCHEDULER) and agent (AGENT) environments.
    """
    pass


@environments.command(name="list")
@click.option("--type", "environment_type", type=TYPE_CHOICE, default=None, help="Only this type.")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def list_environments(
    environment_type: Optional[str], workspace_uid: Optional[str], as_json: bool
) -> None:
    """
    List the workspace's environments.
    """
    common.print_models(
        common.get_admin().environments.list(
            type=EnvironmentType(environment_type.upper()) if environment_type else None,
            workspace_uid=workspace_uid,
        ),
        columns=["Slug", "Type", "Name", "Default", "Branch", "dbt version", "Connection"],
        fields=[
            "slug",
            "type",
            "name",
            "is_default",
            "branch",
            "dbt_version",
            "has_warehouse_connection",
        ],
        as_json=as_json,
    )


@environments.command()
@click.option(
    "--type", "environment_type", type=TYPE_CHOICE, required=True, help="What runs in it."
)
@click.option("--name", required=True, help="The display name.")
@click.option("--slug", default=None, help="The slug to give it; minted when left out.")
@click.option("--branch", default=None, help="The git branch its runs use.")
@click.option("--dbt-version", default=None, help="The dbt version its runs use.")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def create(
    environment_type: str,
    name: str,
    slug: Optional[str],
    branch: Optional[str],
    dbt_version: Optional[str],
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    Create an environment. With --slug, running it again returns the same environment.
    """
    environment = common.get_admin().environments.create(
        EnvironmentType(environment_type.upper()),
        name,
        slug=slug,
        branch=branch,
        dbt_version=dbt_version,
        workspace_uid=workspace_uid,
    )
    common.print_model(environment, as_json=as_json)


@environments.command()
@click.argument("slug")
@click.option("--name", default=None, help="A new display name.")
@click.option("--branch", default=None, help="The git branch its runs use.")
@click.option("--clear-branch", is_flag=True, help="Use the workspace's default branch.")
@click.option("--dbt-version", default=None, help="The dbt version its runs use.")
@click.option("--clear-dbt-version", is_flag=True, help="Use the workspace's dbt version.")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def update(
    slug: str,
    name: Optional[str],
    branch: Optional[str],
    clear_branch: bool,
    dbt_version: Optional[str],
    clear_dbt_version: bool,
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    Change an environment. Settings left out keep their value.
    """
    environment = common.get_admin().environments.update(
        slug,
        name=name if name is not None else UNSET,
        branch=_setting(branch, clear_branch, "branch"),
        dbt_version=_setting(dbt_version, clear_dbt_version, "dbt-version"),
        workspace_uid=workspace_uid,
    )
    common.print_model(environment, as_json=as_json)


def _setting(value: Optional[str], clear: bool, option: str) -> Any:
    if value is not None and clear:
        raise click.UsageError(f"Use --{option} or --clear-{option}, not both.")
    if clear:
        return None
    return value if value is not None else UNSET


@environments.command()
@click.argument("slug")
@click.confirmation_option(prompt="Delete this environment?")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def delete(slug: str, workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Delete an environment. Deleting it again is not an error.
    """
    environment = common.get_admin().environments.delete(slug, workspace_uid=workspace_uid)
    if as_json:
        common.print_model(environment, as_json=True)
        return
    console.success(f"Deleted environment {environment.slug}.")


@environments.command(name="set-default")
@click.argument("slug")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def set_default(slug: str, workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Make an environment the default of its type.
    """
    environment = common.get_admin().environments.set_default(slug, workspace_uid=workspace_uid)
    common.print_model(environment, as_json=as_json)


@environments.command(name="set-defer")
@click.argument("slug")
@click.option("--environment", "environment_slug", default=None, help="A Bolt environment.")
@click.option("--schedule", "schedule_slug", default=None, help="A Bolt schedule's slug.")
@click.option("--clear", is_flag=True, help="Defer to nothing.")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def set_defer(
    slug: str,
    environment_slug: Optional[str],
    schedule_slug: Optional[str],
    clear: bool,
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    Set what an agent environment's runs defer to: --environment, --schedule or --clear.
    """
    if sum([bool(environment_slug), bool(schedule_slug), clear]) != 1:
        raise click.UsageError("Give exactly one of --environment, --schedule or --clear.")
    environment = common.get_admin().environments.set_agent_defer_target(
        slug,
        environment_slug=environment_slug,
        schedule_slug=schedule_slug,
        workspace_uid=workspace_uid,
    )
    common.print_model(environment, as_json=as_json)
