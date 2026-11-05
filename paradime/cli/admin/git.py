from typing import Optional

import click

from paradime.apis.admin.git.client import GitConnection
from paradime.apis.admin.git.types import (
    DeployKeyConnectionInput,
    GitHubAppConnectionInput,
    GitLabAppConnectionInput,
    GitProvider,
)
from paradime.cli import console
from paradime.cli.admin import common

PROVIDER_CHOICE = click.Choice(
    ["GITHUB", "GITLAB", "BITBUCKET", "AZURE_DEVOPS"], case_sensitive=False
)


@click.group()
def git() -> None:
    """
    Manage the workspace's git repository and deploy key.
    """
    pass


@git.command()
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def show(workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Show the workspace's repository, how Paradime reaches it and its deploy key.
    """
    repository = common.get_admin().git.get_repository(workspace_uid=workspace_uid)
    if repository is None:
        console.info("No repository information.")
        return
    common.print_model(repository, as_json=as_json)


def _connect(
    connection: GitConnection,
    *,
    wait: bool,
    timeout: float,
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    result = common.get_admin().git.connect(connection, workspace_uid=workspace_uid)
    if as_json and result.job is None:
        console.json_out(common.to_json(result))
        return
    if not as_json:
        common.print_model(result.git_repository, as_json=False)
    common.report_job(
        result.job, wait=wait, timeout=timeout, workspace_uid=workspace_uid, as_json=as_json
    )


@git.command(name="connect-deploy-key")
@click.option("--provider", type=PROVIDER_CHOICE, required=True, help="The service hosting it.")
@click.option("--uri", required=True, help="The SSH URI, e.g. git@github.com:org/repo.git.")
@click.option("--dbt-project-dir", default=None, help="The folder holding dbt_project.yml.")
@click.option(
    "--no-validate",
    is_flag=True,
    help="Only save the repository, until the deploy key is added to it.",
)
@click.option("--force", is_flag=True, help="Replace a cloned repository with another one.")
@common.wait_options
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def connect_deploy_key(
    provider: str,
    uri: str,
    dbt_project_dir: Optional[str],
    no_validate: bool,
    force: bool,
    wait: bool,
    timeout: float,
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    Connect a repository through the workspace's deploy key. Add the key (paradime admin git
    show) to the repository with write access first.
    """
    connection = DeployKeyConnectionInput(provider=GitProvider(provider.upper()), uri=uri)
    if dbt_project_dir is not None:
        connection.dbt_project_dir = dbt_project_dir
    if no_validate:
        connection.validate_repository = False
    if force:
        connection.force = True
    _connect(connection, wait=wait, timeout=timeout, workspace_uid=workspace_uid, as_json=as_json)


@git.command(name="connect-github-app")
@click.option("--installation-id", default=None, help="An installation the account holds.")
@click.option("--repository-id", default=None, help="The repository's numeric GitHub id.")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def connect_github_app(
    installation_id: Optional[str],
    repository_id: Optional[str],
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    Connect a repository through the Paradime GitHub App. Leave both options out to move the
    current repository to the App.
    """
    connection = GitHubAppConnectionInput(
        installation_id=installation_id, repository_id=repository_id
    )
    _connect(connection, wait=False, timeout=0, workspace_uid=workspace_uid, as_json=as_json)


@git.command(name="connect-gitlab-app")
@click.option("--base-url", default=None, help="The GitLab connection's base URL.")
@click.option("--project-id", default=None, help="The project's numeric GitLab id.")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def connect_gitlab_app(
    base_url: Optional[str],
    project_id: Optional[str],
    workspace_uid: Optional[str],
    as_json: bool,
) -> None:
    """
    Connect a project through the account's GitLab connection. Leave both options out to move
    the current repository to the connection.
    """
    connection = GitLabAppConnectionInput(base_url=base_url, project_id=project_id)
    _connect(connection, wait=False, timeout=0, workspace_uid=workspace_uid, as_json=as_json)


@git.command()
@click.argument("uri")
@common.wait_options
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def check(
    uri: str, wait: bool, timeout: float, workspace_uid: Optional[str], as_json: bool
) -> None:
    """
    Check the workspace's deploy key can push to the repository at URI. Changes nothing.
    """
    job = common.get_admin().git.check_repository(uri, workspace_uid=workspace_uid)
    common.report_job(job, wait=wait, timeout=timeout, workspace_uid=workspace_uid, as_json=as_json)


@git.command(name="rotate-key")
@click.option(
    "--force",
    is_flag=True,
    help="Rotate a key the connected repository uses; clone and push break until the new key "
    "is added to it.",
)
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def rotate_key(force: bool, workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Replace the workspace's deploy key and print the new public key.
    """
    key = common.get_admin().git.rotate_deploy_key(force=force, workspace_uid=workspace_uid)
    if as_json:
        console.json_out(common.to_json(key))
        return
    click.echo(key.public_key)
