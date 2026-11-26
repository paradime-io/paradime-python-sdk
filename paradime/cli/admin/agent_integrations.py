from typing import Optional

import click

from paradime.apis.admin.integrations.types import AgentIntegrationName
from paradime.cli import console
from paradime.cli.admin import common

NAME_CHOICE = click.Choice(
    [name.value for name in AgentIntegrationName if name != AgentIntegrationName.UNKNOWN],
    case_sensitive=False,
)


@click.group(name="agent-integrations")
def agent_integrations() -> None:
    """
    Manage the integrations the workspace's DinoAI agents connect through Nango, such as
    Notion, Jira and Google Drive. A person connects one in a browser.
    """
    pass


@agent_integrations.command(name="list")
@click.option(
    "--install-urls",
    "include_install_urls",
    is_flag=True,
    help="Also show a Connect link for each integration waiting for a person. Each list makes "
    "new links, which work for 30 minutes; anyone who opens one can connect an account. Needs "
    "integration_settings:write.",
)
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def list_agent_integrations(
    include_install_urls: bool, workspace_uid: Optional[str], as_json: bool
) -> None:
    """
    List the workspace's DinoAI agent integrations, connected or not.
    """
    columns = ["Name", "Display name", "Status", "Reconnect required"]
    fields = ["name", "display_name", "status", "reconnect_required"]
    if include_install_urls:
        columns += ["Install URL", "Expires"]
        fields += ["install_url", "install_url_expires_at"]
    common.print_models(
        common.get_admin().integrations.list_agent_integrations(
            include_install_urls=include_install_urls, workspace_uid=workspace_uid
        ),
        columns=columns,
        fields=fields,
        as_json=as_json,
    )


@agent_integrations.command()
@click.argument("name", type=NAME_CHOICE)
@click.confirmation_option(prompt="Disconnect this agent integration?")
@common.workspace_option
@common.json_option
@common.handle_admin_errors
def disconnect(name: str, workspace_uid: Optional[str], as_json: bool) -> None:
    """
    Disconnect a DinoAI agent integration, such as NOTION, and delete its Nango connection.
    Disconnecting it again is not an error.
    """
    integration = common.get_admin().integrations.disconnect_agent_integration(
        AgentIntegrationName(name.upper()), workspace_uid=workspace_uid
    )
    if as_json:
        console.json_out(common.to_json(integration))
        return
    console.success(f"Disconnected {integration.name.value} ({integration.status.value}).")
