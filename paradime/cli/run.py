import sys
from typing import Final, List, Optional

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.airbyte import list_airbyte_connections, trigger_airbyte_jobs
from paradime.core.scripts.fivetran import list_fivetran_connectors, trigger_fivetran_sync
from paradime.core.scripts.montecarlo import search_for_files_to_upload_to_montecarlo
from paradime.core.scripts.power_bi import (
    get_access_token,
    get_power_bi_datasets,
    trigger_power_bi_refreshes,
)
from paradime.core.scripts.tableau import (
    list_tableau_datasources,
    list_tableau_workbooks,
    trigger_tableau_datasource_refresh,
    trigger_tableau_refresh,
)

help_string: Final = (
    "\nTo set environment variables please go to https://app.paradime.io/settings/env-variables"
)


@click.group(context_settings=dict(max_content_width=160))
def run() -> None:
    """
    Run predefined code runs to automate your workflows.
    """
    pass


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "site-name",
    "TABLEAU_SITE_NAME",
    help="The name of the tableau site. Set this only if you are using a site other than the default site.",
    required=False,
    default="",
)
@env_click_option(
    "workbook-name",
    env_var=None,
    multiple=True,
    help="The name or UUID of the workbook(s) you want to refresh",
    required=False,
)
@env_click_option(
    "datasource-name",
    env_var=None,
    multiple=True,
    help="The name or UUID of the data source(s) you want to refresh",
    required=False,
)
@env_click_option(
    "host",
    "TABLEAU_HOST",
    help="The base url of your tableau server (e.g. https://prod-uk-a.online.tableau.com/)",
)
@env_click_option(
    "personal-access-token-secret",
    "TABLEAU_PERSONAL_ACCESS_TOKEN_SECRET",
    help="You can create a personal access token in your tableau account settings: https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm",
)
@env_click_option(
    "personal-access-token-name",
    "TABLEAU_PERSONAL_ACCESS_TOKEN_NAME",
    help="You can create a personal access token in your tableau account settings: https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm",
)
@click.option(
    "--wait-for-completion/--no-wait-for-completion",
    default=True,
    help="Wait for the refresh job to complete before returning. Shows progress and final status.",
)
@env_click_option(
    "timeout-minutes",
    "TABLEAU_REFRESH_TIMEOUT_MINUTES",
    type=int,
    default=30,
    help="Maximum time to wait for refresh completion (in minutes). Only used with --wait-for-completion.",
)
def tableau_refresh(
    site_name: str,
    workbook_name: Optional[List[str]],
    datasource_name: Optional[List[str]],
    host: str,
    personal_access_token_secret: str,
    personal_access_token_name: str,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger a Tableau refresh for workbooks or data sources.
    """
    if not workbook_name and not datasource_name:
        raise click.UsageError("Must specify either --workbook-name or --datasource-name")

    if workbook_name and datasource_name:
        raise click.UsageError(
            "Cannot specify both --workbook-name and --datasource-name. Choose one."
        )

    if workbook_name:
        click.echo(f"Tableau workbook refresh started on site {site_name}...")
        results = trigger_tableau_refresh(
            host=host,
            personal_access_token_name=personal_access_token_name,
            personal_access_token_secret=personal_access_token_secret,
            site_name=site_name or "",
            workbook_names=workbook_name,
            api_version="3.4",
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any refreshes failed
        failed_refreshes = [
            result for result in results if "FAILED" in result or "CANCELED" in result
        ]
        if failed_refreshes:
            sys.exit(1)

    if datasource_name:
        click.echo(f"Tableau data source refresh started on site {site_name}...")
        results = trigger_tableau_datasource_refresh(
            host=host,
            personal_access_token_name=personal_access_token_name,
            personal_access_token_secret=personal_access_token_secret,
            site_name=site_name or "",
            datasource_names=datasource_name,
            api_version="3.4",
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any refreshes failed
        failed_refreshes = [
            result for result in results if "FAILED" in result or "CANCELED" in result
        ]
        if failed_refreshes:
            sys.exit(1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "site-name",
    "TABLEAU_SITE_NAME",
    help="The name of the tableau site. Set this only if you are using a site other than the default site.",
    required=False,
    default="",
)
@env_click_option(
    "host",
    "TABLEAU_HOST",
    help="The base url of your tableau server (e.g. https://prod-uk-a.online.tableau.com/)",
)
@env_click_option(
    "personal-access-token-secret",
    "TABLEAU_PERSONAL_ACCESS_TOKEN_SECRET",
    help="You can create a personal access token in your tableau account settings: https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm",
)
@env_click_option(
    "personal-access-token-name",
    "TABLEAU_PERSONAL_ACCESS_TOKEN_NAME",
    help="You can create a personal access token in your tableau account settings: https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm",
)
def tableau_list_workbooks(
    site_name: str,
    host: str,
    personal_access_token_secret: str,
    personal_access_token_name: str,
) -> None:
    """
    List all Tableau workbooks with their names and UUIDs.
    """
    click.echo(f"Listing Tableau workbooks on site {site_name}...")

    list_tableau_workbooks(
        host=host,
        personal_access_token_name=personal_access_token_name,
        personal_access_token_secret=personal_access_token_secret,
        site_name=site_name or "",
        api_version="3.4",
    )


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "site-name",
    "TABLEAU_SITE_NAME",
    help="The name of the tableau site. Set this only if you are using a site other than the default site.",
    required=False,
    default="",
)
@env_click_option(
    "host",
    "TABLEAU_HOST",
    help="The base url of your tableau server (e.g. https://prod-uk-a.online.tableau.com/)",
)
@env_click_option(
    "personal-access-token-secret",
    "TABLEAU_PERSONAL_ACCESS_TOKEN_SECRET",
    help="You can create a personal access token in your tableau account settings: https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm",
)
@env_click_option(
    "personal-access-token-name",
    "TABLEAU_PERSONAL_ACCESS_TOKEN_NAME",
    help="You can create a personal access token in your tableau account settings: https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm",
)
def tableau_list_datasources(
    site_name: str,
    host: str,
    personal_access_token_secret: str,
    personal_access_token_name: str,
) -> None:
    """
    List all Tableau data sources with their names and UUIDs.
    """
    click.echo(f"Listing Tableau data sources on site {site_name}...")

    list_tableau_datasources(
        host=host,
        personal_access_token_name=personal_access_token_name,
        personal_access_token_secret=personal_access_token_secret,
        site_name=site_name or "",
        api_version="3.4",
    )


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "client-id",
    "POWER_BI_CLIENT_ID",
    help="The client id of your power bi application",
)
@env_click_option(
    "tenant-id",
    "POWER_BI_TENANT_ID",
    help="The tenant id of your power bi application",
)
@env_click_option(
    "client-secret",
    "POWER_BI_CLIENT_SECRET",
    help="The client secret of your power bi application",
)
@env_click_option(
    "group-id",
    "POWER_BI_GROUP_ID",
    help="The group id of your power bi workspace",
)
@env_click_option(
    "dataset-name",
    env_var=None,
    help="The dataset name(s) you want to refresh",
    multiple=True,
)
@env_click_option(
    "refresh-request-body-b64",
    "POWER_BI_REFRESH_REQUEST_BODY_B64",
    help="A base64 encoded json string to send as the request body - https://learn.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh#parameters.",
    required=False,
)
def power_bi_refresh(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    group_id: str,
    dataset_name: List[str],
    refresh_request_body_b64: Optional[str],
) -> None:
    """
    Trigger a Power BI refresh for a specific dataset.
    """
    click.echo(f"Power BI refresh started in group {group_id}...")

    trigger_power_bi_refreshes(
        client_id=client_id,
        client_secret=client_secret,
        group_id=group_id,
        dataset_names=dataset_name,
        refresh_request_body_b64=refresh_request_body_b64,
        tenant_id=tenant_id,
    )


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "client-id",
    "POWER_BI_CLIENT_ID",
    help="The client id of your power bi application",
)
@env_click_option(
    "client-secret",
    "POWER_BI_CLIENT_SECRET",
    help="The client secret of your power bi application",
)
@env_click_option(
    "group-id",
    "POWER_BI_GROUP_ID",
    help="The group id of your power bi workspace",
)
@env_click_option(
    "tenant-id",
    "POWER_BI_TENANT_ID",
    help="The tenant id of your power bi application",
)
def power_bi_list_datasets(
    client_id: str,
    client_secret: str,
    group_id: str,
    tenant_id: str,
) -> None:
    """
    List Power BI datasets.
    """
    access_token = get_access_token(tenant_id, client_id, client_secret)
    datasets = get_power_bi_datasets(
        access_token=access_token,
        group_id=group_id,
    )
    for dataset in datasets.values():
        click.echo(f"{dataset.name}:{dataset.id}")


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "project-name",
    "MONTECARLO_PROJECT_NAME",
    help="The name of the montecarlo project.",
)
@env_click_option(
    "connection-id",
    "MONTECARLO_CONNECTION_ID",
    help="The id of the montecarlo connection.",
)
@env_click_option(
    "paradime-resources-directory",
    "PARADIME_RESOURCES_DIRECTORY",
    help="The directory where the paradime resources are stored.",
    required=False,
)
@env_click_option(
    "paradime-schedule-name",
    "PARADIME_SCHEDULE_NAME",
    help="The name of the paradime schedule.",
)
def montecarlo_artifacts_import(
    paradime_resources_directory: Optional[str],
    paradime_schedule_name: str,
    project_name: str,
    connection_id: str,
) -> None:
    """
    Upload Bolt artifacts to Montecarlo
    """
    success, found_files = search_for_files_to_upload_to_montecarlo(
        paradime_resources_directory=paradime_resources_directory or ".",
        paradime_schedule_name=paradime_schedule_name,
        project_name=project_name,
        connection_id=connection_id,
    )
    if not success:
        sys.exit(1)

    if not found_files:
        click.echo(
            f"No files found in {paradime_resources_directory or 'current directory'} to upload to Montecarlo."
        )


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-key",
    "FIVETRAN_API_KEY",
    help="Your Fivetran API key. You can find this in your Fivetran account settings.",
)
@env_click_option(
    "api-secret",
    "FIVETRAN_API_SECRET",
    help="Your Fivetran API secret. You can find this in your Fivetran account settings.",
)
@click.option(
    "--connector-id",
    multiple=True,
    help="The ID(s) of the connector(s) you want to sync",
    required=True,
)
@click.option(
    "--force",
    is_flag=True,
    help="Force restart any ongoing syncs",
    default=False,
)
@click.option(
    "--wait-for-completion",
    is_flag=True,
    help="Wait for syncs to complete before returning",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for sync completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def fivetran_sync(
    api_key: str,
    api_secret: str,
    connector_id: List[str],
    force: bool,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger sync for Fivetran connectors.
    """
    click.echo(f"Starting sync for {len(connector_id)} Fivetran connector(s)...")

    try:
        results = trigger_fivetran_sync(
            api_key=api_key,
            api_secret=api_secret,
            connector_ids=list(connector_id),
            force=force,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any syncs failed, were paused, or rescheduled
        failed_syncs = [
            result
            for result in results
            if "FAILED" in result or "PAUSED" in result or "RESCHEDULED" in result
        ]
        if failed_syncs:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Fivetran sync failed: {str(e)}")
        raise click.Abort()


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "api-key",
    "FIVETRAN_API_KEY",
    help="Your Fivetran API key. You can find this in your Fivetran account settings.",
)
@env_click_option(
    "api-secret",
    "FIVETRAN_API_SECRET",
    help="Your Fivetran API secret. You can find this in your Fivetran account settings.",
)
@click.option(
    "--group-id",
    help="Optional group ID to filter connectors by group",
    required=False,
)
def fivetran_list_connectors(
    api_key: str,
    api_secret: str,
    group_id: Optional[str],
) -> None:
    """
    List all available Fivetran connectors with their status.
    """
    if group_id:
        click.echo(f"Listing Fivetran connectors for group {group_id}...")
    else:
        click.echo("Listing all Fivetran connectors...")

    list_fivetran_connectors(
        api_key=api_key,
        api_secret=api_secret,
        group_id=group_id,
    )


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "client-id",
    "AIRBYTE_CLIENT_ID",
    help="Your Airbyte client ID.",
)
@env_click_option(
    "client-secret",
    "AIRBYTE_CLIENT_SECRET",
    help="Your Airbyte client secret.",
)
@env_click_option(
    "base-url",
    "AIRBYTE_BASE_URL",
    help="Airbyte API base URL. Default: https://api.airbyte.com/v1 (Cloud)",
    default="https://api.airbyte.com/v1",
)
@click.option(
    "--use-server-auth",
    is_flag=True,
    help="Use basic authentication for self-hosted Airbyte Server (instead of OAuth for Cloud)",
    default=False,
)
@click.option(
    "--connection-id",
    multiple=True,
    help="The ID(s) of the connection(s) you want to run jobs for",
    required=True,
)
@click.option(
    "--job-type",
    type=click.Choice(["sync", "reset"]),
    help="Type of job to run (sync or reset)",
    required=True,
)
@click.option(
    "--workspace-id",
    help="Optional workspace ID",
    required=False,
)
@click.option(
    "--wait-for-completion",
    is_flag=True,
    help="Wait for jobs to complete before returning",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for job completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def airbyte_sync(
    client_id: str,
    client_secret: str,
    base_url: str,
    use_server_auth: bool,
    connection_id: List[str],
    job_type: str,
    workspace_id: Optional[str],
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger sync or reset jobs for Airbyte connections.
    """
    click.echo(f"Starting {job_type} jobs for {len(connection_id)} Airbyte connection(s)...")

    try:
        results = trigger_airbyte_jobs(
            client_id=client_id,
            client_secret=client_secret,
            connection_ids=list(connection_id),
            job_type=job_type,
            workspace_id=workspace_id,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
            base_url=base_url,
            use_cloud_auth=not use_server_auth,
        )

        # Check if any jobs failed, were cancelled, or incomplete
        failed_jobs = [
            result
            for result in results
            if "FAILED" in result or "CANCELLED" in result or "INCOMPLETE" in result
        ]
        if failed_jobs:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Airbyte {job_type} failed: {str(e)}")
        raise click.Abort()


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "client-id",
    "AIRBYTE_CLIENT_ID",
    help="Your Airbyte client ID (Cloud) or API key (Server).",
)
@env_click_option(
    "client-secret",
    "AIRBYTE_CLIENT_SECRET",
    help="Your Airbyte client secret (Cloud) or API secret (Server).",
)
@env_click_option(
    "base-url",
    "AIRBYTE_BASE_URL",
    help="Airbyte API base URL. Default: https://api.airbyte.com/v1 (Cloud)",
    default="https://api.airbyte.com/v1",
)
@click.option(
    "--use-server-auth",
    is_flag=True,
    help="Use basic authentication for self-hosted Airbyte Server (instead of OAuth for Cloud)",
    default=False,
)
@click.option(
    "--workspace-id",
    help="Optional workspace ID to filter connections by workspace",
    required=False,
)
def airbyte_list_connections(
    client_id: str,
    client_secret: str,
    base_url: str,
    use_server_auth: bool,
    workspace_id: Optional[str],
) -> None:
    """
    List all available Airbyte connections with their status.
    """
    if workspace_id:
        click.echo(f"Listing Airbyte connections for workspace {workspace_id}...")
    else:
        click.echo("Listing all Airbyte connections...")

    list_airbyte_connections(
        client_id=client_id,
        client_secret=client_secret,
        workspace_id=workspace_id,
        base_url=base_url,
        use_cloud_auth=not use_server_auth,
    )


run.add_command(tableau_refresh)
run.add_command(tableau_list_workbooks)
run.add_command(tableau_list_datasources)
run.add_command(power_bi_refresh)
run.add_command(power_bi_list_datasets)
run.add_command(fivetran_sync)
run.add_command(fivetran_list_connectors)
run.add_command(airbyte_sync)
run.add_command(airbyte_list_connections)
run.add_command(montecarlo_artifacts_import)
