from typing import Final, List, Optional

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.power_bi import (
    get_access_token,
    get_power_bi_datasets,
    trigger_power_bi_refreshes,
)
from paradime.core.scripts.tableau import trigger_tableau_refresh

help_string: Final = (
    "\nTo set environment variables please go to https://app.paradime.io/account-settings/workspace"
)


@click.group()
def run() -> None:
    """
    Run predefined code runs to automate your workflows.
    """
    pass


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "site-name",
    "TABLEAU_SITE_NAME",
)
@env_click_option(
    "workbook-name",
    env_var=None,
    multiple=True,
    help="The name of the workbook(s) you want to refresh",
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
def tableau_refresh(
    site_name: str,
    workbook_name: List[str],
    host: str,
    personal_access_token_secret: str,
    personal_access_token_name: str,
) -> None:
    """
    Trigger a Tableau refresh for a specific workbook.
    """
    click.echo(f"Tableau refresh started on site {site_name}...")

    trigger_tableau_refresh(
        host=host,
        personal_access_token_name=personal_access_token_name,
        personal_access_token_secret=personal_access_token_secret,
        site_name=site_name,
        workbook_names=workbook_name,
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


run.add_command(tableau_refresh)
run.add_command(power_bi_refresh)
run.add_command(power_bi_list_datasets)
