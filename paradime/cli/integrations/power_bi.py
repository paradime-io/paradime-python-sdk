from typing import List, Optional

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.power_bi import (
    get_access_token,
    get_power_bi_datasets,
    trigger_power_bi_refreshes,
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
