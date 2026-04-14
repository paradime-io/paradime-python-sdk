from __future__ import annotations

import sys
from typing import List, Optional

import click

from paradime.cli import console
from paradime.cli.utils import (
    COMMA_LIST,
    deprecated_alias_option,
    env_click_option,
    resolve_deprecated_option,
)
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
    "dataset-names",
    env_var=None,
    help="Comma-separated dataset name(s) to refresh",
    type=COMMA_LIST,
)
@deprecated_alias_option("dataset-name", "dataset-names", type=COMMA_LIST, default=None)
@env_click_option(
    "refresh-request-body-b64",
    "POWER_BI_REFRESH_REQUEST_BODY_B64",
    help="A base64 encoded json string to send as the request body - https://learn.microsoft.com/en-us/power-bi/connect-data/asynchronous-refresh#parameters.",
    required=False,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def power_bi_refresh(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    group_id: str,
    dataset_names: List[str],
    dataset_name: Optional[List[str]],
    refresh_request_body_b64: Optional[str],
    json_output: bool,
) -> None:
    """
    Trigger a Power BI refresh for a specific dataset.
    """
    dataset_names = resolve_deprecated_option(
        dataset_names, dataset_name, "dataset-names", "dataset-name"
    )

    if not json_output:
        console.header(f"Power BI — Refresh Datasets (group {group_id})")

    try:
        trigger_power_bi_refreshes(
            client_id=client_id,
            client_secret=client_secret,
            group_id=group_id,
            dataset_names=dataset_names,
            refresh_request_body_b64=refresh_request_body_b64,
            tenant_id=tenant_id,
        )
        if json_output:
            console.json_out({"success": True, "datasets": list(dataset_names)})
    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        raise


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
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def power_bi_list_datasets(
    client_id: str,
    client_secret: str,
    group_id: str,
    tenant_id: str,
    json_output: bool,
) -> None:
    """
    List Power BI datasets.
    """
    access_token = get_access_token(tenant_id, client_id, client_secret)
    datasets = get_power_bi_datasets(
        access_token=access_token,
        group_id=group_id,
    )
    if json_output:
        console.json_out(
            [
                {"name": dataset.name, "id": dataset.id, "is_refreshable": dataset.is_refreshable}
                for dataset in datasets.values()
            ]
        )
    else:
        for dataset in datasets.values():
            console.kv(dataset.name, dataset.id)
