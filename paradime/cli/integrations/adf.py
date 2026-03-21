from __future__ import annotations

import sys
from typing import List

import click

from paradime.cli import console
from paradime.cli.utils import COMMA_LIST, env_click_option
from paradime.core.scripts.azure_data_factory import list_adf_pipelines, trigger_adf_pipeline_runs


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "tenant-id",
    "ADF_TENANT_ID",
    help="Your Azure AD tenant ID.",
)
@env_click_option(
    "client-id",
    "ADF_CLIENT_ID",
    help="Your Azure AD application (service principal) client ID.",
)
@env_click_option(
    "client-secret",
    "ADF_CLIENT_SECRET",
    help="Your Azure AD application client secret.",
)
@env_click_option(
    "subscription-id",
    "ADF_SUBSCRIPTION_ID",
    help="Your Azure subscription ID.",
)
@env_click_option(
    "resource-group",
    "ADF_RESOURCE_GROUP",
    help="Your Azure resource group name.",
)
@env_click_option(
    "factory-name",
    "ADF_FACTORY_NAME",
    help="Your Azure Data Factory name.",
)
@click.option(
    "--pipeline-names",
    type=COMMA_LIST,
    help="Comma-separated pipeline name(s) to trigger",
    required=True,
)
@click.option(
    "--wait/--no-wait",
    default=True,
    help="Wait for pipeline runs to complete before returning. Shows progress and final status.",
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def adf_pipelines(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    pipeline_names: List[str],
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger runs for Azure Data Factory pipelines.
    """
    if not json_output:
        console.header(f"Azure Data Factory — {factory_name}")

    try:
        results = trigger_adf_pipeline_runs(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            subscription_id=subscription_id,
            resource_group=resource_group,
            factory_name=factory_name,
            pipeline_names=pipeline_names,
            wait_for_completion=wait,
            timeout_minutes=timeout,
        )

        if json_output:
            failed = [r for r in results if "FAILED" in r or "CANCELLED" in r or "CANCELLING" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any runs failed or were cancelled
        failed_runs = [
            result
            for result in results
            if "FAILED" in result or "CANCELLED" in result or "CANCELLING" in result
        ]
        if failed_runs:
            console.error(f"{len(failed_runs)} pipeline run(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Azure Data Factory pipeline run failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "tenant-id",
    "ADF_TENANT_ID",
    help="Your Azure AD tenant ID.",
)
@env_click_option(
    "client-id",
    "ADF_CLIENT_ID",
    help="Your Azure AD application (service principal) client ID.",
)
@env_click_option(
    "client-secret",
    "ADF_CLIENT_SECRET",
    help="Your Azure AD application client secret.",
)
@env_click_option(
    "subscription-id",
    "ADF_SUBSCRIPTION_ID",
    help="Your Azure subscription ID.",
)
@env_click_option(
    "resource-group",
    "ADF_RESOURCE_GROUP",
    help="Your Azure resource group name.",
)
@env_click_option(
    "factory-name",
    "ADF_FACTORY_NAME",
    help="Your Azure Data Factory name.",
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def adf_list_pipelines(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    json_output: bool,
) -> None:
    """
    List all available pipelines in an Azure Data Factory.
    """
    if not json_output:
        console.info(f"Listing pipelines in Azure Data Factory {factory_name}…")

    result = list_adf_pipelines(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        subscription_id=subscription_id,
        resource_group=resource_group,
        factory_name=factory_name,
        json_output=json_output,
    )
    if json_output and result is not None:
        console.json_out(result)
