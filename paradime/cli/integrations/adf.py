import sys
from typing import List

import click

from paradime.cli.utils import env_click_option
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
    multiple=True,
    help="The name(s) of the pipeline(s) you want to trigger",
    required=True,
)
@click.option(
    "--wait-for-completion/--no-wait-for-completion",
    default=True,
    help="Wait for pipeline runs to complete before returning. Shows progress and final status.",
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for pipeline completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
def adf_pipelines(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
    pipeline_names: List[str],
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger runs for Azure Data Factory pipelines.
    """
    click.echo(
        f"Starting runs for {len(pipeline_names)} Azure Data Factory pipeline(s) "
        f"in factory {factory_name}..."
    )

    try:
        results = trigger_adf_pipeline_runs(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret,
            subscription_id=subscription_id,
            resource_group=resource_group,
            factory_name=factory_name,
            pipeline_names=list(pipeline_names),
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any runs failed or were cancelled
        failed_runs = [
            result
            for result in results
            if "FAILED" in result or "CANCELLED" in result or "CANCELLING" in result
        ]
        if failed_runs:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Azure Data Factory pipeline run failed: {str(e)}")
        raise click.Abort()


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
def adf_list_pipelines(
    tenant_id: str,
    client_id: str,
    client_secret: str,
    subscription_id: str,
    resource_group: str,
    factory_name: str,
) -> None:
    """
    List all available pipelines in an Azure Data Factory.
    """
    click.echo(f"Listing pipelines in Azure Data Factory {factory_name}...")

    list_adf_pipelines(
        tenant_id=tenant_id,
        client_id=client_id,
        client_secret=client_secret,
        subscription_id=subscription_id,
        resource_group=resource_group,
        factory_name=factory_name,
    )
