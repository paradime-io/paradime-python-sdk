from __future__ import annotations

import sys
from typing import Optional

import click

from paradime.cli import console
from paradime.cli.utils import env_click_option
from paradime.core.scripts.datahub import push_artifacts_to_datahub


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "datahub-server",
    "DATAHUB_GMS_URL",
    help="The DataHub GMS server URL (e.g. https://<instance>.acryl.io/gms).",
)
@env_click_option(
    "datahub-token",
    "DATAHUB_GMS_TOKEN",
    help="The DataHub personal access token used to authenticate to GMS. "
    "Required for DataHub Cloud / auth-enabled instances; omit for a local instance "
    "with metadata service auth disabled.",
    required=False,
)
@env_click_option(
    "target-platform",
    "DATAHUB_TARGET_PLATFORM",
    help="The data warehouse platform the dbt models run on (e.g. snowflake, bigquery, redshift).",
)
@env_click_option(
    "domain",
    "DATAHUB_DOMAIN",
    help="Optional DataHub domain URN to associate the pushed assets with "
    "(e.g. urn:li:domain:sales).",
    required=False,
)
@env_click_option(
    "paradime-resources-directory",
    "PARADIME_RESOURCES_DIRECTORY",
    help="The directory where the paradime resources are stored.",
    required=False,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def datahub_artifacts_push(
    datahub_server: str,
    datahub_token: Optional[str],
    target_platform: str,
    domain: Optional[str],
    paradime_resources_directory: Optional[str],
    json_output: bool,
) -> None:
    """
    Push Bolt dbt artifacts (manifest + catalog) to DataHub
    """
    try:
        success, found_files = push_artifacts_to_datahub(
            paradime_resources_directory=paradime_resources_directory or ".",
            datahub_server=datahub_server,
            datahub_token=datahub_token,
            target_platform=target_platform,
            domain=domain,
        )
    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        raise

    if json_output:
        console.json_out({"success": success, "found_files": found_files})
        if not success:
            sys.exit(1)
        return

    if not success:
        sys.exit(1)

    if not found_files:
        console.warning(
            f"No dbt artifacts found in {paradime_resources_directory or 'current directory'} "
            "to push to DataHub."
        )
