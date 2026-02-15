import sys
from typing import Optional

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.montecarlo import search_for_files_to_upload_to_montecarlo


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
