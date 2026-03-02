import sys
from typing import List

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.gcp_datastream import list_datastream_streams, trigger_datastream


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "service-account-key-file",
    "GCP_SERVICE_ACCOUNT_KEY_FILE",
    help="Path to your GCP service account JSON key file.",
)
@env_click_option(
    "project",
    "GCP_PROJECT_ID",
    help="Your GCP project ID.",
)
@env_click_option(
    "location",
    "GCP_LOCATION",
    help="GCP region (e.g. 'us-central1').",
)
@click.option(
    "--stream-name",
    multiple=True,
    help="The display name(s) of the Datastream stream(s) to manage.",
    required=True,
)
@click.option(
    "--action",
    type=click.Choice(["start", "pause", "resume"]),
    help="Action to perform on the stream(s).",
    default="start",
)
@click.option(
    "--wait-for-completion/--no-wait-for-completion",
    help="Wait for the state change to complete before returning.",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for completion (in minutes). Only used with --wait-for-completion.",
    default=60,
)
def gcp_datastream_trigger(
    service_account_key_file: str,
    project: str,
    location: str,
    stream_name: List[str],
    action: str,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Start, pause, or resume Datastream streams by display name.
    """
    click.echo(f"{action.capitalize()}ing {len(stream_name)} Datastream stream(s)...")

    try:
        results = trigger_datastream(
            service_account_key_file=service_account_key_file,
            project=project,
            location=location,
            stream_names=list(stream_name),
            action=action,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        failed = [r for r in results if "FAILED" in r]
        if failed:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Datastream {action} failed: {str(e)}")
        raise click.Abort()


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "service-account-key-file",
    "GCP_SERVICE_ACCOUNT_KEY_FILE",
    help="Path to your GCP service account JSON key file.",
)
@env_click_option(
    "project",
    "GCP_PROJECT_ID",
    help="Your GCP project ID.",
)
@env_click_option(
    "location",
    "GCP_LOCATION",
    help="GCP region (e.g. 'us-central1').",
)
def gcp_datastream_list(
    service_account_key_file: str,
    project: str,
    location: str,
) -> None:
    """
    List all Datastream streams in the specified project and region.
    """
    click.echo("Listing Datastream streams...")

    list_datastream_streams(
        service_account_key_file=service_account_key_file,
        project=project,
        location=location,
    )
