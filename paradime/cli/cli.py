import os

import click
from dotenv import load_dotenv

from paradime.cli import console
from paradime.cli.bolt import bolt
from paradime.cli.catalog import catalog
from paradime.cli.login import login
from paradime.cli.run import run
from paradime.cli.version import version
from paradime.client.paradime_cli_client import get_credentials_path
from paradime.version import get_sdk_version


@click.group(invoke_without_command=True)
@click.pass_context
def cli(ctx: click.Context) -> None:
    """
    Work seamlessly with Paradime from the command line.
    """
    credentials_path = get_credentials_path()
    if credentials_path.exists():
        load_dotenv(dotenv_path=credentials_path)

    _show_welcome()


def _show_welcome() -> None:
    sdk_version = get_sdk_version()
    workspace_endpoint = os.environ.get("PARADIME_API_ENDPOINT", os.environ.get("API_ENDPOINT"))
    console.welcome_panel(version=sdk_version, workspace_endpoint=workspace_endpoint)


cli.add_command(bolt)
cli.add_command(catalog)
cli.add_command(version)
cli.add_command(login)
cli.add_command(run)
