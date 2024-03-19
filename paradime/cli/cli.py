import click
from dotenv import load_dotenv

from paradime.cli.bolt import bolt
from paradime.cli.login import login
from paradime.cli.version import version
from paradime.client.paradime_cli_client import get_credentials_path


@click.group()
def cli() -> None:
    """
    Work seamlessly with Paradime from the command line.
    """
    credentials_path = get_credentials_path()
    if credentials_path.exists():
        load_dotenv(dotenv_path=credentials_path)


cli.add_command(bolt)
cli.add_command(version)
cli.add_command(login)
