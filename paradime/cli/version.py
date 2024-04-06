import click
from rich import box
from rich.console import Console
from rich.text import Text
from rich.json import JSON
from rich.panel import Panel
from rich.table import Table

from paradime.version import get_sdk_version
from rich_text_output import print_cli_header


@click.command()
def version() -> None:
    """
    Get the version of the Paradime CLI.
    """
    print_cli_header(get_sdk_version())

if __name__ == '__main__':
    version()