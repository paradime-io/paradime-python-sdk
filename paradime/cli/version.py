import click

from paradime.cli.rich_text_output import print_cli_header
from paradime.version import get_sdk_version


@click.command()
def version() -> None:
    """
    Get the version of the Paradime CLI.
    """
    print_version()


def print_version() -> None:
    print_cli_header(get_sdk_version())


if __name__ == "__main__":
    version()
