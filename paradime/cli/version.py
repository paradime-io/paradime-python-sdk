import click

from paradime.version import get_sdk_version


@click.command()
def version() -> None:
    """
    Get the version of the Paradime CLI.
    """
    click.echo(get_sdk_version())
