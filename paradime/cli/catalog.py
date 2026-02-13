import click

from paradime.cli.rich_text_output import print_success
from paradime.client.paradime_cli_client import get_cli_client_or_exit


@click.group()
def catalog() -> None:
    """
    Work with Paradime Catalog from the CLI.
    """
    pass


@click.command()
def refresh() -> None:
    """
    Trigger a catalog refresh.
    """

    client = get_cli_client_or_exit()
    client.catalog.refresh()

    print_success(
        "The catalog refresh has been triggered successfully. It may take a few minutes to complete.",
        is_json=False,
    )


catalog.add_command(refresh)
