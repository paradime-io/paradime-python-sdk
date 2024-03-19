import webbrowser

import click


@click.command()
def editor() -> None:
    """
    Open the editor.
    """
    webbrowser.open('https://app.paradime.io/editor', new=2)


@click.command()
def bolt() -> None:
    """
    Open bolt.
    """
    webbrowser.open('https://app.paradime.io/bolt', new=2)


@click.command()
def lineage() -> None:
    """
    Open the lineage.
    """
    webbrowser.open('https://app.paradime.io/lineage/home', new=2)


@click.command()
def catalog() -> None:
    """
    Open the catalogue.
    """
    webbrowser.open('https://app.paradime.io/catalog/search', new=2)


@click.command()
def workspace() -> None:
    """
    Open the workspace settings.
    """
    webbrowser.open('https://app.paradime.io/account-settings/workspace', new=2)


@click.command()
def connections() -> None:
    """
    Open the workspace connections.
    """
    webbrowser.open('https://app.paradime.io/account-settings/connections', new=2)


@click.group()
def open() -> None:
    """
    Open Paradime webpages from the cli.
    """
    pass


# bolt
open.add_command(editor)
open.add_command(bolt)
open.add_command(lineage)
open.add_command(catalog)
open.add_command(workspace)
open.add_command(connections)