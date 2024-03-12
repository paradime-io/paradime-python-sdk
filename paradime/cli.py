
import click
from dotenv import load_dotenv

from paradime.apis.bolt.cli import run, verify
from paradime.client.paradime_cli_client import get_credentials_path
from paradime.version import get_sdk_version


@click.group()
def cli() -> None:
    """
    Work seamlessly with Paradime from the command line.
    """
    credentials_path = get_credentials_path()
    if credentials_path.exists():
        load_dotenv(dotenv_path=credentials_path)


@click.command()
def version() -> None:
    """
    Get the version of the Paradime CLI.
    """
    click.echo(get_sdk_version())


@click.command()
def login() -> None:
    """
    Set the API credentials for the Paradime CLI.
    """
    credentials_path = get_credentials_path()
    if credentials_path.exists():
        click.confirm(
            "Do you want to overwrite existing credentials?",
            abort=True,
        )

    click.echo("Generate new credentials here: https://app.paradime.io/account-settings/workspace")

    api_key = click.prompt("Please enter the API Key")
    api_secret = click.prompt("Please enter the API Secret", hide_input=True)
    api_endpoint = click.prompt("Please enter the API Endpoint")

    # write to env file
    credentials_path.parent.mkdir(parents=True, exist_ok=True)
    credentials_path.write_text(
        f"API_ENDPOINT={api_endpoint}\nAPI_KEY={api_key}\nAPI_SECRET={api_secret}\n"
    )
    click.echo(f"Writen credentials to '{credentials_path}'!")


@click.group()
def bolt() -> None:
    """
    Work with Paradime Bolt from the CLI.
    """
    pass


# bolt
bolt.add_command(run)
bolt.add_command(verify)
cli.add_command(bolt)

# other
cli.add_command(version)
cli.add_command(login)
