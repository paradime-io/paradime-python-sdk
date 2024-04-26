import click

from paradime.cli.version import print_version
from paradime.client.paradime_cli_client import get_credentials_path


@click.command()
def login() -> None:
    """
    Set the API credentials for the Paradime CLI.
    """
    print_version()
    credentials_path = get_credentials_path()
    if credentials_path.exists():
        click.confirm(
            "Do you want to overwrite existing credentials?",
            abort=True,
        )

    click.echo("Generate API credentials here: https://app.paradime.io/account-settings/workspace")

    api_key = click.prompt("Enter API Key")
    api_secret = click.prompt("Enter API Secret", hide_input=True)
    api_endpoint = click.prompt("Enter API Endpoint")

    # write to env file
    credentials_path.parent.mkdir(parents=True, exist_ok=True)
    credentials_path.write_text(
        f"API_ENDPOINT={api_endpoint}\nAPI_KEY={api_key}\nAPI_SECRET={api_secret}\n"
    )
    click.echo(f"✨ Credentials written to '{credentials_path}'!")
