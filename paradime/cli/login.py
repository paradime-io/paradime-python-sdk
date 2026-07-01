import click

from paradime.cli import console
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

    auth_mode = click.prompt(
        "Authentication mode",
        type=click.Choice(["workspace", "company"], case_sensitive=False),
        default="company",
    )

    if auth_mode == "company":
        console.url(
            "Generate API credentials",
            "https://app.paradime.io/settings/account",
        )

        api_token = click.prompt("Enter API Token (prdm_cmp_...)", hide_input=True)
        api_endpoint = click.prompt("Enter API Endpoint")
        workspace_uid = click.prompt(
            "Enter Workspace UID (optional, press Enter to skip)", default="", show_default=False
        )

        credentials_path.parent.mkdir(parents=True, exist_ok=True)
        lines = f"PARADIME_API_ENDPOINT={api_endpoint}\nPARADIME_API_TOKEN={api_token}\n"
        if workspace_uid:
            lines += f"PARADIME_WORKSPACE_UID={workspace_uid}\n"
        credentials_path.write_text(lines)
    else:
        console.url(
            "Generate API credentials",
            "https://app.paradime.io/settings/current-workspace",
        )

        api_key = click.prompt("Enter API Key")
        api_secret = click.prompt("Enter API Secret", hide_input=True)
        api_endpoint = click.prompt("Enter API Endpoint")

        credentials_path.parent.mkdir(parents=True, exist_ok=True)
        credentials_path.write_text(
            f"API_ENDPOINT={api_endpoint}\nAPI_KEY={api_key}\nAPI_SECRET={api_secret}\n"
        )

    console.success(f"Credentials written to '{credentials_path}'.")
