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

    console.url(
        "Generate API credentials",
        "https://app.paradime.io/settings/current-workspace",
    )

    api_secret = click.prompt(
        "Enter API Secret (or API Token, e.g. one starting with 'prdm_wsp_' or 'prdm_cmp_')",
        hide_input=True,
    )

    credentials_lines = []

    if api_secret.startswith("prdm_wsp_") or api_secret.startswith("prdm_cmp_"):
        workspace_uid = ""
        if api_secret.startswith("prdm_cmp_"):
            workspace_uid = click.prompt("Enter Workspace UID")
            while not workspace_uid:
                console.error(
                    "Workspace UID is required when using a company-level ('prdm_cmp_') API token."
                )
                workspace_uid = click.prompt("Enter Workspace UID")

        credentials_lines.append(f"API_SECRET={api_secret}")
        if workspace_uid:
            credentials_lines.append(f"WORKSPACE_UID={workspace_uid}")
    else:
        api_key = click.prompt("Enter API Key")
        credentials_lines.append(f"API_KEY={api_key}")
        credentials_lines.append(f"API_SECRET={api_secret}")

    api_endpoint = click.prompt("Enter API Endpoint")
    credentials_lines.insert(0, f"API_ENDPOINT={api_endpoint}")

    # write to env file
    credentials_path.parent.mkdir(parents=True, exist_ok=True)
    credentials_path.write_text("\n".join(credentials_lines) + "\n")
    console.success(f"Credentials written to '{credentials_path}'.")
