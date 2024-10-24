from typing import Final

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.tableau import trigger_tableau_refresh

help_string: Final = (
    "\nTo set environment variables please go to https://app.paradime.io/account-settings/workspace"
)


@click.group()
def run() -> None:
    """
    Run predefined code runs to automate your workflows.
    """
    pass


@click.command(context_settings=dict(max_content_width=120))
@env_click_option(
    "site-name",
    "TABLEAU_SITE_NAME",
)
@env_click_option(
    "workbook-name",
    "TABLEAU_WORKBOOK_NAME",
)
@env_click_option(
    "host",
    "TABLEAU_HOST",
    help="The base url of your tableau server (e.g. https://prod-uk-a.online.tableau.com/)",
)
@env_click_option(
    "personal-access-token-secret",
    "TABLEAU_PERSONAL_ACCESS_TOKEN_SECRET",
    help="You can create a personal access token in your tableau account settings: https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm",
)
@env_click_option(
    "personal-access-token-name",
    "TABLEAU_PERSONAL_ACCESS_TOKEN_NAME",
    help="You can create a personal access token in your tableau account settings: https://help.tableau.com/current/server/en-us/security_personal_access_tokens.htm",
)
def tableau_refresh(
    site_name: str,
    workbook_name: str,
    host: str,
    personal_access_token_secret: str,
    personal_access_token_name: str,
) -> None:
    """
    Trigger a Tableau refresh for a specific workbook.
    """
    click.echo(f"Tableau refresh started for workbook {workbook_name} on site {site_name}...")

    response_txt = trigger_tableau_refresh(
        host=host,
        personal_access_token_name=personal_access_token_name,
        personal_access_token_secret=personal_access_token_secret,
        site_name=site_name,
        workbook_name=workbook_name,
        api_version="3.4",
    )
    click.echo(response_txt)


run.add_command(tableau_refresh)
