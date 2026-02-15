import sys
from typing import List, Optional

import click

from paradime.cli.utils import env_click_option
from paradime.core.scripts.tableau import (
    list_tableau_datasources,
    list_tableau_workbooks,
    trigger_tableau_datasource_refresh,
    trigger_tableau_refresh,
)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "site-name",
    "TABLEAU_SITE_NAME",
    help="The name of the tableau site. Set this only if you are using a site other than the default site.",
    required=False,
    default="",
)
@env_click_option(
    "workbook-name",
    env_var=None,
    multiple=True,
    help="The name or UUID of the workbook(s) you want to refresh",
    required=False,
)
@env_click_option(
    "datasource-name",
    env_var=None,
    multiple=True,
    help="The name or UUID of the data source(s) you want to refresh",
    required=False,
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
@click.option(
    "--wait-for-completion/--no-wait-for-completion",
    default=True,
    help="Wait for the refresh job to complete before returning. Shows progress and final status.",
)
@env_click_option(
    "timeout-minutes",
    "TABLEAU_REFRESH_TIMEOUT_MINUTES",
    type=int,
    default=30,
    help="Maximum time to wait for refresh completion (in minutes). Only used with --wait-for-completion.",
)
def tableau_refresh(
    site_name: str,
    workbook_name: Optional[List[str]],
    datasource_name: Optional[List[str]],
    host: str,
    personal_access_token_secret: str,
    personal_access_token_name: str,
    wait_for_completion: bool,
    timeout_minutes: int,
) -> None:
    """
    Trigger a Tableau refresh for workbooks or data sources.
    """
    if not workbook_name and not datasource_name:
        raise click.UsageError("Must specify either --workbook-name or --datasource-name")

    if workbook_name and datasource_name:
        raise click.UsageError(
            "Cannot specify both --workbook-name and --datasource-name. Choose one."
        )

    if workbook_name:
        click.echo(f"Tableau workbook refresh started on site {site_name}...")
        results = trigger_tableau_refresh(
            host=host,
            personal_access_token_name=personal_access_token_name,
            personal_access_token_secret=personal_access_token_secret,
            site_name=site_name or "",
            workbook_names=workbook_name,
            api_version="3.4",
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any refreshes failed
        failed_refreshes = [
            result for result in results if "FAILED" in result or "CANCELED" in result
        ]
        if failed_refreshes:
            sys.exit(1)

    if datasource_name:
        click.echo(f"Tableau data source refresh started on site {site_name}...")
        results = trigger_tableau_datasource_refresh(
            host=host,
            personal_access_token_name=personal_access_token_name,
            personal_access_token_secret=personal_access_token_secret,
            site_name=site_name or "",
            datasource_names=datasource_name,
            api_version="3.4",
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
        )

        # Check if any refreshes failed
        failed_refreshes = [
            result for result in results if "FAILED" in result or "CANCELED" in result
        ]
        if failed_refreshes:
            sys.exit(1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "site-name",
    "TABLEAU_SITE_NAME",
    help="The name of the tableau site. Set this only if you are using a site other than the default site.",
    required=False,
    default="",
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
def tableau_list_workbooks(
    site_name: str,
    host: str,
    personal_access_token_secret: str,
    personal_access_token_name: str,
) -> None:
    """
    List all Tableau workbooks with their names and UUIDs.
    """
    click.echo(f"Listing Tableau workbooks on site {site_name}...")

    list_tableau_workbooks(
        host=host,
        personal_access_token_name=personal_access_token_name,
        personal_access_token_secret=personal_access_token_secret,
        site_name=site_name or "",
        api_version="3.4",
    )


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "site-name",
    "TABLEAU_SITE_NAME",
    help="The name of the tableau site. Set this only if you are using a site other than the default site.",
    required=False,
    default="",
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
def tableau_list_datasources(
    site_name: str,
    host: str,
    personal_access_token_secret: str,
    personal_access_token_name: str,
) -> None:
    """
    List all Tableau data sources with their names and UUIDs.
    """
    click.echo(f"Listing Tableau data sources on site {site_name}...")

    list_tableau_datasources(
        host=host,
        personal_access_token_name=personal_access_token_name,
        personal_access_token_secret=personal_access_token_secret,
        site_name=site_name or "",
        api_version="3.4",
    )
