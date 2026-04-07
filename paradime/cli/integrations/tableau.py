from __future__ import annotations

import sys
from typing import List, Optional

import click

from paradime.cli import console
from paradime.cli.utils import COMMA_LIST, deprecated_alias_option, env_click_option, resolve_deprecated_option
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
    "workbook-names",
    env_var=None,
    type=COMMA_LIST,
    help="Comma-separated workbook name(s) or UUID(s) to refresh",
    required=False,
)
@env_click_option(
    "datasource-names",
    env_var=None,
    type=COMMA_LIST,
    help="Comma-separated data source name(s) or UUID(s) to refresh",
    required=False,
)
@deprecated_alias_option("workbook-name", "workbook-names", type=COMMA_LIST, default=None)
@deprecated_alias_option("datasource-name", "datasource-names", type=COMMA_LIST, default=None)
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
    "--wait/--no-wait",
    default=True,
    help="Wait for the refresh job to complete before returning. Shows progress and final status.",
)
@env_click_option(
    "timeout",
    "TABLEAU_REFRESH_TIMEOUT_MINUTES",
    type=int,
    default=30,
    help="Maximum time to wait in minutes.",
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def tableau_refresh(
    site_name: str,
    workbook_names: Optional[List[str]],
    datasource_names: Optional[List[str]],
    workbook_name: Optional[List[str]],
    datasource_name: Optional[List[str]],
    host: str,
    personal_access_token_secret: str,
    personal_access_token_name: str,
    wait: bool,
    timeout: int,
    json_output: bool,
) -> None:
    """
    Trigger a Tableau refresh for workbooks or data sources.
    """
    workbook_names = resolve_deprecated_option(workbook_names, workbook_name, "workbook-names", "workbook-name")
    datasource_names = resolve_deprecated_option(datasource_names, datasource_name, "datasource-names", "datasource-name")

    if not workbook_names and not datasource_names:
        raise click.UsageError("Must specify either --workbook-name or --datasource-name")

    if workbook_names and datasource_names:
        raise click.UsageError(
            "Cannot specify both --workbook-name and --datasource-name. Choose one."
        )

    if workbook_names:
        if not json_output:
            console.header(f"Tableau — Refresh Workbooks (site: {site_name or 'default'})")
        try:
            results = trigger_tableau_refresh(
                host=host,
                personal_access_token_name=personal_access_token_name,
                personal_access_token_secret=personal_access_token_secret,
                site_name=site_name or "",
                workbook_names=workbook_names,
                api_version="3.4",
                wait_for_completion=wait,
                timeout_minutes=timeout,
            )
        except Exception as e:
            if json_output:
                console.json_out({"error": str(e), "success": False})
                sys.exit(1)
            raise

        if json_output:
            failed = [r for r in results if "FAILED" in r or "CANCELED" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any refreshes failed
        failed_refreshes = [
            result for result in results if "FAILED" in result or "CANCELED" in result
        ]
        if failed_refreshes:
            console.error(f"{len(failed_refreshes)} workbook refresh(es) failed.")
            sys.exit(1)

    if datasource_names:
        if not json_output:
            console.header(f"Tableau — Refresh Data Sources (site: {site_name or 'default'})")
        try:
            results = trigger_tableau_datasource_refresh(
                host=host,
                personal_access_token_name=personal_access_token_name,
                personal_access_token_secret=personal_access_token_secret,
                site_name=site_name or "",
                datasource_names=datasource_names,
                api_version="3.4",
                wait_for_completion=wait,
                timeout_minutes=timeout,
            )
        except Exception as e:
            if json_output:
                console.json_out({"error": str(e), "success": False})
                sys.exit(1)
            raise

        if json_output:
            failed = [r for r in results if "FAILED" in r or "CANCELED" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any refreshes failed
        failed_refreshes = [
            result for result in results if "FAILED" in result or "CANCELED" in result
        ]
        if failed_refreshes:
            console.error(f"{len(failed_refreshes)} data source refresh(es) failed.")
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
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def tableau_list_workbooks(
    site_name: str,
    host: str,
    personal_access_token_secret: str,
    personal_access_token_name: str,
    json_output: bool,
) -> None:
    """
    List all Tableau workbooks with their names and UUIDs.
    """
    if not json_output:
        console.info(f"Listing Tableau workbooks on site {site_name or 'default'}…")

    result = list_tableau_workbooks(
        host=host,
        personal_access_token_name=personal_access_token_name,
        personal_access_token_secret=personal_access_token_secret,
        site_name=site_name or "",
        api_version="3.4",
        json_output=json_output,
    )
    if json_output and result is not None:
        console.json_out(result)


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
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def tableau_list_datasources(
    site_name: str,
    host: str,
    personal_access_token_secret: str,
    personal_access_token_name: str,
    json_output: bool,
) -> None:
    """
    List all Tableau data sources with their names and UUIDs.
    """
    if not json_output:
        console.info(f"Listing Tableau data sources on site {site_name or 'default'}…")

    result = list_tableau_datasources(
        host=host,
        personal_access_token_name=personal_access_token_name,
        personal_access_token_secret=personal_access_token_secret,
        site_name=site_name or "",
        api_version="3.4",
        json_output=json_output,
    )
    if json_output and result is not None:
        console.json_out(result)
