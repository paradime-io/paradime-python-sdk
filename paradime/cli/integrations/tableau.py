from __future__ import annotations

import sys
from typing import List, Optional, Tuple

import click

from paradime.cli import console
from paradime.cli.utils import (
    COMMA_LIST,
    deprecated_alias_option,
    env_click_option,
    resolve_deprecated_option,
)
from paradime.core.scripts.tableau import (
    DatasourceIdentifier,
    TableauPathName,
    WorkbookIdentifier,
    list_tableau_datasources,
    list_tableau_workbooks,
    trigger_tableau_datasource_refresh,
    trigger_tableau_refresh,
)


def _parse_path_value(raw: str, flag: str) -> TableauPathName:
    """Parse a ``<project_path>::<name>`` value into a TableauPathName.

    Why ``::``? Tableau workbook and data source names may contain commas and
    slashes, so a single-character separator collides with valid input. The
    double-colon is a clean delimiter that doesn't appear in Tableau project
    or workbook names.
    """
    if "::" not in raw:
        raise click.UsageError(
            f"{flag} value must be of the form '<project_path>::<name>', got: {raw!r}"
        )
    path, _, name = raw.partition("::")
    name = name.strip()
    if not name:
        raise click.UsageError(f"{flag} value is missing a workbook/datasource name: {raw!r}")
    return TableauPathName(project_path=path, name=name)


def _parse_path_values(raw_values: Tuple[str, ...], flag: str) -> List[TableauPathName]:
    return [_parse_path_value(v, flag) for v in raw_values]


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
@click.option(
    "--workbook-paths",
    "workbook_paths",
    multiple=True,
    required=False,
    help=(
        "Fully-qualified workbook in the form '<project_path>::<workbook_name>' "
        "(e.g. 'Explore/Samples/TestNested/Nested2::World Indicators'). Use this "
        "to disambiguate workbooks that share a name across different projects. "
        "Pass the flag once per workbook."
    ),
)
@click.option(
    "--datasource-paths",
    "datasource_paths",
    multiple=True,
    required=False,
    help=(
        "Fully-qualified data source in the form '<project_path>::<datasource_name>'. "
        "Use this to disambiguate data sources that share a name across different "
        "projects. Pass the flag once per data source."
    ),
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
    envvar="TABLEAU_REFRESH_WAIT",
    help="Wait for the refresh job to complete before returning. Shows progress and final status.\n\n [env: TABLEAU_REFRESH_WAIT]",
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
    workbook_paths: Tuple[str, ...],
    datasource_paths: Tuple[str, ...],
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
    workbook_names = resolve_deprecated_option(
        workbook_names, workbook_name, "workbook-names", "workbook-name"
    )
    datasource_names = resolve_deprecated_option(
        datasource_names, datasource_name, "datasource-names", "datasource-name"
    )

    parsed_workbook_paths = _parse_path_values(workbook_paths or (), "--workbook-paths")
    parsed_datasource_paths = _parse_path_values(datasource_paths or (), "--datasource-paths")

    workbook_identifiers: List[WorkbookIdentifier] = [
        *(workbook_names or []),
        *parsed_workbook_paths,
    ]
    datasource_identifiers: List[DatasourceIdentifier] = [
        *(datasource_names or []),
        *parsed_datasource_paths,
    ]

    if not workbook_identifiers and not datasource_identifiers:
        raise click.UsageError(
            "Must specify either --workbook-names / --workbook-paths "
            "or --datasource-names / --datasource-paths"
        )

    if workbook_identifiers and datasource_identifiers:
        raise click.UsageError(
            "Cannot mix workbook and data source flags in a single invocation. "
            "Run two separate commands."
        )

    if workbook_identifiers:
        if not json_output:
            console.header(f"Tableau — Refresh Workbooks (site: {site_name or 'default'})")
        try:
            results = trigger_tableau_refresh(
                host=host,
                personal_access_token_name=personal_access_token_name,
                personal_access_token_secret=personal_access_token_secret,
                site_name=site_name or "",
                workbook_names=workbook_identifiers,
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

    if datasource_identifiers:
        if not json_output:
            console.header(f"Tableau — Refresh Data Sources (site: {site_name or 'default'})")
        try:
            results = trigger_tableau_datasource_refresh(
                host=host,
                personal_access_token_name=personal_access_token_name,
                personal_access_token_secret=personal_access_token_secret,
                site_name=site_name or "",
                datasource_names=datasource_identifiers,
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
