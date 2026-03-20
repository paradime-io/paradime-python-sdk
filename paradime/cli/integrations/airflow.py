from __future__ import annotations

import sys
from typing import List, Optional

import click

from paradime.cli import console
from paradime.cli.utils import env_click_option
from paradime.core.scripts.airflow import list_airflow_dags, trigger_airflow_dags


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "base-url",
    "AIRFLOW_BASE_URL",
    help="Your Airflow base URL (e.g., https://your-airflow.com, MWAA webserver URL, or Cloud Composer URL).",
)
@env_click_option(
    "username",
    "AIRFLOW_USERNAME",
    help="Your Airflow username or API key. Not required for GCP Cloud Composer with --use-gcp-auth.",
    required=False,
)
@env_click_option(
    "password",
    "AIRFLOW_PASSWORD",
    help="Your Airflow password or API secret. Not required for GCP Cloud Composer with --use-gcp-auth.",
    required=False,
)
@env_click_option(
    "bearer-token",
    "AIRFLOW_BEARER_TOKEN",
    help="Optional bearer token for token-based authentication. Not required for basic auth or GCP.",
    required=False,
)
@click.option(
    "--use-gcp-auth",
    is_flag=True,
    help="Use GCP Cloud Composer authentication (Application Default Credentials)",
    default=False,
)
@click.option(
    "--dag-ids",
    multiple=True,
    help="The ID(s) of the DAG(s) you want to trigger",
    required=True,
)
@click.option(
    "--dag-run-conf",
    help="Optional JSON configuration to pass to DAG runs (as a string)",
    required=False,
)
@click.option(
    "--logical-date",
    help="Optional logical date for DAG runs in ISO format (e.g., '2024-01-01T00:00:00Z'). Defaults to current timestamp.",
    required=False,
)
@click.option(
    "--wait/--no-wait",
    help="Wait for DAG runs to complete before returning",
    default=True,
)
@click.option(
    "--timeout",
    type=int,
    help="Maximum time to wait in minutes.",
    default=1440,
)
@click.option(
    "--show-logs/--no-show-logs",
    help="Display task logs during execution. Only used with --wait-for-completion.",
    default=True,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def airflow_trigger(
    base_url: str,
    username: Optional[str],
    password: Optional[str],
    bearer_token: Optional[str],
    use_gcp_auth: bool,
    dag_ids: List[str],
    dag_run_conf: Optional[str],
    logical_date: Optional[str],
    wait: bool,
    timeout: int,
    show_logs: bool,
    json_output: bool,
) -> None:
    """
    Trigger one or more Airflow DAG runs.
    """
    if not json_output:
        console.header("Airflow — Trigger DAG Runs")

    # Parse dag_run_conf if provided
    import json

    parsed_dag_run_conf = None
    if dag_run_conf:
        try:
            parsed_dag_run_conf = json.loads(dag_run_conf)
        except json.JSONDecodeError as e:
            console.error(f"Invalid JSON in --dag-run-conf: {e}", exit_code=1)

    # Validate authentication parameters
    if not use_gcp_auth and not bearer_token:
        if not username or not password:
            console.error(
                "username and password are required for basic authentication. "
                "Use --use-gcp-auth for GCP Cloud Composer or provide --bearer-token for token-based auth.",
                exit_code=1,
            )

    try:
        results = trigger_airflow_dags(
            base_url=base_url,
            username=username,
            password=password,
            dag_ids=list(dag_ids),
            dag_run_conf=parsed_dag_run_conf,
            logical_date=logical_date,
            wait_for_completion=wait,
            timeout_minutes=timeout,
            show_logs=show_logs,
            use_gcp_auth=use_gcp_auth,
            bearer_token=bearer_token,
        )

        if json_output:
            failed = [r for r in results if "FAILED" in r]
            console.json_out(
                {"results": results, "failed_count": len(failed), "success": len(failed) == 0}
            )
            if failed:
                sys.exit(1)
            return

        # Check if any DAG runs failed
        failed_dags = [result for result in results if "FAILED" in result]
        if failed_dags:
            console.error(f"{len(failed_dags)} DAG run(s) failed.")
            sys.exit(1)

    except Exception as e:
        if json_output:
            console.json_out({"error": str(e), "success": False})
            sys.exit(1)
        console.error(f"Airflow DAG trigger failed: {e}", exit_code=1)


@click.command(context_settings=dict(max_content_width=160))
@env_click_option(
    "base-url",
    "AIRFLOW_BASE_URL",
    help="Your Airflow base URL (e.g., https://your-airflow.com, MWAA webserver URL, or Cloud Composer URL).",
)
@env_click_option(
    "username",
    "AIRFLOW_USERNAME",
    help="Your Airflow username or API key. Not required for GCP Cloud Composer with --use-gcp-auth.",
    required=False,
)
@env_click_option(
    "password",
    "AIRFLOW_PASSWORD",
    help="Your Airflow password or API secret. Not required for GCP Cloud Composer with --use-gcp-auth.",
    required=False,
)
@env_click_option(
    "bearer-token",
    "AIRFLOW_BEARER_TOKEN",
    help="Optional bearer token for token-based authentication. Not required for basic auth or GCP.",
    required=False,
)
@click.option(
    "--use-gcp-auth",
    is_flag=True,
    help="Use GCP Cloud Composer authentication (Application Default Credentials)",
    default=False,
)
@click.option(
    "--only-active",
    is_flag=True,
    help="Only show active (non-paused) DAGs",
    default=True,
)
@click.option("--json", "json_output", is_flag=True, help="Output results as JSON.", default=False)
def airflow_list_dags(
    base_url: str,
    username: Optional[str],
    password: Optional[str],
    bearer_token: Optional[str],
    use_gcp_auth: bool,
    only_active: bool,
    json_output: bool,
) -> None:
    """
    List all available Airflow DAGs with their status.
    """
    if not json_output:
        if only_active:
            console.info("Listing active Airflow DAGs…")
        else:
            console.info("Listing all Airflow DAGs…")

    # Validate authentication parameters
    if not use_gcp_auth and not bearer_token:
        if not username or not password:
            console.error(
                "username and password are required for basic authentication. "
                "Use --use-gcp-auth for GCP Cloud Composer or provide --bearer-token for token-based auth.",
                exit_code=1,
            )

    try:
        result = list_airflow_dags(
            base_url=base_url,
            username=username,
            password=password,
            only_active=only_active,
            use_gcp_auth=use_gcp_auth,
            bearer_token=bearer_token,
            json_output=json_output,
        )
        if json_output and result is not None:
            console.json_out(result)
    except Exception as e:
        console.error(f"Failed to list Airflow DAGs: {e}", exit_code=1)
