import sys
from typing import List, Optional

import click

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
    "--dag-id",
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
    "--wait-for-completion",
    is_flag=True,
    help="Wait for DAG runs to complete before returning",
    default=True,
)
@click.option(
    "--timeout-minutes",
    type=int,
    help="Maximum time to wait for DAG completion (in minutes). Only used with --wait-for-completion.",
    default=1440,
)
@click.option(
    "--show-logs",
    is_flag=True,
    help="Display task logs during execution. Only used with --wait-for-completion.",
    default=True,
)
def airflow_trigger(
    base_url: str,
    username: Optional[str],
    password: Optional[str],
    bearer_token: Optional[str],
    use_gcp_auth: bool,
    dag_id: List[str],
    dag_run_conf: Optional[str],
    wait_for_completion: bool,
    timeout_minutes: int,
    show_logs: bool,
) -> None:
    """
    Trigger one or more Airflow DAG runs.
    """
    click.echo(f"Starting {len(dag_id)} Airflow DAG run(s)...")

    # Parse dag_run_conf if provided
    import json

    parsed_dag_run_conf = None
    if dag_run_conf:
        try:
            parsed_dag_run_conf = json.loads(dag_run_conf)
        except json.JSONDecodeError as e:
            click.echo(f"❌ Invalid JSON in --dag-run-conf: {str(e)}")
            sys.exit(1)

    # Validate authentication parameters
    if not use_gcp_auth and not bearer_token:
        if not username or not password:
            click.echo(
                "❌ Error: username and password are required for basic authentication. "
                "Use --use-gcp-auth for GCP Cloud Composer or provide --bearer-token for token-based auth."
            )
            sys.exit(1)

    try:
        results = trigger_airflow_dags(
            base_url=base_url,
            username=username,
            password=password,
            dag_ids=list(dag_id),
            dag_run_conf=parsed_dag_run_conf,
            wait_for_completion=wait_for_completion,
            timeout_minutes=timeout_minutes,
            show_logs=show_logs,
            use_gcp_auth=use_gcp_auth,
            bearer_token=bearer_token,
        )

        # Check if any DAG runs failed
        failed_dags = [result for result in results if "FAILED" in result]
        if failed_dags:
            sys.exit(1)

    except Exception as e:
        click.echo(f"❌ Airflow DAG trigger failed: {str(e)}")
        raise click.Abort()


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
def airflow_list_dags(
    base_url: str,
    username: Optional[str],
    password: Optional[str],
    bearer_token: Optional[str],
    use_gcp_auth: bool,
    only_active: bool,
) -> None:
    """
    List all available Airflow DAGs with their status.
    """
    if only_active:
        click.echo("Listing active Airflow DAGs...")
    else:
        click.echo("Listing all Airflow DAGs...")

    # Validate authentication parameters
    if not use_gcp_auth and not bearer_token:
        if not username or not password:
            click.echo(
                "❌ Error: username and password are required for basic authentication. "
                "Use --use-gcp-auth for GCP Cloud Composer or provide --bearer-token for token-based auth."
            )
            sys.exit(1)

    try:
        list_airflow_dags(
            base_url=base_url,
            username=username,
            password=password,
            only_active=only_active,
            use_gcp_auth=use_gcp_auth,
            bearer_token=bearer_token,
        )
    except Exception as e:
        click.echo(f"❌ Failed to list Airflow DAGs: {str(e)}")
        raise click.Abort()
