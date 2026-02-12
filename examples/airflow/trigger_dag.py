"""
Example: Trigger Airflow DAG runs

This example demonstrates how to trigger Airflow DAG runs using the paradime SDK.
It supports:
- AWS MWAA (Managed Workflows for Apache Airflow)
- Astronomer
- Self-hosted Airflow instances

Prerequisites:
- Set environment variables:
  - AIRFLOW_BASE_URL: Your Airflow instance URL
  - AIRFLOW_USERNAME: Username or API key
  - AIRFLOW_PASSWORD: Password or API secret

Usage via CLI:
    # Trigger a single DAG
    paradime run airflow-trigger --dag-id my_dag

    # Trigger multiple DAGs
    paradime run airflow-trigger --dag-id dag1 --dag-id dag2

    # Trigger without waiting for completion
    paradime run airflow-trigger --dag-id my_dag --no-wait-for-completion

    # Trigger without showing logs
    paradime run airflow-trigger --dag-id my_dag --no-show-logs

Usage via Python:
    from paradime.core.scripts.airflow import trigger_airflow_dags

    results = trigger_airflow_dags(
        base_url="https://your-airflow-instance.com",
        username="your_username",
        password="your_password",
        dag_ids=["dag1", "dag2"],
        wait_for_completion=True,
        timeout_minutes=60,
        show_logs=True,
    )
"""

import os

from paradime.core.scripts.airflow import trigger_airflow_dags

if __name__ == "__main__":
    # Get credentials from environment variables
    base_url = os.getenv("AIRFLOW_BASE_URL")
    username = os.getenv("AIRFLOW_USERNAME")
    password = os.getenv("AIRFLOW_PASSWORD")

    if not all([base_url, username, password]):
        raise ValueError(
            "Missing required environment variables: AIRFLOW_BASE_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD"
        )

    # Trigger DAG runs
    results = trigger_airflow_dags(
        base_url=base_url,
        username=username,
        password=password,
        dag_ids=["example_dag_1", "example_dag_2"],
        wait_for_completion=True,
        timeout_minutes=60,
        show_logs=True,
    )

    print("\nResults:")
    for result in results:
        print(f"  - {result}")
