"""
Example: List Airflow DAGs

This example demonstrates how to list all available Airflow DAGs.

Prerequisites:
- Set environment variables:
  - AIRFLOW_BASE_URL: Your Airflow instance URL
  - AIRFLOW_USERNAME: Username or API key
  - AIRFLOW_PASSWORD: Password or API secret

Usage via CLI:
    # List all active DAGs
    paradime run airflow-list-dags

    # List all DAGs (including paused)
    paradime run airflow-list-dags --no-only-active

Usage via Python:
    from paradime.core.scripts.airflow import list_airflow_dags

    list_airflow_dags(
        base_url="https://your-airflow-instance.com",
        username="your_username",
        password="your_password",
        only_active=True,
    )
"""

import os

from paradime.core.scripts.airflow import list_airflow_dags

if __name__ == "__main__":
    # Get credentials from environment variables
    base_url = os.getenv("AIRFLOW_BASE_URL")
    username = os.getenv("AIRFLOW_USERNAME")
    password = os.getenv("AIRFLOW_PASSWORD")

    if not all([base_url, username, password]):
        raise ValueError(
            "Missing required environment variables: AIRFLOW_BASE_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD"
        )

    # List DAGs
    list_airflow_dags(
        base_url=base_url,
        username=username,
        password=password,
        only_active=True,
    )
