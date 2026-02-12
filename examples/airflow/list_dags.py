"""
Example: List Airflow DAGs

This example demonstrates how to list all available Airflow DAGs.

Prerequisites:
For standard Airflow (MWAA, Astronomer, self-hosted):
- Set environment variables:
  - AIRFLOW_BASE_URL: Your Airflow instance URL
  - AIRFLOW_USERNAME: Username or API key
  - AIRFLOW_PASSWORD: Password or API secret

For GCP Cloud Composer:
- Install google-auth: pip install google-auth
- Set environment variable:
  - AIRFLOW_BASE_URL: Your Cloud Composer Airflow URL
- Authenticate with: gcloud auth application-default login
  OR set GOOGLE_APPLICATION_CREDENTIALS to service account key path

Usage via CLI:
    # Standard Airflow - List all active DAGs
    paradime run airflow-list-dags

    # GCP Cloud Composer - List all active DAGs
    paradime run airflow-list-dags --use-gcp-auth

    # List all DAGs (including paused)
    paradime run airflow-list-dags --no-only-active

Usage via Python:
    # Standard Airflow
    from paradime.core.scripts.airflow import list_airflow_dags

    list_airflow_dags(
        base_url="https://your-airflow-instance.com",
        username="your_username",
        password="your_password",
        only_active=True,
    )

    # GCP Cloud Composer
    list_airflow_dags(
        base_url="https://your-composer-airflow-url.appspot.com",
        use_gcp_auth=True,
        only_active=True,
    )
"""

import os

from paradime.core.scripts.airflow import list_airflow_dags

if __name__ == "__main__":
    # Get base URL from environment
    base_url = os.getenv("AIRFLOW_BASE_URL")
    if not base_url:
        raise ValueError("Missing required environment variable: AIRFLOW_BASE_URL")

    # Check if we should use GCP authentication
    use_gcp_auth = os.getenv("USE_GCP_AUTH", "false").lower() == "true"

    if use_gcp_auth:
        # GCP Cloud Composer authentication
        print("Using GCP Cloud Composer authentication...")
        list_airflow_dags(
            base_url=base_url,
            use_gcp_auth=True,
            only_active=True,
        )
    else:
        # Standard Airflow authentication (MWAA, Astronomer, self-hosted)
        username = os.getenv("AIRFLOW_USERNAME")
        password = os.getenv("AIRFLOW_PASSWORD")

        if not all([username, password]):
            raise ValueError(
                "Missing required environment variables: AIRFLOW_USERNAME, AIRFLOW_PASSWORD\n"
                "For GCP Cloud Composer, set USE_GCP_AUTH=true instead"
            )

        print("Using basic authentication...")
        list_airflow_dags(
            base_url=base_url,
            username=username,
            password=password,
            only_active=True,
        )
