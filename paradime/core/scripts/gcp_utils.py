from typing import Any


def get_gcp_credentials(service_account_key_file: str) -> Any:
    """Load GCP credentials from service account JSON key file."""
    from google.oauth2 import service_account

    return service_account.Credentials.from_service_account_file(
        service_account_key_file,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


def get_gcp_id_token_credentials(service_account_key_file: str, target_audience: str) -> Any:
    """Load GCP ID token credentials for authenticated HTTP invocations (e.g. Cloud Functions)."""
    from google.oauth2 import service_account

    return service_account.IDTokenCredentials.from_service_account_file(
        service_account_key_file,
        target_audience=target_audience,
    )
