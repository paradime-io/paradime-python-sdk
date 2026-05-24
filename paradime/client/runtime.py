import os
import sys
from pathlib import Path

DISABLE_TELEMETRY_ENV_VAR = "PARADIME_DISABLE_TELEMETRY"


def is_telemetry_enabled() -> bool:
    """Return False if the user has opted out via PARADIME_DISABLE_TELEMETRY=true."""
    return os.environ.get(DISABLE_TELEMETRY_ENV_VAR, "").lower() not in (
        "true",
        "1",
        "yes",
    )


def get_python_version() -> str:
    """Return the current Python version as a string, e.g. '3.11.5'."""
    v = sys.version_info
    return f"{v.major}.{v.minor}.{v.micro}"


def detect_runtime() -> str:
    """
    Detect the execution environment by inspecting well-known environment variables.

    Returns a short slug sent as the X-PARADIME-RUNTIME header. Only environment
    variable *presence* is checked — no values are ever read or transmitted.

    Returns 'unknown' if detection fails for any reason.
    """

    try:
        # ── Paradime Bolt (checked first — most specific) ──────────────────────
        if os.environ.get("PARADIME_SCHEDULE_NAME"):
            return "paradime-bolt"

        # ── CI / CD platforms ──────────────────────────────────────────────────
        if os.environ.get("GITHUB_ACTIONS") == "true":
            return "github-actions"
        if os.environ.get("GITLAB_CI") == "true":
            return "gitlab-ci"
        if os.environ.get("CIRCLECI") == "true":
            return "circleci"
        if os.environ.get("JENKINS_URL"):
            return "jenkins"
        if os.environ.get("BITBUCKET_BUILD_NUMBER"):
            return "bitbucket-pipelines"
        if os.environ.get("TF_BUILD"):  # Azure DevOps
            return "azure-devops"

        # ── Workflow orchestrators ─────────────────────────────────────────────
        if os.environ.get("AIRFLOW_CTX_DAG_ID") or os.environ.get("AIRFLOW_HOME"):
            return "airflow"
        if os.environ.get("PREFECT__CONTEXT__FLOW_RUN_ID"):
            return "prefect"
        if os.environ.get("DAGSTER_HOME") or os.environ.get("DAGSTER_PID"):
            return "dagster"

        # ── Serverless / cloud functions ───────────────────────────────────────
        if os.environ.get("AWS_LAMBDA_FUNCTION_NAME"):
            return "aws-lambda"
        if os.environ.get("K_SERVICE") or os.environ.get("FUNCTION_TARGET"):
            return "gcp-cloud"
        if os.environ.get("FUNCTIONS_WORKER_RUNTIME"):
            return "azure-functions"

        # ── Container / cluster ────────────────────────────────────────────────
        if os.environ.get("KUBERNETES_SERVICE_HOST"):
            return "kubernetes"
        try:
            if Path("/.dockerenv").exists():
                return "docker"
        except OSError:
            pass

        return "local"

    except Exception:
        return "unknown"
