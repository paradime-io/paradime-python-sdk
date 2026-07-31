"""Shared constants for the unit test suite."""

API_ENDPOINT = "https://api.paradime.test/graphql"

WORKSPACE_TOKEN = "prdm_wsp_abc123"
COMPANY_TOKEN = "prdm_cmp_abc123"
LEGACY_SECRET = "legacy-secret"
LEGACY_KEY = "legacy-key"

# Environment variables the runtime detector inspects. Cleared before every test so
# telemetry headers are deterministic regardless of where the suite runs.
RUNTIME_ENV_VARS = (
    "PARADIME_DISABLE_TELEMETRY",
    "PARADIME_SCHEDULE_NAME",
    "GITHUB_ACTIONS",
    "GITLAB_CI",
    "CIRCLECI",
    "JENKINS_URL",
    "BITBUCKET_BUILD_NUMBER",
    "TF_BUILD",
    "AIRFLOW_CTX_DAG_ID",
    "AIRFLOW_HOME",
    "PREFECT__CONTEXT__FLOW_RUN_ID",
    "DAGSTER_HOME",
    "DAGSTER_PID",
    "AWS_LAMBDA_FUNCTION_NAME",
    "K_SERVICE",
    "FUNCTION_TARGET",
    "FUNCTIONS_WORKER_RUNTIME",
    "KUBERNETES_SERVICE_HOST",
)
