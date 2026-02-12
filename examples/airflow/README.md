# Airflow DAG Trigger Examples

This directory contains examples for triggering Airflow DAG runs using the Paradime SDK.

## Supported Platforms

- **AWS MWAA** (Managed Workflows for Apache Airflow)
- **Astronomer** (Cloud or Self-hosted)
- **Self-hosted Airflow** instances

## Prerequisites

Set the following environment variables:

```bash
export AIRFLOW_BASE_URL="https://your-airflow-instance.com"
export AIRFLOW_USERNAME="your_username"
export AIRFLOW_PASSWORD="your_password"
```

### Platform-Specific Configuration

#### AWS MWAA

For AWS MWAA, you need to:
1. Use the MWAA webserver URL as `AIRFLOW_BASE_URL`
2. Create an Airflow user with appropriate permissions
3. Use the Airflow username and password for authentication

```bash
export AIRFLOW_BASE_URL="https://your-mwaa-id.mwaa.region.amazonaws.com"
export AIRFLOW_USERNAME="admin"
export AIRFLOW_PASSWORD="your_password"
```

#### Astronomer

For Astronomer:
1. Use your Astronomer Airflow deployment URL as `AIRFLOW_BASE_URL`
2. Create an API key or use your login credentials

```bash
export AIRFLOW_BASE_URL="https://your-deployment.astronomer.run"
export AIRFLOW_USERNAME="your_username"
export AIRFLOW_PASSWORD="your_password_or_api_key"
```

#### Self-hosted Airflow

For self-hosted Airflow:
1. Use your Airflow webserver URL as `AIRFLOW_BASE_URL`
2. Use your Airflow credentials for authentication

```bash
export AIRFLOW_BASE_URL="https://your-airflow.example.com"
export AIRFLOW_USERNAME="admin"
export AIRFLOW_PASSWORD="your_password"
```

## Usage

### CLI Commands

#### List Available DAGs

```bash
# List all active DAGs
paradime run airflow-list-dags

# List all DAGs (including paused)
paradime run airflow-list-dags --no-only-active
```

#### Trigger DAG Runs

```bash
# Trigger a single DAG
paradime run airflow-trigger --dag-id my_dag

# Trigger multiple DAGs
paradime run airflow-trigger --dag-id dag1 --dag-id dag2

# Trigger without waiting for completion
paradime run airflow-trigger --dag-id my_dag --no-wait-for-completion

# Trigger without showing logs
paradime run airflow-trigger --dag-id my_dag --no-show-logs

# Custom timeout (default: 1440 minutes)
paradime run airflow-trigger --dag-id my_dag --timeout-minutes 60
```

### Python SDK

#### List DAGs

```python
from paradime.core.scripts.airflow import list_airflow_dags

list_airflow_dags(
    base_url="https://your-airflow-instance.com",
    username="your_username",
    password="your_password",
    only_active=True,
)
```

#### Trigger DAG Runs

```python
from paradime.core.scripts.airflow import trigger_airflow_dags

results = trigger_airflow_dags(
    base_url="https://your-airflow-instance.com",
    username="your_username",
    password="your_password",
    dag_ids=["dag1", "dag2"],
    dag_run_conf={"key": "value"},  # Optional configuration
    wait_for_completion=True,
    timeout_minutes=60,
    show_logs=True,
)

# Check results
for result in results:
    print(result)
```

## Features

### Real-time Progress Monitoring

When `wait_for_completion=True`, the trigger will:
- Monitor DAG run status in real-time
- Display task execution progress
- Show completion status for each task

### Task Log Display

When `show_logs=True`, the trigger will:
- Automatically fetch and display task logs
- Show logs for completed tasks (success or failure)
- Display the last 50 lines of each task's logs for readability

### Parallel Execution

The SDK supports triggering multiple DAGs in parallel using ThreadPoolExecutor for optimal performance.

### Error Handling

The SDK includes:
- Automatic retry logic for network errors
- Timeout handling for long-running DAGs
- Detailed error messages with context

## Examples

See the example scripts in this directory:
- `list_dags.py` - List all available DAGs
- `trigger_dag.py` - Trigger DAG runs with various options

Run examples:
```bash
python examples/airflow/list_dags.py
python examples/airflow/trigger_dag.py
```

## Authentication

The Airflow REST API uses basic authentication. Make sure your Airflow instance has the REST API enabled and your user has the necessary permissions to:
- View DAGs
- Trigger DAG runs
- View task logs
- View DAG run status

## Troubleshooting

### Connection Issues

If you encounter connection issues:
1. Verify the `AIRFLOW_BASE_URL` is correct and accessible
2. Check that the Airflow REST API is enabled
3. Ensure your credentials are valid

### Authentication Errors

If you get 401/403 errors:
1. Verify your username and password are correct
2. Check that your user has the necessary permissions
3. For MWAA, ensure you've created the user in the Airflow UI

### API Version Compatibility

This implementation uses Airflow REST API v1, which is available in:
- Airflow 2.0+
- AWS MWAA (all versions)
- Astronomer Cloud and Enterprise

For older Airflow versions (1.x), you may need to use the experimental API or upgrade your instance.
