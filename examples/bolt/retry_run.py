# First party modules
from paradime import Paradime

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

BOLT_SCHEDULE_RUN_ID = 1  # Replace with the run ID of the failed Bolt schedule to retry

# Retry only the failed commands from a previous Bolt run.
# Uses `dbt retry` under the hood when the failed command supports it.
new_run_id = paradime.bolt.retry_run(BOLT_SCHEDULE_RUN_ID)
print(f"Retry (failed only) started: run_id={new_run_id}")

# Or retry every original command verbatim, regardless of status.
new_run_id_all = paradime.bolt.retry_run_all(BOLT_SCHEDULE_RUN_ID)
print(f"Retry (all commands) started: run_id={new_run_id_all}")
