# First party modules
from paradime import Paradime

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

# Name of the Bolt schedule to trigger
BOLT_SCHEDULE_NAME = "daily_run"

# Trigger a run of the Bolt schedule and get the run ID
run_id = paradime.bolt.trigger_run(BOLT_SCHEDULE_NAME)

# Trigger a run with custom commands and PR number
run_id_with_pr = paradime.bolt.trigger_run(
    BOLT_SCHEDULE_NAME,
    commands=["dbt run --select modified+", "dbt test --select modified+"],
    pr_number=123
)

# Get the run status
run_status = paradime.bolt.get_run_status(run_id)
