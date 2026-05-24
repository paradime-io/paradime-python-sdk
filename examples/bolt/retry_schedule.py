# First party modules
from paradime import Paradime

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

SCHEDULE_NAME = "my-schedule"  # Replace with the name of the schedule to retry

# Retry the latest failed run of a schedule, resuming from the failed command.
new_run_id = paradime.bolt.retry_schedule_from_failure(SCHEDULE_NAME)
print(f"Retry started: run_id={new_run_id}")
