# First party modules
from paradime import Paradime

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

# Define the schedule name for which to list runs
SCHEDULE_NAME = "daily_run"

# List all runs
runs = paradime.bolt.list_runs(schedule_name=SCHEDULE_NAME).runs
