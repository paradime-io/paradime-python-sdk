# First party modules
from paradime import Paradime

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

BOLT_SCHEDULE_RUN_ID = 1  # Replace with the run ID of the Bolt schedule to cancel

# Cancel the run
paradime.bolt.cancel_run(BOLT_SCHEDULE_RUN_ID)
