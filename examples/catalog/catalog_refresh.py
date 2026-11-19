# First party modules
from paradime import Paradime

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

# Request a refresh of the Paradime catalog. If a refresh is already running, it is returned.
request = paradime.catalog.refresh()
print(f"Refresh id: {request.token}, already running: {request.joined}")

# Read the state of the refresh and of each of its steps
if request.token:
    refresh = paradime.catalog.get_refresh(request.token)
    if refresh:
        print(refresh.status)
        for step in refresh.steps:
            print(step.kind, step.status, step.progress, step.error)
