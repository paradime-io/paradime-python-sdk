# First party modules
from paradime import Paradime

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

# List all schedules
schedules = paradime.bolt.list_schedules().schedules

# Each schedule exposes its paused state
for s in schedules:
    print(s.slug, "suspended" if s.suspended else "active")

# Filter by paused (suspended) state server-side:
#   suspended=True  -> only paused schedules
#   suspended=False -> only active (non-paused) schedules
#   suspended=None  -> all schedules (default)
paused_schedules = paradime.bolt.list_schedules(suspended=True).schedules

