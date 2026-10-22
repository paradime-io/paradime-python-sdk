# First party modules
from paradime import Paradime

# Create a Paradime client with your API credentials
paradime = Paradime(api_endpoint="API_ENDPOINT", api_key="API_KEY", api_secret="API_SECRET")

# Define the run ID for which to list dbt node results
RUN_ID = 12345

# List every dbt node result (models, tests, seeds, snapshots) of the run
node_executions = paradime.bolt.list_run_node_executions(RUN_ID)

# Print the nodes that did not succeed
for node in node_executions:
    if node.status not in ("success", "pass"):
        print(node.unique_id, node.status, node.message)
